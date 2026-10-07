[CmdletBinding()]
param(
    [ValidateSet("local", "ci")]
    [string]$Environment = "local",
    [switch]$SkipBuild,
    [switch]$KeepRunning
)

$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true
$projectRoot = [IO.Path]::GetFullPath((Split-Path -Parent $PSScriptRoot))
$envFile = Join-Path $projectRoot "config\environments\$Environment.env"
$composeFile = Join-Path $projectRoot "docker-compose.yml"
$artifactRoot = [IO.Path]::GetFullPath((Join-Path $projectRoot "artifacts\step14"))
$allowedArtifactRoot = [IO.Path]::GetFullPath((Join-Path $projectRoot "artifacts")) + [IO.Path]::DirectorySeparatorChar
if (-not $artifactRoot.StartsWith($allowedArtifactRoot, [StringComparison]::OrdinalIgnoreCase)) {
    throw "Refusing to reset artifact path outside project artifacts: $artifactRoot"
}
if (Test-Path -LiteralPath $artifactRoot) {
    Remove-Item -LiteralPath $artifactRoot -Recurse -Force
}
New-Item -ItemType Directory -Path $artifactRoot | Out-Null

$runtime = Join-Path $artifactRoot "runtime"
$composeArgs = @(
    "compose", "--env-file", $envFile, "-f", $composeFile,
    "--profile", "tools", "--profile", "serving", "--profile", "monitoring"
)
$python = Join-Path $projectRoot ".venv\Scripts\python.exe"
$apiPort = if ($Environment -eq "ci") { 28000 } else { 18000 }
$prometheusPort = if ($Environment -eq "ci") { 29090 } else { 19090 }
$databaseName = if ($Environment -eq "ci") { "citibike_ci" } else { "citibike" }
$databaseUser = if ($Environment -eq "ci") { "citibike_ci" } else { "citibike" }
$databasePassword = if ($Environment -eq "ci") { "citibike_ci_only" } else { "citibike_local_only" }
$containerDatabaseUrl = "postgresql://${databaseUser}:${databasePassword}@postgres:5432/$databaseName"
$baseUrl = "http://localhost:$apiPort"
$runToken = [DateTime]::UtcNow.ToString("yyyyMMddHHmmss")
$alertTopic = "citibike.station-alerts.step14-demo-$runToken"
$currentTopic = "citibike.station-current.step14-demo-$runToken"
$consumerGroup = "citibike-serving-step14-demo-$runToken"
$createdTopics = [Collections.Generic.List[string]]::new()
$startedAtEpochMs = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
$phases = [ordered]@{}

function Invoke-TimedPhase {
    param([string]$Name, [scriptblock]$Action)
    $stopwatch = [Diagnostics.Stopwatch]::StartNew()
    & $Action
    $stopwatch.Stop()
    $phases[$Name] = [Math]::Round($stopwatch.Elapsed.TotalSeconds, 3)
}

function Wait-Healthy {
    param([string]$Service, [int]$TimeoutSeconds = 180)
    $deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    do {
        $containerId = ((& docker @composeArgs ps -q $Service) | Out-String).Trim()
        if ($containerId) {
            $health = ((& docker inspect --format "{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}" $containerId) | Out-String).Trim()
            if ($health -eq "healthy") { return }
            if ($health -in @("unhealthy", "exited", "dead")) { throw "$Service entered terminal state $health" }
        }
        Start-Sleep -Seconds 2
    } while ([DateTime]::UtcNow -lt $deadline)
    throw "Timed out waiting for $Service"
}

function Wait-Http {
    param([string]$Url, [int]$TimeoutSeconds = 120)
    $deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    do {
        try {
            $response = Invoke-WebRequest -UseBasicParsing -Uri $Url -TimeoutSec 5
            if ($response.StatusCode -eq 200) { return }
        }
        catch { Start-Sleep -Seconds 2 }
    } while ([DateTime]::UtcNow -lt $deadline)
    throw "Timed out waiting for $Url"
}

try {
    Invoke-TimedPhase "preflight_and_prepare" {
        docker info *> $null
        & docker @composeArgs config --quiet
        if (-not $SkipBuild) {
            & docker @composeArgs build serving-api
        }
        & $python -m demo.final_demo prepare --runtime $runtime
    }

    Invoke-TimedPhase "infrastructure_and_schema" {
        & docker @composeArgs up -d postgres kafka
        Wait-Healthy postgres
        Wait-Healthy kafka
        & docker @composeArgs run --rm --no-deps serving-db-init `
            python3 -m serving.init_db --database-url $containerDatabaseUrl --reset
    }

    Invoke-TimedPhase "spark_decision_engine" {
        $sparkBase = @(
            "run", "--rm", "--no-deps", "toolbox",
            "/opt/spark/bin/spark-submit", "--master", "local[2]", "--driver-memory", "2g",
            "/workspace/spark/streaming/station_alert_job.py",
            "--source", "file", "--sink", "file",
            "--input-dir", "/workspace/artifacts/step14/runtime/input",
            "--output-root", "/workspace/artifacts/step14/runtime/spark-output",
            "--checkpoint-root", "/workspace/artifacts/step14/runtime/spark-checkpoints",
            "--rules", "/workspace/config/business-rules-v1.json",
            "--ack-file", "/workspace/artifacts/step14/runtime/acknowledgements.jsonl"
        )
        & docker @composeArgs @sparkBase
        Get-ChildItem -LiteralPath (Join-Path $runtime "staged_phase2") -Filter '*.json' -File |
            Copy-Item -Destination (Join-Path $runtime "input")
        & docker @composeArgs @sparkBase
        & $python -m demo.final_demo package --runtime $runtime
    }

    Invoke-TimedPhase "kafka_and_materialization" {
        foreach ($topic in @($alertTopic, $currentTopic)) {
            & docker @composeArgs exec -T kafka /opt/kafka/bin/kafka-topics.sh `
                --bootstrap-server localhost:29092 --create --topic $topic `
                --partitions 3 --replication-factor 1
            $createdTopics.Add($topic)
        }
        & docker @composeArgs run --rm --no-deps serving-db-init `
            python3 -m tests.serving.publish_fixture `
            --input /workspace/artifacts/step14/runtime/alerts-to-publish.jsonl `
            --topic $alertTopic
        & docker @composeArgs run --rm --no-deps serving-db-init `
            python3 -m tests.serving.publish_fixture `
            --input /workspace/artifacts/step14/runtime/current-state.jsonl `
            --topic $currentTopic
        & docker @composeArgs run --rm --no-deps serving-db-init `
            python3 -m serving.materializer --source kafka `
            --alert-topic $alertTopic --current-topic $currentTopic `
            --group-id $consumerGroup --max-idle-seconds 5 `
            --database-url $containerDatabaseUrl
    }

    Invoke-TimedPhase "serving_and_monitoring" {
        & docker @composeArgs up -d --no-deps serving-api
        Wait-Healthy serving-api
        & docker @composeArgs up -d prometheus
        Wait-Http "http://localhost:$prometheusPort/-/ready"
        Wait-Http "$baseUrl/dashboard"
    }

    [IO.File]::WriteAllText(
        (Join-Path $artifactRoot "demo-timings.json"),
        ([ordered]@{ phases = $phases } | ConvertTo-Json -Depth 5),
        [Text.UTF8Encoding]::new($false)
    )

    & docker @composeArgs run --rm --no-deps serving-db-init `
        python3 /workspace/scripts/verify_step14.py `
        --base-url http://serving-api:8000 `
        --prometheus-url http://prometheus:9090 `
        --database-url $containerDatabaseUrl `
        --alert-topic $alertTopic --current-topic $currentTopic `
        --runtime /workspace/artifacts/step14/runtime `
        --timings /workspace/artifacts/step14/demo-timings.json `
        --started-at-epoch-ms $startedAtEpochMs `
        --output /workspace/artifacts/step14/verification-summary.json

    Get-Content -LiteralPath (Join-Path $artifactRoot "verification-summary.json")
    Write-Output "Step 14 final end-to-end demo and acceptance gate: PASS"
    if ($KeepRunning) {
        Write-Output "Dashboard: $baseUrl/dashboard"
        Write-Output "Prometheus: http://localhost:$prometheusPort"
    }
}
finally {
    foreach ($topic in $createdTopics) {
        & docker @composeArgs exec -T kafka /opt/kafka/bin/kafka-topics.sh `
            --bootstrap-server localhost:29092 --delete --topic $topic 2>$null
    }
    if (-not $KeepRunning) {
        & docker @composeArgs stop prometheus serving-api kafka postgres
    }
}
