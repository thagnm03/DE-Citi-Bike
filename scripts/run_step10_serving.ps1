[CmdletBinding()]
param(
    [ValidateSet("local", "ci")]
    [string]$Environment = "local",
    [switch]$SkipBuild
)

$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true
$projectRoot = [IO.Path]::GetFullPath((Split-Path -Parent $PSScriptRoot))
$envFile = Join-Path $projectRoot "config\environments\$Environment.env"
$composeFile = Join-Path $projectRoot "docker-compose.yml"
$artifactRoot = [IO.Path]::GetFullPath((Join-Path $projectRoot "artifacts\step10"))
$allowedArtifactRoot = [IO.Path]::GetFullPath((Join-Path $projectRoot "artifacts")) + [IO.Path]::DirectorySeparatorChar
if (-not $artifactRoot.StartsWith($allowedArtifactRoot, [StringComparison]::OrdinalIgnoreCase)) {
    throw "Refusing to reset artifact path outside project artifacts: $artifactRoot"
}
if (Test-Path -LiteralPath $artifactRoot) {
    Remove-Item -LiteralPath $artifactRoot -Recurse -Force
}
New-Item -ItemType Directory -Path $artifactRoot | Out-Null

$composeArgs = @("compose", "--env-file", $envFile, "-f", $composeFile, "--profile", "serving")
$databaseUrl = if ($Environment -eq "ci") {
    "postgresql://citibike_ci:citibike_ci_only@postgres:5432/citibike_ci"
} else {
    "postgresql://citibike:citibike_local_only@postgres:5432/citibike"
}
$runToken = [DateTime]::UtcNow.ToString("yyyyMMddHHmmss")
$gateTopic = "citibike.station-alerts.step10-gate-$runToken"
$gateGroup = "citibike-serving-step10-gate-$runToken"
$topicCreated = $false

function Wait-Healthy {
    param([string]$Service, [int]$TimeoutSeconds = 180)
    $deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    do {
        $containerId = ((& docker @composeArgs ps -q $Service) | Out-String).Trim()
        if ($containerId) {
            $health = ((& docker inspect --format "{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}" $containerId) | Out-String).Trim()
            if ($health -eq "healthy") { return }
            if ($health -in @("unhealthy", "exited", "dead")) {
                throw "$Service entered terminal state $health"
            }
        }
        Start-Sleep -Seconds 2
    } while ([DateTime]::UtcNow -lt $deadline)
    throw "Timed out waiting for $Service"
}

try {
    & (Join-Path $projectRoot ".venv\Scripts\python.exe") -m tests.serving.prepare_fixture `
        --output (Join-Path $artifactRoot "fixture")

    if (-not $SkipBuild) {
        & docker @composeArgs build serving-api
    }
    & docker @composeArgs up -d postgres kafka
    Wait-Healthy postgres
    Wait-Healthy kafka

    & docker @composeArgs run --rm --no-deps serving-db-init `
        python3 -m serving.init_db --database-url $databaseUrl --reset

    & docker @composeArgs exec -T kafka /opt/kafka/bin/kafka-topics.sh `
        --bootstrap-server localhost:29092 --create --topic $gateTopic --partitions 3 --replication-factor 1
    $topicCreated = $true

    & docker @composeArgs run --rm --no-deps serving-db-init `
        python3 -m tests.serving.publish_fixture `
        --input /workspace/artifacts/step10/fixture/alerts-phase1.jsonl `
        --topic $gateTopic

    & docker @composeArgs run --rm --no-deps serving-db-init `
        python3 -m serving.materializer --source kafka --no-current `
        --alert-topic $gateTopic --group-id $gateGroup --max-idle-seconds 5 `
        --database-url $databaseUrl

    & docker @composeArgs run --rm --no-deps serving-db-init `
        python3 -m serving.materializer --source file `
        --current-path /workspace/artifacts/step10/fixture/current-state.jsonl `
        --database-url $databaseUrl

    & docker @composeArgs run --rm --no-deps serving-db-init `
        python3 -m tests.serving.publish_fixture `
        --input /workspace/artifacts/step10/fixture/alerts-phase2.jsonl `
        --topic $gateTopic

    & docker @composeArgs up -d --no-deps serving-api
    Wait-Healthy serving-api

    & docker @composeArgs run --rm --no-deps serving-db-init `
        python3 /workspace/scripts/verify_step10.py `
        --base-url http://serving-api:8000 `
        --database-url $databaseUrl `
        --fixture-root /workspace/artifacts/step10/fixture `
        --output /workspace/artifacts/step10/verification-summary.json `
        --kafka-alert-topic $gateTopic `
        --kafka-group-id $gateGroup

    Get-Content -LiteralPath (Join-Path $artifactRoot "verification-summary.json")
    Write-Output "Step 10 serving gate: PASS"
}
finally {
    if ($topicCreated) {
        & docker @composeArgs exec -T kafka /opt/kafka/bin/kafka-topics.sh `
            --bootstrap-server localhost:29092 --delete --topic $gateTopic 2>$null
    }
    & docker @composeArgs stop serving-api kafka postgres
}
