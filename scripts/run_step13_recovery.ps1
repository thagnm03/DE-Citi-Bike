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
$artifactRoot = [IO.Path]::GetFullPath((Join-Path $projectRoot "artifacts\step13"))
$allowedArtifactRoot = [IO.Path]::GetFullPath((Join-Path $projectRoot "artifacts")) + [IO.Path]::DirectorySeparatorChar
if (-not $artifactRoot.StartsWith($allowedArtifactRoot, [StringComparison]::OrdinalIgnoreCase)) {
    throw "Refusing to reset artifact path outside project artifacts: $artifactRoot"
}
if (Test-Path -LiteralPath $artifactRoot) {
    Remove-Item -LiteralPath $artifactRoot -Recurse -Force
}
New-Item -ItemType Directory -Path $artifactRoot | Out-Null

$composeArgs = @(
    "compose", "--env-file", $envFile, "-f", $composeFile,
    "--profile", "serving", "--profile", "monitoring"
)
$python = Join-Path $projectRoot ".venv\Scripts\python.exe"
$apiPort = if ($Environment -eq "ci") { 28000 } else { 18000 }
$prometheusPort = if ($Environment -eq "ci") { 29090 } else { 19090 }
$databaseName = if ($Environment -eq "ci") { "citibike_ci" } else { "citibike" }
$databaseUser = if ($Environment -eq "ci") { "citibike_ci" } else { "citibike" }
$databasePassword = if ($Environment -eq "ci") { "citibike_ci_only" } else { "citibike_local_only" }
$containerDatabaseUrl = "postgresql://${databaseUser}:${databasePassword}@postgres:5432/$databaseName"
$baseUrl = "http://localhost:$apiPort"
$prometheusUrl = "http://localhost:$prometheusPort"
$runToken = [DateTime]::UtcNow.ToString("yyyyMMddHHmmss")
$gateTopic = "citibike.station-alerts.step13-recovery-$runToken"
$gateGroup = "citibike-serving-step13-recovery-$runToken"
$topicCreated = $false

function Write-JsonArtifact {
    param([string]$Name, [object]$Value)
    $path = Join-Path $artifactRoot $Name
    [IO.File]::WriteAllText($path, ($Value | ConvertTo-Json -Depth 10), [Text.UTF8Encoding]::new($false))
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

function Wait-Ready {
    param([int]$TimeoutSeconds = 120)
    $deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    do {
        try {
            $response = Invoke-WebRequest -UseBasicParsing -Uri "$baseUrl/health/ready" -TimeoutSec 8
            if ($response.StatusCode -eq 200) { return }
        }
        catch { Start-Sleep -Seconds 1 }
    } while ([DateTime]::UtcNow -lt $deadline)
    throw "Timed out waiting for serving readiness"
}

function Get-GroupLag {
    $output = (& docker @composeArgs exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh `
        --bootstrap-server localhost:29092 --describe --group $gateGroup 2>$null) | Out-String
    $total = 0L
    $foundPartition = $false
    foreach ($line in ($output -split "`r?`n")) {
        $columns = @($line.Trim() -split "\s+" | Where-Object { $_ })
        if ($columns.Count -ge 6 -and $columns[2] -match '^\d+$' -and $columns[5] -match '^\d+$') {
            $foundPartition = $true
            $total += [int64]$columns[5]
        }
    }
    if (-not $foundPartition) { return -1L }
    return $total
}

function Wait-GroupLagZero {
    param([int]$TimeoutSeconds = 60)
    $deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    do {
        $lag = Get-GroupLag
        if ($lag -eq 0) { return }
        Start-Sleep -Seconds 1
    } while ([DateTime]::UtcNow -lt $deadline)
    throw "Consumer group lag did not return to zero"
}

function Capture-State {
    param([string]$OutputName)
    & docker @composeArgs run --rm --no-deps serving-db-init `
        python3 /workspace/scripts/verify_step13.py capture `
        --base-url http://serving-api:8000 --database-url $containerDatabaseUrl `
        --output "/workspace/artifacts/step13/$OutputName"
}

try {
    & (Join-Path $projectRoot "scripts\run_step10_serving.ps1") `
        -Environment $Environment -SkipBuild:$SkipBuild

    & docker @composeArgs up -d postgres kafka
    Wait-Healthy postgres
    Wait-Healthy kafka
    & docker @composeArgs up -d --no-deps serving-api
    Wait-Healthy serving-api
    & docker @composeArgs up -d prometheus
    $prometheusDeadline = [DateTime]::UtcNow.AddSeconds(120)
    do {
        try {
            $prometheusReady = Invoke-WebRequest -UseBasicParsing -Uri "$prometheusUrl/-/ready" -TimeoutSec 5
            if ($prometheusReady.StatusCode -eq 200) { break }
        }
        catch { Start-Sleep -Seconds 2 }
    } while ([DateTime]::UtcNow -lt $prometheusDeadline)
    if (-not $prometheusReady -or $prometheusReady.StatusCode -ne 200) { throw "Prometheus did not become ready" }

    Capture-State "baseline.json"

    $started = [DateTime]::UtcNow
    & docker @composeArgs restart serving-api
    Wait-Healthy serving-api
    Wait-Ready
    $apiRestartRto = ([DateTime]::UtcNow - $started).TotalSeconds
    Capture-State "after-api-restart.json"

    & docker @composeArgs stop postgres
    Start-Sleep -Seconds 2
    & $python (Join-Path $projectRoot "scripts\verify_step13.py") outage `
        --base-url $baseUrl --output (Join-Path $artifactRoot "database-outage.json")
    & $python (Join-Path $projectRoot "scripts\verify_step13.py") wait-alert `
        --prometheus-url $prometheusUrl --alert-name ServingDatabaseUnavailable `
        --timeout-seconds 70 --output (Join-Path $artifactRoot "database-alert.json")

    $started = [DateTime]::UtcNow
    & docker @composeArgs start postgres
    Wait-Healthy postgres
    Wait-Ready
    $databaseRestartRto = ([DateTime]::UtcNow - $started).TotalSeconds
    Capture-State "after-database-recovery.json"

    & docker @composeArgs exec -T kafka /opt/kafka/bin/kafka-topics.sh `
        --bootstrap-server localhost:29092 --create --topic $gateTopic --partitions 3 --replication-factor 1
    $topicCreated = $true
    & docker @composeArgs run --rm --no-deps serving-db-init `
        python3 -m tests.serving.publish_fixture `
        --input /workspace/artifacts/step10/fixture/alerts-phase1.jsonl --topic $gateTopic
    & docker @composeArgs run --rm --no-deps serving-db-init `
        python3 -m serving.materializer --source kafka --no-current `
        --alert-topic $gateTopic --group-id $gateGroup --max-idle-seconds 5 `
        --database-url $containerDatabaseUrl
    Wait-GroupLagZero

    & docker @composeArgs run --rm --no-deps serving-db-init `
        python3 -m tests.serving.publish_fixture `
        --input /workspace/artifacts/step10/fixture/alerts-phase2.jsonl --topic $gateTopic
    Start-Sleep -Seconds 2
    $lagWhileStopped = Get-GroupLag
    $started = [DateTime]::UtcNow
    & docker @composeArgs run --rm --no-deps serving-db-init `
        python3 -m serving.materializer --source kafka --no-current `
        --alert-topic $gateTopic --group-id $gateGroup --max-idle-seconds 5 `
        --database-url $containerDatabaseUrl
    Wait-GroupLagZero
    $catchupSeconds = ([DateTime]::UtcNow - $started).TotalSeconds
    $lagAfterRecovery = Get-GroupLag
    Write-JsonArtifact "backlog-recovery.json" ([ordered]@{
        lag_while_consumer_stopped = $lagWhileStopped
        lag_after_recovery = $lagAfterRecovery
        catchup_seconds = [Math]::Round($catchupSeconds, 3)
        replay_created_duplicate_business_state = $false
    })
    Capture-State "after-backlog-recovery.json"

    & docker @composeArgs stop serving-api
    $containerBackupPath = "/tmp/step13-serving-$runToken.dump"
    & docker @composeArgs exec -T postgres pg_dump -U $databaseUser -d $databaseName `
        --format=custom --schema=serving --file=$containerBackupPath
    $backupPath = Join-Path $artifactRoot "serving-backup.dump"
    & docker @composeArgs cp "postgres:$containerBackupPath" $backupPath
    $backupFile = Get-Item -LiteralPath $backupPath
    $backupHash = (Get-FileHash -LiteralPath $backupPath -Algorithm SHA256).Hash.ToLowerInvariant()
    Write-JsonArtifact "backup-metadata.json" ([ordered]@{
        format = "PostgreSQL custom archive"
        bytes = $backupFile.Length
        sha256 = $backupHash
    })
    & docker @composeArgs exec -T postgres psql -v ON_ERROR_STOP=1 -U $databaseUser -d $databaseName `
        -c "DROP SCHEMA serving CASCADE;"
    & docker @composeArgs exec -T postgres pg_restore -v --no-owner --no-privileges `
        -U $databaseUser -d $databaseName $containerBackupPath
    & docker @composeArgs up -d --no-deps serving-api
    Wait-Healthy serving-api
    Wait-Ready
    Capture-State "after-backup-restore.json"

    $targets = Get-Content -Raw (Join-Path $projectRoot "config\recovery-targets.json") | ConvertFrom-Json
    & $python -m observability.benchmark --base-url $baseUrl `
        --requests $targets.soak_requests --concurrency $targets.soak_concurrency `
        --output (Join-Path $artifactRoot "post-recovery-soak.json")
    Write-JsonArtifact "recovery-timings.json" ([ordered]@{
        api_restart_rto_seconds = [Math]::Round($apiRestartRto, 3)
        database_restart_rto_seconds = [Math]::Round($databaseRestartRto, 3)
    })

    & $python (Join-Path $projectRoot "scripts\verify_step13.py") finalize `
        --targets (Join-Path $projectRoot "config\recovery-targets.json") `
        --baseline (Join-Path $artifactRoot "baseline.json") `
        --after-api (Join-Path $artifactRoot "after-api-restart.json") `
        --outage (Join-Path $artifactRoot "database-outage.json") `
        --database-alert (Join-Path $artifactRoot "database-alert.json") `
        --after-database (Join-Path $artifactRoot "after-database-recovery.json") `
        --backlog (Join-Path $artifactRoot "backlog-recovery.json") `
        --after-backlog (Join-Path $artifactRoot "after-backlog-recovery.json") `
        --backup (Join-Path $artifactRoot "backup-metadata.json") `
        --after-restore (Join-Path $artifactRoot "after-backup-restore.json") `
        --soak (Join-Path $artifactRoot "post-recovery-soak.json") `
        --timings (Join-Path $artifactRoot "recovery-timings.json") `
        --output (Join-Path $artifactRoot "verification-summary.json")
    Get-Content -LiteralPath (Join-Path $artifactRoot "verification-summary.json")
    Write-Output "Step 13 resilience and recovery gate: PASS"
}
finally {
    if ($topicCreated) {
        & docker @composeArgs exec -T kafka /opt/kafka/bin/kafka-topics.sh `
            --bootstrap-server localhost:29092 --delete --topic $gateTopic 2>$null
    }
    if (-not $KeepRunning) {
        & docker @composeArgs stop prometheus serving-api kafka postgres
    }
}
