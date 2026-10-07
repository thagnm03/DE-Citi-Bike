[CmdletBinding()]
param(
    [ValidateSet("local", "ci")]
    [string]$Environment = "local",
    [string]$Output
)

$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true
$projectRoot = [IO.Path]::GetFullPath((Split-Path -Parent $PSScriptRoot))
$composeFile = Join-Path $projectRoot "docker-compose.yml"
$envFile = Join-Path $projectRoot "config\environments\$Environment.env"
$composeArgs = @("compose", "--env-file", $envFile, "-f", $composeFile)

function Read-EnvironmentFile {
    param([string]$Path)
    $values = @{}
    foreach ($line in Get-Content -LiteralPath $Path -Encoding UTF8) {
        $trimmed = $line.Trim()
        if (-not $trimmed -or $trimmed.StartsWith("#")) { continue }
        $pair = $trimmed.Split("=", 2)
        if ($pair.Count -eq 2) { $values[$pair[0]] = $pair[1] }
    }
    return $values
}

function Wait-ServiceHealthy {
    param(
        [string]$Service,
        [int]$TimeoutSeconds = 180
    )
    $deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    do {
        $containerId = ((& docker @composeArgs ps -q $Service) | Out-String).Trim()
        if ($containerId) {
            $status = ((& docker inspect --format "{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}" $containerId) | Out-String).Trim()
            if ($status -eq "healthy") { return }
            if ($status -in @("exited", "dead", "unhealthy")) {
                throw "Service '$Service' entered terminal status '$status'."
            }
        }
        Start-Sleep -Seconds 2
    } while ([DateTime]::UtcNow -lt $deadline)
    throw "Timed out waiting for service '$Service' to become healthy."
}

if (-not (Test-Path -LiteralPath $envFile)) {
    throw "Environment file does not exist: $envFile"
}

Push-Location $projectRoot
try {
    foreach ($service in @("kafka", "postgres", "spark-master", "spark-worker")) {
        Wait-ServiceHealthy -Service $service
    }

    $expectedTopics = @(
        "citibike.station-status.v1",
        "citibike.station-status.invalid.v1",
        "citibike.station-current.v1",
        "citibike.station-windows.v1",
        "citibike.station-alerts.invalid.v1",
        "citibike.station-alerts.v1",
        "citibike.station-alerts.replay.v1"
    )
    $topics = @(
        & docker @composeArgs exec -T kafka /opt/kafka/bin/kafka-topics.sh `
            --bootstrap-server kafka:29092 --list
    ) | Where-Object { $_ }
    foreach ($topic in $expectedTopics) {
        if ($topic -notin $topics) { throw "Kafka topic is missing: $topic" }
    }

    $settings = Read-EnvironmentFile -Path $envFile
    $expectedPartitions = [int]$settings["KAFKA_TOPIC_PARTITIONS"]
    foreach ($topic in $expectedTopics) {
        $description = (
            & docker @composeArgs exec -T kafka /opt/kafka/bin/kafka-topics.sh `
                --bootstrap-server kafka:29092 --describe --topic $topic
        ) | Out-String
        if ($description -notmatch "PartitionCount:\s+$expectedPartitions") {
            throw "Kafka topic '$topic' does not have $expectedPartitions partitions."
        }
    }

    $postgresCommand = 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "SELECT 1"'
    $postgresResult = ((& docker @composeArgs exec -T postgres sh -ec $postgresCommand) | Out-String).Trim()
    if ($postgresResult -ne "1") { throw "PostgreSQL SELECT 1 smoke test failed." }

    $sparkCheck = @'
import json
import urllib.request

with urllib.request.urlopen("http://localhost:8080/json/", timeout=10) as response:
    payload = json.load(response)
assert payload["status"] == "ALIVE", payload
alive_workers = [worker for worker in payload.get("workers", []) if worker.get("state") == "ALIVE"]
assert alive_workers, payload
print(len(alive_workers))
'@
    $workerCount = ((& docker @composeArgs exec -T spark-master python3 -c $sparkCheck) | Out-String).Trim()
    if ([int]$workerCount -lt 1) { throw "Spark master has no active worker." }

    $summary = [ordered]@{
        gate = "PASS"
        environment = $Environment
        verified_at_utc = [DateTime]::UtcNow.ToString("o")
        services = [ordered]@{
            kafka = "healthy"
            postgres = "healthy"
            spark_master = "healthy"
            spark_worker = "healthy"
        }
        kafka = [ordered]@{
            topics = $expectedTopics
            partitions_per_topic = $expectedPartitions
        }
        postgres = [ordered]@{ select_one = $true }
        spark = [ordered]@{ active_workers = [int]$workerCount }
    }

    if ($Output) {
        $outputPath = [IO.Path]::GetFullPath((Join-Path $projectRoot $Output))
        if (-not $outputPath.StartsWith($projectRoot, [StringComparison]::OrdinalIgnoreCase)) {
            throw "Verification output escaped project root: $outputPath"
        }
        New-Item -ItemType Directory -Path (Split-Path -Parent $outputPath) -Force | Out-Null
        $summary | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $outputPath -Encoding UTF8
    }
    $summary | ConvertTo-Json -Depth 8
}
finally {
    Pop-Location
}
