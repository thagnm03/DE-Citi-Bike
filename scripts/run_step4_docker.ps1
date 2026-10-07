[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true
$projectRoot = [IO.Path]::GetFullPath((Split-Path -Parent $PSScriptRoot))
$composeFile = Join-Path $projectRoot "compose.step4.yml"
$artifactDir = [IO.Path]::GetFullPath((Join-Path $projectRoot "artifacts\step4\docker-latest"))

if (-not $artifactDir.StartsWith($projectRoot, [StringComparison]::OrdinalIgnoreCase)) {
    throw "Artifact directory escaped the project root: $artifactDir"
}

Push-Location $projectRoot
try {
    docker info *> $null
    if ($LASTEXITCODE -ne 0) { throw "Docker Engine is not available." }

    if (Test-Path -LiteralPath $artifactDir) {
        Remove-Item -LiteralPath $artifactDir -Recurse -Force
    }
    New-Item -ItemType Directory -Path $artifactDir -Force | Out-Null

    .\.venv\Scripts\python.exe -m pytest -q
    if ($LASTEXITCODE -ne 0) { throw "Unit tests failed." }
    .\.venv\Scripts\python.exe -m prototype.demo
    if ($LASTEXITCODE -ne 0) { throw "Reference demo failed." }

    docker compose -p citibike-step4 -f $composeFile down --volumes --remove-orphans
    docker compose -p citibike-step4 -f $composeFile build producer
    docker compose -p citibike-step4 -f $composeFile up -d kafka
    docker compose -p citibike-step4 -f $composeFile run --rm kafka-init

    docker compose -p citibike-step4 -f $composeFile run --rm producer
    docker compose -p citibike-step4 -f $composeFile run --rm spark-main

    $mainOutput = "/workspace/artifacts/step4/docker-latest/main-alerts.ndjson"
    docker compose -p citibike-step4 -f $composeFile run --rm toolbox `
        python3 -m prototype.kafka_consumer --bootstrap-servers kafka:29092 `
        --topic citibike.station-alerts.v1 --expected-count 2 --output $mainOutput

    docker compose -p citibike-step4 -f $composeFile run --rm spark-main
    docker compose -p citibike-step4 -f $composeFile run --rm producer `
        python3 -m prototype.kafka_producer --bootstrap-servers kafka:29092 `
        --input /workspace/artifacts/step4/local-demo/late-observation.ndjson
    docker compose -p citibike-step4 -f $composeFile run --rm spark-main
    docker compose -p citibike-step4 -f $composeFile run --rm producer
    docker compose -p citibike-step4 -f $composeFile run --rm spark-main

    docker compose -p citibike-step4 -f $composeFile run --rm spark-replay
    $replayOutput = "/workspace/artifacts/step4/docker-latest/replay-alerts.ndjson"
    docker compose -p citibike-step4 -f $composeFile run --rm toolbox `
        python3 -m prototype.kafka_consumer --bootstrap-servers kafka:29092 `
        --topic citibike.station-alerts.replay.v1 --expected-count 2 --output $replayOutput

    .\.venv\Scripts\python.exe -m prototype.compare_replay `
        "$artifactDir\main-alerts.ndjson" "$artifactDir\replay-alerts.ndjson"
    .\.venv\Scripts\python.exe -m prototype.build_step4_summary `
        --bootstrap-servers localhost:9092 `
        --main-alerts "$artifactDir\main-alerts.ndjson" `
        --replay-alerts "$artifactDir\replay-alerts.ndjson" `
        --main-progress "$artifactDir\main-progress.ndjson" `
        --audit-output "$artifactDir\kafka-audit.json" `
        --output "$artifactDir\run-summary.json"
}
finally {
    Pop-Location
}
