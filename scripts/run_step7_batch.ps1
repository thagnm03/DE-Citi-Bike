[CmdletBinding()]
param(
    [switch]$Sample
)

$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true
$projectRoot = [IO.Path]::GetFullPath((Split-Path -Parent $PSScriptRoot))
$envFile = Join-Path $projectRoot "config\environments\local.env"
$composeFile = Join-Path $projectRoot "docker-compose.yml"
$compose = @("compose", "--env-file", $envFile, "-f", $composeFile)

Push-Location $projectRoot
try {
    docker info *> $null
    docker @compose config --quiet

    if ($Sample) {
        $pipelineArgs = @(
            "--input", "/workspace/data/samples/trips/202604-trip-sample-500.csv",
            "--manifest", "/workspace/artifacts/step7/sample/manifest.json",
            "--silver", "/workspace/artifacts/step7/sample/silver/trips",
            "--quality", "/workspace/artifacts/step7/sample/silver/trip_quality_issues",
            "--hourly", "/workspace/artifacts/step7/sample/gold/station_hourly_demand",
            "--baseline", "/workspace/artifacts/step7/sample/gold/station_demand_baseline",
            "--summary", "/workspace/artifacts/step7/sample/batch-summary.json"
        )
    }
    else {
        & (Join-Path $PSScriptRoot "prepare_step7_raw.ps1")
        $pipelineArgs = @(
            "--input", "/workspace/data/raw/trips/source=citibike/year=2026/month=04",
            "--source-archive", "/workspace/data/samples/trips/202604-citibike-tripdata.zip"
        )
    }

    $dockerArgs = $compose + @(
        "run", "--rm", "--no-deps", "toolbox",
        "/opt/spark/bin/spark-submit",
        "--master", "local[2]",
        "--driver-memory", "3g",
        "--conf", "spark.sql.shuffle.partitions=24",
        "/workspace/spark/batch/historical_pipeline.py"
    ) + $pipelineArgs
    docker @dockerArgs

    if (-not $Sample) {
        $qualityArgs = $compose + @(
            "run", "--rm", "--no-deps", "toolbox",
            "/opt/spark/bin/spark-submit",
            "--master", "local[2]",
            "/workspace/tests/batch/run_spark_quality_fixture.py"
        )
        docker @qualityArgs

        $verifyArgs = $compose + @(
            "run", "--rm", "--no-deps", "toolbox",
            "python3", "/workspace/scripts/verify_step7.py"
        )
        docker @verifyArgs
    }

    $summary = if ($Sample) {
        Join-Path $projectRoot "artifacts\step7\sample\batch-summary.json"
    } else {
        Join-Path $projectRoot "artifacts\step7\batch-summary.json"
    }
    $result = Get-Content -LiteralPath $summary -Raw | ConvertFrom-Json
    if ($result.gate -ne "PASS") { throw "Step 7 batch gate did not pass." }
    Write-Output "Step 7 batch gate: PASS"
    Write-Output "Summary: $summary"
}
finally {
    Pop-Location
}
