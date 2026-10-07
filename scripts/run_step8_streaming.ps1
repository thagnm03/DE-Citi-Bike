[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true
$projectRoot = [IO.Path]::GetFullPath((Split-Path -Parent $PSScriptRoot))
$runtime = [IO.Path]::GetFullPath((Join-Path $projectRoot "artifacts\step8\runtime"))
$allowed = [IO.Path]::GetFullPath((Join-Path $projectRoot "artifacts\step8"))
if (-not $runtime.StartsWith($allowed, [StringComparison]::OrdinalIgnoreCase)) {
    throw "Step 8 runtime path escaped artifacts/step8: $runtime"
}
if (Test-Path -LiteralPath $runtime) {
    Remove-Item -LiteralPath $runtime -Recurse -Force
}

$envFile = Join-Path $projectRoot "config\environments\local.env"
$composeFile = Join-Path $projectRoot "docker-compose.yml"
$compose = @("compose", "--env-file", $envFile, "-f", $composeFile)
$sparkArgs = @(
    "run", "--rm", "--no-deps", "toolbox",
    "/opt/spark/bin/spark-submit",
    "--master", "local[2]",
    "--driver-memory", "2g",
    "/workspace/spark/streaming/station_state_job.py",
    "--source", "file",
    "--sink", "file",
    "--input-dir", "/workspace/artifacts/step8/runtime/input",
    "--output-root", "/workspace/artifacts/step8/runtime/output",
    "--checkpoint-root", "/workspace/artifacts/step8/runtime/checkpoints",
    "--progress-root", "/workspace/artifacts/step8/runtime/progress",
    "--baseline-path", "/workspace/data/gold/station_demand_baseline",
    "--watermark-delay", "10 minutes",
    "--window-duration", "5 minutes"
)

Push-Location $projectRoot
try {
    docker info *> $null
    docker @compose config --quiet
    .\.venv\Scripts\python.exe tests/streaming/prepare_input.py

    $runArgs = $compose + $sparkArgs
    docker @runArgs

    Get-ChildItem -LiteralPath (Join-Path $runtime "staged_phase2") -Filter "*.json" -File |
        Sort-Object Name |
        Copy-Item -Destination (Join-Path $runtime "input")
    docker @runArgs

    $updatesPath = Join-Path $runtime "output\state_updates"
    $beforeRestart = @(
        Get-ChildItem -LiteralPath $updatesPath -Filter "part-*.json" -File |
            ForEach-Object { Get-Content -LiteralPath $_.FullName }
    ).Count
    docker @runArgs
    $afterRestart = @(
        Get-ChildItem -LiteralPath $updatesPath -Filter "part-*.json" -File |
            ForEach-Object { Get-Content -LiteralPath $_.FullName }
    ).Count
    if ($beforeRestart -ne $afterRestart) {
        throw "No-input checkpoint restart emitted additional state updates: $beforeRestart -> $afterRestart"
    }

    .\.venv\Scripts\python.exe scripts/verify_step8.py
    Write-Output "Step 8 streaming gate: PASS"
}
finally {
    Pop-Location
}
