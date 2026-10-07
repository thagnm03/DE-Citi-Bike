[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true
$projectRoot = [IO.Path]::GetFullPath((Split-Path -Parent $PSScriptRoot))
$runtime = [IO.Path]::GetFullPath((Join-Path $projectRoot "artifacts\step9\runtime"))
$allowed = [IO.Path]::GetFullPath((Join-Path $projectRoot "artifacts\step9"))
if (-not $runtime.StartsWith($allowed, [StringComparison]::OrdinalIgnoreCase)) {
    throw "Step 9 runtime path escaped artifacts/step9: $runtime"
}
if (Test-Path -LiteralPath $runtime) {
    Remove-Item -LiteralPath $runtime -Recurse -Force
}

$envFile = Join-Path $projectRoot "config\environments\local.env"
$composeFile = Join-Path $projectRoot "docker-compose.yml"
$compose = @("compose", "--env-file", $envFile, "-f", $composeFile)
$baseSpark = @(
    "run", "--rm", "--no-deps", "toolbox",
    "/opt/spark/bin/spark-submit",
    "--master", "local[2]",
    "--driver-memory", "2g",
    "/workspace/spark/streaming/station_alert_job.py",
    "--source", "file",
    "--sink", "file",
    "--rules", "/workspace/config/business-rules-v1.json",
    "--ack-file", "/workspace/artifacts/step9/runtime/acknowledgements.jsonl"
)
$primary = $compose + $baseSpark + @(
    "--input-dir", "/workspace/artifacts/step9/runtime/input",
    "--output-root", "/workspace/artifacts/step9/runtime/output",
    "--checkpoint-root", "/workspace/artifacts/step9/runtime/checkpoints"
)
$replay = $compose + $baseSpark + @(
    "--input-dir", "/workspace/artifacts/step9/runtime/replay_input",
    "--output-root", "/workspace/artifacts/step9/runtime/replay_output",
    "--checkpoint-root", "/workspace/artifacts/step9/runtime/replay_checkpoints"
)

Push-Location $projectRoot
try {
    docker info *> $null
    docker @compose config --quiet
    .\.venv\Scripts\python.exe tests/alerts/prepare_input.py

    docker @primary

    Get-ChildItem -LiteralPath (Join-Path $runtime "staged_phase2") -Filter "*.json" -File |
        Sort-Object Name |
        Copy-Item -Destination (Join-Path $runtime "input")
    docker @primary

    $eventPath = Join-Path $runtime "output\alert_events"
    $beforeRestart = @(
        Get-ChildItem -LiteralPath $eventPath -Filter "part-*.json" -File |
            ForEach-Object { Get-Content -LiteralPath $_.FullName }
    ).Count
    docker @primary
    $afterRestart = @(
        Get-ChildItem -LiteralPath $eventPath -Filter "part-*.json" -File |
            ForEach-Object { Get-Content -LiteralPath $_.FullName }
    ).Count
    if ($beforeRestart -ne $afterRestart) {
        throw "No-input checkpoint restart emitted alert events: $beforeRestart -> $afterRestart"
    }

    $replayInput = Join-Path $runtime "replay_input"
    New-Item -ItemType Directory -Path $replayInput -Force | Out-Null
    $phase2Names = @(
        Get-ChildItem -LiteralPath (Join-Path $runtime "staged_phase2") -Filter "*.json" -File |
            ForEach-Object { $_.Name }
    )
    Get-ChildItem -LiteralPath (Join-Path $runtime "input") -Filter "*.json" -File |
        Where-Object { $_.Name -notin $phase2Names } |
        Copy-Item -Destination $replayInput
    docker @replay
    Get-ChildItem -LiteralPath (Join-Path $runtime "staged_phase2") -Filter "*.json" -File |
        Copy-Item -Destination $replayInput
    docker @replay

    .\.venv\Scripts\python.exe scripts/verify_step9.py
    Write-Output "Step 9 alert decision gate: PASS"
}
finally {
    Pop-Location
}
