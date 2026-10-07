[CmdletBinding()]
param(
    [ValidateSet("local", "ci")]
    [string]$Environment = "local",
    [switch]$KeepRunning
)

$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true
$projectRoot = [IO.Path]::GetFullPath((Split-Path -Parent $PSScriptRoot))
$envFile = Join-Path $projectRoot "config\environments\$Environment.env"
$composeFile = Join-Path $projectRoot "docker-compose.yml"
$artifactRoot = [IO.Path]::GetFullPath((Join-Path $projectRoot "artifacts\step11"))
$allowedArtifactRoot = [IO.Path]::GetFullPath((Join-Path $projectRoot "artifacts")) + [IO.Path]::DirectorySeparatorChar
if (-not $artifactRoot.StartsWith($allowedArtifactRoot, [StringComparison]::OrdinalIgnoreCase)) {
    throw "Refusing to reset artifact path outside project artifacts: $artifactRoot"
}
if (Test-Path -LiteralPath $artifactRoot) {
    Remove-Item -LiteralPath $artifactRoot -Recurse -Force
}
New-Item -ItemType Directory -Path $artifactRoot | Out-Null

$composeArgs = @("compose", "--env-file", $envFile, "-f", $composeFile, "--profile", "serving")
$apiPort = if ($Environment -eq "ci") { 28000 } else { 18000 }

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

try {
    & (Join-Path $projectRoot "scripts\run_step10_serving.ps1") -Environment $Environment -SkipBuild
    & docker @composeArgs up -d postgres
    Wait-Healthy postgres
    & docker @composeArgs up -d --no-deps serving-api
    Wait-Healthy serving-api

    & (Join-Path $projectRoot ".venv\Scripts\python.exe") `
        (Join-Path $projectRoot "scripts\verify_step11.py") `
        --base-url "http://localhost:$apiPort" `
        --output (Join-Path $artifactRoot "verification-summary.json")

    Get-Content -LiteralPath (Join-Path $artifactRoot "verification-summary.json")
    Write-Output "Step 11 operational dashboard gate: PASS"
    if ($KeepRunning) {
        Write-Output "Dashboard is running at http://localhost:$apiPort/dashboard"
    }
}
finally {
    if (-not $KeepRunning) {
        & docker @composeArgs stop serving-api postgres
    }
}
