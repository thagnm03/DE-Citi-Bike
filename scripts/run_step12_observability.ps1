[CmdletBinding()]
param(
    [ValidateSet("local", "ci")]
    [string]$Environment = "local",
    [switch]$KeepRunning,
    [switch]$SkipBuild
)

$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true
$projectRoot = [IO.Path]::GetFullPath((Split-Path -Parent $PSScriptRoot))
$envFile = Join-Path $projectRoot "config\environments\$Environment.env"
$composeFile = Join-Path $projectRoot "docker-compose.yml"
$artifactRoot = [IO.Path]::GetFullPath((Join-Path $projectRoot "artifacts\step12"))
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
$apiPort = if ($Environment -eq "ci") { 28000 } else { 18000 }
$prometheusPort = if ($Environment -eq "ci") { 29090 } else { 19090 }
$python = Join-Path $projectRoot ".venv\Scripts\python.exe"

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
        catch {
            Start-Sleep -Seconds 2
        }
    } while ([DateTime]::UtcNow -lt $deadline)
    throw "Timed out waiting for $Url"
}

try {
    & (Join-Path $projectRoot "scripts\run_step10_serving.ps1") `
        -Environment $Environment -SkipBuild:$SkipBuild

    & docker @composeArgs up -d postgres
    Wait-Healthy postgres
    & docker @composeArgs up -d --no-deps serving-api
    Wait-Healthy serving-api
    & docker @composeArgs up -d prometheus
    Wait-Http "http://localhost:$prometheusPort/-/ready"

    $targets = Get-Content -Raw (Join-Path $projectRoot "config\slo-targets.json") | ConvertFrom-Json
    & $python -m observability.benchmark `
        --base-url "http://localhost:$apiPort" `
        --requests $targets.gate_requests `
        --concurrency $targets.gate_concurrency `
        --output (Join-Path $artifactRoot "benchmark-report.json")

    & $python (Join-Path $projectRoot "scripts\verify_step12.py") `
        --base-url "http://localhost:$apiPort" `
        --prometheus-url "http://localhost:$prometheusPort" `
        --benchmark (Join-Path $artifactRoot "benchmark-report.json") `
        --slo-targets (Join-Path $projectRoot "config\slo-targets.json") `
        --output (Join-Path $artifactRoot "verification-summary.json") `
        --slo-output (Join-Path $artifactRoot "slo-report.json")

    Get-Content -LiteralPath (Join-Path $artifactRoot "verification-summary.json")
    Write-Output "Step 12 observability, SLO and performance gate: PASS"
    if ($KeepRunning) {
        Write-Output "Prometheus is running at http://localhost:$prometheusPort"
        Write-Output "Dashboard is running at http://localhost:$apiPort/dashboard"
    }
}
finally {
    if (-not $KeepRunning) {
        & docker @composeArgs stop prometheus serving-api postgres
    }
}
