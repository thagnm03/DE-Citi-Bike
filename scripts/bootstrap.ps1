[CmdletBinding()]
param(
    [ValidateSet("local", "ci")]
    [string]$Environment = "local",
    [switch]$SkipBuild
)

$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true
$projectRoot = [IO.Path]::GetFullPath((Split-Path -Parent $PSScriptRoot))
$composeFile = Join-Path $projectRoot "docker-compose.yml"
$envFile = Join-Path $projectRoot "config\environments\$Environment.env"
$composeArgs = @("compose", "--env-file", $envFile, "-f", $composeFile)

if (-not (Test-Path -LiteralPath $envFile)) {
    throw "Environment file does not exist: $envFile"
}

Push-Location $projectRoot
try {
    docker info *> $null
    docker @composeArgs config --quiet

    if (-not $SkipBuild) {
        docker @composeArgs build spark-master
    }

    docker @composeArgs up -d kafka postgres spark-master spark-worker
    docker @composeArgs run --rm kafka-init

    & (Join-Path $PSScriptRoot "verify_infrastructure.ps1") `
        -Environment $Environment `
        -Output "artifacts/step5/bootstrap-summary-$Environment.json"
    if ($LASTEXITCODE -ne 0) { throw "Infrastructure verification failed." }

    docker @composeArgs ps
}
finally {
    Pop-Location
}
