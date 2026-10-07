[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot

$checks = @(
    @{
        Name = 'station-status-valid'
        Instance = Join-Path $projectRoot 'contracts/examples/station-status-valid.json'
        Schema = Join-Path $projectRoot 'contracts/station-status-v1.json'
    },
    @{
        Name = 'station-status-sentinel'
        Instance = Join-Path $projectRoot 'contracts/examples/station-status-sentinel.json'
        Schema = Join-Path $projectRoot 'contracts/station-status-v1.json'
    },
    @{
        Name = 'station-alert-opened'
        Instance = Join-Path $projectRoot 'contracts/examples/station-alert-opened.json'
        Schema = Join-Path $projectRoot 'contracts/station-alert-v1.json'
    },
    @{
        Name = 'station-alert-resolved'
        Instance = Join-Path $projectRoot 'contracts/examples/station-alert-resolved.json'
        Schema = Join-Path $projectRoot 'contracts/station-alert-v1.json'
    }
)

$failed = $false

foreach ($check in $checks) {
    $json = Get-Content -LiteralPath $check.Instance -Raw -Encoding UTF8
    $valid = Test-Json -Json $json -SchemaFile $check.Schema

    if ($valid) {
        Write-Host "PASS $($check.Name)"
    }
    else {
        Write-Host "FAIL $($check.Name)"
        $failed = $true
    }
}

$statusSchema = Join-Path $projectRoot 'contracts/station-status-v1.json'
$invalidStatus = Get-Content -LiteralPath (Join-Path $projectRoot 'contracts/examples/station-status-valid.json') -Raw -Encoding UTF8 | ConvertFrom-Json
$invalidStatus.availability.bikes_available = -1
$negativeCountRejected = -not (Test-Json -Json ($invalidStatus | ConvertTo-Json -Depth 20) -SchemaFile $statusSchema -ErrorAction SilentlyContinue)

if ($negativeCountRejected) {
    Write-Host 'PASS rejects-negative-availability'
}
else {
    Write-Host 'FAIL rejects-negative-availability'
    $failed = $true
}

$alertSchema = Join-Path $projectRoot 'contracts/station-alert-v1.json'
$invalidResolvedAlert = Get-Content -LiteralPath (Join-Path $projectRoot 'contracts/examples/station-alert-resolved.json') -Raw -Encoding UTF8 | ConvertFrom-Json
$invalidResolvedAlert.time.resolved_at_utc = $null
$missingResolvedTimeRejected = -not (Test-Json -Json ($invalidResolvedAlert | ConvertTo-Json -Depth 20) -SchemaFile $alertSchema -ErrorAction SilentlyContinue)

if ($missingResolvedTimeRejected) {
    Write-Host 'PASS rejects-resolved-alert-without-resolved-time'
}
else {
    Write-Host 'FAIL rejects-resolved-alert-without-resolved-time'
    $failed = $true
}

if ($failed) {
    exit 1
}

Write-Host "All contract examples are valid."
