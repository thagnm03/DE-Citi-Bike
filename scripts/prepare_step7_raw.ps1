[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$projectRoot = [IO.Path]::GetFullPath((Split-Path -Parent $PSScriptRoot))
$archive = Join-Path $projectRoot "data\samples\trips\202604-citibike-tripdata.zip"
$target = Join-Path $projectRoot "data\raw\trips\source=citibike\year=2026\month=04"
$expected = @(
    "202604-citibike-tripdata-part1.csv",
    "202604-citibike-tripdata-part2.csv",
    "202604-citibike-tripdata-part3.csv",
    "202604-citibike-tripdata-part4.csv"
)

if (-not (Test-Path -LiteralPath $archive)) {
    throw "Historical source archive is missing: $archive"
}

$resolvedTarget = [IO.Path]::GetFullPath($target)
$allowedRoot = [IO.Path]::GetFullPath((Join-Path $projectRoot "data\raw\trips"))
if (-not $resolvedTarget.StartsWith($allowedRoot, [StringComparison]::OrdinalIgnoreCase)) {
    throw "Raw extraction target escaped the expected project directory: $resolvedTarget"
}

New-Item -ItemType Directory -Path $resolvedTarget -Force | Out-Null
$missing = @($expected | Where-Object { -not (Test-Path -LiteralPath (Join-Path $resolvedTarget $_)) })
if ($missing.Count -gt 0) {
    tar.exe -xf $archive -C $resolvedTarget
    if ($LASTEXITCODE -ne 0) { throw "Could not extract historical archive." }
}

foreach ($name in $expected) {
    $path = Join-Path $resolvedTarget $name
    if (-not (Test-Path -LiteralPath $path)) { throw "Expected CSV is missing after extraction: $name" }
}

$files = Get-ChildItem -LiteralPath $resolvedTarget -Filter "*.csv" -File | Sort-Object Name
[ordered]@{
    gate = "PASS"
    archive = $archive
    target = $resolvedTarget
    csv_files = $files.Count
    extracted_bytes = ($files | Measure-Object -Property Length -Sum).Sum
} | ConvertTo-Json -Depth 4
