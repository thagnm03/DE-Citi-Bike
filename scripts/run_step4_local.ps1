[CmdletBinding()]
param(
    [string]$Python = "python"
)

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot
Push-Location $projectRoot

try {
    & $Python -m pytest -q
    if ($LASTEXITCODE -ne 0) {
        throw "Step 4 tests failed."
    }

    & $Python -m prototype.demo
    if ($LASTEXITCODE -ne 0) {
        throw "Step 4 local demo failed."
    }
}
finally {
    Pop-Location
}
