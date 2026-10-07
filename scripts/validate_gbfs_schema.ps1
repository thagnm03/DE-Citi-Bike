$ErrorActionPreference = 'Stop'

$workspaceRoot = Split-Path -Parent $PSScriptRoot
$gbfsDirectory = Join-Path $workspaceRoot 'data\samples\gbfs'
$schemaDirectory = Join-Path $gbfsDirectory 'schema-v1.1'

$checks = @(
    @{ Data = 'system_information.json'; Schema = 'system_information.json' },
    @{ Data = 'station_information.json'; Schema = 'station_information.json' },
    @{ Data = 'station_status_01.json'; Schema = 'station_status.json' },
    @{ Data = 'station_status_02.json'; Schema = 'station_status.json' },
    @{ Data = 'station_status_03.json'; Schema = 'station_status.json' }
)

$results = foreach ($check in $checks) {
    $dataPath = Join-Path $gbfsDirectory $check.Data
    $schemaPath = Join-Path $schemaDirectory $check.Schema

    try {
        $valid = Get-Content -Raw -LiteralPath $dataPath |
            Test-Json -SchemaFile $schemaPath -ErrorAction Stop
        [pscustomobject]@{
            data = $check.Data
            schema = $check.Schema
            valid = $valid
            error = $null
        }
    }
    catch {
        [pscustomobject]@{
            data = $check.Data
            schema = $check.Schema
            valid = $false
            error = $_.Exception.Message
        }
    }
}

$results | ConvertTo-Json -Depth 4
