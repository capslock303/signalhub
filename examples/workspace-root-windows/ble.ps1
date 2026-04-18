#Requires -Version 5.1
# Copy to workspace root. Usage: .\ble.ps1 -- init-db
$Dev = Join-Path $PSScriptRoot "..\..\scripts\dev\signalhub-ble.ps1"
& (Resolve-Path $Dev) @args
