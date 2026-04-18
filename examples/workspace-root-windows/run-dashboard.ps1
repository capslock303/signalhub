#Requires -Version 5.1
# Copy this file to the folder *above* `signalhub` (workspace root), then: .\run-dashboard.ps1
$Dev = Join-Path $PSScriptRoot "..\..\scripts\dev\run-dashboard.ps1"
& (Resolve-Path $Dev) @args
