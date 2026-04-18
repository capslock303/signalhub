#Requires -Version 5.1
# Copy to workspace root (parent of `signalhub`). One-time: .\setup-signalhub.ps1
$Dev = Join-Path $PSScriptRoot "..\..\scripts\dev\setup-editable.ps1"
& (Resolve-Path $Dev) @args
