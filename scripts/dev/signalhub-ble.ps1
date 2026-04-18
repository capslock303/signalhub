#Requires -Version 5.1
<#
.SYNOPSIS
  Run signalhub-ble with cwd = workspace root (parent of `signalhub`).

.DESCRIPTION
  Prefers an installed `signalhub-ble`; otherwise `python -m signalhub.ble` with PYTHONPATH.
#>
$ErrorActionPreference = "Stop"
$SignalhubRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$WorkspaceRoot = Split-Path $SignalhubRoot -Parent
Set-Location $WorkspaceRoot

$cmd = Get-Command signalhub-ble -ErrorAction SilentlyContinue
if ($cmd) {
    & signalhub-ble @args
} else {
    $env:PYTHONPATH = (Join-Path $SignalhubRoot "src")
    python -m signalhub.ble @args
}
