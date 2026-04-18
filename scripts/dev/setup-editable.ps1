#Requires -Version 5.1
<#
.SYNOPSIS
  pip install -e on the signalhub package (installs signalhub-ble on PATH for this Python).
#>
$ErrorActionPreference = "Stop"
$SignalhubRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
python -m pip install -e $SignalhubRoot
Write-Host "Done. Try: signalhub-ble --help"
