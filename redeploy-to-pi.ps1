#Requires -Version 5.1
<#
.SYNOPSIS
  Redeploy signalhub to the Raspberry Pi from Windows (tarball + remote pip install).

.DESCRIPTION
  Wrapper around scripts\pi\deploy_from_windows.ps1. Run this from the signalhub repo root
  (the folder that contains pyproject.toml).

.EXAMPLE
  .\redeploy-to-pi.ps1

.EXAMPLE
  .\redeploy-to-pi.ps1 -PiHost 192.168.8.112 -PiUser kpi

.EXAMPLE
  .\redeploy-to-pi.ps1 -NonInteractive
#>
$ErrorActionPreference = "Stop"
$deploy = Join-Path $PSScriptRoot "scripts\pi\deploy_from_windows.ps1"
if (-not (Test-Path $deploy)) {
  throw "Expected deploy script at: $deploy - run from the signalhub repo root (folder with pyproject.toml)."
}
Write-Host "Running: $deploy $args"
& $deploy @args
exit $LASTEXITCODE
