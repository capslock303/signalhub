#Requires -Version 5.1
<#
.SYNOPSIS
  Start the Streamlit dashboard with workspace = parent of the `signalhub` folder.

.DESCRIPTION
  Use this when your layout is e.g. `nRF/signalhub/...` and you want `./data` and `.env`
  to live next to `signalhub` (workspace root). Does not require `signalhub-ble` on PATH
  if you have not run setup-editable.ps1 yet (uses PYTHONPATH=signalhub/src).

.EXAMPLE
  pwsh -File signalhub/scripts/dev/run-dashboard.ps1
  pwsh -File signalhub/scripts/dev/run-dashboard.ps1 8502
#>
$ErrorActionPreference = "Stop"
$SignalhubRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$WorkspaceRoot = Split-Path $SignalhubRoot -Parent
Set-Location $WorkspaceRoot

$app = Join-Path $SignalhubRoot "streamlit_app.py"
if (-not (Test-Path $app)) {
    throw "Missing $app"
}

$port = 8501
if ($args.Count -ge 1 -and $args[0] -match '^\d+$') {
    $port = [int]$args[0]
    $rest = $args[1..($args.Count - 1)]
} else {
    $rest = $args
}

$env:PYTHONPATH = (Join-Path $SignalhubRoot "src")
Write-Host "Workspace: $WorkspaceRoot"
Write-Host "Streamlit: $app (port $port)"
python -m streamlit run $app --server.port $port @rest
