#Requires -Version 5.1
<#
.SYNOPSIS
  Run report assess on the Pi and save assessment-from-pi.md in this repo root.

.DESCRIPTION
  Wrapper around scripts\pi\fetch_pi_assessment.ps1. All arguments are forwarded.

.EXAMPLE
  .\fetch-assessment-from-pi.ps1

.EXAMPLE
  .\fetch-assessment-from-pi.ps1 -Ai -OutFile assessment.md

.EXAMPLE
  .\fetch-assessment-from-pi.ps1 -FromDate 2026-04-17 -ToDate 2026-04-20 -FollowUp -BaselineLocal .\last.md

.EXAMPLE
  .\fetch-assessment-from-pi.ps1 -SkipDataCsv

.EXAMPLE
  .\fetch-assessment-from-pi.ps1 -DataCsvOnly
#>
$ErrorActionPreference = "Stop"
$inner = Join-Path $PSScriptRoot "scripts\pi\fetch_pi_assessment.ps1"
if (-not (Test-Path $inner)) {
  throw "Expected: $inner - run from signalhub repo root."
}
Write-Host "Running: $inner $args"
& $inner @args
exit $LASTEXITCODE
