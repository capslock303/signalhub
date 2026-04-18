<#
.SYNOPSIS
  From Windows: SSH to the Pi, run `signalhub-ble report assess`, then copy the Markdown to this repo root.

.DESCRIPTION
  Uses the same host/key defaults as deploy_from_windows.ps1. Requires OpenSSH (ssh/scp).
  After the Markdown report (unless -DataCsvOnly), runs export sessions, observations, and ble_devices
  for the same UTC window and downloads them next to the .md as *-data-*.csv (unless -SkipDataCsv).

.PARAMETER FromDate
  Inclusive UTC day (yyyy-MM-dd). Default: 2026-04-17 (change below or pass -FromDate).

.PARAMETER ToDate
  Inclusive UTC day (yyyy-MM-dd). Leave empty to use this PC's current UTC calendar date as --to
  (always passed to the Pi so older signalhub installs that require --to still work).

.PARAMETER OutFile
  Local path for the downloaded Markdown. Default: <repo>/assessment-from-pi.md

.PARAMETER PiHost
  Default: 192.168.8.112

.PARAMETER PiUser
  Default: kpi

.PARAMETER RemoteBleRoot
  Default: /home/kpi/ble

.PARAMETER NonInteractive
  SSH BatchMode=yes (no password prompt).

.PARAMETER IdentityFile
  If empty and .ssh.pi-deploy/id_ed25519 exists, that key is used.

.PARAMETER FollowUp
  Pass --follow-up on the Pi (DB snapshot + 24h activity + optional baseline diff).

.PARAMETER Ai
  Pass --ai on the Pi (needs OPENAI_API_KEY in Pi environment or ~/ble/signalhub/.env). Implies -FollowUp.

.PARAMETER AiModel
  Only used with -Ai. Default: gpt-4o-mini

.PARAMETER BaselineLocal
  With -FollowUp (or -Ai), uploads this file to the Pi as /tmp/signalhub-assess-baseline.md and passes --baseline.

.PARAMETER SkipDataCsv
  If set, do not run export sessions/observations/csv on the Pi or download companion CSVs.

.PARAMETER DataCsvOnly
  If set, skip the report assess step and only download CSV exports (same date window).
#>
param(
  [string]$FromDate = "",
  [string]$ToDate = "",
  [string]$OutFile = "",
  [string]$PiHost = "192.168.8.112",
  [string]$PiUser = "kpi",
  [string]$RemoteBleRoot = "/home/kpi/ble",
  [string]$IdentityFile = "",
  [switch]$NonInteractive,
  [switch]$FollowUp,
  [switch]$Ai,
  [string]$AiModel = "gpt-4o-mini",
  [string]$BaselineLocal = "",
  [switch]$SkipDataCsv,
  [switch]$DataCsvOnly
)

$ErrorActionPreference = "Stop"
$SignalhubRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path

# Default first day of deployment (override with -FromDate).
$defaultFromGoLive = "2026-04-17"
if ([string]::IsNullOrWhiteSpace($FromDate)) {
  $FromDate = $defaultFromGoLive
}
if ($FromDate -notmatch '^\d{4}-\d{2}-\d{2}$') {
  throw "FromDate must be yyyy-MM-dd (got '$FromDate')."
}

if ([string]::IsNullOrWhiteSpace($ToDate)) {
  $resolvedTo = ([datetime]::UtcNow).ToString("yyyy-MM-dd")
}
else {
  if ($ToDate -notmatch '^\d{4}-\d{2}-\d{2}$') {
    throw "ToDate must be yyyy-MM-dd or empty (got '$ToDate')."
  }
  $resolvedTo = $ToDate
}

$doFollowUp = $FollowUp -or $Ai

$AutonomyKey = Join-Path $SignalhubRoot ".ssh.pi-deploy\id_ed25519"
if ([string]::IsNullOrWhiteSpace($IdentityFile) -and (Test-Path $AutonomyKey)) {
  $IdentityFile = $AutonomyKey
  Write-Host "Using autonomous deploy key: $IdentityFile"
}

if ([string]::IsNullOrWhiteSpace($OutFile)) {
  $OutFile = Join-Path $SignalhubRoot "assessment-from-pi.md"
}
elseif (-not [System.IO.Path]::IsPathRooted($OutFile)) {
  $OutFile = Join-Path $SignalhubRoot $OutFile
}

$parent = Split-Path -Parent $OutFile
if (-not (Test-Path $parent)) {
  New-Item -ItemType Directory -Path $parent -Force | Out-Null
}

$stem = [System.IO.Path]::GetFileNameWithoutExtension($OutFile)

function Invoke-PiCsvScp {
  param(
    [string[]]$SshArgs,
    [string]$RemoteTarget,
    [string]$RemoteAbsolutePath,
    [string]$LocalFile
  )
  $destDir = Split-Path -Parent $LocalFile
  if (-not (Test-Path -LiteralPath $destDir)) {
    New-Item -ItemType Directory -Path $destDir -Force | Out-Null
  }
  $tmp = Join-Path $env:TEMP ("signalhub-scp-{0}.part" -f [guid]::NewGuid().ToString("N"))
  try {
    $uri = "${RemoteTarget}:${RemoteAbsolutePath}"
    Write-Host "  (via temp) $uri -> $LocalFile"
    & scp @SshArgs $uri $tmp
    if ($LASTEXITCODE -ne 0) {
      throw "scp failed (exit $LASTEXITCODE) for $uri"
    }
    if (Test-Path -LiteralPath $LocalFile) {
      Remove-Item -LiteralPath $LocalFile -Force -ErrorAction SilentlyContinue
    }
    Move-Item -LiteralPath $tmp -Destination $LocalFile -Force
  }
  finally {
    if (Test-Path -LiteralPath $tmp) {
      Remove-Item -LiteralPath $tmp -Force -ErrorAction SilentlyContinue
    }
  }
}

$RemoteTarget = "${PiUser}@${PiHost}"
$RemoteReport = "${RemoteBleRoot}/data/exports/assessment.md"
$dataDir = "${RemoteBleRoot}/data"

$SshBase = @("-o", "StrictHostKeyChecking=accept-new", "-o", "ConnectTimeout=15")
if ($NonInteractive) {
  $SshBase += @("-o", "BatchMode=yes")
}
if ($IdentityFile) {
  $SshBase = @("-i", $IdentityFile) + $SshBase
}

$remoteBaseline = "/tmp/signalhub-assess-baseline.md"
if ($doFollowUp -and -not [string]::IsNullOrWhiteSpace($BaselineLocal)) {
  if (-not (Test-Path $BaselineLocal)) {
    throw "BaselineLocal not found: $BaselineLocal"
  }
  Write-Host "Uploading baseline -> ${RemoteTarget}:$remoteBaseline"
  & scp @SshBase $BaselineLocal "${RemoteTarget}:${remoteBaseline}"
  if ($LASTEXITCODE -ne 0) {
    throw "scp baseline failed (exit $LASTEXITCODE)."
  }
}

$extra = ""
if ($doFollowUp) {
  $extra = " --follow-up"
  if (-not [string]::IsNullOrWhiteSpace($BaselineLocal)) {
    $extra += " --baseline $remoteBaseline"
  }
}
if ($Ai) {
  $extra += " --ai"
  if (-not [string]::IsNullOrWhiteSpace($AiModel)) {
    $extra += " --ai-model $AiModel"
  }
}

if (-not $DataCsvOnly) {
  $bashCmd = "export SIGNALHUB_DATA_DIR='$dataDir' && cd '$RemoteBleRoot' && signalhub-ble report assess --from $FromDate --to $resolvedTo --out data/exports/assessment.md$extra"
  Write-Host "Local save: $OutFile"
  Write-Host "Report window end (--to): $resolvedTo UTC (from -ToDate or this PC's UTC today)"
  Write-Host "Remote: $bashCmd"
  $sshArgs = @("ssh") + $SshBase + $RemoteTarget + $bashCmd
  & $sshArgs[0] @($sshArgs[1..($sshArgs.Length - 1)])
  if ($LASTEXITCODE -ne 0) {
    throw "ssh remote report failed (exit $LASTEXITCODE)."
  }

  Write-Host "Downloading $RemoteTarget`:$RemoteReport -> $OutFile"
  Invoke-PiCsvScp -SshArgs $SshBase -RemoteTarget $RemoteTarget -RemoteAbsolutePath $RemoteReport -LocalFile $OutFile
  Write-Host "Done: $OutFile"
}

if (-not $SkipDataCsv) {
  $rs = "data/exports/${stem}-data-sessions.csv"
  $ro = "data/exports/${stem}-data-observations.csv"
  $rl = "data/exports/${stem}-data-ble_devices.csv"
  $exportCmd = "export SIGNALHUB_DATA_DIR='$dataDir' && cd '$RemoteBleRoot' && " +
    "signalhub-ble export sessions --from $FromDate --to $resolvedTo --out $rs && " +
    "signalhub-ble export observations --from $FromDate --to $resolvedTo --out $ro && " +
    "signalhub-ble export csv --from $FromDate --to $resolvedTo --by active --out $rl"
  Write-Host "Remote exports (raw CSV, same UTC window): $exportCmd"
  $sshEx = @("ssh") + $SshBase + $RemoteTarget + $exportCmd
  & $sshEx[0] @($sshEx[1..($sshEx.Length - 1)])
  if ($LASTEXITCODE -ne 0) {
    throw "ssh remote export failed (exit $LASTEXITCODE)."
  }

  $tablesCmd = "export SIGNALHUB_DATA_DIR='$dataDir' && cd '$RemoteBleRoot' && " +
    "signalhub-ble export assessment-tables --from $FromDate --to $resolvedTo --out-dir data/exports --stem $stem"
  Write-Host "Remote export (assessment table CSVs): $tablesCmd"
  $sshTbl = @("ssh") + $SshBase + $RemoteTarget + $tablesCmd
  & $sshTbl[0] @($sshTbl[1..($sshTbl.Length - 1)])
  $tablesOk = ($LASTEXITCODE -eq 0)
  if (-not $tablesOk) {
    Write-Warning "export assessment-tables is missing or failed on the Pi (exit $LASTEXITCODE). Skipping table CSV downloads. Redeploy signalhub from this repo, then re-run."
  }
  $localS = Join-Path $parent "${stem}-data-sessions.csv"
  $localO = Join-Path $parent "${stem}-data-observations.csv"
  $localL = Join-Path $parent "${stem}-data-ble_devices.csv"
  Write-Host "Downloading CSV: $rs"
  Invoke-PiCsvScp -SshArgs $SshBase -RemoteTarget $RemoteTarget -RemoteAbsolutePath "${RemoteBleRoot}/$rs" -LocalFile $localS
  Write-Host "Downloading CSV: $ro"
  Invoke-PiCsvScp -SshArgs $SshBase -RemoteTarget $RemoteTarget -RemoteAbsolutePath "${RemoteBleRoot}/$ro" -LocalFile $localO
  Write-Host "Downloading CSV: $rl"
  Invoke-PiCsvScp -SshArgs $SshBase -RemoteTarget $RemoteTarget -RemoteAbsolutePath "${RemoteBleRoot}/$rl" -LocalFile $localL
  Write-Host "Done CSV: $localS"
  Write-Host "Done CSV: $localO"
  Write-Host "Done CSV: $localL"

  if ($tablesOk) {
    $tableSuffixes = @(
      "table-summary",
      "table-pdu_types",
      "table-address_counts",
      "table-named_devices_window",
      "table-named_devices_cumulative",
      "table-ledger_device_class"
    )
    foreach ($suf in $tableSuffixes) {
      $rTbl = "data/exports/${stem}-${suf}.csv"
      $localTbl = Join-Path $parent "${stem}-${suf}.csv"
      Write-Host "Downloading CSV: $rTbl"
      Invoke-PiCsvScp -SshArgs $SshBase -RemoteTarget $RemoteTarget -RemoteAbsolutePath "${RemoteBleRoot}/$rTbl" -LocalFile $localTbl
      Write-Host "Done CSV: $localTbl"
    }
  }
}

if ($DataCsvOnly) {
  Write-Host "DataCsvOnly: skipped Markdown report."
}
