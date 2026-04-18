<#
.SYNOPSIS
  Pack signalhub, upload to the Pi, extract, pip install -e, symlink signalhub-ble.

.DESCRIPTION
  Requires OpenSSH (ssh/scp) on PATH. You will be prompted for the SSH password unless keys are set up.

.PARAMETER PiHost
  Default: 192.168.8.112

.PARAMETER PiUser
  Default: kpi

.PARAMETER RemoteBleRoot
  Default: /home/kpi/ble  (signalhub lives at RemoteBleRoot/signalhub)

.PARAMETER NonInteractive
  If set, use SSH BatchMode (fails immediately when no key is configured — no password prompt).

.PARAMETER IdentityFile
  Private key path for ssh/scp. If empty and `<repo>/.ssh.pi-deploy/id_ed25519` exists, that key is used.
#>
param(
  [string]$PiHost = "192.168.8.112",
  [string]$PiUser = "kpi",
  [string]$RemoteBleRoot = "/home/kpi/ble",
  [string]$IdentityFile = "",
  [switch]$NonInteractive
)

$ErrorActionPreference = "Stop"
$SignalhubRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$AutonomyKey = Join-Path $SignalhubRoot ".ssh.pi-deploy\id_ed25519"
if ([string]::IsNullOrWhiteSpace($IdentityFile) -and (Test-Path $AutonomyKey)) {
  $IdentityFile = $AutonomyKey
  Write-Host "Using autonomous deploy key: $IdentityFile"
}
$RemoteTarget = "${PiUser}@${PiHost}"
$RemoteSignalhub = "$RemoteBleRoot/signalhub"
$Tgz = Join-Path $env:TEMP ("signalhub-deploy-{0:yyyyMMddHHmmss}.tgz" -f (Get-Date))

Write-Host "Packaging from: $SignalhubRoot"
Push-Location $SignalhubRoot
try {
  if (Test-Path $Tgz) { Remove-Item $Tgz -Force }
  tar -czf $Tgz `
    --exclude .git --exclude .venv --exclude __pycache__ --exclude .pytest_cache --exclude .ruff_cache --exclude .ssh.pi-deploy `
    --exclude "assessment-from-pi.md" --exclude "assessment-from-pi*.csv" `
    .
} finally {
  Pop-Location
}

$size = (Get-Item $Tgz).Length
Write-Host "Created archive ($size bytes): $Tgz"

$SshBase = @("-o", "StrictHostKeyChecking=accept-new", "-o", "ConnectTimeout=15")
if ($NonInteractive) {
  $SshBase += @("-o", "BatchMode=yes")
}
if ($IdentityFile) {
  $SshBase = @("-i", $IdentityFile) + $SshBase
}

try {
  Write-Host "Uploading to ${RemoteTarget}:/tmp/signalhub-deploy.tgz ..."
  & scp @SshBase $Tgz "${RemoteTarget}:/tmp/signalhub-deploy.tgz"
  if ($LASTEXITCODE -ne 0) {
    throw "scp failed (exit $LASTEXITCODE). Configure an SSH key or run without -NonInteractive and enter the Pi password when prompted."
  }

  # Upload a small LF-only bash script (normalize locally) so we never depend on a giant
  # ssh -c string (PowerShell/zsh mangling, CRLF tarballs, etc.).
  $RunnerSrc = Join-Path $PSScriptRoot "remote_extract_and_deploy.sh"
  if (-not (Test-Path $RunnerSrc)) {
    throw "Missing $RunnerSrc"
  }
  $RunnerTmp = Join-Path $env:TEMP ("signalhub-remote-extract-{0}.sh" -f (Get-Random))
  $runnerText = [System.IO.File]::ReadAllText($RunnerSrc)
  $runnerText = $runnerText -replace "`r`n", "`n" -replace "`r", "`n"
  $utf8NoBom = New-Object System.Text.UTF8Encoding $false
  [System.IO.File]::WriteAllText($RunnerTmp, $runnerText, $utf8NoBom)
  try {
    Write-Host "Uploading remote runner to ${RemoteTarget}:/tmp/signalhub-remote-extract.sh ..."
    & scp @SshBase $RunnerTmp "${RemoteTarget}:/tmp/signalhub-remote-extract.sh"
    if ($LASTEXITCODE -ne 0) {
      throw "scp runner failed (exit $LASTEXITCODE)."
    }
  }
  finally {
    Remove-Item $RunnerTmp -Force -ErrorAction SilentlyContinue
  }

  $RemoteOneLine = "/bin/bash /tmp/signalhub-remote-extract.sh $RemoteSignalhub"
  Write-Host "Running remote setup (venv, pip, symlink) via /bin/bash ..."
  # Allocate a TTY so `sudo` can prompt when passwordless sudo is not configured (omit with -NonInteractive).
  $sshArgs = @("ssh")
  if (-not $NonInteractive) {
    $sshArgs += "-t"
  }
  $sshArgs += $SshBase
  $sshArgs += $RemoteTarget
  $sshArgs += $RemoteOneLine
  & $sshArgs[0] @($sshArgs[1..($sshArgs.Length - 1)])
  if ($LASTEXITCODE -ne 0) {
    throw "ssh remote setup failed (exit $LASTEXITCODE)."
  }

  Write-Host ""
  Write-Host "--- Pi $(Join-Path $RemoteSignalhub 'DEPLOY_STAMP.txt') (sanity check) ---"
  $sshCat = @("ssh") + $SshBase + $RemoteTarget + "cat '${RemoteSignalhub}/DEPLOY_STAMP.txt'"
  & $sshCat[0] @($sshCat[1..($sshCat.Length - 1)])
  if ($LASTEXITCODE -ne 0) {
    Write-Host "WARN: could not read DEPLOY_STAMP.txt on Pi."
  }
  Write-Host "---"
  Write-Host "If you see reports.py=OLD or version = 0.1.0, the tarball did not come from this PC's current repo; fix sync then re-run this script."
}
finally {
  if (Test-Path $Tgz) {
    Remove-Item $Tgz -Force -ErrorAction SilentlyContinue
  }
}

Write-Host "Done."
