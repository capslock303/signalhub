<#
.SYNOPSIS
  Create or replace .ssh.pi-deploy/id_ed25519 (+ .pub) under the signalhub repo root.
#>
$ErrorActionPreference = "Stop"
$SignalhubRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$KeyDir = Join-Path $SignalhubRoot ".ssh.pi-deploy"
$KeyPath = Join-Path $KeyDir "id_ed25519"

New-Item -ItemType Directory -Force -Path $KeyDir | Out-Null
Remove-Item $KeyPath, "$KeyPath.pub" -Force -ErrorAction SilentlyContinue

# Start-Process drops an empty -N argument on some hosts; let cmd.exe pass -N "".
$cmdLine = 'ssh-keygen -t ed25519 -f "' + $KeyPath + '" -q -N "" -C signalhub-pi-deploy'
$p = Start-Process -FilePath "cmd.exe" -ArgumentList @("/c", $cmdLine) -Wait -NoNewWindow -PassThru

if ($p.ExitCode -ne 0) {
  throw "ssh-keygen failed (exit $($p.ExitCode))"
}

Write-Host "Created:"
Write-Host "  $KeyPath"
Write-Host "  $KeyPath.pub"
Write-Host "Install the public key on the Pi once; see .ssh.pi-deploy/README.md"
