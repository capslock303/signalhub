@echo off
REM Empty passphrase: use this from cmd.exe (double-click or full path). Do not wrap in PowerShell single quotes.
if not exist "%USERPROFILE%\.ssh" mkdir "%USERPROFILE%\.ssh"
ssh-keygen -t ed25519 -f "%USERPROFILE%\.ssh\id_ed25519" -q -N ""
if errorlevel 1 exit /b 1
echo Created: "%USERPROFILE%\.ssh\id_ed25519" and .pub
