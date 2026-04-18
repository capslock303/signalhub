# Pi deploy SSH key (local only)

This directory holds an **Ed25519** key pair used only for `scripts/pi/deploy_from_windows.ps1` (and similar) so `scp` / `ssh` can run **without a password** once the **public** half is on the Pi.

## One-time on the Pi

Append `id_ed25519.pub` to the target user’s `authorized_keys` (example user `kpi`):

```bash
# From your PC (after the .pub exists), e.g. PowerShell:
Get-Content .ssh.pi-deploy\id_ed25519.pub | ssh kpi@192.168.8.112 "mkdir -p .ssh && chmod 700 .ssh && cat >> .ssh/authorized_keys && chmod 600 .ssh/authorized_keys"
```

Or copy the `.pub` file to the Pi and append it manually.

## Files

| File           | In git? | Notes                                      |
|----------------|---------|--------------------------------------------|
| `id_ed25519`   | **No**  | Private key — listed in `../.gitignore`. |
| `id_ed25519.pub` | Yes (optional) | Safe to share; needed on the Pi only. |

Regenerate if the private key is ever leaked: delete both files, run `pwsh -File scripts/pi/New-SignalhubDeployKey.ps1`, then reinstall the new `.pub` on the Pi.
