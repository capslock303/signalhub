# Signalhub — BLE ledger

Python tooling to import Nordic nRF52840 DK BLE captures (`.pcapng`) via **tshark**, normalize observations into SQLite, and produce CSV/Markdown reports.

## Requirements

- Python 3.10+
- [Wireshark](https://www.wireshark.org/) with **tshark** on `PATH`, or set `SIGNALHUB_TSHARK` to `tshark.exe` (Windows: often `C:\Program Files\Wireshark\tshark.exe`).

## Install (editable)

```bash
cd signalhub
pip install -e .
```

### Run everything from a parent folder (e.g. `nRF/signalhub` inside `nRF`)

If `signalhub` lives next to your **`data/`** and **`.env`**, stay in the **parent** directory and use:

| Goal | Command (PowerShell) |
|------|----------------------|
| One-time install | `pwsh -File .\signalhub\scripts\dev\setup-editable.ps1` (from parent) or `.\setup-local.ps1` from inside `signalhub` |
| Streamlit dashboard | `pwsh -File .\signalhub\scripts\dev\run-dashboard.ps1` or `.\signalhub\run-dashboard.ps1` |
| CLI | `pwsh -File .\signalhub\scripts\dev\signalhub-ble.ps1 -- init-db` (etc.) |

If your workspace root is the **parent** of `signalhub`, copy **`examples/workspace-root-windows/*.ps1`** from this repo into that root (or run them from that folder). They forward to **`scripts/dev/`** so **`./data`**, **`./.env`**, and the default DB path resolve next to your project, not inside `signalhub/`.

Default DB path is **`./data/db/signalhub.sqlite`** relative to the **current working directory**; the dev scripts set cwd to the parent of `signalhub` so data and `.env` stay at workspace root.

## Quick start

```bash
# Initialize database (default: ./data/db/signalhub.sqlite)
signalhub-ble init-db

# Register sensor once
signalhub-ble sensor add --id NRF52840DK-01 --model nRF52840-DK --type ble_sniffer

# Import a capture
signalhub-ble import --pcap path\to\capture.pcapng --sensor NRF52840DK-01

# Session id is printed; then:
signalhub-ble summarize --session <SESSION_ID>
signalhub-ble ledger rebuild
signalhub-ble classify
signalhub-ble export csv --out data\exports\ledger.csv

# Ledger CSV for devices “active” in a UTC day range (default --by active)
signalhub-ble export csv --out data\exports\ledger_apr.csv --from 2026-04-01 --to 2026-04-17
# Same range but filter on last_seen or first_seen instead
signalhub-ble export csv --out data\exports\ledger_last.csv --from 2026-04-01 --to 2026-04-17 --by last_seen

# Sessions overlapping the range; raw observations by packet timestamp
signalhub-ble export sessions --out data\exports\sessions.csv --from 2026-04-01 --to 2026-04-17
signalhub-ble export observations --out data\exports\obs.csv --from 2026-04-01 --to 2026-04-17

signalhub-ble report session --session <SESSION_ID> --out data\exports\session_report.md
signalhub-ble report ledger --out data\exports\ledger_report.md
signalhub-ble report change --from 2026-04-01 --to 2026-04-17 --out data\exports\change_report.md
signalhub-ble report assess --from 2026-04-01 --to 2026-04-17 --out data\exports\assessment.md
# (includes “Named devices” tables: names seen in the date window + cumulative log from the DB)
```

Or: `python -m signalhub.ble --help`

## Environment

| Variable | Meaning |
|----------|---------|
| `SIGNALHUB_TSHARK` | Full path to `tshark` |
| `SIGNALHUB_DATA_DIR` | Root for `captures/`, `exports/`, `db/` (default: `./data`) |
| `SIGNALHUB_DB` | SQLite file path (default: `<data>/db/signalhub.sqlite`) |
| `SIGNALHUB_LOG_LEVEL` | `DEBUG`, `INFO`, `WARNING` |
| `SIGNALHUB_NRFUTIL` | (Pi install) Full path to official Nordic **nRF Util** `nrfutil` binary; default is `<SIGNALHUB_BLE_ROOT>/vendor/nrfutil` |

Field names depend on your Wireshark version. Override with `SIGNALHUB_TSHARK_FIELDS` (comma-separated) if defaults fail; see `signalhub.ble.tshark.DEFAULT_FIELDS`.

## Raspberry Pi 5 (headless hub)

Deploy the `signalhub/` tree under `/home/kpi/ble/signalhub`, data under `/home/kpi/ble/data`, then:

```bash
sudo bash /home/kpi/ble/signalhub/scripts/pi/install_pi.sh
```

`install_pi.sh` links `/usr/local/bin/signalhub-ble` to the project venv so you can run commands from any directory (for example `~/ble`). If you still see `command not found`, re-run the install script once to pick up that step, or call the venv directly:

```bash
/home/kpi/ble/signalhub/.venv/bin/signalhub-ble report assess --from 2026-04-01 --to 2026-04-17 --out data/exports/assessment.md
```

From `~/ble`, the default database is `./data/db/signalhub.sqlite` (same layout as services). Optionally match systemd exactly: `export SIGNALHUB_DATA_DIR=/home/kpi/ble/data`.

### Deploy or refresh `signalhub` sources on the Pi

The CLI is an **editable** install: it runs whatever Python files live under `/home/kpi/ble/signalhub/src/`.

**`pip install -e .` does not download updates.** It only re-links the venv to whatever is **already on disk** on the Pi. If `/home/kpi/ble/signalhub` has no `git` remote (tarball-only Pi), you must **copy a fresh tree from your PC** (recommended: `pwsh -File .\scripts\pi\deploy_from_windows.ps1`), **then** run `pip install -e .` so metadata like `pyproject.toml` / version stays in sync.

1. Deploy or copy the current `signalhub/` project onto the Pi, overwriting `src/signalhub/` (and `pyproject.toml`, `scripts/`, etc.).
2. `grep "Generated by signalhub" /home/kpi/ble/signalhub/src/signalhub/ble/reports.py` — should match once your PC’s tree is deployed.
3. `/home/kpi/ble/signalhub/.venv/bin/pip install -e /home/kpi/ble/signalhub` then `signalhub-ble --version` (expect **0.1.1** or whatever your `pyproject.toml` says).

If you see **`No such command 'assess'`** or reports look unchanged after “upgrading”, the files on the Pi were never replaced — fix the copy step first, not only `pip install`.

### Push this repo from your PC to the Pi (recommended)

This environment **cannot** log into your Pi without either an **SSH key** or you typing the password in an **interactive** terminal. One-time setup: put your **public** key on the Pi.

**Windows — why this is fiddly:** PowerShell often **drops** an empty `-N` argument to native `ssh-keygen`, which yields **`option requires an argument -- N`**. **Command Prompt** does not use PowerShell’s rules; a small **`.cmd`** file avoids both problems.

**Option A — from PowerShell (recommended):** run the helper batch file with `&` (adjust the path if your clone is elsewhere):

```powershell
& "G:\ai\local\nRF\signalhub\scripts\pi\ssh_keygen_ed25519.cmd"
```

**Option B — from Command Prompt (`cmd.exe`),** paste this **one line** exactly (do **not** prefix with `cmd /c`, and do **not** wrap the line in **single** quotes — those are for PowerShell only):

```bat
if not exist "%USERPROFILE%\.ssh" mkdir "%USERPROFILE%\.ssh" & ssh-keygen -t ed25519 -f "%USERPROFILE%\.ssh\id_ed25519" -q -N ""
```

If `id_ed25519` already exists, `ssh-keygen` will ask to overwrite — answer `y`/`n`, or delete `%USERPROFILE%\.ssh\id_ed25519` and `%USERPROFILE%\.ssh\id_ed25519.pub` first.

**Option C — from PowerShell without the helper (one line):** build the key path in PowerShell, then let **cmd** run `ssh-keygen` so `-N ""` is parsed correctly (do not pass `%USERPROFILE%` through nested quotes — it breaks OpenSSH’s `-f` path on some setups):

```powershell
if (-not (Test-Path "$env:USERPROFILE\.ssh")) { New-Item -ItemType Directory -Force -Path "$env:USERPROFILE\.ssh" | Out-Null }; cmd.exe /c ('ssh-keygen -t ed25519 -f "' + $env:USERPROFILE + '\.ssh\id_ed25519" -q -N ""')
```

Copy the public key to the Pi (PowerShell, one line):

```powershell
Get-Content "$env:USERPROFILE\.ssh\id_ed25519.pub" | ssh kpi@192.168.8.112 "mkdir -p .ssh && chmod 700 .ssh && cat >> .ssh/authorized_keys && chmod 600 .ssh/authorized_keys"
```

If `Get-Content` says the `.pub` file is missing, create the key first (Option A or B).

**Git Bash / Linux / macOS:**

```bash
ssh-keygen -t ed25519 -f "$HOME/.ssh/id_ed25519" -N ""
ssh-copy-id -i "$HOME/.ssh/id_ed25519.pub" kpi@192.168.8.112
```

Then deploy **from the repo root’s parent** (or any path) using the bundled script:

**Windows (PowerShell):** run from the `signalhub` directory (or pass a full path to the script).

```powershell
cd path\to\signalhub
pwsh -File .\scripts\pi\deploy_from_windows.ps1
# Optional: -PiHost 192.168.8.x -IdentityFile $env:USERPROFILE\.ssh\id_ed25519
```

**Autonomous deploy key (optional):** run `pwsh -File .\scripts\pi\New-SignalhubDeployKey.ps1` once. That creates **`.ssh.pi-deploy/id_ed25519`** (private; gitignored) and **`.ssh.pi-deploy/id_ed25519.pub`**. Append the `.pub` line to `kpi`’s `authorized_keys` on the Pi (see `.ssh.pi-deploy/README.md`). After that, `deploy_from_windows.ps1` picks up that key automatically and **`pwsh -File .\scripts\pi\deploy_from_windows.ps1 -NonInteractive`** (CI-style) runs work without typing a password. The project tarball **excludes** `.ssh.pi-deploy` so the private key is never uploaded to the Pi.

If remote setup fails with `$'\r': command not found` or `set: invalid option`, pull the latest deploy scripts: **`deploy_from_windows.ps1` now uploads `scripts/pi/remote_extract_and_deploy.sh`** (LF-normalized on your PC) and runs it under **`/bin/bash`**, so CRLF and login-shell (`zsh`) quirks no longer affect extract + `sed` cleanup.

**macOS / Linux / Git Bash:**

```bash
bash /path/to/signalhub/scripts/pi/deploy_from_dev.sh
# Optional: PI_HOST=192.168.8.x PI_USER=kpi bash ...
```

That uploads a tarball, extracts it to `/home/kpi/ble/signalhub`, runs `pip install -e .`, links **`/usr/local/bin/signalhub-ble`** (or falls back to **`/home/kpi/ble/bin/signalhub-ble`** if `sudo` cannot run without a TTY / passwordless sudo), and checks that `report assess` exists. The Windows script uses **`ssh -t`** so `sudo` can prompt for your Pi password when needed.

### nRF Util (Nordic) — BLE Sniffer + Wireshark

Nordic’s tool is **nRF Util**; the on-disk command is still `nrfutil`. It is a **standalone executable** with installable commands from Nordic’s registry — not the legacy **PyPI** package also named `nrfutil` (5.x), which does **not** replace this workflow.

1. From [nRF Util](https://www.nordicsemi.com/Products/Development-tools/nRF-Util), download a **Linux** build. On Raspberry Pi OS / Kali **aarch64**, prefer the **aarch64** build; if you only have the **x86-64** Linux binary, `install_pi.sh` enables **`amd64` multiarch** and installs `libc6:amd64` + `libudev1:amd64` so it runs via the usual **qemu binfmt** path.
2. Copy the binary to the Pi as `/home/kpi/ble/vendor/nrfutil`, `chmod +x`, **or** set `SIGNALHUB_NRFUTIL` to its full path before `install_pi.sh`.
3. `install_pi.sh` runs (as user `kpi`): `nrfutil install ble-sniffer completion device` and `nrfutil ble-sniffer bootstrap` so **tshark** gains the sniffer **extcap** interface.

**Note:** A file named `nrfutil` with no extension is often the **Linux** ELF; use `file nrfutil` locally — **`nrfutil.exe`** is the Windows build.

**Fallback:** if you only have Nordic’s **nRF Sniffer for Bluetooth LE** ZIP (manual extcap bundle), copy it to `/home/kpi/ble/vendor/` and run:

```bash
bash /home/kpi/ble/signalhub/scripts/pi/install_extcap_from_zip.sh /home/kpi/ble/vendor/sniffer.zip
```

Unplug/replug the sniffer (or restart `signalhub-ble.target`). Confirm with `tshark -D` (should list an **nRF** / **Sniffer** interface).

**USB plug-in autostart:** udev starts `signalhub-ble.target` when `1915:522a` appears; stopping when the USB device is removed. Ensure `/home/kpi/ble` is owned by your service user (`chown -R kpi:kpi /home/kpi/ble`).

**Optional:** `SIGNALHUB_FORCE_TSHARK_IFACE=3` forces interface index 3 for debugging (see `tshark -D`).

## Streamlit Community Cloud (dashboard)

The dashboard is `src/signalhub/review/app.py`. Deploy from a **GitHub** repo that contains this `signalhub` tree (for example [github.com/capslock303/signalhub](https://github.com/capslock303/signalhub) once you publish it).

1. **Connect GitHub in the Streamlit UI** — Community Cloud does *not* use a `GITHUB_TOKEN` from your `.env`; you authorize the app in the browser. Keep **tokens out of the repo** (`.env` is gitignored).
2. **App settings**
   - **Main file:** `streamlit_app.py` (repository root — calls `main()` explicitly; avoids a blank app when Cloud does not run `if __name__ == "__main__"` on a nested path).
   - **Python version:** in **Advanced settings** when you first deploy, choose **3.10–3.12** (matches `requires-python` in `pyproject.toml`). The Cloud UI controls this; a `runtime.txt` file is not used for version selection.
   - **Dependencies:** a single **`pyproject.toml`** only (Streamlit installs this package with `streamlit` and `click` as runtime deps). Avoid also committing a root `requirements.txt`, or the host may warn about multiple dependency files and pick the wrong one.
3. **Database:** Cloud instances have **no persistent path** to your Pi/PC SQLite file. Use the sidebar **Upload .sqlite** (e.g. a file from `signalhub-ble export review-db --out …`) each session, or attach [persistent storage](https://docs.streamlit.io/streamlit-community-cloud/manage-your-app#persistent-storage) if your plan supports it.
4. **Secrets (optional):** Streamlit Cloud → your app → **Settings → Secrets** — paste TOML (encrypted at rest). See committed **`secrets.example.toml`** in this repo for all keys the dashboard understands. Minimum for the **AI** tab:

   ```toml
   OPENAI_API_KEY = "sk-..."
   ```

   You can use `SIGNALHUB_OPENAI_API_KEY` instead of `OPENAI_API_KEY` if you prefer. Optional: `SIGNALHUB_OPENAI_BASE_URL`, `SIGNALHUB_AI_MODEL`. Changes can take **~1 minute** to reach the running app (reboot if needed).

### If deploy logs say “Updating the app files has failed” (git exit 1)

That comes from Streamlit’s runner failing to `git pull` your repo, not from Python deps. Try in order:

1. **GitHub → Settings → Applications** — ensure **Streamlit** still has access to `capslock303/signalhub` (re-authorize if needed).
2. In Streamlit **Manage app** → **Reboot** (or **Redeploy** from the latest `main`).
3. If it persists, **delete the Cloud app** and **deploy again** from the same repo/branch (clears a broken workspace). Your URL may change unless you reuse the app name.

### Publish this folder to GitHub (`capslock303/signalhub`)

From `signalhub/` after a commit:

```powershell
# One-time: create the empty repo on GitHub (needs a PAT with `repo` / create scope — do not commit it)
$env:GITHUB_TOKEN = 'ghp_your_pat_here'
python scripts\publish_to_github.py

git init
git add -A
git commit -m "Initial publish: signalhub + Streamlit dashboard"
git remote add origin https://github.com/capslock303/signalhub.git
git branch -M main
git push -u origin main
```

If the repo was already created on the web, skip `publish_to_github.py` and only add `remote` + `push`.
