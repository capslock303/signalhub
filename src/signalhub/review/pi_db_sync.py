"""Copy the Pi edge SQLite database to this PC via ``scp`` (OpenSSH)."""

from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class PullResult:
    ok: bool
    local_path: Path | None
    message: str
    stderr: str = ""


def default_remote_db_path(ble_root: str) -> str:
    """Default Pi DB path matching ``SIGNALHUB_DATA_DIR`` layout (``data/db/signalhub.sqlite``)."""
    root = (ble_root or "/home/kpi/ble").strip().rstrip("/")
    return f"{root}/data/db/signalhub.sqlite"


def default_local_cache_path() -> Path:
    """Per-user cache file (Windows: ``%LOCALAPPDATA%\\Signalhub\\pi-signalhub.sqlite``)."""
    base = os.environ.get("LOCALAPPDATA") or os.environ.get("TMP") or tempfile.gettempdir()
    d = Path(base) / "Signalhub"
    d.mkdir(parents=True, exist_ok=True)
    return d / "pi-signalhub.sqlite"


def _find_scp() -> str | None:
    return shutil.which("scp")


def pull_pi_sqlite(
    *,
    host: str,
    user: str,
    remote_path: str,
    local_path: Path | None = None,
    identity_file: str | None = None,
    non_interactive: bool = False,
    timeout: int = 180,
) -> PullResult:
    """Download ``remote_path`` from ``user@host`` into ``local_path`` (atomic replace)."""
    scp = _find_scp()
    if not scp:
        return PullResult(False, None, "scp not found (install OpenSSH Client on Windows).")

    host = host.strip()
    user = user.strip()
    if not host or not user:
        return PullResult(False, None, "host and user are required.")

    dest = local_path or default_local_cache_path()
    dest = dest.expanduser()
    dest.parent.mkdir(parents=True, exist_ok=True)
    partial = dest.with_suffix(dest.suffix + ".partial")

    remote_uri = f"{user}@{host}:{remote_path}"
    args: list[str] = [scp]
    if identity_file and str(identity_file).strip():
        idp = Path(identity_file).expanduser()
        if idp.is_file():
            args.extend(["-i", str(idp.resolve())])
    args.extend(
        [
            "-o",
            "StrictHostKeyChecking=accept-new",
            "-o",
            "ConnectTimeout=15",
        ],
    )
    if non_interactive:
        args.extend(["-o", "BatchMode=yes"])
    args.extend([remote_uri, str(partial)])

    try:
        proc = subprocess.run(
            args,
            capture_output=True,
            text=True,
            timeout=timeout,
            encoding="utf-8",
            errors="replace",
        )
    except subprocess.TimeoutExpired:
        partial.unlink(missing_ok=True)
        return PullResult(False, None, f"scp timed out after {timeout}s", "")
    except OSError as e:
        partial.unlink(missing_ok=True)
        return PullResult(False, None, str(e), "")

    err = (proc.stderr or "").strip()
    if proc.returncode != 0:
        partial.unlink(missing_ok=True)
        hint = ""
        if non_interactive and "Permission denied" in err:
            hint = " (try SSH key: SIGNALHUB_PI_SSH_IDENTITY or .ssh.pi-deploy)"
        return PullResult(
            False,
            None,
            f"scp failed (exit {proc.returncode}){hint}",
            err,
        )

    try:
        if dest.is_file():
            dest.unlink()
        partial.replace(dest)
    except OSError as e:
        partial.unlink(missing_ok=True)
        return PullResult(False, None, f"could not replace local file: {e}", err)

    return PullResult(True, dest, f"Pulled {remote_uri} → {dest}", err)


def env_auto_sync_enabled() -> bool:
    v = os.environ.get("SIGNALHUB_AUTO_SYNC_PI_DB", "").strip().lower()
    return v in ("1", "true", "yes", "on")
