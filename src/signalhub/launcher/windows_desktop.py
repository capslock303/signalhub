"""Windows desktop entry: start Streamlit locally and open the browser.

Build (from repo root ``signalhub/``)::

  pip install -e ".[desktop]"
  pwsh -File scripts/build/build_desktop_exe.ps1

Output: workspace ``SignalhubDashboard/SignalhubDashboard.exe`` (folder next to ``signalhub/``; see build script).
"""

from __future__ import annotations

import os
import sys
import threading
import time
import webbrowser
from pathlib import Path


def _repo_root() -> Path:
    """Directory that contains ``streamlit_app.py`` (editable install / dev)."""
    here = Path(__file__).resolve()
    # src/signalhub/launcher/windows_desktop.py -> parents[3] = repo root
    return here.parents[3]


def _streamlit_app_path() -> Path:
    if getattr(sys, "frozen", False):
        base = Path(getattr(sys, "_MEIPASS", Path(sys.executable).parent))
        return base / "streamlit_app.py"
    return _repo_root() / "streamlit_app.py"


def _open_browser_later(url: str, delay_s: float = 2.0) -> None:
    def _run() -> None:
        time.sleep(delay_s)
        webbrowser.open(url)

    threading.Thread(target=_run, daemon=True).start()


def _env_file_search_bases() -> list[Path]:
    """Where to look for a local ``.env`` before Streamlit starts (EXE folder first when frozen)."""
    if getattr(sys, "frozen", False):
        exe_dir = Path(sys.executable).resolve().parent
        return [exe_dir, exe_dir.parent]
    repo = _repo_root()
    return [repo, repo.parent]


def _maybe_sync_pi_db_before_streamlit() -> None:
    """If ``SIGNALHUB_AUTO_SYNC_PI_DB`` is set, ``scp`` the Pi SQLite to a local cache and set ``SIGNALHUB_REVIEW_DB``."""
    from signalhub.config import apply_dotenv_path, load_dotenv_files
    from signalhub.review.pi_db_sync import (
        default_local_cache_path,
        default_remote_db_path,
        env_auto_sync_enabled,
        pull_pi_sqlite,
    )

    load_dotenv_files()
    for base in _env_file_search_bases():
        envp = base / ".env"
        if envp.is_file():
            apply_dotenv_path(envp, override=True)

    if not env_auto_sync_enabled():
        return

    host = os.environ.get("SIGNALHUB_PI_HOST", "").strip()
    user = os.environ.get("SIGNALHUB_PI_USER", "").strip()
    if not host or not user:
        sys.stderr.write(
            "SIGNALHUB_AUTO_SYNC_PI_DB is set but SIGNALHUB_PI_HOST / SIGNALHUB_PI_USER are missing; skipping DB pull.\n",
        )
        return

    ble = os.environ.get("SIGNALHUB_PI_BLE_ROOT", "/home/kpi/ble").strip()
    remote = os.environ.get("SIGNALHUB_PI_REMOTE_DB", "").strip() or default_remote_db_path(ble)
    ident_raw = os.environ.get("SIGNALHUB_PI_SSH_IDENTITY", "").strip()
    ident = ident_raw or None
    local_s = os.environ.get("SIGNALHUB_PI_LOCAL_DB_CACHE", "").strip()
    local = Path(local_s).expanduser() if local_s else default_local_cache_path()

    res = pull_pi_sqlite(
        host=host,
        user=user,
        remote_path=remote,
        local_path=local,
        identity_file=ident,
        non_interactive=True,
    )
    if res.ok and res.local_path is not None:
        os.environ["SIGNALHUB_REVIEW_DB"] = str(res.local_path.resolve())
        sys.stderr.write(f"Pi database synced → {res.local_path}\n")
    else:
        sys.stderr.write(f"Pi database sync skipped or failed: {res.message}\n")
        if res.stderr:
            sys.stderr.write(res.stderr + "\n")


def main() -> None:
    app = _streamlit_app_path()
    if not app.is_file():
        sys.stderr.write(f"Missing Streamlit entry: {app}\n")
        raise SystemExit(2)

    _maybe_sync_pi_db_before_streamlit()

    port = os.environ.get("SIGNALHUB_STREAMLIT_PORT", "8501").strip() or "8501"
    url = f"http://127.0.0.1:{port}"

    os.environ.setdefault("STREAMLIT_BROWSER_GATHER_USAGE_STATS", "false")

    # Frozen installs often look like a dev tree; Streamlit then forces developmentMode,
    # which rejects explicit server.port unless we disable it.
    os.environ.setdefault("STREAMLIT_GLOBAL_DEVELOPMENT_MODE", "false")

    sys.argv = [
        "streamlit",
        "run",
        str(app),
        f"--server.port={port}",
        "--server.headless=true",
        "--browser.gatherUsageStats=false",
        "--global.developmentMode=false",
    ]

    _open_browser_later(url)

    from streamlit.web import cli as stcli

    code = stcli.main()
    raise SystemExit(int(code) if isinstance(code, int) else 0)


if __name__ == "__main__":
    main()
