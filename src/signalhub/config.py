from __future__ import annotations

import os
from pathlib import Path


def _package_root_with_pyproject() -> Path | None:
    """Directory containing this package's pyproject.toml, if present (editable installs)."""
    here = Path(__file__).resolve().parent
    for d in (here, *here.parents):
        if (d / "pyproject.toml").is_file():
            return d
    return None


def _iter_dotenv_paths() -> list[Path]:
    """Candidate .env paths; earlier entries win per key (see load_dotenv_files)."""
    out: list[Path] = []
    env_file = os.environ.get("SIGNALHUB_ENV_FILE")
    if env_file:
        out.append(Path(env_file).expanduser())
    cwd = Path.cwd()
    out.append(cwd / ".env")
    for par in cwd.parents:
        out.append(par / ".env")
    root = _package_root_with_pyproject()
    if root is not None:
        out.append(root / ".env")
    # De-duplicate while preserving order
    seen: set[Path] = set()
    unique: list[Path] = []
    for p in out:
        try:
            rp = p.resolve()
        except OSError:
            continue
        if rp in seen:
            continue
        seen.add(rp)
        unique.append(rp)
    return unique


def _apply_env_file(path: Path) -> None:
    raw = path.read_text(encoding="utf-8", errors="replace")
    if raw.startswith("\ufeff"):
        raw = raw[1:]
    for line in raw.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].strip()
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        if not key or key.startswith("#"):
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
            value = value[1:-1]
        if key not in os.environ:
            os.environ[key] = value


def load_dotenv_files() -> list[Path]:
    """Load variables from .env files into os.environ (does not override existing env).

    Search order: SIGNALHUB_ENV_FILE, ./.env, parents of cwd, then package tree root .env
    (so e.g. ~/ble/signalhub/.env is picked up when cwd is ~/ble).
    """
    loaded: list[Path] = []
    for path in _iter_dotenv_paths():
        if not path.is_file():
            continue
        _apply_env_file(path)
        loaded.append(path)
    return loaded


def _default_data_dir() -> Path:
    env = os.environ.get("SIGNALHUB_DATA_DIR")
    if env:
        return Path(env).expanduser().resolve()
    return (Path.cwd() / "data").resolve()


def data_dir() -> Path:
    return _default_data_dir()


def captures_dir() -> Path:
    d = data_dir() / "captures"
    d.mkdir(parents=True, exist_ok=True)
    return d


def exports_dir() -> Path:
    d = data_dir() / "exports"
    d.mkdir(parents=True, exist_ok=True)
    return d


def db_path() -> Path:
    env = os.environ.get("SIGNALHUB_DB")
    if env:
        p = Path(env).expanduser().resolve()
        p.parent.mkdir(parents=True, exist_ok=True)
        return p
    d = data_dir() / "db"
    d.mkdir(parents=True, exist_ok=True)
    return (d / "signalhub.sqlite").resolve()


def tshark_path() -> str:
    env = os.environ.get("SIGNALHUB_TSHARK")
    if env:
        return env
    return "tshark"


def tshark_field_list() -> list[str]:
    raw = os.environ.get("SIGNALHUB_TSHARK_FIELDS")
    if raw:
        return [f.strip() for f in raw.split(",") if f.strip()]
    return []


def log_level() -> str:
    return os.environ.get("SIGNALHUB_LOG_LEVEL", "INFO").upper()
