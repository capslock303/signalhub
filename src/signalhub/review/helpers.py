from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any


def open_dashboard_connection(db_path: Path, *, readonly: bool = False) -> sqlite3.Connection:
    """Open SQLite for the dashboard. Default read-write; optional read-only URI mode."""
    db_path = db_path.expanduser().resolve()
    if readonly:
        uri = db_path.as_uri() + "?mode=ro"
        conn = sqlite3.connect(uri, uri=True, check_same_thread=False)
    else:
        conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (name,),
    ).fetchone()
    return row is not None


def db_capabilities(conn: sqlite3.Connection) -> dict[str, Any]:
    """Which logical features this file supports."""
    obs = table_exists(conn, "ble_observations")
    return {
        "ble_observations": obs,
        "session_stats": table_exists(conn, "session_stats"),
        "ble_devices": table_exists(conn, "ble_devices"),
        "capture_sessions": table_exists(conn, "capture_sessions"),
        "ble_device_session_summary": table_exists(conn, "ble_device_session_summary"),
        "ble_aliases": table_exists(conn, "ble_aliases"),
        "sensors": table_exists(conn, "sensors"),
        "schema_meta": table_exists(conn, "schema_meta"),
        "has_identity_kind": bool(
            conn.execute(
                "SELECT 1 FROM pragma_table_info('ble_devices') WHERE name='identity_kind' LIMIT 1",
            ).fetchone()
            if table_exists(conn, "ble_devices")
            else None
        ),
        "mode": "full" if obs else "review",
    }


def is_safe_select(sql: str) -> bool:
    """Very small guardrail for ad-hoc SQL (read-only intent)."""
    s = sql.strip()
    if not s:
        return False
    low = s.lower()
    if not low.startswith("select") and not low.startswith("with"):
        return False
    forbidden = (
        "attach",
        "detach",
        "pragma",
        "delete ",
        "insert ",
        "update ",
        "drop ",
        "create ",
        "alter ",
        "replace ",
        "truncate",
    )
    for tok in forbidden:
        if tok in low:
            return False
    return True


def rows_to_dicts(rows: list[sqlite3.Row]) -> list[dict[str, Any]]:
    return [dict(r) for r in rows]
