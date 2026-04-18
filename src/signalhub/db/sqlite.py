from __future__ import annotations

import sqlite3
import time
from pathlib import Path

from signalhub.db.schema import DDL, SCHEMA_VERSION


def connect(db_file: Path) -> sqlite3.Connection:
    db_file.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_file))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {str(r[1]) for r in rows}


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(DDL)
    cols = _table_columns(conn, "ble_devices")
    if "identity_kind" not in cols:
        conn.execute("ALTER TABLE ble_devices ADD COLUMN identity_kind TEXT DEFAULT 'mac'")
    conn.execute(
        "INSERT OR REPLACE INTO schema_meta(key, value) VALUES('version', ?)",
        (str(SCHEMA_VERSION),),
    )
    conn.commit()


def now_epoch() -> float:
    return time.time()
