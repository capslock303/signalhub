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
    obs_cols = _table_columns(conn, "ble_observations")
    for col, typ in (
        ("appearance_hint", "TEXT"),
        ("tx_power_dbm", "REAL"),
        ("adv_flags_hex", "TEXT"),
        ("service_uuid128_hint", "TEXT"),
    ):
        if col not in obs_cols:
            conn.execute(f"ALTER TABLE ble_observations ADD COLUMN {col} {typ}")
    dss_cols = _table_columns(conn, "ble_device_session_summary")
    if "adv_profile_json" not in dss_cols:
        conn.execute("ALTER TABLE ble_device_session_summary ADD COLUMN adv_profile_json TEXT")
    if "fingerprint_profile" not in cols:
        conn.execute(
            "ALTER TABLE ble_devices ADD COLUMN fingerprint_profile TEXT NOT NULL DEFAULT 'v1'",
        )
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS sensor_positions (
          sensor_id TEXT PRIMARY KEY REFERENCES sensors(sensor_id),
          x_m REAL NOT NULL,
          y_m REAL NOT NULL,
          z_m REAL,
          site_label TEXT,
          updated_at REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS ble_session_crypto (
          session_id TEXT PRIMARY KEY REFERENCES capture_sessions(session_id),
          secrets_file_path TEXT,
          encrypted_packets_observed INTEGER NOT NULL DEFAULT 0,
          decrypt_attempted INTEGER NOT NULL DEFAULT 0,
          decrypt_last_message TEXT
        );
        """,
    )
    conn.execute(
        "INSERT OR REPLACE INTO schema_meta(key, value) VALUES('version', ?)",
        (str(SCHEMA_VERSION),),
    )
    conn.commit()


def now_epoch() -> float:
    return time.time()
