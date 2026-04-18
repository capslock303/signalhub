from __future__ import annotations

import sqlite3
import time
from pathlib import Path


def build_review_database(source_conn: sqlite3.Connection, dest_path: Path) -> None:
    """Copy the working DB, retain aggregates, drop raw ble_observations, VACUUM.

    Adds session_stats (per-session observation and distinct-address counts) so the
    review copy stays useful without the heavy observation table.
    """
    dest_path = dest_path.resolve()
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    if dest_path.exists():
        dest_path.unlink()

    dest = sqlite3.connect(str(dest_path))
    try:
        dest.row_factory = sqlite3.Row
        source_conn.backup(dest)
        dest.executescript(
            """
            CREATE TABLE IF NOT EXISTS session_stats (
              session_id TEXT PRIMARY KEY,
              observation_count INTEGER NOT NULL,
              distinct_addresses INTEGER NOT NULL
            );
            """
        )
        dest.execute("DELETE FROM session_stats")
        dest.execute(
            """
            INSERT INTO session_stats(session_id, observation_count, distinct_addresses)
            SELECT session_id, COUNT(*), COUNT(DISTINCT address)
            FROM ble_observations
            GROUP BY session_id
            """
        )
        dest.execute("DROP TABLE IF EXISTS ble_observations")
        dest.execute(
            """
            INSERT OR REPLACE INTO schema_meta(key, value)
            VALUES('review_strip', ?)
            """,
            (f"ble_observations removed; session_stats added at {time.time():.0f}",),
        )
        dest.commit()
        dest.execute("VACUUM")
        dest.commit()
    finally:
        dest.close()
