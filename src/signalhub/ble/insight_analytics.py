"""Deterministic capture-health and per-address window metrics for insights reports."""

from __future__ import annotations

import json
import sqlite3
from typing import Any

def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (name,),
    ).fetchone()
    return row is not None


def ensure_window_stats_table(conn: sqlite3.Connection) -> None:
    """Create materialized window stats table if missing (dashboard DB may never have run CLI init)."""
    if _table_exists(conn, "ble_device_window_stats"):
        return
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ble_device_window_stats (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          utc_from TEXT NOT NULL,
          utc_to TEXT NOT NULL,
          address TEXT NOT NULL,
          ledger_id TEXT,
          obs_rows INTEGER NOT NULL,
          distinct_sessions INTEGER NOT NULL,
          distinct_utc_hours INTEGER NOT NULL,
          first_ts REAL,
          last_ts REAL,
          span_seconds REAL,
          avg_inter_obs_seconds REAL,
          UNIQUE(utc_from, utc_to, address)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_ble_winstat_window ON ble_device_window_stats(utc_from, utc_to)",
    )
    conn.commit()


def capture_health_metrics(
    conn: sqlite3.Connection,
    start: float,
    end: float,
) -> dict[str, Any]:
    """Session/observation alignment and volume for a UTC epoch window."""
    overlap_sess = conn.execute(
        """
        SELECT COUNT(*) AS n FROM capture_sessions
        WHERE COALESCE(started_at, imported_at) <= ?
          AND COALESCE(ended_at, started_at, imported_at) >= ?
        """,
        (end, start),
    ).fetchone()
    n_overlap = int(overlap_sess["n"] or 0) if overlap_sess else 0

    obs_row = conn.execute(
        """
        SELECT COUNT(*) AS n, COUNT(DISTINCT address) AS addr, COUNT(DISTINCT session_id) AS sess
        FROM ble_observations
        WHERE timestamp IS NOT NULL AND timestamp >= ? AND timestamp <= ?
        """,
        (start, end),
    ).fetchone()
    n_obs = int(obs_row["n"] or 0) if obs_row else 0
    n_addr = int(obs_row["addr"] or 0) if obs_row else 0
    n_sess_with_obs = int(obs_row["sess"] or 0) if obs_row else 0

    per_sess = conn.execute(
        """
        SELECT s.session_id,
               (SELECT COUNT(*) FROM ble_observations o
                WHERE o.session_id = s.session_id
                  AND o.timestamp IS NOT NULL AND o.timestamp >= ? AND o.timestamp <= ?) AS obs_n,
               s.imported_at,
               s.started_at,
               s.ended_at
        FROM capture_sessions s
        WHERE COALESCE(s.started_at, s.imported_at) <= ?
          AND COALESCE(s.ended_at, s.started_at, s.imported_at) >= ?
        """,
        (start, end, end, start),
    ).fetchall()

    zero_obs = 0
    obs_counts: list[int] = []
    for r in per_sess:
        on = int(r["obs_n"] or 0)
        if on == 0:
            zero_obs += 1
        else:
            obs_counts.append(on)

    obs_counts.sort()
    median_obs = _median(obs_counts) if obs_counts else 0.0
    mean_obs = sum(obs_counts) / len(obs_counts) if obs_counts else 0.0

    health_flags: list[str] = []
    if n_overlap > 0 and n_obs > 0 and n_obs < n_overlap * 2:
        health_flags.append(
            "very_few_packets_per_overlap_session",
        )
    if n_overlap > 50 and zero_obs > n_overlap * 0.4:
        health_flags.append("many_overlap_sessions_with_zero_obs_in_window")
    if n_sess_with_obs > 0 and n_obs / n_sess_with_obs < 3:
        health_flags.append("low_mean_obs_per_active_session")

    return {
        "overlap_session_count": n_overlap,
        "observation_rows_in_window": n_obs,
        "distinct_addresses_in_window": n_addr,
        "distinct_sessions_with_obs": n_sess_with_obs,
        "overlap_sessions_with_zero_obs": zero_obs,
        "overlap_sessions_with_obs": len(obs_counts),
        "median_obs_per_session_with_obs": median_obs,
        "mean_obs_per_session_with_obs": mean_obs,
        "health_flags": health_flags,
    }


def _median(sorted_vals: list[int]) -> float:
    if not sorted_vals:
        return 0.0
    n = len(sorted_vals)
    m = n // 2
    if n % 2:
        return float(sorted_vals[m])
    return (sorted_vals[m - 1] + sorted_vals[m]) / 2.0


def refresh_ble_device_window_stats(
    conn: sqlite3.Connection,
    utc_from: str,
    utc_to: str,
    start: float,
    end: float,
) -> int:
    """Replace rows for (utc_from, utc_to). Returns rows inserted."""
    ensure_window_stats_table(conn)
    conn.execute(
        "DELETE FROM ble_device_window_stats WHERE utc_from = ? AND utc_to = ?",
        (utc_from, utc_to),
    )
    conn.execute(
        """
        INSERT INTO ble_device_window_stats(
          utc_from, utc_to, address, ledger_id, obs_rows, distinct_sessions,
          distinct_utc_hours, first_ts, last_ts, span_seconds, avg_inter_obs_seconds
        )
        SELECT
          ? AS utc_from,
          ? AS utc_to,
          o.address,
          (SELECT d.ledger_id FROM ble_devices d
           WHERE d.address = o.address
           ORDER BY (d.last_seen IS NULL) ASC, d.last_seen DESC
           LIMIT 1) AS ledger_id,
          COUNT(*) AS obs_rows,
          COUNT(DISTINCT o.session_id) AS distinct_sessions,
          COUNT(DISTINCT CAST(o.timestamp / 3600 AS INTEGER)) AS distinct_utc_hours,
          MIN(o.timestamp) AS first_ts,
          MAX(o.timestamp) AS last_ts,
          MAX(o.timestamp) - MIN(o.timestamp) AS span_seconds,
          CASE WHEN COUNT(*) > 1
            THEN (MAX(o.timestamp) - MIN(o.timestamp)) / (COUNT(*) - 1.0)
            ELSE NULL END AS avg_inter_obs_seconds
        FROM ble_observations o
        WHERE o.timestamp IS NOT NULL AND o.timestamp >= ? AND o.timestamp <= ?
          AND o.address IS NOT NULL AND TRIM(o.address) != ''
        GROUP BY o.address
        """,
        (utc_from, utc_to, start, end),
    )
    conn.commit()
    row = conn.execute(
        "SELECT COUNT(*) AS n FROM ble_device_window_stats WHERE utc_from = ? AND utc_to = ?",
        (utc_from, utc_to),
    ).fetchone()
    return int(row["n"] or 0) if row else 0


def top_window_stats_rows(
    conn: sqlite3.Connection,
    utc_from: str,
    utc_to: str,
    *,
    limit: int = 22,
) -> list[sqlite3.Row]:
    if not _table_exists(conn, "ble_device_window_stats"):
        return []
    return list(
        conn.execute(
            """
            SELECT * FROM ble_device_window_stats
            WHERE utc_from = ? AND utc_to = ?
            ORDER BY obs_rows DESC
            LIMIT ?
            """,
            (utc_from, utc_to, int(limit)),
        ).fetchall(),
    )


def compute_top_device_window_rows_inline(
    conn: sqlite3.Connection,
    start: float,
    end: float,
    *,
    limit: int = 22,
) -> list[sqlite3.Row]:
    """Same shape as window stats SELECT (no table), for read-only DBs."""
    return list(
        conn.execute(
            """
            SELECT
              o.address AS address,
              (SELECT d.ledger_id FROM ble_devices d
               WHERE d.address = o.address
               ORDER BY (d.last_seen IS NULL) ASC, d.last_seen DESC
               LIMIT 1) AS ledger_id,
              COUNT(*) AS obs_rows,
              COUNT(DISTINCT o.session_id) AS distinct_sessions,
              COUNT(DISTINCT CAST(o.timestamp / 3600 AS INTEGER)) AS distinct_utc_hours,
              MIN(o.timestamp) AS first_ts,
              MAX(o.timestamp) AS last_ts,
              MAX(o.timestamp) - MIN(o.timestamp) AS span_seconds,
              CASE WHEN COUNT(*) > 1
                THEN (MAX(o.timestamp) - MIN(o.timestamp)) / (COUNT(*) - 1.0)
                ELSE NULL END AS avg_inter_obs_seconds
            FROM ble_observations o
            WHERE o.timestamp IS NOT NULL AND o.timestamp >= ? AND o.timestamp <= ?
              AND o.address IS NOT NULL AND TRIM(o.address) != ''
            GROUP BY o.address
            ORDER BY obs_rows DESC
            LIMIT ?
            """,
            (start, end, int(limit)),
        ).fetchall(),
    )


def row_to_metric_dict(r: sqlite3.Row) -> dict[str, Any]:
    return {
        "address": r["address"],
        "ledger_id": r["ledger_id"],
        "obs_rows": int(r["obs_rows"] or 0),
        "distinct_sessions": int(r["distinct_sessions"] or 0),
        "distinct_utc_hours": int(r["distinct_utc_hours"] or 0),
        "span_seconds": float(r["span_seconds"] or 0) if r["span_seconds"] is not None else None,
        "avg_inter_obs_seconds": float(r["avg_inter_obs_seconds"])
        if r["avg_inter_obs_seconds"] is not None
        else None,
    }


def baseline_delta(current: dict[str, Any], baseline: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k in (
        "overlap_session_count",
        "observation_rows_in_window",
        "distinct_addresses_in_window",
        "distinct_sessions_with_obs",
        "overlap_sessions_with_zero_obs",
    ):
        if k in current and k in baseline:
            try:
                out[k] = int(current[k]) - int(baseline[k])
            except (TypeError, ValueError):
                pass
    return out


def insights_metrics_json_blob(
    *,
    capture_health: dict[str, Any],
    baseline_health: dict[str, Any] | None,
    delta: dict[str, Any] | None,
    window_rows_sample: list[dict[str, Any]],
    rf_inference: dict[str, Any] | None = None,
) -> str:
    payload: dict[str, Any] = {
        "capture_health": capture_health,
        "baseline_capture_health": baseline_health,
        "baseline_delta": delta,
        "top_devices_window_sample": window_rows_sample,
    }
    if rf_inference:
        payload["rf_inference"] = rf_inference
    return json.dumps(payload, indent=2)
