"""Multi-sniffer: clock-skew hints, RSSI consistency, coarse 2D position from sensor_positions."""

from __future__ import annotations

import math
import sqlite3
from typing import Any


def _pragma_cols(conn: sqlite3.Connection, table: str) -> set[str]:
    return {str(r[1]) for r in conn.execute(f"PRAGMA table_info({table})")}


def has_sensor_positions(conn: sqlite3.Connection) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='sensor_positions' LIMIT 1",
    ).fetchone()
    return row is not None


def clock_skew_public_mac_pairs(
    conn: sqlite3.Connection,
    start: float,
    end: float,
    *,
    limit_pairs: int = 12,
) -> list[dict[str, Any]]:
    """Median timestamp delta for the same *public* MAC heard on two sensors (weak clock-offset hint)."""
    rows = conn.execute(
        f"""
        WITH pub AS (
          SELECT DISTINCT o.address
          FROM ble_observations o
          WHERE o.timestamp IS NOT NULL AND o.timestamp >= ? AND o.timestamp <= ?
            AND o.address_type = 'public'
            AND o.address LIKE '%:%:%:%:%:%'
          LIMIT 80
        ),
        ranked AS (
          SELECT o.address, s.sensor_id, o.timestamp,
                 ROW_NUMBER() OVER (PARTITION BY o.address, s.sensor_id ORDER BY o.timestamp) AS rn
          FROM ble_observations o
          JOIN capture_sessions s ON s.session_id = o.session_id
          JOIN pub p ON p.address = o.address
          WHERE o.timestamp IS NOT NULL AND o.timestamp >= ? AND o.timestamp <= ?
        ),
        pairs AS (
          SELECT a.address, a.sensor_id AS s1, b.sensor_id AS s2,
                 (a.timestamp - b.timestamp) AS dt
          FROM ranked a
          JOIN ranked b ON a.address = b.address AND a.sensor_id < b.sensor_id AND a.rn = b.rn AND a.rn <= 6
        )
        SELECT address, s1, s2, AVG(dt) AS mean_dt, COUNT(*) AS n
        FROM pairs
        GROUP BY address, s1, s2
        HAVING COUNT(*) >= 3
        ORDER BY n DESC
        LIMIT {int(limit_pairs)}
        """,
        (start, end, start, end),
    ).fetchall()
    return [
        {
            "address": r["address"],
            "sensor_a": r["s1"],
            "sensor_b": r["s2"],
            "mean_timestamp_delta_sec": float(r["mean_dt"] or 0),
            "paired_rows": int(r["n"] or 0),
        }
        for r in rows
    ]


def spatial_rssi_consistency(
    conn: sqlite3.Connection,
    start: float,
    end: float,
    *,
    min_rows_per_sensor: int = 5,
    limit: int = 18,
) -> list[dict[str, Any]]:
    """Among addresses heard on ≥2 sensors: coefficient of variation of per-sensor mean RSSI (rank stability)."""
    rows = conn.execute(
        f"""
        WITH per AS (
          SELECT o.address, s.sensor_id, AVG(o.rssi) AS mr, COUNT(*) AS n
          FROM ble_observations o
          JOIN capture_sessions s ON s.session_id = o.session_id
          WHERE o.timestamp IS NOT NULL AND o.timestamp >= ? AND o.timestamp <= ?
            AND o.rssi IS NOT NULL
            AND o.address IS NOT NULL AND TRIM(o.address) != ''
          GROUP BY o.address, s.sensor_id
          HAVING COUNT(*) >= {int(min_rows_per_sensor)}
        ),
        agg AS (
          SELECT address, COUNT(*) AS k, AVG(mr) AS grand, GROUP_CONCAT(mr) AS mrs
          FROM per
          GROUP BY address
          HAVING COUNT(*) >= 2
        )
        SELECT address, k, grand, mrs FROM agg
        ORDER BY k DESC, address
        LIMIT {int(limit)}
        """,
        (start, end),
    ).fetchall()
    out: list[dict[str, Any]] = []
    for r in rows:
        parts = [float(x) for x in str(r["mrs"] or "").split(",") if x.strip()]
        if len(parts) < 2:
            continue
        meanv = sum(parts) / len(parts)
        var = sum((x - meanv) ** 2 for x in parts) / max(len(parts) - 1, 1)
        std = math.sqrt(var)
        cv = std / max(abs(meanv), 1e-6)
        out.append(
            {
                "address": r["address"],
                "sensors_heard": int(r["k"] or 0),
                "mean_rssi_across_sensors": meanv,
                "rssi_mean_cv": cv,
            },
        )
    return out


def rssi_weighted_centroid_estimates(
    conn: sqlite3.Connection,
    start: float,
    end: float,
    *,
    min_rows: int = 6,
    limit: int = 15,
) -> list[dict[str, Any]]:
    """2D centroid using inverse-distance RSSI weights if sensor_positions exist."""
    if not has_sensor_positions(conn):
        return []
    rows = conn.execute(
        f"""
        SELECT o.address, s.sensor_id, AVG(o.rssi) AS mr, COUNT(*) AS n
        FROM ble_observations o
        JOIN capture_sessions s ON s.session_id = o.session_id
        WHERE o.timestamp IS NOT NULL AND o.timestamp >= ? AND o.timestamp <= ?
          AND o.rssi IS NOT NULL
        GROUP BY o.address, s.sensor_id
        HAVING COUNT(*) >= {int(min_rows)}
        """,
        (start, end),
    ).fetchall()
    by_addr: dict[str, list[tuple[str, float, int]]] = {}
    for r in rows:
        by_addr.setdefault(r["address"], []).append(
            (str(r["sensor_id"]), float(r["mr"]), int(r["n"] or 0)),
        )
    pos_rows = list(conn.execute("SELECT sensor_id, x_m, y_m, z_m FROM sensor_positions").fetchall())
    pos = {str(p["sensor_id"]): (float(p["x_m"]), float(p["y_m"]), float(p["z_m"] or 0)) for p in pos_rows}
    out: list[dict[str, Any]] = []
    for addr, lst in by_addr.items():
        pts: list[tuple[float, float, float]] = []
        wts: list[float] = []
        for sid, mr, _n in lst:
            if sid not in pos:
                continue
            # weight: stronger RSSI (less negative) → larger weight
            w = 10 ** (mr / 10.0)
            pts.append((pos[sid][0], pos[sid][1], pos[sid][2]))
            wts.append(w)
        if len(pts) < 2:
            continue
        sw = sum(wts)
        x = sum(px * ww for (px, _, _), ww in zip(pts, wts)) / sw
        y = sum(py * ww for (_, py, _), ww in zip(pts, wts)) / sw
        out.append({"address": addr, "x_m_est": x, "y_m_est": y, "sensors_used": len(pts)})
        if len(out) >= limit:
            break
    return out
