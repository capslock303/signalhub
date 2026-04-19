"""Entropy, drift, lightweight graph metrics, and narrative hooks over BLE observations."""

from __future__ import annotations

import math
import sqlite3
from collections import Counter
from typing import Any


def _shannon_entropy(counts: list[int]) -> float:
    total = sum(counts)
    if total <= 0:
        return 0.0
    h = 0.0
    for c in counts:
        if c <= 0:
            continue
        p = c / total
        h -= p * math.log2(p)
    return h


def pdu_entropy_top_addresses(
    conn: sqlite3.Connection,
    start: float,
    end: float,
    *,
    limit: int = 12,
) -> list[dict[str, Any]]:
    """Shannon entropy of `pdu_type` mix per address (high → diverse / chatty link-layer mix)."""
    rows = conn.execute(
        """
        SELECT address, pdu_type, COUNT(*) AS n
        FROM ble_observations
        WHERE timestamp IS NOT NULL AND timestamp >= ? AND timestamp <= ?
          AND address IS NOT NULL AND TRIM(address) != ''
        GROUP BY address, pdu_type
        """,
        (start, end),
    ).fetchall()
    by_addr: dict[str, Counter[str]] = {}
    for r in rows:
        by_addr.setdefault(r["address"], Counter())[str(r["pdu_type"] or "")] += int(r["n"] or 0)
    scored: list[tuple[float, str, int]] = []
    for addr, ctr in by_addr.items():
        vals = list(ctr.values())
        if sum(vals) < 15:
            continue
        scored.append((_shannon_entropy(vals), addr, sum(vals)))
    scored.sort(reverse=True)
    return [
        {"address": a, "pdu_entropy_bits": round(h, 3), "packets": n}
        for h, a, n in scored[: int(limit)]
    ]


def interarrival_cv_top(
    conn: sqlite3.Connection,
    start: float,
    end: float,
    *,
    limit: int = 10,
) -> list[dict[str, Any]]:
    """Coefficient of variation of inter-arrival gaps (population std / mean)."""
    rows = conn.execute(
        f"""
        WITH o AS (
          SELECT address, timestamp,
                 LAG(timestamp) OVER (PARTITION BY address ORDER BY timestamp) AS prev_ts
          FROM ble_observations
          WHERE timestamp IS NOT NULL AND timestamp >= ? AND timestamp <= ?
            AND address IS NOT NULL AND TRIM(address) != ''
        ),
        gaps AS (
          SELECT address, (timestamp - prev_ts) AS gap
          FROM o
          WHERE prev_ts IS NOT NULL AND (timestamp - prev_ts) > 0 AND (timestamp - prev_ts) < 3600
        ),
        agg AS (
          SELECT address, AVG(gap) AS mg, COUNT(*) AS cnt,
                 (AVG(gap * gap) - AVG(gap) * AVG(gap)) AS var_pop
          FROM gaps
          GROUP BY address
          HAVING COUNT(*) >= 12 AND AVG(gap) > 1e-6
        )
        SELECT address, mg, cnt,
               CASE WHEN var_pop > 0 THEN SQRT(var_pop) / mg ELSE 0 END AS cv
        FROM agg
        ORDER BY cv DESC
        LIMIT {int(limit)}
        """,
        (start, end),
    ).fetchall()
    return [
        {
            "address": r["address"],
            "interarrival_cv": float(r["cv"] or 0),
            "gaps": int(r["cnt"] or 0),
        }
        for r in rows
    ]


def _co_presence_edge_endpoints(p: dict[str, Any]) -> tuple[str, str]:
    if "address_a" in p and "address_b" in p:
        return str(p["address_a"]), str(p["address_b"])
    if "a" in p and "b" in p:
        return str(p["a"]), str(p["b"])
    raise KeyError("pair needs address_a/address_b or a/b keys")


def co_presence_graph_metrics(
    pairs: list[dict[str, Any]],
) -> dict[str, Any]:
    """Degree stats from co-presence edge list (undirected)."""
    deg: Counter[str] = Counter()
    for p in pairs:
        ea, eb = _co_presence_edge_endpoints(p)
        deg[ea] += 1
        deg[eb] += 1
    if not deg:
        return {"edges": 0, "nodes": 0, "max_degree": 0, "top_hubs": []}
    top = deg.most_common(8)
    return {
        "edges": len(pairs),
        "nodes": len(deg),
        "max_degree": top[0][1] if top else 0,
        "top_hubs": [{"address": a, "degree": d} for a, d in top],
    }


def ledger_hint_drift_events(
    conn: sqlite3.Connection,
    *,
    limit: int = 15,
) -> list[dict[str, Any]]:
    """Ledger rows where name vs manufacturer vs service suggests drift (simple string inequality)."""
    rows = conn.execute(
        """
        SELECT ledger_id, address, current_name_hint, current_manufacturer_hint, current_service_hint
        FROM ble_devices
        WHERE current_name_hint IS NOT NULL AND LENGTH(TRIM(current_name_hint)) > 0
        ORDER BY (last_seen IS NULL) ASC, last_seen DESC
        LIMIT 200
        """,
    ).fetchall()
    out: list[dict[str, Any]] = []
    for r in rows:
        aliases = conn.execute(
            """
            SELECT alias_type, COUNT(DISTINCT alias_value) AS k
            FROM ble_aliases
            WHERE ledger_id = ? AND alias_type IN ('name','manufacturer','service_uuid16')
            GROUP BY alias_type
            HAVING k >= 2
            """,
            (r["ledger_id"],),
        ).fetchall()
        if not aliases:
            continue
        out.append(
            {
                "ledger_id": r["ledger_id"],
                "address": r["address"],
                "current_name": r["current_name_hint"],
                "alias_variants": [dict(a) for a in aliases],
            },
        )
        if len(out) >= limit:
            break
    return out


def privacy_exposure_score_window(
    conn: sqlite3.Connection,
    start: float,
    end: float,
) -> dict[str, Any]:
    """Coarse score: rows with human-correlatable hints (name / company / service) per 1000 packets."""
    row = conn.execute(
        """
        SELECT COUNT(*) AS n,
               SUM(CASE WHEN name_hint IS NOT NULL AND LENGTH(TRIM(name_hint))>0 THEN 1 ELSE 0 END) AS nn,
               SUM(CASE WHEN manufacturer_hint IS NOT NULL AND LENGTH(TRIM(manufacturer_hint))>0 THEN 1 ELSE 0 END) AS nm,
               SUM(CASE WHEN service_hint IS NOT NULL AND LENGTH(TRIM(service_hint))>0 THEN 1 ELSE 0 END) AS ns
        FROM ble_observations
        WHERE timestamp IS NOT NULL AND timestamp >= ? AND timestamp <= ?
        """,
        (start, end),
    ).fetchone()
    n = int(row["n"] or 0) if row else 0
    if n == 0:
        return {"packets": 0, "exposure_per_1000": 0.0}
    exposed = int(row["nn"] or 0) + int(row["nm"] or 0) + int(row["ns"] or 0)
    # crude: count rows that have at least one hint once each max
    row2 = conn.execute(
        """
        SELECT COUNT(*) AS k FROM ble_observations
        WHERE timestamp IS NOT NULL AND timestamp >= ? AND timestamp <= ?
          AND (
            (name_hint IS NOT NULL AND LENGTH(TRIM(name_hint))>0)
            OR (manufacturer_hint IS NOT NULL AND LENGTH(TRIM(manufacturer_hint))>0)
            OR (service_hint IS NOT NULL AND LENGTH(TRIM(service_hint))>0)
          )
        """,
        (start, end),
    ).fetchone()
    k = int(row2["k"] or 0) if row2 else 0
    return {
        "packets": n,
        "rows_with_any_identity_hint": k,
        "exposure_per_1000": round(1000.0 * k / n, 2),
    }


def composite_narrative_hints(
    *,
    capture_health_flags: list[str],
    rf_pairs_count: int,
    drift_count: int,
) -> list[str]:
    bullets: list[str] = []
    if capture_health_flags:
        bullets.append(f"Capture-health flags fired: {', '.join(capture_health_flags)}.")
    if rf_pairs_count > 0:
        bullets.append("Co-presence edges exist — consider graph view for hubs vs isolates.")
    if drift_count > 0:
        bullets.append("Ledger alias drift detected — verify firmware changes vs device swaps.")
    if not bullets:
        bullets.append("No composite red flags from this pass — still review top recurrence manually.")
    return bullets
