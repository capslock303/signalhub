"""Higher-order BLE RF inferences for insights (co-presence, rotation, OUI families, sensors, RSSI shape)."""

from __future__ import annotations

import sqlite3
from typing import Any

from signalhub.ble import advanced_analytics, insight_analytics, multi_sensor
from signalhub.common.manuf_lookup import vendor_for_mac
from signalhub.common.textutil import sanitize_ble_display_string


def _pragma_cols(conn: sqlite3.Connection, table: str) -> set[str]:
    return {str(r[1]) for r in conn.execute(f"PRAGMA table_info({table})")}


def has_adv_extension_columns(conn: sqlite3.Connection) -> bool:
    return "appearance_hint" in _pragma_cols(conn, "ble_observations")


def co_presence_top_pairs(
    conn: sqlite3.Connection,
    start: float,
    end: float,
    *,
    bin_seconds: int = 120,
    top_addresses: int = 36,
    max_pairs: int = 18,
) -> list[dict[str, Any]]:
    """Addresses that often share coarse time bins (weak association, not pairing proof)."""
    rows = conn.execute(
        f"""
        WITH vol AS (
          SELECT address, COUNT(*) AS n
          FROM ble_observations
          WHERE timestamp IS NOT NULL AND timestamp >= ? AND timestamp <= ?
            AND address IS NOT NULL AND TRIM(address) != ''
          GROUP BY address
          ORDER BY n DESC
          LIMIT {int(top_addresses)}
        ),
        bins AS (
          SELECT CAST(o.timestamp / ? AS INTEGER) AS bin, o.address
          FROM ble_observations o
          JOIN vol v ON v.address = o.address
          WHERE o.timestamp IS NOT NULL AND o.timestamp >= ? AND o.timestamp <= ?
        ),
        pairs AS (
          SELECT b1.bin, b1.address AS a, b2.address AS b
          FROM bins b1
          JOIN bins b2 ON b1.bin = b2.bin AND b1.address < b2.address
        )
        SELECT a, b, COUNT(*) AS shared_bins
        FROM pairs
        GROUP BY a, b
        ORDER BY shared_bins DESC
        LIMIT {int(max_pairs)}
        """,
        (start, end, float(bin_seconds), start, end),
    ).fetchall()
    return [
        {"address_a": r["a"], "address_b": r["b"], "shared_time_bins": int(r["shared_bins"] or 0)}
        for r in rows
    ]


def mac_rotation_clusters(
    conn: sqlite3.Connection,
    start: float,
    end: float,
    *,
    limit: int = 14,
) -> list[dict[str, Any]]:
    """Same advertised name (+ optional service hint) seen on multiple MACs → possible randomization."""
    rows = conn.execute(
        """
        SELECT
          LOWER(TRIM(name_hint)) AS nk,
          LOWER(TRIM(COALESCE(service_hint, ''))) AS sk,
          COUNT(DISTINCT address) AS n_mac,
          GROUP_CONCAT(address, ' | ') AS macs
        FROM ble_observations
        WHERE timestamp IS NOT NULL AND timestamp >= ? AND timestamp <= ?
          AND name_hint IS NOT NULL AND LENGTH(TRIM(name_hint)) >= 5
        GROUP BY nk, sk
        HAVING COUNT(DISTINCT address) >= 2
        ORDER BY n_mac DESC, MAX(timestamp) DESC
        LIMIT ?
        """,
        (start, end, int(limit)),
    ).fetchall()
    out: list[dict[str, Any]] = []
    for r in rows:
        macs = str(r["macs"] or "")
        if len(macs) > 400:
            macs = macs[:400] + "…"
        out.append(
            {
                "name_key": r["nk"],
                "service_hint_key": r["sk"] or "(none)",
                "distinct_macs": int(r["n_mac"] or 0),
                "sample_macs": macs,
            },
        )
    return out


def oui_vendor_families(
    conn: sqlite3.Connection,
    start: float,
    end: float,
    *,
    limit: int = 16,
) -> list[dict[str, Any]]:
    """OUI (first 3 octets) → vendor + count of distinct full MACs in window."""
    rows = conn.execute(
        """
        SELECT
          UPPER(SUBSTR(REPLACE(address, ':', ''), 1, 6)) AS oui_hex,
          COUNT(DISTINCT address) AS n_mac,
          COUNT(*) AS n_rows
        FROM ble_observations
        WHERE timestamp IS NOT NULL AND timestamp >= ? AND timestamp <= ?
          AND address LIKE '%:%:%:%:%:%'
        GROUP BY oui_hex
        ORDER BY n_rows DESC
        LIMIT ?
        """,
        (start, end, int(limit)),
    ).fetchall()
    out: list[dict[str, Any]] = []
    for r in rows:
        hx = r["oui_hex"]
        pseudo = ":".join(hx[i : i + 2] for i in range(0, 6, 2)) + ":00:00:00"
        v = vendor_for_mac(pseudo, use_http_fallback=False)
        out.append(
            {
                "oui_hex": hx,
                "distinct_macs": int(r["n_mac"] or 0),
                "observation_rows": int(r["n_rows"] or 0),
                "oui_vendor_hint": v,
            },
        )
    return out


def sensor_coverage_rows(
    conn: sqlite3.Connection,
    start: float,
    end: float,
) -> list[dict[str, Any]]:
    """Per physical sniffer (`sensor_id`): how many addresses and rows in window."""
    return [
        dict(r)
        for r in conn.execute(
            """
            SELECT
              s.sensor_id,
              COUNT(DISTINCT o.address) AS distinct_addresses,
              COUNT(*) AS observation_rows,
              COUNT(DISTINCT o.session_id) AS sessions
            FROM ble_observations o
            JOIN capture_sessions s ON s.session_id = o.session_id
            WHERE o.timestamp IS NOT NULL AND o.timestamp >= ? AND o.timestamp <= ?
            GROUP BY s.sensor_id
            ORDER BY observation_rows DESC
            """,
            (start, end),
        ).fetchall()
    ]


def rssi_spread_extremes(
    conn: sqlite3.Connection,
    start: float,
    end: float,
    *,
    min_packets: int = 8,
    limit_each: int = 10,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Narrow RSSI spread vs wide spread (same window) — crude 'static vs mobile / multipath' hint."""
    narrow = conn.execute(
        f"""
        SELECT address,
               MIN(rssi) AS rmin, MAX(rssi) AS rmax,
               (MAX(rssi) - MIN(rssi)) AS spread,
               COUNT(*) AS n
        FROM ble_observations
        WHERE timestamp IS NOT NULL AND timestamp >= ? AND timestamp <= ?
          AND rssi IS NOT NULL AND address IS NOT NULL AND TRIM(address) != ''
        GROUP BY address
        HAVING COUNT(*) >= {int(min_packets)}
        ORDER BY spread ASC, n DESC
        LIMIT {int(limit_each)}
        """,
        (start, end),
    ).fetchall()
    wide = conn.execute(
        f"""
        SELECT address,
               MIN(rssi) AS rmin, MAX(rssi) AS rmax,
               (MAX(rssi) - MIN(rssi)) AS spread,
               COUNT(*) AS n
        FROM ble_observations
        WHERE timestamp IS NOT NULL AND timestamp >= ? AND timestamp <= ?
          AND rssi IS NOT NULL AND address IS NOT NULL AND TRIM(address) != ''
        GROUP BY address
        HAVING COUNT(*) >= {int(min_packets)}
        ORDER BY spread DESC, n DESC
        LIMIT {int(limit_each)}
        """,
        (start, end),
    ).fetchall()
    def pack(rows: list[sqlite3.Row]) -> list[dict[str, Any]]:
        return [
            {
                "address": r["address"],
                "rssi_spread_db": float(r["spread"] or 0),
                "rssi_min": float(r["rmin"]) if r["rmin"] is not None else None,
                "rssi_max": float(r["rmax"]) if r["rmax"] is not None else None,
                "packets": int(r["n"] or 0),
            }
            for r in rows
        ]

    return pack(narrow), pack(wide)


def global_address_context(
    conn: sqlite3.Connection,
    start: float,
    end: float,
) -> dict[str, Any]:
    """Whole-DB vs window distinct address counts (coarse anomaly context)."""
    win = conn.execute(
        """
        SELECT COUNT(DISTINCT address) AS n
        FROM ble_observations
        WHERE timestamp IS NOT NULL AND timestamp >= ? AND timestamp <= ?
          AND address IS NOT NULL AND TRIM(address) != ''
        """,
        (start, end),
    ).fetchone()
    all_t = conn.execute(
        """
        SELECT COUNT(DISTINCT address) AS n
        FROM ble_observations
        WHERE address IS NOT NULL AND TRIM(address) != ''
        """,
    ).fetchone()
    return {
        "distinct_addresses_in_window": int(win["n"] or 0) if win else 0,
        "distinct_addresses_all_time": int(all_t["n"] or 0) if all_t else 0,
    }


def adv_hints_summary(
    conn: sqlite3.Connection,
    start: float,
    end: float,
    *,
    limit: int = 20,
) -> dict[str, Any]:
    """Distinct appearance / flags / tx_power / uuid128 hints observed in window."""
    if not has_adv_extension_columns(conn):
        return {"available": False}
    ap = conn.execute(
        """
        SELECT appearance_hint, COUNT(*) AS n
        FROM ble_observations
        WHERE timestamp IS NOT NULL AND timestamp >= ? AND timestamp <= ?
          AND appearance_hint IS NOT NULL AND LENGTH(TRIM(appearance_hint)) > 0
        GROUP BY appearance_hint
        ORDER BY n DESC
        LIMIT ?
        """,
        (start, end, int(limit)),
    ).fetchall()
    u8 = conn.execute(
        """
        SELECT service_uuid128_hint, COUNT(*) AS n
        FROM ble_observations
        WHERE timestamp IS NOT NULL AND timestamp >= ? AND timestamp <= ?
          AND service_uuid128_hint IS NOT NULL AND LENGTH(TRIM(service_uuid128_hint)) > 0
        GROUP BY service_uuid128_hint
        ORDER BY n DESC
        LIMIT ?
        """,
        (start, end, int(limit)),
    ).fetchall()
    tx = conn.execute(
        """
        SELECT MIN(tx_power_dbm) AS tmin, MAX(tx_power_dbm) AS tmax, COUNT(*) AS n
        FROM ble_observations
        WHERE timestamp IS NOT NULL AND timestamp >= ? AND timestamp <= ?
          AND tx_power_dbm IS NOT NULL
        """,
        (start, end),
    ).fetchone()
    return {
        "available": True,
        "top_appearances": [{"hint": r["appearance_hint"], "rows": int(r["n"] or 0)} for r in ap],
        "top_uuid128_hints": [{"hint": r["service_uuid128_hint"], "rows": int(r["n"] or 0)} for r in u8],
        "tx_power_dbm_window": {
            "min": float(tx["tmin"]) if tx and tx["tmin"] is not None else None,
            "max": float(tx["tmax"]) if tx and tx["tmax"] is not None else None,
            "rows_with_tx": int(tx["n"] or 0) if tx else 0,
        },
    }


def render_rf_inference_markdown(
    conn: sqlite3.Connection,
    start: float,
    end: float,
) -> tuple[list[str], dict[str, Any]]:
    """Markdown sections + structured dict for machine-readable insights payload."""
    lines: list[str] = []
    structured: dict[str, Any] = {}

    ctx = global_address_context(conn, start, end)
    structured["address_context"] = ctx
    lines += [
        "## RF-derived inferences (heuristic)",
        "",
        "_These blocks go beyond raw counts: they are **hypotheses** from timing, RSSI, and identifiers. "
        "Random MACs, decode gaps, and venue effects can invalidate naive conclusions._",
        "",
        "### Global address context",
        "",
        f"- **Distinct addresses (this window):** {ctx['distinct_addresses_in_window']}",
        f"- **Distinct addresses (all time in DB):** {ctx['distinct_addresses_all_time']}",
        "",
    ]

    cov = sensor_coverage_rows(conn, start, end)
    structured["sensor_coverage"] = cov
    lines += ["### Sensor / sniffer coverage (by `sensor_id`)", "", "| sensor_id | Distinct MACs | Rows | Sessions |", "|---|---:|---:|---:|"]
    for r in cov:
        lines.append(
            f"| `{_md_cell(str(r['sensor_id']))}` | {int(r['distinct_addresses'] or 0)} | "
            f"{int(r['observation_rows'] or 0)} | {int(r['sessions'] or 0)} |",
        )
    if not cov:
        lines.append("| — | — | — | — |")
    lines.append("")

    oui = oui_vendor_families(conn, start, end)
    structured["oui_families"] = oui
    lines += [
        "### OUI families (first 3 octets → public vendor hint)",
        "",
        "| OUI (hex) | Distinct MACs | Rows | Vendor (OUI registry) |",
        "|---|---:|---:|---|",
    ]
    for r in oui:
        v = _md_cell(r.get("oui_vendor_hint") or "—")
        lines.append(
            f"| `{r['oui_hex']}` | {r['distinct_macs']} | {r['observation_rows']} | {v or '—'} |",
        )
    if not oui:
        lines.append("| — | — | — | — |")
    lines.append("")

    rot = mac_rotation_clusters(conn, start, end)
    structured["name_service_mac_clusters"] = rot
    lines += [
        "### Possible MAC rotation (same name + service key, multiple MACs)",
        "",
        "| Name key | Service hint key | #MACs | Sample MACs |",
        "|---|---|---:|---|",
    ]
    for r in rot:
        nm = sanitize_ble_display_string(r["name_key"]) or r["name_key"]
        lines.append(
            f"| {_md_cell(nm)} | `{_md_cell(r['service_hint_key'])}` | {r['distinct_macs']} | {_md_cell(r['sample_macs'])} |",
        )
    if not rot:
        lines.append("| — | — | — | _None detected._ |")
    lines.append("")

    pairs = co_presence_top_pairs(conn, start, end)
    structured["co_presence_top_pairs"] = pairs
    lines += [
        "### Co-presence (shared coarse time bins)",
        "",
        "_Pairs of addresses that frequently appear in the same **~2 min** UTC bins among the busiest ~36 MACs. "
        "Weak evidence of co-location or correlated movement — not proof of interaction._",
        "",
        "| Address A | Address B | Shared bins |",
        "|---|---|---:|",
    ]
    for p in pairs:
        lines.append(f"| `{p['address_a']}` | `{p['address_b']}` | {p['shared_time_bins']} |")
    if not pairs:
        lines.append("| — | — | — |")
    lines.append("")

    narrow, wide = rssi_spread_extremes(conn, start, end)
    structured["rssi_narrow_spread"] = narrow
    structured["rssi_wide_spread"] = wide
    lines += [
        "### RSSI spread (static-ish vs volatile RF)",
        "",
        "_Among addresses with ≥8 RSSI samples: **narrow** spread can mean stable path loss; **wide** can mean "
        "motion, multipath, or body blocking._",
        "",
        "**Narrowest spreads**",
        "",
        "| Address | Spread (dB) | min | max | n |",
        "|---|---:|---:|---:|---:|",
    ]
    for x in narrow:
        lines.append(
            f"| `{x['address']}` | {x['rssi_spread_db']:.1f} | {x['rssi_min']} | {x['rssi_max']} | {x['packets']} |",
        )
    if not narrow:
        lines.append("| — | — | — | — | — |")
    lines += ["", "**Widest spreads**", "", "| Address | Spread (dB) | min | max | n |", "|---|---:|---:|---:|---:|"]
    for x in wide:
        lines.append(
            f"| `{x['address']}` | {x['rssi_spread_db']:.1f} | {x['rssi_min']} | {x['rssi_max']} | {x['packets']} |",
        )
    if not wide:
        lines.append("| — | — | — | — | — |")
    lines.append("")

    adv = adv_hints_summary(conn, start, end)
    structured["adv_hints"] = adv
    if adv.get("available"):
        lines += [
            "### GAP / AD extensions (appearance, TX power, flags, 128-bit UUID)",
            "",
            "**Top appearance raw values**",
            "",
            "| Hint | Rows |",
            "|---|---:|",
        ]
        for t in adv.get("top_appearances") or []:
            lines.append(f"| `{_md_cell(str(t['hint']))}` | {t['rows']} |")
        if not adv.get("top_appearances"):
            lines.append("| — | — |")
        lines += ["", "**Top 128-bit service UUID hints**", "", "| Hint | Rows |", "|---|---:|"]
        for t in adv.get("top_uuid128_hints") or []:
            lines.append(f"| `{_md_cell(str(t['hint']))}` | {t['rows']} |")
        if not adv.get("top_uuid128_hints"):
            lines.append("| — | — |")
        txw = adv.get("tx_power_dbm_window") or {}
        lines += [
            "",
            f"_TX power (decoded dBm) rows in window: **{txw.get('rows_with_tx', 0)}** "
            f"(min {txw.get('min')}, max {txw.get('max')})._",
            "",
        ]
    else:
        lines += [
            "### GAP / AD extensions",
            "",
            "_No `appearance_hint` column on `ble_observations` — run **`signalhub-ble`** with a current checkout "
            "so `init-db` adds columns, then **re-import** pcaps to populate appearance / TX power / UUID-128 fields._",
            "",
        ]

    # --- Multi-sensor & advanced analytics (optional; queries may fail on very old SQLite) ---
    try:
        skew = multi_sensor.clock_skew_public_mac_pairs(conn, start, end)
        structured["multi_sensor_clock_skew"] = skew
        lines += [
            "### Multi-sensor: public-MAC clock skew (median Δt)",
            "",
            "_Same public address observed on two sensors; paired rows binned by occurrence index. "
            "Large |Δt| can indicate clock skew or import ordering — not proof of distance._",
            "",
            "| Address | Sensor A | Sensor B | mean Δt (s) | pairs |",
            "|---|---|---|---:|---:|",
        ]
        for s in skew:
            lines.append(
                f"| `{s['address']}` | `{_md_cell(s['sensor_a'])}` | `{_md_cell(s['sensor_b'])}` | "
                f"{s['mean_timestamp_delta_sec']:.4f} | {s['paired_rows']} |",
            )
        if not skew:
            lines.append("| — | — | — | — | — |")
        lines.append("")
    except sqlite3.Error:
        structured["multi_sensor_clock_skew"] = []

    try:
        sp = multi_sensor.spatial_rssi_consistency(conn, start, end)
        structured["multi_sensor_rssi_consistency"] = sp
        lines += [
            "### Multi-sensor: RSSI mean CV across sniffers",
            "",
            "_Lower CV → more stable **relative** RSSI ordering across sensors; higher CV → multipath / motion / asymmetry._",
            "",
            "| Address | #sensors | mean RSSI | CV |",
            "|---|---:|---:|---:|",
        ]
        for s in sp:
            lines.append(
                f"| `{s['address']}` | {s['sensors_heard']} | {s['mean_rssi_across_sensors']:.1f} | "
                f"{s['rssi_mean_cv']:.3f} |",
            )
        if not sp:
            lines.append("| — | — | — | — |")
        lines.append("")
    except sqlite3.Error:
        structured["multi_sensor_rssi_consistency"] = []

    cent = multi_sensor.rssi_weighted_centroid_estimates(conn, start, end)
    structured["rssi_weighted_centroid_m"] = cent
    if cent:
        lines += [
            "### Coarse indoor position (RSSI-weighted centroid, meters)",
            "",
            "_Requires **`sensor_positions`** (CLI: `signalhub-ble sensor position-set …`). "
            "This is a **rough** 2D estimate, not surveyed truth._",
            "",
            "| Address | x̂ (m) | ŷ (m) | #sniffers |",
            "|---|---:|---:|---:|",
        ]
        for c in cent:
            lines.append(
                f"| `{c['address']}` | {c['x_m_est']:.2f} | {c['y_m_est']:.2f} | {c['sensors_used']} |",
            )
        lines.append("")

    ent = advanced_analytics.pdu_entropy_top_addresses(conn, start, end)
    structured["pdu_entropy"] = ent
    lines += [
        "### PDU-type entropy (address chatter mix)",
        "",
        "| Address | Entropy (bits) | packets |",
        "|---|---:|---:|",
    ]
    for e in ent:
        lines.append(f"| `{e['address']}` | {e['pdu_entropy_bits']} | {e['packets']} |")
    if not ent:
        lines.append("| — | — | — |")
    lines.append("")

    iacv = advanced_analytics.interarrival_cv_top(conn, start, end)
    structured["interarrival_cv"] = iacv
    lines += [
        "### Inter-arrival irregularity (CV of gaps)",
        "",
        "| Address | CV | gaps |",
        "|---|---:|---:|",
    ]
    for e in iacv:
        lines.append(f"| `{e['address']}` | {e['interarrival_cv']:.3f} | {e['gaps']} |")
    if not iacv:
        lines.append("| — | — | — |")
    lines.append("")

    graph = advanced_analytics.co_presence_graph_metrics(pairs)
    structured["co_presence_graph"] = graph
    lines += [
        "### Co-presence graph (lightweight)",
        "",
        f"_Edges: **{graph['edges']}**, nodes: **{graph['nodes']}**, max degree: **{graph['max_degree']}**._",
        "",
    ]
    if graph.get("top_hubs"):
        lines.append("| Hub address | degree |")
        lines.append("|---|---:|")
        for h in graph["top_hubs"]:
            lines.append(f"| `{h['address']}` | {h['degree']} |")
        lines.append("")

    drift = advanced_analytics.ledger_hint_drift_events(conn, limit=12)
    structured["ledger_hint_drift"] = drift
    lines += [
        "### Ledger hint drift (multiple alias values)",
        "",
        "| ledger_id | address | variants |",
        "|---|---|---|",
    ]
    for d in drift:
        var = "; ".join(f"{v['alias_type']}×{v['k']}" for v in d["alias_variants"])
        lines.append(f"| `{d['ledger_id']}` | `{d['address']}` | {_md_cell(var)} |")
    if not drift:
        lines.append("| — | — | — |")
    lines.append("")

    priv = advanced_analytics.privacy_exposure_score_window(conn, start, end)
    structured["privacy_exposure"] = priv
    lines += [
        "### Privacy exposure (identity hints per 1000 packets)",
        "",
        f"- Packets in window: **{priv['packets']}**",
        f"- Rows with any name/mfg/service hint: **{priv.get('rows_with_any_identity_hint', 0)}**",
        f"- **Exposure index:** {priv.get('exposure_per_1000', 0)} hint-rows / 1000 packets",
        "",
    ]

    ch_metrics = insight_analytics.capture_health_metrics(conn, start, end)
    health_flags = list(ch_metrics.get("health_flags") or [])
    structured["capture_health_flags"] = health_flags
    nar = advanced_analytics.composite_narrative_hints(
        capture_health_flags=health_flags,
        rf_pairs_count=len(pairs),
        drift_count=len(drift),
    )
    structured["composite_narrative_hints"] = nar
    lines += ["### Composite narrative hints", ""]
    for b in nar:
        lines.append(f"- {b}")
    lines.append("")

    lines += [
        "### Encrypted ATT / decrypt workflow",
        "",
        "_Decrypting user payloads requires **keys** (LTK/link key) and usually Wireshark/tshark prefs aligned to your "
        "Wireshark version._",
        "",
        "- Use **`ble_session_crypto`** (populated on import) to see **encrypted row counts** per session.",
        "- CLI: `signalhub-ble crypto status --session <UUID>` and `crypto set-secrets --session <UUID> --file path`.",
        "- Run `signalhub-ble crypto suggest-decrypt --pcap in.pcapng --out out.pcapng` for a **tshark** starter line.",
        "",
    ]

    return lines, structured


def _md_cell(value: str | None) -> str:
    if value is None:
        return ""
    return str(value).replace("|", "\\|").replace("\n", " ").replace("\r", "")
