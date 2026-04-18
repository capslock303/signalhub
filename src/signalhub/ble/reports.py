from __future__ import annotations

import csv
import sqlite3
from pathlib import Path

from signalhub import __version__ as signalhub_version
from signalhub.common.textutil import sanitize_ble_display_string
from signalhub.common.timeutil import epoch_to_iso, utc_day_range_epoch


def _md_cell(value: str | None) -> str:
    if value is None:
        return ""
    return str(value).replace("|", "\\|").replace("\n", " ").replace("\r", "")


def render_session_report(conn: sqlite3.Connection, session_id: str) -> str:
    s = conn.execute(
        "SELECT * FROM capture_sessions WHERE session_id = ?",
        (session_id,),
    ).fetchone()
    if not s:
        return f"# Session report\n\nUnknown session `{session_id}`.\n"

    lines: list[str] = [
        "# Session report",
        "",
        "## Metadata",
        "",
        f"- **session_id**: `{s['session_id']}`",
        f"- **sensor_id**: `{s['sensor_id']}`",
        f"- **started_at**: {epoch_to_iso(s['started_at'])}",
        f"- **ended_at**: {epoch_to_iso(s['ended_at'])}",
        f"- **source_path**: `{s['source_path']}`",
        f"- **imported_at**: {epoch_to_iso(s['imported_at'])}",
        "",
    ]

    top = conn.execute(
        """
        SELECT address, COUNT(*) c
        FROM ble_observations
        WHERE session_id = ?
        GROUP BY address
        ORDER BY c DESC
        LIMIT 25
        """,
        (session_id,),
    ).fetchall()
    lines += ["## Top recurring identities (by packet count)", ""]
    for addr, c in top:
        lines.append(f"- `{addr}` — {c} observations")
    lines.append("")

    rssi = conn.execute(
        """
        SELECT address, MIN(rssi) rmin, MAX(rssi) rmax, COUNT(*) c
        FROM ble_observations
        WHERE session_id = ? AND rssi IS NOT NULL
        GROUP BY address
        ORDER BY rmax DESC
        LIMIT 15
        """,
        (session_id,),
    ).fetchall()
    lines += ["## Strongest RSSI (max per address)", ""]
    for addr, rmin, rmax, c in rssi:
        lines.append(f"- `{addr}` — max {rmax} dBm (min {rmin}, n={c})")
    lines.append("")

    conn_rows = conn.execute(
        """
        SELECT DISTINCT address
        FROM ble_observations
        WHERE session_id = ? AND connection_seen = 1
        LIMIT 50
        """,
        (session_id,),
    ).fetchall()
    lines += ["## Observed connection-related frames (address list)", ""]
    if not conn_rows:
        lines.append("_None recorded in parsed flags for this session._")
    else:
        for (addr,) in conn_rows:
            lines.append(f"- `{addr}`")
    lines.append("")

    hints = conn.execute(
        """
        SELECT DISTINCT address, name_hint, manufacturer_hint, service_hint
        FROM ble_observations
        WHERE session_id = ?
          AND (name_hint IS NOT NULL OR manufacturer_hint IS NOT NULL OR service_hint IS NOT NULL)
        LIMIT 50
        """,
        (session_id,),
    ).fetchall()
    lines += ["## Identity hints (name / manufacturer / service)", ""]
    if not hints:
        lines.append("_No decoded hints in this import (check tshark field list)._")
    else:
        for addr, name, mfg, svc in hints:
            parts = [f"`{addr}`"]
            sn = sanitize_ble_display_string(name)
            if sn:
                parts.append(f"name={sn}")
            sm = sanitize_ble_display_string(mfg)
            if sm:
                parts.append(f"mfg={sm}")
            ss = sanitize_ble_display_string(svc)
            if ss:
                parts.append(f"svc={ss}")
            lines.append("- " + ", ".join(parts))
    lines.append("")

    amb = conn.execute(
        """
        SELECT address, COUNT(*) c
        FROM ble_observations
        WHERE session_id = ?
        GROUP BY address
        HAVING c < 5
        ORDER BY c ASC
        LIMIT 40
        """,
        (session_id,),
    ).fetchall()
    lines += ["## Sparse identities (manual review)", ""]
    if not amb:
        lines.append("_No ultra-sparse addresses in the top query._")
    else:
        for addr, c in amb:
            lines.append(f"- `{addr}` — {c} observations")
    lines.append("")
    return "\n".join(lines)


def render_ledger_report(conn: sqlite3.Connection) -> str:
    rows = list(
        conn.execute(
            "SELECT * FROM ble_devices ORDER BY (last_seen IS NULL), last_seen DESC",
        ),
    )
    lines = [
        "# Ledger report",
        "",
        f"**Devices:** {len(rows)}",
        "",
        "| ledger_id | address | class | confidence | last_seen | name |",
        "|---|---|---|---|---|---|",
    ]
    for d in rows:
        lines.append(
            "| {lid} | `{addr}` | {cls} | {conf} | {ls} | {name} |".format(
                lid=d["ledger_id"],
                addr=d["address"] or "",
                cls=d["probable_device_class"] or "",
                conf=d["confidence"] or "",
                ls=epoch_to_iso(d["last_seen"]),
                name=_md_cell(sanitize_ble_display_string(d["current_name_hint"])),
            )
        )
    lines.append("")
    return "\n".join(lines)


def render_change_report(conn: sqlite3.Connection, from_date: str, to_date: str) -> str:
    start, end = utc_day_range_epoch(from_date, to_date)
    lines = [
        "# Change report",
        "",
        f"**Range (UTC):** {from_date} → {to_date}",
        "",
    ]

    new_d = list(
        conn.execute(
            """
            SELECT * FROM ble_devices
            WHERE first_seen IS NOT NULL AND first_seen >= ? AND first_seen <= ?
            ORDER BY first_seen
            """,
            (start, end),
        ),
    )
    lines += ["## New identities (first seen in range)", ""]
    for d in new_d:
        lines.append(
            f"- `{d['address']}` ({d['ledger_id']}) first {epoch_to_iso(d['first_seen'])} "
            f"class={d['probable_device_class']} conf={d['confidence']}"
        )
    if not new_d:
        lines.append("_None._")
    lines.append("")

    gone = list(
        conn.execute(
            """
            SELECT * FROM ble_devices
            WHERE last_seen IS NOT NULL AND last_seen < ?
            ORDER BY last_seen DESC
            """,
            (start,),
        ),
    )
    lines += [
        "## Not observed since range start (ledger last_seen before range)",
        "",
        "_Interpret conservatively: capture gaps look like disappearance._",
        "",
    ]
    for d in gone[:80]:
        lines.append(
            f"- `{d['address']}` ({d['ledger_id']}) last {epoch_to_iso(d['last_seen'])}"
        )
    if not gone:
        lines.append("_None._")
    lines.append("")

    stepped = list(
        conn.execute(
            """
            SELECT d.ledger_id, d.address, d.rssi_min, d.rssi_max, s.rssi_max AS session_peak
            FROM ble_devices d
            JOIN ble_device_session_summary s ON s.ledger_id = d.ledger_id
            WHERE s.last_seen IS NOT NULL AND s.last_seen >= ? AND s.last_seen <= ?
              AND s.rssi_max IS NOT NULL AND d.rssi_max IS NOT NULL
              AND s.rssi_max >= d.rssi_max + 10
            LIMIT 40
            """,
            (start, end),
        ),
    )
    lines += ["## Sessions with much stronger peak RSSI than ledger max (+10 dB rule)", ""]
    if not stepped:
        lines.append("_None matched this coarse heuristic._")
    else:
        for row in stepped:
            lines.append(
                f"- `{row['address']}` session_peak {row['session_peak']} vs ledger_max {row['rssi_max']}"
            )
    lines.append("")

    newly_gatt = list(
        conn.execute(
            """
            SELECT ledger_id, address, gatt_seen
            FROM ble_devices
            WHERE gatt_seen = 1 AND last_seen >= ? AND last_seen <= ?
            LIMIT 60
            """,
            (start, end),
        ),
    )
    lines += ["## Devices with GATT observed (ledger flag) and recent last_seen in range", ""]
    if not newly_gatt:
        lines.append("_None._")
    else:
        for row in newly_gatt:
            lines.append(f"- `{row['address']}` ({row['ledger_id']})")
    lines.append("")
    return "\n".join(lines)


def ble_devices_rows_for_export(
    conn: sqlite3.Connection,
    *,
    from_date: str | None,
    to_date: str | None,
    by: str,
) -> list[sqlite3.Row]:
    """Return ledger rows, optionally filtered by UTC inclusive day range."""
    if from_date is None and to_date is None:
        return list(conn.execute("SELECT * FROM ble_devices ORDER BY ledger_id"))
    if from_date is None or to_date is None:
        raise ValueError("from_date and to_date must both be set when filtering")
    start, end = utc_day_range_epoch(from_date, to_date)
    if by == "last_seen":
        return list(
            conn.execute(
                """
                SELECT * FROM ble_devices
                WHERE last_seen IS NOT NULL AND last_seen >= ? AND last_seen <= ?
                ORDER BY ledger_id
                """,
                (start, end),
            ),
        )
    if by == "first_seen":
        return list(
            conn.execute(
                """
                SELECT * FROM ble_devices
                WHERE first_seen IS NOT NULL AND first_seen >= ? AND first_seen <= ?
                ORDER BY ledger_id
                """,
                (start, end),
            ),
        )
    if by == "active":
        return list(
            conn.execute(
                """
                SELECT * FROM ble_devices
                WHERE first_seen IS NOT NULL
                  AND first_seen <= ?
                  AND (last_seen IS NULL OR last_seen >= ?)
                ORDER BY ledger_id
                """,
                (end, start),
            ),
        )
    raise ValueError(by)


def capture_sessions_rows_for_export(
    conn: sqlite3.Connection,
    *,
    from_date: str,
    to_date: str,
) -> list[sqlite3.Row]:
    """Sessions whose time span overlaps the UTC inclusive day range."""
    start, end = utc_day_range_epoch(from_date, to_date)
    return list(
        conn.execute(
            """
            SELECT * FROM capture_sessions
            WHERE COALESCE(started_at, imported_at) <= ?
              AND COALESCE(ended_at, started_at, imported_at) >= ?
            ORDER BY imported_at
            """,
            (end, start),
        ),
    )


def ble_observations_rows_for_export(
    conn: sqlite3.Connection,
    *,
    from_date: str,
    to_date: str,
) -> list[sqlite3.Row]:
    start, end = utc_day_range_epoch(from_date, to_date)
    return list(
        conn.execute(
            """
            SELECT * FROM ble_observations
            WHERE timestamp IS NOT NULL AND timestamp >= ? AND timestamp <= ?
            ORDER BY observation_id
            """,
            (start, end),
        ),
    )


def write_assessment_table_csvs(
    conn: sqlite3.Connection,
    *,
    from_date: str,
    to_date: str,
    directory: Path,
    stem: str,
) -> list[Path]:
    """CSV rollups aligned with `report assess` tables (summary, PDU, addresses, names, ledger class)."""
    start, end = utc_day_range_epoch(from_date, to_date)
    directory = Path(directory)
    written: list[Path] = []

    def wcsv(suffix: str, fieldnames: list[str], rows: list[dict]) -> None:
        path = directory / f"{stem}-{suffix}.csv"
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            for r in rows:
                w.writerow(r)
        written.append(path)

    obs_count_row = conn.execute(
        """
        SELECT COUNT(*) AS n, COUNT(DISTINCT address) AS addr
        FROM ble_observations
        WHERE timestamp IS NOT NULL AND timestamp >= ? AND timestamp <= ?
        """,
        (start, end),
    ).fetchone()
    n_obs = int(obs_count_row["n"] or 0) if obs_count_row else 0
    n_addr = int(obs_count_row["addr"] or 0) if obs_count_row else 0
    c_conn = c_gatt = c_smp = c_enc = 0
    if n_obs:
        flags = conn.execute(
            """
            SELECT
              SUM(connection_seen) AS c_conn,
              SUM(gatt_seen) AS c_gatt,
              SUM(smp_seen) AS c_smp,
              SUM(encrypted_seen) AS c_enc
            FROM ble_observations
            WHERE timestamp IS NOT NULL AND timestamp >= ? AND timestamp <= ?
            """,
            (start, end),
        ).fetchone()
        c_conn = int(flags["c_conn"] or 0)
        c_gatt = int(flags["c_gatt"] or 0)
        c_smp = int(flags["c_smp"] or 0)
        c_enc = int(flags["c_enc"] or 0)

    wcsv(
        "table-summary",
        [
            "utc_from",
            "utc_to",
            "packets",
            "distinct_addresses",
            "rows_connection_seen",
            "rows_gatt_seen",
            "rows_smp_seen",
            "rows_encrypted_seen",
        ],
        [
            {
                "utc_from": from_date,
                "utc_to": to_date,
                "packets": n_obs,
                "distinct_addresses": n_addr,
                "rows_connection_seen": c_conn,
                "rows_gatt_seen": c_gatt,
                "rows_smp_seen": c_smp,
                "rows_encrypted_seen": c_enc,
            },
        ],
    )

    pdu_rows = conn.execute(
        """
        SELECT pdu_type, COUNT(*) AS n
        FROM ble_observations
        WHERE timestamp IS NOT NULL AND timestamp >= ? AND timestamp <= ?
        GROUP BY pdu_type
        ORDER BY n DESC
        """,
        (start, end),
    ).fetchall()
    wcsv(
        "table-pdu_types",
        ["pdu_type", "count"],
        [{"pdu_type": r["pdu_type"] or "", "count": int(r["n"])} for r in pdu_rows],
    )

    addr_rows = conn.execute(
        """
        SELECT address, COUNT(*) AS n
        FROM ble_observations
        WHERE timestamp IS NOT NULL AND timestamp >= ? AND timestamp <= ?
          AND address IS NOT NULL AND address != ''
        GROUP BY address
        ORDER BY n DESC
        """,
        (start, end),
    ).fetchall()
    wcsv(
        "table-address_counts",
        ["address", "row_count"],
        [{"address": r["address"], "row_count": int(r["n"])} for r in addr_rows],
    )

    named_w = list(
        conn.execute(
            """
            SELECT
              MAX(TRIM(name_hint)) AS display_name,
              MIN(timestamp) AS first_ts,
              MAX(timestamp) AS last_ts,
              COUNT(*) AS row_n,
              COUNT(DISTINCT address) AS addr_n,
              GROUP_CONCAT(DISTINCT CASE
                WHEN address IS NOT NULL AND TRIM(address) != '' THEN TRIM(address)
              END) AS addrs
            FROM ble_observations
            WHERE timestamp IS NOT NULL AND timestamp >= ? AND timestamp <= ?
              AND name_hint IS NOT NULL AND LENGTH(TRIM(name_hint)) > 0
            GROUP BY LOWER(TRIM(name_hint))
            ORDER BY last_ts DESC, display_name ASC
            """,
            (start, end),
        ),
    )
    wcsv(
        "table-named_devices_window",
        [
            "friendly_name",
            "first_seen_utc",
            "last_seen_utc",
            "packets",
            "distinct_addresses",
            "sample_mac_addresses",
        ],
        [
            {
                "friendly_name": sanitize_ble_display_string(r["display_name"]) or "(non-printable name)",
                "first_seen_utc": epoch_to_iso(r["first_ts"]),
                "last_seen_utc": epoch_to_iso(r["last_ts"]),
                "packets": int(r["row_n"]),
                "distinct_addresses": int(r["addr_n"]),
                "sample_mac_addresses": (r["addrs"] or "").replace(",", ", "),
            }
            for r in named_w
        ],
    )

    named_c = list(
        conn.execute(
            """
            SELECT
              MAX(TRIM(name_hint)) AS display_name,
              MIN(timestamp) AS first_ts,
              MAX(timestamp) AS last_ts,
              COUNT(*) AS row_n,
              COUNT(DISTINCT address) AS addr_n,
              GROUP_CONCAT(DISTINCT CASE
                WHEN address IS NOT NULL AND TRIM(address) != '' THEN TRIM(address)
              END) AS addrs
            FROM ble_observations
            WHERE timestamp IS NOT NULL
              AND name_hint IS NOT NULL AND LENGTH(TRIM(name_hint)) > 0
            GROUP BY LOWER(TRIM(name_hint))
            ORDER BY last_ts DESC, display_name ASC
            LIMIT 500
            """,
        ),
    )
    wcsv(
        "table-named_devices_cumulative",
        [
            "friendly_name",
            "first_seen_utc",
            "last_seen_utc",
            "packets",
            "distinct_addresses",
            "sample_mac_addresses",
        ],
        [
            {
                "friendly_name": sanitize_ble_display_string(r["display_name"]) or "(non-printable name)",
                "first_seen_utc": epoch_to_iso(r["first_ts"]),
                "last_seen_utc": epoch_to_iso(r["last_ts"]),
                "packets": int(r["row_n"]),
                "distinct_addresses": int(r["addr_n"]),
                "sample_mac_addresses": (r["addrs"] or "").replace(",", ", "),
            }
            for r in named_c
        ],
    )

    active_devices = ble_devices_rows_for_export(
        conn,
        from_date=from_date,
        to_date=to_date,
        by="active",
    )
    by_class: dict[str, int] = {}
    for d in active_devices:
        k = d["probable_device_class"] or "(unclassified)"
        by_class[k] = by_class.get(k, 0) + 1
    wcsv(
        "table-ledger_device_class",
        ["probable_device_class", "device_count"],
        [
            {"probable_device_class": k, "device_count": v}
            for k, v in sorted(by_class.items(), key=lambda x: (-x[1], x[0]))
        ],
    )

    return written


def render_assessment_report(conn: sqlite3.Connection, from_date: str, to_date: str) -> str:
    """Conservative, DB-driven summary for a UTC day range (heuristics, not ground truth)."""
    start, end = utc_day_range_epoch(from_date, to_date)
    lines = [
        "# BLE assessment (automated)",
        "",
        f"> **Generated by signalhub `{signalhub_version}`** — if this block is missing, the Pi (or PC) that ran `report assess` is still on an **older** checkout; run `pip install -e` on the current `signalhub` tree and regenerate.",
        "",
        "_Heuristics over imported captures. Gaps in sniffing can look like “devices left”._",
        "",
        f"**UTC range:** `{from_date}` → `{to_date}` (inclusive days)",
        "",
    ]

    sessions = capture_sessions_rows_for_export(conn, from_date=from_date, to_date=to_date)
    lines += ["## Capture sessions (overlap window)", "", f"**Count:** {len(sessions)}", ""]
    for s in sessions[:25]:
        lines.append(
            f"- `{s['session_id'][:8]}…` sensor={s['sensor_id']} "
            f"started={epoch_to_iso(s['started_at'])} ended={epoch_to_iso(s['ended_at'])} "
            f"imported={epoch_to_iso(s['imported_at'])}",
        )
    if len(sessions) > 25:
        lines.append(f"- _…and {len(sessions) - 25} more_")
    if not sessions:
        lines.append("_No sessions overlap this window (check dates or import coverage)._")
    lines.append("")

    obs_count_row = conn.execute(
        """
        SELECT COUNT(*) AS n,
               COUNT(DISTINCT address) AS addr
        FROM ble_observations
        WHERE timestamp IS NOT NULL AND timestamp >= ? AND timestamp <= ?
        """,
        (start, end),
    ).fetchone()
    n_obs = int(obs_count_row["n"]) if obs_count_row else 0
    n_addr = int(obs_count_row["addr"]) if obs_count_row else 0
    lines += [
        "## Observations in window",
        "",
        f"**Packets:** {n_obs} &nbsp; **Distinct addresses:** {n_addr}",
        "",
    ]

    if n_obs:
        flags = conn.execute(
            """
            SELECT
              SUM(connection_seen) AS c_conn,
              SUM(gatt_seen) AS c_gatt,
              SUM(smp_seen) AS c_smp,
              SUM(encrypted_seen) AS c_enc
            FROM ble_observations
            WHERE timestamp IS NOT NULL AND timestamp >= ? AND timestamp <= ?
            """,
            (start, end),
        ).fetchone()
        c_conn = int(flags["c_conn"] or 0)
        c_gatt = int(flags["c_gatt"] or 0)
        c_smp = int(flags["c_smp"] or 0)
        c_enc = int(flags["c_enc"] or 0)
        lines += [
            "**Protocol hints (frame-level flags; not mutual exclusive):**",
            "",
            f"- Rows with `connection_seen`: {c_conn} ({100.0 * c_conn / n_obs:.1f}%)",
            f"- Rows with `gatt_seen`: {c_gatt} ({100.0 * c_gatt / n_obs:.1f}%)",
            f"- Rows with `smp_seen`: {c_smp} ({100.0 * c_smp / n_obs:.1f}%)",
            f"- Rows with `encrypted_seen`: {c_enc} ({100.0 * c_enc / n_obs:.1f}%)",
            "",
        ]

        pdu_rows = conn.execute(
            """
            SELECT pdu_type, COUNT(*) AS n
            FROM ble_observations
            WHERE timestamp IS NOT NULL AND timestamp >= ? AND timestamp <= ?
            GROUP BY pdu_type
            ORDER BY n DESC
            LIMIT 15
            """,
            (start, end),
        ).fetchall()
        lines += ["### Top PDU types", "", "| pdu_type | count |", "|---|---:|"]
        for r in pdu_rows:
            pt = (r["pdu_type"] or "").replace("|", "\\|")
            lines.append(f"| {pt} | {r['n']} |")
        lines.append("")

        top_addr = conn.execute(
            """
            SELECT address, COUNT(*) AS n
            FROM ble_observations
            WHERE timestamp IS NOT NULL AND timestamp >= ? AND timestamp <= ?
              AND address IS NOT NULL AND address != ''
            GROUP BY address
            ORDER BY n DESC
            LIMIT 15
            """,
            (start, end),
        ).fetchall()
        lines += ["### Busiest addresses (by row count)", "", "| address | rows |", "|---|---:|"]
        for r in top_addr:
            lines.append(f"| `{r['address']}` | {r['n']} |")
        lines.append("")

    lines += [
        "",
        "_Advertising **Complete Local Name** / **Shortened Local Name** rollups (`name_hint`) are not listed in this Markdown; "
        "use **`signalhub-ble export assessment-tables`** "
        "(`*-table-named_devices_window.csv`, `*-table-named_devices_cumulative.csv`)._",
        "",
    ]

    active_devices = ble_devices_rows_for_export(
        conn,
        from_date=from_date,
        to_date=to_date,
        by="active",
    )
    lines += [
        "## Ledger identities “active” in window",
        "",
        "_Overlap rule: `first_seen` ≤ range end and (`last_seen` is null or `last_seen` ≥ range start)._",
        "",
        f"**Count:** {len(active_devices)}",
        "",
    ]

    if active_devices:
        by_class: dict[str, int] = {}
        for d in active_devices:
            k = d["probable_device_class"] or "(unclassified)"
            by_class[k] = by_class.get(k, 0) + 1
        lines += ["### By `probable_device_class`", "", "| class | devices |", "|---|---:|"]
        for cls, cnt in sorted(by_class.items(), key=lambda x: (-x[1], x[0]))[:20]:
            cesc = cls.replace("|", "\\|")
            lines.append(f"| {cesc} | {cnt} |")
        lines.append("")

    lines += ["## Automated takeaways (conservative)", ""]
    bullets: list[str] = []
    if not sessions and n_obs == 0:
        bullets.append(
            "- No overlapping sessions and no timestamped observations in this window — "
            " widen the range or confirm imports completed.",
        )
    if n_obs and n_addr:
        ratio = n_obs / max(n_addr, 1)
        if ratio < 3:
            bullets.append(
                f"- Low packets-per-address ratio (~{ratio:.1f}) may indicate sparse captures, "
                "many quiet devices, or address rotation — not necessarily “light usage”.",
            )
        elif ratio > 80:
            bullets.append(
                f"- High packets-per-address ratio (~{ratio:.0f}) often means a few advertisers dominated the airtime "
                "in these files (beacons, static fixtures, or replay-heavy sources).",
            )
    if n_obs:
        gatt_row = conn.execute(
            """
            SELECT COUNT(*) AS n FROM ble_observations
            WHERE timestamp IS NOT NULL AND timestamp >= ? AND timestamp <= ? AND gatt_seen = 1
            """,
            (start, end),
        ).fetchone()
        gatt_n = int(gatt_row["n"] if gatt_row else 0)
        gatt_pct = 100.0 * gatt_n / n_obs
        if gatt_pct >= 8.0:
            bullets.append(
                f"- ~{gatt_pct:.1f}% of rows carry a GATT dissector hint — connectable peripherals or "
                "rich L2CAP/GATT traffic may be present (still depends on Wireshark decode depth).",
            )
        elif gatt_pct <= 0.5 and n_obs > 500:
            bullets.append(
                "- Very few GATT-tagged rows versus volume — environment may be mostly advertising traffic "
                "(or GATT not visible on this link layer path).",
            )
    if active_devices:
        beaconish = sum(
            1
            for d in active_devices
            if (d["probable_device_class"] or "").lower().find("beacon") >= 0
            or (d["probable_device_class"] or "").lower().find("nonconn") >= 0
        )
        if beaconish >= max(3, len(active_devices) // 3):
            bullets.append(
                f"- {beaconish} ledger rows look broadcast-heavy by classifier — useful for venue/fixed-sensor "
                "contexts; fewer assumptions about paired phone peripherals.",
            )
    if not bullets:
        bullets.append("- No strong automated pattern fired for this window; review tables above.")
    for b in bullets:
        lines.append(b)
    lines.append("")
    return "\n".join(lines)


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
