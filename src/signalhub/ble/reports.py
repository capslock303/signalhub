from __future__ import annotations

import csv
import logging
import sqlite3
from pathlib import Path

logger = logging.getLogger(__name__)

from signalhub import __version__ as signalhub_version
from signalhub.ble import insight_analytics, rf_inferences
from signalhub.common import sig_lookup
from signalhub.common.manuf_lookup import vendor_for_mac
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

    if rf_inferences.has_adv_extension_columns(conn):
        adv_rows = conn.execute(
            """
            SELECT DISTINCT appearance_hint, tx_power_dbm, adv_flags_hex, service_uuid128_hint
            FROM ble_observations
            WHERE session_id = ?
              AND (
                appearance_hint IS NOT NULL OR tx_power_dbm IS NOT NULL
                OR adv_flags_hex IS NOT NULL OR service_uuid128_hint IS NOT NULL
              )
            LIMIT 30
            """,
            (session_id,),
        ).fetchall()
        lines += ["## GAP / AD extensions (appearance, TX power, flags, UUID-128)", ""]
        if not adv_rows:
            lines.append("_No extended AD fields decoded in this session (or not present on air)._")
        else:
            lines.append("| appearance | TX dBm | flags | UUID-128 |")
            lines.append("|---|---|---|---|")
            for ar in adv_rows:
                lines.append(
                    f"| {_md_cell(str(ar['appearance_hint'] or ''))} | "
                    f"{ar['tx_power_dbm'] if ar['tx_power_dbm'] is not None else ''} | "
                    f"{_md_cell(str(ar['adv_flags_hex'] or ''))} | "
                    f"{_md_cell(str(ar['service_uuid128_hint'] or ''))} |",
                )
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


def _capture_health_markdown(ch: dict) -> list[str]:
    lines = [
        "## Capture health (sessions vs observations)",
        "",
        "_Separates **import/session bookkeeping** from **frames whose timestamps fall in the UTC window**. "
        "Many overlap sessions with almost no rows usually mean micro-captures, clock skew, or a range that "
        "does not align with where frame timestamps landed._",
        "",
        f"- **Overlap sessions** (metadata intersects window): **{ch['overlap_session_count']}**",
        f"- **Observation rows** with `timestamp` in window: **{ch['observation_rows_in_window']}**",
        f"- **Distinct addresses** (window): **{ch['distinct_addresses_in_window']}**",
        f"- **Distinct sessions** contributing ≥1 row in window: **{ch['distinct_sessions_with_obs']}**",
        f"- **Overlap sessions with 0 rows in window:** **{ch['overlap_sessions_with_zero_obs']}** "
        f"of {ch['overlap_session_count']}",
        f"- **Among sessions with data:** median rows/session ≈ **{ch['median_obs_per_session_with_obs']:.1f}**, "
        f"mean ≈ **{ch['mean_obs_per_session_with_obs']:.1f}**",
        "",
    ]
    if ch.get("health_flags"):
        lines.append("**Flags:**")
        for f in ch["health_flags"]:
            lines.append(f"- `{f}`")
        lines.append("")
    return lines


def _baseline_compare_markdown(
    from_date: str,
    to_date: str,
    bf: str,
    bt: str,
    cur: dict,
    base: dict,
    delta: dict,
) -> list[str]:
    lines = [
        "## Baseline comparison (prior UTC window)",
        "",
        f"_Current: `{from_date}` → `{to_date}` · Baseline: `{bf}` → `{bt}` (inclusive days)._",
        "",
        "| Metric | Baseline | Current | Δ |",
        "|---|---:|---:|---:|",
    ]

    def row(label: str, key: str) -> None:
        b = int(base.get(key) or 0)
        c = int(cur.get(key) or 0)
        d = int(delta[key]) if delta and key in delta else c - b
        lines.append(f"| {label} | {b} | {c} | {d:+d} |")

    row("Overlap sessions", "overlap_session_count")
    row("Observation rows in window", "observation_rows_in_window")
    row("Distinct addresses in window", "distinct_addresses_in_window")
    row("Sessions with ≥1 obs in window", "distinct_sessions_with_obs")
    row("Overlap sessions with 0 obs in window", "overlap_sessions_with_zero_obs")
    lines.append("")
    return lines


def _device_window_rows_for_report(
    conn: sqlite3.Connection,
    from_date: str,
    to_date: str,
    start: float,
    end: float,
    *,
    materialize: bool,
) -> tuple[list[sqlite3.Row], str]:
    """Return top window rows and a note about materialization."""
    note = ""
    rows: list[sqlite3.Row] = []
    if materialize:
        try:
            insight_analytics.ensure_window_stats_table(conn)
            n = insight_analytics.refresh_ble_device_window_stats(conn, from_date, to_date, start, end)
            rows = insight_analytics.top_window_stats_rows(conn, from_date, to_date, limit=22)
            note = f"_Materialized **{n}** address row(s) into `ble_device_window_stats` for this window._"
        except sqlite3.Error as e:
            note = f"_Could not write window stats ({e}); using inline SQL (read-only or DB locked)._"
            rows = insight_analytics.compute_top_device_window_rows_inline(conn, start, end, limit=22)
    else:
        note = "_Read-only / materialize off — temporal metrics computed inline (not stored)._"
        rows = insight_analytics.compute_top_device_window_rows_inline(conn, start, end, limit=22)
    return rows, note


def render_insights_report(
    conn: sqlite3.Connection,
    from_date: str,
    to_date: str,
    *,
    enrich_registry: bool = True,
    materialize_window_stats: bool = True,
    baseline_from_date: str | None = None,
    baseline_to_date: str | None = None,
) -> str:
    """Single consolidated narrative: activity, new/returning devices, registry hints, next steps.

    Replaces juggling separate assessment / change / ledger markdowns in the UI; those
    exports remain available from the CLI for raw tables.
    """
    start, end = utc_day_range_epoch(from_date, to_date)
    lines: list[str] = [
        "# BLE insights (consolidated)",
        "",
        f"> **signalhub `{signalhub_version}`** · UTC window `{from_date}` → `{to_date}` (inclusive days)",
        "",
        "_Public vendor names are registry hints (OUI assignment ≠ proof a device is that product). "
        "BLE advertising can omit or spoof identifiers._",
        "",
    ]

    ch = insight_analytics.capture_health_metrics(conn, start, end)
    lines += _capture_health_markdown(ch)

    baseline_health: dict | None = None
    delta: dict | None = None
    if (
        baseline_from_date
        and baseline_to_date
        and baseline_from_date.strip()
        and baseline_to_date.strip()
    ):
        bf, bt = baseline_from_date.strip(), baseline_to_date.strip()
        try:
            sb, seb = utc_day_range_epoch(bf, bt)
            baseline_health = insight_analytics.capture_health_metrics(conn, sb, seb)
            delta = insight_analytics.baseline_delta(ch, baseline_health)
            lines += _baseline_compare_markdown(from_date, to_date, bf, bt, ch, baseline_health, delta)
        except ValueError as e:
            lines += ["## Baseline comparison", "", f"_Skipped: invalid baseline range ({e})._", ""]

    win_rows, win_note = _device_window_rows_for_report(
        conn, from_date, to_date, start, end, materialize=materialize_window_stats,
    )
    lines += [
        "## Per-address recurrence (this window)",
        "",
        win_note,
        "",
        "_**Sess** = distinct capture sessions; **UTC hours** = distinct hour buckets of frame timestamps; "
        "**Avg gap** ≈ mean seconds between consecutive frames (same address), when computable._",
        "",
        "| Address | Rows | Sess | Active UTC hrs | Span (s) | Avg gap (s) | Ledger class | Identity | Vendor (OUI) |",
        "|---|---:|---:|---:|---:|---:|---|---|---|",
    ]
    for r in win_rows:
        addr = r["address"]
        dr = conn.execute(
            """
            SELECT probable_device_class, identity_kind FROM ble_devices
            WHERE address = ?
            ORDER BY (last_seen IS NULL) ASC, last_seen DESC LIMIT 1
            """,
            (addr,),
        ).fetchone()
        cls = (dr["probable_device_class"] if dr else None) or "—"
        ik = (dr["identity_kind"] if dr else None) or "—"
        reg = "—"
        if enrich_registry:
            reg = _md_cell(vendor_for_mac(addr, use_http_fallback=True) or "")
        span = r["span_seconds"]
        gap = r["avg_inter_obs_seconds"]
        span_s = f"{float(span):.1f}" if span is not None else "—"
        gap_s = f"{float(gap):.3f}" if gap is not None else "—"
        lines.append(
            f"| `{addr}` | {int(r['obs_rows'] or 0)} | {int(r['distinct_sessions'] or 0)} | "
            f"{int(r['distinct_utc_hours'] or 0)} | {span_s} | {gap_s} | {cls} | {ik} | {reg or '—'} |",
        )
    lines.append("")

    sessions = capture_sessions_rows_for_export(conn, from_date=from_date, to_date=to_date)
    obs_row = conn.execute(
        """
        SELECT COUNT(*) AS n, COUNT(DISTINCT address) AS addr
        FROM ble_observations
        WHERE timestamp IS NOT NULL AND timestamp >= ? AND timestamp <= ?
        """,
        (start, end),
    ).fetchone()
    n_obs = int(obs_row["n"] or 0) if obs_row else 0
    n_addr = int(obs_row["addr"] or 0) if obs_row else 0

    new_d = list(
        conn.execute(
            """
            SELECT address, ledger_id, first_seen, probable_device_class, current_name_hint
            FROM ble_devices
            WHERE first_seen IS NOT NULL AND first_seen >= ? AND first_seen <= ?
            ORDER BY first_seen
            LIMIT 40
            """,
            (start, end),
        ),
    )
    lines += ["## New to the ledger (first seen in this window)", ""]
    if new_d:
        for d in new_d:
            nm = sanitize_ble_display_string(d["current_name_hint"]) or "—"
            lines.append(
                f"- `{d['address']}` — class `{d['probable_device_class'] or '?'}` — name hint `{nm}` — "
                f"first {epoch_to_iso(d['first_seen'])}",
            )
    else:
        lines.append("_No ledger rows with first_seen inside the window._")
    lines.append("")

    returning = list(
        conn.execute(
            """
            SELECT address, ledger_id, first_seen, last_seen, probable_device_class
            FROM ble_devices
            WHERE first_seen IS NOT NULL AND first_seen < ?
              AND last_seen IS NOT NULL AND last_seen >= ? AND last_seen <= ?
            ORDER BY last_seen DESC
            LIMIT 35
            """,
            (start, start, end),
        ),
    )
    lines += [
        "## Seen again this window (ledger first_seen *before* window, activity inside)",
        "",
        "_Useful for “who came back” vs brand-new hardware._",
        "",
    ]
    if returning:
        for d in returning:
            lines.append(
                f"- `{d['address']}` — since {epoch_to_iso(d['first_seen'])} — last {epoch_to_iso(d['last_seen'])} "
                f"— class `{d['probable_device_class'] or '?'}`",
            )
    else:
        lines.append("_None matched (sparse ledger dates or empty overlap)._")
    lines.append("")

    lines += [
        "## Bluetooth SIG hints (company ID & 16-bit service)",
        "",
        "_Decoded `manufacturer_hint` / `service_hint` fields cross-checked with the public "
        "[Bluetooth numbers database](https://github.com/NordicSemiconductor/bluetooth-numbers-database) "
        "(cached locally). Hints can be absent or ambiguous._",
        "",
    ]
    try:
        companies = sig_lookup.distinct_company_labels(conn, start, end, limit=28)
        services = sig_lookup.distinct_service_labels(conn, start, end, limit=28)
    except (OSError, ValueError, TypeError):
        companies, services = [], []
    lines.append("### Company IDs (from advertising manufacturer / company field)")
    if companies:
        lines.append("| Raw hint | Resolved company (SIG) | Sample address |")
        lines.append("|---|---|---|")
        for raw, resolved, addr in companies:
            res = resolved or "_(unresolved)_"
            lines.append(f"| `{_md_cell(raw)}` | {_md_cell(res)} | `{_md_cell(addr or '')}` |")
    else:
        lines.append("_No manufacturer/company hints in this window._")
    lines.append("")
    lines.append("### 16-bit service UUIDs")
    if services:
        lines.append("| Raw hint | Resolved service (GSS) | Sample address |")
        lines.append("|---|---|---|")
        for raw, resolved, addr in services:
            res = resolved or "_(unresolved)_"
            lines.append(f"| `{_md_cell(raw)}` | {_md_cell(res)} | `{_md_cell(addr or '')}` |")
    else:
        lines.append("_No service UUID hints in this window._")
    lines.append("")

    rf_struct: dict = {}
    try:
        rf_lines, rf_struct = rf_inferences.render_rf_inference_markdown(conn, start, end)
        lines += rf_lines
    except Exception as exc:
        logger.warning("RF inferences skipped for window %s–%s: %s", from_date, to_date, exc)
        lines += [
            "## RF-derived inferences (heuristic)",
            "",
            "_Skipped: RF analytics block failed (partial DB, SQLite build limits, or unexpected data). "
            "Core insights above remain valid._",
            "",
        ]

    interp: list[str] = []
    if not sessions and n_obs == 0:
        interp.append(
            "- **Coverage gap:** no overlapping sessions and no observations in-range — widen dates or confirm the Pi importer.",
        )
    elif n_obs and n_addr:
        ratio = n_obs / max(n_addr, 1)
        if ratio < 4:
            interp.append(
                f"- **Sparse airtime per address** (~{ratio:.1f} rows/address): many quiet advertisers, short captures, "
                "or MAC rotation — do not read as “low threat” by itself.",
            )
        elif ratio > 60:
            interp.append(
                f"- **Concentrated traffic** (~{ratio:.0f} rows/address): a few devices dominated the window — "
                "prioritize those rows for vendor enrichment and session drill-down.",
            )
    if new_d and not returning:
        interp.append(
            "- **Mostly new ledger IDs** in-window with few “returning” rows — greenfield capture day or ledger rebuild effect.",
        )
    if returning and len(new_d) > 0 and len(returning) > len(new_d) * 2:
        interp.append(
            "- **Many returning devices** — stable venue population or repeated fixtures vs. transient phones.",
        )
    for flag in ch.get("health_flags") or []:
        if flag == "many_overlap_sessions_with_zero_obs_in_window":
            interp.append(
                "- **Capture / window alignment:** many overlap sessions have **zero** observations in this UTC window — "
                "check whether frame timestamps mostly fall outside the chosen days, or sessions are micro-captures.",
            )
        elif flag == "very_few_packets_per_overlap_session":
            interp.append(
                "- **Low volume vs session count:** few packets per overlap session — typical of short sniffer clips "
                "or a date filter that only grazes your capture span.",
            )
        elif flag == "low_mean_obs_per_active_session":
            interp.append(
                "- **Thin sessions:** low mean rows per session that *do* have data — consider longer runs or "
                "confirming the importer is attaching observations to the expected `session_id`.",
            )
    if rf_struct.get("name_service_mac_clusters"):
        interp.append(
            "- **Name/service → multiple MACs** detected — consider privacy-preserving rotation or a fleet of similar devices.",
        )
    if rf_struct.get("co_presence_top_pairs"):
        interp.append(
            "- **Co-presence pairs** surfaced — validate whether those devices are physically related or only share busy hours.",
        )
    ctx_all = rf_struct.get("address_context") or {}
    if ctx_all.get("distinct_addresses_all_time", 0) > 0 and ctx_all.get("distinct_addresses_in_window", 0) > 0:
        ratio_all = ctx_all["distinct_addresses_in_window"] / max(ctx_all["distinct_addresses_all_time"], 1)
        if ratio_all > 0.35:
            interp.append(
                "- **Large fraction of your all-time MAC set active in this window** — high churn site or a window that "
                "matches heavy collection days.",
            )
    if not interp:
        interp.append(
            "- No strong single-story pattern; use **RF inferences**, **recurrence**, **SIG hints**, and ledger for triage.",
        )
    lines += ["## Automated interpretation (conservative)", ""] + interp + [""]

    lines += [
        "## Next steps (human + tooling)",
        "",
        "- Cross-check **capture health** with **Pi Edge hub** (collector/importer healthy?) and longer capture slices.",
        "- Compare **OUI / company ID / service UUID** columns — disagreement is informative (random MAC, proxy, decode gap).",
        "- Re-import pcaps after upgrading so **appearance / TX power / UUID-128** columns populate (see RF section).",
        "- Export raw CSV tables: `signalhub-ble export assessment-tables --from … --to …` for spreadsheets.",
        "",
    ]

    win_sample = [insight_analytics.row_to_metric_dict(r) for r in win_rows[:18]]
    payload = insight_analytics.insights_metrics_json_blob(
        capture_health=ch,
        baseline_health=baseline_health,
        delta=delta,
        window_rows_sample=win_sample,
        rf_inference=rf_struct if rf_struct else None,
    )
    lines += [
        "",
        "<!-- signalhub-insights-json",
        payload,
        "-->",
    ]
    return "\n".join(lines)


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
