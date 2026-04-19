from __future__ import annotations

import json
import re
import sqlite3
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from signalhub.common.timeutil import epoch_to_iso, utc_day_range_epoch
from signalhub.db.sqlite import now_epoch


@dataclass(frozen=True)
class ParsedAssessment:
    utc_from: str | None
    utc_to: str | None
    session_count: int | None
    packet_count: int | None
    distinct_addresses: int | None


def parse_previous_assessment(markdown: str) -> ParsedAssessment:
    """Best-effort parse of `report assess` output for numeric diffing."""
    r_from = r_to = None
    m = re.search(
        r"\*\*UTC range:\*\* `(\d{4}-\d{2}-\d{2})` → `(\d{4}-\d{2}-\d{2})`",
        markdown,
    )
    if m:
        r_from, r_to = m.group(1), m.group(2)
    sess = pkt = addr = None
    m2 = re.search(r"## Capture sessions.*?\*\*Count:\*\* (\d+)", markdown, re.DOTALL)
    if m2:
        sess = int(m2.group(1))
    m3 = re.search(
        r"\*\*Packets:\*\* (\d+).*?\*\*Distinct addresses:\*\* (\d+)",
        markdown,
        re.DOTALL,
    )
    if m3:
        pkt, addr = int(m3.group(1)), int(m3.group(2))
    return ParsedAssessment(r_from, r_to, sess, pkt, addr)


def extract_insights_json_block(markdown: str) -> str | None:
    """Return JSON inside `<!-- signalhub-insights-json ... -->` if present and valid."""
    m = re.search(
        r"<!--\s*signalhub-insights-json\s*(.*?)\s*-->",
        markdown,
        flags=re.DOTALL | re.IGNORECASE,
    )
    if not m:
        return None
    raw = m.group(1).strip()
    try:
        json.loads(raw)
    except json.JSONDecodeError:
        return None
    return raw


def strip_insights_json_block(markdown: str) -> str:
    """Remove the hidden machine-metrics comment for human/LLM excerpts."""
    return re.sub(
        r"<!--\s*signalhub-insights-json\s*.*?\s*-->",
        "",
        markdown,
        flags=re.DOTALL | re.IGNORECASE,
    ).strip()


def _global_db_stats(conn: sqlite3.Connection) -> dict[str, Any]:
    t_now = now_epoch()
    since_24h = t_now - 86400.0
    row_obs = conn.execute(
        "SELECT COUNT(*) AS n, MAX(timestamp) AS ts_max, MIN(timestamp) AS ts_min FROM ble_observations",
    ).fetchone()
    row_sess = conn.execute(
        "SELECT COUNT(*) AS n, MAX(imported_at) AS imp_max FROM capture_sessions",
    ).fetchone()
    row_dev = conn.execute("SELECT COUNT(*) AS n FROM ble_devices").fetchone()
    obs_24h = conn.execute(
        """
        SELECT COUNT(*) AS n FROM ble_observations
        WHERE timestamp IS NOT NULL AND timestamp >= ?
        """,
        (since_24h,),
    ).fetchone()
    imp_sess_24h = conn.execute(
        """
        SELECT COUNT(*) AS n FROM capture_sessions WHERE imported_at >= ?
        """,
        (since_24h,),
    ).fetchone()
    obs_rows_24h = conn.execute(
        """
        SELECT COUNT(*) AS n FROM ble_observations o
        JOIN capture_sessions s ON s.session_id = o.session_id
        WHERE s.imported_at >= ?
        """,
        (since_24h,),
    ).fetchone()
    return {
        "total_observations": int(row_obs["n"] or 0),
        "total_sessions": int(row_sess["n"] or 0),
        "total_ledger_devices": int(row_dev["n"] or 0),
        "obs_earliest_ts": row_obs["ts_min"],
        "obs_latest_ts": row_obs["ts_max"],
        "session_latest_import": row_sess["imp_max"],
        "obs_frame_time_last_24h": int(obs_24h["n"] or 0),
        "sessions_imported_last_24h": int(imp_sess_24h["n"] or 0),
        "observation_rows_from_sessions_imported_last_24h": int(obs_rows_24h["n"] or 0),
        "wall_clock_now_epoch": t_now,
    }


def render_follow_up_appendix(
    conn: sqlite3.Connection,
    *,
    from_date: str,
    to_date: str,
    baseline_markdown: str | None,
    ai_narrative: str | None,
) -> str:
    """Markdown block appended after the main assessment (deterministic + optional AI)."""
    start, end = utc_day_range_epoch(from_date, to_date)
    lines = [
        "---",
        "",
        "## Assessment follow-up (baseline + database)",
        "",
        f"_Generated at UTC {epoch_to_iso(now_epoch())} — compares this run’s window to an optional prior report and to the whole SQLite database._",
        "",
        "### Why totals may stay flat",
        "",
        "The **Packets** figure counts `ble_observations` rows whose **frame timestamp** falls inside "
        f"`{from_date}` → `{to_date}` (UTC inclusive days). If you use the **same dates** and no new "
        "frames fall in that window (for example new airtime is entirely on the **next** UTC day), "
        "the number will **not** change. Extend `--to` through **today (UTC)** to include fresh days.",
        "",
    ]

    cur_sess = conn.execute(
        """
        SELECT COUNT(*) AS n FROM capture_sessions
        WHERE COALESCE(started_at, imported_at) <= ?
          AND COALESCE(ended_at, started_at, imported_at) >= ?
        """,
        (end, start),
    ).fetchone()
    cur_sess_n = int(cur_sess["n"] or 0)
    cur_obs = conn.execute(
        """
        SELECT COUNT(*) AS n, COUNT(DISTINCT address) AS addr
        FROM ble_observations
        WHERE timestamp IS NOT NULL AND timestamp >= ? AND timestamp <= ?
        """,
        (start, end),
    ).fetchone()
    cur_pkt = int(cur_obs["n"] or 0)
    cur_addr = int(cur_obs["addr"] or 0)

    if baseline_markdown:
        prev = parse_previous_assessment(baseline_markdown)
        lines += ["### Compared to previous `assessment.md`", ""]
        lines.append("| Metric | Previous file | This run (same window) | Δ |")
        lines.append("|---|---:|---:|---:|")

        def row(label: str, a: int | None, b: int) -> None:
            if a is None:
                lines.append(f"| {label} | _(unparsed)_ | {b} | — |")
            else:
                lines.append(f"| {label} | {a} | {b} | {b - a:+d} |")

        row("Capture sessions (overlap)", prev.session_count, cur_sess_n)
        row("Packets in window", prev.packet_count, cur_pkt)
        row("Distinct addresses", prev.distinct_addresses, cur_addr)
        if prev.utc_from and prev.utc_to and (prev.utc_from, prev.utc_to) != (from_date, to_date):
            lines.append("")
            lines.append(
                f"_Note: previous file’s stated range was `{prev.utc_from}` → `{prev.utc_to}`; "
                f"this run used `{from_date}` → `{to_date}` — compare numbers only if ranges match._",
            )
        lines.append("")
    else:
        lines += [
            "### Compared to previous `assessment.md`",
            "",
            "_No `--baseline` file provided — skipped file-to-file diff._",
            "",
        ]

    g = _global_db_stats(conn)
    lines += [
        "### Database snapshot (all imported data)",
        "",
        f"- **Total observation rows:** {g['total_observations']}",
        f"- **Total capture sessions:** {g['total_sessions']}",
        f"- **Ledger devices (`ble_devices`):** {g['total_ledger_devices']}",
        f"- **Newest frame timestamp in DB:** {epoch_to_iso(g['obs_latest_ts']) if g['obs_latest_ts'] else '—'}",
        f"- **Oldest frame timestamp in DB:** {epoch_to_iso(g['obs_earliest_ts']) if g['obs_earliest_ts'] else '—'}",
        f"- **Newest session `imported_at`:** {epoch_to_iso(g['session_latest_import']) if g['session_latest_import'] else '—'}",
        "",
        "### Last ~24 hours (wall clock, UTC-based epoch)",
        "",
        f"- **Sessions imported** (`imported_at` in last 24h): **{g['sessions_imported_last_24h']}**",
        f"- **Observation rows** whose frames fall in last 24h: **{g['obs_frame_time_last_24h']}**",
        f"- **Observation rows** tied to sessions imported in last 24h: **{g['observation_rows_from_sessions_imported_last_24h']}**",
        "",
    ]
    if g["sessions_imported_last_24h"] == 0 and g["obs_frame_time_last_24h"] == 0:
        lines += [
            "> **Heads-up:** No imports and no frame timestamps in the last 24h — the Pi may be idle, "
            "off-air, or the clock/import pipeline may need checking (not definitive).",
            "",
        ]

    if ai_narrative:
        lines += [
            "### AI narrative (experimental)",
            "",
            "_Model-generated from the metrics above; verify against the database._",
            "",
            ai_narrative.strip(),
            "",
        ]

    return "\n".join(lines)


def fetch_openai_narrative(
    *,
    user_prompt: str,
    api_key: str,
    model: str = "gpt-4o-mini",
    base_url: str = "https://api.openai.com/v1",
) -> str:
    """Call Chat Completions API (OpenAI or compatible). Raises on HTTP/API errors."""
    url = base_url.rstrip("/") + "/chat/completions"
    body = json.dumps(
        {
            "model": model,
            "temperature": 0.25,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You are a senior BLE/RF analyst. Do NOT summarize tables or repeat raw statistics "
                        "that already appear in the user message. Instead: explain what the patterns likely mean "
                        "for operators (venue vs transient devices, beacon vs connectable traffic, import health), "
                        "call out uncertainty (MAC randomization, decode gaps, sparse captures), and give 4–7 concrete "
                        "next actions (what to verify on the sniffer host, what longer capture window to try, "
                        "which addresses deserve physical correlation). Use short bullets. Never invent counts—"
                        "only reference numbers explicitly present in the user text. When JSON appears, treat it as "
                        "structured facts (capture_health, baseline deltas, top device recurrence) — interpret, "
                        "do not paste it back."
                    ),
                },
                {"role": "user", "content": user_prompt},
            ],
        },
    ).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"OpenAI HTTP {e.code}: {detail}") from e
    try:
        return str(payload["choices"][0]["message"]["content"]).strip()
    except (KeyError, IndexError, TypeError) as e:
        raise RuntimeError(f"Unexpected API response: {payload!r}") from e


def build_ai_user_prompt(
    *,
    from_date: str,
    to_date: str,
    baseline_summary: str,
    deterministic_appendix_excerpt: str,
) -> str:
    return (
        f"Assessment window: {from_date} to {to_date} (UTC inclusive days).\n\n"
        f"{baseline_summary}\n\n"
        "Context (deterministic metrics — do not restate verbatim as a table; use them only to ground reasoning):\n"
        f"{deterministic_appendix_excerpt}\n\n"
        "Respond with: (1) What this environment probably looks like operationally, "
        "(2) which risks of mis-read exist, (3) prioritized next steps for the analyst."
    )


def build_insights_ai_user_prompt(
    *,
    from_date: str,
    to_date: str,
    insights_excerpt: str,
    appendix_excerpt: str,
    structured_metrics_json: str | None = None,
) -> str:
    """Prompt for interpretation after the consolidated insights markdown."""
    parts = [
        f"UTC window: {from_date} → {to_date}.",
        "",
        "### Consolidated insights (excerpt)",
        insights_excerpt,
        "",
        "### Extra metrics (excerpt)",
        appendix_excerpt,
    ]
    if structured_metrics_json:
        parts += [
            "",
            "### Structured metrics (JSON — facts for reasoning, not for quoting verbatim)",
            structured_metrics_json[:14000],
        ]
    parts += [
        "",
        "Task: interpret what this means for someone securing or auditing a BLE-rich site. "
        "Avoid repeating markdown tables. Focus on implications, blind spots, baseline deltas vs capture-health "
        "flags, recurrence patterns, and a short checklist of follow-ups.",
    ]
    return "\n".join(parts)


def read_baseline(path: Path | None) -> str | None:
    if path is None:
        return None
    return path.read_text(encoding="utf-8", errors="replace")
