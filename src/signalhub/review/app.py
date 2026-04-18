from __future__ import annotations

import csv
import io
import os
import sqlite3
import tempfile
import uuid
from pathlib import Path

import streamlit as st

from signalhub.ble import assessment_enrichment, reports
from signalhub.common.csv_export import row_dict_for_csv
from signalhub.common.timeutil import epoch_to_iso, utc_day_range_epoch, utc_today_iso
from signalhub.config import db_path as default_db_path, load_dotenv_files
from signalhub.review.helpers import (
    db_capabilities,
    is_safe_select,
    open_dashboard_connection,
    rows_to_dicts,
    table_exists,
)


def _env_or_secret(*names: str) -> str | None:
    """Prefer process env (local .env); on Streamlit Cloud use Secrets."""
    for n in names:
        v = os.environ.get(n, "").strip()
        if v:
            return v
    try:
        sec = st.secrets
        for n in names:
            if n in sec:
                out = str(sec[n]).strip()
                if out:
                    return out
    except Exception:
        pass
    return None


def _sidebar_db_path() -> tuple[Path, bool] | None:
    load_dotenv_files()
    st.sidebar.markdown("### Database")
    uploaded = st.sidebar.file_uploader(
        "Upload .sqlite (needed on Streamlit Cloud)",
        type=["sqlite", "db", "sqlite3"],
        help="Session-only temp copy. Locally you can use a path instead.",
    )
    if uploaded is not None:
        tmp = Path(tempfile.gettempdir()) / f"signalhub_{uuid.uuid4().hex}.sqlite"
        tmp.write_bytes(uploaded.getvalue())
        st.sidebar.caption(f"Uploaded → `{tmp}`")
        if st.sidebar.button("Clear connection cache"):
            get_connection.clear()
        return tmp, False

    env_review = os.environ.get("SIGNALHUB_REVIEW_DB", "").strip()
    env_main = os.environ.get("SIGNALHUB_DB", "").strip()
    default_p = env_review or env_main or str(default_db_path())
    readonly = st.sidebar.checkbox(
        "Open read-only",
        value=False,
        help="Uses SQLite URI mode=ro (fails if another writer locks the file).",
    )
    path_in = st.sidebar.text_input(
        "SQLite path (local / server filesystem)",
        value=default_p,
        help="Main DB or `export review-db` copy. On Community Cloud this path is usually empty unless you mount storage.",
    )
    if st.sidebar.button("Clear connection cache"):
        get_connection.clear()
    if not path_in.strip():
        return None
    p = Path(path_in).expanduser()
    if not p.is_file():
        st.sidebar.error(f"Not a file: {p}")
        return None
    st.sidebar.caption(f"Resolved: `{p.resolve()}`")
    return p, readonly


@st.cache_resource
def get_connection(db_path_str: str, readonly: bool) -> sqlite3.Connection:
    return open_dashboard_connection(Path(db_path_str), readonly=readonly)


def _tab_overview(conn: sqlite3.Connection, caps: dict) -> None:
    st.subheader("Overview")
    mode = caps.get("mode", "?")
    st.caption(f"Database mode: **{mode}** — {'raw observations present' if mode == 'full' else 'stripped copy (use full DB for observation-level reports).'}")

    c1, c2, c3, c4 = st.columns(4)
    if table_exists(conn, "ble_devices"):
        n_dev = conn.execute("SELECT COUNT(*) AS n FROM ble_devices").fetchone()["n"]
        c1.metric("Ledger devices", int(n_dev))
    else:
        c1.metric("Ledger devices", "—")
    if table_exists(conn, "capture_sessions"):
        n_s = conn.execute("SELECT COUNT(*) AS n FROM capture_sessions").fetchone()["n"]
        c2.metric("Capture sessions", int(n_s))
    else:
        c2.metric("Capture sessions", "—")
    if caps["ble_observations"]:
        row = conn.execute("SELECT COUNT(*) AS n FROM ble_observations").fetchone()
        c3.metric("Observation rows", int(row["n"]))
        row2 = conn.execute(
            "SELECT MIN(timestamp) AS a, MAX(timestamp) AS b FROM ble_observations WHERE timestamp IS NOT NULL",
        ).fetchone()
        if row2["a"] is not None and row2["b"] is not None:
            c4.metric("Frame span (days)", f"{max(0.0, (row2['b'] - row2['a']) / 86400.0):.2f}")
            st.caption(
                f"Oldest frame: `{epoch_to_iso(row2['a'])}` — Newest: `{epoch_to_iso(row2['b'])}`"
            )
        else:
            c4.metric("Frame span", "—")
    else:
        c3.metric("Observation rows", "0 (stripped)")
        c4.metric("Frame time span", "N/A")

    if caps["session_stats"]:
        st.markdown("#### Session rollup (`session_stats`)")
        agg = conn.execute(
            """
            SELECT COALESCE(SUM(observation_count), 0) AS packets,
                   COUNT(*) AS sessions_with_stats
            FROM session_stats
            """
        ).fetchone()
        st.dataframe(rows_to_dicts([agg]), hide_index=True)

    st.markdown("#### Quick assessment (UTC date range)")
    col_a, col_b = st.columns(2)
    today = utc_today_iso()
    from_d = col_a.text_input("From (YYYY-MM-DD)", value=today, key="ov_from")
    to_d = col_b.text_input("To (YYYY-MM-DD)", value=today, key="ov_to")
    if caps["ble_observations"]:
        try:
            md = reports.render_assessment_report(conn, from_d.strip(), to_d.strip())
        except ValueError as e:
            st.error(str(e))
            return
        st.download_button(
            "Download assessment (.md)",
            data=md,
            file_name=f"assessment-{from_d}-{to_d}.md",
            mime="text/markdown",
        )
        st.markdown(md)
    else:
        st.info("Open the **full** SQLite (not a review-only export) to generate packet-level assessment markdown here.")


def _tab_sessions(conn: sqlite3.Connection, caps: dict) -> None:
    st.subheader("Capture sessions")
    if not caps["capture_sessions"]:
        st.warning("No `capture_sessions` table.")
        return
    sensor_f = st.text_input("Filter sensor_id contains", value="")
    lim = st.number_input("Row limit", min_value=50, max_value=5000, value=500, step=50)
    has_stats = caps["session_stats"]
    filter_sql = ""
    params: list = []
    if sensor_f.strip():
        filter_sql = " AND s.sensor_id LIKE ?" if has_stats else " AND sensor_id LIKE ?"
        params.append(f"%{sensor_f.strip()}%")
    params.append(int(lim))
    if has_stats:
        q = f"""
        SELECT s.*, IFNULL(st.observation_count, 0) AS observation_count,
               IFNULL(st.distinct_addresses, 0) AS distinct_addresses
        FROM capture_sessions s
        LEFT JOIN session_stats st ON st.session_id = s.session_id
        WHERE 1=1 {filter_sql}
        ORDER BY s.imported_at DESC
        LIMIT ?
        """
    else:
        q = f"""
        SELECT * FROM capture_sessions
        WHERE 1=1 {filter_sql}
        ORDER BY imported_at DESC
        LIMIT ?
        """

    rows = conn.execute(q, params).fetchall()
    st.metric("Rows", len(rows))
    st.dataframe(rows_to_dicts(rows), use_container_width=True, height=360)

    st.markdown("#### Session report")
    sid = st.text_input("session_id (full UUID)", value="", key="sess_report_id")
    if sid.strip() and st.button("Render session markdown report"):
        if not caps["ble_observations"]:
            st.warning("Session reports need `ble_observations` (full DB).")
        else:
            st.markdown(reports.render_session_report(conn, sid.strip()))


def _tab_ledger(conn: sqlite3.Connection, caps: dict) -> None:
    st.subheader("Ledger (`ble_devices`)")
    if not caps["ble_devices"]:
        st.warning("No ledger table.")
        return
    has_ik = caps.get("has_identity_kind")
    kind = st.selectbox("identity_kind", ["(all)", "mac", "fingerprint"], key="ledger_kind")
    name_q = st.text_input("Name contains", value="", key="ledger_name")
    lim = st.number_input("Row limit", min_value=50, max_value=10000, value=2000, step=100, key="ledger_lim")
    q = "SELECT * FROM ble_devices WHERE 1=1"
    params: list = []
    if has_ik and kind != "(all)":
        q += " AND identity_kind = ?"
        params.append(kind)
    if name_q.strip():
        q += " AND current_name_hint LIKE ?"
        params.append(f"%{name_q.strip()}%")
    q += " ORDER BY (last_seen IS NULL), last_seen DESC LIMIT ?"
    params.append(int(lim))
    dev_rows = conn.execute(q, params).fetchall()
    st.metric("Devices shown", len(dev_rows))
    st.dataframe(rows_to_dicts(dev_rows), use_container_width=True, height=420)

    st.markdown("#### Aliases for a ledger_id")
    lid = st.text_input("ledger_id", value="", key="ledger_alias_id")
    if lid.strip() and caps["ble_aliases"]:
        ar = conn.execute(
            "SELECT * FROM ble_aliases WHERE ledger_id = ? ORDER BY alias_type, id",
            (lid.strip(),),
        ).fetchall()
        st.dataframe(rows_to_dicts(ar), use_container_width=True)


def _tab_summaries(conn: sqlite3.Connection, caps: dict) -> None:
    st.subheader("Per-session address summaries (`ble_device_session_summary`)")
    if not caps["ble_device_session_summary"]:
        st.warning("No summary table.")
        return
    sid = st.text_input("Filter session_id (substring)", value="")
    addr = st.text_input("Filter address (substring)", value="")
    lim = st.number_input("Row limit", min_value=50, max_value=20000, value=3000, step=100, key="sum_lim")
    q = "SELECT * FROM ble_device_session_summary WHERE 1=1"
    params: list = []
    if sid.strip():
        q += " AND session_id LIKE ?"
        params.append(f"%{sid.strip()}%")
    if addr.strip():
        q += " AND address LIKE ?"
        params.append(f"%{addr.strip()}%")
    q += " ORDER BY id DESC LIMIT ?"
    params.append(int(lim))
    rows = conn.execute(q, params).fetchall()
    st.metric("Rows", len(rows))
    st.dataframe(rows_to_dicts(rows), use_container_width=True, height=460)


def _tab_observations(conn: sqlite3.Connection, caps: dict) -> None:
    st.subheader("Observations (`ble_observations`)")
    if not caps["ble_observations"]:
        st.info("This database has no observation table — use your main `signalhub.sqlite` or re-import captures.")
        return
    st.caption("Large queries can be slow; keep limits modest.")
    col1, col2, col3 = st.columns(3)
    from_d = col1.text_input("From date UTC", value="", key="obs_from", help="YYYY-MM-DD optional window")
    to_d = col2.text_input("To date UTC", value="", key="obs_to")
    sid = col3.text_input("session_id filter (substring)", value="")
    lim = st.number_input("Max rows", min_value=100, max_value=50000, value=5000, step=500, key="obs_lim")
    q = "SELECT * FROM ble_observations WHERE 1=1"
    params: list = []
    if from_d.strip() and to_d.strip():
        try:
            start, end = utc_day_range_epoch(from_d.strip(), to_d.strip())
        except ValueError as e:
            st.error(str(e))
            return
        q += " AND timestamp IS NOT NULL AND timestamp >= ? AND timestamp <= ?"
        params.extend([start, end])
    if sid.strip():
        q += " AND session_id LIKE ?"
        params.append(f"%{sid.strip()}%")
    q += " ORDER BY observation_id DESC LIMIT ?"
    params.append(int(lim))
    rows = conn.execute(q, params).fetchall()
    st.metric("Rows loaded", len(rows))
    st.dataframe(rows_to_dicts(rows), use_container_width=True, height=480)

    if from_d.strip() and to_d.strip() and st.button("Prepare observations CSV (full UTC window)", key="obs_csv_prep"):
        try:
            export_rows = reports.ble_observations_rows_for_export(
                conn, from_date=from_d.strip(), to_date=to_d.strip()
            )
        except ValueError as e:
            st.error(str(e))
        else:
            buf = io.StringIO()
            if export_rows:
                w = csv.DictWriter(buf, fieldnames=list(export_rows[0].keys()))
                w.writeheader()
                for r in export_rows:
                    w.writerow(row_dict_for_csv(r))
            st.session_state["_obs_csv_data"] = buf.getvalue()
            st.session_state["_obs_csv_name"] = f"observations-{from_d}-{to_d}.csv"
    if st.session_state.get("_obs_csv_data"):
        st.download_button(
            "Download observations CSV",
            data=st.session_state["_obs_csv_data"],
            file_name=st.session_state.get("_obs_csv_name", "observations.csv"),
            mime="text/csv",
            key="obs_csv_dl",
        )


def _tab_meta(conn: sqlite3.Connection, caps: dict) -> None:
    st.subheader("Sensors, aliases, schema meta")
    if caps["sensors"]:
        st.markdown("#### `sensors`")
        st.dataframe(
            rows_to_dicts(conn.execute("SELECT * FROM sensors ORDER BY sensor_id").fetchall()),
            use_container_width=True,
        )
    if caps["ble_aliases"]:
        st.markdown("#### `ble_aliases` (recent)")
        lim = st.number_input("Limit", min_value=100, max_value=20000, value=800, key="alias_lim")
        st.dataframe(
            rows_to_dicts(
                conn.execute(
                    f"SELECT * FROM ble_aliases ORDER BY id DESC LIMIT {int(lim)}",
                ).fetchall(),
            ),
            use_container_width=True,
            height=320,
        )
    if caps["schema_meta"]:
        st.markdown("#### `schema_meta`")
        st.dataframe(
            rows_to_dicts(conn.execute("SELECT * FROM schema_meta").fetchall()),
            hide_index=True,
        )


def _tab_reports_ai(conn: sqlite3.Connection, caps: dict) -> None:
    st.subheader("Markdown reports & AI")
    r1, r2 = st.columns(2)
    from_d = r1.text_input("From (UTC)", value=utc_today_iso(), key="rep_from")
    to_d = r2.text_input("To (UTC)", value=utc_today_iso(), key="rep_to")

    t1, t2, t3, t4 = st.tabs(["Assessment", "Change report", "Ledger report", "AI narrative"])

    with t1:
        if not caps["ble_observations"]:
            st.warning("Full assessment needs `ble_observations`.")
        else:
            if st.button("Generate assessment markdown", key="btn_assess"):
                try:
                    st.session_state["_assess_md"] = reports.render_assessment_report(
                        conn, from_d.strip(), to_d.strip()
                    )
                except ValueError as e:
                    st.error(str(e))
            if "_assess_md" in st.session_state:
                st.download_button(
                    "Download",
                    st.session_state["_assess_md"],
                    file_name=f"assessment-{from_d}-{to_d}.md",
                    mime="text/markdown",
                )
                st.markdown(st.session_state["_assess_md"])

    with t2:
        if not caps["ble_observations"]:
            st.warning("Change report uses observation timestamps in range.")
        elif st.button("Generate change report", key="btn_change"):
            try:
                st.session_state["_change_md"] = reports.render_change_report(
                    conn, from_d.strip(), to_d.strip()
                )
            except ValueError as e:
                st.error(str(e))
        if "_change_md" in st.session_state:
            st.download_button(
                "Download",
                st.session_state["_change_md"],
                file_name=f"change-{from_d}-{to_d}.md",
                mime="text/markdown",
            )
            st.markdown(st.session_state["_change_md"])

    with t3:
        if st.button("Generate full ledger report", key="btn_ledger"):
            st.session_state["_ledger_md"] = reports.render_ledger_report(conn)
        if "_ledger_md" in st.session_state:
            st.download_button(
                "Download",
                st.session_state["_ledger_md"],
                file_name="ledger-report.md",
                mime="text/markdown",
            )
            st.markdown(st.session_state["_ledger_md"])

    with t4:
        st.markdown(
            "Keys: local `.env`, or Streamlit **Secrets** (`OPENAI_API_KEY` / `SIGNALHUB_OPENAI_API_KEY`)."
        )
        model = st.text_input(
            "Model",
            value=_env_or_secret("SIGNALHUB_AI_MODEL") or "gpt-4o-mini",
        )
        base_url = st.text_input(
            "Base URL",
            value=_env_or_secret("SIGNALHUB_OPENAI_BASE_URL") or "https://api.openai.com/v1",
        )
        baseline = st.text_area("Optional baseline assessment markdown (for diff text in prompt)", height=120)
        if not caps["ble_observations"]:
            st.error("AI tab builds on the deterministic follow-up appendix, which requires observation counts.")
        elif st.button("Run AI analysis (window + follow-up appendix)", key="btn_ai"):
            api_key = _env_or_secret("SIGNALHUB_OPENAI_API_KEY", "OPENAI_API_KEY")
            if not api_key:
                st.error("No API key (set in environment or Streamlit Secrets).")
            else:
                try:
                    main_md = reports.render_assessment_report(conn, from_d.strip(), to_d.strip())
                except ValueError as e:
                    st.error(str(e))
                    return
                appendix = assessment_enrichment.render_follow_up_appendix(
                    conn,
                    from_date=from_d.strip(),
                    to_date=to_d.strip(),
                    baseline_markdown=baseline.strip() or None,
                    ai_narrative=None,
                )
                if baseline.strip():
                    p = assessment_enrichment.parse_previous_assessment(baseline)
                    bsum = (
                        f"Parsed baseline file: range {p.utc_from}→{p.utc_to}, "
                        f"sessions={p.session_count}, packets={p.packet_count}, addrs={p.distinct_addresses}."
                    )
                else:
                    bsum = "No baseline markdown provided."
                excerpt = appendix[:12000] + ("…" if len(appendix) > 12000 else "")
                user_prompt = assessment_enrichment.build_ai_user_prompt(
                    from_date=from_d.strip(),
                    to_date=to_d.strip(),
                    baseline_summary=bsum,
                    deterministic_appendix_excerpt=excerpt,
                )
                try:
                    narr = assessment_enrichment.fetch_openai_narrative(
                        user_prompt=user_prompt,
                        api_key=api_key,
                        model=model.strip(),
                        base_url=base_url.strip(),
                    )
                except Exception as e:
                    st.exception(e)
                else:
                    full = main_md.rstrip() + "\n\n" + appendix.rstrip() + "\n\n### AI narrative\n\n" + narr
                    st.session_state["_ai_full_md"] = full
                    st.success("Done.")
        if "_ai_full_md" in st.session_state:
            st.download_button(
                "Download combined markdown",
                st.session_state["_ai_full_md"],
                file_name=f"assessment-with-ai-{from_d}-{to_d}.md",
                mime="text/markdown",
            )
            st.markdown(st.session_state["_ai_full_md"])


def _tab_sql(conn: sqlite3.Connection) -> None:
    st.subheader("Read-only SQL")
    st.caption("Only `SELECT` / `WITH` statements; dangerous keywords rejected.")
    q = st.text_area("SQL", height=140, placeholder="SELECT * FROM ble_devices LIMIT 20")
    lim = st.number_input("Max rows to display", 10, 5000, 500, key="sql_disp")
    if st.button("Run", key="sql_run"):
        if not is_safe_select(q):
            st.error("Only read-only SELECT/WITH queries are allowed.")
        else:
            try:
                cur = conn.execute(q)
                rows = cur.fetchmany(int(lim) + 1)
            except Exception as e:
                st.exception(e)
            else:
                if len(rows) > int(lim):
                    st.warning(f"Truncated to {lim} rows.")
                    rows = rows[: int(lim)]
                if cur.description:
                    st.dataframe(rows_to_dicts(rows), use_container_width=True, height=400)
                else:
                    st.info("No result set.")


def main() -> None:
    load_dotenv_files()
    st.set_page_config(page_title="Signalhub dashboard", layout="wide", initial_sidebar_state="expanded")
    st.title("Signalhub BLE dashboard")
    st.caption("Browse SQLite captures, ledger, and optional AI narrative — pair with `signalhub-ble` CLI for imports and rebuilds.")

    side = _sidebar_db_path()
    if side is None:
        st.info(
            "**Database:** upload a `.sqlite` file in the sidebar (required on **Streamlit Community Cloud**), "
            "or set a local **SQLite path** / `SIGNALHUB_DB`."
        )
        return
    db_path, readonly = side

    try:
        conn = get_connection(str(db_path.resolve()), readonly)
    except sqlite3.Error as e:
        st.error(f"Could not open database: {e}")
        return

    caps = db_capabilities(conn)
    st.sidebar.markdown("---")
    st.sidebar.markdown(f"**Tables detected:** {'full' if caps['ble_observations'] else 'review/stripped'}")

    tabs = st.tabs(
        [
            "Overview",
            "Sessions",
            "Ledger",
            "Summaries",
            "Observations",
            "Sensors & aliases",
            "Reports & AI",
            "SQL",
        ]
    )
    with tabs[0]:
        _tab_overview(conn, caps)
    with tabs[1]:
        _tab_sessions(conn, caps)
    with tabs[2]:
        _tab_ledger(conn, caps)
    with tabs[3]:
        _tab_summaries(conn, caps)
    with tabs[4]:
        _tab_observations(conn, caps)
    with tabs[5]:
        _tab_meta(conn, caps)
    with tabs[6]:
        _tab_reports_ai(conn, caps)
    with tabs[7]:
        _tab_sql(conn)


# Community Cloud: prefer repo-root ``streamlit_app.py`` as the Streamlit "Main file" (see README).
if __name__ == "__main__":
    main()
