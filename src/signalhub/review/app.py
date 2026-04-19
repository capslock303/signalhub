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
from signalhub.review import pi_db_sync, pi_status
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


def _nav_definitions() -> list[tuple[str, str]]:
    return [
        ("edge", "Edge hub"),
        ("overview", "Overview"),
        ("sessions", "Sessions"),
        ("ledger", "Ledger"),
        ("summaries", "Session summaries"),
        ("observations", "Observations"),
        ("meta", "Sensors & aliases"),
        ("reports", "Insights & AI"),
        ("sql", "SQL explorer"),
    ]


def _sidebar_pi_config() -> dict[str, str]:
    with st.sidebar.expander("🔌 Edge hub (kpi) — SSH", expanded=False):
        st.caption(
            "SSH to the Pi for **Edge hub** diagnostics and for **scp** database sync. "
            "Requires `ssh` / `scp` on this PC (OpenSSH Client)."
        )
        host = st.text_input(
            "Host",
            value=_env_or_secret("SIGNALHUB_PI_HOST") or "192.168.8.112",
            key="pi_ssh_host",
        )
        user = st.text_input(
            "User",
            value=_env_or_secret("SIGNALHUB_PI_USER") or "kpi",
            key="pi_ssh_user",
        )
        id_def = _env_or_secret("SIGNALHUB_PI_SSH_IDENTITY") or ""
        identity = st.text_input(
            "Identity file (optional)",
            value=id_def,
            placeholder=r"C:\Users\you\.ssh\id_ed25519",
            key="pi_ssh_id",
        )
        ble_root = st.text_input(
            "Remote BLE root",
            value=os.environ.get("SIGNALHUB_PI_BLE_ROOT", "/home/kpi/ble"),
            key="pi_ble_root",
        )
    return {
        "host": host,
        "user": user,
        "identity": identity,
        "ble_root": ble_root,
    }


def _auto_sync_pref_from_env() -> bool:
    v = (_env_or_secret("SIGNALHUB_AUTO_SYNC_PI_DB") or "").strip().lower()
    return v in ("1", "true", "yes", "on")


def _sidebar_database_section(pi_cfg: dict[str, str]) -> tuple[Path, bool] | None:
    ble = (pi_cfg.get("ble_root") or "/home/kpi/ble").strip()
    remote_default = (
        _env_or_secret("SIGNALHUB_PI_REMOTE_DB") or pi_db_sync.default_remote_db_path(ble)
    )

    with st.sidebar.expander("💾 Database", expanded=True):
        st.checkbox(
            "Sync database from Pi when the app starts",
            key="auto_sync_on_start",
            help="Runs once per browser session (uses scp; SSH key recommended). You can also set SIGNALHUB_AUTO_SYNC_PI_DB=1 in .env.",
        )
        if "pi_remote_db_path" not in st.session_state:
            st.session_state["pi_remote_db_path"] = remote_default
        st.text_input(
            "Remote SQLite path (on Pi)",
            key="pi_remote_db_path",
            help="Usually …/ble/data/db/signalhub.sqlite on the Pi.",
        )
        if st.button("⬇️ Pull latest DB from Pi now", type="secondary", key="pi_pull_db_now"):
            host = (pi_cfg.get("host") or "").strip()
            user = (pi_cfg.get("user") or "").strip()
            ident = (pi_cfg.get("identity") or "").strip() or None
            remote = (st.session_state.get("pi_remote_db_path") or remote_default).strip()
            if not host or not user:
                st.session_state["pi_sync_toast"] = "Set Pi **Host** and **User** in the SSH expander above."
            else:
                local_s = (_env_or_secret("SIGNALHUB_PI_LOCAL_DB_CACHE") or "").strip()
                local = Path(local_s).expanduser() if local_s else pi_db_sync.default_local_cache_path()
                with st.spinner("scp from Pi…"):
                    res = pi_db_sync.pull_pi_sqlite(
                        host=host,
                        user=user,
                        remote_path=remote,
                        local_path=local,
                        identity_file=ident,
                        non_interactive=True,
                    )
                if res.ok and res.local_path is not None:
                    st.session_state["db_path_in"] = str(res.local_path.resolve())
                    get_connection.clear()
                    st.session_state["pi_sync_toast"] = f"Pulled Pi DB → `{res.local_path.name}`"
                else:
                    st.session_state["pi_sync_toast"] = f"Pull failed: {res.message}"

        uploaded = st.file_uploader(
            "Upload .sqlite",
            type=["sqlite", "db", "sqlite3"],
            help="Cloud or quick open. Session temp file.",
        )
        if uploaded is not None:
            tmp = Path(tempfile.gettempdir()) / f"signalhub_{uuid.uuid4().hex}.sqlite"
            tmp.write_bytes(uploaded.getvalue())
            st.caption(f"Using upload → `{tmp.name}`")
            if st.button("Clear connection cache", key="db_clear_upload"):
                get_connection.clear()
            return tmp, False

        env_review = os.environ.get("SIGNALHUB_REVIEW_DB", "").strip()
        env_main = os.environ.get("SIGNALHUB_DB", "").strip()
        candidate = env_review or env_main or str(default_db_path())
        path_default = ""
        if candidate.strip():
            cand_path = Path(candidate).expanduser()
            if cand_path.is_file():
                path_default = str(cand_path.resolve())
        if "db_path_in" not in st.session_state:
            st.session_state["db_path_in"] = path_default

        readonly = st.checkbox(
            "Open read-only",
            value=False,
            help="SQLite URI mode=ro.",
            key="db_readonly",
        )
        st.text_input(
            "Local SQLite file path",
            help="PC path after a Pi pull, sync, or manual copy. Leave empty only if you use Upload above.",
            key="db_path_in",
        )
        path_in = (st.session_state.get("db_path_in") or "").strip()
        if st.button("Clear connection cache", key="db_clear_path"):
            get_connection.clear()
        if not path_in.strip():
            return None
        p = Path(path_in).expanduser()
        if not p.is_file():
            st.error(f"Not a file: {p}")
            return None
        st.caption(f"Resolved: `{p.resolve()}`")
        return p, readonly


def _sidebar_nav_vertical() -> str:
    """Stacked sidebar buttons (tab-like) instead of radio dots."""
    st.sidebar.markdown("### Menu")
    defs = _nav_definitions()
    if "sb_nav_page" not in st.session_state:
        if "nav_page_radio" in st.session_state:
            old = st.session_state.pop("nav_page_radio")
            label_to_id = {d[1]: d[0] for d in defs}
            st.session_state.sb_nav_page = label_to_id.get(old, "overview")
        else:
            st.session_state.sb_nav_page = "overview"
    for pid, label in defs:
        active = st.session_state.sb_nav_page == pid
        clicked = st.sidebar.button(
            label,
            key=f"sbnav_{pid}",
            use_container_width=True,
            type="primary" if active else "secondary",
        )
        if clicked:
            st.session_state.sb_nav_page = pid
    return str(st.session_state.sb_nav_page)


def _sidebar_connection_status(pi_cfg: dict[str, str], db_tuple: tuple[Path, bool] | None) -> None:
    st.sidebar.markdown("#### Active connections")
    st.sidebar.caption(
        f"**Pi (SSH):** `{pi_cfg.get('user')}@{pi_cfg.get('host')}` — open **Edge hub** for live status."
    )
    if db_tuple:
        p, ro = db_tuple
        st.sidebar.caption(f"**SQLite:** `{p.name}` {'(read-only)' if ro else ''}")
    else:
        st.sidebar.caption("**SQLite:** none — use **Database** to pull from Pi, upload, or set a path.")


def _maybe_pull_pi_db_on_startup(pi_cfg: dict[str, str]) -> None:
    if st.session_state.get("_pi_startup_db_pull_done"):
        return
    st.session_state["_pi_startup_db_pull_done"] = True
    if not st.session_state.get("auto_sync_on_start"):
        return
    host = (pi_cfg.get("host") or "").strip()
    user = (pi_cfg.get("user") or "").strip()
    if not host or not user:
        st.session_state["pi_sync_toast"] = "Auto-sync is on but Pi **Host** / **User** are empty."
        return
    ble = (pi_cfg.get("ble_root") or "/home/kpi/ble").strip()
    remote = (_env_or_secret("SIGNALHUB_PI_REMOTE_DB") or "").strip() or pi_db_sync.default_remote_db_path(ble)
    ident = (pi_cfg.get("identity") or "").strip() or None
    local_s = (_env_or_secret("SIGNALHUB_PI_LOCAL_DB_CACHE") or "").strip()
    local = Path(local_s).expanduser() if local_s else pi_db_sync.default_local_cache_path()
    with st.spinner("Pulling database from Pi…"):
        res = pi_db_sync.pull_pi_sqlite(
            host=host,
            user=user,
            remote_path=remote,
            local_path=local,
            identity_file=ident,
            non_interactive=True,
        )
    if res.ok and res.local_path is not None:
        st.session_state["db_path_in"] = str(res.local_path.resolve())
        get_connection.clear()
        st.session_state["pi_sync_toast"] = f"Startup sync: `{res.local_path.name}` ready."
    else:
        st.session_state["pi_sync_toast"] = f"Startup sync failed: {res.message}"


def _page_edge_hub(pi: dict[str, str]) -> None:
    st.header("📡 Edge hub — Raspberry Pi & BLE sniffer")
    st.caption(
        "Live **operational** view over SSH (systemd, tshark interfaces, disk, importer logs). "
        "This is not the SQLite analytics area — switch **Navigate** for Overview / Ledger / Reports."
    )
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**Checks run on the Pi**")
        st.markdown(
            "- `signalhub-ble.target`, collector, importer, ledger timer  \n"
            "- USB + serial device nodes (Nordic sniffer presence)  \n"
            "- `nrfutil` binary under your BLE root (when present)  \n"
            "- `tshark -D` — see note below (sniffer line + optional iface index)  \n"
            "- Disk under BLE root; **collector** + **importer** journal tails"
        )
    with c2:
        st.markdown("**On this PC**")
        st.markdown(
            "- OpenSSH Client (`ssh` in PATH)  \n"
            "- Key-based SSH to the Pi (set identity file in the sidebar if needed)  \n"
            "- SSH is used **only when you click refresh** below"
        )

    if st.button("🔄 Refresh Pi status", type="primary", key="pi_refresh_btn"):
        host = (pi.get("host") or "").strip()
        user = (pi.get("user") or "").strip()
        if not host or not user:
            st.error("Set **Host** and **User** in the sidebar (Edge hub expander).")
        else:
            with st.spinner("Querying Pi over SSH…"):
                st.session_state["pi_snapshot"] = pi_status.collect_edge_snapshot(
                    host=host,
                    user=user,
                    identity_file=(pi.get("identity") or "").strip() or None,
                    remote_ble_root=(pi.get("ble_root") or "/home/kpi/ble").strip(),
                )
            st.session_state["pi_snapshot_host"] = f"{user}@{host}"

    snap = st.session_state.get("pi_snapshot")
    if not snap:
        st.info("Configure SSH in the sidebar, then click **Refresh Pi status**.")
        return

    st.success(f"Snapshot for **{st.session_state.get('pi_snapshot_host', 'Pi')}** (copy is local to this browser session).")

    def _metric_val(key: str) -> str:
        r = snap[key]
        return (r.stdout or "").strip() or ("err" if r.exit_code else "—")

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("ble.target", _metric_val("target"))
    m2.metric("collector", _metric_val("collector"))
    m3.metric("importer", _metric_val("importer"))
    m4.metric("ledger timer", _metric_val("ledger_timer"))

    tshark_out = (snap["tshark_ifaces"].stdout or "").strip()
    sniff_idx = pi_status.nrf_sniffer_iface_index(tshark_out)
    m5, m6 = st.columns(2)
    with m5:
        st.metric(
            "Nordic sniffer (tshark iface #)",
            sniff_idx if sniff_idx is not None else "—",
            help="Index from `tshark -D` for the nRF Sniffer line. Matches optional SIGNALHUB_FORCE_TSHARK_IFACE on the Pi.",
        )
    with m6:
        focus_lines, _rest = pi_status.partition_tshark_for_ble(tshark_out)
        st.metric(
            "BLE/sniffer-related tshark rows",
            str(len(focus_lines)),
            help="Lines from tshark -D that mention Nordic, sniffer, ttyACM, Bluetooth adapters, or extcap.",
        )

    st.subheader("Uptime")
    st.code((snap["uptime"].stdout or snap["uptime"].stderr or "").strip(), language="text")

    st.subheader("systemd — signalhub-ble.target (excerpt)")
    st.code((snap["target_status"].stdout or snap["target_status"].stderr or "").strip(), language="text")

    st.subheader("BLE sniffer — hardware & Nordic tooling")
    st.caption(
        "Signalhub captures over **Bluetooth LE** via the **nRF Sniffer** (USB serial + Wireshark extcap). "
        "Kernel Wi‑Fi / Cisco / sshdump entries in `tshark -D` are normal Wireshark noise on the Pi."
    )
    hw1, hw2 = st.columns(2)
    with hw1:
        st.markdown("**USB (`lsusb` — Nordic / fallback)**")
        st.code((snap["sniffer_usb"].stdout or snap["sniffer_usb"].stderr or "").strip(), language="text")
    with hw2:
        st.markdown("**Serial devices (`ttyACM` / `ttyUSB`)**")
        st.code((snap["sniffer_serial"].stdout or snap["sniffer_serial"].stderr or "").strip(), language="text")
    st.markdown("**`nrfutil` (sniffer / extcap install path)**")
    st.code((snap["sniffer_nrfutil"].stdout or snap["sniffer_nrfutil"].stderr or "").strip(), language="text")

    a, b = st.columns(2)
    with a:
        st.subheader("Disk (BLE root)")
        st.code((snap["disk"].stdout or snap["disk"].stderr or "").strip(), language="text")
    with b:
        st.subheader("tshark — BLE / sniffer lines first")
        focus_lines, rest_lines = pi_status.partition_tshark_for_ble(
            (snap["tshark_ifaces"].stdout or snap["tshark_ifaces"].stderr or "").strip(),
        )
        st.caption(
            "**Why the full `tshark -D` list exists:** Wireshark registers every capture source (Wi‑Fi, loopback, remote dumps, **and** extcap plugins). "
            "Only the **nRF Sniffer** line (and its **interface number**) matter for this stack; the rest confirms tshark is healthy."
        )
        if focus_lines:
            st.code("\n".join(focus_lines), language="text")
        else:
            st.warning("No BLE/sniffer-focused lines parsed — check `tshark -D` below.")
        with st.expander("Full `tshark -D` (all interfaces)"):
            st.code(
                (snap["tshark_ifaces"].stdout or snap["tshark_ifaces"].stderr or "").strip(),
                language="text",
            )
        if rest_lines:
            st.caption(f"{len(rest_lines)} other Wireshark interface(s) — see expander (not used by the nRF sniffer path).")

    st.subheader("Recent collector journal (capture loop)")
    st.code((snap["recent_collector"].stdout or snap["recent_collector"].stderr or "").strip(), language="text")

    st.subheader("Recent importer journal")
    st.code((snap["recent_importer"].stdout or snap["recent_importer"].stderr or "").strip(), language="text")


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

    st.markdown("#### Analysis")
    st.info(
        "Use **Insights & AI** in the sidebar for a **single consolidated report** (new vs returning devices, "
        "busiest addresses, public OUI/vendor hints, interpretation, and AI). "
        "This page stays a lightweight **dashboard**; raw duplicate markdown was removed to cut overlap."
    )


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

    if caps.get("sensor_positions"):
        st.markdown("#### `sensor_positions` (RSSI centroid / multi-sensor layout)")
        st.caption("Set via CLI: `signalhub-ble sensor position-set --id … --x … --y …`.")
        st.dataframe(
            rows_to_dicts(conn.execute("SELECT * FROM sensor_positions ORDER BY sensor_id").fetchall()),
            use_container_width=True,
            hide_index=True,
        )

    if caps.get("ble_session_crypto"):
        st.markdown("#### `ble_session_crypto` (encrypted counts + operator key path)")
        st.caption("Updated on import; use `signalhub-ble crypto status --session …`.")
        st.dataframe(
            rows_to_dicts(conn.execute("SELECT * FROM ble_session_crypto ORDER BY session_id").fetchall()),
            use_container_width=True,
            hide_index=True,
        )


def _tab_reports_ai(conn: sqlite3.Connection, caps: dict, *, readonly: bool) -> None:
    st.subheader("Insights & AI")
    st.caption(
        "One **consolidated** report: capture health, per-address recurrence, SIG company/service names, "
        "new vs returning ledger rows, optional baseline comparison, and interpretation. "
        "AI uses a **JSON metrics** payload plus excerpts — meaning over table dumps."
    )
    r1, r2, r3 = st.columns(3)
    from_d = r1.text_input("From (UTC)", value=utc_today_iso(), key="rep_from")
    to_d = r2.text_input("To (UTC)", value=utc_today_iso(), key="rep_to")
    enrich = r3.checkbox("OUI lookup (Wireshark manuf + API fallback)", value=True, key="rep_enrich")
    b1, b2 = st.columns(2)
    baseline_from = b1.text_input(
        "Baseline from (UTC, optional)",
        value="",
        placeholder="YYYY-MM-DD",
        key="rep_base_from",
        help="Second UTC range for capture-health deltas in the report.",
    )
    baseline_to = b2.text_input(
        "Baseline to (UTC, optional)",
        value="",
        placeholder="YYYY-MM-DD",
        key="rep_base_to",
    )
    if readonly:
        st.caption("Database is **read-only** — window stats are computed inline (not written to `ble_device_window_stats`).")

    if not caps["ble_observations"]:
        st.warning("Insights need `ble_observations` (full DB).")
        return

    if st.button("Generate consolidated insights", type="primary", key="btn_insights"):
        try:
            with st.spinner("Building insights (may cache Wireshark manuf + Bluetooth numbers JSON)…"):
                st.session_state["_insights_md"] = reports.render_insights_report(
                    conn,
                    from_d.strip(),
                    to_d.strip(),
                    enrich_registry=enrich,
                    materialize_window_stats=not readonly,
                    baseline_from_date=baseline_from.strip() or None,
                    baseline_to_date=baseline_to.strip() or None,
                )
        except ValueError as e:
            st.error(str(e))

    if "_insights_md" in st.session_state:
        md = st.session_state["_insights_md"]
        st.download_button(
            "Download insights (.md)",
            data=md,
            file_name=f"insights-{from_d}-{to_d}.md",
            mime="text/markdown",
            key="dl_insights",
        )
        st.markdown(md)

    st.divider()
    st.markdown("#### AI interpretation (optional)")
    st.markdown(
        "Uses **OpenAI-compatible** API from `.env` / **Secrets** (`OPENAI_API_KEY` or `SIGNALHUB_OPENAI_API_KEY`). "
        "Generate **insights** above first so the model can react to that narrative."
    )
    model = st.text_input(
        "Model",
        value=_env_or_secret("SIGNALHUB_AI_MODEL") or "gpt-4o-mini",
        key="ai_model_ins",
    )
    base_url = st.text_input(
        "Base URL",
        value=_env_or_secret("SIGNALHUB_OPENAI_BASE_URL") or "https://api.openai.com/v1",
        key="ai_base_ins",
    )
    baseline = st.text_area(
        "Optional prior assessment markdown (baseline diff still uses appendix parser)",
        height=100,
        key="ai_base_md",
    )
    if st.button("Run AI interpretation (insights + metrics)", key="btn_ai_ins"):
        api_key = _env_or_secret("SIGNALHUB_OPENAI_API_KEY", "OPENAI_API_KEY")
        if not api_key:
            st.error("No API key.")
        elif "_insights_md" not in st.session_state:
            st.error("Generate **consolidated insights** first.")
        else:
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
                    f"Baseline file: range {p.utc_from}→{p.utc_to}, "
                    f"sessions={p.session_count}, packets={p.packet_count}, addrs={p.distinct_addresses}."
                )
            else:
                bsum = "No baseline markdown."
            full_ins = st.session_state["_insights_md"]
            metrics_json = assessment_enrichment.extract_insights_json_block(full_ins)
            ins_ex = assessment_enrichment.strip_insights_json_block(full_ins)[:10000]
            ap_ex = appendix[:6000] + ("…" if len(appendix) > 6000 else "")
            user_prompt = assessment_enrichment.build_insights_ai_user_prompt(
                from_date=from_d.strip(),
                to_date=to_d.strip(),
                insights_excerpt=ins_ex,
                appendix_excerpt=f"{bsum}\n\n{ap_ex}",
                structured_metrics_json=metrics_json,
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
                st.session_state["_ai_interp_md"] = narr
                st.success("AI interpretation ready.")
    if st.session_state.get("_ai_interp_md"):
        st.markdown("### AI output")
        st.markdown(st.session_state["_ai_interp_md"])
        full = (
            (st.session_state.get("_insights_md") or "").rstrip()
            + "\n\n---\n\n### AI interpretation\n\n"
            + st.session_state["_ai_interp_md"]
        )
        st.download_button(
            "Download insights + AI (.md)",
            data=full,
            file_name=f"insights-with-ai-{from_d}-{to_d}.md",
            mime="text/markdown",
            key="dl_ins_ai",
        )

    with st.expander("Legacy separate markdown (CLI parity only)"):
        st.caption("Prefer the consolidated report above; these overlap heavily.")
        c1, c2, c3 = st.columns(3)
        with c1:
            if st.button("Raw assessment", key="leg_ass"):
                try:
                    st.session_state["_leg_a"] = reports.render_assessment_report(
                        conn, from_d.strip(), to_d.strip()
                    )
                except ValueError as e:
                    st.error(str(e))
            if st.session_state.get("_leg_a"):
                st.download_button("DL", st.session_state["_leg_a"], file_name="assessment.md")
        with c2:
            if st.button("Change report", key="leg_ch"):
                try:
                    st.session_state["_leg_c"] = reports.render_change_report(conn, from_d.strip(), to_d.strip())
                except ValueError as e:
                    st.error(str(e))
            if st.session_state.get("_leg_c"):
                st.download_button("DL", st.session_state["_leg_c"], file_name="change.md")
        with c3:
            if st.button("Full ledger", key="leg_led"):
                st.session_state["_leg_l"] = reports.render_ledger_report(conn)
            if st.session_state.get("_leg_l"):
                st.download_button("DL", st.session_state["_leg_l"], file_name="ledger.md")


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
    st.set_page_config(
        page_title="Signalhub",
        page_icon="📶",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    st.markdown(
        """
<style>
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #14161c 0%, #0d0e12 100%);
        border-right: 1px solid rgba(255, 255, 255, 0.06);
    }
    [data-testid="stSidebar"] button[kind="secondary"] {
        background-color: rgba(255, 255, 255, 0.04);
        color: #e8eaed;
        border: 1px solid rgba(255, 255, 255, 0.08);
        border-radius: 8px;
    }
    [data-testid="stSidebar"] button[kind="primary"] {
        border-radius: 8px;
        border: 1px solid rgba(108, 140, 255, 0.35);
    }
    .block-container {
        padding-top: 1.25rem;
        max-width: 1180px;
    }
    h1, h2, h3 {
        font-weight: 600;
        letter-spacing: -0.02em;
    }
    div[data-testid="stMetricValue"] {
        font-variant-numeric: tabular-nums;
    }
    .signalhub-hero {
        background: linear-gradient(135deg, rgba(108, 140, 255, 0.12) 0%, rgba(22, 24, 29, 0.9) 55%);
        border: 1px solid rgba(255, 255, 255, 0.08);
        border-radius: 12px;
        padding: 1rem 1.25rem 1.1rem;
        margin-bottom: 1rem;
    }
    hr {
        border-color: rgba(255, 255, 255, 0.08);
    }
</style>
""",
        unsafe_allow_html=True,
    )
    st.sidebar.title("Signalhub")
    st.sidebar.caption("BLE review · Edge · SQLite")

    if "auto_sync_on_start" not in st.session_state:
        st.session_state["auto_sync_on_start"] = _auto_sync_pref_from_env()

    page = _sidebar_nav_vertical()
    pi_cfg = _sidebar_pi_config()
    _maybe_pull_pi_db_on_startup(pi_cfg)
    db_tuple = _sidebar_database_section(pi_cfg)
    _sidebar_connection_status(pi_cfg, db_tuple)

    toast = st.session_state.pop("pi_sync_toast", None)
    if toast:
        st.sidebar.success(toast)

    st.markdown(
        '<div class="signalhub-hero">'
        "<h1 style='margin:0 0 0.35rem 0;'>Dashboard</h1>"
        "<p style='margin:0; opacity:0.88; font-size:0.95rem;'>"
        "Navigate from the sidebar, connect a <strong>SQLite</strong> capture, or open "
        "<strong>Edge hub</strong> for Pi health. Optional: <code>SIGNALHUB_AUTO_SYNC_PI_DB=1</code> in <code>.env</code>."
        "</p></div>",
        unsafe_allow_html=True,
    )

    if page == "edge":
        _page_edge_hub(pi_cfg)
        return

    if db_tuple is None:
        st.info(
            "⬅️ In the sidebar, open **💾 Database**: **Pull latest DB from Pi**, set a **local path**, or **upload** a `.sqlite`. "
            "**Edge hub** works without a database."
        )
        return

    db_path, db_readonly = db_tuple
    try:
        conn = get_connection(str(db_path.resolve()), db_readonly)
    except sqlite3.Error as e:
        st.error(f"Could not open database: {e}")
        return

    caps = db_capabilities(conn)
    st.sidebar.divider()
    st.sidebar.caption(f"DB: **{'full' if caps['ble_observations'] else 'review'}** • `{db_path.name}`")

    if page == "overview":
        _tab_overview(conn, caps)
    elif page == "sessions":
        _tab_sessions(conn, caps)
    elif page == "ledger":
        _tab_ledger(conn, caps)
    elif page == "summaries":
        _tab_summaries(conn, caps)
    elif page == "observations":
        _tab_observations(conn, caps)
    elif page == "meta":
        _tab_meta(conn, caps)
    elif page == "reports":
        _tab_reports_ai(conn, caps, readonly=db_readonly)
    elif page == "sql":
        _tab_sql(conn)


# Community Cloud: prefer repo-root ``streamlit_app.py`` as the Streamlit "Main file" (see README).
if __name__ == "__main__":
    main()
