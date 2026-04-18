from __future__ import annotations

import csv
import os
import sys
from pathlib import Path

import click

from signalhub import __version__
from signalhub.ble import assessment_enrichment, ingest, reports
from signalhub.ble.ledger import apply_classify_to_ledger, rebuild_ledger, summarize_session
from signalhub.review.build import build_review_database
from signalhub.common.logging import configure_logging
from signalhub.common.csv_export import row_dict_for_csv
from signalhub.config import db_path as default_db_path, load_dotenv_files
from signalhub.common.timeutil import utc_today_iso
from signalhub.db.sqlite import connect, init_db


def _write_csv_rows(out: Path, rows: list) -> None:
    if not rows:
        return
    out.parent.mkdir(parents=True, exist_ok=True)
    names = list(rows[0].keys())
    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=names)
        w.writeheader()
        for r in rows:
            w.writerow(row_dict_for_csv(r))


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.option(
    "--db",
    type=click.Path(dir_okay=False, path_type=Path),
    help="SQLite database path (overrides SIGNALHUB_DB).",
)
@click.version_option(__version__, prog_name="signalhub-ble")
@click.pass_context
def main(ctx: click.Context, db: Path | None) -> None:
    """BLE session ingestion and ledger tooling for nRF52840 DK captures."""
    load_dotenv_files()
    configure_logging()
    ctx.ensure_object(dict)
    ctx.obj["db_path"] = db if db is not None else default_db_path()


def _conn(ctx: click.Context):
    return connect(ctx.obj["db_path"])


def _root_ctx(ctx: click.Context) -> click.Context:
    root = ctx
    while root.parent is not None:
        root = root.parent
    return root


def _resolve_dashboard_sqlite(ctx: click.Context, explicit: Path | None) -> Path:
    """Path for Streamlit: explicit --db, then env, then same default as `signalhub-ble --db`."""
    if explicit is not None:
        p = explicit.expanduser().resolve()
        if not p.is_file():
            raise click.ClickException(f"Database not found: {p}")
        return p
    for env_key in ("SIGNALHUB_REVIEW_DB", "SIGNALHUB_DB"):
        raw = os.environ.get(env_key, "").strip()
        if raw:
            p = Path(raw).expanduser().resolve()
            if p.is_file():
                return p
    root = _root_ctx(ctx)
    root.ensure_object(dict)
    raw_default = root.obj.get("db_path")
    cand = Path(raw_default if raw_default is not None else default_db_path()).resolve()
    if cand.is_file():
        return cand
    raise click.ClickException(
        "No SQLite database file found.\n\n"
        "Pass a real path, for example:\n"
        f"  signalhub-ble review serve --db \"{default_db_path()}\"\n\n"
        "Or set SIGNALHUB_DB or SIGNALHUB_REVIEW_DB to an existing .sqlite file.\n"
        "(The string path\\to\\signalhub.sqlite in docs is only an example, not a real path.)"
    )


@main.command("init-db")
@click.pass_context
def init_db_cmd(ctx: click.Context) -> None:
    """Create database tables."""
    conn = _conn(ctx)
    init_db(conn)
    click.echo(f"Initialized {ctx.obj['db_path']}")


@main.group("sensor")
@click.pass_context
def sensor_cli(ctx: click.Context) -> None:
    """Manage sensors table."""
    del ctx  # unused


@sensor_cli.command("add")
@click.option("--id", "sensor_id", required=True, help="Sensor identifier, e.g. NRF52840DK-01")
@click.option("--type", "sensor_type", required=True, help="e.g. ble_sniffer")
@click.option("--model", default="", show_default=False)
@click.option("--notes", default="")
@click.pass_context
def sensor_add(ctx: click.Context, sensor_id: str, sensor_type: str, model: str, notes: str) -> None:
    conn = _conn(ctx)
    init_db(conn)
    conn.execute(
        """
        INSERT INTO sensors(sensor_id, sensor_type, model, notes)
        VALUES (?,?,?,?)
        ON CONFLICT(sensor_id) DO UPDATE SET
          sensor_type = excluded.sensor_type,
          model = excluded.model,
          notes = excluded.notes
        """,
        (sensor_id, sensor_type, model or None, notes or None),
    )
    conn.commit()
    click.echo(f"Registered sensor {sensor_id}")


@main.command("import")
@click.option("--pcap", type=click.Path(exists=True, dir_okay=False, path_type=Path), required=True)
@click.option("--sensor", "sensor_id", required=True)
@click.option("--notes", default="")
@click.option("--environment-tag", default=None)
@click.pass_context
def import_cmd(
    ctx: click.Context,
    pcap: Path,
    sensor_id: str,
    notes: str,
    environment_tag: str | None,
) -> None:
    """Import a .pcap/.pcapng via tshark into ble_observations."""
    conn = _conn(ctx)
    init_db(conn)
    row = conn.execute(
        "SELECT 1 FROM sensors WHERE sensor_id = ?",
        (sensor_id,),
    ).fetchone()
    if not row:
        raise click.ClickException(
            f"Unknown sensor {sensor_id!r}. Create it first: signalhub-ble sensor add ...",
        )
    sid = ingest.import_pcap(
        conn,
        pcap_path=pcap,
        sensor_id=sensor_id,
        notes=notes or None,
        environment_tag=environment_tag,
    )
    click.echo(sid)


@main.command("summarize")
@click.option("--session", "session_id", required=False, default=None)
@click.option(
    "--all",
    "all_sessions",
    is_flag=True,
    help="Backfill ble_device_session_summary for every capture session.",
)
@click.pass_context
def summarize_cmd(ctx: click.Context, session_id: str | None, all_sessions: bool) -> None:
    """Build per-address session summaries (required for a full ledger rebuild)."""
    conn = _conn(ctx)
    init_db(conn)
    if all_sessions:
        rows = conn.execute(
            "SELECT session_id FROM capture_sessions ORDER BY imported_at",
        ).fetchall()
        total_addrs = 0
        for (sid,) in rows:
            total_addrs += summarize_session(conn, sid)
        click.echo(f"Backfill: {len(rows)} sessions, {total_addrs} session/address summary rows.")
    elif session_id:
        n = summarize_session(conn, session_id)
        click.echo(f"Summarized {n} addresses for session {session_id}")
    else:
        raise click.ClickException("Pass --session <UUID> or --all.")


@main.group("ledger")
@click.pass_context
def ledger_cli(ctx: click.Context) -> None:
    """Ledger maintenance."""
    del ctx


@ledger_cli.command("rebuild")
@click.pass_context
def ledger_rebuild_cmd(ctx: click.Context) -> None:
    """Rebuild ble_devices from session summaries + observations."""
    conn = _conn(ctx)
    init_db(conn)
    n = rebuild_ledger(conn)
    click.echo(f"Rebuilt ledger with {n} devices")


@main.command("classify")
@click.pass_context
def classify_cmd(ctx: click.Context) -> None:
    """Re-apply heuristic classification to existing ledger rows."""
    conn = _conn(ctx)
    init_db(conn)
    n = apply_classify_to_ledger(conn)
    click.echo(f"Updated {n} ledger rows")


@main.group("export")
@click.pass_context
def export_cli(ctx: click.Context) -> None:
    """Exports."""
    del ctx


@export_cli.command("csv")
@click.option("--out", type=click.Path(dir_okay=False, path_type=Path), required=True)
@click.option(
    "--from",
    "from_date",
    default=None,
    help="UTC date YYYY-MM-DD (inclusive, with --to).",
)
@click.option("--to", "to_date", default=None, help="UTC date YYYY-MM-DD (inclusive).")
@click.option(
    "--by",
    "by_field",
    type=click.Choice(["active", "last_seen", "first_seen"], case_sensitive=False),
    default="active",
    help="How to filter ble_devices when --from/--to are set: "
    "active=overlap window; last_seen / first_seen=timestamps in window.",
)
@click.pass_context
def export_csv(
    ctx: click.Context,
    out: Path,
    from_date: str | None,
    to_date: str | None,
    by_field: str,
) -> None:
    """Export ble_devices to CSV (optionally filtered by UTC day range)."""
    conn = _conn(ctx)
    init_db(conn)
    if (from_date is None) ^ (to_date is None):
        raise click.ClickException("Use both --from and --to together, or omit both for full export.")
    try:
        rows = reports.ble_devices_rows_for_export(
            conn,
            from_date=from_date,
            to_date=to_date,
            by=by_field.lower(),
        )
    except ValueError as e:
        raise click.ClickException(str(e)) from e
    if not rows:
        click.echo("No ledger rows for this query.", err=True)
        return
    _write_csv_rows(out, rows)
    click.echo(f"Wrote {out} ({len(rows)} rows)")


@export_cli.command("sessions")
@click.option("--out", type=click.Path(dir_okay=False, path_type=Path), required=True)
@click.option("--from", "from_date", required=True, help="UTC date YYYY-MM-DD (inclusive).")
@click.option("--to", "to_date", required=True, help="UTC date YYYY-MM-DD (inclusive).")
@click.pass_context
def export_sessions(ctx: click.Context, out: Path, from_date: str, to_date: str) -> None:
    """Export capture_sessions whose time span overlaps the UTC day range."""
    conn = _conn(ctx)
    init_db(conn)
    rows = reports.capture_sessions_rows_for_export(conn, from_date=from_date, to_date=to_date)
    if not rows:
        click.echo("No sessions overlap this window.", err=True)
        return
    _write_csv_rows(out, rows)
    click.echo(f"Wrote {out} ({len(rows)} rows)")


@export_cli.command("observations")
@click.option("--out", type=click.Path(dir_okay=False, path_type=Path), required=True)
@click.option("--from", "from_date", required=True, help="UTC date YYYY-MM-DD (inclusive).")
@click.option("--to", "to_date", required=True, help="UTC date YYYY-MM-DD (inclusive).")
@click.pass_context
def export_observations(ctx: click.Context, out: Path, from_date: str, to_date: str) -> None:
    """Export ble_observations with packet timestamps in the UTC day range."""
    conn = _conn(ctx)
    init_db(conn)
    rows = reports.ble_observations_rows_for_export(conn, from_date=from_date, to_date=to_date)
    if not rows:
        click.echo("No observations in this window.", err=True)
        return
    _write_csv_rows(out, rows)
    click.echo(f"Wrote {out} ({len(rows)} rows)")


@export_cli.command("assessment-tables")
@click.option("--from", "from_date", required=True, help="UTC date YYYY-MM-DD (inclusive).")
@click.option("--to", "to_date", required=True, help="UTC date YYYY-MM-DD (inclusive).")
@click.option(
    "--out-dir",
    type=click.Path(path_type=Path),
    default=Path("data/exports"),
    show_default=True,
    help="Directory for CSV files (relative to current working directory unless absolute).",
)
@click.option(
    "--stem",
    default="assessment-tables",
    show_default=True,
    help="Filename stem: writes {stem}-table-summary.csv, {stem}-table-pdu_types.csv, ...",
)
@click.pass_context
def export_assessment_tables(
    ctx: click.Context,
    from_date: str,
    to_date: str,
    out_dir: Path,
    stem: str,
) -> None:
    """Write CSVs that mirror `report assess` summary tables (same UTC window)."""
    conn = _conn(ctx)
    init_db(conn)
    paths = reports.write_assessment_table_csvs(
        conn,
        from_date=from_date,
        to_date=to_date,
        directory=out_dir,
        stem=stem,
    )
    for p in paths:
        click.echo(f"Wrote {p}")


@export_cli.command("review-db")
@click.option(
    "--out",
    type=click.Path(dir_okay=False, path_type=Path),
    required=True,
    help="New SQLite file (parent dirs created). Raw ble_observations dropped after snapshot.",
)
@click.pass_context
def export_review_db(ctx: click.Context, out: Path) -> None:
    """Write a smaller copy: ledger, summaries, sessions, session_stats; no observation rows."""
    conn = _conn(ctx)
    init_db(conn)
    build_review_database(conn, out)
    click.echo(f"Wrote {out}")


@main.group("review")
@click.pass_context
def review_cli(ctx: click.Context) -> None:
    """Streamlit dashboard: tables, assessment markdown, optional AI (pip install 'signalhub[review]')."""
    del ctx


@review_cli.command("serve")
@click.option(
    "--db",
    "db_path",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help="SQLite file (optional if SIGNALHUB_DB / SIGNALHUB_REVIEW_DB or default data/db exists).",
)
@click.option("--port", default=8501, show_default=True)
@click.pass_context
def review_serve(ctx: click.Context, db_path: Path | None, port: int) -> None:
    """Launch the Signalhub dashboard (full or review SQLite) in the browser."""
    resolved = _resolve_dashboard_sqlite(ctx, db_path)
    try:
        import streamlit  # noqa: F401
    except ImportError as e:
        raise click.ClickException(
            "Streamlit is not installed. Use: pip install 'signalhub[review]'",
        ) from e
    import subprocess
    import sys

    import signalhub.review as review_pkg

    app_py = Path(review_pkg.__file__).resolve().parent / "app.py"
    repo_root = app_py.resolve().parents[3]
    entry = repo_root / "streamlit_app.py"
    if entry.is_file():
        streamlit_target = entry
    else:
        streamlit_target = app_py
    env = {**os.environ, "SIGNALHUB_REVIEW_DB": str(resolved)}
    click.echo(f"Dashboard database: {resolved}")
    click.echo(f"Streamlit script: {streamlit_target}")
    raise SystemExit(
        subprocess.call(
            [
                sys.executable,
                "-m",
                "streamlit",
                "run",
                str(streamlit_target),
                "--server.port",
                str(port),
            ],
            env=env,
        ),
    )


@main.group("report")
@click.pass_context
def report_cli(ctx: click.Context) -> None:
    """Markdown reports."""
    del ctx


@report_cli.command("session")
@click.option("--session", "session_id", required=True)
@click.option("--out", type=click.Path(dir_okay=False, path_type=Path), required=True)
@click.pass_context
def report_session(ctx: click.Context, session_id: str, out: Path) -> None:
    conn = _conn(ctx)
    init_db(conn)
    text = reports.render_session_report(conn, session_id)
    reports.write_text(out, text)
    click.echo(f"Wrote {out}")


@report_cli.command("ledger")
@click.option("--out", type=click.Path(dir_okay=False, path_type=Path), required=True)
@click.pass_context
def report_ledger(ctx: click.Context, out: Path) -> None:
    conn = _conn(ctx)
    init_db(conn)
    text = reports.render_ledger_report(conn)
    reports.write_text(out, text)
    click.echo(f"Wrote {out}")


@report_cli.command("change")
@click.option("--from", "from_date", required=True)
@click.option("--to", "to_date", required=True)
@click.option("--out", type=click.Path(dir_okay=False, path_type=Path), required=True)
@click.pass_context
def report_change(ctx: click.Context, from_date: str, to_date: str, out: Path) -> None:
    conn = _conn(ctx)
    init_db(conn)
    text = reports.render_change_report(conn, from_date, to_date)
    reports.write_text(out, text)
    click.echo(f"Wrote {out}")


@report_cli.command("assess")
@click.option("--from", "from_date", required=True, help="UTC date YYYY-MM-DD (inclusive).")
@click.option(
    "--to",
    "to_date",
    required=False,
    default=None,
    help="UTC date YYYY-MM-DD (inclusive). Default: today (UTC). You may pass the word 'today'.",
)
@click.option("--out", type=click.Path(dir_okay=False, path_type=Path), required=True)
@click.option(
    "--baseline",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    default=None,
    help="Optional prior assessment.md; used with --follow-up to diff headline counts.",
)
@click.option(
    "--follow-up/--no-follow-up",
    default=False,
    help="Append DB-wide snapshot, last-24h import/frame activity, and optional baseline diff.",
)
@click.option(
    "--ai/--no-ai",
    default=False,
    help="With --follow-up, call OpenAI Chat Completions (OPENAI_API_KEY or SIGNALHUB_OPENAI_API_KEY).",
)
@click.option(
    "--ai-model",
    default="gpt-4o-mini",
    show_default=True,
)
@click.pass_context
def report_assess(
    ctx: click.Context,
    from_date: str,
    to_date: str | None,
    out: Path,
    baseline: Path | None,
    follow_up: bool,
    ai: bool,
    ai_model: str,
) -> None:
    """Markdown summary: sessions, observations, ledger activity, coarse protocol hints."""
    if to_date is None or str(to_date).strip().lower() == "today":
        to_date = utc_today_iso()
    else:
        to_date = str(to_date).strip()
    conn = _conn(ctx)
    init_db(conn)
    text = reports.render_assessment_report(conn, from_date, to_date)
    if follow_up:
        baseline_text = assessment_enrichment.read_baseline(baseline)
        appendix = assessment_enrichment.render_follow_up_appendix(
            conn,
            from_date=from_date,
            to_date=to_date,
            baseline_markdown=baseline_text,
            ai_narrative=None,
        )
        if ai:
            api_key = os.environ.get("SIGNALHUB_OPENAI_API_KEY") or os.environ.get("OPENAI_API_KEY")
            base_url = os.environ.get("SIGNALHUB_OPENAI_BASE_URL", "https://api.openai.com/v1")
            if not api_key:
                click.echo(
                    "No OPENAI_API_KEY or SIGNALHUB_OPENAI_API_KEY in environment — skipping --ai.",
                    err=True,
                )
            else:
                if baseline_text:
                    p = assessment_enrichment.parse_previous_assessment(baseline_text)
                    bsum = (
                        f"Parsed baseline file: range {p.utc_from}→{p.utc_to}, "
                        f"sessions={p.session_count}, packets={p.packet_count}, addrs={p.distinct_addresses}."
                    )
                else:
                    bsum = "No baseline file."
                user_prompt = assessment_enrichment.build_ai_user_prompt(
                    from_date=from_date,
                    to_date=to_date,
                    baseline_summary=bsum,
                    deterministic_appendix_excerpt=appendix,
                )
                try:
                    narr = assessment_enrichment.fetch_openai_narrative(
                        user_prompt=user_prompt,
                        api_key=api_key,
                        model=ai_model,
                        base_url=base_url,
                    )
                except Exception as exc:
                    click.echo(f"AI narrative failed: {exc}", err=True)
                else:
                    appendix = assessment_enrichment.render_follow_up_appendix(
                        conn,
                        from_date=from_date,
                        to_date=to_date,
                        baseline_markdown=baseline_text,
                        ai_narrative=narr,
                    )
        text = text.rstrip() + "\n\n" + appendix
    reports.write_text(out, text)
    click.echo(f"Wrote {out}")


@main.command("diff")
@click.option("--from", "from_date", required=True)
@click.option("--to", "to_date", required=True)
@click.pass_context
def diff_cmd(ctx: click.Context, from_date: str, to_date: str) -> None:
    """Print a short stdout summary between two dates (UTC)."""
    conn = _conn(ctx)
    init_db(conn)
    text = reports.render_change_report(conn, from_date, to_date)
    # Already markdown; fine for console review
    click.echo(text)


if __name__ == "__main__":
    sys.exit(main())
