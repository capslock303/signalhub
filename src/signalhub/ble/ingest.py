from __future__ import annotations

import logging
import sqlite3
import uuid
from pathlib import Path

from signalhub.ble.decrypt_workflow import upsert_session_crypto_from_observations
from signalhub.ble.ledger import summarize_session
from signalhub.ble.parser import observation_flags, row_from_tshark_dict
from signalhub.ble.tshark import iter_field_rows
from signalhub.common.io import sha256_file
from signalhub.db.sqlite import init_db, now_epoch

logger = logging.getLogger(__name__)


def import_pcap(
    conn: sqlite3.Connection,
    *,
    pcap_path: Path,
    sensor_id: str,
    notes: str | None = None,
    environment_tag: str | None = None,
) -> str:
    pcap_path = pcap_path.resolve()
    if not pcap_path.is_file():
        raise FileNotFoundError(pcap_path)

    init_db(conn)

    session_id = str(uuid.uuid4())
    imported_at = now_epoch()
    sha = sha256_file(pcap_path)

    conn.execute(
        """
        INSERT INTO capture_sessions(
          session_id, sensor_id, started_at, ended_at, source_path, source_sha256,
          imported_at, notes, environment_tag
        ) VALUES (?,?,?,?,?,?,?,?,?)
        """,
        (
            session_id,
            sensor_id,
            None,
            None,
            str(pcap_path),
            sha,
            imported_at,
            notes,
            environment_tag,
        ),
    )

    batch: list[tuple] = []
    ts_first: float | None = None
    ts_last: float | None = None

    for raw in iter_field_rows(pcap_path):
        obs = row_from_tshark_dict(raw)
        if obs is None:
            continue
        flags = observation_flags(obs)
        ts = obs.time_epoch
        if ts is not None:
            ts_first = ts if ts_first is None else min(ts_first, ts)
            ts_last = ts if ts_last is None else max(ts_last, ts)

        manufacturer = obs.company_id
        service = obs.uuid16
        u128 = (obs.uuid128 or "").strip().lower() or None
        ap_raw = (obs.appearance or "").strip() or None
        fl_raw = (obs.adv_flags_hex or "").strip() or None
        batch.append(
            (
                session_id,
                ts,
                (obs.advertising_address or obs.scanning_address),
                obs.address_type,
                obs.pdu_type,
                obs.rssi,
                obs.device_name,
                manufacturer,
                service,
                ap_raw,
                obs.tx_power_dbm,
                fl_raw,
                u128,
                flags["connection_seen"],
                flags["gatt_seen"],
                flags["smp_seen"],
                flags["encrypted_seen"],
                obs.frame_protocols,
                obs.frame_number,
            ),
        )
        if len(batch) >= 750:
            _flush_batch(conn, batch)
            batch.clear()

    if batch:
        _flush_batch(conn, batch)

    conn.execute(
        """
        UPDATE capture_sessions
        SET started_at = ?, ended_at = ?
        WHERE session_id = ?
        """,
        (ts_first, ts_last, session_id),
    )
    conn.commit()
    n_obs = _count_obs(conn, session_id)
    logger.info("Imported session %s (%d observations)", session_id, n_obs)
    summarize_session(conn, session_id)
    try:
        upsert_session_crypto_from_observations(conn, session_id)
    except sqlite3.Error:
        logger.warning("Could not update ble_session_crypto for session %s", session_id)
    return session_id


def _flush_batch(conn: sqlite3.Connection, batch: list[tuple]) -> None:
    conn.executemany(
        """
        INSERT INTO ble_observations(
          session_id, timestamp, address, address_type, pdu_type, rssi,
          name_hint, manufacturer_hint, service_hint,
          appearance_hint, tx_power_dbm, adv_flags_hex, service_uuid128_hint,
          connection_seen, gatt_seen, smp_seen, encrypted_seen,
          frame_protocols, raw_ref
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        batch,
    )


def _count_obs(conn: sqlite3.Connection, session_id: str) -> int:
    row = conn.execute(
        "SELECT COUNT(*) FROM ble_observations WHERE session_id = ?",
        (session_id,),
    ).fetchone()
    return int(row[0]) if row else 0
