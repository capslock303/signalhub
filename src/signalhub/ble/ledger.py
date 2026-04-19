from __future__ import annotations

import json
import logging
import os
import sqlite3
from typing import Any

from signalhub.ble.classify import (
    classify_from_rollup,
    confidence_from_evidence,
    refine_device_class_with_history,
)
from signalhub.ble.identity import stable_identity_key_and_kind
from signalhub.db.sqlite import init_db
from signalhub.ble.models import SessionAddressRollup
from signalhub.ble.normalize import adv_profile_json_for_rollup, appearance_pattern, rollup_session

logger = logging.getLogger(__name__)


def summarize_session(conn: sqlite3.Connection, session_id: str) -> int:
    rows = list(
        conn.execute(
            "SELECT * FROM ble_observations WHERE session_id = ? ORDER BY observation_id",
            (session_id,),
        ),
    )
    if not rows:
        logger.warning("No observations for session %s", session_id)
        return 0

    roll = rollup_session(rows)
    conn.execute("DELETE FROM ble_device_session_summary WHERE session_id = ?", (session_id,))

    n = 0
    for addr, r in roll.items():
        pat = appearance_pattern(r)
        conn.execute(
            """
            INSERT INTO ble_device_session_summary(
              ledger_id, session_id, address, first_seen, last_seen,
              rssi_min, rssi_max, pdu_summary, connection_seen, gatt_seen,
              smp_seen, encrypted_seen, appearance_pattern, packet_count,
              adv_profile_json
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                None,
                session_id,
                addr,
                r.first_seen,
                r.last_seen,
                r.rssi_min,
                r.rssi_max,
                json.dumps(sorted(r.pdu_types), separators=(",", ":")),
                int(r.connection_seen),
                int(r.gatt_seen),
                int(r.smp_seen),
                int(r.encrypted_seen),
                pat,
                r.packet_count,
                adv_profile_json_for_rollup(r),
            ),
        )
        n += 1
    conn.commit()
    logger.info("Session %s summarized (%d addresses)", session_id, n)
    return n


def _linked_ble_addresses(conn: sqlite3.Connection, ledger_id: str, primary: str | None) -> list[str]:
    macs: list[str] = []
    seen: set[str] = set()
    if primary:
        macs.append(primary)
        seen.add(primary)
    for (v,) in conn.execute(
        """
        SELECT alias_value FROM ble_aliases
        WHERE ledger_id = ? AND alias_type = 'ble_address'
        """,
        (ledger_id,),
    ):
        if v and v not in seen:
            macs.append(str(v))
            seen.add(str(v))
    return macs


def _merge_address_buckets(mac_list: list[str], buckets: list[dict[str, Any]]) -> dict[str, Any]:
    """Merge per-MAC aggregates that share the same stable_identity_key."""
    primary_mac = mac_list[0]
    primary_ts: float | None = None
    for addr, b in zip(mac_list, buckets):
        ts = b.get("recent_ts")
        if ts is not None and (primary_ts is None or ts >= primary_ts):
            primary_ts = ts
            primary_mac = addr

    merged: dict[str, Any] = {
        "first_seen": None,
        "last_seen": None,
        "rssi_min": None,
        "rssi_max": None,
        "pdus": set(),
        "packets": 0,
        "sessions": set(),
        "name": None,
        "name_ts": None,
        "mfg": None,
        "mfg_ts": None,
        "svc": None,
        "svc_ts": None,
        "addr_type": "unknown",
        "connection": False,
        "gatt": False,
        "smp": False,
        "enc": False,
        "recent_session": None,
        "recent_ts": None,
        "primary_mac": primary_mac,
        "all_macs": list(mac_list),
        "u128_hints": set(),
    }

    rank = {"public": 3, "random": 2, "unknown": 1}

    def better_addr_type(cur: str, new: str) -> str:
        return new if rank.get(new, 0) > rank.get(cur, 0) else cur

    for b in buckets:
        merged["packets"] += int(b["packets"] or 0)
        merged["sessions"].update(b["sessions"])
        merged["pdus"].update(b["pdus"])
        merged["connection"] = merged["connection"] or bool(b["connection"])
        merged["gatt"] = merged["gatt"] or bool(b["gatt"])
        merged["smp"] = merged["smp"] or bool(b["smp"])
        merged["enc"] = merged["enc"] or bool(b["enc"])
        merged["addr_type"] = better_addr_type(str(merged["addr_type"]), str(b["addr_type"]))

        for ts_key, dest in (("first_seen", "first_seen"), ("last_seen", "last_seen")):
            ts = b.get(dest)
            if ts is None:
                continue
            cur = merged[dest]
            if dest == "first_seen":
                merged[dest] = ts if cur is None else min(cur, ts)
            else:
                merged[dest] = ts if cur is None else max(cur, ts)
        for rs_key, dest in (("rssi_min", "rssi_min"), ("rssi_max", "rssi_max")):
            val = b.get(dest)
            if val is None:
                continue
            cur = merged[dest]
            if cur is None:
                merged[dest] = val
            elif "min" in dest:
                merged[dest] = min(cur, val)
            else:
                merged[dest] = max(cur, val)

        if b.get("name"):
            ts_n = b.get("name_ts")
            if merged["name_ts"] is None or (ts_n is not None and ts_n >= (merged["name_ts"] or -1e100)):
                merged["name"] = b["name"]
                merged["name_ts"] = ts_n
        if b.get("mfg"):
            ts_m = b.get("mfg_ts")
            if merged["mfg_ts"] is None or (ts_m is not None and ts_m >= (merged["mfg_ts"] or -1e100)):
                merged["mfg"] = b["mfg"]
                merged["mfg_ts"] = ts_m
        if b.get("svc"):
            ts_s = b.get("svc_ts")
            if merged["svc_ts"] is None or (ts_s is not None and ts_s >= (merged["svc_ts"] or -1e100)):
                merged["svc"] = b["svc"]
                merged["svc_ts"] = ts_s
        merged["u128_hints"].update(b.get("u128_hints") or set())

        last = b.get("recent_ts")
        if last is not None and (merged["recent_ts"] is None or last >= merged["recent_ts"]):
            merged["recent_ts"] = last
            merged["recent_session"] = b.get("recent_session")

    return merged


def _next_ledger_id(conn: sqlite3.Connection) -> str:
    rows = conn.execute(
        "SELECT ledger_id FROM ble_devices WHERE ledger_id GLOB 'BLE-[0-9]*'",
    ).fetchall()
    best = 0
    for (lid,) in rows:
        try:
            best = max(best, int(str(lid).split("-", 1)[1]))
        except (IndexError, ValueError):
            continue
    return f"BLE-{best + 1:04d}"


def rebuild_ledger(conn: sqlite3.Connection, *, fingerprint_profile: str | None = None) -> int:
    init_db(conn)
    conn.execute("DELETE FROM ble_aliases")
    conn.execute("DELETE FROM ble_devices")
    conn.execute("UPDATE ble_device_session_summary SET ledger_id = NULL")

    fp = (fingerprint_profile or os.environ.get("SIGNALHUB_FINGERPRINT_PROFILE") or "v1").strip().lower()
    if fp not in ("v1", "v2"):
        fp = "v1"

    rows = list(conn.execute("SELECT * FROM ble_device_session_summary"))
    if not rows:
        conn.commit()
        return 0

    by_addr: dict[str, dict[str, Any]] = {}

    def ensure(addr: str) -> dict[str, Any]:
        if addr not in by_addr:
            by_addr[addr] = {
                "first_seen": None,
                "last_seen": None,
                "rssi_min": None,
                "rssi_max": None,
                "pdus": set(),
                "packets": 0,
                "sessions": set(),
                "name": None,
                "name_ts": None,
                "mfg": None,
                "mfg_ts": None,
                "svc": None,
                "svc_ts": None,
                "addr_type": "unknown",
                "connection": False,
                "gatt": False,
                "smp": False,
                "enc": False,
                "recent_session": None,
                "recent_ts": None,
                "u128_hints": set(),
            }
        return by_addr[addr]

    for s in rows:
        addr = s["address"]
        if not addr:
            continue
        b = ensure(addr)
        sid = s["session_id"]
        b["sessions"].add(sid)
        b["packets"] += int(s["packet_count"] or 0)
        for ts_key, dest in (("first_seen", "first_seen"), ("last_seen", "last_seen")):
            ts = s[ts_key]
            if ts is None:
                continue
            cur = b[dest]
            if dest == "first_seen":
                b[dest] = ts if cur is None else min(cur, ts)
            else:
                b[dest] = ts if cur is None else max(cur, ts)
        for rs_key, dest in (("rssi_min", "rssi_min"), ("rssi_max", "rssi_max")):
            val = s[rs_key]
            if val is None:
                continue
            cur = b[dest]
            if cur is None:
                b[dest] = val
            elif "min" in rs_key:
                b[dest] = min(cur, val)
            else:
                b[dest] = max(cur, val)
        try:
            for p in json.loads(s["pdu_summary"] or "[]"):
                b["pdus"].add(str(p))
        except json.JSONDecodeError:
            pass
        b["connection"] = b["connection"] or bool(s["connection_seen"])
        b["gatt"] = b["gatt"] or bool(s["gatt_seen"])
        b["smp"] = b["smp"] or bool(s["smp_seen"])
        b["enc"] = b["enc"] or bool(s["encrypted_seen"])
        last = s["last_seen"]
        if last is not None and (b["recent_ts"] is None or last >= b["recent_ts"]):
            b["recent_ts"] = last
            b["recent_session"] = sid

        obs_hints = conn.execute(
            """
            SELECT name_hint, manufacturer_hint, service_hint, service_uuid128_hint, timestamp
            FROM ble_observations
            WHERE session_id = ? AND address = ?
            ORDER BY observation_id
            """,
            (sid, addr),
        ).fetchall()
        for name_hint, mfg, svc, u128, ts in obs_hints:
            if name_hint:
                if b["name_ts"] is None or (ts is not None and ts >= b["name_ts"]):
                    b["name"] = str(name_hint)
                    b["name_ts"] = ts
            if mfg:
                if b["mfg_ts"] is None or (ts is not None and ts >= b["mfg_ts"]):
                    b["mfg"] = str(mfg)
                    b["mfg_ts"] = ts
            if svc:
                if b["svc_ts"] is None or (ts is not None and ts >= b["svc_ts"]):
                    b["svc"] = str(svc)
                    b["svc_ts"] = ts
            if u128 and str(u128).strip():
                b["u128_hints"].add(str(u128).strip().lower())

        ot = conn.execute(
            """
            SELECT address_type FROM ble_observations
            WHERE session_id = ? AND address = ? AND address_type IS NOT NULL AND address_type != 'unknown'
            ORDER BY observation_id DESC LIMIT 1
            """,
            (sid, addr),
        ).fetchone()
        if ot and ot[0]:
            new_t = str(ot[0]).strip().lower()
            cur_t = str(b["addr_type"]).strip().lower()
            _rank = {"public": 3, "random": 2, "unknown": 1}
            if _rank.get(new_t, 0) >= _rank.get(cur_t, 0):
                b["addr_type"] = new_t

    clusters: dict[str, list[str]] = {}
    for addr, b in by_addr.items():
        sk, _ik = stable_identity_key_and_kind(
            address=addr,
            address_type=b["addr_type"],
            name=b["name"],
            manufacturer=b["mfg"],
            service=b["svc"],
            uuid128s=frozenset(b.get("u128_hints") or ()),
            fingerprint_profile=fp,
        )
        clusters.setdefault(sk, []).append(addr)

    inserted = 0
    addr_to_ledger: dict[str, str] = {}
    for mac_list in clusters.values():
        mac_list_sorted = sorted(mac_list)
        buckets = [by_addr[m] for m in mac_list_sorted]
        b = _merge_address_buckets(mac_list_sorted, buckets)
        stable_key_final, identity_kind = stable_identity_key_and_kind(
            address=b["primary_mac"],
            address_type=b["addr_type"],
            name=b["name"],
            manufacturer=b["mfg"],
            service=b["svc"],
            uuid128s=frozenset(b.get("u128_hints") or ()),
            fingerprint_profile=fp,
        )
        rollup = SessionAddressRollup(
            address=b["primary_mac"],
            first_seen=b["first_seen"],
            last_seen=b["last_seen"],
            rssi_min=b["rssi_min"],
            rssi_max=b["rssi_max"],
            pdu_types=set(b["pdus"]),
            packet_count=int(b["packets"]),
            name_hints={b["name"]} if b["name"] else set(),
            manufacturer_hints={b["mfg"]} if b["mfg"] else set(),
            service_hints={b["svc"]} if b["svc"] else set(),
            connection_seen=bool(b["connection"]),
            gatt_seen=bool(b["gatt"]),
            smp_seen=bool(b["smp"]),
            encrypted_seen=bool(b["enc"]),
            uuid128_hints=set(b.get("u128_hints") or ()),
        )
        pat = appearance_pattern(rollup)
        device_guess, _conf0, conn_guess, scan_guess = classify_from_rollup(rollup)
        device = refine_device_class_with_history(
            device_guess,
            any_gatt=bool(b["gatt"]),
            any_smp=bool(b["smp"]),
            any_enc=bool(b["enc"]),
            sessions_seen=len(b["sessions"]),
        )
        has_id = bool(b["name"] or b["mfg"] or b["svc"] or b.get("u128_hints"))
        confidence = confidence_from_evidence(
            packet_count_total=int(b["packets"]),
            sessions_seen=len(b["sessions"]),
            has_identity=has_id,
            any_gatt=bool(b["gatt"]),
        )

        merge_note: str | None = None
        if identity_kind == "fingerprint" and len(b["all_macs"]) > 1:
            key_bits = "name/mfg/service +UUID-128" if fp == "v2" else "name/mfg/service"
            merge_note = (
                f"Fingerprint ({fp}): merged {len(b['all_macs'])} random BLE addresses "
                f"({key_bits} key; collisions possible)."
            )

        ledger_id = _next_ledger_id(conn)
        for m in b["all_macs"]:
            addr_to_ledger[m] = ledger_id
        conn.execute(
            """
            INSERT INTO ble_devices(
              ledger_id, stable_identity_key, first_seen, last_seen, most_recent_session_id,
              address, address_type, identity_kind, primary_pdu_types, connectable, scannable,
              connection_seen, gatt_seen, smp_seen, encrypted_seen,
              current_name_hint, current_manufacturer_hint, current_service_hint,
              rssi_min, rssi_max, appearance_pattern, probable_device_class, confidence, notes,
              fingerprint_profile
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                ledger_id,
                stable_key_final,
                b["first_seen"],
                b["last_seen"],
                b["recent_session"],
                b["primary_mac"],
                b["addr_type"],
                identity_kind,
                json.dumps(sorted(b["pdus"]), separators=(",", ":")),
                conn_guess,
                scan_guess,
                int(b["connection"]),
                int(b["gatt"]),
                int(b["smp"]),
                int(b["enc"]),
                b["name"],
                b["mfg"],
                b["svc"],
                b["rssi_min"],
                b["rssi_max"],
                pat,
                device,
                confidence,
                merge_note,
                fp,
            ),
        )
        inserted += 1
        for m in b["all_macs"]:
            if m != b["primary_mac"]:
                conn.execute(
                    """
                    INSERT INTO ble_aliases(ledger_id, alias_type, alias_value, first_seen, last_seen)
                    VALUES (?,?,?,?,?)
                    """,
                    (ledger_id, "ble_address", m, b["first_seen"], b["last_seen"]),
                )
        if b["name"]:
            conn.execute(
                """
                INSERT INTO ble_aliases(ledger_id, alias_type, alias_value, first_seen, last_seen)
                VALUES (?,?,?,?,?)
                """,
                (ledger_id, "name", b["name"], b["first_seen"], b["last_seen"]),
            )
        if b["mfg"]:
            conn.execute(
                """
                INSERT INTO ble_aliases(ledger_id, alias_type, alias_value, first_seen, last_seen)
                VALUES (?,?,?,?,?)
                """,
                (ledger_id, "manufacturer", b["mfg"], b["first_seen"], b["last_seen"]),
            )
        if b["svc"]:
            conn.execute(
                """
                INSERT INTO ble_aliases(ledger_id, alias_type, alias_value, first_seen, last_seen)
                VALUES (?,?,?,?,?)
                """,
                (ledger_id, "service_uuid16", b["svc"], b["first_seen"], b["last_seen"]),
            )

    for s in rows:
        addr = s["address"]
        if not addr or addr not in addr_to_ledger:
            continue
        conn.execute(
            """
            UPDATE ble_device_session_summary
            SET ledger_id = ?
            WHERE session_id = ? AND address = ?
            """,
            (addr_to_ledger[addr], s["session_id"], addr),
        )

    conn.commit()
    logger.info("Ledger rebuilt (%d devices)", inserted)
    return inserted


def apply_classify_to_ledger(conn: sqlite3.Connection) -> int:
    """Re-run conservative heuristics on merged device rows (no new merges)."""
    rows = list(conn.execute("SELECT * FROM ble_devices"))
    n = 0
    for d in rows:
        lid = str(d["ledger_id"])
        addr = d["address"] or d["stable_identity_key"]
        macs = _linked_ble_addresses(conn, lid, d["address"])
        if not macs and addr:
            macs = [str(addr)]
        placeholders = ",".join("?" * len(macs)) if macs else ""
        pkt_count = 0
        sessions_seen = 0
        if macs:
            pkt_count = int(
                conn.execute(
                    f"SELECT COUNT(*) FROM ble_observations WHERE address IN ({placeholders})",
                    macs,
                ).fetchone()[0],
            )
            sessions_seen = int(
                conn.execute(
                    f"""
                    SELECT COUNT(DISTINCT session_id) FROM ble_device_session_summary
                    WHERE address IN ({placeholders})
                    """,
                    macs,
                ).fetchone()[0],
            )
        pdus = set()
        try:
            pdus = set(json.loads(d["primary_pdu_types"] or "[]"))
        except json.JSONDecodeError:
            pdus = set()
        rollup = SessionAddressRollup(
            address=str(addr),
            first_seen=d["first_seen"],
            last_seen=d["last_seen"],
            rssi_min=d["rssi_min"],
            rssi_max=d["rssi_max"],
            pdu_types=pdus,
            packet_count=pkt_count,
            name_hints={d["current_name_hint"]} if d["current_name_hint"] else set(),
            manufacturer_hints={d["current_manufacturer_hint"]}
            if d["current_manufacturer_hint"]
            else set(),
            service_hints={d["current_service_hint"]} if d["current_service_hint"] else set(),
            connection_seen=bool(d["connection_seen"]),
            gatt_seen=bool(d["gatt_seen"]),
            smp_seen=bool(d["smp_seen"]),
            encrypted_seen=bool(d["encrypted_seen"]),
            uuid128_hints=set(),
        )
        device_guess, _, conn_guess, scan_guess = classify_from_rollup(rollup)
        device = refine_device_class_with_history(
            device_guess,
            any_gatt=bool(d["gatt_seen"]),
            any_smp=bool(d["smp_seen"]),
            any_enc=bool(d["encrypted_seen"]),
            sessions_seen=sessions_seen,
        )
        has_id = bool(
            d["current_name_hint"] or d["current_manufacturer_hint"] or d["current_service_hint"],
        )
        confidence = confidence_from_evidence(
            packet_count_total=rollup.packet_count,
            sessions_seen=sessions_seen,
            has_identity=has_id,
            any_gatt=bool(d["gatt_seen"]),
        )
        conn.execute(
            """
            UPDATE ble_devices
            SET probable_device_class = ?, confidence = ?, connectable = ?, scannable = ?
            WHERE ledger_id = ?
            """,
            (device, confidence, conn_guess, scan_guess, d["ledger_id"]),
        )
        n += 1
    conn.commit()
    return n
