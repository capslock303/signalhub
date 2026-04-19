from __future__ import annotations

import json
from collections import defaultdict
from typing import Iterable

import sqlite3

from signalhub.ble.models import SessionAddressRollup


def _row_get(r: sqlite3.Row, key: str) -> object | None:
    try:
        return r[key]
    except (KeyError, IndexError, TypeError):
        return None


def adv_profile_json_for_rollup(r: SessionAddressRollup) -> str | None:
    """Compact JSON of non-empty GAP/AD fields aggregated for one address in a session."""
    if (
        not r.appearance_hints
        and not r.adv_flags_hex
        and not r.uuid128_hints
        and not r.tx_power_dbm_samples
    ):
        return None
    tx = r.tx_power_dbm_samples
    obj: dict = {
        "appearances": sorted(r.appearance_hints),
        "adv_flags_hex": sorted(r.adv_flags_hex),
        "uuid128s": sorted(r.uuid128_hints)[:24],
    }
    if tx:
        obj["tx_power_dbm"] = {"min": min(tx), "max": max(tx)}
    return json.dumps(obj, separators=(",", ":"))


def rollup_session(obs_rows: Iterable[sqlite3.Row]) -> dict[str, SessionAddressRollup]:
    by_addr: dict[str, dict] = defaultdict(
        lambda: {
            "first": None,
            "last": None,
            "rssi_min": None,
            "rssi_max": None,
            "pdus": set(),
            "count": 0,
            "names": set(),
            "mfg": set(),
            "svc": set(),
            "connection": False,
            "gatt": False,
            "smp": False,
            "enc": False,
            "appearance": set(),
            "flags": set(),
            "u128": set(),
            "tx": [],
        },
    )

    for r in obs_rows:
        addr = r["address"]
        if not addr:
            continue
        b = by_addr[addr]
        ts = r["timestamp"]
        if ts is not None:
            b["first"] = ts if b["first"] is None else min(b["first"], ts)
            b["last"] = ts if b["last"] is None else max(b["last"], ts)
        rssi = r["rssi"]
        if rssi is not None:
            b["rssi_min"] = rssi if b["rssi_min"] is None else min(b["rssi_min"], rssi)
            b["rssi_max"] = rssi if b["rssi_max"] is None else max(b["rssi_max"], rssi)
        if r["pdu_type"]:
            b["pdus"].add(str(r["pdu_type"]))
        b["count"] += 1
        if r["name_hint"]:
            b["names"].add(str(r["name_hint"]))
        if r["manufacturer_hint"]:
            b["mfg"].add(str(r["manufacturer_hint"]))
        if r["service_hint"]:
            b["svc"].add(str(r["service_hint"]))
        ap = _row_get(r, "appearance_hint")
        if ap and str(ap).strip():
            b["appearance"].add(str(ap).strip())
        fl = _row_get(r, "adv_flags_hex")
        if fl and str(fl).strip():
            b["flags"].add(str(fl).strip())
        u8 = _row_get(r, "service_uuid128_hint")
        if u8 and str(u8).strip():
            b["u128"].add(str(u8).strip().lower())
        txp = _row_get(r, "tx_power_dbm")
        if txp is not None:
            try:
                b["tx"].append(float(txp))
            except (TypeError, ValueError):
                pass
        b["connection"] = b["connection"] or bool(r["connection_seen"])
        b["gatt"] = b["gatt"] or bool(r["gatt_seen"])
        b["smp"] = b["smp"] or bool(r["smp_seen"])
        b["enc"] = b["enc"] or bool(r["encrypted_seen"])

    out: dict[str, SessionAddressRollup] = {}
    for addr, b in by_addr.items():
        out[addr] = SessionAddressRollup(
            address=addr,
            first_seen=b["first"],
            last_seen=b["last"],
            rssi_min=b["rssi_min"],
            rssi_max=b["rssi_max"],
            pdu_types=set(b["pdus"]),
            packet_count=int(b["count"]),
            name_hints=set(b["names"]),
            manufacturer_hints=set(b["mfg"]),
            service_hints=set(b["svc"]),
            connection_seen=bool(b["connection"]),
            gatt_seen=bool(b["gatt"]),
            smp_seen=bool(b["smp"]),
            encrypted_seen=bool(b["enc"]),
            appearance_hints=set(b["appearance"]),
            adv_flags_hex=set(b["flags"]),
            tx_power_dbm_samples=list(b["tx"]),
            uuid128_hints=set(b["u128"]),
        )
    return out


def appearance_pattern(rollup: SessionAddressRollup) -> str:
    c = rollup.packet_count
    t0 = rollup.first_seen
    t1 = rollup.last_seen
    if c <= 2:
        return "one_off"
    if t0 is None or t1 is None:
        return "intermittent"
    span = max(t1 - t0, 1e-9)
    rate = c / span
    if rate > 8.0 and span < 15.0:
        return "bursty"
    if span >= 60.0 and c >= 15:
        return "persistent"
    return "intermittent"


def pdu_summary_json(rollup: SessionAddressRollup) -> str:
    return json.dumps(sorted(rollup.pdu_types), separators=(",", ":"))
