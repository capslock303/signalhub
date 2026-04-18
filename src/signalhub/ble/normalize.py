from __future__ import annotations

import json
from collections import defaultdict
from typing import Iterable

import sqlite3

from signalhub.ble.models import SessionAddressRollup


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
