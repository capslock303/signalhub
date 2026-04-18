from __future__ import annotations

import re
from typing import Any

from signalhub.ble.models import ObservationRow
from signalhub.ble.tshark import resolve_fields
from signalhub.common.textutil import sanitize_ble_display_string
from signalhub.common.timeutil import parse_epoch

_ADDR_RE = re.compile(r"^[0-9a-f]{1,2}(:[0-9a-f]{1,2}){5}$")

# btle.advertising_header.pdu_type is numeric on Wireshark 4.6+; map 4-bit common advertising PDUs.
_BLE_ADV_PDU_NAMES: dict[str, str] = {
    "0": "ADV_IND",
    "1": "ADV_DIRECT_IND",
    "2": "ADV_NONCONN_IND",
    "3": "SCAN_REQ",
    "4": "SCAN_RSP",
    "5": "CONNECT_IND",
    "6": "ADV_SCAN_IND",
    "0x0": "ADV_IND",
    "0x1": "ADV_DIRECT_IND",
    "0x2": "ADV_NONCONN_IND",
    "0x3": "SCAN_REQ",
    "0x4": "SCAN_RSP",
    "0x5": "CONNECT_IND",
    "0x6": "ADV_SCAN_IND",
}


def _normalize_adv_pdu(raw: str) -> str:
    s = (raw or "").strip()
    if not s:
        return ""
    if any(x in s.upper() for x in ("ADV_", "SCAN_", "CONNECT")):
        return s
    key = s.lower()
    if key in _BLE_ADV_PDU_NAMES:
        return _BLE_ADV_PDU_NAMES[key]
    try:
        n = int(s, 0) & 0xF
        return _BLE_ADV_PDU_NAMES.get(str(n), _BLE_ADV_PDU_NAMES.get(hex(n), s))
    except ValueError:
        return s


def normalize_address(addr: str | None) -> str | None:
    if not addr:
        return None
    s = addr.strip().lower().replace("-", ":")
    parts = [p.zfill(2) for p in s.split(":") if p]
    if len(parts) != 6:
        return None
    out = ":".join(parts)
    if not _ADDR_RE.match(out):
        return None
    return out


def row_from_tshark_dict(d: dict[str, str]) -> ObservationRow | None:
    """Map tshark field dict to ObservationRow; returns None if no usable address."""

    def col(name: str) -> str:
        return (d.get(name) or "").strip()

    def col_any(*names: str) -> str:
        for n in names:
            v = col(n)
            if v:
                return v
        return ""

    adv = normalize_address(col("btle.advertising_address"))
    scan = normalize_address(col("btle.scanning_address"))
    addr = adv or scan
    if not addr:
        return None

    protocols = col("frame.protocols")
    pdu_raw = col_any(
        "btle.advertising_pdu_type",
        "btle.advertising_header.pdu_type",
    )
    pdu = _normalize_adv_pdu(pdu_raw)
    rssi_raw = col_any("nordic_ble.rssi", "btle.rssi")
    rssi_val: float | None
    try:
        rssi_val = float(rssi_raw) if rssi_raw else None
    except ValueError:
        rssi_val = parse_epoch(rssi_raw)

    ts = parse_epoch(col("frame.time_epoch"))
    addr_type_raw = col_any(
        "btle.advertising_address_type",
        "bluetooth.addr_type",
    ).lower()
    if not addr_type_raw:
        addr_type = "unknown"
    elif "random" in addr_type_raw:
        addr_type = "random"
    elif "public" in addr_type_raw:
        addr_type = "public"
    else:
        addr_type = addr_type_raw or "unknown"

    known = set(resolve_fields())
    extras = {k: v for k, v in d.items() if k not in known}

    return ObservationRow(
        frame_number=col("frame.number") or None,
        time_epoch=ts,
        frame_protocols=protocols or None,
        advertising_address=adv,
        scanning_address=scan or None,
        pdu_type=pdu or None,
        rssi=rssi_val,
        device_name=sanitize_ble_display_string(
            col_any(
                "btcommon.eir_ad.entry.device_name",
                "btcommon.eir.entry.device_name",
            )
            or None,
        ),
        company_id=col_any(
            "btcommon.eir_ad.entry.company_id",
            "btcommon.eir.company_id",
        )
        or None,
        uuid16=col_any(
            "btcommon.eir_ad.entry.uuid_16",
            "btcommon.eir.uuid_16",
        )
        or None,
        encrypted_flag=None,
        address_type=addr_type,
        extras=extras,
    )


def observation_flags(row: ObservationRow) -> dict[str, Any]:
    pdu = (row.pdu_type or "").upper()
    connection = "CONNECT" in pdu
    protocols = (row.frame_protocols or "").lower()
    gatt = "btatt" in protocols
    smp = "btsmp" in protocols
    enc = bool(row.encrypted_flag and row.encrypted_flag.strip() in ("1", "True", "true"))
    return {
        "connection_seen": 1 if connection else 0,
        "gatt_seen": 1 if gatt else 0,
        "smp_seen": 1 if smp else 0,
        "encrypted_seen": 1 if enc else 0,
    }
