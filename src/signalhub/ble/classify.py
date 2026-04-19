from __future__ import annotations

from signalhub.ble.models import SessionAddressRollup


def infer_connectable_scannable(pdu_types: set[str]) -> tuple[str, str]:
    blob = " ".join(sorted(pdu_types)).upper()
    connectable = "unknown"
    scannable = "unknown"
    if "ADV_IND" in blob or "ADV_DIRECT_IND" in blob or "ADV_DIRECT" in blob:
        connectable = "yes"
    elif "ADV_NONCONN_IND" in blob or "NONCONN" in blob:
        connectable = "no"
    if "SCAN_RSP" in blob or "ADV_SCAN_IND" in blob or "AUX_SCAN_RSP" in blob:
        scannable = "yes"
    elif connectable == "no" and ("ADV_NONCONN" in blob or "NONCONN" in blob):
        scannable = "no"
    return connectable, scannable


def classify_from_rollup(rollup: SessionAddressRollup) -> tuple[str, str, str, str]:
    """Return probable_device_class, confidence, connectable, scannable (controlled vocab)."""
    pdus = rollup.pdu_types
    blob = " ".join(sorted(pdus)).upper()
    connectable, scannable = infer_connectable_scannable(pdus)

    if rollup.gatt_seen:
        device = "accessory"
    elif "NONCONN" in blob and "SCAN_RSP" not in blob and "ADV_IND" not in blob:
        device = "beacon"
    elif rollup.connection_seen or "CONNECT" in blob:
        device = "connectable_device"
    else:
        device = "unknown"

    has_identity = bool(
        rollup.name_hints
        or rollup.manufacturer_hints
        or rollup.service_hints
        or rollup.uuid128_hints
    )
    if has_identity and rollup.packet_count >= 5:
        confidence = "high"
    elif rollup.packet_count >= 15 or (has_identity and rollup.packet_count >= 3):
        confidence = "medium"
    else:
        confidence = "low"

    return device, confidence, connectable, scannable


def refine_device_class_with_history(
    probable: str,
    *,
    any_gatt: bool,
    any_smp: bool,
    any_enc: bool,
    sessions_seen: int,
) -> str:
    """Optional second pass after cross-session merge (still conservative)."""
    if any_gatt:
        return "accessory"
    if any_smp:
        return "pairing_or_security_activity"
    if probable != "unknown" and sessions_seen >= 3:
        return probable
    return probable


def confidence_from_evidence(
    *,
    packet_count_total: int,
    sessions_seen: int,
    has_identity: bool,
    any_gatt: bool,
) -> str:
    if has_identity and packet_count_total >= 20 and sessions_seen >= 2:
        return "high"
    if packet_count_total >= 30 or (has_identity and packet_count_total >= 10):
        return "medium"
    return "low"
