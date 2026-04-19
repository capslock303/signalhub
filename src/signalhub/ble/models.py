from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class ObservationRow:
    frame_number: str | None = None
    time_epoch: float | None = None
    frame_protocols: str | None = None
    advertising_address: str | None = None
    scanning_address: str | None = None
    pdu_type: str | None = None
    rssi: float | None = None
    device_name: str | None = None
    company_id: str | None = None
    uuid16: str | None = None
    uuid128: str | None = None
    appearance: str | None = None
    adv_flags_hex: str | None = None
    tx_power_dbm: float | None = None
    encrypted_flag: str | None = None
    address_type: str | None = None
    extras: dict[str, Any] = field(default_factory=dict)


@dataclass
class SessionAddressRollup:
    address: str
    first_seen: float | None
    last_seen: float | None
    rssi_min: float | None
    rssi_max: float | None
    pdu_types: set[str]
    packet_count: int
    name_hints: set[str]
    manufacturer_hints: set[str]
    service_hints: set[str]
    connection_seen: bool
    gatt_seen: bool
    smp_seen: bool
    encrypted_seen: bool
    appearance_hints: set[str] = field(default_factory=set)
    adv_flags_hex: set[str] = field(default_factory=set)
    tx_power_dbm_samples: list[float] = field(default_factory=list)
    uuid128_hints: set[str] = field(default_factory=set)
