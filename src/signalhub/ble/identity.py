from __future__ import annotations

import hashlib
import re
from typing import FrozenSet

from signalhub.common.textutil import sanitize_ble_display_string

_MAC_NORM_RE = re.compile(r"[^0-9a-fA-F]")


def normalize_mac_for_key(address: str) -> str:
    """Lowercase hex-only MAC for stable `mac:` keys."""
    hexonly = _MAC_NORM_RE.sub("", address or "")
    if len(hexonly) == 12:
        pairs = ":".join(hexonly[i : i + 2] for i in range(0, 12, 2))
        return pairs.lower()
    return (address or "").strip().lower()


def mac_identity_key(address: str) -> str:
    return f"mac:{normalize_mac_for_key(address)}"


def fingerprint_eligible(
    *,
    address_type: str | None,
    name: str | None,
    manufacturer: str | None,
    service: str | None,
) -> bool:
    """Whether we may merge random addresses using a name+mfg/service fingerprint.

    Conservative: only random (advertised) addresses, non-trivial name, and
    at least one structured hint to reduce collisions on generic names.
    """
    at = (address_type or "unknown").strip().lower()
    if at != "random":
        return False
    n = sanitize_ble_display_string(name) or ""
    if len(n) < 6:
        return False
    if not manufacturer and not service:
        return False
    return True


def fingerprint_eligible_v2(
    *,
    address_type: str | None,
    name: str | None,
    manufacturer: str | None,
    service: str | None,
    uuid128s: FrozenSet[str],
) -> bool:
    """v2: same as v1, or strong name + ≥1 decoded 128-bit UUID (still random MAC only)."""
    if fingerprint_eligible(
        address_type=address_type,
        name=name,
        manufacturer=manufacturer,
        service=service,
    ):
        return True
    at = (address_type or "unknown").strip().lower()
    if at != "random":
        return False
    n = sanitize_ble_display_string(name) or ""
    if len(n) < 8:
        return False
    if not uuid128s:
        return False
    return True


def fingerprint_identity_key_v1(
    *,
    name: str | None,
    manufacturer: str | None,
    service: str | None,
) -> str:
    """Stable key for fingerprint-clustered devices (versioned prefix)."""
    n = sanitize_ble_display_string(name) or ""
    m = str(manufacturer or "").strip()
    s = str(service or "").strip()
    payload = f"v1\0{n}\0{m}\0{s}".encode("utf-8", errors="surrogatepass")
    digest = hashlib.sha256(payload).hexdigest()[:24]
    return f"fp:v1:{digest}"


def fingerprint_identity_key_v2(
    *,
    name: str | None,
    manufacturer: str | None,
    service: str | None,
    uuid128s: FrozenSet[str],
) -> str:
    """Like v1 but folds sorted 128-bit UUID hints into the hash (fewer false merges)."""
    n = sanitize_ble_display_string(name) or ""
    m = str(manufacturer or "").strip()
    s = str(service or "").strip()
    uu = "\n".join(sorted(uuid128s))
    payload = f"v2\0{n}\0{m}\0{s}\0{uu}".encode("utf-8", errors="surrogatepass")
    digest = hashlib.sha256(payload).hexdigest()[:24]
    return f"fp:v2:{digest}"


def stable_identity_key_and_kind(
    *,
    address: str,
    address_type: str | None,
    name: str | None,
    manufacturer: str | None,
    service: str | None,
    uuid128s: FrozenSet[str] | None = None,
    fingerprint_profile: str = "v1",
) -> tuple[str, str]:
    """Return (stable_identity_key, identity_kind) with identity_kind mac|fingerprint."""
    u128 = uuid128s or frozenset()
    fp = (fingerprint_profile or "v1").strip().lower()
    if fp not in ("v1", "v2"):
        fp = "v1"

    at = (address_type or "unknown").strip().lower()
    if at == "public" or at == "unknown":
        return mac_identity_key(address), "mac"

    if fp == "v2":
        if fingerprint_eligible_v2(
            address_type=address_type,
            name=name,
            manufacturer=manufacturer,
            service=service,
            uuid128s=u128,
        ):
            return (
                fingerprint_identity_key_v2(
                    name=name,
                    manufacturer=manufacturer,
                    service=service,
                    uuid128s=u128,
                ),
                "fingerprint",
            )
    elif fingerprint_eligible(
        address_type=address_type,
        name=name,
        manufacturer=manufacturer,
        service=service,
    ):
        return (
            fingerprint_identity_key_v1(
                name=name,
                manufacturer=manufacturer,
                service=service,
            ),
            "fingerprint",
        )

    return mac_identity_key(address), "mac"
