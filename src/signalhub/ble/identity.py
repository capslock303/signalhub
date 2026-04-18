from __future__ import annotations

import hashlib
import re

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


def stable_identity_key_and_kind(
    *,
    address: str,
    address_type: str | None,
    name: str | None,
    manufacturer: str | None,
    service: str | None,
) -> tuple[str, str]:
    """Return (stable_identity_key, identity_kind) with identity_kind mac|fingerprint."""
    at = (address_type or "unknown").strip().lower()
    if at == "public" or at == "unknown":
        return mac_identity_key(address), "mac"
    if fingerprint_eligible(
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
