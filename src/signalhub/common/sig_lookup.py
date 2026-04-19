"""Resolve Bluetooth SIG company IDs and 16-bit GATT service UUIDs (Nordic bluetooth-numbers-database, cached)."""

from __future__ import annotations

import json
import os
import re
import sqlite3
import time
import urllib.error
import urllib.request
from pathlib import Path

_COMPANY_URL = (
    "https://raw.githubusercontent.com/NordicSemiconductor/bluetooth-numbers-database/"
    "master/v1/company_ids.json"
)
_SERVICE_URL = (
    "https://raw.githubusercontent.com/NordicSemiconductor/bluetooth-numbers-database/"
    "master/v1/service_uuids.json"
)
_CACHE_SUBDIR = "sig-numbers"
_MAX_AGE_SEC = 14 * 86400

_company_map: dict[int, str] | None = None
_service_map: dict[str, str] | None = None
_load_lock_time: float = 0.0


def _cache_dir() -> Path:
    base = os.environ.get("LOCALAPPDATA") or os.environ.get("TMP") or "."
    d = Path(base) / "Signalhub" / _CACHE_SUBDIR
    d.mkdir(parents=True, exist_ok=True)
    return d


def _fetch_json(url: str, path: Path) -> None:
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "signalhub-review/1.0 (sig lookup cache)"},
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        path.write_bytes(resp.read())


def _load_cached_json(name: str, url: str) -> list | dict:
    path = _cache_dir() / name
    if path.is_file():
        age = time.time() - path.stat().st_mtime
        if age > _MAX_AGE_SEC:
            try:
                path.unlink()
            except OSError:
                pass
    if not path.is_file():
        try:
            _fetch_json(url, path)
        except (OSError, urllib.error.URLError, TimeoutError):
            if not path.is_file():
                return []
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return []


def _ensure_maps() -> None:
    global _company_map, _service_map
    if _company_map is not None and _service_map is not None:
        return
    companies_raw = _load_cached_json("company_ids.json", _COMPANY_URL)
    services_raw = _load_cached_json("service_uuids.json", _SERVICE_URL)

    cm: dict[int, str] = {}
    if isinstance(companies_raw, list):
        for item in companies_raw:
            if not isinstance(item, dict):
                continue
            try:
                code = int(item.get("code"))
            except (TypeError, ValueError):
                continue
            name = str(item.get("name") or "").strip()
            if name:
                cm[code] = name
    _company_map = cm

    sm: dict[str, str] = {}
    if isinstance(services_raw, list):
        for item in services_raw:
            if not isinstance(item, dict):
                continue
            u = str(item.get("uuid") or "").strip().lower().replace("0x", "")
            name = str(item.get("name") or "").strip()
            if u and name:
                sm[u] = name
    _service_map = sm


def parse_company_id_decimal(hint: str | None) -> int | None:
    """Parse Wireshark-style manufacturer / company id hint to decimal SIG code."""
    if not hint:
        return None
    s = str(hint).strip()
    if not s:
        return None
    s = s.replace("_", " ")
    m = re.search(r"0x\s*([0-9a-fA-F]{1,4})\b", s)
    if m:
        try:
            return int(m.group(1), 16) & 0xFFFF
        except ValueError:
            return None
    m2 = re.fullmatch(r"([0-9a-fA-F]{4})", s.replace(" ", ""))
    if m2:
        try:
            v = int(m2.group(1), 16)
            return ((v & 0xFF) << 8) | (v >> 8) & 0xFF
        except ValueError:
            return None
    m3 = re.search(r"\b(\d{1,5})\b", s)
    if m3:
        try:
            v = int(m3.group(1))
            if 0 <= v <= 0xFFFF:
                return v
        except ValueError:
            return None
    return None


def parse_uuid16(hint: str | None) -> str | None:
    """Return normalized 4-hex lowercase uuid16 key, or None."""
    if not hint:
        return None
    s = str(hint).strip().lower()
    m = re.search(r"0x\s*([0-9a-f]{1,4})\b", s)
    if m:
        return m.group(1).zfill(4)
    m2 = re.fullmatch(r"([0-9a-f]{4})", s.replace(" ", ""))
    if m2:
        return m2.group(1).zfill(4)
    m3 = re.search(r"\b([0-9a-f]{4})\b", s)
    if m3:
        return m3.group(1).zfill(4)
    return None


def company_name_for_hint(manufacturer_hint: str | None) -> str | None:
    """Public SIG company name for a decoded manufacturer/company_id field."""
    cid = parse_company_id_decimal(manufacturer_hint)
    if cid is None:
        return None
    _ensure_maps()
    assert _company_map is not None
    return _company_map.get(cid)


def service_name_for_hint(service_hint: str | None) -> str | None:
    """Human-readable GATT service name for a 16-bit UUID hint."""
    u = parse_uuid16(service_hint)
    if not u:
        return None
    _ensure_maps()
    assert _service_map is not None
    return _service_map.get(u)


def distinct_company_labels(
    conn: sqlite3.Connection,
    start: float,
    end: float,
    *,
    limit: int = 40,
) -> list[tuple[str, str | None, str | None]]:
    """Rows: (raw_manufacturer_hint, resolved_name or None, sample_address)."""
    rows = conn.execute(
        """
        SELECT TRIM(manufacturer_hint) AS mfg, MIN(address) AS addr
        FROM ble_observations
        WHERE timestamp IS NOT NULL AND timestamp >= ? AND timestamp <= ?
          AND manufacturer_hint IS NOT NULL AND LENGTH(TRIM(manufacturer_hint)) > 0
        GROUP BY LOWER(TRIM(manufacturer_hint))
        ORDER BY MAX(timestamp) DESC
        LIMIT ?
        """,
        (start, end, int(limit)),
    ).fetchall()
    out: list[tuple[str, str | None, str | None]] = []
    for r in rows:
        raw = str(r["mfg"] or "")
        resolved = company_name_for_hint(raw)
        out.append((raw, resolved, r["addr"]))
    return out


def distinct_service_labels(
    conn: sqlite3.Connection,
    start: float,
    end: float,
    *,
    limit: int = 40,
) -> list[tuple[str, str | None, str | None]]:
    """Rows: (raw_service_hint, resolved_name or None, sample_address)."""
    rows = conn.execute(
        """
        SELECT TRIM(service_hint) AS svc, MIN(address) AS addr
        FROM ble_observations
        WHERE timestamp IS NOT NULL AND timestamp >= ? AND timestamp <= ?
          AND service_hint IS NOT NULL AND LENGTH(TRIM(service_hint)) > 0
        GROUP BY LOWER(TRIM(service_hint))
        ORDER BY MAX(timestamp) DESC
        LIMIT ?
        """,
        (start, end, int(limit)),
    ).fetchall()
    out: list[tuple[str, str | None, str | None]] = []
    for r in rows:
        raw = str(r["svc"] or "")
        resolved = service_name_for_hint(raw)
        out.append((raw, resolved, r["addr"]))
    return out
