"""Resolve IEEE OUI / vendor hints for MAC addresses (Wireshark ``manuf`` + optional API fallback)."""

from __future__ import annotations

import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

_MANUF_URL = "https://www.wireshark.org/download/automated/data/manuf"
_CACHE_NAME = "wireshark-manuf.txt"
_MAX_AGE_SEC = 30 * 86400
_HTTP_CACHE: dict[str, tuple[float, str | None]] = {}
_HTTP_TTL_SEC = 86400.0


def _cache_dir() -> Path:
    base = os.environ.get("LOCALAPPDATA") or os.environ.get("TMP") or "."
    d = Path(base) / "Signalhub" / "vendor-cache"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _oui_key_from_mac(mac: str | None) -> str | None:
    if not mac:
        return None
    m = re.findall(r"[0-9a-fA-F]{2}", mac)
    if len(m) < 3:
        return None
    return (m[0] + m[1] + m[2]).upper()


def _load_manuf_map() -> dict[str, str]:
    """Parse Wireshark ``manuf`` (24-bit prefixes)."""
    path = _cache_dir() / _CACHE_NAME
    if path.is_file():
        age = time.time() - path.stat().st_mtime
        if age > _MAX_AGE_SEC:
            try:
                path.unlink()
            except OSError:
                pass
    if not path.is_file():
        req = urllib.request.Request(
            _MANUF_URL,
            headers={"User-Agent": "signalhub-review/1.0 (manuf cache)"},
            method="GET",
        )
        with urllib.request.urlopen(req, timeout=120) as resp:
            path.write_bytes(resp.read())
    raw = path.read_text(encoding="utf-8", errors="replace")
    out: dict[str, str] = {}
    for line in raw.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        m = re.match(
            r"^([0-9a-fA-F]{2}:[0-9a-fA-F]{2}:[0-9a-fA-F]{2})\s+(\S+)\s+(.*)$",
            line,
        )
        if not m:
            continue
        key = m.group(1).replace(":", "").upper()
        long_name = m.group(3).strip()
        if long_name:
            out[key] = long_name
    return out


_map_singleton: dict[str, str] | None = None


def vendor_for_mac(mac: str | None, *, use_http_fallback: bool = True) -> str | None:
    """Return a public-registry vendor string, or None if unknown."""
    global _map_singleton
    key = _oui_key_from_mac(mac)
    if not key:
        return None
    if _map_singleton is None:
        try:
            _map_singleton = _load_manuf_map()
        except (OSError, urllib.error.URLError, TimeoutError):
            _map_singleton = {}
    v = _map_singleton.get(key)
    if v:
        return v
    if not use_http_fallback:
        return None
    now = time.time()
    ck = mac.strip().lower()
    hit = _HTTP_CACHE.get(ck)
    if hit and now - hit[0] < _HTTP_TTL_SEC:
        return hit[1]
    try:
        url = "https://api.macvendors.com/" + urllib.parse.quote(ck, safe="")
        req = urllib.request.Request(url, headers={"User-Agent": "signalhub-review/1.0"}, method="GET")
        with urllib.request.urlopen(req, timeout=8) as resp:
            body = resp.read().decode("utf-8", errors="replace").strip()
        if body and "not found" not in body.lower() and "errors" not in body.lower():
            _HTTP_CACHE[ck] = (now, body)
            return body
    except Exception:
        pass
    _HTTP_CACHE[ck] = (now, None)
    return None
