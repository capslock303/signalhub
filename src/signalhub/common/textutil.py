from __future__ import annotations

import re
import unicodedata

# C0 controls except tab; DEL; C1 controls (often show as mojibake in terminals).
_CTRL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]")
# Bidirectional overrides (spoofing / broken rendering in viewers).
_BIDI_FMT_RE = re.compile("[\u202a-\u202e\u2066-\u2069]")
# Unpaired UTF-16 surrogates are not valid in UTF-8 text — drop them (do not insert U+FFFD).
_SURROGATE_RE = re.compile(r"[\ud800-\udfff]")
# Zero-width space and BOM — invisible clutter in tables.
_ZWSP_BOM_RE = re.compile(r"[\u200b\ufeff]")
# Line / paragraph separators (Zl / Zp) — normalize to space before category filter.
_SEP_RE = re.compile(r"[\u2028\u2029]")


def _char_ok_for_ble_display(ch: str) -> bool:
    """Keep letters, marks, numbers, punctuation, symbols, and ordinary spaces.

    Rejects all *C* categories (Cc, Cf, …) except U+200D ZWJ (emoji sequences).
    U+FFFD is *So* (Symbol other); we reject it explicitly so decoder junk never
    appears in Markdown. Variation selectors are *Mn* and pass via *M*.
    """
    if ch == "\ufffd":
        return False
    if ch == "\u200d":  # ZWJ — category Cf
        return True
    cat = unicodedata.category(ch)
    if cat[0] == "C":
        return False
    if cat in ("Zl", "Zp"):
        return False
    if cat[0] in "LMNPS":
        return True
    if cat == "Zs":
        return True
    return False


def sanitize_ble_display_string(value: str | None, *, max_chars: int = 248) -> str | None:
    """Make BLE EIR / tshark user strings safe for SQLite and Markdown.

    BLE names are nominally UTF-8; air captures often contain binary, control
    bytes, U+FFFD from bad decoding, or bidi overrides. This strips non-text
    code points (while keeping letters, numbers, punctuation, symbols, Zs
    spaces, and common emoji mechanics), normalizes NFC, and caps length.
    """
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    s = _SURROGATE_RE.sub("", s)
    try:
        s = unicodedata.normalize("NFC", s)
    except Exception:
        pass
    s = _BIDI_FMT_RE.sub("", s)
    s = _ZWSP_BOM_RE.sub("", s)
    s = _SEP_RE.sub(" ", s)
    s = _CTRL_RE.sub(" ", s)
    s = " ".join(s.split())
    if not s:
        return None
    s = "".join(ch for ch in s if _char_ok_for_ble_display(ch))
    s = " ".join(s.split())
    if not s:
        return None
    if len(s) > max_chars:
        s = s[: max_chars - 1] + "…"
    return s
