from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from signalhub.common.timeutil import epoch_to_iso

# DB REAL columns that store Unix epoch seconds (UTC) in signalhub exports.
_EPOCH_CSV_FIELDS = frozenset(
    {
        "started_at",
        "ended_at",
        "imported_at",
        "timestamp",
        "first_seen",
        "last_seen",
    },
)


def row_dict_for_csv(row: Mapping[str, Any]) -> dict[str, str]:
    """Format a DB row for CSV: epoch columns as ISO-8601 UTC, empty string for SQL NULL."""
    out: dict[str, str] = {}
    for key in row.keys():
        val = row[key]
        if val is None:
            out[key] = ""
            continue
        if key in _EPOCH_CSV_FIELDS:
            try:
                ts = float(val)
            except (TypeError, ValueError):
                out[key] = str(val)
            else:
                out[key] = epoch_to_iso(ts)
        else:
            out[key] = str(val)
    return out
