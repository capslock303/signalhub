from __future__ import annotations

import datetime as _dt
from typing import Union

EpochLike = Union[float, int, str, None]


def parse_epoch(value: EpochLike) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def epoch_to_iso(epoch: float | None) -> str:
    if epoch is None:
        return ""
    return _dt.datetime.fromtimestamp(epoch, tz=_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_date(s: str) -> _dt.date:
    return _dt.date.fromisoformat(s.strip())


def utc_today_iso() -> str:
    """Current UTC calendar date as YYYY-MM-DD."""
    return _dt.datetime.now(tz=_dt.timezone.utc).date().isoformat()


def utc_day_range_epoch(from_date: str, to_date: str) -> tuple[float, float]:
    """Inclusive UTC day range [start, end] as Unix timestamps for filtering."""
    d0 = parse_date(from_date)
    d1 = parse_date(to_date)
    start = _dt.datetime(d0.year, d0.month, d0.day, tzinfo=_dt.timezone.utc).timestamp()
    end = _dt.datetime(d1.year, d1.month, d1.day, 23, 59, 59, tzinfo=_dt.timezone.utc).timestamp()
    return start, end
