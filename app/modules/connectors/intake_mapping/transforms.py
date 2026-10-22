"""Value transforms shared by upload landing and the connector worker (stdlib only)."""

from __future__ import annotations

import hashlib
import hmac
import re
from collections.abc import Callable
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

# Python 3.10 (landing Lambda) has no datetime.UTC alias, so timezone.utc stays.
UTC = timezone.utc  # noqa: UP017
NULL_TOKENS = frozenset({"", "-", "--", "n/a", "na", "null", "none", "#n/a", "#value!", "#ref!",
                         "#div/0!"})


class TransformError(ValueError):
    pass


def clean(value: Any) -> Any:
    """Source cell -> python value; spreadsheet placeholders become None."""
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        return None if text.lower() in NULL_TOKENS else text
    return value


def _text(value: Any) -> str:
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


def digits(value: Any) -> str | None:
    out = re.sub(r"[^0-9]", "", _text(value))
    return out or None


def th_mobile(value: Any) -> str | None:
    d = re.sub(r"[^0-9]", "", _text(value))
    if re.fullmatch(r"0[689][0-9]{8}", d):
        return d
    if re.fullmatch(r"66[689][0-9]{8}", d):
        return "0" + d[2:]
    if re.fullmatch(r"[689][0-9]{8}", d):
        return "0" + d
    return None


def to_number(value: Any) -> str:
    if isinstance(value, (int, Decimal)) or (isinstance(value, float) and value == value):
        return format(Decimal(repr(value)) if isinstance(value, float) else Decimal(value), "f")
    text = re.sub(r"[,\s฿]", "", _text(value))
    try:
        number = Decimal(text)
    except InvalidOperation:
        raise TransformError("not a number") from None
    if not number.is_finite():
        raise TransformError("not a number")
    return format(number, "f")


_DATE_FORMATS = ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d", "%d/%m/%y", "%Y%m%d")
_TS_FORMATS = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%d/%m/%Y %H:%M:%S",
               "%m/%d/%Y %I:%M:%S %p", "%d/%m/%Y %H:%M", "%Y-%m-%d %H:%M")


def _fix_buddhist(d: date) -> date:
    return d.replace(year=d.year - 543) if d.year > 2400 else d


def parse_date(value: Any) -> str:
    if isinstance(value, datetime):
        return _fix_buddhist(value.date()).isoformat()
    if isinstance(value, date):
        return _fix_buddhist(value).isoformat()
    text = _text(value).strip().split(" ")[0]
    for fmt in _DATE_FORMATS:
        try:
            return _fix_buddhist(datetime.strptime(text, fmt).date()).isoformat()
        except ValueError:
            continue
    raise TransformError("unrecognised date")


def parse_timestamp(value: Any, tz_offset_hours: float = 7.0) -> str:
    """Naive source timestamps are local time (default Asia/Bangkok, UTC+7); output is UTC ISO."""
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, date):
        dt = datetime(value.year, value.month, value.day)
    else:
        text, dt = _text(value).strip(), None
        for fmt in _TS_FORMATS:
            try:
                dt = datetime.strptime(text, fmt)
                break
            except ValueError:
                continue
        if dt is None:
            dt = datetime.strptime(parse_date(text), "%Y-%m-%d")
    if dt.year > 2400:
        dt = dt.replace(year=dt.year - 543)
    if dt.tzinfo is None:
        dt = (dt - timedelta(hours=tz_offset_hours)).replace(tzinfo=UTC)
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


class Transforms:
    """Named transforms; `hmac` needs a per-tenant secret so hashed IDs cannot be brute-forced."""

    def __init__(self, hmac_key: bytes | None = None, tz_offset_hours: float = 7.0):
        self.hmac_key = hmac_key
        self.tz_offset_hours = tz_offset_hours
        self._table: dict[str, Callable[[Any], Any]] = {
            "trim": lambda v: _text(v).strip(),
            "lowercase": lambda v: _text(v).lower(),
            "uppercase": lambda v: _text(v).upper(),
            "stringify": _text,
            "digits": digits,
            "th_mobile": th_mobile,
            "to_number": to_number,
            "parse_date": parse_date,
            "parse_timestamp": lambda v: parse_timestamp(v, self.tz_offset_hours),
            "nospace": lambda v: re.sub(r"\s+", "", _text(v)),
            "alnum": lambda v: re.sub(r"[^0-9A-Za-z]", "", _text(v)).upper() or None,
            "hmac": self._hmac,
        }

    @property
    def names(self) -> frozenset[str]:
        return frozenset(self._table)

    def _hmac(self, value: Any) -> str:
        if not self.hmac_key:
            raise TransformError("hmac transform needs a tenant key")
        return hmac.new(self.hmac_key, _text(value).encode(), hashlib.sha256).hexdigest()

    def apply(self, value: Any, names: list[str]) -> Any:
        for name in names:
            if value is None:
                return None
            if name not in self._table:
                raise TransformError(f"unknown transform {name}")
            value = self._table[name](value)
        return value
