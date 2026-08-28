"""Watermark parsing, formatting, lookback, and computation helpers.

A "watermark" is the value used to ask the API for only new/changed records
since the last successful run (e.g. `?updated_since=2024-01-01T00:00:00Z`).
These helpers keep that logic independent of Spark and the HTTP layer so it
can be unit tested in isolation.
"""

import re
from datetime import datetime, timedelta
from typing import Optional

_ISO8601_DURATION = re.compile(
    r"^P(?:(?P<days>\d+)D)?"
    r"(?:T(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+)S)?)?$"
)


def parse_iso8601_duration(duration: str) -> timedelta:
    """Parse a subset of ISO8601 durations used for lookback windows,
    e.g. ``PT15M``, ``P1D``, ``PT1H30M``."""
    match = _ISO8601_DURATION.match(duration)
    if not match or duration in ("P", "PT"):
        raise ValueError(f"Unsupported ISO8601 duration: {duration!r}")
    parts = {k: int(v) if v else 0 for k, v in match.groupdict().items()}
    return timedelta(
        days=parts["days"],
        hours=parts["hours"],
        minutes=parts["minutes"],
        seconds=parts["seconds"],
    )


def parse_value(value: str, value_type: str, date_format: Optional[str] = None):
    """Parse a stored/serialized watermark string back into a comparable value."""
    if value is None:
        return None
    if value_type == "datetime":
        return (
            datetime.strptime(value, date_format)
            if date_format
            else datetime.fromisoformat(value)
        )
    if value_type == "integer":
        return int(value)
    return value  # opaque string / cursor — compared lexicographically


def format_value(value, value_type: str, date_format: Optional[str] = None) -> str:
    """Serialize a comparable value back to the string form stored in the DB
    and sent as a request parameter."""
    if value_type == "datetime":
        return value.strftime(date_format) if date_format else value.isoformat()
    return str(value)


def apply_lookback(
    watermark: str,
    lookback: Optional[str],
    value_type: str,
    date_format: Optional[str] = None,
) -> str:
    """Rewind a datetime watermark by a small overlap window before using it
    in the next request, to tolerate clock skew / late-arriving records
    without re-reading the full history. No-op for non-datetime watermarks."""
    if not lookback or value_type != "datetime":
        return watermark
    parsed = parse_value(watermark, value_type, date_format)
    adjusted = parsed - parse_iso8601_duration(lookback)
    return format_value(adjusted, value_type, date_format)


def compute_next_watermark(
    records,
    watermark_column: str,
    value_type: str,
    date_format: Optional[str] = None,
    fallback: Optional[str] = None,
) -> Optional[str]:
    """Scan fetched records for the maximum value of ``watermark_column`` and
    return it serialized for storage. Falls back to the previous watermark if
    no records were returned or none contain a usable value (e.g. an empty
    page means "nothing new", not "reset to the beginning")."""
    if not records:
        return fallback

    parsed_values = []
    for record in records:
        raw_value = record.get(watermark_column) if isinstance(record, dict) else None
        if raw_value is None:
            continue
        try:
            parsed_values.append(parse_value(str(raw_value), value_type, date_format))
        except (ValueError, TypeError):
            continue

    if not parsed_values:
        return fallback

    return format_value(max(parsed_values), value_type, date_format)
