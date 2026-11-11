"""Validate and normalize NYC 311 records into the Kafka message contract."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from nyc311_stream.nyc311 import NYC311Error, parse_api_timestamp

SCHEMA_VERSION = 1


class InvalidRecord(ValueError):
    """An NYC 311 record cannot be normalized safely."""


def _optional_text(record: dict[str, Any], key: str) -> str | None:
    value = record.get(key)
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _required_text(record: dict[str, Any], key: str) -> str:
    value = _optional_text(record, key)
    if value is None:
        raise InvalidRecord(f"missing required field: {key}")
    return value


def _timestamp(record: dict[str, Any], key: str, *, required: bool = False) -> str | None:
    value = _optional_text(record, key)
    if value is None:
        if required:
            raise InvalidRecord(f"missing required field: {key}")
        return None
    try:
        parsed = parse_api_timestamp(value)
    except NYC311Error:
        raise InvalidRecord(f"invalid timestamp in field: {key}") from None
    return parsed.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _optional_float(record: dict[str, Any], key: str) -> float | None:
    value = _optional_text(record, key)
    if value is None:
        return None
    try:
        return float(value)
    except ValueError:
        raise InvalidRecord(f"invalid numeric field: {key}") from None


def format_request(raw: dict[str, Any], now: datetime | None = None) -> dict[str, Any]:
    """Return a stable, typed message for one NYC 311 service request."""
    if not isinstance(raw, dict):
        raise InvalidRecord(f"record is not an object: {type(raw).__name__}")

    created_date = _timestamp(raw, "created_date", required=True)
    if now is not None:
        # `now` is accepted for deterministic tests and future audit fields.
        now.astimezone(UTC)
    return {
        "schema_version": SCHEMA_VERSION,
        "unique_key": _required_text(raw, "unique_key"),
        "created_date": created_date,
        "closed_date": _timestamp(raw, "closed_date"),
        "agency": _required_text(raw, "agency"),
        "agency_name": _optional_text(raw, "agency_name"),
        "complaint_type": _required_text(raw, "complaint_type"),
        "descriptor": _optional_text(raw, "descriptor"),
        "location_type": _optional_text(raw, "location_type"),
        "incident_zip": _optional_text(raw, "incident_zip"),
        "city": _optional_text(raw, "city"),
        "borough": _optional_text(raw, "borough"),
        "status": _required_text(raw, "status"),
        "due_date": _timestamp(raw, "due_date"),
        "resolution_description": _optional_text(raw, "resolution_description"),
        "latitude": _optional_float(raw, "latitude"),
        "longitude": _optional_float(raw, "longitude"),
        "community_board": _optional_text(raw, "community_board"),
        "council_district": _optional_text(raw, "council_district"),
        "event_time": created_date,
    }
