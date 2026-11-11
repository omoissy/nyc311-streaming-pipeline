"""Validate raw randomuser.me records and flatten them into the Kafka message contract.

The message fields must stay in sync with USER_SCHEMA in spark/app/stream_to_cassandra.py;
tests/test_stream_parsing.py enforces that.
"""

from __future__ import annotations

import re
import uuid
from datetime import UTC, datetime
from typing import Any

SCHEMA_VERSION = 1
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


class InvalidRecord(ValueError):
    """A raw record cannot be turned into a valid message."""


def _lookup(record: dict[str, Any], path: tuple[str, ...]) -> Any:
    value: Any = record
    for key in path:
        if not isinstance(value, dict) or key not in value:
            return None
        value = value[key]
    return value


def _optional_text(record: dict[str, Any], *path: str) -> str | None:
    value = _lookup(record, path)
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _required_text(record: dict[str, Any], *path: str) -> str:
    text = _optional_text(record, *path)
    if text is None:
        raise InvalidRecord(f"missing required field: {'.'.join(path)}")
    return text


def format_user(raw: dict[str, Any], now: datetime | None = None) -> dict[str, Any]:
    """Turn one raw API record into a flat, validated message. Raises InvalidRecord."""
    if not isinstance(raw, dict):
        raise InvalidRecord(f"record is not an object: {type(raw).__name__}")

    raw_id = _required_text(raw, "login", "uuid")
    try:
        user_id = str(uuid.UUID(raw_id))
    except ValueError:
        raise InvalidRecord(f"login.uuid is not a valid UUID: {raw_id!r}") from None

    email = _required_text(raw, "email").lower()
    if not EMAIL_RE.match(email):
        raise InvalidRecord(f"email is not valid: {email!r}")

    street = " ".join(
        part
        for part in (
            _optional_text(raw, "location", "street", "number"),
            _optional_text(raw, "location", "street", "name"),
        )
        if part
    )
    address_parts = (
        street,
        _optional_text(raw, "location", "city"),
        _optional_text(raw, "location", "state"),
        _optional_text(raw, "location", "country"),
    )
    address = ", ".join(part for part in address_parts if part) or None

    event_time = (now or datetime.now(UTC)).isoformat(timespec="milliseconds")

    return {
        "schema_version": SCHEMA_VERSION,
        "id": user_id,
        "first_name": _required_text(raw, "name", "first"),
        "last_name": _required_text(raw, "name", "last"),
        "gender": _optional_text(raw, "gender"),
        "address": address,
        # Postcodes come back as ints for some nationalities and strings for others.
        "post_code": _optional_text(raw, "location", "postcode"),
        "email": email,
        "username": _optional_text(raw, "login", "username"),
        "dob": _optional_text(raw, "dob", "date"),
        "registered_date": _optional_text(raw, "registered", "date"),
        "phone": _optional_text(raw, "phone"),
        "picture": _optional_text(raw, "picture", "medium"),
        "nationality": _optional_text(raw, "nat"),
        "event_time": event_time,
    }
