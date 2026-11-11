"""Resilient client for the official NYC Open Data 311 Socrata API."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

RETRY_STATUSES = (429, 500, 502, 503, 504)


class NYC311Error(RuntimeError):
    """The API returned an invalid or unusable response."""


@dataclass(frozen=True, order=True)
class Cursor:
    created_date: datetime
    unique_key: str


def build_session(max_retries: int = 5, backoff_factor: float = 1.0) -> requests.Session:
    retry = Retry(
        total=max_retries,
        backoff_factor=backoff_factor,
        status_forcelist=RETRY_STATUSES,
        allowed_methods=frozenset({"GET"}),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers["User-Agent"] = "omoissy-nyc311-streaming-pipeline/2.0"
    return session


def parse_api_timestamp(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise NYC311Error(f"invalid NYC 311 timestamp: {value!r}") from exc
    return (parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)).astimezone(UTC)


def cursor_for_record(record: dict[str, Any]) -> Cursor | None:
    key = record.get("unique_key")
    created = record.get("created_date")
    if not key or not created:
        return None
    try:
        timestamp = parse_api_timestamp(str(created))
    except NYC311Error:
        return None
    return Cursor(timestamp, str(key))


def latest_cursor(records: list[dict[str, Any]]) -> Cursor | None:
    cursors = [cursor_for_record(record) for record in records]
    valid = [cursor for cursor in cursors if cursor is not None]
    return max(valid, key=lambda cursor: (cursor.created_date, cursor.unique_key), default=None)


class NYC311Client:
    def __init__(
        self,
        base_url: str = "https://data.cityofnewyork.us/resource/erm2-nwe9.json",
        timeout: float = 20.0,
        session: requests.Session | None = None,
    ) -> None:
        self.base_url = base_url
        self.timeout = timeout
        self.session = session or build_session()

    def fetch_requests(
        self,
        cursor: Cursor | None,
        *,
        limit: int,
        initial_lookback_hours: int = 24,
        now: datetime | None = None,
    ) -> list[dict[str, Any]]:
        if limit < 1:
            raise ValueError("limit must be at least 1")
        if cursor:
            where = (
                f"created_date > '{cursor.created_date.isoformat()}' OR "
                f"(created_date = '{cursor.created_date.isoformat()}' "
                f"AND unique_key > '{cursor.unique_key}')"
            )
        else:
            reference = (now or datetime.now(UTC)).astimezone(UTC)
            start = reference - timedelta(hours=initial_lookback_hours)
            where = f"created_date >= '{start.isoformat()}'"

        response = self.session.get(
            self.base_url,
            params={
                "$limit": limit,
                "$order": "created_date ASC, unique_key ASC",
                "$where": where,
            },
            timeout=self.timeout,
        )
        response.raise_for_status()
        try:
            payload = response.json()
        except ValueError as exc:
            raise NYC311Error("NYC Open Data response was not valid JSON") from exc
        if not isinstance(payload, list):
            raise NYC311Error("NYC Open Data response was not a list")
        if not all(isinstance(record, dict) for record in payload):
            raise NYC311Error("NYC Open Data response contained a non-object record")
        return payload
