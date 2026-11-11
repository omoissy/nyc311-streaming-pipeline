"""Resilient client for the official NYC Open Data 311 Socrata API.

Two facts about the source shape this client:

* `created_date`, `closed_date` and `due_date` are Socrata *floating* timestamps:
  NYC wall-clock time with no zone. Query literals must use the same zone-less
  format (`2026-11-04T00:00:00.000`); an ISO string with an offset is rejected
  with `query.soql.type-mismatch`.
* NYC publishes changes in daily bulk loads, and the API cannot sort the full
  dataset by `:updated_at`. Instead, the dataset's `rowsUpdatedAt` metadata tells
  us when a load happened, and each load triggers a keyset-paged re-read of recent
  requests ordered by `(created_date, unique_key)`.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

NYC_TZ = ZoneInfo("America/New_York")
RETRY_STATUSES = (429, 500, 502, 503, 504)
FLOATING_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"


class NYC311Error(RuntimeError):
    """The API returned an invalid or unusable response."""


@dataclass(frozen=True)
class Cursor:
    """Position within a refresh: the last row read, in source order.

    `created_date` is kept exactly as the API returned it (zone-less NYC time), so it
    round-trips into the next query without any time-zone conversion.
    """

    created_date: str
    unique_key: str


def build_session(
    max_retries: int = 5, backoff_factor: float = 1.0, app_token: str = ""
) -> requests.Session:
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
    if app_token:
        session.headers["X-App-Token"] = app_token
    return session


def floating_literal(moment: datetime) -> str:
    """Format a datetime as a Socrata floating-timestamp literal in NYC local time."""
    local = moment.astimezone(NYC_TZ) if moment.tzinfo else moment
    return local.strftime(FLOATING_FORMAT)[:-3]


def parse_floating(value: str) -> datetime:
    """Parse a floating NYC timestamp (no zone) and return it as an aware UTC datetime."""
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise NYC311Error(f"invalid NYC 311 timestamp: {value!r}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=NYC_TZ)
    return parsed.astimezone(UTC)


def parse_fixed(value: str) -> datetime:
    """Parse a Socrata fixed timestamp such as `:updated_at` (`...Z`) as UTC."""
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise NYC311Error(f"invalid Socrata timestamp: {value!r}") from exc
    return (parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)).astimezone(UTC)


def window_start(now: datetime, days: int) -> str:
    """Floating literal for the start of the refresh window, `days` before `now`."""
    return floating_literal(now.astimezone(UTC) - timedelta(days=days))


def page_where(start: str, cursor: Cursor | None) -> str:
    """SoQL filter for one page: inside the window and after the cursor, if any."""
    _check_literal(start)
    where = f"created_date >= '{start}'"
    if cursor is None:
        return where
    _check_literal(cursor.created_date)
    key = cursor.unique_key.replace("'", "''")
    return (
        f"{where} AND (created_date > '{cursor.created_date}' OR "
        f"(created_date = '{cursor.created_date}' AND unique_key > '{key}'))"
    )


def _check_literal(value: str) -> None:
    # Anything else would either be rejected by Socrata or break out of the quotes.
    try:
        parsed = datetime.strptime(value, FLOATING_FORMAT)
    except ValueError:
        raise NYC311Error(f"not a floating timestamp literal: {value!r}") from None
    if floating_literal(parsed) != value:
        raise NYC311Error(f"floating timestamp must have millisecond precision: {value!r}")


class NYC311Client:
    def __init__(
        self,
        base_url: str = "https://data.cityofnewyork.us/resource/erm2-nwe9.json",
        metadata_url: str = "https://data.cityofnewyork.us/api/views/erm2-nwe9.json",
        timeout: float = 30.0,
        session: requests.Session | None = None,
    ) -> None:
        self.base_url = base_url
        self.metadata_url = metadata_url
        self.timeout = timeout
        self.session = session or build_session()

    def _get_json(self, url: str, params: dict[str, Any] | None = None) -> Any:
        response = self.session.get(url, params=params, timeout=self.timeout)
        response.raise_for_status()
        try:
            return response.json()
        except ValueError as exc:
            raise NYC311Error("NYC Open Data response was not valid JSON") from exc

    def source_version(self) -> datetime:
        """When NYC last loaded data into the dataset (`rowsUpdatedAt`), in UTC."""
        metadata = self._get_json(self.metadata_url)
        value = metadata.get("rowsUpdatedAt") if isinstance(metadata, dict) else None
        if not isinstance(value, int) or value <= 0:
            raise NYC311Error("dataset metadata has no usable rowsUpdatedAt")
        return datetime.fromtimestamp(value, UTC)

    def fetch_page(self, start: str, cursor: Cursor | None, *, limit: int) -> list[dict]:
        """One page of requests created since `start`, after `cursor`, in source order."""
        if limit < 1:
            raise ValueError("limit must be at least 1")
        payload = self._get_json(
            self.base_url,
            params={
                "$select": "*, :updated_at",
                "$where": page_where(start, cursor),
                "$order": "created_date ASC, unique_key ASC",
                "$limit": limit,
            },
        )
        if not isinstance(payload, list):
            raise NYC311Error("NYC Open Data response was not a list")
        if not all(isinstance(record, dict) for record in payload):
            raise NYC311Error("NYC Open Data response contained a non-object record")
        return payload


def cursor_after(page: list[dict]) -> Cursor | None:
    """Cursor for the last row of a page, which the API returned in source order."""
    if not page:
        return None
    last = page[-1]
    created, key = last.get("created_date"), last.get("unique_key")
    if not created or not key:
        raise NYC311Error("cannot advance the cursor: last row lacks created_date/unique_key")
    return Cursor(str(created), str(key))
