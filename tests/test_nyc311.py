from __future__ import annotations

import re
from datetime import UTC, datetime

import pytest
import requests

from nyc311_stream.nyc311 import (
    Cursor,
    NYC311Client,
    NYC311Error,
    build_session,
    cursor_after,
    page_where,
    parse_floating,
    window_start,
)

FLOATING_LITERAL = re.compile(r"'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3})'")


class FakeResponse:
    def __init__(self, payload, status_code=200):
        self.payload = payload
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(response=self)

    def json(self):
        if isinstance(self.payload, Exception):
            raise self.payload
        return self.payload


class FakeSession:
    def __init__(self, *responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, params=None, timeout=None):
        self.calls.append({"url": url, "params": params, "timeout": timeout})
        return self.responses.pop(0)


def test_page_query_uses_floating_literals_and_source_order(raw_requests):
    session = FakeSession(FakeResponse(raw_requests))
    client = NYC311Client("https://data.test/resource.json", timeout=4, session=session)

    records = client.fetch_page("2026-11-04T07:00:00.000", None, limit=25)

    assert records == raw_requests
    [call] = session.calls
    assert call["url"] == "https://data.test/resource.json"
    assert call["timeout"] == 4
    assert call["params"] == {
        "$select": "*, :updated_at",
        "$where": "created_date >= '2026-11-04T07:00:00.000'",
        "$order": "created_date ASC, unique_key ASC",
        "$limit": 25,
    }


def test_page_query_continues_after_the_cursor():
    where = page_where(
        "2026-11-04T07:00:00.000", Cursor("2026-11-10T10:05:00.000", "70000002")
    )

    assert where == (
        "created_date >= '2026-11-04T07:00:00.000' AND "
        "(created_date > '2026-11-10T10:05:00.000' OR "
        "(created_date = '2026-11-10T10:05:00.000' AND unique_key > '70000002'))"
    )
    # Socrata rejects literals with a zone offset (query.soql.type-mismatch).
    assert all(FLOATING_LITERAL.fullmatch(f"'{v}'") for v in FLOATING_LITERAL.findall(where))
    assert "+" not in where and "Z'" not in where


@pytest.mark.parametrize(
    ("now", "days", "expected"),
    [
        # November: Eastern Standard Time, UTC-5.
        (datetime(2026, 11, 11, 12, 0, tzinfo=UTC), 7, "2026-11-04T07:00:00.000"),
        # July: Eastern Daylight Time, UTC-4.
        (datetime(2026, 7, 10, 12, 0, tzinfo=UTC), 1, "2026-07-09T08:00:00.000"),
    ],
)
def test_window_start_is_nyc_local_time(now, days, expected):
    assert window_start(now, days) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("2026-11-10T10:00:00.000", datetime(2026, 11, 10, 15, 0, tzinfo=UTC)),
        ("2026-07-04T12:00:00.000", datetime(2026, 7, 4, 16, 0, tzinfo=UTC)),
    ],
)
def test_source_timestamps_are_read_as_nyc_local_time(value, expected):
    assert parse_floating(value) == expected


@pytest.mark.parametrize(
    "cursor",
    [
        Cursor("2026-11-10T10:05:00+00:00", "1"),
        Cursor("2026-11-10T10:05:00", "1"),
        Cursor("2026-11-10' OR '1'='1", "1"),
    ],
)
def test_unsafe_or_malformed_cursor_literals_are_refused(cursor):
    with pytest.raises(NYC311Error):
        page_where("2026-11-04T07:00:00.000", cursor)


def test_unique_key_quotes_are_escaped():
    where = page_where("2026-11-04T07:00:00.000", Cursor("2026-11-10T10:05:00.000", "a'b"))
    assert "unique_key > 'a''b'" in where


@pytest.mark.parametrize(
    "payload", [{"error": "bad"}, "not-a-list", ["bad"], ValueError("not json")]
)
def test_bad_page_payloads_raise(payload):
    client = NYC311Client(session=FakeSession(FakeResponse(payload)))

    with pytest.raises(NYC311Error):
        client.fetch_page("2026-11-04T07:00:00.000", None, limit=1)


def test_http_errors_propagate():
    client = NYC311Client(session=FakeSession(FakeResponse([], status_code=400)))
    with pytest.raises(requests.HTTPError):
        client.fetch_page("2026-11-04T07:00:00.000", None, limit=1)


def test_page_limit_must_be_positive():
    session = FakeSession()
    with pytest.raises(ValueError):
        NYC311Client(session=session).fetch_page("2026-11-04T07:00:00.000", None, limit=0)
    assert session.calls == []


def test_source_version_reads_rows_updated_at():
    session = FakeSession(FakeResponse({"rowsUpdatedAt": 1794364787, "name": "311"}))
    client = NYC311Client(metadata_url="https://data.test/views/x.json", session=session)

    assert client.source_version() == datetime(2026, 11, 11, 2, 39, 47, tzinfo=UTC)
    assert session.calls[0]["url"] == "https://data.test/views/x.json"


@pytest.mark.parametrize("payload", [{}, {"rowsUpdatedAt": "soon"}, ["x"]])
def test_source_version_requires_usable_metadata(payload):
    with pytest.raises(NYC311Error):
        NYC311Client(session=FakeSession(FakeResponse(payload))).source_version()


def test_cursor_after_is_the_last_row_as_returned(raw_requests):
    assert cursor_after(raw_requests) == Cursor("2026-11-10T10:05:00.000", "70000002")
    assert cursor_after([]) is None
    with pytest.raises(NYC311Error):
        cursor_after([{"unique_key": "1"}])


def test_app_token_is_sent_only_when_configured():
    assert build_session(app_token="abc123").headers["X-App-Token"] == "abc123"
    assert "X-App-Token" not in build_session().headers
