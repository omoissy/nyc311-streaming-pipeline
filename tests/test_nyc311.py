from __future__ import annotations

import pytest
import requests

from nyc311_stream.nyc311 import Cursor, NYC311Client, NYC311Error, latest_cursor


class FakeResponse:
    def __init__(self, payload, status_code=200):
        self.payload = payload
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(response=self)

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def get(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return self.response


def test_fetch_requests_builds_incremental_soc_query(raw_requests):
    session = FakeSession(FakeResponse(raw_requests))
    client = NYC311Client("https://data.test/resource.json", timeout=4, session=session)

    records = client.fetch_requests(None, limit=25, initial_lookback_hours=12)

    assert records == raw_requests
    args, kwargs = session.calls[0]
    assert args == ("https://data.test/resource.json",)
    assert kwargs["params"]["$limit"] == 25
    assert "created_date >= '" in kwargs["params"]["$where"]
    assert kwargs["timeout"] == 4


def test_fetch_requests_uses_cursor_in_where_clause(raw_requests):
    session = FakeSession(FakeResponse(raw_requests))
    client = NYC311Client(session=session)
    cursor = Cursor(
        created_date=latest_cursor(raw_requests).created_date,
        unique_key="70000000",
    )

    client.fetch_requests(cursor, limit=5)

    where = session.calls[0][1]["params"]["$where"]
    assert "created_date > '2026-11-10T10:05:00+00:00'" in where
    assert "unique_key > '70000000'" in where


@pytest.mark.parametrize("payload", [{"error": "bad"}, "not-a-list", ["bad"]])
def test_bad_api_payloads_raise(payload):
    client = NYC311Client(session=FakeSession(FakeResponse(payload)))

    with pytest.raises(NYC311Error):
        client.fetch_requests(None, limit=1)


def test_latest_cursor_is_lexicographic_by_timestamp_then_key(raw_requests):
    cursor = latest_cursor(raw_requests)

    assert cursor == Cursor(
        created_date=cursor.created_date,
        unique_key="70000002",
    )
