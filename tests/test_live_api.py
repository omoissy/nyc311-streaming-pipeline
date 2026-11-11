"""Checks against the real NYC Open Data API. Opt in with RUN_LIVE_TESTS=1.

The unit tests use a fake API; these confirm that Socrata accepts the queries the
client actually builds (an earlier version passed every unit test while every real
request failed with HTTP 400).
"""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta

import pytest

from nyc311_stream.nyc311 import NYC311Client, build_session, cursor_after, window_start
from nyc311_stream.transform import InvalidRecord, format_request

pytestmark = [
    pytest.mark.live,
    pytest.mark.skipif(os.getenv("RUN_LIVE_TESTS") != "1", reason="set RUN_LIVE_TESTS=1"),
]


@pytest.fixture(scope="module")
def client():
    session = build_session(max_retries=2, app_token=os.getenv("NYC311_APP_TOKEN", ""))
    return NYC311Client(session=session)


def test_source_version_is_recent(client):
    version = client.source_version()
    assert datetime.now(UTC) - timedelta(days=14) < version <= datetime.now(UTC)


def test_keyset_pages_are_accepted_ordered_and_disjoint(client):
    start = window_start(datetime.now(UTC), 7)

    first = client.fetch_page(start, None, limit=5)
    second = client.fetch_page(start, cursor_after(first), limit=5)

    assert len(first) == 5 and len(second) == 5
    assert ":updated_at" in first[0]
    order = [(row["created_date"], row["unique_key"]) for row in first + second]
    assert order == sorted(order)
    assert len(set(order)) == len(order)

    valid = []
    for row in first + second:
        try:
            valid.append(format_request(row))
        except InvalidRecord:
            pass
    assert valid, "expected at least one real record to pass validation"
