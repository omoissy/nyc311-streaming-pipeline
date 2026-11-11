from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest
import requests

from nyc311_stream.config import Settings
from nyc311_stream.nyc311 import Cursor
from nyc311_stream.producer import refresh_requests, run_ingestion
from nyc311_stream.state import RefreshState

LOAD_1 = datetime(2026, 11, 10, 2, 32, tzinfo=UTC)
LOAD_2 = datetime(2026, 11, 11, 2, 39, tzinfo=UTC)
NOW = datetime(2026, 11, 11, 12, 0, tzinfo=UTC)
WINDOW_7_DAYS = "2026-11-04T07:00:00.000"


class FakeMessage:
    def __init__(self, topic, key):
        self._topic = topic
        self._key = key

    def topic(self):
        return self._topic

    def key(self):
        return self._key


class FakeProducer:
    def __init__(self):
        self.messages = []

    def produce(self, topic, key, value, on_delivery):
        self.messages.append((topic, key, value))
        on_delivery(None, FakeMessage(topic, key))

    def poll(self, _timeout):
        return 0

    def flush(self, _timeout):
        return 0


class FakeClient:
    """Serves rows like Socrata: filtered by window and cursor, in source order."""

    def __init__(self, records, version=LOAD_1):
        self.records = sorted(records, key=lambda r: (r["created_date"], r["unique_key"]))
        self.version = version
        self.page_calls = []

    def source_version(self):
        return self.version

    def fetch_page(self, start, cursor, *, limit):
        self.page_calls.append((start, cursor, limit))
        rows = [r for r in self.records if r["created_date"] >= start]
        if cursor:
            rows = [
                r
                for r in rows
                if (r["created_date"], r["unique_key"]) > (cursor.created_date, cursor.unique_key)
            ]
        return rows[:limit]


class FakeStore:
    def __init__(self, state=None):
        self.state = state
        self.runs = []

    def ensure_schema(self):
        pass

    def get_state(self, _source_name):
        return self.state

    def save_state(self, _source_name, state):
        self.state = state

    def record_run(self, values):
        self.runs.append(dict(values))


def make_requests(count):
    return [
        {
            "unique_key": f"7{index:07d}",
            "created_date": f"2026-11-10T10:{index // 60:02d}:{index % 60:02d}.000",
            "agency": "DOT",
            "complaint_type": "Street Condition",
            "status": "Open",
            ":updated_at": "2026-11-10T02:32:48.798Z",
        }
        for index in range(count)
    ]


def ingest(client, store, producer=None, **settings):
    return run_ingestion(
        Settings(**settings),
        client=client,
        producer=producer or FakeProducer(),
        store=store,
        now=lambda: NOW,
    )


def test_first_run_refreshes_the_window_and_records_success(raw_requests):
    producer = FakeProducer()
    bad = {"unique_key": "70000003", "created_date": "2026-11-10T11:00:00.000"}
    client = FakeClient([*raw_requests, bad])
    store = FakeStore()

    result = ingest(client, store, producer)

    assert result["status"] == "success"
    assert (result["fetched"], result["produced"], result["dead_lettered"]) == (3, 2, 1)
    assert result["refresh_complete"] is True
    assert [(topic, key) for topic, key, _ in producer.messages] == [
        ("nyc311_requests", "70000001"),
        ("nyc311_requests", "70000002"),
        ("nyc311_requests_dlq", "70000003"),
    ]
    assert json.loads(producer.messages[2][2])["payload"] == bad
    assert client.page_calls[0] == (WINDOW_7_DAYS, None, 5000)
    assert store.state == RefreshState(
        LOAD_1, WINDOW_7_DAYS, Cursor("2026-11-10T11:00:00.000", "70000003"), True
    )
    [run] = store.runs
    assert run["status"] == "success"
    assert run["cursor_before"] is None
    json.dumps(result)  # XCom-safe


def test_no_new_nyc_load_is_a_no_op(raw_requests):
    client = FakeClient(raw_requests, version=LOAD_1)
    store = FakeStore(RefreshState(LOAD_1, WINDOW_7_DAYS, None, True))

    result = ingest(client, store)

    assert result["status"] == "no_change"
    assert result["fetched"] == 0
    assert client.page_calls == []
    assert store.runs[0]["status"] == "no_change"


def test_capped_refresh_resumes_from_its_cursor():
    client = FakeClient(make_requests(25))
    store = FakeStore()

    first = ingest(client, store, batch_size=10, max_records_per_run=15)
    assert (first["fetched"], first["pages"], first["refresh_complete"]) == (15, 2, False)
    assert [limit for *_, limit in client.page_calls] == [10, 5]
    assert store.state.complete is False

    second = ingest(client, store, batch_size=10, max_records_per_run=15)

    assert second["fetched"] == 10
    assert second["refresh_complete"] is True
    assert client.page_calls[2][1] == store.runs[0]["cursor_after"]
    assert store.state.complete is True


def test_new_nyc_load_restarts_the_window_and_picks_up_changes(raw_requests):
    store = FakeStore(RefreshState(LOAD_1, "2026-11-03T07:00:00.000", None, True))
    raw_requests[1]["status"] = "Closed"
    producer = FakeProducer()

    result = ingest(FakeClient(raw_requests, version=LOAD_2), store, producer)

    assert result["status"] == "success"
    assert store.state.source_version == LOAD_2
    assert store.state.window_start == WINDOW_7_DAYS
    statuses = [json.loads(value)["status"] for _, _, value in producer.messages]
    assert statuses == ["Closed", "Closed"]


def test_failed_delivery_fails_the_run_keeps_state_and_is_audited(raw_requests):
    class RejectingProducer(FakeProducer):
        def produce(self, topic, key, value, on_delivery):
            on_delivery("broker unavailable", FakeMessage(topic, key))

    previous = RefreshState(LOAD_1, WINDOW_7_DAYS, None, True)
    store = FakeStore(previous)

    with pytest.raises(RuntimeError, match="2 messages failed delivery"):
        ingest(FakeClient(raw_requests, version=LOAD_2), store, RejectingProducer())

    assert store.state == previous  # retried from the old position next time
    [run] = store.runs
    assert run["status"] == "failed"
    assert "2 messages failed delivery" in run["error"]


def test_api_failure_is_audited():
    class DownClient(FakeClient):
        def source_version(self):
            raise requests.ConnectionError("NYC Open Data is down")

    store = FakeStore()

    with pytest.raises(requests.ConnectionError):
        ingest(DownClient([]), store)

    assert store.runs[0]["status"] == "failed"
    assert "NYC Open Data is down" in store.runs[0]["error"]


def test_undelivered_messages_after_flush_fail_the_refresh(raw_requests):
    class StuckProducer(FakeProducer):
        def flush(self, _timeout):
            return 3

    with pytest.raises(RuntimeError, match="3 messages still undelivered"):
        refresh_requests(
            FakeClient(raw_requests), StuckProducer(), Settings(), WINDOW_7_DAYS, None, limit=10
        )


def test_full_local_queue_is_drained_and_retried(raw_requests):
    class BusyProducer(FakeProducer):
        def __init__(self):
            super().__init__()
            self.polls = []
            self.rejections = 2

        def produce(self, topic, key, value, on_delivery):
            if self.rejections:
                self.rejections -= 1
                raise BufferError("queue full")
            super().produce(topic, key, value, on_delivery)

        def poll(self, timeout):
            self.polls.append(timeout)
            return 0

    producer = BusyProducer()

    stats, _, _ = refresh_requests(
        FakeClient(raw_requests), producer, Settings(), WINDOW_7_DAYS, None, limit=10
    )

    assert stats.delivered == 2
    assert producer.polls.count(1) == 2


@pytest.mark.parametrize(
    "overrides",
    [
        {"topic": ""},
        {"dlq_topic": ""},
        {"topic": "same", "dlq_topic": "same"},
        {"pipeline_db_url": " "},
        {"batch_size": 0},
        {"batch_size": 50_001},
        {"max_records_per_run": 0},
        {"refresh_window_days": 0},
    ],
)
def test_invalid_settings_are_rejected(overrides):
    with pytest.raises(ValueError):
        Settings(**overrides)


def test_settings_read_from_environment(monkeypatch):
    monkeypatch.setenv("REFRESH_WINDOW_DAYS", "3")
    monkeypatch.setenv("API_BATCH_SIZE", "1000")
    monkeypatch.setenv("NYC311_APP_TOKEN", "token-123")
    monkeypatch.setenv("MAX_RECORDS_PER_RUN", "")  # empty falls back to the default

    settings = Settings.from_env()

    assert settings.refresh_window_days == 3
    assert settings.batch_size == 1000
    assert settings.app_token == "token-123"
    assert settings.max_records_per_run == Settings.max_records_per_run
