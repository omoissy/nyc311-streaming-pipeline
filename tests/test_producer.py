from __future__ import annotations

import json

import pytest

from nyc311_stream.config import Settings
from nyc311_stream.nyc311 import latest_cursor
from nyc311_stream.producer import run_ingestion, stream_requests


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
    def __init__(self, records):
        self.records = records

    def fetch_requests(self, _cursor, *, limit, initial_lookback_hours):
        assert limit > 0
        assert initial_lookback_hours > 0
        return self.records


class FakeStore:
    def __init__(self):
        self.cursor = None
        self.runs = []

    def ensure_schema(self):
        pass

    def get_cursor(self, _source_name):
        return self.cursor

    def save_cursor(self, _source_name, cursor):
        self.cursor = cursor

    def record_run(self, values):
        self.runs.append(values)


def test_stream_requests_publishes_valid_and_dlq_records(raw_requests):
    settings = Settings()
    producer = FakeProducer()
    bad = {"unique_key": "70000003", "created_date": "bad"}

    stats, cursor = stream_requests(
        FakeClient([raw_requests[0], bad]), producer, settings, cursor=None
    )

    assert stats.fetched == 2
    assert stats.produced == 1
    assert stats.dead_lettered == 1
    assert stats.delivered == 2
    assert cursor.unique_key == "70000001"
    assert producer.messages[0][0] == settings.topic
    assert producer.messages[0][1] == "70000001"
    assert producer.messages[1][0] == settings.dlq_topic
    assert json.loads(producer.messages[1][2])["payload"] == bad


def test_run_ingestion_persists_cursor_and_audit(raw_requests):
    settings = Settings()
    store = FakeStore()

    result = run_ingestion(
        settings,
        client=FakeClient(raw_requests),
        producer=FakeProducer(),
        store=store,
    )

    assert result["produced"] == 2
    assert store.cursor == latest_cursor(raw_requests)
    assert store.runs[0]["status"] == "success"
    assert store.runs[0]["cursor_after"] == latest_cursor(raw_requests)


@pytest.mark.parametrize("field", ["topic", "dlq_topic", "pipeline_db_url"])
def test_settings_are_required(field):
    overrides = {field: ""}
    if field == "topic":
        overrides["topic"] = ""
    with pytest.raises(ValueError):
        Settings(**overrides)
