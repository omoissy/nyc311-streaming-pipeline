from __future__ import annotations

import json

import pytest
import requests

from user_stream.config import Settings
from user_stream.producer import stream_users


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
    def __init__(self, users):
        self.users = users
        self.calls = []

    def fetch_users(self, count):
        self.calls.append(count)
        return self.users


def test_stream_users_sends_valid_records_and_dlq(raw_user):
    invalid = {"email": "bad", "login": {"uuid": "not-a-uuid"}}
    settings = Settings(fetch_interval_seconds=0, stream_duration_seconds=1)
    client = FakeClient([raw_user, invalid])
    producer = FakeProducer()
    now = iter([0.0, 0.0, 2.0])

    stats = stream_users(client, producer, settings, clock=lambda: next(now), sleep=lambda _: None)

    assert stats.fetched == 2
    assert stats.produced == 1
    assert stats.dead_lettered == 1
    assert stats.delivered == 2
    assert stats.delivery_failures == 0
    assert producer.messages[0][0] == settings.topic
    assert producer.messages[1][0] == settings.dlq_topic


def test_stream_users_fails_after_consecutive_api_errors():
    class FailingClient:
        def fetch_users(self, _count):
            raise requests.ConnectionError("network down")

    settings = Settings(max_consecutive_api_failures=1)
    producer = FakeProducer()

    with pytest.raises(RuntimeError, match="failed 1 times"):
        stream_users(FailingClient(), producer, settings, duration_seconds=1, clock=lambda: 0)


class FakeClock:
    """Monotonic clock that only advances when the code under test sleeps."""

    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


def test_messages_are_keyed_by_user_id_and_json_encoded(raw_users):
    settings = Settings(fetch_interval_seconds=1, stream_duration_seconds=1)
    producer = FakeProducer()
    clock = FakeClock()

    stream_users(FakeClient(raw_users), producer, settings, clock=clock, sleep=clock.sleep)

    keys = [key for _, key, _ in producer.messages]
    assert keys == [
        "44957db7-9c04-4cff-8020-85aa39e37b4b",
        "0f3c2a9e-6b1d-4e57-9c8a-2d4b6f8e1a30",
    ]
    first = json.loads(producer.messages[0][2])
    assert first["id"] == keys[0]
    assert first["schema_version"] == 1


def test_dead_letter_carries_reason_and_original_payload():
    settings = Settings(fetch_interval_seconds=1, stream_duration_seconds=1)
    producer = FakeProducer()
    clock = FakeClock()
    bad = {"login": {"uuid": "nope"}, "email": "x@example.com"}

    stream_users(FakeClient([bad]), producer, settings, clock=clock, sleep=clock.sleep)

    [(topic, key, value)] = producer.messages
    assert topic == settings.dlq_topic
    assert key is None
    dead_letter = json.loads(value)
    assert "not a valid UUID" in dead_letter["error"]
    assert dead_letter["payload"] == bad


def test_transient_api_failure_is_retried(raw_user):
    class FlakyClient:
        def __init__(self):
            self.calls = 0

        def fetch_users(self, _count):
            self.calls += 1
            if self.calls == 1:
                raise requests.Timeout("slow API")
            return [raw_user]

    settings = Settings(fetch_interval_seconds=1, stream_duration_seconds=3)
    clock = FakeClock()

    stats = stream_users(FlakyClient(), FakeProducer(), settings, clock=clock, sleep=clock.sleep)

    assert stats.api_failures == 1
    assert stats.produced >= 1
    assert clock.sleeps[0] == 2  # exponential backoff after the first failure


def test_failed_delivery_fails_the_run(raw_user):
    class RejectingProducer(FakeProducer):
        def produce(self, topic, key, value, on_delivery):
            self.messages.append((topic, key, value))
            on_delivery("broker unavailable", FakeMessage(topic, key))

    settings = Settings(fetch_interval_seconds=1, stream_duration_seconds=1)
    clock = FakeClock()

    with pytest.raises(RuntimeError, match="1 messages failed delivery"):
        stream_users(
            FakeClient([raw_user]), RejectingProducer(), settings, clock=clock, sleep=clock.sleep
        )


def test_undelivered_messages_after_flush_fail_the_run(raw_user):
    class StuckProducer(FakeProducer):
        def flush(self, _timeout):
            return 3

    settings = Settings(fetch_interval_seconds=1, stream_duration_seconds=1)
    clock = FakeClock()

    with pytest.raises(RuntimeError, match="3 messages still undelivered"):
        stream_users(
            FakeClient([raw_user]), StuckProducer(), settings, clock=clock, sleep=clock.sleep
        )


def test_full_local_queue_is_drained_and_retried(raw_user):
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

    settings = Settings(fetch_interval_seconds=1, stream_duration_seconds=1)
    producer = BusyProducer()
    clock = FakeClock()

    stats = stream_users(
        FakeClient([raw_user]), producer, settings, clock=clock, sleep=clock.sleep
    )

    assert stats.delivered == 1
    assert producer.polls.count(1) == 2


@pytest.mark.parametrize(
    "overrides",
    [
        {"topic": "same", "dlq_topic": "same"},
        {"batch_size": 0},
        {"bootstrap_servers": " "},
        {"stream_duration_seconds": 0},
    ],
)
def test_invalid_settings_are_rejected(overrides):
    with pytest.raises(ValueError):
        Settings(**overrides)


def test_settings_read_from_environment(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    monkeypatch.setenv("KAFKA_TOPIC", "users_test")
    monkeypatch.setenv("API_BATCH_SIZE", "25")
    monkeypatch.setenv("STREAM_DURATION_SECONDS", "")  # empty falls back to the default

    settings = Settings.from_env()

    assert settings.bootstrap_servers == "localhost:9092"
    assert settings.topic == "users_test"
    assert settings.batch_size == 25
    assert settings.stream_duration_seconds == Settings.stream_duration_seconds
