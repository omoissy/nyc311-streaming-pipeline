"""Publish user profiles to Kafka with delivery tracking and a dead-letter topic."""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass
from typing import Any

import requests
from confluent_kafka import KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from user_stream.config import Settings
from user_stream.randomuser import RandomUserClient, RandomUserError
from user_stream.transform import InvalidRecord, format_user

log = logging.getLogger(__name__)


@dataclass
class StreamStats:
    fetched: int = 0
    produced: int = 0
    dead_lettered: int = 0
    delivered: int = 0  # acknowledged by the broker, main topic and DLQ combined
    delivery_failures: int = 0
    api_failures: int = 0


def build_producer(bootstrap_servers: str, client_id: str = "user-profile-producer") -> Producer:
    return Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "client.id": client_id,
            # Idempotence + acks=all: broker-side retries never duplicate or reorder
            # messages within a partition.
            "enable.idempotence": True,
            "acks": "all",
            "compression.type": "lz4",
            "linger.ms": 50,
            "delivery.timeout.ms": 120_000,
        }
    )


def ensure_topics(
    bootstrap_servers: str,
    topics: dict[str, int],
    replication_factor: int = 1,
    timeout: float = 30.0,
) -> list[str]:
    """Create any topics that don't exist yet. Returns the names that were created."""
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    existing = set(admin.list_topics(timeout=timeout).topics)
    missing = [
        NewTopic(name, num_partitions=partitions, replication_factor=replication_factor)
        for name, partitions in topics.items()
        if name not in existing
    ]
    if not missing:
        log.info("all topics already exist: %s", sorted(topics))
        return []

    created = []
    for name, future in admin.create_topics(missing, request_timeout=timeout).items():
        try:
            future.result()
            created.append(name)
        except KafkaException as exc:
            if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise
    log.info("created topics: %s", created)
    return created


def _produce(
    producer: Producer,
    topic: str,
    key: str | None,
    value: dict[str, Any],
    on_delivery: Callable,
) -> None:
    payload = json.dumps(value, default=str).encode("utf-8")
    while True:
        try:
            producer.produce(topic, key=key, value=payload, on_delivery=on_delivery)
            return
        except BufferError:
            # Local queue is full: serve delivery callbacks so it drains, then retry.
            producer.poll(1)


def stream_users(
    client: RandomUserClient,
    producer: Producer,
    settings: Settings,
    *,
    duration_seconds: int | None = None,
    batch_size: int | None = None,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> StreamStats:
    """Fetch users for `duration_seconds` and publish them.

    Valid records go to `settings.topic`, keyed by user id so a user always lands on the
    same partition. Records that fail validation go to `settings.dlq_topic` with the
    reason attached. Raises if the API keeps failing or any message is not delivered.
    """
    duration = settings.stream_duration_seconds if duration_seconds is None else duration_seconds
    size = settings.batch_size if batch_size is None else batch_size
    if size < 1:
        raise ValueError("batch_size must be at least 1")
    stats = StreamStats()

    def on_delivery(err: KafkaError | None, msg: Any) -> None:
        if err is not None:
            stats.delivery_failures += 1
            log.error("delivery failed (topic=%s key=%s): %s", msg.topic(), msg.key(), err)
        else:
            stats.delivered += 1

    deadline = clock() + duration
    consecutive_failures = 0
    while clock() < deadline:
        try:
            raw_users = client.fetch_users(size)
        except (requests.RequestException, RandomUserError) as exc:
            stats.api_failures += 1
            consecutive_failures += 1
            log.warning("API fetch failed (%d in a row): %s", consecutive_failures, exc)
            if consecutive_failures >= settings.max_consecutive_api_failures:
                raise RuntimeError(
                    f"randomuser.me failed {consecutive_failures} times in a row"
                ) from exc
            sleep(min(2**consecutive_failures, 30))
            continue

        consecutive_failures = 0
        stats.fetched += len(raw_users)
        for raw in raw_users:
            try:
                record = format_user(raw)
            except InvalidRecord as exc:
                dead_letter = {"error": str(exc), "payload": raw}
                _produce(producer, settings.dlq_topic, None, dead_letter, on_delivery)
                stats.dead_lettered += 1
                continue
            _produce(producer, settings.topic, record["id"], record, on_delivery)
            stats.produced += 1

        producer.poll(0)
        sleep(settings.fetch_interval_seconds)

    undelivered = producer.flush(settings.flush_timeout_seconds)
    log.info("stream finished: %s", stats)
    if undelivered:
        raise RuntimeError(f"{undelivered} messages still undelivered after flush")
    if stats.delivery_failures:
        raise RuntimeError(f"{stats.delivery_failures} messages failed delivery")
    return stats


def run_stream(
    settings: Settings,
    duration_seconds: int | None = None,
    batch_size: int | None = None,
) -> dict[str, int]:
    """Build the API client and Kafka producer from settings, stream, and return stats."""
    client = RandomUserClient(settings.api_url, timeout=settings.api_timeout_seconds)
    producer = build_producer(settings.bootstrap_servers)
    stats = stream_users(
        client, producer, settings, duration_seconds=duration_seconds, batch_size=batch_size
    )
    return asdict(stats)
