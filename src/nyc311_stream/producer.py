"""Publish normalized NYC 311 events to Kafka with a durable source cursor."""

from __future__ import annotations

import json
import logging
import time
import uuid
from collections.abc import Callable
from dataclasses import asdict, dataclass
from typing import Any, Protocol

from confluent_kafka import KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from nyc311_stream.config import Settings
from nyc311_stream.nyc311 import Cursor, NYC311Client, latest_cursor
from nyc311_stream.state import PostgresStateStore
from nyc311_stream.transform import InvalidRecord, format_request

log = logging.getLogger(__name__)


class CursorStore(Protocol):
    def ensure_schema(self) -> None: ...

    def get_cursor(self, source_name: str) -> Cursor | None: ...

    def save_cursor(self, source_name: str, cursor_value: Cursor) -> None: ...

    def record_run(self, values: dict[str, Any]) -> None: ...


@dataclass
class StreamStats:
    run_id: str
    fetched: int = 0
    produced: int = 0
    dead_lettered: int = 0
    delivered: int = 0
    delivery_failures: int = 0


def build_producer(bootstrap_servers: str) -> Producer:
    return Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "nyc311-producer",
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
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    existing = set(admin.list_topics(timeout=timeout).topics)
    missing = [
        NewTopic(name, num_partitions=partitions, replication_factor=replication_factor)
        for name, partitions in topics.items()
        if name not in existing
    ]
    if not missing:
        return []
    created = []
    for name, future in admin.create_topics(missing, request_timeout=timeout).items():
        try:
            future.result()
            created.append(name)
        except KafkaException as exc:
            if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise
    return created


def _produce(
    producer: Producer,
    topic: str,
    key: str | None,
    value: dict[str, Any],
    on_delivery: Callable,
) -> None:
    payload = json.dumps(value, separators=(",", ":"), default=str).encode("utf-8")
    while True:
        try:
            producer.produce(topic, key=key, value=payload, on_delivery=on_delivery)
            return
        except BufferError:
            producer.poll(1)


def stream_requests(
    client: NYC311Client,
    producer: Producer,
    settings: Settings,
    cursor: Cursor | None,
    *,
    limit: int | None = None,
    initial_lookback_hours: int | None = None,
) -> tuple[StreamStats, Cursor | None]:
    run_id = str(uuid.uuid4())
    stats = StreamStats(run_id=run_id)
    records = client.fetch_requests(
        cursor,
        limit=limit or settings.max_records_per_run,
        initial_lookback_hours=initial_lookback_hours or settings.initial_lookback_hours,
    )

    def on_delivery(err: KafkaError | None, msg: Any) -> None:
        if err:
            stats.delivery_failures += 1
            log.error("delivery failed topic=%s key=%s error=%s", msg.topic(), msg.key(), err)
        else:
            stats.delivered += 1

    stats.fetched = len(records)
    for raw in records:
        try:
            record = format_request(raw)
        except InvalidRecord as exc:
            _produce(
                producer,
                settings.dlq_topic,
                str(raw.get("unique_key")) if raw.get("unique_key") else None,
                {"error": str(exc), "payload": raw},
                on_delivery,
            )
            stats.dead_lettered += 1
            continue
        _produce(producer, settings.topic, record["unique_key"], record, on_delivery)
        stats.produced += 1

    undelivered = producer.flush(settings.flush_timeout_seconds)
    if undelivered:
        raise RuntimeError(f"{undelivered} messages still undelivered after flush")
    if stats.delivery_failures:
        raise RuntimeError(f"{stats.delivery_failures} messages failed delivery")
    return stats, latest_cursor(records)


def run_ingestion(
    settings: Settings,
    *,
    max_records: int | None = None,
    initial_lookback_hours: int | None = None,
    client: NYC311Client | None = None,
    producer: Producer | None = None,
    store: CursorStore | None = None,
    now: Callable[[], float] = time.monotonic,
) -> dict[str, Any]:
    del now  # retained as a seam for callers that instrument run timing
    state = store or PostgresStateStore(settings.pipeline_db_url)
    state.ensure_schema()
    before = state.get_cursor(settings.source_name)
    started_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    client = client or NYC311Client(settings.api_url, timeout=settings.api_timeout_seconds)
    producer = producer or build_producer(settings.bootstrap_servers)
    stats, after = stream_requests(
        client,
        producer,
        settings,
        before,
        limit=max_records,
        initial_lookback_hours=initial_lookback_hours,
    )
    if after and (before is None or after > before):
        state.save_cursor(settings.source_name, after)
    state.record_run(
        {
            "run_id": stats.run_id,
            "source_name": settings.source_name,
            "started_at": started_at,
            "fetched": stats.fetched,
            "produced": stats.produced,
            "dead_lettered": stats.dead_lettered,
            "cursor_before": before,
            "cursor_after": after,
            "status": "success",
        }
    )
    log.info("NYC 311 ingestion completed: %s", stats)
    return {**asdict(stats), "cursor_after": asdict(after) if after else None}
