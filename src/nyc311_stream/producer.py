"""Publish NYC 311 requests to Kafka, refreshing recent requests after each NYC data load."""

from __future__ import annotations

import json
import logging
import uuid
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

from confluent_kafka import KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from nyc311_stream.config import Settings
from nyc311_stream.nyc311 import Cursor, NYC311Client, build_session, cursor_after, window_start
from nyc311_stream.state import PostgresStateStore, RefreshState
from nyc311_stream.transform import InvalidRecord, format_request

log = logging.getLogger(__name__)


class StateStore(Protocol):
    def ensure_schema(self) -> None: ...

    def get_state(self, source_name: str) -> RefreshState | None: ...

    def save_state(self, source_name: str, state: RefreshState) -> None: ...

    def record_run(self, values: dict[str, Any]) -> None: ...


@dataclass
class StreamStats:
    pages: int = 0
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


def build_client(settings: Settings) -> NYC311Client:
    return NYC311Client(
        settings.api_url,
        settings.metadata_url,
        timeout=settings.api_timeout_seconds,
        session=build_session(app_token=settings.app_token),
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
            # Local queue is full: serve delivery callbacks so it drains, then retry.
            producer.poll(1)


def refresh_requests(
    client: NYC311Client,
    producer: Producer,
    settings: Settings,
    start: str,
    cursor: Cursor | None,
    *,
    limit: int,
) -> tuple[StreamStats, Cursor | None, bool]:
    """Page through requests created since `start`, after `cursor`, publishing each one.

    Returns the stats, the cursor of the last row read, and whether the window was
    exhausted (False means `limit` was reached and the next run should resume).
    Raises if any message is not acknowledged by Kafka.
    """
    stats = StreamStats()

    def on_delivery(err: KafkaError | None, msg: Any) -> None:
        if err:
            stats.delivery_failures += 1
            log.error("delivery failed topic=%s key=%s error=%s", msg.topic(), msg.key(), err)
        else:
            stats.delivered += 1

    remaining = limit
    complete = False
    while remaining > 0:
        page_size = min(settings.batch_size, remaining)
        page = client.fetch_page(start, cursor, limit=page_size)
        stats.pages += 1
        stats.fetched += len(page)
        for raw in page:
            try:
                record = format_request(raw)
            except InvalidRecord as exc:
                key = str(raw["unique_key"]) if raw.get("unique_key") else None
                _produce(
                    producer,
                    settings.dlq_topic,
                    key,
                    {"error": str(exc), "payload": raw},
                    on_delivery,
                )
                stats.dead_lettered += 1
                continue
            _produce(producer, settings.topic, record["unique_key"], record, on_delivery)
            stats.produced += 1
        producer.poll(0)
        cursor = cursor_after(page) or cursor
        remaining -= len(page)
        if len(page) < page_size:
            complete = True
            break

    undelivered = producer.flush(settings.flush_timeout_seconds)
    if undelivered:
        raise RuntimeError(f"{undelivered} messages still undelivered after flush")
    if stats.delivery_failures:
        raise RuntimeError(f"{stats.delivery_failures} messages failed delivery")
    return stats, cursor, complete


def run_ingestion(
    settings: Settings,
    *,
    max_records: int | None = None,
    refresh_window_days: int | None = None,
    client: NYC311Client | None = None,
    producer: Producer | None = None,
    store: StateStore | None = None,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> dict[str, Any]:
    """Run one ingestion step and record it in the audit table, whether it succeeds or not.

    * NYC has not loaded new data since the last complete refresh: no-op.
    * The last refresh stopped at the record cap: resume it from its cursor.
    * NYC loaded new data: start a new refresh of the trailing window.

    The refresh state is saved only after Kafka has acknowledged every message, so a
    failed run is retried from the previous cursor. Re-sent requests are harmless
    because Cassandra upserts on `unique_key`.
    """
    state_store = store or PostgresStateStore(settings.pipeline_db_url)
    state_store.ensure_schema()
    audit: dict[str, Any] = {
        "run_id": str(uuid.uuid4()),
        "source_name": settings.source_name,
        "started_at": now(),
    }
    try:
        client = client or build_client(settings)
        version = client.source_version()
        previous = state_store.get_state(settings.source_name)
        audit["source_version"] = version

        if previous and previous.source_version == version and previous.complete:
            log.info("no new NYC data since %s; nothing to do", version.isoformat())
            audit.update(status="no_change", window_start=previous.window_start)
            state_store.record_run(audit)
            return _result(audit, StreamStats(), complete=True)

        if previous and previous.source_version == version:
            start, cursor = previous.window_start, previous.cursor
            log.info("resuming refresh for %s from %s", version.isoformat(), cursor)
        else:
            days = refresh_window_days or settings.refresh_window_days
            start, cursor = window_start(now(), days), None
            log.info("new NYC data at %s; refreshing requests since %s", version.isoformat(), start)
        audit.update(window_start=start, cursor_before=cursor)

        stats, after, complete = refresh_requests(
            client,
            producer or build_producer(settings.bootstrap_servers),
            settings,
            start,
            cursor,
            limit=max_records or settings.max_records_per_run,
        )
        state_store.save_state(settings.source_name, RefreshState(version, start, after, complete))
        audit.update(
            {key: getattr(stats, key) for key in AUDITED_COUNTS},
            status="success",
            cursor_after=after,
            refresh_complete=complete,
        )
        state_store.record_run(audit)
        log.info("NYC 311 ingestion finished: %s", audit)
        return _result(audit, stats, complete=complete)
    except Exception as exc:
        audit.update(status="failed", error=f"{type(exc).__name__}: {exc}"[:2000])
        try:
            state_store.record_run(audit)
        except Exception:
            log.exception("could not record the failed run in the audit table")
        raise


AUDITED_COUNTS = ("pages", "fetched", "produced", "dead_lettered")


def _result(audit: dict[str, Any], stats: StreamStats, *, complete: bool) -> dict[str, Any]:
    """JSON-safe run summary, returned to Airflow as the task's XCom."""
    summary: dict[str, Any] = {**asdict(stats), "refresh_complete": complete}
    for key, value in audit.items():
        if isinstance(value, datetime):
            value = value.isoformat()
        elif isinstance(value, Cursor):
            value = asdict(value)
        summary[key] = value
    return summary
