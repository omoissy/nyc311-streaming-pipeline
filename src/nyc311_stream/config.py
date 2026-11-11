"""Configuration for the NYC 311 ingestion service."""

from __future__ import annotations

import os
from dataclasses import dataclass


def _env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value not in (None, "") else default


@dataclass(frozen=True)
class Settings:
    bootstrap_servers: str = "kafka:29092"
    topic: str = "nyc311_requests"
    dlq_topic: str = "nyc311_requests_dlq"
    topic_partitions: int = 3
    api_url: str = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
    api_timeout_seconds: float = 20.0
    batch_size: int = 100
    max_records_per_run: int = 500
    initial_lookback_hours: int = 168
    pipeline_db_url: str = "postgresql://airflow:airflow@postgres/airflow"
    source_name: str = "nyc311_service_requests"
    flush_timeout_seconds: float = 60.0

    def __post_init__(self) -> None:
        if not self.bootstrap_servers.strip():
            raise ValueError("bootstrap_servers must not be empty")
        if not self.topic.strip() or not self.dlq_topic.strip():
            raise ValueError("Kafka topic names must not be empty")
        if self.topic == self.dlq_topic:
            raise ValueError("Kafka topic and dead-letter topic must differ")
        if self.topic_partitions < 1:
            raise ValueError("topic_partitions must be at least 1")
        if self.api_timeout_seconds <= 0:
            raise ValueError("api_timeout_seconds must be positive")
        if self.batch_size < 1 or self.max_records_per_run < 1:
            raise ValueError("batch sizes must be positive")
        if self.initial_lookback_hours < 1:
            raise ValueError("initial_lookback_hours must be at least 1")
        if not self.pipeline_db_url.strip():
            raise ValueError("pipeline_db_url must not be empty")
        if self.flush_timeout_seconds <= 0:
            raise ValueError("flush_timeout_seconds must be positive")

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            bootstrap_servers=_env("KAFKA_BOOTSTRAP_SERVERS", cls.bootstrap_servers),
            topic=_env("KAFKA_TOPIC", cls.topic),
            dlq_topic=_env("KAFKA_DLQ_TOPIC", cls.dlq_topic),
            topic_partitions=int(_env("KAFKA_TOPIC_PARTITIONS", str(cls.topic_partitions))),
            api_url=_env("NYC311_API_URL", cls.api_url),
            api_timeout_seconds=float(_env("API_TIMEOUT_SECONDS", str(cls.api_timeout_seconds))),
            batch_size=int(_env("API_BATCH_SIZE", str(cls.batch_size))),
            max_records_per_run=int(
                _env("MAX_RECORDS_PER_RUN", str(cls.max_records_per_run))
            ),
            initial_lookback_hours=int(
                _env("INITIAL_LOOKBACK_HOURS", str(cls.initial_lookback_hours))
            ),
            pipeline_db_url=_env("PIPELINE_DB_URL", cls.pipeline_db_url),
            source_name=_env("SOURCE_NAME", cls.source_name),
            flush_timeout_seconds=float(
                _env("FLUSH_TIMEOUT_SECONDS", str(cls.flush_timeout_seconds))
            ),
        )
