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
    # Socrata dataset metadata; `rowsUpdatedAt` changes whenever NYC loads new data.
    metadata_url: str = "https://data.cityofnewyork.us/api/views/erm2-nwe9.json"
    # Optional Socrata app token. Without one, requests share a lower anonymous rate limit.
    app_token: str = ""
    api_timeout_seconds: float = 30.0
    # Rows per API page. Socrata allows up to 50,000; 5,000 keeps responses small.
    batch_size: int = 5000
    # Upper bound per run. A refresh that hits it resumes on the next run.
    max_records_per_run: int = 100_000
    # Each refresh re-reads requests created this many days back, which picks up
    # late-published requests and status/closure changes inside the window.
    refresh_window_days: int = 7
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
        if not 1 <= self.batch_size <= 50_000:
            raise ValueError("batch_size must be between 1 and 50000")
        if self.max_records_per_run < 1:
            raise ValueError("max_records_per_run must be at least 1")
        if self.refresh_window_days < 1:
            raise ValueError("refresh_window_days must be at least 1")
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
            metadata_url=_env("NYC311_METADATA_URL", cls.metadata_url),
            app_token=_env("NYC311_APP_TOKEN", cls.app_token),
            api_timeout_seconds=float(_env("API_TIMEOUT_SECONDS", str(cls.api_timeout_seconds))),
            batch_size=int(_env("API_BATCH_SIZE", str(cls.batch_size))),
            max_records_per_run=int(_env("MAX_RECORDS_PER_RUN", str(cls.max_records_per_run))),
            refresh_window_days=int(_env("REFRESH_WINDOW_DAYS", str(cls.refresh_window_days))),
            pipeline_db_url=_env("PIPELINE_DB_URL", cls.pipeline_db_url),
            source_name=_env("SOURCE_NAME", cls.source_name),
            flush_timeout_seconds=float(
                _env("FLUSH_TIMEOUT_SECONDS", str(cls.flush_timeout_seconds))
            ),
        )
