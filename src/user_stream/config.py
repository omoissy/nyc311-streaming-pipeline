"""Producer settings, read from environment variables with sensible defaults."""

from __future__ import annotations

import os
from dataclasses import dataclass


def _env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value not in (None, "") else default


@dataclass(frozen=True)
class Settings:
    bootstrap_servers: str = "kafka:29092"
    topic: str = "users_created"
    dlq_topic: str = "users_created_dlq"
    topic_partitions: int = 3
    api_url: str = "https://randomuser.me/api/"
    api_timeout_seconds: float = 10.0
    batch_size: int = 10
    fetch_interval_seconds: float = 1.0
    stream_duration_seconds: int = 60
    max_consecutive_api_failures: int = 5
    flush_timeout_seconds: float = 60.0

    def __post_init__(self) -> None:
        """Fail early on settings that would otherwise create a broken run."""
        if not self.bootstrap_servers.strip():
            raise ValueError("bootstrap_servers must not be empty")
        if not self.topic.strip() or not self.dlq_topic.strip():
            raise ValueError("Kafka topic names must not be empty")
        if self.topic == self.dlq_topic:
            raise ValueError("Kafka topic and dead-letter topic must be different")
        if self.topic_partitions < 1:
            raise ValueError("topic_partitions must be at least 1")
        if self.api_timeout_seconds <= 0:
            raise ValueError("api_timeout_seconds must be positive")
        if self.batch_size < 1:
            raise ValueError("batch_size must be at least 1")
        if self.fetch_interval_seconds < 0:
            raise ValueError("fetch_interval_seconds must not be negative")
        if self.stream_duration_seconds < 1:
            raise ValueError("stream_duration_seconds must be at least 1")
        if self.max_consecutive_api_failures < 1:
            raise ValueError("max_consecutive_api_failures must be at least 1")
        if self.flush_timeout_seconds <= 0:
            raise ValueError("flush_timeout_seconds must be positive")

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            bootstrap_servers=_env("KAFKA_BOOTSTRAP_SERVERS", cls.bootstrap_servers),
            topic=_env("KAFKA_TOPIC", cls.topic),
            dlq_topic=_env("KAFKA_DLQ_TOPIC", cls.dlq_topic),
            topic_partitions=int(_env("KAFKA_TOPIC_PARTITIONS", str(cls.topic_partitions))),
            api_url=_env("RANDOMUSER_API_URL", cls.api_url),
            api_timeout_seconds=float(_env("API_TIMEOUT_SECONDS", str(cls.api_timeout_seconds))),
            batch_size=int(_env("API_BATCH_SIZE", str(cls.batch_size))),
            fetch_interval_seconds=float(
                _env("FETCH_INTERVAL_SECONDS", str(cls.fetch_interval_seconds))
            ),
            stream_duration_seconds=int(
                _env("STREAM_DURATION_SECONDS", str(cls.stream_duration_seconds))
            ),
            max_consecutive_api_failures=int(
                _env("MAX_CONSECUTIVE_API_FAILURES", str(cls.max_consecutive_api_failures))
            ),
            flush_timeout_seconds=float(
                _env("FLUSH_TIMEOUT_SECONDS", str(cls.flush_timeout_seconds))
            ),
        )
