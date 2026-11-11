"""
### user_profiles_stream

Pulls live user profiles from the [randomuser.me](https://randomuser.me) API and
publishes them to Kafka.

1. **ensure_kafka_topics**: creates the main and dead-letter topics if missing.
2. **stream_users_to_kafka**: fetches batches for `duration_seconds`, validates each
   record, sends valid ones to `users_created` (keyed by user id) and invalid ones
   to `users_created_dlq`.
3. **quality_gate**: fails the run if nothing was produced or too many records were
   rejected.

The Spark Structured Streaming job (`spark/app/stream_to_cassandra.py`) consumes the
topic continuously and upserts the records into Cassandra.

Trigger with custom settings: `{"duration_seconds": 120, "batch_size": 25}`.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param

log = logging.getLogger(__name__)

DEFAULT_DURATION_SECONDS = int(os.getenv("STREAM_DURATION_SECONDS", "60"))
DEFAULT_BATCH_SIZE = int(os.getenv("API_BATCH_SIZE", "10"))
MAX_REJECT_RATIO = float(os.getenv("MAX_REJECT_RATIO", "0.2"))


@dag(
    dag_id="user_profiles_stream",
    description="Stream live randomuser.me profiles into Kafka",
    schedule="@hourly",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "omoissy",
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
        "retry_exponential_backoff": True,
    },
    params={
        "duration_seconds": Param(
            DEFAULT_DURATION_SECONDS,
            type="integer",
            minimum=5,
            maximum=3300,
            description="How long to keep pulling from the API",
        ),
        "batch_size": Param(
            DEFAULT_BATCH_SIZE,
            type="integer",
            minimum=1,
            maximum=500,
            description="Users requested per API call",
        ),
    },
    tags=["streaming", "kafka", "spark", "cassandra"],
    doc_md=__doc__,
)
def user_profiles_stream():
    # Project imports live inside the tasks so DAG parsing stays fast and never
    # depends on Kafka being reachable.

    @task
    def ensure_kafka_topics() -> list[str]:
        from user_stream.config import Settings
        from user_stream.producer import ensure_topics

        settings = Settings.from_env()
        return ensure_topics(
            settings.bootstrap_servers,
            {settings.topic: settings.topic_partitions, settings.dlq_topic: 1},
        )

    @task(execution_timeout=timedelta(minutes=58))
    def stream_users_to_kafka() -> dict[str, int]:
        from airflow.operators.python import get_current_context
        from user_stream.config import Settings
        from user_stream.producer import run_stream

        params = get_current_context()["params"]
        return run_stream(
            Settings.from_env(),
            duration_seconds=params["duration_seconds"],
            batch_size=params["batch_size"],
        )

    @task
    def quality_gate(stats: dict[str, int]) -> None:
        log.info("run stats: %s", stats)
        if stats["produced"] == 0:
            raise AirflowFailException("no valid records were produced")
        reject_ratio = stats["dead_lettered"] / max(stats["fetched"], 1)
        if reject_ratio > MAX_REJECT_RATIO:
            raise AirflowFailException(
                f"{reject_ratio:.0%} of records were rejected (limit {MAX_REJECT_RATIO:.0%})"
            )

    stats = stream_users_to_kafka()
    ensure_kafka_topics() >> stats
    quality_gate(stats)


user_profiles_stream()
