"""
### nyc311_stream

Ingests real NYC 311 service requests from NYC Open Data into Kafka. Each run checks
when NYC last loaded data; after a new load it re-reads every request created in the
last `refresh_window_days`, so new requests, late arrivals and status/closure changes
all reach Cassandra (upserted on `unique_key`). Without a new load the run is a no-op.
Postgres stores the refresh progress and an audit row for every run, including failures.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param

log = logging.getLogger(__name__)

DEFAULT_MAX_RECORDS = int(os.getenv("MAX_RECORDS_PER_RUN", "100000"))
DEFAULT_WINDOW_DAYS = int(os.getenv("REFRESH_WINDOW_DAYS", "7"))
MAX_REJECT_RATIO = float(os.getenv("MAX_REJECT_RATIO", "0.2"))


@dag(
    dag_id="nyc311_stream",
    description="Refresh recent NYC 311 service requests into Kafka after each NYC load",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "omoissy",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": True,
    },
    params={
        "max_records": Param(
            DEFAULT_MAX_RECORDS,
            type="integer",
            minimum=1,
            maximum=500_000,
            description="Maximum records to fetch; a capped refresh resumes next run",
        ),
        "refresh_window_days": Param(
            DEFAULT_WINDOW_DAYS,
            type="integer",
            minimum=1,
            maximum=30,
            description="Days of requests to re-read after a new NYC data load",
        ),
    },
    tags=["nyc311", "streaming", "kafka", "spark", "cassandra"],
    doc_md=__doc__,
)
def nyc311_stream():
    @task
    def ensure_kafka_topics() -> list[str]:
        from nyc311_stream.config import Settings
        from nyc311_stream.producer import ensure_topics

        settings = Settings.from_env()
        return ensure_topics(
            settings.bootstrap_servers,
            {settings.topic: settings.topic_partitions, settings.dlq_topic: 1},
        )

    @task(execution_timeout=timedelta(minutes=50))
    def ingest_nyc311() -> dict:
        from airflow.operators.python import get_current_context
        from nyc311_stream.config import Settings
        from nyc311_stream.producer import run_ingestion

        params = get_current_context()["params"]
        return run_ingestion(
            Settings.from_env(),
            max_records=params["max_records"],
            refresh_window_days=params["refresh_window_days"],
        )

    @task
    def quality_gate(stats: dict) -> None:
        log.info("NYC 311 ingestion stats: %s", stats)
        if stats["fetched"] == 0:
            log.info("No new NYC data was loaded since the last refresh; healthy no-op")
            return
        reject_ratio = stats["dead_lettered"] / stats["fetched"]
        if reject_ratio > MAX_REJECT_RATIO:
            raise AirflowFailException(
                f"{reject_ratio:.0%} of records were rejected "
                f"(limit {MAX_REJECT_RATIO:.0%})"
            )

    topics = ensure_kafka_topics()
    stats = ingest_nyc311()
    topics >> stats
    quality_gate(stats)


nyc311_stream()
