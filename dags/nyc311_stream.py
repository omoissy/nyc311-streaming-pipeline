"""
### nyc311_stream

Incrementally ingests real NYC 311 service requests from NYC Open Data into Kafka.
Kafka is the durable event boundary; Spark Structured Streaming validates and writes
the events into Cassandra. Postgres stores the source cursor and an ingestion audit.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param

log = logging.getLogger(__name__)

DEFAULT_MAX_RECORDS = int(os.getenv("MAX_RECORDS_PER_RUN", "500"))
DEFAULT_LOOKBACK_HOURS = int(os.getenv("INITIAL_LOOKBACK_HOURS", "168"))
MAX_REJECT_RATIO = float(os.getenv("MAX_REJECT_RATIO", "0.2"))


@dag(
    dag_id="nyc311_stream",
    description="Incrementally ingest NYC 311 service requests into Kafka",
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
            maximum=5000,
            description="Maximum new NYC 311 records to fetch in this run",
        ),
        "initial_lookback_hours": Param(
            DEFAULT_LOOKBACK_HOURS,
            type="integer",
            minimum=1,
            maximum=720,
            description="Lookback used only on the first run without a Postgres cursor",
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

    @task(execution_timeout=timedelta(minutes=10))
    def ingest_nyc311() -> dict:
        from airflow.operators.python import get_current_context
        from nyc311_stream.config import Settings
        from nyc311_stream.producer import run_ingestion

        params = get_current_context()["params"]
        return run_ingestion(
            Settings.from_env(),
            max_records=params["max_records"],
            initial_lookback_hours=params["initial_lookback_hours"],
        )

    @task
    def quality_gate(stats: dict) -> None:
        log.info("NYC 311 ingestion stats: %s", stats)
        if stats["fetched"] == 0:
            log.info("No new NYC 311 records were available; this is a healthy no-op run")
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
