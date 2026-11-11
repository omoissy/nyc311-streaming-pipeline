"""Run one NYC 311 ingestion step outside Airflow."""

from __future__ import annotations

import argparse
import json
import logging
from dataclasses import replace

from nyc311_stream.config import Settings
from nyc311_stream.producer import ensure_topics, run_ingestion


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest NYC 311 service requests into Kafka")
    parser.add_argument("--bootstrap", help="Kafka bootstrap servers")
    parser.add_argument("--max-records", type=int, help="maximum records to fetch")
    parser.add_argument("--refresh-window-days", type=int, help="days of requests to re-read")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    settings = Settings.from_env()
    if args.bootstrap:
        settings = replace(settings, bootstrap_servers=args.bootstrap)
    ensure_topics(
        settings.bootstrap_servers,
        {settings.topic: settings.topic_partitions, settings.dlq_topic: 1},
    )
    result = run_ingestion(
        settings, max_records=args.max_records, refresh_window_days=args.refresh_window_days
    )
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
