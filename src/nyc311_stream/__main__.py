"""Run one NYC 311 ingestion batch outside Airflow."""

from __future__ import annotations

import argparse
import json
import logging

from nyc311_stream.config import Settings
from nyc311_stream.producer import ensure_topics, run_ingestion


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest NYC 311 service requests into Kafka")
    parser.add_argument("--bootstrap", help="Kafka bootstrap servers")
    parser.add_argument("--max-records", type=int, help="maximum records to fetch")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    settings = Settings.from_env()
    if args.bootstrap:
        from dataclasses import replace

        settings = replace(settings, bootstrap_servers=args.bootstrap)
    ensure_topics(
        settings.bootstrap_servers,
        {settings.topic: settings.topic_partitions, settings.dlq_topic: 1},
    )
    print(json.dumps(run_ingestion(settings, max_records=args.max_records), indent=2, default=str))


if __name__ == "__main__":
    main()
