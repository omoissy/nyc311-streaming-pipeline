"""Run the producer outside Airflow, e.g. `python -m user_stream --bootstrap localhost:9092`."""

from __future__ import annotations

import argparse
import dataclasses
import json
import logging

from user_stream.config import Settings
from user_stream.producer import ensure_topics, run_stream


def main() -> None:
    parser = argparse.ArgumentParser(description="Stream randomuser.me profiles into Kafka")
    parser.add_argument("--bootstrap", help="Kafka bootstrap servers (default: from env)")
    parser.add_argument("--duration", type=int, help="seconds to stream (default: from env)")
    parser.add_argument("--batch-size", type=int, help="users per API call (default: from env)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    settings = Settings.from_env()
    if args.bootstrap:
        settings = dataclasses.replace(settings, bootstrap_servers=args.bootstrap)

    ensure_topics(
        settings.bootstrap_servers,
        {settings.topic: settings.topic_partitions, settings.dlq_topic: 1},
    )
    stats = run_stream(settings, duration_seconds=args.duration, batch_size=args.batch_size)
    print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
