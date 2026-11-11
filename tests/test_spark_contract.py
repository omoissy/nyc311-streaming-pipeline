from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pyspark.sql.types import (
    BinaryType,
    IntegerType,
    LongType,
    StructField,
    StructType,
    TimestampType,
)
from stream_to_cassandra import (
    REJECTED_COLUMNS,
    REQUEST_COLUMNS,
    REQUEST_SCHEMA,
    parse_requests,
    rejected_records,
    valid_requests,
)

from nyc311_stream.transform import format_request

SCHEMA_CQL = Path(__file__).resolve().parents[1] / "cassandra" / "schema.cql"
KAFKA_SCHEMA = StructType(
    [
        StructField("value", BinaryType()),
        StructField("partition", IntegerType()),
        StructField("offset", LongType()),
        StructField("timestamp", TimestampType()),
    ]
)


def kafka_rows(spark, *values):
    rows = [
        (None if value is None else value.encode(), 0, offset, datetime(2026, 11, 11, tzinfo=UTC))
        for offset, value in enumerate(values)
    ]
    return spark.createDataFrame(rows, KAFKA_SCHEMA)


def cql_columns(table: str) -> list[str]:
    body = re.search(
        rf"CREATE TABLE IF NOT EXISTS nyc311_streams\.{table} \((.*?)\n\);",
        SCHEMA_CQL.read_text(),
        re.S,
    ).group(1)
    return [
        line.split()[0]
        for line in body.strip().splitlines()
        if line.strip() and not line.strip().startswith("PRIMARY KEY")
    ]


def test_spark_schema_matches_producer_contract(raw_request):
    assert REQUEST_SCHEMA.names[:-1] == list(format_request(raw_request))


@pytest.mark.parametrize(
    ("table", "columns"),
    [("service_requests", REQUEST_COLUMNS), ("rejected_records", REJECTED_COLUMNS)],
)
def test_spark_output_matches_cassandra_tables(table, columns):
    assert cql_columns(table) == columns


def test_valid_and_invalid_messages_are_partitioned(spark, raw_request):
    valid = format_request(raw_request)
    kafka_df = kafka_rows(spark, json.dumps(valid), '{"unique_key":"bad"}', "{not-json", None)

    parsed = parse_requests(kafka_df)
    valid_rows = valid_requests(parsed).collect()
    rejected = rejected_records(parsed).collect()

    assert len(valid_rows) == 1
    assert valid_rows[0]["unique_key"] == "70000001"
    # collect() returns naive local-time datetimes; compare the instant in UTC.
    assert valid_rows[0]["created_date"].astimezone(UTC) == datetime(2026, 11, 10, 15, tzinfo=UTC)
    assert {row["reason"] for row in rejected} == {
        "empty_message",
        "malformed_json",
        "missing_fields:created_date,agency,complaint_type,status",
    }


def test_unparseable_created_date_is_rejected(spark, raw_request):
    message = format_request(raw_request)
    message["created_date"] = "not-a-date"

    [row] = rejected_records(parse_requests(kafka_rows(spark, json.dumps(message)))).collect()

    assert row["reason"] == "invalid_created_date"


def test_newest_version_of_a_request_wins_within_a_batch(spark, raw_request):
    first_seen = format_request(raw_request) | {
        "status": "Open",
        "source_updated_at": "2026-11-10T02:32:48.798Z",
    }
    closed = first_seen | {"status": "Closed", "source_updated_at": "2026-11-11T02:39:47.123Z"}
    # A replay of the older version arrives later (higher offset) but must not win.
    kafka_df = kafka_rows(spark, json.dumps(first_seen), json.dumps(closed), json.dumps(first_seen))

    [row] = valid_requests(parse_requests(kafka_df)).collect()

    assert row["status"] == "Closed"
    assert row["kafka_offset"] == 1


def test_every_message_is_either_written_or_rejected(spark, raw_requests):
    messages = [json.dumps(format_request(raw)) for raw in raw_requests] + ["{bad", None]
    parsed = parse_requests(kafka_rows(spark, *messages))

    assert valid_requests(parsed).count() + rejected_records(parsed).count() == len(messages)
