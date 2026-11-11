from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from pathlib import Path

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
    assert cql_columns("service_requests") == REQUEST_COLUMNS
    assert cql_columns("rejected_records") == REJECTED_COLUMNS


def test_valid_and_invalid_messages_are_partitioned(spark, raw_request):
    valid = format_request(raw_request)
    kafka_df = kafka_rows(spark, json.dumps(valid), '{"unique_key":"bad"}', "{not-json", None)

    parsed = parse_requests(kafka_df)
    valid_rows = valid_requests(parsed).collect()
    rejected = rejected_records(parsed).collect()

    assert len(valid_rows) == 1
    assert valid_rows[0]["unique_key"] == "70000001"
    assert len(rejected) == 3
    assert {row["reason"] for row in rejected} == {
        "empty_message",
        "malformed_json",
        "missing_fields:created_date,agency,complaint_type,status",
    }
