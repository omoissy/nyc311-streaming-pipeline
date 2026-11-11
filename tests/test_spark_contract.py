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
    USER_COLUMNS,
    USER_SCHEMA,
    parse_users,
    rejected_records,
    valid_users,
)

from user_stream.transform import format_user

SCHEMA_CQL = Path(__file__).resolve().parents[1] / "cassandra" / "schema.cql"
KAFKA_SCHEMA = StructType(
    [
        StructField("value", BinaryType()),
        StructField("partition", IntegerType()),
        StructField("offset", LongType()),
        StructField("timestamp", TimestampType()),
    ]
)
KAFKA_TS = datetime(2026, 11, 11, 9, 30, tzinfo=UTC)


def kafka_rows(spark, *values):
    """Build a DataFrame shaped like the Kafka source from raw message values."""
    rows = [
        (None if value is None else value.encode("utf-8"), 0, offset, KAFKA_TS)
        for offset, value in enumerate(values)
    ]
    return spark.createDataFrame(rows, KAFKA_SCHEMA)


def cql_columns(table: str) -> list[str]:
    body = re.search(
        rf"CREATE TABLE IF NOT EXISTS spark_streams\.{table} \((.*?)\n\);",
        SCHEMA_CQL.read_text(),
        re.S,
    ).group(1)
    return [
        line.split()[0]
        for line in body.strip().splitlines()
        if line.strip() and not line.strip().startswith("PRIMARY KEY")
    ]


def test_spark_schema_matches_producer_contract(raw_user):
    producer_fields = list(format_user(raw_user))
    assert USER_SCHEMA.names[:-1] == producer_fields


@pytest.mark.parametrize(
    ("table", "columns"),
    [("created_users", USER_COLUMNS), ("rejected_records", REJECTED_COLUMNS)],
)
def test_spark_output_matches_cassandra_tables(table, columns):
    assert cql_columns(table) == columns


def test_parse_users_separates_valid_and_invalid_json(spark, raw_user):
    valid = format_user(raw_user)
    kafka_df = kafka_rows(spark, json.dumps(valid), '{"id":"not-a-uuid"}', "{not-json")

    result = parse_users(kafka_df).select("id", "error").orderBy("id").collect()
    errors = {row["error"] for row in result}

    assert len(result) == 3
    assert any(row["error"] is None for row in result)
    assert "malformed_json" in errors
    assert any(error and error.startswith("missing_fields:") for error in errors)


def test_valid_record_is_typed_for_cassandra(spark, raw_user):
    kafka_df = kafka_rows(spark, json.dumps(format_user(raw_user)))

    [row] = valid_users(parse_users(kafka_df)).collect()

    assert row["id"] == "44957db7-9c04-4cff-8020-85aa39e37b4b"
    assert row["email"] == "emma.johansen@example.com"
    # collect() returns naive local-time datetimes; compare the instant in UTC.
    assert row["dob"].astimezone(UTC) == datetime(1982, 6, 24, 23, 39, 34, 775000, tzinfo=UTC)
    assert row["kafka_partition"] == 0
    assert row["kafka_offset"] == 0
    assert row["ingested_at"] is not None


@pytest.mark.parametrize(
    ("mutate", "reason"),
    [
        (lambda r: r.update(id="not-a-uuid"), "invalid_id"),
        (lambda r: r.update(email="nobody"), "invalid_email"),
        (lambda r: r.pop("email"), "missing_fields:email"),
        (lambda r: r.update(first_name="  ", last_name=None), "missing_fields:first_name,last_name"),
    ],
)
def test_invalid_records_are_rejected_with_reason(spark, raw_user, mutate, reason):
    message = format_user(raw_user)
    mutate(message)

    [row] = rejected_records(parse_users(kafka_rows(spark, json.dumps(message)))).collect()

    assert row["reason"] == reason
    assert row["raw_value"] == json.dumps(message)


def test_empty_and_malformed_messages_are_rejected(spark):
    rows = rejected_records(parse_users(kafka_rows(spark, None, "{oops", "[1, 2"))).collect()

    assert {(row["kafka_offset"], row["reason"]) for row in rows} == {
        (0, "empty_message"),
        (1, "malformed_json"),
        (2, "malformed_json"),
    }


def test_no_record_is_lost_between_outputs(spark, raw_users):
    messages = [json.dumps(format_user(raw)) for raw in raw_users] + ["{bad", None]
    parsed = parse_users(kafka_rows(spark, *messages))

    assert valid_users(parsed).count() + rejected_records(parsed).count() == len(messages)
