"""Spark Structured Streaming job: Kafka -> validated, typed rows -> Cassandra.

Valid records are upserted into `created_users`, keyed by user id, so replaying a
micro-batch after a failure rewrites the same rows instead of duplicating them.
Records that fail parsing or validation go to `rejected_records` with the reason and
their Kafka coordinates, so nothing is silently dropped.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from functools import partial

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

log = logging.getLogger("stream_to_cassandra")

CORRUPT_RECORD = "_corrupt_record"
UUID_PATTERN = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
EMAIL_PATTERN = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"

# Message contract produced by src/user_stream/transform.py.
USER_SCHEMA = StructType(
    [
        StructField("schema_version", IntegerType()),
        StructField("id", StringType()),
        StructField("first_name", StringType()),
        StructField("last_name", StringType()),
        StructField("gender", StringType()),
        StructField("address", StringType()),
        StructField("post_code", StringType()),
        StructField("email", StringType()),
        StructField("username", StringType()),
        StructField("dob", StringType()),
        StructField("registered_date", StringType()),
        StructField("phone", StringType()),
        StructField("picture", StringType()),
        StructField("nationality", StringType()),
        StructField("event_time", StringType()),
        StructField(CORRUPT_RECORD, StringType()),
    ]
)
REQUIRED_FIELDS = ("id", "first_name", "last_name", "email")

# Column order of the Cassandra tables in cassandra/schema.cql.
USER_COLUMNS = [
    "id",
    "first_name",
    "last_name",
    "gender",
    "address",
    "post_code",
    "email",
    "username",
    "dob",
    "registered_date",
    "phone",
    "picture",
    "nationality",
    "event_time",
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
    "ingested_at",
]
REJECTED_COLUMNS = [
    "kafka_partition",
    "kafka_offset",
    "raw_value",
    "reason",
    "kafka_timestamp",
    "ingested_at",
]


@dataclass(frozen=True)
class JobConfig:
    kafka_bootstrap_servers: str = "kafka:29092"
    topic: str = "users_created"
    starting_offsets: str = "earliest"
    max_offsets_per_trigger: int = 5000
    cassandra_host: str = "cassandra"
    cassandra_port: int = 9042
    keyspace: str = "spark_streams"
    checkpoint_dir: str = "/opt/spark-checkpoints/users_to_cassandra"
    trigger_interval: str = "10 seconds"

    @classmethod
    def from_env(cls) -> JobConfig:
        env = os.environ.get
        return cls(
            kafka_bootstrap_servers=env("KAFKA_BOOTSTRAP_SERVERS", cls.kafka_bootstrap_servers),
            topic=env("KAFKA_TOPIC", cls.topic),
            starting_offsets=env("KAFKA_STARTING_OFFSETS", cls.starting_offsets),
            max_offsets_per_trigger=int(
                env("MAX_OFFSETS_PER_TRIGGER", str(cls.max_offsets_per_trigger))
            ),
            cassandra_host=env("CASSANDRA_HOST", cls.cassandra_host),
            cassandra_port=int(env("CASSANDRA_PORT", str(cls.cassandra_port))),
            keyspace=env("CASSANDRA_KEYSPACE", cls.keyspace),
            checkpoint_dir=env("CHECKPOINT_DIR", cls.checkpoint_dir),
            trigger_interval=env("TRIGGER_INTERVAL", cls.trigger_interval),
        )


def _validation_error(data: Column) -> Column:
    """Null for a valid record, otherwise a short machine-readable reason."""
    missing = F.array_compact(
        F.array(
            *[
                F.when(F.coalesce(F.trim(data[field]), F.lit("")) == "", F.lit(field))
                for field in REQUIRED_FIELDS
            ]
        )
    )
    return (
        F.when(F.col("raw_value").isNull(), F.lit("empty_message"))
        .when(data[CORRUPT_RECORD].isNotNull(), F.lit("malformed_json"))
        .when(F.size(missing) > 0, F.concat(F.lit("missing_fields:"), F.array_join(missing, ",")))
        .when(~F.lower(F.trim(data["id"])).rlike(UUID_PATTERN), F.lit("invalid_id"))
        .when(~F.trim(data["email"]).rlike(EMAIL_PATTERN), F.lit("invalid_email"))
    )


def parse_users(kafka_df: DataFrame) -> DataFrame:
    """Kafka source rows -> typed user columns plus an `error` column (null when valid)."""
    data = F.col("data")
    decoded = kafka_df.select(
        F.col("value").cast("string").alias("raw_value"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
    ).withColumn(
        "data",
        F.from_json(
            "raw_value",
            USER_SCHEMA,
            {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": CORRUPT_RECORD},
        ),
    )
    return decoded.select(
        "raw_value",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        _validation_error(data).alias("error"),
        F.lower(F.trim(data["id"])).alias("id"),
        F.trim(data["first_name"]).alias("first_name"),
        F.trim(data["last_name"]).alias("last_name"),
        data["gender"].alias("gender"),
        data["address"].alias("address"),
        data["post_code"].alias("post_code"),
        F.lower(F.trim(data["email"])).alias("email"),
        data["username"].alias("username"),
        F.to_timestamp(data["dob"]).alias("dob"),
        F.to_timestamp(data["registered_date"]).alias("registered_date"),
        data["phone"].alias("phone"),
        data["picture"].alias("picture"),
        data["nationality"].alias("nationality"),
        F.to_timestamp(data["event_time"]).alias("event_time"),
    )


def valid_users(parsed: DataFrame) -> DataFrame:
    return (
        parsed.filter(F.col("error").isNull())
        .withColumn("ingested_at", F.current_timestamp())
        .select(*USER_COLUMNS)
    )


def rejected_records(parsed: DataFrame) -> DataFrame:
    return (
        parsed.filter(F.col("error").isNotNull())
        .withColumnRenamed("error", "reason")
        .withColumn("ingested_at", F.current_timestamp())
        .select(*REJECTED_COLUMNS)
    )


def _save(df: DataFrame, keyspace: str, table: str) -> None:
    (
        df.write.format("org.apache.spark.sql.cassandra")
        .mode("append")
        .options(keyspace=keyspace, table=table)
        .save()
    )


def write_batch(batch_df: DataFrame, batch_id: int, keyspace: str) -> None:
    batch_df.persist()
    try:
        users = valid_users(batch_df)
        rejected = rejected_records(batch_df)
        user_count, rejected_count = users.count(), rejected.count()
        if user_count:
            _save(users, keyspace, "created_users")
        if rejected_count:
            _save(rejected, keyspace, "rejected_records")
        log.info(
            "batch %d: %d users upserted, %d records rejected", batch_id, user_count, rejected_count
        )
    finally:
        batch_df.unpersist()


class ProgressLogger(StreamingQueryListener):
    def onQueryStarted(self, event):
        log.info("query started: name=%s id=%s", event.name, event.id)

    def onQueryProgress(self, event):
        progress = event.progress
        if progress.numInputRows:
            log.info(
                "progress: batch=%s rows=%s rows/sec=%.1f trigger_ms=%s",
                progress.batchId,
                progress.numInputRows,
                progress.processedRowsPerSecond or 0.0,
                progress.durationMs.get("triggerExecution"),
            )

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        if event.exception:
            log.error("query %s terminated with error: %s", event.id, event.exception)
        else:
            log.info("query %s stopped", event.id)


def build_spark(config: JobConfig) -> SparkSession:
    return (
        SparkSession.builder.appName("users-kafka-to-cassandra")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.cassandra.connection.host", config.cassandra_host)
        .config("spark.cassandra.connection.port", str(config.cassandra_port))
        .getOrCreate()
    )


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s"
    )
    config = JobConfig.from_env()
    log.info("starting with %s", config)

    spark = build_spark(config)
    spark.sparkContext.setLogLevel("WARN")
    spark.streams.addListener(ProgressLogger())

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.kafka_bootstrap_servers)
        .option("subscribe", config.topic)
        .option("startingOffsets", config.starting_offsets)
        .option("maxOffsetsPerTrigger", config.max_offsets_per_trigger)
        # Don't crash if retention deleted offsets the checkpoint still points at.
        .option("failOnDataLoss", "false")
        .load()
    )

    query = (
        parse_users(kafka_df)
        .writeStream.queryName("users_to_cassandra")
        .foreachBatch(partial(write_batch, keyspace=config.keyspace))
        .option("checkpointLocation", config.checkpoint_dir)
        .trigger(processingTime=config.trigger_interval)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
