"""Spark Structured Streaming job: Kafka NYC 311 events -> Cassandra."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from functools import partial

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

log = logging.getLogger("nyc311_to_cassandra")

CORRUPT_RECORD = "_corrupt_record"
UUID_PATTERN = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"

REQUEST_SCHEMA = StructType(
    [
        StructField("schema_version", IntegerType()),
        StructField("unique_key", StringType()),
        StructField("created_date", StringType()),
        StructField("closed_date", StringType()),
        StructField("agency", StringType()),
        StructField("agency_name", StringType()),
        StructField("complaint_type", StringType()),
        StructField("descriptor", StringType()),
        StructField("location_type", StringType()),
        StructField("incident_zip", StringType()),
        StructField("city", StringType()),
        StructField("borough", StringType()),
        StructField("status", StringType()),
        StructField("due_date", StringType()),
        StructField("resolution_description", StringType()),
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
        StructField("community_board", StringType()),
        StructField("council_district", StringType()),
        StructField("event_time", StringType()),
        StructField(CORRUPT_RECORD, StringType()),
    ]
)
REQUIRED_FIELDS = ("unique_key", "created_date", "agency", "complaint_type", "status")

REQUEST_COLUMNS = [
    "unique_key",
    "created_date",
    "closed_date",
    "agency",
    "agency_name",
    "complaint_type",
    "descriptor",
    "location_type",
    "incident_zip",
    "city",
    "borough",
    "status",
    "due_date",
    "resolution_description",
    "latitude",
    "longitude",
    "community_board",
    "council_district",
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
    topic: str = "nyc311_requests"
    starting_offsets: str = "earliest"
    max_offsets_per_trigger: int = 5000
    cassandra_host: str = "cassandra"
    cassandra_port: int = 9042
    keyspace: str = "nyc311_streams"
    checkpoint_dir: str = "/opt/spark-checkpoints/nyc311_to_cassandra"
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
        .when(F.to_timestamp(data["created_date"]).isNull(), F.lit("invalid_created_date"))
    )


def parse_requests(kafka_df: DataFrame) -> DataFrame:
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
            REQUEST_SCHEMA,
            {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": CORRUPT_RECORD},
        ),
    )
    return decoded.select(
        "raw_value",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        _validation_error(data).alias("error"),
        F.trim(data["unique_key"]).alias("unique_key"),
        F.to_timestamp(data["created_date"]).alias("created_date"),
        F.to_timestamp(data["closed_date"]).alias("closed_date"),
        F.trim(data["agency"]).alias("agency"),
        data["agency_name"].alias("agency_name"),
        F.trim(data["complaint_type"]).alias("complaint_type"),
        data["descriptor"].alias("descriptor"),
        data["location_type"].alias("location_type"),
        data["incident_zip"].alias("incident_zip"),
        data["city"].alias("city"),
        data["borough"].alias("borough"),
        F.trim(data["status"]).alias("status"),
        F.to_timestamp(data["due_date"]).alias("due_date"),
        data["resolution_description"].alias("resolution_description"),
        data["latitude"].alias("latitude"),
        data["longitude"].alias("longitude"),
        data["community_board"].alias("community_board"),
        data["council_district"].alias("council_district"),
        F.to_timestamp(data["event_time"]).alias("event_time"),
    )


def valid_requests(parsed: DataFrame) -> DataFrame:
    return (
        parsed.filter(F.col("error").isNull())
        .withColumn("ingested_at", F.current_timestamp())
        .select(*REQUEST_COLUMNS)
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
        requests = valid_requests(batch_df)
        rejected = rejected_records(batch_df)
        request_count, rejected_count = requests.count(), rejected.count()
        if request_count:
            _save(requests, keyspace, "service_requests")
        if rejected_count:
            _save(rejected, keyspace, "rejected_records")
        log.info(
            "batch %d: %d NYC 311 requests written, %d records rejected",
            batch_id,
            request_count,
            rejected_count,
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
        SparkSession.builder.appName("nyc311-kafka-to-cassandra")
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
        .option("failOnDataLoss", "true")
        .load()
    )
    query = (
        parse_requests(kafka_df)
        .writeStream.queryName("nyc311_to_cassandra")
        .foreachBatch(partial(write_batch, keyspace=config.keyspace))
        .option("checkpointLocation", config.checkpoint_dir)
        .trigger(processingTime=config.trigger_interval)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
