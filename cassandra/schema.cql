-- Applied by the cassandra-init service. Every statement is idempotent.

CREATE KEYSPACE IF NOT EXISTS spark_streams
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- One row per user. Writes are upserts on id, so replayed micro-batches are harmless.
CREATE TABLE IF NOT EXISTS spark_streams.created_users (
  id uuid PRIMARY KEY,
  first_name text,
  last_name text,
  gender text,
  address text,
  post_code text,
  email text,
  username text,
  dob timestamp,
  registered_date timestamp,
  phone text,
  picture text,
  nationality text,
  event_time timestamp,
  kafka_partition int,
  kafka_offset bigint,
  kafka_timestamp timestamp,
  ingested_at timestamp
);

-- Messages that failed parsing or validation, addressable by their Kafka coordinates.
CREATE TABLE IF NOT EXISTS spark_streams.rejected_records (
  kafka_partition int,
  kafka_offset bigint,
  raw_value text,
  reason text,
  kafka_timestamp timestamp,
  ingested_at timestamp,
  PRIMARY KEY ((kafka_partition), kafka_offset)
);
