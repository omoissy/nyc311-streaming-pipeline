-- NYC 311 data tables. Statements are idempotent for container restarts.

CREATE KEYSPACE IF NOT EXISTS nyc311_streams
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS nyc311_streams.service_requests (
  unique_key text PRIMARY KEY,
  created_date timestamp,
  closed_date timestamp,
  agency text,
  agency_name text,
  complaint_type text,
  descriptor text,
  location_type text,
  incident_zip text,
  city text,
  borough text,
  status text,
  due_date timestamp,
  resolution_description text,
  latitude double,
  longitude double,
  community_board text,
  council_district text,
  event_time timestamp,
  kafka_partition int,
  kafka_offset bigint,
  kafka_timestamp timestamp,
  ingested_at timestamp
);

CREATE TABLE IF NOT EXISTS nyc311_streams.rejected_records (
  kafka_partition int,
  kafka_offset bigint,
  raw_value text,
  reason text,
  kafka_timestamp timestamp,
  ingested_at timestamp,
  PRIMARY KEY ((kafka_partition), kafka_offset)
);
