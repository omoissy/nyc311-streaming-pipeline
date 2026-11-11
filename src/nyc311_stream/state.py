"""Postgres-backed cursor and run audit state for incremental ingestion."""

from __future__ import annotations

from typing import Any

from nyc311_stream.nyc311 import Cursor

SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS pipeline;
CREATE TABLE IF NOT EXISTS pipeline.ingestion_cursor (
  source_name text PRIMARY KEY,
  created_date timestamptz NOT NULL,
  unique_key text NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS pipeline.ingestion_runs (
  run_id uuid PRIMARY KEY,
  source_name text NOT NULL,
  started_at timestamptz NOT NULL,
  finished_at timestamptz NOT NULL DEFAULT now(),
  fetched integer NOT NULL,
  produced integer NOT NULL,
  dead_lettered integer NOT NULL,
  cursor_before_created_date timestamptz,
  cursor_before_unique_key text,
  cursor_after_created_date timestamptz,
  cursor_after_unique_key text,
  status text NOT NULL
);
"""


class PostgresStateStore:
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn

    def _connect(self):
        import psycopg2

        return psycopg2.connect(self.dsn)

    def ensure_schema(self) -> None:
        with self._connect() as connection, connection.cursor() as cursor:
            cursor.execute(SCHEMA_SQL)

    def get_cursor(self, source_name: str) -> Cursor | None:
        with self._connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT created_date, unique_key FROM pipeline.ingestion_cursor "
                "WHERE source_name = %s",
                (source_name,),
            )
            row = cursor.fetchone()
        if row is None:
            return None
        return Cursor(created_date=row[0], unique_key=row[1])

    def save_cursor(self, source_name: str, cursor_value: Cursor) -> None:
        with self._connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                "INSERT INTO pipeline.ingestion_cursor "
                "(source_name, created_date, unique_key) VALUES (%s, %s, %s) "
                "ON CONFLICT (source_name) DO UPDATE SET created_date = EXCLUDED.created_date, "
                "unique_key = EXCLUDED.unique_key, updated_at = now()",
                (source_name, cursor_value.created_date, cursor_value.unique_key),
            )

    def record_run(self, values: dict[str, Any]) -> None:
        values = dict(values)
        before = values.pop("cursor_before", None)
        after = values.pop("cursor_after", None)
        values.update(
            {
                "cursor_before_created_date": before.created_date if before else None,
                "cursor_before_unique_key": before.unique_key if before else None,
                "cursor_after_created_date": after.created_date if after else None,
                "cursor_after_unique_key": after.unique_key if after else None,
            }
        )
        columns = ", ".join(values)
        placeholders = ", ".join(["%s"] * len(values))
        with self._connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                f"INSERT INTO pipeline.ingestion_runs ({columns}) VALUES ({placeholders})",
                tuple(values.values()),
            )
