"""Postgres-backed refresh state and run audit for NYC 311 ingestion."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from nyc311_stream.nyc311 import Cursor

SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS pipeline;
CREATE TABLE IF NOT EXISTS pipeline.refresh_state (
  source_name text PRIMARY KEY,
  source_version timestamptz NOT NULL,
  window_start text NOT NULL,
  cursor_created_date text,
  cursor_unique_key text,
  complete boolean NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS pipeline.ingestion_runs (
  run_id uuid PRIMARY KEY,
  source_name text NOT NULL,
  status text NOT NULL,
  started_at timestamptz NOT NULL,
  finished_at timestamptz NOT NULL DEFAULT now(),
  source_version timestamptz,
  window_start text,
  pages integer NOT NULL DEFAULT 0,
  fetched integer NOT NULL DEFAULT 0,
  produced integer NOT NULL DEFAULT 0,
  dead_lettered integer NOT NULL DEFAULT 0,
  cursor_before text,
  cursor_after text,
  refresh_complete boolean,
  error text
);
"""


@dataclass(frozen=True)
class RefreshState:
    """Progress of the refresh for one source version (one NYC data load)."""

    source_version: datetime
    window_start: str
    cursor: Cursor | None
    complete: bool


def _format_cursor(cursor: Cursor | None) -> str | None:
    return f"{cursor.created_date}|{cursor.unique_key}" if cursor else None


class PostgresStateStore:
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn

    def _connect(self):
        import psycopg2

        return psycopg2.connect(self.dsn)

    def ensure_schema(self) -> None:
        with self._connect() as connection, connection.cursor() as cursor:
            cursor.execute(SCHEMA_SQL)

    def get_state(self, source_name: str) -> RefreshState | None:
        with self._connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT source_version, window_start, cursor_created_date, cursor_unique_key, "
                "complete FROM pipeline.refresh_state WHERE source_name = %s",
                (source_name,),
            )
            row = cursor.fetchone()
        if row is None:
            return None
        version, start, created, key, complete = row
        return RefreshState(version, start, Cursor(created, key) if created else None, complete)

    def save_state(self, source_name: str, state: RefreshState) -> None:
        cursor_value = state.cursor
        with self._connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                "INSERT INTO pipeline.refresh_state (source_name, source_version, window_start, "
                "cursor_created_date, cursor_unique_key, complete) "
                "VALUES (%s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (source_name) DO UPDATE SET "
                "source_version = EXCLUDED.source_version, window_start = EXCLUDED.window_start, "
                "cursor_created_date = EXCLUDED.cursor_created_date, "
                "cursor_unique_key = EXCLUDED.cursor_unique_key, "
                "complete = EXCLUDED.complete, updated_at = now()",
                (
                    source_name,
                    state.source_version,
                    state.window_start,
                    cursor_value.created_date if cursor_value else None,
                    cursor_value.unique_key if cursor_value else None,
                    state.complete,
                ),
            )

    def record_run(self, values: dict[str, Any]) -> None:
        values = dict(values)
        for key in ("cursor_before", "cursor_after"):
            values[key] = _format_cursor(values.get(key))
        columns = ", ".join(values)
        placeholders = ", ".join(["%s"] * len(values))
        with self._connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                f"INSERT INTO pipeline.ingestion_runs ({columns}) VALUES ({placeholders})",
                tuple(values.values()),
            )
