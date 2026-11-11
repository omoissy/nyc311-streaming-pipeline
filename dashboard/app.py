"""Operations dashboard for the real NYC 311 streaming pipeline."""

from __future__ import annotations

import os
from datetime import UTC, datetime
from itertools import islice
from typing import Any

import altair as alt
import pandas as pd
import requests
import streamlit as st
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from confluent_kafka import Consumer, TopicPartition
from metrics import (
    RECENT_HOURS_MAY_BE_INCOMPLETE,
    data_quality,
    hourly_volume,
    prepare,
    recent_cutoff,
    resolution_times,
    top_counts,
)
from psycopg2 import connect

AIRFLOW_URL = os.getenv("AIRFLOW_URL", "http://airflow-webserver:8080")
AIRFLOW_USER = os.getenv("AIRFLOW_ADMIN_USER", "airflow")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_ADMIN_PASSWORD", "airflow")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "nyc311_requests")
KAFKA_DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "nyc311_requests_dlq")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "nyc311_streams")
PIPELINE_DB_URL = os.getenv("PIPELINE_DB_URL", "postgresql://airflow:airflow@postgres/airflow")
GITHUB_REPO = os.getenv("GITHUB_REPO", "omoissy/nyc311-streaming-pipeline")
# Charts read every stored request up to this many rows, and say so if they hit it.
MAX_ROWS = int(os.getenv("DASHBOARD_MAX_ROWS", "300000"))

ACCENT = "#2563eb"
COMPOSE_SERVICES = [
    "postgres",
    "airflow-init",
    "airflow-webserver",
    "airflow-scheduler",
    "kafka",
    "kafka-init",
    "kafka-ui",
    "spark-master",
    "spark-worker",
    "spark-streaming",
    "cassandra",
    "cassandra-init",
    "dashboard",
]

st.set_page_config(
    page_title="NYC 311 Pipeline",
    page_icon="🏙️",
    layout="wide",
    initial_sidebar_state="expanded",
)


def http_json(
    url: str, *, auth: tuple[str, str] | None = None
) -> tuple[dict | list | None, str | None]:
    try:
        response = requests.get(url, auth=auth, timeout=4)
        response.raise_for_status()
        return response.json(), None
    except (requests.RequestException, ValueError) as exc:
        return None, str(exc)


@st.cache_data(ttl=60, show_spinner="Reading requests from Cassandra…")
def load_requests() -> tuple[pd.DataFrame, bool]:
    """All stored requests (paged), plus whether MAX_ROWS cut the read short."""
    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
    try:
        session = cluster.connect(CASSANDRA_KEYSPACE)
        statement = SimpleStatement(
            "SELECT unique_key, created_date, closed_date, complaint_type, agency, borough, "
            "status FROM service_requests",
            fetch_size=5000,
        )
        rows = list(islice(session.execute(statement), MAX_ROWS + 1))
    finally:
        cluster.shutdown()
    truncated = len(rows) > MAX_ROWS
    return pd.DataFrame(rows[:MAX_ROWS]), truncated


@st.cache_data(ttl=60, show_spinner=False)
def load_rejected() -> pd.DataFrame:
    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
    try:
        session = cluster.connect(CASSANDRA_KEYSPACE)
        return pd.DataFrame(list(session.execute("SELECT reason FROM rejected_records")))
    finally:
        cluster.shutdown()


@st.cache_data(ttl=30, show_spinner=False)
def load_postgres_state() -> tuple[dict[str, Any] | None, pd.DataFrame]:
    with connect(PIPELINE_DB_URL) as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT source_version, window_start, cursor_created_date, cursor_unique_key, "
            "complete, updated_at FROM pipeline.refresh_state"
        )
        row = cursor.fetchone()
        state = dict(zip([d.name for d in cursor.description], row)) if row else None

        cursor.execute(
            "SELECT finished_at, status, pages, fetched, produced, dead_lettered, "
            "refresh_complete, error FROM pipeline.ingestion_runs "
            "ORDER BY finished_at DESC LIMIT 20"
        )
        runs = pd.DataFrame(cursor.fetchall(), columns=[d.name for d in cursor.description])
    return state, runs


@st.cache_data(ttl=300, show_spinner=False)
def latest_ci_run() -> tuple[dict | None, str | None]:
    payload, error = http_json(
        f"https://api.github.com/repos/{GITHUB_REPO}/actions/runs?per_page=1"
    )
    runs = payload.get("workflow_runs") if isinstance(payload, dict) else None
    return (runs[0] if runs else None), error


def kafka_status() -> list[dict[str, Any]]:
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": "nyc311-dashboard-observer",
            "enable.auto.commit": False,
        }
    )
    try:
        metadata = consumer.list_topics(timeout=4).topics
        result = []
        for topic in (KAFKA_TOPIC, KAFKA_DLQ_TOPIC):
            partitions = metadata[topic].partitions if topic in metadata else {}
            end_offsets = [
                consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=4)[1]
                for p in partitions
            ]
            result.append(
                {
                    "topic": topic,
                    "exists": topic in metadata,
                    "partitions": len(partitions),
                    "messages": sum(end_offsets),
                }
            )
        return result
    finally:
        consumer.close()


def horizontal_bars(frame: pd.DataFrame, category: str, title: str) -> alt.LayerChart:
    """Bars sorted largest first, each labelled with its share of all requests."""
    base = alt.Chart(frame).encode(
        # Start at zero; leave headroom on the right for the share labels.
        x=alt.X(
            "requests:Q",
            title=title,
            scale=alt.Scale(domain=[0, float(frame["requests"].max()) * 1.15]),
        ),
        y=alt.Y(f"{category}:N", sort="-x", title=None),
        tooltip=[
            category,
            alt.Tooltip("requests:Q", format=","),
            alt.Tooltip("share:Q", title="share of all requests", format=".1%"),
        ],
    )
    bars = base.mark_bar(color=ACCENT)
    labels = base.mark_text(align="left", dx=4, color="#334155").encode(text="label:N")
    return (bars + labels).properties(height=max(120, 26 * len(frame)))


def render_health(kafka: list[dict[str, Any]] | None) -> None:
    st.subheader("Pipeline health")
    airflow, airflow_error = http_json(
        f"{AIRFLOW_URL}/api/v1/health", auth=(AIRFLOW_USER, AIRFLOW_PASSWORD)
    )
    spark_master, spark_error = http_json("http://spark-master:8080/json/")
    spark_driver, driver_error = http_json("http://spark-streaming:4040/api/v1/applications")
    kafka_ok = bool(kafka) and all(topic["exists"] for topic in kafka)

    columns = st.columns(5)
    columns[0].metric("Airflow", "healthy" if airflow else "unavailable")
    columns[1].metric("Kafka topics", "healthy" if kafka_ok else "unavailable")
    columns[2].metric("Spark master", "healthy" if spark_master else "unavailable")
    columns[3].metric("Spark streaming job", "running" if spark_driver else "unavailable")
    columns[4].metric("Checked (UTC)", datetime.now(UTC).strftime("%H:%M:%S"))

    with st.expander("Health details"):
        if airflow:
            st.json(airflow)
        for name, error in (
            ("Airflow", airflow_error),
            ("Spark master", spark_error),
            ("Spark streaming job", driver_error),
        ):
            if error:
                st.warning(f"{name} unavailable: {error}")


def render_charts(raw: pd.DataFrame, truncated: bool) -> None:
    st.subheader("What is in Cassandra?")
    if raw.empty:
        st.info("No service requests have arrived yet. Trigger the `nyc311_stream` DAG in Airflow.")
        return

    data = prepare(raw)
    quality = data_quality(data)
    first, last = data["created_date"].min(), data["created_date"].max()
    coverage = f"the first {MAX_ROWS:,} rows read" if truncated else "every stored request"
    st.caption(
        f"{quality['rows']:,} requests created {first:%b %d %H:%M} to {last:%b %d %H:%M} "
        f"New York time; charts use {coverage}. NYC publishes daily, so the newest requests "
        "are usually one to two days old."
    )
    if truncated:
        st.warning(
            f"Only {MAX_ROWS:,} rows were read (DASHBOARD_MAX_ROWS); counts are understated."
        )

    left, right = st.columns(2)
    with left:
        st.markdown("**How many requests are created each hour?**")
        volume = hourly_volume(data)
        line = (
            alt.Chart(volume)
            .mark_line(color=ACCENT)
            .encode(
                x=alt.X("hour:T", title="Hour created (New York time)"),
                y=alt.Y("requests:Q", title="Requests"),
                tooltip=[alt.Tooltip("hour:T", format="%b %d %H:00"), "requests:Q"],
            )
        )
        recent = pd.DataFrame(
            {"start": [recent_cutoff(data)], "end": [volume["hour"].max()],
             "note": ["Recent hours: may still be filling in"]}
        )
        shade = alt.Chart(recent).mark_rect(color="#cbd5e1", opacity=0.6).encode(
            x="start:T", x2="end:T", tooltip=["note:N"]
        )
        st.altair_chart((shade + line).properties(height=280), use_container_width=True)
        st.caption(
            "Requests per hour of creation; the daily cycle peaks in late morning. The "
            f"shaded last {RECENT_HOURS_MAY_BE_INCOMPLETE} hours may still be filling in, "
            "because NYC publishes recent requests in batches."
        )
    with right:
        st.markdown("**Which complaint types are most common?**")
        st.altair_chart(
            horizontal_bars(top_counts(data, "complaint_type", 10), "complaint_type", "Requests"),
            use_container_width=True,
        )
        st.caption("The ten most frequent complaint types; labels show share of all requests.")

    left, right = st.columns(2)
    with left:
        st.markdown("**How long do requests take to close?**")
        times = resolution_times(data)
        if times.empty:
            st.info("Not enough closed requests yet to compare closure times.")
        else:
            base = alt.Chart(times).encode(
                y=alt.Y("complaint_type:N", sort=alt.SortField("median_hours"), title=None)
            )
            spread = base.mark_rule(color="#94a3b8").encode(
                x=alt.X("p25:Q", title="Hours from creation to closure"), x2="p75:Q"
            )
            median = base.mark_point(color=ACCENT, filled=True, size=70).encode(
                x="median_hours:Q",
                tooltip=[
                    "complaint_type",
                    alt.Tooltip("median_hours:Q", title="median hours", format=".1f"),
                    alt.Tooltip("p25:Q", title="25th percentile", format=".1f"),
                    alt.Tooltip("p75:Q", title="75th percentile", format=".1f"),
                    alt.Tooltip("closed:Q", title="closed requests", format=","),
                ],
            )
            st.altair_chart(
                (spread + median).properties(height=max(120, 28 * len(times))),
                use_container_width=True,
            )
            st.caption(
                f"Dot: median hours to close; line: middle 50% of closures. Covers the ten "
                f"most common types with at least 20 closures ({quality['closed']:,} closed, "
                f"{quality['open']:,} still open). Open requests are excluded, so times lean "
                "short for types that close slowly."
            )
    with right:
        st.markdown("**What share of requests is still open?**")
        st.altair_chart(
            horizontal_bars(top_counts(data, "status"), "status", "Requests"),
            use_container_width=True,
        )
        st.caption(
            "Current status of each stored request. Statuses update after each daily NYC load."
        )

    st.markdown("**Where are requests concentrated?**")
    st.altair_chart(
        horizontal_bars(top_counts(data, "borough"), "borough", "Requests"),
        use_container_width=True,
    )
    if quality["closed_before_created"]:
        st.caption(
            f"{quality['closed_before_created']:,} requests close before they were created "
            "in the source data; they are excluded from closure times."
        )


def render_operations(
    state: dict[str, Any] | None, runs: pd.DataFrame, kafka: list[dict[str, Any]] | None
) -> None:
    st.subheader("Operations and lineage")
    left, right = st.columns(2)
    with left:
        st.markdown("**Refresh state (Postgres)**")
        if state:
            st.dataframe(pd.DataFrame([state]), hide_index=True, use_container_width=True)
        else:
            st.info("No refresh yet; the first Airflow run will create it.")
        st.markdown("**Recent ingestion runs**")
        st.dataframe(runs, hide_index=True, use_container_width=True)
    with right:
        st.markdown("**Kafka topics**")
        if kafka is None:
            st.warning("Kafka unavailable.")
        else:
            st.dataframe(pd.DataFrame(kafka), hide_index=True, use_container_width=True)
        st.markdown("**Records Spark rejected**")
        try:
            rejected = load_rejected()
        except Exception as exc:
            st.warning(f"Cassandra unavailable: {exc}")
        else:
            if rejected.empty:
                st.caption("None: every message parsed and validated.")
            else:
                st.dataframe(top_counts(rejected, "reason"), hide_index=True)


def render_stack_links() -> None:
    st.subheader("Full stack")
    links = {
        "Airflow": "http://localhost:8080",
        "Kafka UI": "http://localhost:8085",
        "Spark master": "http://localhost:9090",
        "Spark streaming job": "http://localhost:4040",
        "GitHub Actions": f"https://github.com/{GITHUB_REPO}/actions",
    }
    st.markdown(" · ".join(f"[{name}]({url})" for name, url in links.items()))
    with st.expander(f"Docker Compose services ({len(COMPOSE_SERVICES)})"):
        st.code("\n".join(COMPOSE_SERVICES), language="text")
    run, error = latest_ci_run()
    if run:
        st.caption(
            f"Latest GitHub Actions run: {run['status']} / {run.get('conclusion') or 'running'} "
            f"on {run['head_branch']}"
        )
    elif error:
        st.caption(f"GitHub Actions status unavailable: {error}")


def main() -> None:
    st.title("NYC 311 Streaming Pipeline")
    st.caption("Airflow → Kafka → Spark Structured Streaming → Cassandra, on live NYC Open Data")
    if st.button("Refresh dashboard"):
        st.cache_data.clear()
        st.rerun()

    try:
        kafka = kafka_status()
    except Exception:
        kafka = None
    render_health(kafka)

    try:
        data, truncated = load_requests()
    except Exception as exc:
        st.error(f"Cassandra is not ready: {exc}")
        data, truncated = pd.DataFrame(), False
    render_charts(data, truncated)

    try:
        state, runs = load_postgres_state()
    except Exception as exc:
        st.error(f"Postgres state is not ready: {exc}")
        state, runs = None, pd.DataFrame()
    render_operations(state, runs, kafka)
    render_stack_links()


if __name__ == "__main__":
    main()
