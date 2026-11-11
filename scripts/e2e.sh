#!/usr/bin/env bash
# End-to-end check against a running stack (`docker compose up -d --build` first):
# trigger the DAG, require the run to succeed, then require NYC 311 rows in Cassandra.
set -euo pipefail

compose=(docker compose)
dag_id=nyc311_stream
airflow_url=${AIRFLOW_URL:-http://localhost:8080}
airflow_auth="${AIRFLOW_ADMIN_USER:-airflow}:${AIRFLOW_ADMIN_PASSWORD:-airflow}"
# Keep CI light: a few hundred real requests from the last two days.
max_records=${E2E_MAX_RECORDS:-300}
window_days=${E2E_REFRESH_WINDOW_DAYS:-2}

api() {
  curl --fail --silent --show-error -u "$airflow_auth" -H "Content-Type: application/json" "$@"
}

json_field() {
  python3 -c "import json, sys; print(json.load(sys.stdin)$1)"
}

# Read cqlsh's whole output before parsing it. Piping straight into `awk ... exit` let
# awk close the pipe while cqlsh was still printing warnings; under pipefail that
# failed the pipeline and `set -e` killed the script. A failed query prints nothing,
# which the caller treats as "not ready yet".
cql_count() {
  local out
  out=$("${compose[@]}" exec -T cassandra cqlsh -e "SELECT COUNT(*) FROM nyc311_streams.$1;" 2>/dev/null) \
    || return 0
  awk '/^[[:space:]]*[0-9]+[[:space:]]*$/ {print $1; exit}' <<<"$out"
}

wait_for() {
  local description=$1 attempts=$2
  shift 2
  for attempt in $(seq 1 "$attempts"); do
    if "$@" >/dev/null 2>&1; then
      return 0
    fi
    echo "waiting for ${description} (${attempt}/${attempts})"
    sleep 5
  done
  echo "${description} did not become ready" >&2
  return 1
}

wait_for "Cassandra" 60 "${compose[@]}" exec -T cassandra cqlsh -e "SELECT now() FROM system.local;"
wait_for "Airflow webserver" 60 curl --fail --silent "$airflow_url/health"
wait_for "dashboard" 36 curl --fail --silent "${DASHBOARD_URL:-http://localhost:8501}/_stcore/health"

# The webserver can be healthy before the scheduler has parsed and registered the DAG.
if ! wait_for "DAG ${dag_id} to be registered" 36 api "$airflow_url/api/v1/dags/$dag_id"; then
  echo "DAG import errors:" >&2
  api "$airflow_url/api/v1/importErrors" >&2 || true
  exit 1
fi

run_id="e2e_$(date +%Y%m%dT%H%M%S)"
echo "unpausing ${dag_id} and triggering run ${run_id} (up to ${max_records} records)"
api -X PATCH "$airflow_url/api/v1/dags/$dag_id?update_mask=is_paused" -d '{"is_paused": false}' >/dev/null
api -X POST "$airflow_url/api/v1/dags/$dag_id/dagRuns" \
  -d "{\"dag_run_id\": \"${run_id}\", \"conf\": {\"max_records\": ${max_records}, \"refresh_window_days\": ${window_days}}}" \
  >/dev/null

state=queued
for attempt in $(seq 1 90); do
  state=$(api "$airflow_url/api/v1/dags/$dag_id/dagRuns/$run_id" | json_field '["state"]') \
    || state=unknown  # a transient API error is retried, not fatal
  [[ "$state" == "success" || "$state" == "failed" ]] && break
  echo "DAG run ${run_id} is ${state} (${attempt}/90)"
  sleep 5
done

if [[ "$state" != "success" ]]; then
  echo "DAG run ${run_id} ended in state '${state}'" >&2
  api "$airflow_url/api/v1/dags/$dag_id/dagRuns/$run_id/taskInstances" \
    | python3 -c "import json, sys; [print(f\"  {t['task_id']}: {t['state']}\") for t in json.load(sys.stdin)['task_instances']]" >&2 || true
  "${compose[@]}" logs --no-color --tail=100 airflow-scheduler >&2 || true
  exit 1
fi
echo "DAG run ${run_id} succeeded"

for attempt in $(seq 1 60); do
  count=$(cql_count service_requests)
  if [[ "${count:-0}" =~ ^[1-9][0-9]*$ ]]; then
    echo "end-to-end check passed: ${count} NYC 311 requests in Cassandra," \
      "$(cql_count rejected_records) rejected records"
    "${compose[@]}" exec -T cassandra cqlsh -e \
      "SELECT unique_key, complaint_type, agency, borough, status, kafka_partition, kafka_offset
       FROM nyc311_streams.service_requests LIMIT 5;"
    exit 0
  fi
  echo "waiting for NYC 311 rows in Cassandra (${attempt}/60)"
  sleep 5
done

echo "no NYC 311 rows arrived in Cassandra" >&2
"${compose[@]}" logs --no-color --tail=150 spark-streaming >&2 || true
exit 1
