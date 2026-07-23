#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

export DBOS_DATABASE_URL="${DBOS_DATABASE_URL:-postgres://postgres:dbos@localhost:5344/dbos?sslmode=disable}"
export ACCOUNTABLE_DB_URL="${ACCOUNTABLE_DB_URL:-postgres://postgres:accountable@localhost:5434/accountable?sslmode=disable}"
export AUTHORITY_DB_URL="${AUTHORITY_DB_URL:-postgres://postgres:authority@localhost:5444/authority?sslmode=disable}"
export ACCOUNTABLE_URL="${ACCOUNTABLE_URL:-http://localhost:8081}"
export AUTHORITY_URL="${AUTHORITY_URL:-http://localhost:8082}"
export KAFKA_BROKERS="${KAFKA_BROKERS:-localhost:19092}"

case "${DBOS_LAB_MODE:-fast}" in
  fast)
    export AUTHORITY_COMPLETION_DELAY="${AUTHORITY_COMPLETION_DELAY:-200ms}"
    export AUTHORITY_AMBIGUOUS_COMPLETION_DELAY="${AUTHORITY_AMBIGUOUS_COMPLETION_DELAY:-2s}"
    export AUTHORITY_DELAYED_COMPLETION_DELAY="${AUTHORITY_DELAYED_COMPLETION_DELAY:-2s}"
    export AUTHORITY_LATE_CALLBACK_DELAY="${AUTHORITY_LATE_CALLBACK_DELAY:-1s}"
    export AUTHORITY_LOST_RESPONSE_DELAY="${AUTHORITY_LOST_RESPONSE_DELAY:-1s}"
    export WORKFLOW_HTTP_TIMEOUT="${WORKFLOW_HTTP_TIMEOUT:-500ms}"
    export SUBMIT_RETRY_BASE_INTERVAL="${SUBMIT_RETRY_BASE_INTERVAL:-50ms}"
    export RECONCILE_STALE_SECONDS="${RECONCILE_STALE_SECONDS:-2}"
    export LAB_RECONCILE_CRON="${LAB_RECONCILE_CRON:-*/2 * * * * *}"
    export LAB_SCHEDULE_DOWNTIME_SECONDS="${LAB_SCHEDULE_DOWNTIME_SECONDS:-3}"
    export LAB_SCHEDULE_REPEAT_SECONDS="${LAB_SCHEDULE_REPEAT_SECONDS:-5}"
    export LAB_CALLBACK_RECV_TIMEOUT="${LAB_CALLBACK_RECV_TIMEOUT:-500ms}"
    ;;
  full)
    export AUTHORITY_COMPLETION_DELAY="${AUTHORITY_COMPLETION_DELAY:-2s}"
    export AUTHORITY_AMBIGUOUS_COMPLETION_DELAY="${AUTHORITY_AMBIGUOUS_COMPLETION_DELAY:-2s}"
    export AUTHORITY_DELAYED_COMPLETION_DELAY="${AUTHORITY_DELAYED_COMPLETION_DELAY:-25s}"
    export AUTHORITY_LATE_CALLBACK_DELAY="${AUTHORITY_LATE_CALLBACK_DELAY:-23s}"
    export AUTHORITY_LOST_RESPONSE_DELAY="${AUTHORITY_LOST_RESPONSE_DELAY:-8s}"
    export WORKFLOW_HTTP_TIMEOUT="${WORKFLOW_HTTP_TIMEOUT:-5s}"
    export SUBMIT_RETRY_BASE_INTERVAL="${SUBMIT_RETRY_BASE_INTERVAL:-500ms}"
    export RECONCILE_STALE_SECONDS="${RECONCILE_STALE_SECONDS:-30}"
    export LAB_RECONCILE_CRON="${LAB_RECONCILE_CRON:-*/15 * * * * *}"
    export LAB_SCHEDULE_DOWNTIME_SECONDS="${LAB_SCHEDULE_DOWNTIME_SECONDS:-35}"
    export LAB_SCHEDULE_REPEAT_SECONDS="${LAB_SCHEDULE_REPEAT_SECONDS:-20}"
    export LAB_CALLBACK_RECV_TIMEOUT="${LAB_CALLBACK_RECV_TIMEOUT:-3s}"
    ;;
  *)
    printf 'DBOS_LAB_MODE must be fast or full\n' >&2
    exit 1
    ;;
esac

WORKER_LOG="${WORKER_LOG:-/tmp/dbos-spike-worker.log}"
AUTHORITY_LOG="${AUTHORITY_LOG:-/tmp/dbos-spike-authority.log}"
ACCOUNTABLE_LOG="${ACCOUNTABLE_LOG:-/tmp/dbos-spike-accountable.log}"

log()  { printf '\n\033[1;34m[scenario]\033[0m %s\n' "$*"; }
pass() { printf '\033[1;32m[PASS]\033[0m %s\n' "$*"; }
fail() { printf '\033[1;31m[FAIL]\033[0m %s\n' "$*"; exit 1; }

build() {
  if [ "${DBOS_LAB_SKIP_BUILD:-0}" != 1 ]; then
    make -s build
  fi
}

start_support() {
  ./bin/accountable >"$ACCOUNTABLE_LOG" 2>&1 &
  ACCOUNTABLE_PID=$!
  start_authority
  wait_http "$ACCOUNTABLE_URL/healthz" "accountable"
}

start_authority() {
  ./bin/authority >>"$AUTHORITY_LOG" 2>&1 &
  AUTHORITY_PID=$!
  wait_http "$AUTHORITY_URL/healthz" "authority"
}

start_worker() {
  rm -rf .chaos
  local binary="${WORKER_BINARY:-./bin/worker}"
  env "$@" "$binary" >>"$WORKER_LOG" 2>&1 &
  WORKER_PID=$!
  wait_http "http://localhost:8080/healthz" "worker"
}

restart_worker_clean() {
  wait_worker_dead
  start_worker RECONCILER="${RECONCILER:-0}"
}

kill_worker() {
	pid="${WORKER_PID:-}"
	if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
		kill -9 "$pid" 2>/dev/null || true
		wait "$pid" 2>/dev/null || true
	fi
  WORKER_PID=""
  sleep 0.5
}

stop_all() {
  kill_worker
  for name in AUTHORITY_PID ACCOUNTABLE_PID; do
    pid="${!name:-}"
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
    fi
  done
  AUTHORITY_PID=""
  ACCOUNTABLE_PID=""
}

wait_http() {
  for _ in $(seq 1 60); do
    curl -sf "$1" >/dev/null 2>&1 && return 0
    sleep 0.5
  done
  fail "$2 did not become healthy at $1"
}

wait_worker_dead() {
  for _ in $(seq 1 240); do
	if [ -z "${WORKER_PID:-}" ] || ! kill -0 "$WORKER_PID" 2>/dev/null; then
	  wait "${WORKER_PID:-}" 2>/dev/null || true
	  return 0
	fi
    sleep 0.5
  done
  fail "worker did not die (expected chaos crash)"
}

seed() {
  curl -sf -XPUT "$ACCOUNTABLE_URL/filings/$1" \
    -d "{\"tax_year\":$2,\"scenario\":\"$3\"}" >/dev/null
}

produce() {
  local f="$1" s="$2"; shift 2
  ./bin/producer -filing "$f" -scenario "$s" "$@"
}

filing_state() {
  filing_year_state "$1" 2025
}

filing_year_state() {
  curl -sf "$ACCOUNTABLE_URL/filings/$1?tax_year=$2" | jq -r .state
}

wait_filing_state() {
  local timeout="${3:-90}"
  for _ in $(seq 1 $((timeout*2))); do
    [ "$(filing_state "$1" 2>/dev/null || true)" = "$2" ] && return 0
    sleep 0.5
  done
  fail "filing $1 never reached state $2 (currently: $(filing_state "$1" 2>/dev/null || echo '?'))"
}

wait_filing_year_state() {
  local timeout="${4:-90}"
  for _ in $(seq 1 $((timeout*2))); do
    [ "$(filing_year_state "$1" "$2" 2>/dev/null || true)" = "$3" ] && return 0
    sleep 0.5
  done
  fail "filing $1/$2 never reached state $3"
}

wf_status() {
  ./bin/opsctl list -limit 500 | awk -v id="$1" '$1==id {print $2}'
}

wait_wf_status() {
  local timeout="${3:-90}"
  for _ in $(seq 1 $((timeout*2))); do
    [ "$(wf_status "$1")" = "$2" ] && return 0
    sleep 0.5
  done
  fail "workflow $1 never reached $2 (currently: $(wf_status "$1"))"
}

accountable_metric() {
  curl -sf "$ACCOUNTABLE_URL/metrics" | jq -r "$1"
}

authority_metric() {
  curl -sf "$AUTHORITY_URL/metrics" | jq -r "$1"
}

wait_authority_metric() {
  local timeout="${3:-90}"
  for _ in $(seq 1 $((timeout*2))); do
    [ "$(authority_metric "$1" 2>/dev/null || true)" = "$2" ] && return 0
    sleep 0.5
  done
  fail "authority metric $1 never reached $2 (currently: $(authority_metric "$1" 2>/dev/null || echo '?'))"
}

wait_authority_status() {
  local timeout="${3:-90}"
  for _ in $(seq 1 $((timeout*2))); do
    status=$(curl -sf "$AUTHORITY_URL/submissions/$1" 2>/dev/null | jq -r .status || true)
    [ "$status" = "$2" ] && return 0
    sleep 0.5
  done
  fail "provider operation $1 never reached $2"
}

assert_eq() {
  [ "$1" = "$2" ] && pass "$3 (=$2)" || fail "$3: expected $2, got $1"
}

assert_ge() {
  [ "$1" -ge "$2" ] && pass "$3 ($1 >= $2)" || fail "$3: expected >= $2, got $1"
}

assert_le() {
  [ "$1" -le "$2" ] && pass "$3 ($1 <= $2)" || fail "$3: expected <= $2, got $1"
}

reset_state() {
  docker compose exec -T accountable-postgres psql -U postgres accountable \
	-v ON_ERROR_STOP=1 -c "DO \$\$ BEGIN
	  IF to_regclass('public.filings') IS NOT NULL THEN
	    TRUNCATE filings, domain_commands;
	  END IF;
	  IF to_regclass('public.workflow_start_conflicts') IS NOT NULL THEN
	    TRUNCATE workflow_start_conflicts;
	  END IF;
	END \$\$" >/dev/null
  authority_table=$(docker compose exec -T authority-postgres psql -U postgres authority \
	-v ON_ERROR_STOP=1 -Atc "SELECT to_regclass('public.authority_state')")
  if [ "$authority_table" = "authority_state" ]; then
    docker compose exec -T authority-postgres psql -U postgres authority \
	  -v ON_ERROR_STOP=1 -c "TRUNCATE authority_state" >/dev/null
  fi
  docker compose exec -T dbos-postgres psql -U postgres dbos \
	-v ON_ERROR_STOP=1 -c "DROP SCHEMA IF EXISTS dbos CASCADE" >/dev/null
  group_list=$(docker compose exec -T redpanda rpk group list)
  if awk '$1=="filing-worker" {found=1} END {exit !found}' <<<"$group_list"; then
    docker compose exec -T redpanda rpk group delete filing-worker >/dev/null
  fi
  topic_list=$(docker compose exec -T redpanda rpk topic list)
  if awk '$1=="filing-events" {found=1} END {exit !found}' <<<"$topic_list"; then
    docker compose exec -T redpanda rpk topic delete filing-events >/dev/null
  fi
  rm -rf .chaos
  : > "$WORKER_LOG"
}

trap 'stop_all' EXIT
