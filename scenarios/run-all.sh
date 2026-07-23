#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT/scenarios"

report_path="${DBOS_LAB_REPORT:-$ROOT/artifacts/dbos-lab-report.json}"
if [[ "$report_path" != /* ]]; then
  report_path="$ROOT/$report_path"
fi
results_file=$(mktemp)
suite_started_at=$(date +%s)
suite_started_iso=$(date -u +%Y-%m-%dT%H:%M:%SZ)

write_report() {
  local exit_code=$?
  local suite_status=passed
  [ "$exit_code" -eq 0 ] || suite_status=failed
  mkdir -p "$(dirname "$report_path")"
  jq -s \
    --arg commit_sha "$(git -C "$ROOT" rev-parse HEAD 2>/dev/null || printf unknown)" \
    --arg dbos_version "$(go list -m -f '{{.Version}}' github.com/dbos-inc/dbos-transact-golang 2>/dev/null || printf unknown)" \
    --arg timing_mode "${DBOS_LAB_MODE:-fast}" \
    --arg started_at "$suite_started_iso" \
    --arg status "$suite_status" \
    --argjson duration_seconds "$(( $(date +%s) - suite_started_at ))" \
    '{commit_sha: $commit_sha, dbos_version: $dbos_version, timing_mode: $timing_mode,
      started_at: $started_at, status: $status, duration_seconds: $duration_seconds,
      scenarios: .}' "$results_file" >"$report_path.tmp"
  mv "$report_path.tmp" "$report_path"
  rm -f "$results_file"
  printf 'Evidence report: %s\n' "$report_path"
}
trap write_report EXIT

if [ "${DBOS_LAB_SKIP_BUILD:-0}" != 1 ]; then
  make -s build
fi
export DBOS_LAB_SKIP_BUILD=1
export DBOS_LAB_MODE="${DBOS_LAB_MODE:-fast}"

scenarios=(
  duplicate-starts.sh
  crash-recovery.sh
  lost-provider-response.sh
  callbacks.sh
  retries-throttling-rejection.sh
  cancellation.sh
  schedule-downtime.sh
  code-upgrade.sh
  dbos-database-loss.sh
  late-resolution.sh
  reconciliation-health.sh
)

for s in "${scenarios[@]}"; do
  echo
  echo "════════════════════════════════════════════════════════════"
  echo "  RUNNING $s"
  echo "════════════════════════════════════════════════════════════"
  started_at=$(date +%s)
  base="${s%.sh}"
  scenario_status=passed
  scenario_exit=0
  if ! WORKER_LOG="/tmp/dbos-spike-${base}-worker.log" \
      AUTHORITY_LOG="/tmp/dbos-spike-${base}-authority.log" \
      ACCOUNTABLE_LOG="/tmp/dbos-spike-${base}-accountable.log" \
      ./"$s"; then
    scenario_status=failed
    scenario_exit=1
  fi
  elapsed=$(( $(date +%s) - started_at ))
  jq -cn --arg name "$base" --arg status "$scenario_status" \
    --argjson duration_seconds "$elapsed" \
    '{name: $name, status: $status, duration_seconds: $duration_seconds}' >>"$results_file"
  echo "COMPLETED scenario=${base} status=${scenario_status} seconds=${elapsed}"
  [ "$scenario_exit" -eq 0 ] || exit "$scenario_exit"
done
echo
echo "ALL SCENARIOS PASSED mode=$DBOS_LAB_MODE"
