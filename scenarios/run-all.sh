#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

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
)

for s in "${scenarios[@]}"; do
  echo
  echo "════════════════════════════════════════════════════════════"
  echo "  RUNNING $s"
  echo "════════════════════════════════════════════════════════════"
  started_at=$(date +%s)
  base="${s%.sh}"
  WORKER_LOG="/tmp/dbos-spike-${base}-worker.log" \
    AUTHORITY_LOG="/tmp/dbos-spike-${base}-authority.log" \
    ACCOUNTABLE_LOG="/tmp/dbos-spike-${base}-accountable.log" \
    ./"$s"
  elapsed=$(( $(date +%s) - started_at ))
  echo "COMPLETED scenario=${base} seconds=${elapsed}"
done
echo
echo "ALL SCENARIOS PASSED mode=$DBOS_LAB_MODE"
