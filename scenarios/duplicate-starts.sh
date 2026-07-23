#!/usr/bin/env bash
source "$(dirname "$0")/lib.sh"

build
reset_state
start_support

log "D0: 20 concurrent direct starts race on the same deterministic workflow ID"
# Keeping execution stopped makes every caller race against the same queued row.
start_worker RECONCILER=0
kill_worker
seed D0 2025 ok
pids=()
for i in $(seq 1 20); do
  ./bin/opsctl start D0 -scenario ok >"/tmp/dbos-start-$i.log" 2>&1 &
  pids+=("$!")
done
successful_starts=0
for pid in "${pids[@]}"; do
  if wait "$pid"; then successful_starts=$((successful_starts+1)); fi
done
assert_ge "$successful_starts" 1 "at least one concurrent start created the operation"
count=$(./bin/opsctl list -limit 500 | grep -c '^filing-D0-y2025 ' || true)
assert_eq "$count" 1 "concurrent starts created exactly one DBOS workflow"
if ./bin/opsctl start D0 -scenario reject >/tmp/dbos-start-conflict.log 2>&1; then
  fail "direct start with conflicting input unexpectedly succeeded"
fi
assert_eq "$(accountable_metric .workflow_start_conflicts)" 1 "conflicting direct start was recorded"
start_worker RECONCILER=0
wait_filing_state D0 accepted 60

log "D1: same event delivered 3 times"
seed D1 2025 ok
produce D1 ok -dup 3
wait_filing_state D1 accepted 60

log "DY: the same filing identifier in two tax years creates two distinct logical operations"
seed DY 2024 ok
seed DY 2025 ok
produce DY ok -year 2024
produce DY ok -year 2025
wait_filing_year_state DY 2024 accepted 60
wait_filing_year_state DY 2025 accepted 60

log "D1 again: replay the event after completion — must not refile"
produce D1 ok

log "D1 conflict: the same filing/year with different input is durable evidence, not a silent duplicate"
produce D1 reject
for _ in $(seq 1 30); do
  [ "$(accountable_metric .workflow_start_conflicts 2>/dev/null || true)" = 2 ] && break
  sleep 0.5
done
assert_eq "$(accountable_metric .workflow_start_conflicts)" 2 "conflicting event start was recorded"
assert_eq "$(filing_state D1)" accepted "conflicting input did not alter the completed operation"

assert_eq "$(authority_metric .submissions_recorded)" 4 "exactly one authority submission per filing/year"
assert_eq "$(authority_metric .duplicate_submission_attempts)" 0 "no duplicate submission attempts"
assert_eq "$(filing_state D1)" accepted "D1 still accepted after replayed event"
log "duplicate starts complete"
