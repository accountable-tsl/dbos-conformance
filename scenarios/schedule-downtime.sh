#!/usr/bin/env bash
source "$(dirname "$0")/lib.sh"

build
reset_state
start_support

log "SC1 is due in Accountable but its Redpanda event was lost"
seed SC1 2025 ok

log "starting worker with recurring reconciliation"
start_worker RECONCILER=1 RECONCILE_CRON="$LAB_RECONCILE_CRON"

log "SC1 must be picked up by the scheduler, not an event"
wait_filing_state SC1 accepted 120
wait_wf_status filing-SC1-y2025 SUCCESS 120
pass "missed work reconciled from authoritative due records"

log "downtime: SC2 becomes due while every scheduler is stopped"
kill_worker
seed SC2 2025 ok
sleep "$LAB_SCHEDULE_DOWNTIME_SECONDS"
count=$(./bin/opsctl list -limit 500 | grep -c '^filing-SC2-y2025 ' || true)
assert_eq "$count" 0 "no runtime workflow existed while the application was down"

log "restart: the first later occurrence must recover work missed during downtime"
start_worker RECONCILER=1 RECONCILE_CRON="$LAB_RECONCILE_CRON"
wait_filing_state SC2 accepted 120
wait_wf_status filing-SC2-y2025 SUCCESS 120
pass "post-downtime schedule reconciled the authoritative due record"

log "duplicate firing check: many reconciler runs, still one workflow per filing"
sleep "$LAB_SCHEDULE_REPEAT_SECONDS"
count=$(./bin/opsctl list -limit 500 | grep -c "^filing-SC1-y2025 " || true)
assert_eq "$count" 1 "exactly one workflow for SC1 despite repeated schedule runs"
assert_eq "$(authority_metric .submissions_recorded)" 2 "no duplicate submissions from scheduler"

reconciler_runs=$(./bin/opsctl list -limit 500 | grep -cvE '^filing-|^\(' || true)
assert_ge "$reconciler_runs" 2 "multiple schedule occurrences executed"

log "schedule downtime complete"
