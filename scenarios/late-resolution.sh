#!/usr/bin/env bash
source "$(dirname "$0")/lib.sh"

build
reset_state
start_support
start_worker RECONCILER=0 RECV_TIMEOUT="$LAB_REVIEW_RECV_TIMEOUT" STATUS_POLLS=2

log "provider remains processing beyond the workflow's bounded wait"
seed LR1 2025 delayed
produce LR1 delayed
wait_filing_state LR1 needs_review 30
wait_wf_status filing-LR1-y2025 SUCCESS 30
assert_eq "$(authority_metric .submissions_recorded)" 1 "one provider effect existed before review"

log "provider completes after Accountable records needs_review"
wait_authority_status filing-LR1-y2025 completed 40
commands_before=$(accountable_metric .commands_applied)
./bin/opsctl reconcile -apply
wait_filing_state LR1 accepted 10
assert_eq "$(authority_metric .submission_attempts)" 1 "late resolution issued no second POST"
assert_eq "$(authority_metric .submissions_recorded)" 1 "late resolution preserved one provider effect"
assert_eq "$(accountable_metric .commands_applied)" "$((commands_before + 1))" "final outcome was applied once"

./bin/opsctl reconcile -apply
assert_eq "$(accountable_metric .commands_applied)" "$((commands_before + 1))" "repeat reconciliation was idempotent"
pass "needs_review resolved to accepted exactly once"

log "scheduled reconciliation resolves a later completed provider operation"
seed LR2 2025 delayed
produce LR2 delayed
wait_filing_state LR2 needs_review 30
wait_authority_status filing-LR2-y2025 completed 40
commands_before=$(accountable_metric .commands_applied)
kill_worker
start_worker RECONCILER=1 RECONCILE_CRON="$LAB_RECONCILE_CRON"
wait_filing_state LR2 accepted "$LAB_RECONCILE_RESOLUTION_TIMEOUT"
assert_eq "$(authority_metric .submission_attempts)" 2 "scheduled resolution issued no second POST"
assert_eq "$(authority_metric .submissions_recorded)" 2 "scheduled resolution preserved one effect per filing"
assert_eq "$(accountable_metric .commands_applied)" "$((commands_before + 1))" "scheduler applied the final outcome once"
pass "scheduled reconciliation resolved needs_review"
