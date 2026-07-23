#!/usr/bin/env bash
source "$(dirname "$0")/lib.sh"

build
reset_state
start_support
start_worker RECONCILER=0

log "failed validation becomes recoverable after Accountable receives the filing"
./bin/opsctl start RH1 -scenario ok
wait_wf_status filing-RH1-y2025 ERROR 30
seed RH1 2025 ok

report=$(./bin/opsctl reconcile)
grep -q "RECREATE filing-RH1-y2025" <<<"$report" \
  || fail "ERROR workflow was not classified for recreation: $report"
./bin/opsctl reconcile -apply
wait_filing_state RH1 accepted 30
wait_wf_status filing-RH1-y2025 SUCCESS 30
assert_eq "$(authority_metric .submissions_recorded)" 1 "recreated workflow created one provider effect"
pass "ERROR workflow recreated after its external cause was repaired"

log "cancelled workflow requires an explicit human decision"
seed RH2 2025 delayed
produce RH2 delayed
wait_filing_state RH2 submitted 30
./bin/opsctl cancel filing-RH2-y2025
wait_wf_status filing-RH2-y2025 CANCELLED 30
attempts_before=$(authority_metric .submission_attempts)

report=$(./bin/opsctl reconcile)
grep -q "ESCALATE filing-RH2-y2025.*dbos_status=CANCELLED" <<<"$report" \
  || fail "CANCELLED workflow was not escalated: $report"
./bin/opsctl reconcile -apply
assert_eq "$(wf_status filing-RH2-y2025)" CANCELLED "reconciliation did not resume deliberate cancellation"
assert_eq "$(filing_state RH2)" submitted "reconciliation did not invent a cancelled outcome"
assert_eq "$(authority_metric .submission_attempts)" "$attempts_before" "cancel escalation issued no provider POST"
pass "CANCELLED workflow remained an explicit escalation"

log "active workflow without a live worker becomes a stale escalation"
seed RH3 2025 delayed
produce RH3 delayed
wait_filing_state RH3 submitted 30
kill_worker
wait_wf_status filing-RH3-y2025 PENDING 30
sleep 2
attempts_before=$(authority_metric .submission_attempts)

report=$(./bin/opsctl reconcile -stale-seconds 1)
grep -q "ESCALATE filing-RH3-y2025.*dbos_status=PENDING.*reason=stale_active" <<<"$report" \
  || fail "stale PENDING workflow was not escalated: $report"
./bin/opsctl reconcile -apply -stale-seconds 1
assert_eq "$(wf_status filing-RH3-y2025)" PENDING "stale escalation did not mutate active workflow"
assert_eq "$(filing_state RH3)" submitted "stale escalation did not invent an outcome"
assert_eq "$(authority_metric .submission_attempts)" "$attempts_before" "stale escalation issued no provider POST"
pass "stale active workflow remained an explicit escalation"

log "successful DBOS history cannot hide unresolved Accountable state"
start_worker RECONCILER=0 RECV_TIMEOUT="$LAB_REVIEW_RECV_TIMEOUT" STATUS_POLLS=2
seed RH4 2025 delayed
produce RH4 delayed
wait_filing_state RH4 needs_review 30
wait_wf_status filing-RH4-y2025 SUCCESS 30
attempts_before=$(authority_metric .submission_attempts)

report=$(./bin/opsctl reconcile)
grep -q "ESCALATE filing-RH4-y2025.*dbos_status=SUCCESS.*provider_status=processing" <<<"$report" \
  || fail "inconsistent SUCCESS workflow was not escalated: $report"
./bin/opsctl reconcile -apply
assert_eq "$(wf_status filing-RH4-y2025)" SUCCESS "reconciliation preserved completed DBOS history"
assert_eq "$(filing_state RH4)" needs_review "processing provider did not invent an outcome"
assert_eq "$(authority_metric .submission_attempts)" "$attempts_before" "success escalation issued no provider POST"
pass "inconsistent SUCCESS workflow remained an explicit escalation"

log "scheduled reconciliation applies the same terminal-state policy"
./bin/opsctl start RH5 -scenario ok
wait_wf_status filing-RH5-y2025 ERROR 30
seed RH5 2025 ok
kill_worker
start_worker RECONCILER=1 RECONCILE_CRON="$LAB_RECONCILE_CRON"
wait_filing_state RH5 accepted "$LAB_RECONCILE_RESOLUTION_TIMEOUT"
wait_wf_status filing-RH5-y2025 SUCCESS 30
assert_eq "$(wf_status filing-RH2-y2025)" CANCELLED "scheduler preserved deliberate cancellation"
pass "scheduler recreated ERROR and escalated CANCELLED consistently"
