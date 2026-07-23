#!/usr/bin/env bash
source "$(dirname "$0")/lib.sh"

build
reset_state
start_support

log "early callback is durable before workflow execution"
seed CB1 2025 ok
out=$(./bin/opsctl reconcile -apply 2>&1) || fail "reconcile -apply errored: $out"
echo "$out"
grep -q "RECREATED filing-CB1-y2025" <<<"$out" || fail "could not enqueue CB1"
./bin/opsctl send filing-CB1-y2025 -outcome accepted -key early-manual-cb
start_worker RECONCILER=0 RECV_TIMEOUT="$LAB_CALLBACK_RECV_TIMEOUT" STATUS_POLLS=5
wait_filing_state CB1 accepted 90
wait_wf_status filing-CB1-y2025 SUCCESS 90
./bin/opsctl inspect filing-CB1-y2025 | grep "early-manual-cb" >/dev/null \
  && pass "pre-delivered message consumed by Recv (sent before workflow started)" \
  || fail "workflow did not consume the pre-delivered message"
wait_authority_metric .callbacks_sent 1 30

log "duplicate callbacks collapse into one workflow outcome"
seed CB2 2025 dup_callback
produce CB2 dup_callback
wait_filing_state CB2 accepted 60
wait_wf_status filing-CB2-y2025 SUCCESS 60
wait_authority_metric .callbacks_sent 4 30
assert_eq "$(filing_state CB2)" accepted "duplicate callbacks preserved one outcome"

log "late callback cannot rewrite terminal truth"
seed CB3 2025 late_callback
produce CB3 late_callback
wait_filing_state CB3 accepted 60
wait_wf_status filing-CB3-y2025 SUCCESS 60
commands_at_terminal=$(accountable_metric .commands_applied)
wait_authority_metric .callbacks_sent 5 40
assert_eq "$(filing_state CB3)" accepted "late callback did not reverse the terminal outcome"
assert_eq "$(accountable_metric .commands_applied)" "$commands_at_terminal" "late callback created no second domain effect"

assert_eq "$(authority_metric .submissions_recorded)" 3 "one submission per callback case"
log "callbacks complete"
