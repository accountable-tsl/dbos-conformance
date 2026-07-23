#!/usr/bin/env bash
source "$(dirname "$0")/lib.sh"

build
reset_state
start_support
start_worker RECONCILER=0

log "CN3: provider completes and calls back after the workflow was cancelled"
seed CN3 2025 delayed
produce CN3 delayed
wait_filing_state CN3 submitted 60
./bin/opsctl cancel filing-CN3-y2025
wait_authority_status filing-CN3-y2025 completed 60
assert_eq "$(wf_status filing-CN3-y2025)" CANCELLED "late provider completion did not undo cancellation"
assert_eq "$(filing_state CN3)" submitted "late completion was not falsely recorded while workflow was cancelled"
./bin/opsctl resume filing-CN3-y2025
wait_filing_state CN3 accepted 60
pass "explicit resume consumed/reconciled the late provider outcome"

log "CN4: cancel while the provider request is executing after its durable effect"
seed CN4 2025 timeout_ambiguous
produce CN4 timeout_ambiguous
wait_authority_status filing-CN4-y2025 processing 30
./bin/opsctl cancel filing-CN4-y2025
wait_wf_status filing-CN4-y2025 CANCELLED 30
assert_eq "$(filing_state CN4)" submitting "request-race cancellation did not invent an outcome"
wait_authority_status filing-CN4-y2025 completed 30
assert_eq "$(filing_state CN4)" submitting "provider completion remained external while cancelled"
./bin/opsctl resume filing-CN4-y2025
wait_filing_state CN4 accepted 60
assert_eq "$(authority_metric .submissions_recorded)" 2 "request cancellation caused no duplicate provider effect"
assert_eq "$(authority_metric .submission_attempts)" 2 "cancellation paths issued exactly one POST per external operation"
assert_eq "$(authority_metric .duplicate_submission_attempts)" 0 "no second POST was hidden by provider deduplication"
pass "resume adopted the provider effect created during the cancelled request"
log "cancellation complete"
