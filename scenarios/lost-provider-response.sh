#!/usr/bin/env bash
source "$(dirname "$0")/lib.sh"

build
reset_state
start_support
start_worker RECONCILER=0

log "AM1: timeout_ambiguous — authority records the filing, response is lost"
seed AM1 2025 timeout_ambiguous
produce AM1 timeout_ambiguous
wait_filing_state AM1 accepted 90
assert_eq "$(authority_metric .submissions_recorded)" 1 "one submission despite lost response"
assert_eq "$(authority_metric .submission_attempts)" 1 "lost response did not trigger another POST"
assert_eq "$(authority_metric .duplicate_submission_attempts)" 0 "resolved by status check, not resubmission"
log "lost provider response complete"
