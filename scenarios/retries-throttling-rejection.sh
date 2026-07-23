#!/usr/bin/env bash
source "$(dirname "$0")/lib.sh"

build
reset_state
start_support
start_worker RECONCILER=0 RECV_TIMEOUT=2s STATUS_POLLS=5

log "RT1: two 429 responses are retried, then the single operation succeeds"
retry_started=$(date +%s)
seed RT1 2025 rate_limit
produce RT1 rate_limit
wait_filing_state RT1 accepted 60
retry_elapsed=$(( $(date +%s) - retry_started ))
if [ "${DBOS_LAB_MODE:-fast}" = full ]; then
  assert_ge "$retry_elapsed" 2 "full check preserved observable retry backoff"
fi
assert_le "$retry_elapsed" 30 "retry backoff remained within the recovery bound"
assert_eq "$(authority_metric .submission_attempts)" 3 "429 path used exactly two retries"
assert_eq "$(authority_metric .throttled_429)" 2 "provider emitted exactly two 429s"
assert_eq "$(authority_metric .submissions_recorded)" 1 "retries produced one provider effect"

log "RT2: two no-effect 503 responses are retried, then succeed"
seed RT2 2025 transient_5xx
produce RT2 transient_5xx
wait_filing_state RT2 accepted 60
assert_eq "$(authority_metric .submission_attempts)" 6 "503 path used exactly two retries"
assert_eq "$(authority_metric .transient_5xx)" 2 "provider emitted exactly two transient 503s"
assert_eq "$(authority_metric .submissions_recorded)" 2 "503 retries produced one additional provider effect"

log "RT3: permanent throttling exhausts six total attempts and becomes needs_review"
seed RT3 2025 rate_limit_exhausted
produce RT3 rate_limit_exhausted
wait_filing_state RT3 needs_review 90
assert_eq "$(authority_metric .submission_attempts)" 12 "retry budget is one initial attempt plus five retries"
assert_eq "$(authority_metric .throttled_429)" 8 "all exhausted attempts were observable 429s"
assert_eq "$(authority_metric .submissions_recorded)" 2 "exhausted retries created no provider effect"

log "RT4: permanent business rejection is terminal and is never retried"
seed RT4 2025 reject
produce RT4 reject
wait_filing_state RT4 rejected 60
assert_eq "$(authority_metric .submission_attempts)" 13 "rejection added exactly one attempt"
assert_eq "$(authority_metric .submissions_recorded)" 3 "rejection was recorded exactly once"
assert_eq "$(authority_metric .duplicate_submission_attempts)" 0 "no unsafe provider retry was hidden by deduplication"

log "retries, throttling and rejection complete"
