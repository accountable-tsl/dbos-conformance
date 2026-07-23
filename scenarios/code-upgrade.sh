#!/usr/bin/env bash
source "$(dirname "$0")/lib.sh"

build
if cmp -s ./bin/worker-v1 ./bin/worker-v2; then
  fail "upgrade artifacts are byte-identical"
fi
pass "separate v1 and v2 worker artifacts were built"
reset_state
start_support

log "v1 workflow crashes in flight"
seed UP1 2025 ok
WORKER_BINARY=./bin/worker-v1 start_worker RECONCILER=0 APP_VERSION=patchline CHAOS_CRASH_AT=after-submit
produce UP1 ok
wait_worker_dead

log "v2 recovers the v1 history under the same application version"
WORKER_BINARY=./bin/worker-v2 start_worker RECONCILER=0 APP_VERSION=patchline
wait_filing_state UP1 accepted 90
wait_wf_status filing-UP1-y2025 SUCCESS 90
if ./bin/opsctl inspect filing-UP1-y2025 | grep -q riskPrecheck; then
  fail "pre-patch workflow executed the new riskPrecheck step"
else
  pass "pre-patch workflow recovered through its original step history"
fi

log "new workflow takes the v2 patch"
seed UP2 2025 ok
produce UP2 ok
wait_filing_state UP2 accepted 90
wait_wf_status filing-UP2-y2025 SUCCESS 90
grep -q riskPrecheck <(./bin/opsctl inspect filing-UP2-y2025) \
  && pass "new workflow executed the patch path" \
  || fail "new workflow did not execute riskPrecheck"
assert_eq "$(authority_metric .submissions_recorded)" 2 "patch deployment preserved one provider effect per filing"
log "code upgrade complete"
