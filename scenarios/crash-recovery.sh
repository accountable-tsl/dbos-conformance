#!/usr/bin/env bash
source "$(dirname "$0")/lib.sh"

CASES=(
	"after-event-start-before-offset-commit ok"
	"before-offset-commit ok"
	"after-offset-commit ok"
  "wf-start ok"
  "after-validate ok"
	"before-patch-evaluation ok v2"
	"after-patch-evaluation ok v2"
	"in-risk-precheck ok v2"
	"after-risk-precheck ok v2"
  "before-prepare ok"
  "in-prepare-after-apply ok"
  "after-prepare ok"
  "before-find-existing ok"
  "after-find-existing ok"
  "before-submit ok"
  "in-submit-after-accept ok"
  "after-submit ok"
  "before-resolve-ambiguous timeout_ambiguous"
  "after-resolve-ambiguous timeout_ambiguous"
  "before-resubmit timeout_lost"
	"in-resubmit-after-accept timeout_lost"
  "after-resubmit timeout_lost"
  "before-mark-submitted ok"
  "in-mark-submitted-after-apply ok"
  "after-mark-submitted ok"
  "before-wait ok"
  "after-callback ok"
  "before-status-poll no_callback_done"
  "after-status-poll no_callback_done"
  "before-record ok"
  "in-record-after-apply ok"
  "after-record ok"
  "before-final-reconcile ok"
  "after-final-reconcile ok"
)

build
reset_state
start_support
export RECONCILER=0
export RECV_TIMEOUT=1s
export STATUS_POLLS=6

i=0
for case_spec in "${CASES[@]}"; do
	read -r point scenario variant <<<"$case_spec"
  i=$((i+1))
  filing="CR$i"
  log "crash point: $point (filing $filing, scenario $scenario)"
  seed "$filing" 2025 "$scenario"
  if [ "${variant:-}" = v2 ]; then
    WORKER_BINARY=./bin/worker-v2 start_worker RECONCILER=0 CHAOS_CRASH_AT="$point"
  else
    start_worker RECONCILER=0 CHAOS_CRASH_AT="$point"
  fi
  produce "$filing" "$scenario"
  wait_worker_dead
  log "worker died at $point — restarting clean"
  if [ "${variant:-}" = v2 ]; then
    WORKER_BINARY=./bin/worker-v2 start_worker RECONCILER=0
  else
    start_worker RECONCILER=0
  fi
  wait_filing_state "$filing" accepted 90
  wait_wf_status "filing-${filing}-y2025" SUCCESS 90
  pass "$filing recovered to accepted after crash at $point"
  kill_worker
done

log "verifying single business effect per filing"
assert_eq "$(authority_metric .submissions_recorded)" "${#CASES[@]}" "one authority submission per filing"
# The in-submit crash proves at-least-once execution occurred, so a zero count
# would mean provider deduplication was never exercised.
assert_ge "$(authority_metric .duplicate_submission_attempts)" 1 "at-least-once replay observed and deduplicated"
assert_ge "$(accountable_metric .duplicate_command_attempts)" 3 "domain-command response-loss replays observed and deduplicated"
log "crash recovery complete"
