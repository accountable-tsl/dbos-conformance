#!/usr/bin/env bash
source "$(dirname "$0")/lib.sh"

BACKUP=/tmp/dbos-spike-backup.sql

build
reset_state
start_support
start_worker RECONCILER=0

log "R1 completes normally BEFORE the backup (must remain intact after restore)"
seed R1 2025 ok
produce R1 ok
wait_filing_state R1 accepted 60
wait_wf_status filing-R1-y2025 SUCCESS 60

log "taking DBOS system DB backup"
docker compose exec -T dbos-postgres pg_dump -U postgres dbos > "$BACKUP"
pass "backup taken ($(wc -l < "$BACKUP") lines)"

log "R3 starts after backup and reaches authoritative submitted state"
seed R3 2025 no_callback_done
produce R3 no_callback_done
wait_filing_state R3 submitted 60
kill_worker
wait_authority_status filing-R3-y2025 completed 30

log "R2 loses DBOS after provider acceptance but before submit-step checkpoint"
seed R2 2025 ok
start_worker RECONCILER=0 CHAOS_CRASH_AT=in-submit-after-accept
produce R2 ok
wait_worker_dead
assert_eq "$(filing_state R2)" submitting "authoritative submission intent survived before provider call"

log "simulating DBOS database loss + restore from stale backup"
docker compose exec -T dbos-postgres psql -U postgres -c "DROP DATABASE dbos WITH (FORCE)"
docker compose exec -T dbos-postgres psql -U postgres -c "CREATE DATABASE dbos"
docker compose exec -T dbos-postgres psql -U postgres dbos < "$BACKUP" >/dev/null
pass "DBOS DB restored — R2 and R3 runtime workflows no longer exist"

start_worker RECONCILER=0

log "reconcile: detect missing runtime work from authoritative state"
drift=$(./bin/opsctl reconcile)
grep "MISSING filing-R2-y2025" <<<"$drift" >/dev/null || fail "reconcile did not flag R2"
grep "MISSING filing-R3-y2025" <<<"$drift" >/dev/null || fail "reconcile did not flag R3"
pass "both pre-checkpoint and post-submit missing workflows were detected"

./bin/opsctl reconcile -apply
wait_filing_state R2 accepted 120
wait_filing_state R3 accepted 120
wait_wf_status filing-R2-y2025 SUCCESS 120
wait_wf_status filing-R3-y2025 SUCCESS 120
pass "R2 and R3 reconstructed via canonical provider-operation adoption"

assert_eq "$(authority_metric .submissions_recorded)" 3 "stale restore did not duplicate either provider effect"
assert_eq "$(authority_metric .duplicate_submission_attempts)" 0 "reconstruction checked provider state before POST"
assert_eq "$(filing_state R1)" accepted "pre-backup work intact"
assert_eq "$(wf_status filing-R1-y2025)" SUCCESS "pre-backup DBOS workflow history restored"
./bin/opsctl reconcile | grep "nothing to reconcile" >/dev/null \
  && pass "reconcile converges to a clean state" \
  || fail "reconcile still finds drift"

log "complete DBOS loss: reconstruct work already in flight, not merely new ready work"
seed R4 2025 no_callback_done
produce R4 no_callback_done
wait_filing_state R4 submitted 60
wait_authority_status filing-R4-y2025 completed 30
kill_worker
docker compose exec -T dbos-postgres psql -U postgres -c "DROP DATABASE dbos WITH (FORCE)"
docker compose exec -T dbos-postgres psql -U postgres -c "CREATE DATABASE dbos"
start_worker RECONCILER=0
./bin/opsctl reconcile | grep "MISSING filing-R4-y2025" >/dev/null \
  || fail "empty DBOS database did not expose in-flight R4 as missing"
./bin/opsctl reconcile -apply
wait_filing_state R4 accepted 120
wait_wf_status filing-R4-y2025 SUCCESS 120
assert_eq "$(authority_metric .submissions_recorded)" 4 "empty-database reconstruction adopted R4 exactly once"
count=$(./bin/opsctl list -limit 500 | grep -c '^filing-R4-y2025 ' || true)
assert_eq "$count" 1 "empty database reconstruction created one logical workflow"

log "DBOS database loss complete"
