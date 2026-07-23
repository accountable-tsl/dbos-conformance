# What this lab proves

Passing the suite demonstrates the behaviour below for the pinned DBOS Go
version and this local Docker topology. It does not prove arbitrary providers,
deployment platforms or future DBOS versions.

## External-effect contract

Every filing/year uses the stable operation ID
`filing-<filing-id>-y<tax-year>` for both DBOS and the provider.

Before submission, Accountable durably records that ID. The provider must then
offer:

1. durable idempotency by operation ID for the entire recovery window; and
2. status lookup by operation ID.

If the real provider lacks either property, the product needs a durable gateway
that supplies it. DBOS steps are at-least-once, so DBOS alone cannot guarantee a
single external effect.

## Executable evidence

| Question | Scenario | Passing evidence |
|---|---|---|
| Duplicate starts | `duplicate-starts.sh` | Concurrent direct starts, redelivered events and a completed-event replay leave one workflow and one provider effect per filing/year; conflicting input is recorded instead of replacing the original. |
| Crash recovery | `crash-recovery.sh` | The worker is killed at every workflow and event durability boundary. Every filing reaches `accepted`, while provider and Accountable metrics show replays were deduplicated. |
| Lost provider response | `lost-provider-response.sh` | The provider persists the operation but its response exceeds the client timeout. Recovery uses status lookup and records exactly one provider attempt/effect. |
| Callback timing | `callbacks.sh` | A message sent before execution is consumed, duplicate callback IDs produce one outcome, and a callback arriving after terminal status creates no new domain effect. |
| Retry taxonomy | `retries-throttling-rejection.sh` | 429 and declared-no-effect 503 responses retry with exact attempt counts; exhausted throttling creates no provider effect; permanent rejection is attempted once. |
| Cancellation | `cancellation.sh` | Cancellation after provider acceptance leaves Accountable non-terminal. Explicit resume adopts the external result without another provider POST. |
| Schedule downtime | `schedule-downtime.sh` | Work that becomes due while the application is stopped is recovered by a later scheduled reconciliation; repeated occurrences still create one workflow/effect. |
| Code upgrade | `code-upgrade.sh` | A separately built v2 worker resumes v1 history without inserting its new patched step; newly started work takes the patch path. |
| DBOS database loss | `dbos-database-loss.sh` | A stale restore and an empty DBOS database are reconciled from Accountable. Existing provider effects are adopted, histories present in the backup survive, and a second reconciliation is clean. |
| Late review resolution | `late-resolution.sh` | A workflow first records `needs_review`; later provider completion moves Accountable to `accepted` exactly once through both operator and scheduled reconciliation without another provider POST. |
| Reconciliation health | `reconciliation-health.sh` | Operator and scheduled reconciliation recreate repairable `ERROR` histories, preserve and escalate `CANCELLED` or stale active histories, and inspect provider state before trusting `SUCCESS`. |

## Timing modes

Both modes run the table above and write a JSON report with the commit SHA,
DBOS version, timing mode, suite result, and each scenario's result and duration.

- `fast` compresses provider delays, HTTP timeouts, retry intervals, stale-record
  thresholds and schedule intervals. It is intended for development feedback.
- `full` uses seconds-long provider responses, backoff, late callbacks and
  downtime windows so those behaviours are observable in real time.

Assertions use durable state and exact effect/attempt counts in both modes.
The full mode additionally asserts that retry backoff consumes observable time.

## Boundaries

- The lab deliberately contains no load test, queue-priority test, retention
  system, production failover simulation or deployment runbook.
- Cron timezone and DST semantics are outside the question being tested. The
  schedule proof is about due-record recovery after no process was running.
- Code-upgrade compatibility is proven for DBOS patching under one application
  version. It is not a blue/green deployment proof.
- Re-run the lab when changing DBOS or when a new product requirement needs a
  capability proof.
