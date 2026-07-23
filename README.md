# DBOS capability lab

This is a disposable lab for deciding whether `dbos-transact-golang v0.20.0`
can support the failure modes we care about. It is not a production service or
a release pipeline.

The lab answers nine questions:

1. Do duplicate starts create one logical operation?
2. Does a workflow recover after a crash around every durable step?
3. Does provider success followed by a lost response avoid duplicate submission?
4. Are early, late and duplicate callbacks safe?
5. Do retries, throttling and permanent rejection behave correctly?
6. Does cancellation avoid falsely reversing an external effect?
7. Does scheduled reconciliation recover work created during downtime?
8. Does an in-flight workflow survive a compatible code upgrade?
9. Can runtime state be restored or reconstructed after DBOS database loss?

Each question has one scenario in [`scenarios/`](scenarios). The required
provider guarantees and the exact evidence are in
[`docs/PROOF_CONTRACT.md`](docs/PROOF_CONTRACT.md).

## Run it

```bash
make test
make check
make check-full
```

`make test` runs unit and shell-syntax checks without infrastructure.
`make check` is the normal feedback loop. It changes timings, not coverage.
`make check-full` preserves the longer delays needed to observe realistic
timeouts, backoff, late callbacks and missed schedule ticks. A manually
triggered GitHub Actions workflow is also available for the full run.

Every scenario resets the three lab databases and Kafka state. Do not point
these commands at data you care about. You can run one proof directly, for
example:

```bash
DBOS_LAB_MODE=fast ./scenarios/lost-provider-response.sh
DBOS_LAB_MODE=full ./scenarios/lost-provider-response.sh
```

The lab uses three Postgres databases and Redpanda in Docker. The Go processes
run on the host so the scenarios can kill a worker at exact fault boundaries.

## Shape of the lab

```text
Redpanda event -> DBOS workflow -> Accountable submission intent
  -> fake authority -> callback or status lookup
  -> idempotent Accountable outcome command
```

- `cmd/worker` contains the DBOS workflow, event consumer and callback endpoint.
- `cmd/accountable` is the fake authoritative product service.
- `cmd/authority` is a fake provider with a durable idempotency/status ledger.
- `cmd/opsctl` exposes only the operations needed by the nine proofs.
- `internal/chaos` kills the worker at named durability boundaries.

DBOS owns only its runtime database. Accountable remains the source of business
truth, which is what makes reconstruction after DBOS database loss testable.
