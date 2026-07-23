package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"

	"github.com/accountable/dbos-conformance/internal/accountable"
	"github.com/accountable/dbos-conformance/internal/authority"
	"github.com/accountable/dbos-conformance/internal/domain"
	"github.com/accountable/dbos-conformance/internal/workflow"
)

const usage = `opsctl — operator CLI for the DBOS conformance spike

  list      [-status S] [-limit N] [-queues]     list workflows
  inspect   <workflow-id>                        status + step history
  cancel    <workflow-id>                        cancel a workflow
  resume    <workflow-id>                        resume a cancelled/failed workflow
  send      <workflow-id> -outcome accepted|rejected [-key K]
                                                 manually deliver a lost callback
  start     <filing-id> [-year Y] [-scenario S] [-version V]
                                                 enqueue one deterministic filing workflow
  reconcile [-apply] [-stale-seconds N]          compare Accountable, DBOS and provider state
`

func main() {
	if len(os.Args) < 2 {
		fmt.Print(usage)
		os.Exit(2)
	}
	cmd, args := os.Args[1], os.Args[2:]

	client, err := dbos.NewClient(context.Background(), dbos.ClientConfig{
		DatabaseURL: envOr("DBOS_DATABASE_URL", "postgres://postgres:dbos@localhost:5344/dbos?sslmode=disable"),
	})
	check(err)
	defer client.Shutdown(5 * time.Second)

	switch cmd {
	case "list":
		fs := flag.NewFlagSet("list", flag.ExitOnError)
		status := fs.String("status", "", "filter by status (PENDING, ENQUEUED, SUCCESS, ERROR, CANCELLED, ...)")
		limit := fs.Int("limit", 50, "max rows")
		queuesOnly := fs.Bool("queues", false, "only queued workflows")
		check(fs.Parse(args))
		opts := []dbos.ListWorkflowsOption{dbos.WithLimit(*limit), dbos.WithSortDesc()}
		if *status != "" {
			opts = append(opts, dbos.WithStatus([]dbos.WorkflowStatusType{dbos.WorkflowStatusType(*status)}))
		}
		if *queuesOnly {
			opts = append(opts, dbos.WithQueuesOnly())
		}
		wfs, err := client.ListWorkflows(opts...)
		check(err)
		for _, wf := range wfs {
			fmt.Printf("%-40s %-10s %-22s v=%s attempts=%d queue=%s executor=%s\n",
				wf.ID, wf.Status, wf.CreatedAt.Format(time.RFC3339), wf.ApplicationVersion,
				wf.Attempts, wf.QueueName, wf.ExecutorID)
		}
		fmt.Printf("(%d workflows)\n", len(wfs))

	case "inspect":
		requireArg(args, "workflow-id")
		handle, err := client.RetrieveWorkflow(args[0])
		check(err)
		st, err := handle.GetStatus()
		check(err)
		printJSON(st)
		steps, err := client.GetWorkflowSteps(args[0], dbos.WithStepsLoadOutput(true))
		check(err)
		fmt.Println("steps:")
		for _, s := range steps {
			errStr := ""
			if s.Error != nil {
				errStr = " error=" + s.Error.Error()
			}
			fmt.Printf("  [%d] %-28s output=%.80v%s\n", s.StepID, s.StepName, s.Output, errStr)
		}

	case "cancel":
		requireArg(args, "workflow-id")
		check(client.CancelWorkflow(args[0]))
		fmt.Println("cancelled", args[0])

	case "resume":
		requireArg(args, "workflow-id")
		_, err := client.ResumeWorkflow(args[0])
		check(err)
		fmt.Println("resumed", args[0])

	case "send":
		requireArg(args, "workflow-id")
		fs := flag.NewFlagSet("send", flag.ExitOnError)
		outcome := fs.String("outcome", "accepted", "accepted|rejected")
		key := fs.String("key", "", "idempotency key (default: operator-<wfid>-<outcome>)")
		check(fs.Parse(args[1:]))
		if *key == "" {
			*key = "operator-" + args[0] + "-" + *outcome
		}
		cb := domain.Callback{CallbackID: *key, SubmissionID: args[0],
			Outcome: *outcome, Detail: "manually delivered by operator"}
		check(client.Send(args[0], cb, domain.CallbackTopic, dbos.WithIdempotencyKey(*key)))
		fmt.Println("signalled", args[0], "with outcome", *outcome)

	case "start":
		requireArg(args, "filing-id")
		fs := flag.NewFlagSet("start", flag.ExitOnError)
		year := fs.Int("year", 2025, "filing tax year")
		scenario := fs.String("scenario", domain.ScenarioOK, "authority scenario")
		version := fs.String("version", envOr("APP_VERSION", "v1"), "application version")
		check(fs.Parse(args[1:]))
		wfID := domain.WorkflowID(args[0], *year)
		requested := domain.FilingInput{FilingID: args[0], TaxYear: *year, Scenario: *scenario}
		check(enqueueFiling(client, wfID, requested, *version))
		fmt.Println("enqueued", wfID)

	case "reconcile":
		fs := flag.NewFlagSet("reconcile", flag.ExitOnError)
		apply := fs.Bool("apply", false, "apply safe reconciliation actions (default: report only)")
		staleSeconds := fs.Int("stale-seconds", 30, "age after which an active workflow requires investigation")
		// DBOS workers only dequeue their own application version, so an
		// unstamped reconstruction would remain ENQUEUED forever.
		version := fs.String("version", envOr("APP_VERSION", "v1"), "application version of the worker fleet that must run the recreated workflows")
		check(fs.Parse(args))
		reconcile(client, *apply, *version, time.Duration(*staleSeconds)*time.Second)

	default:
		fmt.Print(usage)
		os.Exit(2)
	}
}

func enqueueFiling(client dbos.Client, wfID string, requested domain.FilingInput, appVersion string) error {
	_, enqueueErr := client.Enqueue(workflow.QueueName, workflow.FilingWorkflowName,
		requested,
		dbos.WithEnqueueWorkflowID(wfID),
		dbos.WithEnqueueApplicationVersion(appVersion))
	if enqueueErr != nil {
		var dbosErr *dbos.DBOSError
		if !errors.As(enqueueErr, &dbosErr) || dbosErr.Code != dbos.ConflictingWorkflowError {
			return enqueueErr
		}
	}

	// DBOS v0.20 can return an existing handle without a conflict, so the
	// durable input must be compared before acknowledging a direct start.
	workflows, err := client.ListWorkflows(
		dbos.WithWorkflowIDs([]string{wfID}), dbos.WithLoadInput(true), dbos.WithLimit(1))
	if err != nil {
		return fmt.Errorf("verify persisted workflow input: %w", err)
	}
	if len(workflows) != 1 {
		return fmt.Errorf("verify persisted workflow input: found %d workflows", len(workflows))
	}
	existing, err := domain.DecodeFilingInput(workflows[0].Input)
	if err != nil {
		return fmt.Errorf("decode persisted workflow input: %w", err)
	}
	if existing != requested {
		existingJSON, _ := json.Marshal(existing)
		requestedJSON, _ := json.Marshal(requested)
		material := append(append([]byte{}, existingJSON...), requestedJSON...)
		conflictID := fmt.Sprintf("opsctl-%x", sha256.Sum256(material))
		acct := &accountable.Client{
			BaseURL: envOr("ACCOUNTABLE_URL", "http://localhost:8081"),
			HTTP:    &http.Client{Timeout: 5 * time.Second},
		}
		if recordErr := acct.RecordWorkflowStartConflict(context.Background(), accountable.WorkflowStartConflict{
			EventID: conflictID, FilingID: requested.FilingID, TaxYear: requested.TaxYear,
			ExistingInput: existingJSON, RequestedInput: requestedJSON,
		}); recordErr != nil {
			return fmt.Errorf("record direct workflow-start conflict: %w", recordErr)
		}
		return fmt.Errorf("workflow %s already exists with input %+v; requested %+v", wfID, existing, requested)
	}
	return nil
}

func reconcile(client dbos.Client, apply bool, appVersion string, staleAfter time.Duration) {
	acct := &accountable.Client{
		BaseURL: envOr("ACCOUNTABLE_URL", "http://localhost:8081"),
		HTTP:    &http.Client{Timeout: 5 * time.Second},
	}
	auth := &authority.Client{
		BaseURL: envOr("AUTHORITY_URL", "http://localhost:8082"),
		HTTP:    &http.Client{Timeout: 5 * time.Second},
	}
	filings, err := acct.ListFilings(context.Background(), "reconcilable=1")
	check(err)
	if len(filings) == 0 {
		fmt.Println("no non-terminal filings — nothing to reconcile")
		return
	}
	missing := 0
	for _, f := range filings {
		wfID := domain.WorkflowID(f.FilingID, f.TaxYear)
		wfs, err := client.ListWorkflows(dbos.WithWorkflowIDs([]string{wfID}))
		check(err)
		if f.State == domain.StateNeedsReview {
			status, err := auth.Status(context.Background(), domain.ProviderOperationID(f.FilingID, f.TaxYear))
			check(err)
			if status.Status != "completed" {
				dbosStatus := "MISSING"
				if len(wfs) > 0 {
					dbosStatus = string(wfs[0].Status)
				}
				fmt.Printf("ESCALATE %-38s dbos_status=%s provider_status=%s reason=outcome_unresolved\n",
					wfID, dbosStatus, status.Status)
				continue
			}
			commandType, valid := finalCommandType(status.Outcome)
			if !valid {
				fmt.Printf("ESCALATE %-38s provider_status=completed provider_outcome=%s reason=unknown_outcome\n",
					wfID, status.Outcome)
				continue
			}
			if !apply {
				fmt.Printf("FINAL   %-40s provider_outcome=%s (run with -apply to record)\n", wfID, status.Outcome)
				continue
			}
			_, err = acct.ApplyCommand(context.Background(), accountable.CommandRequest{
				IdempotencyKey: domain.OutcomeCommandKey(wfID),
				CommandType:    commandType,
				FilingID:       f.FilingID,
				TaxYear:        f.TaxYear,
				Detail:         "resolved from completed provider operation",
			})
			check(err)
			fmt.Printf("FINALIZED %-38s provider_outcome=%s\n", wfID, status.Outcome)
			continue
		}
		if len(wfs) > 0 {
			if wfs[0].Status == dbos.WorkflowStatusPending || wfs[0].Status == dbos.WorkflowStatusEnqueued {
				age := time.Since(wfs[0].UpdatedAt)
				if age >= staleAfter {
					fmt.Printf("ESCALATE %-38s dbos_status=%s reason=stale_active age=%s\n",
						wfID, wfs[0].Status, age.Round(time.Second))
				} else {
					fmt.Printf("OK      %-40s dbos_status=%s\n", wfID, wfs[0].Status)
				}
				continue
			}
			if wfs[0].Status == dbos.WorkflowStatusDelayed {
				if time.Now().Before(wfs[0].DelayUntil.Add(staleAfter)) {
					fmt.Printf("OK      %-40s dbos_status=%s\n", wfID, wfs[0].Status)
				} else {
					fmt.Printf("ESCALATE %-38s dbos_status=%s reason=overdue_delay\n", wfID, wfs[0].Status)
				}
				continue
			}
			if wfs[0].Status == dbos.WorkflowStatusCancelled {
				fmt.Printf("ESCALATE %-38s dbos_status=%s reason=cancelled_requires_operator\n",
					wfID, wfs[0].Status)
				continue
			}
			if wfs[0].Status == dbos.WorkflowStatusError {
				if !apply {
					fmt.Printf("RECREATE %-39s dbos_status=%s (run with -apply to recreate)\n", wfID, wfs[0].Status)
					continue
				}
				err := client.DeleteWorkflows([]string{wfID})
				check(err)
				_, err = client.Enqueue(workflow.QueueName, workflow.FilingWorkflowName,
					domain.FilingInput{FilingID: f.FilingID, TaxYear: f.TaxYear, Scenario: f.Scenario},
					dbos.WithEnqueueWorkflowID(wfID),
					dbos.WithEnqueueApplicationVersion(appVersion))
				check(err)
				fmt.Printf("RECREATED %-38s from dbos_status=%s\n", wfID, wfs[0].Status)
				continue
			}
			if wfs[0].Status == dbos.WorkflowStatusSuccess {
				status, err := auth.Status(context.Background(), domain.ProviderOperationID(f.FilingID, f.TaxYear))
				check(err)
				commandType, valid := finalCommandType(status.Outcome)
				if status.Status != "completed" || !valid {
					fmt.Printf("ESCALATE %-38s dbos_status=%s provider_status=%s reason=successful_history_without_outcome\n",
						wfID, wfs[0].Status, status.Status)
					continue
				}
				if !apply {
					fmt.Printf("FINAL   %-40s dbos_status=%s provider_outcome=%s (run with -apply to record)\n",
						wfID, wfs[0].Status, status.Outcome)
					continue
				}
				_, err = acct.ApplyCommand(context.Background(), accountable.CommandRequest{
					IdempotencyKey: domain.OutcomeCommandKey(wfID),
					CommandType:    commandType,
					FilingID:       f.FilingID,
					TaxYear:        f.TaxYear,
					Detail:         "resolved from completed provider operation",
				})
				check(err)
				fmt.Printf("FINALIZED %-38s dbos_status=%s provider_outcome=%s\n",
					wfID, wfs[0].Status, status.Outcome)
				continue
			}
			fmt.Printf("ESCALATE %-38s dbos_status=%s reason=unsupported_terminal_status\n",
				wfID, wfs[0].Status)
			continue
		}
		missing++
		if !apply {
			fmt.Printf("MISSING %-40s accountable_state=%s (run with -apply to recreate)\n", wfID, f.State)
			continue
		}
		_, err = client.Enqueue(workflow.QueueName, workflow.FilingWorkflowName,
			domain.FilingInput{FilingID: f.FilingID, TaxYear: f.TaxYear, Scenario: f.Scenario},
			dbos.WithEnqueueWorkflowID(wfID),
			dbos.WithEnqueueApplicationVersion(appVersion))
		check(err)
		fmt.Printf("RECREATED %-40s from accountable_state=%s\n", wfID, f.State)
	}
	fmt.Printf("reconcile complete: %d filings checked, %d missing\n", len(filings), missing)
}

func finalCommandType(outcome string) (string, bool) {
	switch outcome {
	case "accepted":
		return "MarkFilingAccepted", true
	case "rejected":
		return "MarkFilingRejected", true
	default:
		return "", false
	}
}

func requireArg(args []string, name string) {
	if len(args) < 1 {
		fmt.Fprintf(os.Stderr, "missing <%s>\n%s", name, usage)
		os.Exit(2)
	}
}

func printJSON(v any) {
	b, _ := json.MarshalIndent(v, "", "  ")
	fmt.Println(string(b))
}

func check(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
