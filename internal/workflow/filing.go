// Keeping DBOS out of authoritative Postgres ensures every business effect is
// visible through a canonically idempotent Accountable command.
package workflow

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"

	"github.com/accountable/dbos-conformance/internal/accountable"
	"github.com/accountable/dbos-conformance/internal/authority"
	"github.com/accountable/dbos-conformance/internal/chaos"
	"github.com/accountable/dbos-conformance/internal/domain"
)

// Stable names preserve recovery and enqueue-by-name compatibility across deployments.
const (
	FilingWorkflowName    = "FilingWorkflow"
	ReconcileWorkflowName = "ReconcileWorkflow"
	QueueName             = "filing-queue"
)

type Config struct {
	AccountableURL        string
	AuthorityURL          string
	CallbackURL           string
	V2                    bool
	RecvTimeout           time.Duration
	StatusPolls           int
	HTTPTimeout           time.Duration
	SubmitRetryBase       time.Duration
	ReconcileStaleSeconds int
}

var (
	cfg  Config
	acct *accountable.Client
	auth *authority.Client
)

func Configure(c Config) {
	if c.RecvTimeout == 0 {
		c.RecvTimeout = 20 * time.Second
	}
	if c.StatusPolls == 0 {
		c.StatusPolls = 3
	}
	if c.HTTPTimeout == 0 {
		c.HTTPTimeout = 5 * time.Second
	}
	if c.SubmitRetryBase == 0 {
		c.SubmitRetryBase = 500 * time.Millisecond
	}
	if c.ReconcileStaleSeconds == 0 {
		c.ReconcileStaleSeconds = 30
	}
	cfg = c
	httpc := &http.Client{Timeout: c.HTTPTimeout}
	acct = &accountable.Client{BaseURL: c.AccountableURL, HTTP: httpc}
	auth = &authority.Client{BaseURL: c.AuthorityURL, HTTP: httpc}
}

// Only provable no-effect failures are wrapped because an ambiguous retry could
// duplicate an external filing.
func retryable(err error) bool {
	var re *authority.RetryableError
	return errors.As(err, &re)
}

func Filing(ctx dbos.DBOSContext, in domain.FilingInput) (domain.FilingResult, error) {
	// Filing-derived identities make reconstructed workflows converge on the same effect.
	canonID := domain.WorkflowID(in.FilingID, in.TaxYear)
	operationID := domain.ProviderOperationID(in.FilingID, in.TaxYear)
	chaos.Crash("wf-start")

	filing, err := dbos.RunAsStep(ctx, func(c context.Context) (*accountable.Filing, error) {
		return acct.GetFiling(c, in.FilingID, in.TaxYear)
	}, dbos.WithStepName("validate"))
	if err != nil {
		return domain.FilingResult{}, fmt.Errorf("validate: %w", err)
	}
	chaos.Crash("after-validate")

	switch filing.State {
	case domain.StateAccepted, domain.StateRejected:
		// Accountable remains authoritative after runtime reconstruction, so a
		// terminal filing must not be replayed.
		return domain.FilingResult{FilingID: in.FilingID, Outcome: "already_final",
			Detail: "filing already in state " + filing.State}, nil
	}
	if filing.ProviderOperationID != "" && filing.ProviderOperationID != operationID {
		return domain.FilingResult{}, fmt.Errorf(
			"provider operation mismatch: accountable=%s canonical=%s",
			filing.ProviderOperationID, operationID)
	}

	// A patch marker preserves old histories while letting new executions take
	// the v2 pre-check.
	if cfg.V2 {
		chaos.Crash("before-patch-evaluation")
		patched, err := dbos.Patch(ctx, "v2-risk-precheck")
		if err != nil {
			return domain.FilingResult{}, fmt.Errorf("evaluate v2 patch: %w", err)
		}
		chaos.Crash("after-patch-evaluation")
		if patched {
			if _, err := dbos.RunAsStep(ctx, func(c context.Context) (string, error) {
				out := "risk-ok"
				chaos.Crash("in-risk-precheck")
				return out, nil
			}, dbos.WithStepName("riskPrecheck")); err != nil {
				return domain.FilingResult{}, err
			}
			chaos.Crash("after-risk-precheck")
		}
	}

	// Intent is durable before the external effect so total runtime loss can
	// reconstruct the same provider correlation ID.
	if filing.State == domain.StateReady {
		chaos.Crash("before-prepare")
		prepared, err := dbos.RunAsStep(ctx, func(c context.Context) (*accountable.CommandResult, error) {
			out, err := acct.ApplyCommand(c, accountable.CommandRequest{
				IdempotencyKey:      domain.SubmissionIntentCommandKey(canonID),
				CommandType:         "PrepareFilingSubmission",
				FilingID:            in.FilingID,
				TaxYear:             in.TaxYear,
				ProviderOperationID: operationID,
			})
			if err == nil {
				chaos.Crash("in-prepare-after-apply")
			}
			return out, err
		}, dbos.WithStepName("prepareSubmission"), dbos.WithStepMaxRetries(5))
		if err != nil {
			return domain.FilingResult{}, err
		}
		filing.State = prepared.NewState
		filing.ProviderOperationID = operationID
		chaos.Crash("after-prepare")
	}

	// Status is checked before every POST because a stale restore may hide an
	// already-created provider operation.
	submissionID := operationID
	skipSubmit := false
	chaos.Crash("before-find-existing")
	existing, err := dbos.RunAsStep(ctx, func(c context.Context) (authority.StatusResponse, error) {
		return auth.Status(c, operationID)
	}, dbos.WithStepName("findExistingSubmission"), dbos.WithStepMaxRetries(5))
	if err != nil {
		return domain.FilingResult{}, err
	}
	chaos.Crash("after-find-existing")
	switch existing.Status {
	case "completed":
		if err := recordOutcome(ctx, canonID, in, mapOutcome(existing.Outcome), "adopted completed submission "+existing.SubmissionID); err != nil {
			return domain.FilingResult{}, err
		}
		return domain.FilingResult{FilingID: in.FilingID, Outcome: existing.Outcome,
			Detail: "adopted submission " + existing.SubmissionID}, nil
	case "processing":
		submissionID = existing.SubmissionID
		skipSubmit = true
	}

	var sub authority.SubmitOutcome
	if skipSubmit {
		sub = authority.SubmitOutcome{Result: "accepted", Detail: "adopted in-flight submission " + submissionID}
	} else {
		chaos.Crash("before-submit")
		sub, err = dbos.RunAsStep(ctx, func(c context.Context) (authority.SubmitOutcome, error) {
			out, err := auth.Submit(c, authority.SubmitRequest{
				SubmissionID: operationID, FilingID: in.FilingID,
				Scenario: in.Scenario, CallbackURL: cfg.CallbackURL,
			})
			if err == nil {
				chaos.Crash("in-submit-after-accept")
			}
			return out, err
		}, dbos.WithStepName("submit"),
			dbos.WithStepMaxRetries(5),
			dbos.WithBaseInterval(cfg.SubmitRetryBase),
			dbos.WithRetryPredicate(retryable))
		if err != nil {
			if recordErr := recordOutcome(ctx, canonID, in, domain.StateNeedsReview,
				"submit retries exhausted: "+err.Error()); recordErr != nil {
				return domain.FilingResult{}, fmt.Errorf("record exhausted submit retries: %w", recordErr)
			}
			return domain.FilingResult{FilingID: in.FilingID, Outcome: "needs_review",
				Detail: err.Error()}, nil
		}
		chaos.Crash("after-submit")
	}

	// Ambiguity is resolved by lookup because blind resubmission can duplicate
	// an external effect.
	if sub.Result == "ambiguous" {
		chaos.Crash("before-resolve-ambiguous")
		st, err := dbos.RunAsStep(ctx, func(c context.Context) (authority.StatusResponse, error) {
			return auth.Status(c, operationID)
		}, dbos.WithStepName("resolveAmbiguousSubmit"), dbos.WithStepMaxRetries(5))
		if err != nil {
			return domain.FilingResult{}, err
		}
		chaos.Crash("after-resolve-ambiguous")
		switch st.Status {
		case "not_found":
			// A not-found result proves the first request had no external effect.
			chaos.Crash("before-resubmit")
			sub, err = dbos.RunAsStep(ctx, func(c context.Context) (authority.SubmitOutcome, error) {
				out, err := auth.Submit(c, authority.SubmitRequest{
					SubmissionID: operationID, FilingID: in.FilingID,
					Scenario: in.Scenario, CallbackURL: cfg.CallbackURL,
				})
				if err == nil {
					chaos.Crash("in-resubmit-after-accept")
				}
				return out, err
			}, dbos.WithStepName("resubmit"), dbos.WithStepMaxRetries(5),
				dbos.WithBaseInterval(cfg.SubmitRetryBase),
				dbos.WithRetryPredicate(retryable))
			if err != nil {
				return domain.FilingResult{}, err
			}
			chaos.Crash("after-resubmit")
		case "completed":
			if err := recordOutcome(ctx, canonID, in, mapOutcome(st.Outcome), "resolved after ambiguous submit"); err != nil {
				return domain.FilingResult{}, err
			}
			return domain.FilingResult{FilingID: in.FilingID, Outcome: st.Outcome}, nil
		default:
			sub = authority.SubmitOutcome{Result: "accepted", Detail: "confirmed via status check"}
		}
	}

	if sub.Result == "rejected" {
		if err := recordOutcome(ctx, canonID, in, domain.StateRejected, sub.Detail); err != nil {
			return domain.FilingResult{}, err
		}
		return domain.FilingResult{FilingID: in.FilingID, Outcome: "rejected", Detail: sub.Detail}, nil
	}

	if filing.State != domain.StateSubmitted {
		chaos.Crash("before-mark-submitted")
		if _, err := dbos.RunAsStep(ctx, func(c context.Context) (*accountable.CommandResult, error) {
			out, err := acct.ApplyCommand(c, accountable.CommandRequest{
				IdempotencyKey: domain.SubmittedCommandKey(canonID),
				CommandType:    "MarkFilingSubmitted",
				FilingID:       in.FilingID,
				TaxYear:        in.TaxYear,
			})
			if err == nil {
				chaos.Crash("in-mark-submitted-after-apply")
			}
			return out, err
		}, dbos.WithStepName("markSubmitted"), dbos.WithStepMaxRetries(5)); err != nil {
			return domain.FilingResult{}, err
		}
		chaos.Crash("after-mark-submitted")
	}
	chaos.Crash("before-wait")

	// Bounded polling prevents a lost callback from hanging forever while still
	// surfacing unresolved outcomes to operators.
	outcome, detail := "", ""
	for poll := 0; poll < cfg.StatusPolls; poll++ {
		cb, err := dbos.Recv[domain.Callback](ctx, domain.CallbackTopic, cfg.RecvTimeout)
		if err == nil {
			chaos.Crash("after-callback")
			outcome, detail = cb.Outcome, "via callback "+cb.CallbackID
			break
		}
		var dbosErr *dbos.DBOSError
		if !errors.As(err, &dbosErr) {
			return domain.FilingResult{}, err
		}
		// Provider state, not timeout interpretation, decides the outcome.
		chaos.Crash("before-status-poll")
		st, serr := dbos.RunAsStep(ctx, func(c context.Context) (authority.StatusResponse, error) {
			return auth.Status(c, submissionID)
		}, dbos.WithStepName("pollStatus"), dbos.WithStepMaxRetries(5))
		if serr != nil {
			return domain.FilingResult{}, serr
		}
		chaos.Crash("after-status-poll")
		if st.Status == "completed" {
			outcome, detail = st.Outcome, "via status poll (callback missing)"
			break
		}
	}
	if outcome == "" {
		outcome, detail = "needs_review", "no callback and authority still processing"
	}

	chaos.Crash("before-record")

	if err := recordOutcome(ctx, canonID, in, mapOutcome(outcome), detail); err != nil {
		return domain.FilingResult{}, err
	}
	chaos.Crash("after-record")

	chaos.Crash("before-final-reconcile")
	final, err := dbos.RunAsStep(ctx, func(c context.Context) (*accountable.Filing, error) {
		return acct.GetFiling(c, in.FilingID, in.TaxYear)
	}, dbos.WithStepName("reconcileFinal"), dbos.WithStepMaxRetries(5))
	if err != nil {
		return domain.FilingResult{}, err
	}
	if final.State != mapOutcome(outcome) {
		return domain.FilingResult{}, fmt.Errorf(
			"reconcile mismatch: accountable=%s workflow-outcome=%s", final.State, outcome)
	}
	chaos.Crash("after-final-reconcile")
	return domain.FilingResult{FilingID: in.FilingID, Outcome: outcome, Detail: detail}, nil
}

func mapOutcome(outcome string) string {
	switch outcome {
	case "accepted":
		return domain.StateAccepted
	case "rejected":
		return domain.StateRejected
	default:
		return domain.StateNeedsReview
	}
}

func recordOutcome(ctx dbos.DBOSContext, canonID string, in domain.FilingInput, state, detail string) error {
	cmdType := map[string]string{
		domain.StateAccepted:    "MarkFilingAccepted",
		domain.StateRejected:    "MarkFilingRejected",
		domain.StateNeedsReview: "MarkFilingNeedsReview",
	}[state]
	commandKey := domain.OutcomeCommandKey(canonID)
	if state == domain.StateNeedsReview {
		commandKey = domain.ReviewCommandKey(canonID)
	}
	_, err := dbos.RunAsStep(ctx, func(c context.Context) (*accountable.CommandResult, error) {
		out, err := acct.ApplyCommand(c, accountable.CommandRequest{
			IdempotencyKey: commandKey,
			CommandType:    cmdType,
			FilingID:       in.FilingID,
			TaxYear:        in.TaxYear,
			Detail:         detail,
		})
		if err == nil {
			chaos.Crash("in-record-after-apply")
		}
		return out, err
	}, dbos.WithStepName("recordOutcome"), dbos.WithStepMaxRetries(5))
	return err
}

// Authoritative due records and deterministic IDs make the safety net a no-op
// when healthy and a reconstruction mechanism after runtime loss.
func Reconcile(ctx dbos.DBOSContext, scheduledAt time.Time) (string, error) {
	stale, err := dbos.RunAsStep(ctx, func(c context.Context) ([]accountable.Filing, error) {
		return acct.ListFilings(c, fmt.Sprintf("reconcilable=1&stale_seconds=%d", cfg.ReconcileStaleSeconds))
	}, dbos.WithStepName("fetchDueRecords"), dbos.WithStepMaxRetries(3))
	if err != nil {
		return "", err
	}
	started := 0
	finalized := 0
	recreated := 0
	escalated := 0
	for _, f := range stale {
		wfID := domain.WorkflowID(f.FilingID, f.TaxYear)
		if f.State == domain.StateNeedsReview {
			status, err := dbos.RunAsStep(ctx, func(c context.Context) (authority.StatusResponse, error) {
				return auth.Status(c, domain.ProviderOperationID(f.FilingID, f.TaxYear))
			}, dbos.WithStepName("resolveReviewedProvider"), dbos.WithStepMaxRetries(3))
			if err != nil {
				return "", err
			}
			if status.Status != "completed" {
				escalated++
				continue
			}
			state := mapOutcome(status.Outcome)
			if state == domain.StateNeedsReview {
				escalated++
				continue
			}
			if err := recordOutcome(ctx, wfID, domain.FilingInput{
				FilingID: f.FilingID, TaxYear: f.TaxYear, Scenario: f.Scenario,
			}, state, "resolved by scheduled reconciliation"); err != nil {
				return "", err
			}
			finalized++
			continue
		}
		wfs, err := dbos.ListWorkflows(ctx, dbos.WithWorkflowIDs([]string{wfID}))
		if err != nil {
			return "", err
		}
		if len(wfs) > 0 {
			switch wfs[0].Status {
			case dbos.WorkflowStatusCancelled:
				escalated++
				continue
			case dbos.WorkflowStatusPending, dbos.WorkflowStatusEnqueued:
				if scheduledAt.Sub(wfs[0].UpdatedAt) >= time.Duration(cfg.ReconcileStaleSeconds)*time.Second {
					escalated++
				}
				continue
			case dbos.WorkflowStatusDelayed:
				if !scheduledAt.Before(wfs[0].DelayUntil.Add(time.Duration(cfg.ReconcileStaleSeconds) * time.Second)) {
					escalated++
				}
				continue
			case dbos.WorkflowStatusSuccess:
				status, err := dbos.RunAsStep(ctx, func(c context.Context) (authority.StatusResponse, error) {
					return auth.Status(c, domain.ProviderOperationID(f.FilingID, f.TaxYear))
				}, dbos.WithStepName("inspectSuccessfulProvider"), dbos.WithStepMaxRetries(3))
				if err != nil {
					return "", err
				}
				state := mapOutcome(status.Outcome)
				if status.Status != "completed" || state == domain.StateNeedsReview {
					escalated++
					continue
				}
				if err := recordOutcome(ctx, wfID, domain.FilingInput{
					FilingID: f.FilingID, TaxYear: f.TaxYear, Scenario: f.Scenario,
				}, state, "resolved by scheduled reconciliation"); err != nil {
					return "", err
				}
				finalized++
				continue
			case dbos.WorkflowStatusError:
				if err := dbos.DeleteWorkflows(ctx, []string{wfID}); err != nil {
					return "", err
				}
				recreated++
			default:
				escalated++
				continue
			}
		}
		_, err = dbos.RunWorkflow(ctx, Filing, domain.FilingInput{
			FilingID: f.FilingID, TaxYear: f.TaxYear, Scenario: f.Scenario,
		}, dbos.WithWorkflowID(wfID), dbos.WithQueue(QueueName))
		if err != nil {
			var dbosErr *dbos.DBOSError
			if errors.As(err, &dbosErr) && dbosErr.Code == dbos.ConflictingWorkflowError {
				continue
			}
			return "", err
		}
		started++
	}
	return fmt.Sprintf("checked=%d started=%d recreated=%d finalized=%d escalated=%d at=%s",
		len(stale), started, recreated, finalized, escalated, scheduledAt.Format(time.RFC3339)), nil
}
