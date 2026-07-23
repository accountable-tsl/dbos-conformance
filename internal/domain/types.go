// PII boundary: none of these types may ever carry taxpayer personal data.
// Workflow inputs/outputs are persisted verbatim in the DBOS system database,
// so they are restricted to opaque identifiers and enum-like fields.
package domain

import "fmt"

type FilingEvent struct {
	EventID  string `json:"event_id"`
	FilingID string `json:"filing_id"`
	TaxYear  int    `json:"tax_year"`
	Scenario string `json:"scenario"`
}

type FilingInput struct {
	FilingID string `json:"filing_id"`
	TaxYear  int    `json:"tax_year"`
	Scenario string `json:"scenario"`
}

type FilingResult struct {
	FilingID string `json:"filing_id"`
	Outcome  string `json:"outcome"`
	Detail   string `json:"detail,omitempty"`
}

type Callback struct {
	CallbackID   string `json:"callback_id"`
	SubmissionID string `json:"submission_id"`
	Outcome      string `json:"outcome"`
	Detail       string `json:"detail,omitempty"`
}

const CallbackTopic = "authority-callback"

const (
	ScenarioOK                   = "ok"
	ScenarioDelayed              = "delayed"
	ScenarioReject               = "reject"
	ScenarioRateLimit            = "rate_limit"
	ScenarioRateLimitExhausted   = "rate_limit_exhausted"
	ScenarioTransientServerError = "transient_5xx"
	ScenarioTimeoutLost          = "timeout_lost"
	ScenarioTimeoutAmbiguous     = "timeout_ambiguous"
	ScenarioDupCallback          = "dup_callback"
	ScenarioLateCallback         = "late_callback"
	ScenarioNoCallbackDone       = "no_callback_done"
)

// Deterministic identity makes duplicate delivery and reconstruction converge.
func WorkflowID(filingID string, taxYear int) string {
	return fmt.Sprintf("filing-%s-y%d", filingID, taxYear)
}

func ProviderOperationID(filingID string, taxYear int) string {
	return fmt.Sprintf("filing-%s-y%d", filingID, taxYear)
}

// Stable command keys make replay and reconstruction reuse the original effect.
func SubmittedCommandKey(workflowID string) string { return workflowID + "-submitted" }
func OutcomeCommandKey(workflowID string) string   { return workflowID + "-outcome" }
func ReviewCommandKey(workflowID string) string    { return workflowID + "-needs-review" }
func SubmissionIntentCommandKey(operationID string) string {
	return operationID + "-submission-intent"
}

const (
	StateReady       = "ready"
	StateSubmitting  = "submitting"
	StateSubmitted   = "submitted"
	StateAccepted    = "accepted"
	StateRejected    = "rejected"
	StateNeedsReview = "needs_review"
)
