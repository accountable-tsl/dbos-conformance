package accountable

import (
	"testing"

	"github.com/accountable/dbos-conformance/internal/domain"
)

func TestTerminalStatesRejectEveryCommand(t *testing.T) {
	t.Parallel()

	for commandType := range commandTransitions {
		for _, state := range []string{domain.StateAccepted, domain.StateRejected} {
			if commandAllowsState(commandType, state) {
				t.Fatalf("%s unexpectedly permits terminal state %s", commandType, state)
			}
		}
	}
}

func TestOutcomeCanResolveNeedsReviewButSubmittedCannotOverwriteIt(t *testing.T) {
	t.Parallel()

	if commandAllowsState("MarkFilingSubmitted", domain.StateNeedsReview) {
		t.Fatal("a delayed MarkFilingSubmitted must not overwrite needs_review")
	}
	if !commandAllowsState("MarkFilingAccepted", domain.StateNeedsReview) {
		t.Fatal("operator/reconciliation recovery must be able to resolve needs_review")
	}
}
