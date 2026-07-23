package domain

import "testing"

func TestProviderOperationIdentityIsCanonical(t *testing.T) {
	t.Parallel()

	got := ProviderOperationID("F-17", 2025)
	if want := "filing-F-17-y2025"; got != want {
		t.Fatalf("ProviderOperationID() = %q, want %q", got, want)
	}
}

func TestSubmissionIntentCommandKeyIsCanonical(t *testing.T) {
	t.Parallel()

	opID := ProviderOperationID("F-17", 2025)
	if got, want := SubmissionIntentCommandKey(opID), "filing-F-17-y2025-submission-intent"; got != want {
		t.Fatalf("SubmissionIntentCommandKey() = %q, want %q", got, want)
	}
}
