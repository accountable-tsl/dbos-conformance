package main

import (
	"testing"

	"github.com/accountable/dbos-conformance/internal/domain"
)

func TestDecodeFilingInputFromDBOSArgumentList(t *testing.T) {
	t.Parallel()

	want := domain.FilingInput{FilingID: "F1", TaxYear: 2025, Scenario: "ok"}
	got, err := domain.DecodeFilingInput([]any{map[string]any{
		"filing_id": "F1", "tax_year": float64(2025), "scenario": "ok",
	}})
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("decoded input = %#v, want %#v", got, want)
	}
}

func TestDecodeFilingInputRejectsUnknownShape(t *testing.T) {
	t.Parallel()

	if _, err := domain.DecodeFilingInput(map[string]any{"unrelated": true}); err == nil {
		t.Fatal("expected unknown DBOS input shape to fail closed")
	}
}

func TestDecodeFilingInputFromDBOSJSONString(t *testing.T) {
	t.Parallel()

	want := domain.FilingInput{FilingID: "F1", TaxYear: 2025, Scenario: "ok"}
	got, err := domain.DecodeFilingInput(`{"filing_id":"F1","tax_year":2025,"scenario":"ok"}`)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("decoded input = %#v, want %#v", got, want)
	}
}
