package authority

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/accountable/dbos-conformance/internal/domain"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func TestRateLimitIsObservableSeparatelyFromBusinessEffects(t *testing.T) {
	t.Parallel()

	handler := NewServer(Timing{}, slog.New(slog.NewTextHandler(io.Discard, nil))).Handler()

	req := SubmitRequest{
		SubmissionID: "filing-R1-y2025",
		FilingID:     "R1",
		Scenario:     domain.ScenarioRateLimit,
	}
	for attempt, wantStatus := range []int{http.StatusTooManyRequests, http.StatusTooManyRequests, http.StatusOK} {
		if got := postSubmission(t, handler, req); got != wantStatus {
			t.Fatalf("attempt %d status = %d, want %d", attempt+1, got, wantStatus)
		}
	}

	metrics := getMetrics(t, handler)
	assertMetric(t, metrics, "submission_attempts", 3)
	assertMetric(t, metrics, "submissions_recorded", 1)
	assertMetric(t, metrics, "throttled_429", 2)
}

func TestPermanentRateLimitNeverCreatesBusinessEffect(t *testing.T) {
	t.Parallel()

	handler := NewServer(Timing{}, slog.New(slog.NewTextHandler(io.Discard, nil))).Handler()

	req := SubmitRequest{
		SubmissionID: "filing-R2-y2025",
		FilingID:     "R2",
		Scenario:     domain.ScenarioRateLimitExhausted,
	}
	for attempt := 1; attempt <= 6; attempt++ {
		if got := postSubmission(t, handler, req); got != http.StatusTooManyRequests {
			t.Fatalf("attempt %d status = %d, want %d", attempt, got, http.StatusTooManyRequests)
		}
	}

	metrics := getMetrics(t, handler)
	assertMetric(t, metrics, "submission_attempts", 6)
	assertMetric(t, metrics, "submissions_recorded", 0)
	assertMetric(t, metrics, "throttled_429", 6)
}

func TestTransientServerErrorsRetryBeforeCreatingOneEffect(t *testing.T) {
	t.Parallel()

	handler := NewServer(Timing{}, slog.New(slog.NewTextHandler(io.Discard, nil))).Handler()
	req := SubmitRequest{
		SubmissionID: "filing-R3-y2025",
		FilingID:     "R3",
		Scenario:     domain.ScenarioTransientServerError,
	}
	for attempt, wantStatus := range []int{http.StatusServiceUnavailable, http.StatusServiceUnavailable, http.StatusOK} {
		if got := postSubmission(t, handler, req); got != wantStatus {
			t.Fatalf("attempt %d status = %d, want %d", attempt+1, got, wantStatus)
		}
	}

	metrics := getMetrics(t, handler)
	assertMetric(t, metrics, "submission_attempts", 3)
	assertMetric(t, metrics, "transient_5xx", 2)
	assertMetric(t, metrics, "submissions_recorded", 1)
}

func TestSubmissionIDCollisionWithDifferentIdentityIsRejected(t *testing.T) {
	t.Parallel()

	handler := NewServer(Timing{}, slog.New(slog.NewTextHandler(io.Discard, nil))).Handler()
	first := SubmitRequest{SubmissionID: "filing-SAME-y2025", FilingID: "SAME", Scenario: domain.ScenarioNoCallbackDone}
	if got := postSubmission(t, handler, first); got != http.StatusOK {
		t.Fatalf("first submit status = %d", got)
	}
	second := SubmitRequest{SubmissionID: first.SubmissionID, FilingID: "OTHER", Scenario: first.Scenario}
	if got := postSubmission(t, handler, second); got != http.StatusConflict {
		t.Fatalf("colliding submit status = %d, want %d", got, http.StatusConflict)
	}
	assertMetric(t, getMetrics(t, handler), "submissions_recorded", 1)
}

func TestProviderStateRollbackSnapshotIsIndependent(t *testing.T) {
	t.Parallel()

	original := persistentState{
		Submissions: map[string]*submission{"op": {ID: "op", Status: "processing"}},
		RateLimits:  map[string]int{"op": 1},
	}
	rollback := cloneState(original)
	original.Submissions["op"].Status = "completed"
	original.RateLimits["op"] = 2

	if got := rollback.Submissions["op"].Status; got != "processing" {
		t.Fatalf("rollback submission status = %q, want processing", got)
	}
	if got := rollback.RateLimits["op"]; got != 1 {
		t.Fatalf("rollback rate-limit attempts = %d, want 1", got)
	}
}

func TestUnmarked5xxIsAmbiguousInsteadOfRetryable(t *testing.T) {
	t.Parallel()

	client := &Client{BaseURL: "http://authority.invalid", HTTP: &http.Client{
		Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusServiceUnavailable,
				Header: http.Header{}, Body: io.NopCloser(strings.NewReader("unknown"))}, nil
		}),
	}}
	outcome, err := client.Submit(context.Background(), SubmitRequest{SubmissionID: "op"})
	if err != nil {
		t.Fatalf("unmarked 5xx must not enter blind retry path: %v", err)
	}
	if outcome.Result != "ambiguous" {
		t.Fatalf("outcome = %q, want ambiguous", outcome.Result)
	}
}

func TestProviderDeclaredNoEffect5xxIsRetryable(t *testing.T) {
	t.Parallel()

	client := &Client{BaseURL: "http://authority.invalid", HTTP: &http.Client{
		Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusServiceUnavailable,
				Header: http.Header{"X-Request-Effect": []string{"none"}},
				Body:   io.NopCloser(strings.NewReader("not applied"))}, nil
		}),
	}}
	if _, err := client.Submit(context.Background(), SubmitRequest{SubmissionID: "op"}); err == nil {
		t.Fatal("provider-declared no-effect 5xx should be retryable")
	} else if _, ok := err.(*RetryableError); !ok {
		t.Fatalf("error type = %T, want *RetryableError", err)
	}
}

func postSubmission(t *testing.T, handler http.Handler, req SubmitRequest) int {
	t.Helper()
	body, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	httpReq := httptest.NewRequest(http.MethodPost, "/submissions", bytes.NewReader(body))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httpReq)
	return recorder.Code
}

func getMetrics(t *testing.T, handler http.Handler) map[string]int {
	t.Helper()
	httpReq := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httpReq)
	var metrics map[string]int
	if err := json.NewDecoder(recorder.Body).Decode(&metrics); err != nil {
		t.Fatal(err)
	}
	return metrics
}

func assertMetric(t *testing.T, metrics map[string]int, name string, want int) {
	t.Helper()
	if got := metrics[name]; got != want {
		t.Fatalf("metric %s = %d, want %d", name, got, want)
	}
}
