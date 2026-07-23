package authority

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/accountable/dbos-conformance/internal/domain"
)

type submission struct {
	ID               string    `json:"submission_id"`
	FilingID         string    `json:"filing_id"`
	Scenario         string    `json:"scenario"`
	CallbackURL      string    `json:"callback_url"`
	Status           string    `json:"status"`
	Outcome          string    `json:"outcome"`
	Attempts         int       `json:"attempts"`
	CreatedAt        time.Time `json:"created_at"`
	CallbackFinished bool      `json:"callback_finished"`
}

type Metrics struct {
	SubmissionAttempts   int `json:"submission_attempts"`
	Submissions          int `json:"submissions_recorded"`
	DuplicateSubmissions int `json:"duplicate_submission_attempts"`
	CallbacksSent        int `json:"callbacks_sent"`
	Throttled429         int `json:"throttled_429"`
	Transient5xx         int `json:"transient_5xx"`
}

type persistentState struct {
	Submissions  map[string]*submission `json:"submissions"`
	RateLimits   map[string]int         `json:"rate_limit_attempts"`
	ServerErrors map[string]int         `json:"server_error_attempts"`
	Timeouts     map[string]int         `json:"timeout_attempts"`
	Metrics      Metrics                `json:"metrics"`
}

type Timing struct {
	CompletionDelay          time.Duration
	AmbiguousCompletionDelay time.Duration
	DelayedCompletionDelay   time.Duration
	LateCallbackDelay        time.Duration
	LostResponseDelay        time.Duration
}

func (t Timing) withDefaults() Timing {
	if t.CompletionDelay == 0 {
		t.CompletionDelay = 2 * time.Second
	}
	if t.DelayedCompletionDelay == 0 {
		t.DelayedCompletionDelay = 25 * time.Second
	}
	if t.AmbiguousCompletionDelay == 0 {
		t.AmbiguousCompletionDelay = t.CompletionDelay
	}
	if t.LateCallbackDelay == 0 {
		t.LateCallbackDelay = 23 * time.Second
	}
	if t.LostResponseDelay == 0 {
		t.LostResponseDelay = 8 * time.Second
	}
	return t
}

const persistentSchema = `
CREATE TABLE IF NOT EXISTS authority_state (
	singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
	state jsonb NOT NULL,
	updated_at timestamptz NOT NULL DEFAULT now()
);`

type Server struct {
	mu   sync.Mutex
	subs map[string]*submission

	rlAttempts map[string]int
	seAttempts map[string]int
	tlAttempts map[string]int

	metrics      Metrics
	pool         *pgxpool.Pool
	timing       Timing
	callbackHTTP *http.Client

	log *slog.Logger
}

func NewServer(timing Timing, log *slog.Logger) *Server {
	return &Server{
		subs:         map[string]*submission{},
		rlAttempts:   map[string]int{},
		seAttempts:   map[string]int{},
		tlAttempts:   map[string]int{},
		timing:       timing.withDefaults(),
		callbackHTTP: &http.Client{Timeout: 5 * time.Second},
		log:          log,
	}
}

// Provider truth must survive independently when DBOS state is deliberately lost.
func NewPersistentServer(ctx context.Context, dbURL string, timing Timing, log *slog.Logger) (*Server, error) {
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		return nil, err
	}
	if _, err := pool.Exec(ctx, persistentSchema); err != nil {
		pool.Close()
		return nil, fmt.Errorf("apply authority schema: %w", err)
	}
	s := NewServer(timing, log)
	s.pool = pool
	var raw []byte
	err = pool.QueryRow(ctx, `SELECT state FROM authority_state WHERE singleton=true`).Scan(&raw)
	if err != nil && err != pgx.ErrNoRows {
		pool.Close()
		return nil, fmt.Errorf("load authority state: %w", err)
	}
	if err == nil {
		var state persistentState
		if err := json.Unmarshal(raw, &state); err != nil {
			pool.Close()
			return nil, fmt.Errorf("decode authority state: %w", err)
		}
		s.restore(state)
	}
	s.resumePending()
	return s, nil
}

func (s *Server) Close() {
	if s.pool != nil {
		s.pool.Close()
	}
}

func (s *Server) snapshotLocked() persistentState {
	return persistentState{
		Submissions: s.subs, RateLimits: s.rlAttempts, ServerErrors: s.seAttempts,
		Timeouts: s.tlAttempts, Metrics: s.metrics,
	}
}

func cloneState(state persistentState) persistentState {
	clone := state
	clone.Submissions = make(map[string]*submission, len(state.Submissions))
	for id, sub := range state.Submissions {
		copy := *sub
		clone.Submissions[id] = &copy
	}
	clone.RateLimits = cloneIntMap(state.RateLimits)
	clone.ServerErrors = cloneIntMap(state.ServerErrors)
	clone.Timeouts = cloneIntMap(state.Timeouts)
	return clone
}

func cloneIntMap(source map[string]int) map[string]int {
	clone := make(map[string]int, len(source))
	for key, value := range source {
		clone[key] = value
	}
	return clone
}

func (s *Server) restore(state persistentState) {
	if state.Submissions != nil {
		s.subs = state.Submissions
	}
	if state.RateLimits != nil {
		s.rlAttempts = state.RateLimits
	}
	if state.ServerErrors != nil {
		s.seAttempts = state.ServerErrors
	}
	if state.Timeouts != nil {
		s.tlAttempts = state.Timeouts
	}
	s.metrics = state.Metrics
}

// The ledger is persisted before acknowledgement because it represents the
// external effect under test.
func (s *Server) persistLocked(ctx context.Context) error {
	if s.pool == nil {
		return nil
	}
	raw, err := json.Marshal(s.snapshotLocked())
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `INSERT INTO authority_state(singleton,state)
		VALUES(true,$1) ON CONFLICT(singleton) DO UPDATE
		SET state=excluded.state, updated_at=now()`, string(raw))
	return err
}

func (s *Server) persistDurablyLocked() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.persistLocked(ctx)
}

func (s *Server) resumePending() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, sub := range s.subs {
		if sub.Status == "processing" || !sub.CallbackFinished {
			s.scheduleCompletion(sub)
		}
	}
}

type SubmitRequest struct {
	SubmissionID string `json:"submission_id"`
	FilingID     string `json:"filing_id"`
	Scenario     string `json:"scenario"`
	CallbackURL  string `json:"callback_url"`
}

type SubmitResponse struct {
	SubmissionID string `json:"submission_id"`
	Status       string `json:"status"`
	Detail       string `json:"detail,omitempty"`
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /submissions", s.handleSubmit)
	mux.HandleFunc("GET /submissions/{id}", s.handleStatus)
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		writeJSON(w, s.metrics)
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	return mux
}

func (s *Server) handleSubmit(w http.ResponseWriter, r *http.Request) {
	var req SubmitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.SubmissionID == "" {
		http.Error(w, "bad submission", http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	before := cloneState(s.snapshotLocked())
	s.metrics.SubmissionAttempts++

	// Reusing the original result turns at-least-once execution into one provider
	// effect.
	if existing, ok := s.subs[req.SubmissionID]; ok {
		if existing.FilingID != req.FilingID || existing.Scenario != req.Scenario {
			if !s.saveOrFailLocked(w, before) {
				return
			}
			s.mu.Unlock()
			http.Error(w, "submission ID already belongs to a different operation", http.StatusConflict)
			return
		}
		existing.Attempts++
		s.metrics.DuplicateSubmissions++
		resp := SubmitResponse{SubmissionID: existing.ID, Status: statusFromOutcome(existing)}
		s.log.Info("duplicate submission attempt deduplicated",
			"submission", existing.ID, "attempts", existing.Attempts)
		if !s.saveOrFailLocked(w, before) {
			return
		}
		s.mu.Unlock()
		writeJSON(w, resp)
		return
	}

	switch req.Scenario {
	case domain.ScenarioRateLimitExhausted:
		s.metrics.Throttled429++
		if !s.saveOrFailLocked(w, before) {
			return
		}
		s.mu.Unlock()
		w.Header().Set("Retry-After", "1")
		http.Error(w, "rate limited", http.StatusTooManyRequests)
		return

	case domain.ScenarioRateLimit:
		s.rlAttempts[req.SubmissionID]++
		if s.rlAttempts[req.SubmissionID] <= 2 {
			s.metrics.Throttled429++
			if !s.saveOrFailLocked(w, before) {
				return
			}
			s.mu.Unlock()
			w.Header().Set("Retry-After", "1")
			http.Error(w, "rate limited", http.StatusTooManyRequests)
			return
		}

	case domain.ScenarioTransientServerError:
		s.seAttempts[req.SubmissionID]++
		if s.seAttempts[req.SubmissionID] <= 2 {
			s.metrics.Transient5xx++
			if !s.saveOrFailLocked(w, before) {
				return
			}
			s.mu.Unlock()
			w.Header().Set("X-Request-Effect", "none")
			http.Error(w, "temporary provider outage", http.StatusServiceUnavailable)
			return
		}

	case domain.ScenarioTimeoutLost:
		s.tlAttempts[req.SubmissionID]++
		if s.tlAttempts[req.SubmissionID] == 1 {
			// Persisting no operation makes a later not-found lookup sufficient to
			// prove resubmission safe.
			if !s.saveOrFailLocked(w, before) {
				return
			}
			s.mu.Unlock()
			time.Sleep(s.timing.LostResponseDelay)
			http.Error(w, "gateway timeout", http.StatusGatewayTimeout)
			return
		}

	case domain.ScenarioReject:
		sub := s.recordLocked(req)
		sub.Status = "completed"
		sub.Outcome = "rejected"
		sub.CallbackFinished = true
		if !s.saveOrFailLocked(w, before) {
			return
		}
		s.mu.Unlock()
		writeJSON(w, SubmitResponse{SubmissionID: req.SubmissionID, Status: "rejected",
			Detail: "permanent validation failure"})
		return
	}

	sub := s.recordLocked(req)
	if !s.saveOrFailLocked(w, before) {
		return
	}
	s.scheduleCompletion(sub)

	if req.Scenario == domain.ScenarioTimeoutAmbiguous {
		// Losing the response after persistence forces the client to resolve the
		// ambiguity through status lookup.
		s.mu.Unlock()
		time.Sleep(s.timing.LostResponseDelay)
		writeJSON(w, SubmitResponse{SubmissionID: req.SubmissionID, Status: "accepted"})
		return
	}
	s.mu.Unlock()
	writeJSON(w, SubmitResponse{SubmissionID: req.SubmissionID, Status: "accepted"})
}

// State is saved before it becomes observable so a successful response cannot
// outrun provider durability.
func (s *Server) saveOrFailLocked(w http.ResponseWriter, before persistentState) bool {
	if err := s.persistDurablyLocked(); err != nil {
		s.log.Error("persist provider state", "err", err)
		s.restore(before)
		s.mu.Unlock()
		http.Error(w, "provider ledger unavailable", http.StatusInternalServerError)
		return false
	}
	return true
}

func (s *Server) recordLocked(req SubmitRequest) *submission {
	sub := &submission{
		ID: req.SubmissionID, FilingID: req.FilingID, Scenario: req.Scenario,
		CallbackURL: req.CallbackURL, Status: "processing", Attempts: 1,
		CreatedAt: time.Now(),
	}
	s.subs[req.SubmissionID] = sub
	s.metrics.Submissions++
	s.log.Info("submission recorded", "submission", sub.ID, "scenario", sub.Scenario)
	return sub
}

func (s *Server) scheduleCompletion(sub *submission) {
	completionDelay := s.timing.CompletionDelay
	if sub.Scenario == domain.ScenarioDelayed {
		completionDelay = s.timing.DelayedCompletionDelay
	} else if sub.Scenario == domain.ScenarioTimeoutAmbiguous {
		completionDelay = s.timing.AmbiguousCompletionDelay
	}
	if elapsed := time.Since(sub.CreatedAt); elapsed > completionDelay {
		completionDelay = 0
	} else {
		completionDelay -= elapsed
	}
	id := sub.ID
	go func() {
		time.Sleep(completionDelay)
		s.mu.Lock()
		current, ok := s.subs[id]
		if !ok {
			s.mu.Unlock()
			return
		}
		if current.Status == "processing" {
			oldStatus, oldOutcome := current.Status, current.Outcome
			current.Status = "completed"
			current.Outcome = "accepted"
			if err := s.persistDurablyLocked(); err != nil {
				current.Status, current.Outcome = oldStatus, oldOutcome
				s.log.Error("persist provider completion", "submission", id, "err", err)
				s.mu.Unlock()
				return
			}
		}
		if current.CallbackFinished {
			s.mu.Unlock()
			return
		}
		scenario, url := current.Scenario, current.CallbackURL
		s.mu.Unlock()
		if scenario == domain.ScenarioLateCallback {
			time.Sleep(s.timing.LateCallbackDelay)
		}

		delivered := true
		switch scenario {
		case domain.ScenarioNoCallbackDone:
			// Persisting the omission prevents restart from inventing a callback.
		case domain.ScenarioDupCallback:
			cb := domain.Callback{CallbackID: id + "-cb1", SubmissionID: id, Outcome: "accepted"}
			delivered = s.sendCallback(url, cb)
			delivered = s.sendCallback(url, cb) && delivered
			delivered = s.sendCallback(url, domain.Callback{
				CallbackID: id + "-cb2", SubmissionID: id, Outcome: "accepted"}) && delivered
		default:
			delivered = s.sendCallback(url, domain.Callback{
				CallbackID: id + "-cb1", SubmissionID: id, Outcome: "accepted"})
		}
		if delivered {
			s.mu.Lock()
			if current := s.subs[id]; current != nil {
				current.CallbackFinished = true
				if err := s.persistDurablyLocked(); err != nil {
					current.CallbackFinished = false
					s.log.Error("persist callback completion", "submission", id, "err", err)
				}
			}
			s.mu.Unlock()
		}
	}()
}

func (s *Server) sendCallback(url string, cb domain.Callback) bool {
	body, _ := json.Marshal(cb)
	for attempt := 1; attempt <= 10; attempt++ {
		resp, err := s.callbackHTTP.Post(url, "application/json", bytes.NewReader(body))
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode < 300 {
				s.mu.Lock()
				s.metrics.CallbacksSent++
				if err := s.persistDurablyLocked(); err != nil {
					s.metrics.CallbacksSent--
					s.log.Error("persist callback metric", "callback", cb.CallbackID, "err", err)
					s.mu.Unlock()
					return false
				}
				s.mu.Unlock()
				s.log.Info("callback delivered", "callback", cb.CallbackID, "attempt", attempt)
				return true
			}
		}
		time.Sleep(time.Duration(attempt) * time.Second)
	}
	s.log.Error("callback delivery gave up", "callback", cb.CallbackID)
	return false
}

type StatusResponse struct {
	SubmissionID string `json:"submission_id"`
	Status       string `json:"status"`
	Outcome      string `json:"outcome,omitempty"`
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	s.mu.Lock()
	sub, ok := s.subs[id]
	var resp StatusResponse
	if !ok {
		resp = StatusResponse{SubmissionID: id, Status: "not_found"}
	} else {
		resp = StatusResponse{SubmissionID: id, Status: sub.Status, Outcome: sub.Outcome}
	}
	s.mu.Unlock()
	writeJSON(w, resp)
}

func statusFromOutcome(sub *submission) string {
	if sub.Status == "completed" && sub.Outcome == "rejected" {
		return "rejected"
	}
	return "accepted"
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

type Client struct {
	BaseURL string
	HTTP    *http.Client
}

// Only proven no-effect failures use this type because ambiguous failures must
// be resolved by status lookup.
type RetryableError struct{ Msg string }

func (e *RetryableError) Error() string { return e.Msg }

type SubmitOutcome struct {
	Result string `json:"result"`
	Detail string `json:"detail,omitempty"`
}

func (c *Client) Submit(ctx context.Context, req SubmitRequest) (SubmitOutcome, error) {
	body, _ := json.Marshal(req)
	httpReq, _ := http.NewRequestWithContext(ctx, http.MethodPost,
		c.BaseURL+"/submissions", bytes.NewReader(body))
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := c.HTTP.Do(httpReq)
	if err != nil {
		// A transport failure may follow provider persistence, so blind retry is
		// unsafe.
		return SubmitOutcome{Result: "ambiguous", Detail: err.Error()}, nil
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode == http.StatusTooManyRequests:
		return SubmitOutcome{}, &RetryableError{Msg: "authority rate limited (429)"}
	case resp.StatusCode == http.StatusGatewayTimeout:
		// The gateway timeout cannot prove whether the provider persisted work.
		return SubmitOutcome{Result: "ambiguous", Detail: "gateway timeout"}, nil
	case resp.StatusCode >= 500:
		if resp.Header.Get("X-Request-Effect") == "none" {
			return SubmitOutcome{}, &RetryableError{Msg: fmt.Sprintf("authority no-effect 5xx: %d", resp.StatusCode)}
		}
		return SubmitOutcome{Result: "ambiguous", Detail: fmt.Sprintf("authority ambiguous 5xx: %d", resp.StatusCode)}, nil
	}
	var sr SubmitResponse
	if err := json.NewDecoder(resp.Body).Decode(&sr); err != nil {
		return SubmitOutcome{Result: "ambiguous", Detail: "unreadable response"}, nil
	}
	return SubmitOutcome{Result: sr.Status, Detail: sr.Detail}, nil
}

func (c *Client) Status(ctx context.Context, id string) (StatusResponse, error) {
	return c.getStatus(ctx, c.BaseURL+"/submissions/"+id)
}

func (c *Client) getStatus(ctx context.Context, url string) (StatusResponse, error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return StatusResponse{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 500 {
		return StatusResponse{}, fmt.Errorf("authority status lookup HTTP %d", resp.StatusCode)
	}
	if resp.StatusCode != http.StatusOK {
		return StatusResponse{}, fmt.Errorf("authority status lookup unexpected HTTP %d", resp.StatusCode)
	}
	var sr StatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&sr); err != nil {
		return StatusResponse{}, err
	}
	return sr, nil
}
