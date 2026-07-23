// DBOS never writes to authoritative Postgres directly because deterministic
// domain commands keep replay visible and idempotent.
package accountable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/accountable/dbos-conformance/internal/domain"
)

const schema = `
CREATE TABLE IF NOT EXISTS filings (
	filing_id  text NOT NULL,
    tax_year   int  NOT NULL,
    scenario   text NOT NULL DEFAULT 'ok',
    state      text NOT NULL DEFAULT 'ready',
	provider_operation_id text,
	PRIMARY KEY (filing_id, tax_year),
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS filings_provider_operation_id_idx
	ON filings (provider_operation_id) WHERE provider_operation_id IS NOT NULL;
CREATE TABLE IF NOT EXISTS domain_commands (
    idempotency_key    text PRIMARY KEY,
    command_type       text NOT NULL,
	filing_id          text NOT NULL,
	tax_year           int NOT NULL DEFAULT 0,
    payload            jsonb,
    result             jsonb,
    duplicate_attempts int NOT NULL DEFAULT 0,
    created_at         timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS workflow_start_conflicts (
	event_id text PRIMARY KEY,
	filing_id text NOT NULL,
	tax_year int NOT NULL,
	existing_input jsonb NOT NULL,
	requested_input jsonb NOT NULL,
	created_at timestamptz NOT NULL DEFAULT now()
);
`

type Server struct {
	pool *pgxpool.Pool
	log  *slog.Logger
}

func NewServer(ctx context.Context, dbURL string, log *slog.Logger) (*Server, error) {
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		return nil, err
	}
	if _, err := pool.Exec(ctx, schema); err != nil {
		return nil, fmt.Errorf("apply schema: %w", err)
	}
	return &Server{pool: pool, log: log}, nil
}

type CommandRequest struct {
	IdempotencyKey      string `json:"idempotency_key"`
	CommandType         string `json:"command_type"`
	FilingID            string `json:"filing_id"`
	TaxYear             int    `json:"tax_year"`
	ProviderOperationID string `json:"provider_operation_id,omitempty"`
	Detail              string `json:"detail,omitempty"`
}

type CommandResult struct {
	FilingID            string `json:"filing_id"`
	NewState            string `json:"new_state"`
	ProviderOperationID string `json:"provider_operation_id,omitempty"`
	Applied             bool   `json:"applied"`
	DuplicateAttempts   int    `json:"duplicate_attempts"`
}

type WorkflowStartConflict struct {
	EventID        string          `json:"event_id"`
	FilingID       string          `json:"filing_id"`
	TaxYear        int             `json:"tax_year"`
	ExistingInput  json.RawMessage `json:"existing_input"`
	RequestedInput json.RawMessage `json:"requested_input"`
}

var commandTransitions = map[string]string{
	"PrepareFilingSubmission": domain.StateSubmitting,
	"MarkFilingSubmitted":     domain.StateSubmitted,
	"MarkFilingAccepted":      domain.StateAccepted,
	"MarkFilingRejected":      domain.StateRejected,
	"MarkFilingNeedsReview":   domain.StateNeedsReview,
}

type conflictError struct{ message string }

func (e *conflictError) Error() string { return e.message }

func commandAllowsState(commandType, state string) bool {
	switch commandType {
	case "PrepareFilingSubmission":
		return state == domain.StateReady || state == domain.StateSubmitting
	case "MarkFilingSubmitted":
		return state == domain.StateSubmitting
	case "MarkFilingAccepted", "MarkFilingRejected", "MarkFilingNeedsReview":
		return state == domain.StateSubmitting || state == domain.StateSubmitted || state == domain.StateNeedsReview
	default:
		return false
	}
}

// The command and transition share a transaction so a replay can return the
// original result without applying the business effect twice.
func (s *Server) applyCommand(ctx context.Context, req CommandRequest) (CommandResult, error) {
	newState, ok := commandTransitions[req.CommandType]
	if !ok {
		return CommandResult{}, fmt.Errorf("unknown command type %q", req.CommandType)
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return CommandResult{}, err
	}
	defer tx.Rollback(ctx)

	var stored []byte
	var storedPayload []byte
	var storedType, storedFilingID string
	var storedTaxYear int
	err = tx.QueryRow(ctx,
		`SELECT command_type, filing_id, tax_year, payload, result
		 FROM domain_commands WHERE idempotency_key=$1 FOR UPDATE`,
		req.IdempotencyKey).Scan(&storedType, &storedFilingID, &storedTaxYear, &storedPayload, &stored)
	if err == nil {
		var original CommandRequest
		if err := json.Unmarshal(storedPayload, &original); err != nil {
			return CommandResult{}, err
		}
		if storedType != req.CommandType || storedFilingID != req.FilingID ||
			storedTaxYear != req.TaxYear || original != req {
			return CommandResult{}, &conflictError{message: fmt.Sprintf(
				"idempotency key %q was already used for a different command", req.IdempotencyKey)}
		}
		var res CommandResult
		if err := json.Unmarshal(stored, &res); err != nil {
			return CommandResult{}, err
		}
		res.Applied = false
		row := tx.QueryRow(ctx,
			`UPDATE domain_commands SET duplicate_attempts = duplicate_attempts + 1
			 WHERE idempotency_key=$1 RETURNING duplicate_attempts`, req.IdempotencyKey)
		if err := row.Scan(&res.DuplicateAttempts); err != nil {
			return CommandResult{}, err
		}
		return res, tx.Commit(ctx)
	}
	if err != pgx.ErrNoRows {
		return CommandResult{}, err
	}

	var currentState, providerOperationID string
	if err := tx.QueryRow(ctx,
		`SELECT state, coalesce(provider_operation_id,'') FROM filings
		 WHERE filing_id=$1 AND tax_year=$2 FOR UPDATE`, req.FilingID, req.TaxYear).
		Scan(&currentState, &providerOperationID); err != nil {
		if err == pgx.ErrNoRows {
			return CommandResult{}, fmt.Errorf("filing %s/%d not found", req.FilingID, req.TaxYear)
		}
		return CommandResult{}, err
	}
	if !commandAllowsState(req.CommandType, currentState) {
		return CommandResult{}, &conflictError{message: fmt.Sprintf(
			"command %s cannot transition filing %s/%d from %s",
			req.CommandType, req.FilingID, req.TaxYear, currentState)}
	}

	updateSQL := `UPDATE filings SET state=$1, updated_at=now()
		WHERE filing_id=$2 AND tax_year=$3`
	updateArgs := []any{newState, req.FilingID, req.TaxYear}
	if req.CommandType == "PrepareFilingSubmission" {
		if req.ProviderOperationID == "" {
			return CommandResult{}, fmt.Errorf("provider operation ID is required")
		}
		updateSQL = `UPDATE filings
			SET state=$1, provider_operation_id=$2, updated_at=now()
			WHERE filing_id=$3 AND tax_year=$4
			  AND state IN ('ready','submitting')
			  AND (provider_operation_id IS NULL OR provider_operation_id=$2)`
		updateArgs = []any{newState, req.ProviderOperationID, req.FilingID, req.TaxYear}
		providerOperationID = req.ProviderOperationID
	}
	tag, err := tx.Exec(ctx, updateSQL, updateArgs...)
	if err != nil {
		return CommandResult{}, err
	}
	if tag.RowsAffected() == 0 {
		return CommandResult{}, &conflictError{message: fmt.Sprintf(
			"filing %s/%d no longer permits %s", req.FilingID, req.TaxYear, req.CommandType)}
	}
	res := CommandResult{FilingID: req.FilingID, NewState: newState,
		ProviderOperationID: providerOperationID, Applied: true}
	resJSON, _ := json.Marshal(res)
	payload, _ := json.Marshal(req)
	if _, err := tx.Exec(ctx,
		`INSERT INTO domain_commands (idempotency_key, command_type, filing_id, tax_year, payload, result)
		 VALUES ($1,$2,$3,$4,$5,$6)`,
		req.IdempotencyKey, req.CommandType, req.FilingID, req.TaxYear, payload, resJSON); err != nil {
		return CommandResult{}, err
	}
	return res, tx.Commit(ctx)
}

type Filing struct {
	FilingID            string    `json:"filing_id"`
	TaxYear             int       `json:"tax_year"`
	Scenario            string    `json:"scenario"`
	State               string    `json:"state"`
	ProviderOperationID string    `json:"provider_operation_id,omitempty"`
	UpdatedAt           time.Time `json:"updated_at"`
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /commands", func(w http.ResponseWriter, r *http.Request) {
		var req CommandRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.IdempotencyKey == "" {
			http.Error(w, "bad command", http.StatusBadRequest)
			return
		}
		res, err := s.applyCommand(r.Context(), req)
		if err != nil {
			s.log.Error("command failed", "err", err, "key", req.IdempotencyKey)
			var conflict *conflictError
			if errors.As(err, &conflict) {
				http.Error(w, err.Error(), http.StatusConflict)
				return
			}
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		s.log.Info("command", "type", req.CommandType, "filing", req.FilingID,
			"applied", res.Applied, "dups", res.DuplicateAttempts)
		writeJSON(w, res)
	})

	mux.HandleFunc("POST /workflow-start-conflicts", func(w http.ResponseWriter, r *http.Request) {
		var conflict WorkflowStartConflict
		if err := json.NewDecoder(r.Body).Decode(&conflict); err != nil ||
			conflict.EventID == "" || conflict.FilingID == "" || conflict.TaxYear == 0 ||
			len(conflict.ExistingInput) == 0 || len(conflict.RequestedInput) == 0 {
			http.Error(w, "bad workflow-start conflict", http.StatusBadRequest)
			return
		}
		_, err := s.pool.Exec(r.Context(), `INSERT INTO workflow_start_conflicts
			(event_id,filing_id,tax_year,existing_input,requested_input)
			VALUES($1,$2,$3,$4,$5) ON CONFLICT(event_id) DO NOTHING`,
			conflict.EventID, conflict.FilingID, conflict.TaxYear,
			string(conflict.ExistingInput), string(conflict.RequestedInput))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	})

	mux.HandleFunc("POST /seed", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Prefix   string `json:"prefix"`
			Count    int    `json:"count"`
			TaxYear  int    `json:"tax_year"`
			Scenario string `json:"scenario"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if req.Count == 0 {
			req.Count = 1
		}
		if req.Scenario == "" {
			req.Scenario = domain.ScenarioOK
		}
		ids := []string{}
		for i := 1; i <= req.Count; i++ {
			id := fmt.Sprintf("%s%d", req.Prefix, i)
			_, err := s.pool.Exec(r.Context(),
				`INSERT INTO filings (filing_id, tax_year, scenario, state) VALUES ($1,$2,$3,'ready')
					 ON CONFLICT (filing_id, tax_year) DO UPDATE SET state='ready', scenario=$3,
					 provider_operation_id=NULL, updated_at=now()`,
				id, req.TaxYear, req.Scenario)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			ids = append(ids, id)
		}
		writeJSON(w, map[string]any{"seeded": ids})
	})

	mux.HandleFunc("PUT /filings/{id}", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			TaxYear  int    `json:"tax_year"`
			Scenario string `json:"scenario"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if req.Scenario == "" {
			req.Scenario = domain.ScenarioOK
		}
		_, err := s.pool.Exec(r.Context(),
			`INSERT INTO filings (filing_id, tax_year, scenario, state) VALUES ($1,$2,$3,'ready')
				 ON CONFLICT (filing_id, tax_year) DO UPDATE SET state='ready', scenario=$3,
				 provider_operation_id=NULL, updated_at=now()`,
			r.PathValue("id"), req.TaxYear, req.Scenario)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, map[string]string{"filing_id": r.PathValue("id"), "state": "ready"})
	})

	mux.HandleFunc("GET /filings/{id}", func(w http.ResponseWriter, r *http.Request) {
		var f Filing
		year := r.URL.Query().Get("tax_year")
		if year == "" {
			http.Error(w, "tax_year is required", http.StatusBadRequest)
			return
		}
		q := `SELECT filing_id, tax_year, scenario, state,
			coalesce(provider_operation_id,''), updated_at
			FROM filings WHERE filing_id=$1 AND tax_year=$2`
		args := []any{r.PathValue("id"), year}
		err := s.pool.QueryRow(r.Context(), q, args...).
			Scan(&f.FilingID, &f.TaxYear, &f.Scenario, &f.State,
				&f.ProviderOperationID, &f.UpdatedAt)
		if err == pgx.ErrNoRows {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, f)
	})

	mux.HandleFunc("GET /filings", func(w http.ResponseWriter, r *http.Request) {
		q := `SELECT filing_id, tax_year, scenario, state, coalesce(provider_operation_id,''),
			updated_at FROM filings WHERE 1=1`
		args := []any{}
		if st := r.URL.Query().Get("state"); st != "" {
			args = append(args, st)
			q += ` AND state=$` + strconv.Itoa(len(args))
		}
		if r.URL.Query().Get("nonterminal") != "" {
			q += ` AND state IN ('ready','submitting','submitted')`
		}
		if ss := r.URL.Query().Get("stale_seconds"); ss != "" {
			args = append(args, ss)
			q += ` AND updated_at < now() - ($` + strconv.Itoa(len(args)) + ` || ' seconds')::interval`
		}
		q += ` ORDER BY filing_id`
		rows, err := s.pool.Query(r.Context(), q, args...)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer rows.Close()
		out := []Filing{}
		for rows.Next() {
			var f Filing
			if err := rows.Scan(&f.FilingID, &f.TaxYear, &f.Scenario, &f.State,
				&f.ProviderOperationID, &f.UpdatedAt); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			out = append(out, f)
		}
		if err := rows.Err(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, out)
	})

	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		m := map[string]any{}
		var totalCmds, totalDups int
		if err := s.pool.QueryRow(r.Context(),
			`SELECT count(*), coalesce(sum(duplicate_attempts),0) FROM domain_commands`).
			Scan(&totalCmds, &totalDups); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		m["commands_applied"] = totalCmds
		m["duplicate_command_attempts"] = totalDups
		states := map[string]int{}
		rows, err := s.pool.Query(r.Context(), `SELECT state, count(*) FROM filings GROUP BY state`)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer rows.Close()
		for rows.Next() {
			var st string
			var c int
			if err := rows.Scan(&st, &c); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			states[st] = c
		}
		if err := rows.Err(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		m["filings_by_state"] = states
		var startConflicts int
		if err := s.pool.QueryRow(r.Context(),
			`SELECT count(*) FROM workflow_start_conflicts`).Scan(&startConflicts); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		m["workflow_start_conflicts"] = startConflicts
		writeJSON(w, m)
	})

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	return mux
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

type Client struct {
	BaseURL string
	HTTP    *http.Client
}

func (c *Client) GetFiling(ctx context.Context, filingID string, taxYear int) (*Filing, error) {
	url := fmt.Sprintf("%s/filings/%s?tax_year=%d", c.BaseURL, filingID, taxYear)
	req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("filing %s not found in accountable", filingID)
	}
	var f Filing
	if err := json.NewDecoder(resp.Body).Decode(&f); err != nil {
		return nil, err
	}
	return &f, nil
}

func (c *Client) ApplyCommand(ctx context.Context, cmd CommandRequest) (*CommandResult, error) {
	body, _ := json.Marshal(cmd)
	req, _ := http.NewRequestWithContext(ctx, "POST", c.BaseURL+"/commands", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("command %s failed: HTTP %d", cmd.CommandType, resp.StatusCode)
	}
	var res CommandResult
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, err
	}
	return &res, nil
}

func (c *Client) RecordWorkflowStartConflict(ctx context.Context, conflict WorkflowStartConflict) error {
	body, _ := json.Marshal(conflict)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost,
		c.BaseURL+"/workflow-start-conflicts", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("record workflow-start conflict failed: HTTP %d", resp.StatusCode)
	}
	return nil
}

func (c *Client) ListFilings(ctx context.Context, query string) ([]Filing, error) {
	req, _ := http.NewRequestWithContext(ctx, "GET", c.BaseURL+"/filings?"+query, nil)
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var out []Filing
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return out, nil
}
