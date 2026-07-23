package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/accountable/dbos-conformance/internal/authority"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil)).With("svc", "authority")
	addr := envOr("AUTHORITY_ADDR", ":8082")
	srv, err := authority.NewPersistentServer(context.Background(),
		envOr("AUTHORITY_DB_URL", "postgres://postgres:authority@localhost:5444/authority?sslmode=disable"),
		authority.Timing{
			CompletionDelay:          envDuration("AUTHORITY_COMPLETION_DELAY", 2*time.Second),
			AmbiguousCompletionDelay: envDuration("AUTHORITY_AMBIGUOUS_COMPLETION_DELAY", 2*time.Second),
			DelayedCompletionDelay:   envDuration("AUTHORITY_DELAYED_COMPLETION_DELAY", 25*time.Second),
			LateCallbackDelay:        envDuration("AUTHORITY_LATE_CALLBACK_DELAY", 23*time.Second),
			LostResponseDelay:        envDuration("AUTHORITY_LOST_RESPONSE_DELAY", 8*time.Second),
		}, log)
	if err != nil {
		log.Error("initialise durable provider ledger", "err", err)
		os.Exit(1)
	}
	defer srv.Close()
	log.Info("listening", "addr", addr)
	if err := http.ListenAndServe(addr, srv.Handler()); err != nil {
		log.Error("server exited", "err", err)
		os.Exit(1)
	}
}

func envDuration(k string, def time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
