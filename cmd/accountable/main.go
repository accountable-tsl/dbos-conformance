package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"

	"github.com/accountable/dbos-conformance/internal/accountable"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil)).With("svc", "accountable")
	dbURL := envOr("ACCOUNTABLE_DB_URL", "postgres://postgres:accountable@localhost:5434/accountable?sslmode=disable")
	addr := envOr("ACCOUNTABLE_ADDR", ":8081")

	srv, err := accountable.NewServer(context.Background(), dbURL, log)
	if err != nil {
		log.Error("startup failed", "err", err)
		os.Exit(1)
	}
	log.Info("listening", "addr", addr)
	if err := http.ListenAndServe(addr, srv.Handler()); err != nil {
		log.Error("server exited", "err", err)
		os.Exit(1)
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
