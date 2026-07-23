package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/accountable/dbos-conformance/internal/accountable"
	"github.com/accountable/dbos-conformance/internal/chaos"
	"github.com/accountable/dbos-conformance/internal/domain"
	"github.com/accountable/dbos-conformance/internal/workflow"
)

var buildVariant = "v1"

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil)).With("svc", "worker")
	chaos.Init()

	appVersion := envOr("APP_VERSION", buildVariant)
	dbosCtx, err := dbos.NewDBOSContext(context.Background(), dbos.Config{
		AppName:            "accountable-filing",
		DatabaseURL:        envOr("DBOS_DATABASE_URL", "postgres://postgres:dbos@localhost:5344/dbos?sslmode=disable"),
		EnablePatching:     true,
		ApplicationVersion: appVersion,
		ExecutorID:         envOr("EXECUTOR_ID", "conformance-worker-1"),
		Logger:             log,
	})
	if err != nil {
		fatal(log, "dbos init", err)
	}
	defer dbos.Shutdown(dbosCtx, 10*time.Second)

	accountableURL := envOr("ACCOUNTABLE_URL", "http://localhost:8081")
	accountableClient := &accountable.Client{
		BaseURL: accountableURL, HTTP: &http.Client{Timeout: 5 * time.Second},
	}
	workflow.Configure(workflow.Config{
		AccountableURL:        accountableURL,
		AuthorityURL:          envOr("AUTHORITY_URL", "http://localhost:8082"),
		CallbackURL:           envOr("CALLBACK_URL", "http://localhost:8080/callbacks"),
		V2:                    buildVariant == "v2" || os.Getenv("V2_ENABLED") == "1",
		RecvTimeout:           envDuration("RECV_TIMEOUT", 20*time.Second),
		StatusPolls:           envInt("STATUS_POLLS", 3),
		HTTPTimeout:           envDuration("WORKFLOW_HTTP_TIMEOUT", 5*time.Second),
		SubmitRetryBase:       envDuration("SUBMIT_RETRY_BASE_INTERVAL", 500*time.Millisecond),
		ReconcileStaleSeconds: envInt("RECONCILE_STALE_SECONDS", 30),
	})

	concurrency, _ := strconv.Atoi(envOr("QUEUE_WORKER_CONCURRENCY", "3"))
	queueOpts := []dbos.QueueOption{
		dbos.WithWorkerConcurrency(concurrency),
	}
	if _, err := dbos.RegisterQueue(dbosCtx, workflow.QueueName, queueOpts...); err != nil {
		fatal(log, "register queue", err)
	}

	dbos.RegisterWorkflow(dbosCtx, workflow.Filing,
		dbos.WithWorkflowName(workflow.FilingWorkflowName),
		dbos.WithMaxRetries(20))

	// Crash tests disable reconciliation so they isolate DBOS recovery instead
	// of being rescued by the product safety net.
	if os.Getenv("RECONCILER") != "0" {
		dbos.RegisterWorkflow(dbosCtx, workflow.Reconcile,
			dbos.WithWorkflowName(workflow.ReconcileWorkflowName),
			dbos.WithSchedule(envOr("RECONCILE_CRON", "*/30 * * * * *")))
	}

	if err := dbos.Launch(dbosCtx); err != nil {
		fatal(log, "dbos launch", err)
	}
	log.Info("dbos launched", "app_version", appVersion, "build_variant", buildVariant,
		"queue_concurrency", concurrency)

	go serveCallbacks(dbosCtx, log)
	go consumeEvents(dbosCtx, accountableClient, log)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Info("shutting down")
}

func serveCallbacks(dbosCtx dbos.DBOSContext, log *slog.Logger) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /callbacks", func(w http.ResponseWriter, r *http.Request) {
		var cb domain.Callback
		if err := json.NewDecoder(r.Body).Decode(&cb); err != nil || cb.SubmissionID == "" || cb.CallbackID == "" {
			http.Error(w, "bad callback", http.StatusBadRequest)
			return
		}
		workflowID := cb.SubmissionID
		err := dbos.Send(dbosCtx, workflowID, cb, domain.CallbackTopic,
			dbos.WithIdempotencyKey(cb.CallbackID))
		if err != nil {
			var dbosErr *dbos.DBOSError
			if errors.As(err, &dbosErr) && dbosErr.Code == dbos.NonExistentWorkflowError {
				log.Warn("callback for unknown workflow — asking authority to retry",
					"submission", cb.SubmissionID)
				http.Error(w, "workflow not yet started", http.StatusServiceUnavailable)
				return
			}
			log.Error("send failed", "err", err, "callback", cb.CallbackID)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		log.Info("callback signalled", "workflow", workflowID, "operation", cb.SubmissionID,
			"callback", cb.CallbackID)
		w.WriteHeader(http.StatusAccepted)
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	addr := envOr("WORKER_ADDR", ":8080")
	log.Info("callback endpoint listening", "addr", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		fatal(log, "callback server", err)
	}
}

func consumeEvents(dbosCtx dbos.DBOSContext, acct *accountable.Client, log *slog.Logger) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(envOr("KAFKA_BROKERS", "localhost:19092")),
		kgo.ConsumerGroup(envOr("KAFKA_GROUP", "filing-worker")),
		kgo.ConsumeTopics(envOr("KAFKA_TOPIC", "filing-events")),
		kgo.DisableAutoCommit(),
		// Static membership and a short timeout prevent repeated kill -9 tests
		// from waiting on the dead member's full session.
		kgo.InstanceID(envOr("KAFKA_INSTANCE_ID", "conformance-worker-1")),
		kgo.SessionTimeout(6*time.Second),
	)
	if err != nil {
		fatal(log, "kafka client", err)
	}
	defer client.Close()
	log.Info("consuming filing events")

	for {
		fetches := client.PollFetches(context.Background())
		if fetches.IsClientClosed() {
			return
		}
		fetches.EachError(func(t string, p int32, err error) {
			log.Error("fetch error", "topic", t, "partition", p, "err", err)
		})
		var processed []*kgo.Record
		blocked := map[topicPartition]bool{}
		fetches.EachRecord(func(rec *kgo.Record) {
			partition := topicPartition{topic: rec.Topic, partition: rec.Partition}
			if blocked[partition] {
				return // preserve a contiguous committed prefix for this partition
			}
			var ev domain.FilingEvent
			if err := json.Unmarshal(rec.Value, &ev); err != nil {
				log.Error("bad event skipped", "err", err)
				processed = append(processed, rec)
				return
			}
			wfID := domain.WorkflowID(ev.FilingID, ev.TaxYear)
			opts := []dbos.WorkflowOption{
				dbos.WithWorkflowID(wfID),
				dbos.WithQueue(workflow.QueueName),
			}
			requested := domain.FilingInput{FilingID: ev.FilingID, TaxYear: ev.TaxYear, Scenario: ev.Scenario}
			_, err := dbos.RunWorkflow(dbosCtx, workflow.Filing, requested, opts...)
			if err != nil {
				var dbosErr *dbos.DBOSError
				if !errors.As(err, &dbosErr) || dbosErr.Code != dbos.ConflictingWorkflowError {
					log.Error("workflow start failed — leaving event uncommitted",
						"workflow", wfID, "err", err)
					blocked[partition] = true
					return // do not mark processed; redelivered after restart
				}
			}

			// DBOS v0.20 may return an existing handle without a conflict, so the
			// persisted input must be checked before acknowledging the event.
			workflows, lookupErr := dbos.ListWorkflows(dbosCtx,
				dbos.WithWorkflowIDs([]string{wfID}), dbos.WithLoadInput(true), dbos.WithLimit(1))
			if lookupErr != nil || len(workflows) != 1 {
				log.Error("cannot verify persisted workflow input — leaving event uncommitted",
					"workflow", wfID, "err", lookupErr)
				blocked[partition] = true
				return
			}
			existing, decodeErr := domain.DecodeFilingInput(workflows[0].Input)
			if decodeErr != nil {
				log.Error("cannot decode persisted workflow input — leaving event uncommitted",
					"workflow", wfID, "err", decodeErr)
				blocked[partition] = true
				return
			}
			if existing != requested {
				existingJSON, _ := json.Marshal(existing)
				requestedJSON, _ := json.Marshal(requested)
				conflictErr := acct.RecordWorkflowStartConflict(context.Background(),
					accountable.WorkflowStartConflict{
						EventID: ev.EventID, FilingID: ev.FilingID, TaxYear: ev.TaxYear,
						ExistingInput: existingJSON, RequestedInput: requestedJSON,
					})
				if conflictErr != nil {
					log.Error("record conflicting start — leaving event uncommitted",
						"workflow", wfID, "err", conflictErr)
					blocked[partition] = true
					return
				}
				log.Error("conflicting duplicate start recorded durably",
					"workflow", wfID, "event", ev.EventID,
					"existing", existing, "requested", requested)
			} else if err != nil {
				log.Info("duplicate event absorbed", "workflow", wfID, "event", ev.EventID)
			} else {
				log.Info("workflow enqueued", "workflow", wfID, "event", ev.EventID, "scenario", ev.Scenario)
			}
			chaos.Crash("after-event-start-before-offset-commit")
			processed = append(processed, rec)
		})
		if len(processed) > 0 {
			chaos.Crash("before-offset-commit")
			if err := client.CommitRecords(context.Background(), processed...); err != nil {
				log.Error("offset commit failed", "err", err)
			} else {
				chaos.Crash("after-offset-commit")
			}
		}
	}
}

type topicPartition struct {
	topic     string
	partition int32
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envDuration(k string, def time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func fatal(log *slog.Logger, what string, err error) {
	log.Error(what+" failed", "err", err)
	os.Exit(1)
}
