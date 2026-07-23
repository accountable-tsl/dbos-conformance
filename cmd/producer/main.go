package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/accountable/dbos-conformance/internal/domain"
)

func main() {
	filing := flag.String("filing", "", "filing ID (required)")
	year := flag.Int("year", 2025, "tax year")
	scenario := flag.String("scenario", domain.ScenarioOK, "authority scenario")
	dup := flag.Int("dup", 1, "publish the event this many times")
	flag.Parse()
	if *filing == "" {
		fmt.Fprintln(os.Stderr, "-filing is required")
		os.Exit(2)
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(envOr("KAFKA_BROKERS", "localhost:19092")),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, "kafka:", err)
		os.Exit(1)
	}
	defer client.Close()

	ev := domain.FilingEvent{
		EventID:  fmt.Sprintf("evt-%s-%d", *filing, time.Now().UnixNano()),
		FilingID: *filing, TaxYear: *year, Scenario: *scenario,
	}
	value, _ := json.Marshal(ev)
	topic := envOr("KAFKA_TOPIC", "filing-events")
	for i := 0; i < *dup; i++ {
		rec := &kgo.Record{Topic: topic, Key: []byte(*filing), Value: value}
		if err := client.ProduceSync(context.Background(), rec).FirstErr(); err != nil {
			fmt.Fprintln(os.Stderr, "produce:", err)
			os.Exit(1)
		}
	}
	fmt.Printf("published %s ×%d (scenario=%s) to %s\n", ev.EventID, *dup, *scenario, topic)
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
