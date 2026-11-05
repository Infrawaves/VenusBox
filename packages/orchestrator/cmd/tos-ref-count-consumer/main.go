// tos-ref-count-consumer is a standalone, dev-validation consumer for the TOS
// layer reference-counting design. It drains tosLayerEvent payloads pushed by
// orchestrator's upload paths (packages/orchestrator/pkg/sandbox/uploads.go,
// publishTOSLayerEvent) from a Redis List and persists the dependency edges
// and ancestor reference counts to Postgres.
//
// This is a throwaway validation tool, not production hardening: it uses a
// plain Redis List (BRPOP), which has no redelivery guarantee — if this
// process crashes after BRPOP pops an event but before the DB transaction
// commits, that event is gone. The write logic itself (idempotent edge
// insert gating the +1) is written to tolerate redelivery from a more
// reliable queue later; it is not written to tolerate the List's silent
// loss, which is a separate, deliberate limitation of this dev run.
//
// Configuration via environment variables:
//
//	REDIS_URL                     Redis address (default "localhost:6379")
//	POSTGRES_CONNECTION_STRING    Postgres DSN
//
// Usage:
//
//	tos-ref-count-consumer
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

const tosLayerEventsListKey = "tos.layer.ref.events"

// tosLayerEvent mirrors packages/orchestrator/pkg/sandbox/uploads.go's
// tosLayerEvent. Kept as an independent copy here rather than importing the
// orchestrator package, since this is a standalone dev tool and the
// orchestrator module pulls in a much heavier dependency tree (Firecracker,
// NBD, etc.) that this consumer has no need for.
type tosLayerEvent struct {
	BuildID     string   `json:"build_id"`
	FileType    string   `json:"file_type"`
	AncestorIDs []string `json:"ancestor_ids"`
	EventType   string   `json:"event_type"`
	Timestamp   int64    `json:"timestamp"`
}

func main() {
	redisURL := getenv("REDIS_URL", "localhost:6379")
	pgDSN := os.Getenv("POSTGRES_CONNECTION_STRING")
	if pgDSN == "" {
		log.Fatal("POSTGRES_CONNECTION_STRING is required")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	rdb := redis.NewClient(&redis.Options{Addr: redisURL})
	defer rdb.Close()

	pool, err := pgxpool.New(ctx, pgDSN)
	if err != nil {
		log.Fatalf("connect postgres: %s", err)
	}
	defer pool.Close()

	log.Printf("tos-ref-count-consumer started, redis=%s list=%s", redisURL, tosLayerEventsListKey)

	for {
		select {
		case <-ctx.Done():
			log.Println("shutting down")

			return
		default:
		}

		// BRPOP blocks for up to 5s so the loop can observe ctx cancellation
		// between pops instead of blocking indefinitely.
		res, err := rdb.BRPop(ctx, 5*time.Second, tosLayerEventsListKey).Result()
		if errors.Is(err, redis.Nil) {
			continue // timeout, no event — loop back and recheck ctx
		}
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("brpop error: %s", err)
			time.Sleep(time.Second)

			continue
		}

		// res[0] is the key name, res[1] is the popped value.
		if err := handleEvent(ctx, pool, res[1]); err != nil {
			log.Printf("handle event failed, dropping (dev tool, no redelivery): %s", err)
		}
	}
}

func handleEvent(ctx context.Context, pool *pgxpool.Pool, payload string) error {
	var event tosLayerEvent
	if err := json.Unmarshal([]byte(payload), &event); err != nil {
		return fmt.Errorf("unmarshal event: %w", err)
	}

	buildID, err := uuid.Parse(event.BuildID)
	if err != nil {
		return fmt.Errorf("parse build_id %q: %w", event.BuildID, err)
	}

	ancestorIDs := make([]uuid.UUID, 0, len(event.AncestorIDs))
	for _, s := range event.AncestorIDs {
		id, err := uuid.Parse(s)
		if err != nil {
			return fmt.Errorf("parse ancestor_id %q: %w", s, err)
		}
		if id == buildID {
			continue // a header's own entry in Mapping.Builds() is not an ancestor
		}
		ancestorIDs = append(ancestorIDs, id)
	}

	switch event.EventType {
	case "upload":
		return applyUploadEvent(ctx, pool, buildID, event.FileType, ancestorIDs)
	default:
		return fmt.Errorf("unknown event_type %q", event.EventType)
	}
}

// applyUploadEvent registers buildID itself in tos_ancestor_reference_count
// (ref_count defaulting to 0 if it has no row yet — every build that lands
// in TOS must be queryable there, not just builds that happen to already
// have a downstream dependent), then records build_id -> each ancestor_id as
// an edge and bumps ref_count only for edges that did not already exist —
// this is what makes the +1 tolerant of the same event being processed more
// than once (not a concern for this List-based dev run, but kept correct for
// when this logic moves onto a redelivering queue).
func applyUploadEvent(ctx context.Context, pool *pgxpool.Pool, buildID uuid.UUID, fileType string, ancestorIDs []uuid.UUID) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback is a no-op after Commit

	if _, err := tx.Exec(ctx, `
		INSERT INTO tos_ancestor_reference_count (ancestor_id, ref_count)
		VALUES ($1, 0)
		ON CONFLICT (ancestor_id) DO NOTHING
	`, buildID); err != nil {
		return fmt.Errorf("register build %s in reference count table: %w", buildID, err)
	}

	for _, ancestorID := range ancestorIDs {
		var inserted bool
		err := tx.QueryRow(ctx, `
			INSERT INTO tos_build_edges (build_id, ancestor_id, source)
			VALUES ($1, $2, 'live_upload')
			ON CONFLICT (build_id, ancestor_id) DO NOTHING
			RETURNING true
		`, buildID, ancestorID).Scan(&inserted)

		switch {
		case errors.Is(err, pgx.ErrNoRows):
			// Edge already existed — this (build_id, ancestor_id) pair was
			// already recorded by a prior delivery of this same event (or an
			// earlier file_type's call for the same build/ancestor pair).
			// Do not bump ref_count again.
			continue
		case err != nil:
			return fmt.Errorf("insert edge %s -> %s: %w", buildID, ancestorID, err)
		}

		if _, err := tx.Exec(ctx, `
			INSERT INTO tos_ancestor_reference_count (ancestor_id, ref_count)
			VALUES ($1, 1)
			ON CONFLICT (ancestor_id) DO UPDATE
			SET ref_count = tos_ancestor_reference_count.ref_count + 1,
			    updated_at = now()
		`, ancestorID); err != nil {
			return fmt.Errorf("bump ref_count for %s: %w", ancestorID, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}

	log.Printf("build=%s file_type=%s ancestors=%d edges processed", buildID, fileType, len(ancestorIDs))

	return nil
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}

	return fallback
}
