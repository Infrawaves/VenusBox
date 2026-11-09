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
	EnvID       string   `json:"env_id"` // envs.id / env_builds.env_id, e.g. "ihvujp327d7kt4rox2l5"; empty for dev tooling
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

	go runSoftDeleteScanner(ctx, pool, 30*time.Second)

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

	// A header's own entry in Mapping.Builds() (buildID appearing in its own
	// ancestor list) is kept, not filtered out — this is the self-reference
	// edge that makes ref_count=0 mean "this template's env was soft-deleted"
	// rather than just "no other build depends on it yet". See the design
	// doc's route B: the self-edge is cleared only by applyTemplateSoftDelete
	// (keyed on env_id), so it can never reach zero while the template is
	// still alive — there's no path that decrements it except that one.
	ancestorIDs := make([]uuid.UUID, 0, len(event.AncestorIDs))
	for _, s := range event.AncestorIDs {
		id, err := uuid.Parse(s)
		if err != nil {
			return fmt.Errorf("parse ancestor_id %q: %w", s, err)
		}
		ancestorIDs = append(ancestorIDs, id)
	}

	switch event.EventType {
	case "upload":
		return applyUploadEvent(ctx, pool, buildID, event.EnvID, event.FileType, ancestorIDs)
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
//
// Route B (self-reference): ancestorIDs includes buildID itself (handleEvent
// no longer filters it out), so the loop below inserts a (buildID, buildID)
// edge like any other, bumping its own ref_count from 0 to 1 on first
// insert. The standalone zero-row insert that used to run here before the
// loop is now redundant for any build whose Mapping.Builds() is non-empty
// (every real build's own entry, per header.go) — kept as a defensive
// no-op (ON CONFLICT DO NOTHING) in case ancestorIDs is ever empty.
func applyUploadEvent(ctx context.Context, pool *pgxpool.Pool, buildID uuid.UUID, envID string, fileType string, ancestorIDs []uuid.UUID) error {
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

	// Informational-only snapshot (see schema.sql) — a lookup failure here
	// must not fail the whole event, so errors are swallowed to a nil
	// timestamp rather than propagated.
	envLastSpawnedAt := lookupEnvLastSpawnedAt(ctx, tx, envID)

	for _, ancestorID := range ancestorIDs {
		var inserted bool
		err := tx.QueryRow(ctx, `
			INSERT INTO tos_build_edges (build_id, ancestor_id, env_id, env_last_spawned_at, source)
			VALUES ($1, $2, $3, $4, 'live_upload')
			ON CONFLICT (build_id, ancestor_id) DO NOTHING
			RETURNING true
		`, buildID, ancestorID, envID, envLastSpawnedAt).Scan(&inserted)

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

// lookupEnvLastSpawnedAt reads envs.last_spawned_at for envID, for the
// informational snapshot stored on the edge (see schema.sql). Returns nil on
// any failure to look it up (empty envID, no matching row, or a query
// error) — this is a best-effort display field, not load-bearing for any
// ref_count logic, so a lookup problem here must never fail the edge write.
func lookupEnvLastSpawnedAt(ctx context.Context, tx pgx.Tx, envID string) *time.Time {
	if envID == "" {
		return nil
	}

	var lastSpawnedAt *time.Time
	if err := tx.QueryRow(ctx, `SELECT last_spawned_at FROM envs WHERE id = $1`, envID).Scan(&lastSpawnedAt); err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			log.Printf("lookup last_spawned_at for env %s failed (informational field only): %s", envID, err)
		}

		return nil
	}

	return lastSpawnedAt
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}

	return fallback
}

// runSoftDeleteScanner periodically reads envs.deleted_at (the production
// template soft-delete timestamp, set by packages/api/internal/handlers/
// template_delete.go — no event hook into that path exists or is needed)
// and clears every edge this consumer has recorded for each newly-deleted
// template, decrementing ref_count accordingly.
//
// Polling envs.deleted_at directly — rather than wiring a delete event out
// of the API — means zero changes to API code: this is a read-only query
// against a timestamp the production delete path already sets.
//
// watermark starts at the scanner's own start time so a restart does not
// re-scan the full historical backlog of already-deleted templates; each
// tick advances it past the newest deleted_at seen. Since the edge-clearing
// UPDATE below is itself idempotent (its WHERE deleted_at IS NULL guard
// means re-processing the same env_id a second time is a no-op), losing
// track of the watermark across a restart only costs a wider re-scan, not
// correctness.
func runSoftDeleteScanner(ctx context.Context, pool *pgxpool.Pool, interval time.Duration) {
	watermark := time.Now().Add(-interval) // catch anything deleted just before startup

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		newWatermark, err := scanSoftDeletedTemplates(ctx, pool, watermark)
		if err != nil {
			log.Printf("soft-delete scan failed, will retry next tick: %s", err)

			continue
		}
		watermark = newWatermark
	}
}

// scanSoftDeletedTemplates finds envs soft-deleted since since, clears their
// edges, and returns the newest deleted_at seen (or since, unchanged, if
// nothing new was found).
func scanSoftDeletedTemplates(ctx context.Context, pool *pgxpool.Pool, since time.Time) (time.Time, error) {
	rows, err := pool.Query(ctx, `
		SELECT id, deleted_at FROM envs
		WHERE deleted_at IS NOT NULL AND deleted_at > $1
		ORDER BY deleted_at ASC
	`, since)
	if err != nil {
		return since, fmt.Errorf("query soft-deleted envs: %w", err)
	}
	defer rows.Close()

	newest := since
	count := 0
	for rows.Next() {
		var envID string
		var deletedAt time.Time
		if err := rows.Scan(&envID, &deletedAt); err != nil {
			return since, fmt.Errorf("scan soft-deleted env row: %w", err)
		}

		if err := applyTemplateSoftDelete(ctx, pool, envID); err != nil {
			// Fail closed: stop advancing the watermark past an env_id that
			// did not get its edges cleared, so the next tick retries it
			// rather than silently skipping it forever.
			return newest, fmt.Errorf("apply soft delete for env %s: %w", envID, err)
		}

		if deletedAt.After(newest) {
			newest = deletedAt
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return since, fmt.Errorf("iterate soft-deleted envs: %w", err)
	}

	if count > 0 {
		log.Printf("soft-delete scan: processed %d newly-deleted template(s)", count)
	}

	return newest, nil
}

// applyTemplateSoftDelete clears every edge recorded under envID (every
// build this template ever produced, not just whatever it currently points
// to — this is the whole point of carrying env_id on the edge instead of
// re-deriving a build history from env_builds/env_build_assignments) and
// decrements ref_count for each ancestor that actually had a live edge
// cleared. The UPDATE's WHERE deleted_at IS NULL guard makes this
// idempotent: re-running it for an env_id already processed finds no rows
// and does nothing.
func applyTemplateSoftDelete(ctx context.Context, pool *pgxpool.Pool, envID string) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback is a no-op after Commit

	rows, err := tx.Query(ctx, `
		UPDATE tos_build_edges
		SET deleted_at = now()
		WHERE env_id = $1 AND deleted_at IS NULL
		RETURNING ancestor_id
	`, envID)
	if err != nil {
		return fmt.Errorf("clear edges for env %s: %w", envID, err)
	}

	var ancestorIDs []uuid.UUID
	for rows.Next() {
		var ancestorID uuid.UUID
		if err := rows.Scan(&ancestorID); err != nil {
			rows.Close()

			return fmt.Errorf("scan cleared edge ancestor_id: %w", err)
		}
		ancestorIDs = append(ancestorIDs, ancestorID)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate cleared edges for env %s: %w", envID, err)
	}

	for _, ancestorID := range ancestorIDs {
		// ref_count's CHECK (ref_count >= 0) is the backstop here: every live
		// edge (deleted_at IS NULL) should correspond to exactly one +1
		// already applied, so clearing it should never drive the count
		// negative. If this fails, something upstream double-counted a -1 or
		// under-counted a +1 — fail the whole batch loudly rather than let
		// the constraint violation get swallowed into a silently wrong count.
		if _, err := tx.Exec(ctx, `
			UPDATE tos_ancestor_reference_count
			SET ref_count = ref_count - 1, updated_at = now()
			WHERE ancestor_id = $1
		`, ancestorID); err != nil {
			return fmt.Errorf("decrement ref_count for %s: %w", ancestorID, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}

	log.Printf("env=%s soft-deleted, cleared %d edge(s)", envID, len(ancestorIDs))

	return nil
}
