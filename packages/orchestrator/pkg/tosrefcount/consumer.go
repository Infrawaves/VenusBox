// Package tosrefcount maintains the TOS layer reference-counting tables
// (tos_build_edges, tos_ancestor_reference_count) described in
// tos-reference-counting-plan.md. It drains tosLayerEvent payloads pushed by
// orchestrator's upload paths (pkg/sandbox/uploads.go, publishTOSLayerEvent)
// from a Redis List, and separately polls envs.deleted_at to clear the edges
// of soft-deleted templates.
//
// This logic originally lived only in cmd/tos-ref-count-consumer as a
// standalone dev-validation binary; it is now also run in-process by the
// orchestrator binary itself (pkg/factories/run.go, under the
// template-manager role) so it no longer needs to be started by hand. The
// standalone binary remains as a thin wrapper over this package, still
// useful for local dev runs and for driving the historical backfill mode
// that reads tos-layer-scan's JSON Lines output.
package tosrefcount

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

// EventsListKey is the Redis List publishTOSLayerEvent pushes onto.
const EventsListKey = "tos.layer.ref.events"

// Event mirrors pkg/sandbox/uploads.go's tosLayerEvent. Kept as an
// independent copy rather than a shared type, since the two sides
// communicate over a wire format (JSON on a Redis List), not a Go import —
// decoding is deliberately tolerant of fields the producer may add later.
type Event struct {
	BuildID     string   `json:"build_id"`
	EnvID       string   `json:"env_id"` // envs.id / env_builds.env_id; empty for dev tooling
	FileType    string   `json:"file_type"`
	AncestorIDs []string `json:"ancestor_ids"`
	EventType   string   `json:"event_type"`
	Timestamp   int64    `json:"timestamp"`
}

// Consumer holds the Postgres pool the event and soft-delete-scan loops
// write through. It carries no Redis client: ConsumeEvents takes one
// explicitly since it is only needed for the live-event loop, not for the
// scanner or the backfill path.
type Consumer struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Consumer {
	return &Consumer{pool: pool}
}

// ConsumeEvents blocks, draining EventsListKey via BRPOP until ctx is
// canceled. A handling failure for one event is logged and dropped rather
// than retried — redelivery guarantees belong to the queue technology
// eventually replacing this Redis List (see tos-reference-counting-plan.md
// §10), not to this loop.
func (c *Consumer) ConsumeEvents(ctx context.Context, rdb redis.UniversalClient) error {
	for {
		if ctx.Err() != nil {
			return nil
		}

		// BRPOP blocks for up to 5s so the loop can observe ctx cancellation
		// between pops instead of blocking indefinitely.
		res, err := rdb.BRPop(ctx, 5*time.Second, EventsListKey).Result()
		if errors.Is(err, redis.Nil) {
			continue // timeout, no event — loop back and recheck ctx
		}
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}

			logger.L().Warn(ctx, "tos-ref-count: brpop error", zap.Error(err))

			select {
			case <-ctx.Done():
				return nil
			case <-time.After(time.Second):
			}

			continue
		}

		// res[0] is the key name, res[1] is the popped value.
		if err := c.HandleEvent(ctx, res[1]); err != nil {
			logger.L().Warn(ctx, "tos-ref-count: handle event failed, dropping", zap.Error(err))
		}
	}
}

// HandleEvent decodes and applies a single JSON event payload.
func (c *Consumer) HandleEvent(ctx context.Context, payload string) error {
	var event Event
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
	// doc's route B: the self-edge is cleared only by ApplyTemplateSoftDelete
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
		return c.ApplyUploadEvent(ctx, buildID, event.EnvID, event.FileType, ancestorIDs, "live_upload")
	default:
		return fmt.Errorf("unknown event_type %q", event.EventType)
	}
}

// ApplyUploadEvent registers buildID itself in tos_ancestor_reference_count
// (ref_count defaulting to 0 if it has no row yet — every build that lands
// in TOS must be queryable there, not just builds that happen to already
// have a downstream dependent), then records build_id -> each ancestor_id as
// an edge and bumps ref_count only for edges that did not already exist —
// this is what makes the +1 tolerant of the same event being processed more
// than once.
//
// source is stamped on each inserted edge ("live_upload" from the event
// loop, "backfill_scan" from the historical backfill path) so the two write
// paths remain distinguishable in tos_build_edges without needing a second
// table.
func (c *Consumer) ApplyUploadEvent(ctx context.Context, buildID uuid.UUID, envID string, fileType string, ancestorIDs []uuid.UUID, source string) error {
	tx, err := c.pool.Begin(ctx)
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
	envLastSpawnedAt := c.lookupEnvLastSpawnedAt(ctx, tx, envID)

	for _, ancestorID := range ancestorIDs {
		var inserted bool
		err := tx.QueryRow(ctx, `
			INSERT INTO tos_build_edges (build_id, ancestor_id, env_id, env_last_spawned_at, source)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (build_id, ancestor_id) DO NOTHING
			RETURNING true
		`, buildID, ancestorID, envID, envLastSpawnedAt, source).Scan(&inserted)

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

	logger.L().Info(ctx, "tos-ref-count: edges processed",
		zap.String("build_id", buildID.String()),
		zap.String("file_type", fileType),
		zap.Int("ancestors", len(ancestorIDs)),
	)

	return nil
}

// lookupEnvLastSpawnedAt reads envs.last_spawned_at for envID, for the
// informational snapshot stored on the edge (see schema.sql). Returns nil on
// any failure to look it up (empty envID, no matching row, or a query
// error) — this is a best-effort display field, not load-bearing for any
// ref_count logic, so a lookup problem here must never fail the edge write.
func (c *Consumer) lookupEnvLastSpawnedAt(ctx context.Context, tx pgx.Tx, envID string) *time.Time {
	if envID == "" {
		return nil
	}

	var lastSpawnedAt *time.Time
	if err := tx.QueryRow(ctx, `SELECT last_spawned_at FROM envs WHERE id = $1`, envID).Scan(&lastSpawnedAt); err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			logger.L().Warn(ctx, "tos-ref-count: lookup last_spawned_at failed (informational field only)",
				zap.String("env_id", envID), zap.Error(err),
			)
		}

		return nil
	}

	return lastSpawnedAt
}
