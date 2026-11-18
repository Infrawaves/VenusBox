// Package tosrefcount maintains the TOS layer reference-counting tables (tos_build_edges, tos_ancestor_reference_count) described in tos-reference-counting-plan.md.
//
// It drains tosLayerEvent payloads pushed by orchestrator's upload paths (pkg/sandbox/uploads.go, publishTOSLayerEvent) from a Redis List, and separately polls envs.deleted_at to clear the edges of soft-deleted templates.
//
// It is run standalone by cmd/tos-ref-count-consumer, which deploys and scales independently of the orchestrator binary and also drives the historical backfill mode that reads tos-layer-scan's JSON Lines output.
package tosrefcount

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
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

// processingListKey holds events BLMOVEd out of EventsListKey while they're being applied, so a crash or transient
// HandleEvent failure leaves the event sitting here instead of gone for good — BRPOP pops and removes atomically,
// which made every such failure a silent, permanent drop. An event is LREMed from this list only after it's been
// applied successfully; ApplyUploadEvent's ON CONFLICT DO NOTHING on the edge insert makes redelivery safe.
const processingListKey = "tos.layer.ref.events.processing"

// Event mirrors pkg/sandbox/uploads.go's tosLayerEvent, kept as an independent copy since the two sides talk over JSON on a Redis List, not a Go import.
type Event struct {
	BuildID     string   `json:"build_id"`
	EnvID       string   `json:"env_id"` // envs.id / env_builds.env_id; empty for dev tooling
	FileType    string   `json:"file_type"`
	AncestorIDs []string `json:"ancestor_ids"`
	EventType   string   `json:"event_type"`
	Timestamp   int64    `json:"timestamp"`
}

// Consumer holds the Postgres pool the event and soft-delete-scan loops write through; it carries no Redis client since only ConsumeEvents needs one.
//
// teamFilter, when non-empty, restricts the soft-delete scan (see softdelete.go) to envs.team_id being one of its entries — a rollout aid, not a permanent feature. Event consumption (HandleEvent/ApplyUploadEvent) always processes every team regardless of this filter.
type Consumer struct {
	pool       *pgxpool.Pool
	teamFilter []uuid.UUID
}

func New(pool *pgxpool.Pool, teamFilter []uuid.UUID) *Consumer {
	return &Consumer{pool: pool, teamFilter: teamFilter}
}

// ConsumeEvents blocks, draining EventsListKey via BLMOVE into processingListKey until ctx is canceled. A handling
// failure leaves the event in processingListKey for the next tick to retry (redelivery guarantees beyond that
// belong to a future queue, see tos-reference-counting-plan.md §10) instead of losing it the way a plain BRPOP would.
func (c *Consumer) ConsumeEvents(ctx context.Context, rdb redis.UniversalClient) error {
	for {
		if ctx.Err() != nil {
			return nil //nolint:nilerr // graceful shutdown, not a failure
		}

		// BLMOVE blocks for up to 5s so the loop can observe ctx cancellation between pops instead of blocking indefinitely.
		payload, err := rdb.BLMove(ctx, EventsListKey, processingListKey, "right", "left", 5*time.Second).Result()
		if errors.Is(err, redis.Nil) {
			continue // timeout, no event — loop back and recheck ctx
		}
		if err != nil {
			if ctx.Err() != nil {
				return nil //nolint:nilerr // graceful shutdown, not a failure
			}

			logger.L().Warn(ctx, "tos-ref-count: blmove error", zap.Error(err))

			select {
			case <-ctx.Done():
				return nil
			case <-time.After(time.Second):
			}

			continue
		}

		if err := c.HandleEvent(ctx, payload); err != nil {
			logger.L().Warn(ctx, "tos-ref-count: handle event failed, left in processing list for retry", zap.Error(err))

			continue
		}

		if err := rdb.LRem(ctx, processingListKey, 1, payload).Err(); err != nil {
			logger.L().Warn(ctx, "tos-ref-count: failed to remove applied event from processing list", zap.Error(err))
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

// ApplyUploadEvent records build_id -> each ancestor_id as an edge (including an explicit self-reference for buildID, deduplicated against ancestorIDs if it's already present), bumping ref_count only for edges that did not already exist.
//
// The self-reference is added here rather than relying on ancestorIDs containing buildID, since a valid empty diff (e.g. a filesystem-only snapshot with no dirty blocks) fully inherits its parent's mapping and never lists its own ID — without an explicit self-reference, such a build would sit at ref_count=0 and look reclaimable despite being live.
//
// source distinguishes the two write paths ("live_upload" vs "backfill_scan") without needing a second table.
func (c *Consumer) ApplyUploadEvent(ctx context.Context, buildID uuid.UUID, envID string, fileType string, ancestorIDs []uuid.UUID, source string) error {
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback is a no-op after Commit

	if !slices.Contains(ancestorIDs, buildID) {
		ancestorIDs = append(ancestorIDs, buildID)
	}

	for _, ancestorID := range ancestorIDs {
		var inserted bool
		err := tx.QueryRow(ctx, `
			INSERT INTO tos_build_edges (build_id, ancestor_id, env_id, source)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (build_id, ancestor_id) DO NOTHING
			RETURNING true
		`, buildID, ancestorID, envID, source).Scan(&inserted)

		switch {
		case errors.Is(err, pgx.ErrNoRows): //nolint:forbidigo // tosrefcount is its own module and doesn't depend on packages/db
			// Edge already existed from a prior delivery — do not bump ref_count again.
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
