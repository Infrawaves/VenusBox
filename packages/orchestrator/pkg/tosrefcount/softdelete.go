package tosrefcount

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

// RunSoftDeleteScanner periodically reads envs.deleted_at (the production
// template soft-delete timestamp, set by
// packages/api/internal/handlers/template_delete.go — no event hook into
// that path exists or is needed) and clears every edge recorded for each
// newly-deleted template, decrementing ref_count accordingly. Blocks until
// ctx is canceled.
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
func (c *Consumer) RunSoftDeleteScanner(ctx context.Context, interval time.Duration) error {
	watermark := time.Now().Add(-interval) // catch anything deleted just before startup

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}

		newWatermark, err := c.scanSoftDeletedTemplates(ctx, watermark)
		if err != nil {
			logger.L().Warn(ctx, "tos-ref-count: soft-delete scan failed, will retry next tick", zap.Error(err))

			continue
		}
		watermark = newWatermark
	}
}

// scanSoftDeletedTemplates finds envs soft-deleted since since, clears their
// edges, and returns the newest deleted_at seen (or since, unchanged, if
// nothing new was found).
func (c *Consumer) scanSoftDeletedTemplates(ctx context.Context, since time.Time) (time.Time, error) {
	rows, err := c.pool.Query(ctx, `
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

		if err := c.ApplyTemplateSoftDelete(ctx, envID); err != nil {
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
		logger.L().Info(ctx, "tos-ref-count: soft-delete scan processed templates", zap.Int("count", count))
	}

	return newest, nil
}

// ApplyTemplateSoftDelete clears every edge recorded under envID (every
// build this template ever produced, not just whatever it currently points
// to — this is the whole point of carrying env_id on the edge instead of
// re-deriving a build history from env_builds/env_build_assignments) and
// decrements ref_count for each ancestor that actually had a live edge
// cleared. The UPDATE's WHERE deleted_at IS NULL guard makes this
// idempotent: re-running it for an env_id already processed finds no rows
// and does nothing.
func (c *Consumer) ApplyTemplateSoftDelete(ctx context.Context, envID string) error {
	tx, err := c.pool.Begin(ctx)
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

	logger.L().Info(ctx, "tos-ref-count: template soft-deleted, cleared edges",
		zap.String("env_id", envID), zap.Int("edges_cleared", len(ancestorIDs)),
	)

	return nil
}
