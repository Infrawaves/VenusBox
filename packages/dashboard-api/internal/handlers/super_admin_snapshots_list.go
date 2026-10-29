package handlers

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/dashboard-api/internal/api"
	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

const (
	defaultSnapshotsLimit = int32(50)
	maxSnapshotsLimit     = int32(100)
)

func normalizeSnapshotsLimit(limit *api.SnapshotsLimit) int32 {
	v := utils.DerefOrDefault((*int32)(limit), defaultSnapshotsLimit)
	if v < 1 {
		return 1
	}
	if v > maxSnapshotsLimit {
		return maxSnapshotsLimit
	}

	return v
}

// parseSnapshotsCursor parses a `created_at|snapshot_id` cursor. An empty cursor
// is the first page (far-future time + max uuid), mirroring the builds cursor.
func parseSnapshotsCursor(cursor *api.SnapshotsCursor) (time.Time, uuid.UUID, error) {
	defaultID := uuid.MustParse(maxCursorID)
	if cursor == nil || *cursor == "" {
		return maxCursorTime, defaultID, nil
	}

	parts := strings.SplitN(string(*cursor), "|", 2)
	if len(parts) != 2 {
		return time.Time{}, uuid.Nil, errors.New("invalid cursor format")
	}

	cursorTime, err := parseCursorTime(parts[0])
	if err != nil {
		return time.Time{}, uuid.Nil, err
	}

	cursorID, err := uuid.Parse(parts[1])
	if err != nil {
		return time.Time{}, uuid.Nil, err
	}

	return cursorTime, cursorID, nil
}

func (s *APIStore) GetSuperAdminSnapshots(c *gin.Context, params api.GetSuperAdminSnapshotsParams) {
	ctx := c.Request.Context()
	telemetry.ReportEvent(ctx, "list all snapshots (super-admin)")

	limit := normalizeSnapshotsLimit(params.Limit)
	cursorTime, cursorID, err := parseSnapshotsCursor(params.Cursor)
	if err != nil {
		s.sendAPIStoreError(c, http.StatusBadRequest, "invalid cursor")

		return
	}

	rows, err := s.db.ListAllSnapshots(ctx, queries.ListAllSnapshotsParams{
		TeamID:          params.TeamId,
		Search:          strings.TrimSpace(utils.DerefOrDefault(params.Search, "")),
		CursorCreatedAt: cursorTime,
		CursorID:        cursorID,
		LimitPlusOne:    limit + 1,
	})
	if err != nil {
		logger.L().Error(ctx, "Error listing all snapshots", zap.Error(err))
		s.sendAPIStoreError(c, http.StatusInternalServerError, "Error when listing snapshots")

		return
	}

	hasMore := len(rows) > int(limit)
	if hasMore {
		rows = rows[:limit]
	}

	snapshots := make([]api.SuperAdminSnapshot, 0, len(rows))
	for _, row := range rows {
		snapshots = append(snapshots, superAdminSnapshotToAPI(row))
	}

	var nextCursor *string
	if hasMore && len(rows) > 0 {
		last := rows[len(rows)-1]
		cursor := fmt.Sprintf("%s|%s", last.CreatedAt.Time.UTC().Format(time.RFC3339Nano), last.ID.String())
		nextCursor = &cursor
	}

	c.JSON(http.StatusOK, api.SuperAdminSnapshotsResponse{
		Data:       snapshots,
		NextCursor: nextCursor,
	})
}

func superAdminSnapshotToAPI(row queries.ListAllSnapshotsRow) api.SuperAdminSnapshot {
	out := api.SuperAdminSnapshot{
		SnapshotID:          row.ID,
		SandboxID:           row.SandboxID,
		EnvID:               row.EnvID,
		BaseTemplateID:      row.BaseEnvID,
		TeamID:              row.TeamID,
		TeamName:            row.TeamName,
		SandboxStartedAt:    row.SandboxStartedAt.Time,
		PausedAt:            row.CreatedAt.Time,
		AutoPause:           row.AutoPause,
		EnvSecure:           row.EnvSecure,
		AllowInternetAccess: row.AllowInternetAccess,
		CpuCount:            row.CpuCount,
		MemoryMB:            row.MemoryMb,
		DiskSizeMB:          row.DiskSizeMb,
		EnvdVersion:         row.EnvdVersion,
	}

	if row.BaseTemplateAlias != "" {
		alias := row.BaseTemplateAlias
		out.BaseTemplateAlias = &alias
	}
	if row.OriginNodeID != "" {
		origin := row.OriginNodeID
		out.OriginNodeID = &origin
	}
	if len(row.Metadata) > 0 {
		meta := map[string]string(row.Metadata)
		out.Metadata = &meta
	}

	return out
}
