package handlers

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/dashboard-api/internal/api"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// DeleteSuperAdminTeamsTeamID deletes a team, but only when it owns no blocking
// resources (envs, snapshots, volumes — all ON DELETE NO ACTION). A team with
// any of those cannot be cascade-deleted from Postgres alone; tearing those
// down requires the orchestrator, so we refuse with 409 and leave it untouched.
// Super-admin only (enforced by middleware).
func (s *APIStore) DeleteSuperAdminTeamsTeamID(c *gin.Context, teamID api.TeamID) {
	ctx := c.Request.Context()
	telemetry.ReportEvent(ctx, "delete team (super-admin)")

	counts, err := s.db.Dashboard.CountTeamBlockingResources(ctx, teamID)
	if err != nil {
		logger.L().Error(ctx, "failed to count team resources", zap.Error(err))
		s.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to check team resources")

		return
	}

	if counts.EnvCount > 0 || counts.SnapshotCount > 0 || counts.VolumeCount > 0 {
		s.sendAPIStoreError(c, http.StatusConflict, fmt.Sprintf(
			"Team still owns resources and cannot be deleted: %d templates/envs, %d snapshots, %d volumes. Remove them first.",
			counts.EnvCount, counts.SnapshotCount, counts.VolumeCount,
		))

		return
	}

	if err := s.authDB.DeleteTeamByID(ctx, teamID); err != nil {
		logger.L().Error(ctx, "failed to delete team", zap.Error(err))
		s.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to delete team")

		return
	}

	c.Status(http.StatusNoContent)
}
