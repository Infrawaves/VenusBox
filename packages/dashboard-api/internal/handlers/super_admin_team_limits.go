package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/dashboard-api/internal/api"
	"github.com/e2b-dev/infra/packages/db/pkg/dberrors"
	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/ginutils"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// PutSuperAdminTeamsTeamIDLimits overrides a team's effective quota by writing
// project_limits directly. Unlike the billing projection endpoint it carries no
// revision: a platform admin's edit is authoritative for a self-hosted deployment.
func (s *APIStore) PutSuperAdminTeamsTeamIDLimits(c *gin.Context, teamID api.TeamID) {
	ctx := c.Request.Context()
	telemetry.ReportEvent(ctx, "update team limits (super-admin)")

	body, err := ginutils.ParseBody[api.UpdateTeamLimitsRequest](ctx, c)
	if err != nil {
		s.sendAPIStoreError(c, http.StatusBadRequest, "Invalid request body")

		return
	}

	if body.DefaultFreeDiskSizeMb > body.MaxDiskSizeMb {
		s.sendAPIStoreError(c, http.StatusBadRequest, "defaultFreeDiskSizeMb must not exceed maxDiskSizeMb")

		return
	}

	if err := s.db.UpsertProjectLimits(ctx, queries.UpsertProjectLimitsParams{
		TeamID:                   teamID,
		MaxLengthHours:           body.MaxLengthHours,
		ConcurrentSandboxes:      body.ConcurrentSandboxes,
		ConcurrentTemplateBuilds: body.ConcurrentTemplateBuilds,
		MaxVcpu:                  body.MaxVcpu,
		MaxRamMb:                 body.MaxRamMb,
		DiskMb:                   body.DiskMb,
		EventsTtlDays:            body.EventsTtlDays,
		DefaultFreeDiskSizeMb:    body.DefaultFreeDiskSizeMb,
		MaxFreeDiskSizeMb:        body.MaxDiskSizeMb,
	}); err != nil {
		if dberrors.IsCheckViolation(err) {
			s.sendAPIStoreError(c, http.StatusBadRequest, "Limits violate a database constraint")

			return
		}
		logger.L().Error(ctx, "failed to update team limits", zap.Error(err))
		s.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to update team limits")

		return
	}

	c.Status(http.StatusNoContent)
}
