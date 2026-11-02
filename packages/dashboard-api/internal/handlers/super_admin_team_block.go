package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/dashboard-api/internal/api"
	dashboardqueries "github.com/e2b-dev/infra/packages/db/pkg/dashboard/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/ginutils"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// PutSuperAdminTeamsTeamIDBlock blocks or unblocks a team. A blocked team has
// its requests rejected by the blocked-team middleware. Super-admin only.
func (s *APIStore) PutSuperAdminTeamsTeamIDBlock(c *gin.Context, teamID api.TeamID) {
	ctx := c.Request.Context()
	telemetry.ReportEvent(ctx, "set team blocked (super-admin)")

	body, err := ginutils.ParseBody[api.SetTeamBlockedRequest](ctx, c)
	if err != nil {
		s.sendAPIStoreError(c, http.StatusBadRequest, "Invalid request body")

		return
	}

	if _, err := s.db.Dashboard.SetTeamBlocked(ctx, dashboardqueries.SetTeamBlockedParams{
		TeamID:        teamID,
		IsBlocked:     body.IsBlocked,
		BlockedReason: body.BlockedReason,
	}); err != nil {
		logger.L().Error(ctx, "failed to set team blocked state", zap.Error(err))
		s.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to update team blocked state")

		return
	}

	c.Status(http.StatusNoContent)
}
