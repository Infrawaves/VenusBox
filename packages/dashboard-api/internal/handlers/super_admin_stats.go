package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/dashboard-api/internal/api"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// GetSuperAdminStats returns platform-wide aggregate counts for the overview.
func (s *APIStore) GetSuperAdminStats(c *gin.Context) {
	ctx := c.Request.Context()
	telemetry.ReportEvent(ctx, "get platform stats (super-admin)")

	stats, err := s.db.Dashboard.GetPlatformStats(ctx)
	if err != nil {
		logger.L().Error(ctx, "Error getting platform stats", zap.Error(err))
		s.sendAPIStoreError(c, http.StatusInternalServerError, "Error when getting platform stats")

		return
	}

	c.JSON(http.StatusOK, api.SuperAdminStats{
		TeamCount:     stats.TeamCount,
		UserCount:     stats.UserCount,
		TemplateCount: stats.TemplateCount,
		BuildCount:    stats.BuildCount,
	})
}
