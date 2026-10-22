package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/dashboard-api/internal/api"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// GetSuperAdminMe returns the caller's platform role, or null when they have none, so the dashboard can gate admin surfaces without triggering a 403.
func (s *APIStore) GetSuperAdminMe(c *gin.Context) {
	ctx := c.Request.Context()
	telemetry.ReportEvent(ctx, "get caller platform role (super-admin)")

	var platformRole *api.SuperAdminMePlatformRole
	if role, ok := auth.GetPlatformRole(c); ok {
		converted := api.SuperAdminMePlatformRole(role)
		platformRole = &converted
	}

	c.JSON(http.StatusOK, api.SuperAdminMe{PlatformRole: platformRole})
}
