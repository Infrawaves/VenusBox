package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/dashboard-api/internal/api"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// GetSuperAdminUsersUserID returns a single platform user with the teams they belong to.
func (s *APIStore) GetSuperAdminUsersUserID(c *gin.Context, userID api.UserID) {
	ctx := c.Request.Context()
	telemetry.ReportEvent(ctx, "get user detail (super-admin)")

	rows, err := s.db.Dashboard.ListUsersByIDs(ctx, []uuid.UUID{userID})
	if err != nil {
		logger.L().Error(ctx, "Error getting user detail", zap.Error(err))
		s.sendAPIStoreError(c, http.StatusInternalServerError, "Error when getting user")

		return
	}
	if len(rows) == 0 {
		s.sendAPIStoreError(c, http.StatusNotFound, "User not found")

		return
	}

	profiles := s.resolveUserProfiles(ctx, []uuid.UUID{userID})
	row := rows[0]
	user := buildSuperAdminUser(row.ID, row.CreatedAt, row.TeamIds, row.TeamNames, profiles)

	c.JSON(http.StatusOK, user)
}
