package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	openapi_types "github.com/oapi-codegen/runtime/types"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/dashboard-api/internal/api"
	"github.com/e2b-dev/infra/packages/dashboard-api/internal/identity"
	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/ginutils"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// GetSuperAdminAdmins lists platform admins with their resolved profiles. Super-admin only.
func (s *APIStore) GetSuperAdminAdmins(c *gin.Context) {
	ctx := c.Request.Context()
	telemetry.ReportEvent(ctx, "list platform admins (super-admin)")

	rows, err := s.db.ListPlatformAdmins(ctx)
	if err != nil {
		logger.L().Error(ctx, "failed to list platform admins", zap.Error(err))
		s.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to list platform admins")

		return
	}

	userIDs := make([]uuid.UUID, 0, len(rows))
	for _, row := range rows {
		userIDs = append(userIDs, row.UserID)
	}

	profiles := map[uuid.UUID]identity.Profile{}
	if len(userIDs) > 0 {
		resolved, resolveErr := s.identityService.ProfilesByUserID(ctx, userIDs)
		if resolveErr != nil {
			// Non-fatal: still return admins, just without email/name.
			logger.L().Error(ctx, "failed to resolve admin profiles", zap.Error(resolveErr))
		} else {
			profiles = resolved
		}
	}

	entries := make([]api.PlatformAdminEntry, 0, len(rows))
	for _, row := range rows {
		entry := api.PlatformAdminEntry{
			UserID:    row.UserID,
			Role:      api.PlatformAdminEntryRole(row.Role),
			CreatedAt: row.CreatedAt,
		}
		if p, ok := profiles[row.UserID]; ok {
			if p.Email != "" {
				email := p.Email
				entry.Email = &email
			}
			if p.Name != "" {
				name := p.Name
				entry.Name = &name
			}
		}
		entries = append(entries, entry)
	}

	c.JSON(http.StatusOK, api.PlatformAdminsResponse{Data: entries})
}

// PostSuperAdminAdmins grants (or updates) a platform role for the user with the given email. Super-admin only.
func (s *APIStore) PostSuperAdminAdmins(c *gin.Context) {
	ctx := c.Request.Context()

	body, err := ginutils.ParseBody[api.GrantPlatformAdminRequest](ctx, c)
	if err != nil {
		s.sendAPIStoreError(c, http.StatusBadRequest, "Invalid request body")

		return
	}

	role := string(body.Role)
	if role != "platform_admin" && role != "super_admin" {
		s.sendAPIStoreError(c, http.StatusBadRequest, "role must be platform_admin or super_admin")

		return
	}

	profiles, err := s.identityService.FindProfilesByEmail(ctx, string(body.Email))
	if err != nil {
		logger.L().Error(ctx, "failed to look up user by email", zap.Error(err))
		s.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to look up user")

		return
	}
	if len(profiles) == 0 {
		s.sendAPIStoreError(c, http.StatusNotFound, "No user found with that email")

		return
	}

	if err := s.db.UpsertPlatformAdmin(ctx, queries.UpsertPlatformAdminParams{
		UserID: profiles[0].UserID,
		Role:   role,
	}); err != nil {
		logger.L().Error(ctx, "failed to grant platform role", zap.Error(err))
		s.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to grant platform role")

		return
	}

	c.Status(http.StatusNoContent)
}

// DeleteSuperAdminAdminsUserID revokes a user's platform role. Super-admin only.
func (s *APIStore) DeleteSuperAdminAdminsUserID(c *gin.Context, userID openapi_types.UUID) {
	ctx := c.Request.Context()
	telemetry.ReportEvent(ctx, "revoke platform role (super-admin)")

	if err := s.db.DeletePlatformAdmin(ctx, userID); err != nil {
		logger.L().Error(ctx, "failed to revoke platform role", zap.Error(err))
		s.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to revoke platform role")

		return
	}

	c.Status(http.StatusNoContent)
}
