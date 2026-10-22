package handlers

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/dashboard-api/internal/api"
	"github.com/e2b-dev/infra/packages/dashboard-api/internal/identity"
	"github.com/e2b-dev/infra/packages/db/pkg/dberrors"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

const identityDeleteMaxRetries = 3

func (s *APIStore) DeleteAdminUsersUserId(c *gin.Context, userId api.UserId) {
	if status, msg := s.deleteUserByID(c.Request.Context(), userId); status != 0 {
		s.sendAPIStoreError(c, status, msg)

		return
	}

	c.Status(http.StatusNoContent)
}

// deleteUserByID removes the user's platform records and identity-provider identity.
// Returns (0, "") on success, or an HTTP status + message describing the failure.
func (s *APIStore) deleteUserByID(ctx context.Context, userID uuid.UUID) (int, string) {
	handle, err := s.identityService.PrepareDeleteUser(ctx, userID)
	if err != nil {
		if errors.Is(err, identity.ErrUserNotFound) {
			return http.StatusNotFound, fmt.Sprintf("User %s not found or has no identity provider record", userID)
		}

		logger.L().Error(ctx, "failed to prepare user deletion", zap.String("user_id", userID.String()), zap.Error(err))

		return http.StatusInternalServerError, "Failed to resolve identity provider record for user"
	}

	// Done before the IdP removal so a DB failure does not orphan the identity.
	if err := s.authDB.DeletePublicUser(ctx, userID); err != nil {
		if dberrors.IsNotFoundError(err) {
			return http.StatusNotFound, fmt.Sprintf("User %s not found", userID)
		}

		if dberrors.IsForeignKeyViolation(err) {
			logger.L().Warn(ctx, "cannot delete user due to existing references", zap.String("user_id", userID.String()), zap.Error(err))

			return http.StatusConflict, "Cannot delete user: existing references (e.g. addons) must be removed first"
		}

		logger.L().Error(ctx, "failed to delete public user", zap.String("user_id", userID.String()), zap.Error(err))

		return http.StatusInternalServerError, "Failed to delete public user record"
	}

	// Retry since the DB rows are already gone and we must not leave the IdP identity active.
	// Use a detached context so a client disconnect does not cancel the cleanup.
	cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cleanupCancel()

	var identityErr error
	for attempt := range identityDeleteMaxRetries {
		identityErr = handle.Execute(cleanupCtx)
		if identityErr == nil {
			break
		}

		logger.L().Warn(ctx, "retrying identity deletion",
			zap.String("user_id", userID.String()),
			zap.Int("attempt", attempt+1),
			zap.Error(identityErr),
		)

		time.Sleep(time.Duration(attempt+1) * 200 * time.Millisecond)
	}

	if identityErr != nil {
		logger.L().Error(ctx, "failed to delete user identity provider record after retries", zap.String("user_id", userID.String()), zap.Error(identityErr))

		return http.StatusInternalServerError, "User DB records deleted but identity provider removal failed — the IdP identity may need manual cleanup"
	}

	return 0, ""
}
