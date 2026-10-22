package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	openapi_types "github.com/oapi-codegen/runtime/types"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/dashboard-api/internal/api"
	"github.com/e2b-dev/infra/packages/db/pkg/dberrors"
	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/ginutils"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// DeleteSuperAdminUsersUserID deletes a user platform-wide. Super-admin only.
func (s *APIStore) DeleteSuperAdminUsersUserID(c *gin.Context, userID openapi_types.UUID) {
	ctx := c.Request.Context()
	telemetry.ReportEvent(ctx, "delete user (super-admin)")

	if status, msg := s.deleteUserByID(ctx, userID); status != 0 {
		s.sendAPIStoreError(c, status, msg)

		return
	}

	c.Status(http.StatusNoContent)
}

// PostSuperAdminUsersUserIDTeams adds a user to a team. Super-admin only.
func (s *APIStore) PostSuperAdminUsersUserIDTeams(c *gin.Context, userID openapi_types.UUID) {
	ctx := c.Request.Context()
	telemetry.ReportEvent(ctx, "add user to team (super-admin)")

	body, err := ginutils.ParseBody[api.AddUserToTeamRequest](ctx, c)
	if err != nil {
		s.sendAPIStoreError(c, http.StatusBadRequest, "Invalid request body")

		return
	}

	// Anchor the public user record before the membership FK references it.
	if err := s.authDB.UpsertPublicUser(ctx, userID); err != nil {
		logger.L().Error(ctx, "failed to create public user anchor", zap.Error(err), logger.WithUserID(userID.String()))
		s.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to add user to team")

		return
	}

	err = s.db.AddTeamMember(ctx, queries.AddTeamMemberParams{
		UserID:  userID,
		TeamID:  body.TeamID,
		AddedBy: auth.MustGetUserID(c),
	})
	if err != nil {
		if dberrors.IsUniqueConstraintViolation(err) {
			s.sendAPIStoreError(c, http.StatusBadRequest, "User is already a member of this team")

			return
		}

		if dberrors.IsForeignKeyViolation(err) {
			s.sendAPIStoreError(c, http.StatusNotFound, "Team not found")

			return
		}

		logger.L().Error(ctx, "failed to add user to team", zap.Error(err), logger.WithTeamID(body.TeamID.String()), logger.WithUserID(userID.String()))
		s.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to add user to team")

		return
	}

	s.authService.InvalidateTeamMemberCache(ctx, userID, body.TeamID.String())

	c.Status(http.StatusCreated)
}

// DeleteSuperAdminUsersUserIDTeamsTeamID removes a user from a team. A team's
// default member and its last remaining member cannot be removed. Super-admin only.
func (s *APIStore) DeleteSuperAdminUsersUserIDTeamsTeamID(c *gin.Context, userID openapi_types.UUID, teamID api.TeamID) {
	ctx := c.Request.Context()
	telemetry.ReportEvent(ctx, "remove user from team (super-admin)")

	txDB, tx, err := s.db.WithTx(ctx)
	if err != nil {
		logger.L().Error(ctx, "failed to start transaction for removing team member", zap.Error(err))
		s.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to remove user from team")

		return
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	lockedMembers, err := txDB.LockTeamMembersForUpdate(ctx, teamID)
	if err != nil {
		logger.L().Error(ctx, "failed to lock team members", zap.Error(err))
		s.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to check team members")

		return
	}

	relation, err := txDB.GetTeamMemberRelation(ctx, queries.GetTeamMemberRelationParams{
		TeamID: teamID,
		UserID: userID,
	})
	if err != nil {
		if dberrors.IsNotFoundError(err) {
			s.sendAPIStoreError(c, http.StatusBadRequest, "User is not a member of this team")

			return
		}

		logger.L().Error(ctx, "failed to get team member relation", zap.Error(err))
		s.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to get team member")

		return
	}

	if relation.IsDefault {
		s.sendAPIStoreError(c, http.StatusBadRequest, "Cannot remove a default team member")

		return
	}

	if len(lockedMembers) <= 1 {
		s.sendAPIStoreError(c, http.StatusBadRequest, "Cannot remove the last team member")

		return
	}

	if err := txDB.RemoveTeamMember(ctx, queries.RemoveTeamMemberParams{
		TeamID: teamID,
		UserID: userID,
	}); err != nil {
		logger.L().Error(ctx, "failed to remove team member", zap.Error(err), logger.WithTeamID(teamID.String()), logger.WithUserID(userID.String()))
		s.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to remove user from team")

		return
	}

	if err := tx.Commit(ctx); err != nil {
		logger.L().Error(ctx, "failed to commit team member removal", zap.Error(err), logger.WithTeamID(teamID.String()))
		s.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to remove user from team")

		return
	}

	s.authService.InvalidateTeamMemberCache(ctx, userID, teamID.String())

	c.Status(http.StatusNoContent)
}
