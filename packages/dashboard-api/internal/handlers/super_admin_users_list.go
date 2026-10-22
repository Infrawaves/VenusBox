package handlers

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	openapi_types "github.com/oapi-codegen/runtime/types"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/dashboard-api/internal/api"
	"github.com/e2b-dev/infra/packages/dashboard-api/internal/identity"
	dashboardqueries "github.com/e2b-dev/infra/packages/db/pkg/dashboard/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

const (
	// A search shorter than this scans too much of the directory for too little
	// signal, so it is treated as no match.
	superAdminUserSearchMinLen = 2
	// Upper bound on how many matching users a search returns; the identity
	// directory scan stops once this many are found.
	superAdminUserSearchMaxResults = 100
)

func (s *APIStore) GetSuperAdminUsers(c *gin.Context, params api.GetSuperAdminUsersParams) {
	ctx := c.Request.Context()
	telemetry.ReportEvent(ctx, "list all users (super-admin)")

	if search := strings.TrimSpace(utils.DerefOrDefault(params.Search, "")); search != "" {
		s.searchSuperAdminUsers(c, search)

		return
	}

	limit := normalizeBuildsLimit(params.Limit)
	cursorTime, cursorID, err := parseBuildsCursor(params.Cursor)
	if err != nil {
		s.sendAPIStoreError(c, http.StatusBadRequest, "invalid cursor")

		return
	}

	rows, err := s.db.Dashboard.ListAllUsers(ctx, dashboardqueries.ListAllUsersParams{
		CursorCreatedAt: cursorTime,
		CursorID:        cursorID,
		LimitPlusOne:    limit + 1,
	})
	if err != nil {
		logger.L().Error(ctx, "Error getting all users", zap.Error(err))
		s.sendAPIStoreError(c, http.StatusInternalServerError, "Error when getting users")

		return
	}

	hasMore := len(rows) > int(limit)
	if hasMore {
		rows = rows[:limit]
	}

	userIDs := make([]uuid.UUID, 0, len(rows))
	for _, row := range rows {
		userIDs = append(userIDs, row.ID)
	}
	profiles := s.resolveUserProfiles(ctx, userIDs)

	users := make([]api.SuperAdminUser, 0, len(rows))
	for _, row := range rows {
		users = append(users, buildSuperAdminUser(row.ID, row.CreatedAt, row.TeamIds, row.TeamNames, profiles))
	}

	var nextCursor *string
	if hasMore && len(rows) > 0 {
		last := rows[len(rows)-1]
		cursor := fmt.Sprintf("%s|%s", last.CreatedAt.UTC().Format(time.RFC3339Nano), last.ID.String())
		nextCursor = &cursor
	}

	c.JSON(http.StatusOK, api.SuperAdminUsersResponse{
		Data:       users,
		NextCursor: nextCursor,
	})
}

// searchSuperAdminUsers resolves matching user ids from the identity directory
// (email/name substring), hydrates them from the database, and returns a single
// unpaginated page. email/name live only in the identity provider, so search
// cannot be expressed as a SQL filter over the users table.
func (s *APIStore) searchSuperAdminUsers(c *gin.Context, search string) {
	ctx := c.Request.Context()

	if len(search) < superAdminUserSearchMinLen {
		c.JSON(http.StatusOK, api.SuperAdminUsersResponse{Data: []api.SuperAdminUser{}})

		return
	}

	userIDs, err := s.identityService.FindUserIDsByQuery(ctx, search, superAdminUserSearchMaxResults)
	if err != nil {
		logger.L().Error(ctx, "Error searching users via identity directory", zap.Error(err))
		s.sendAPIStoreError(c, http.StatusInternalServerError, "Error when searching users")

		return
	}
	if len(userIDs) == 0 {
		c.JSON(http.StatusOK, api.SuperAdminUsersResponse{Data: []api.SuperAdminUser{}})

		return
	}

	rows, err := s.db.Dashboard.ListUsersByIDs(ctx, userIDs)
	if err != nil {
		logger.L().Error(ctx, "Error hydrating searched users", zap.Error(err))
		s.sendAPIStoreError(c, http.StatusInternalServerError, "Error when getting users")

		return
	}

	profiles := s.resolveUserProfiles(ctx, userIDs)

	users := make([]api.SuperAdminUser, 0, len(rows))
	for _, row := range rows {
		users = append(users, buildSuperAdminUser(row.ID, row.CreatedAt, row.TeamIds, row.TeamNames, profiles))
	}

	c.JSON(http.StatusOK, api.SuperAdminUsersResponse{Data: users})
}

// resolveUserProfiles fetches email/name from the identity service; a failure
// is non-fatal and yields an empty map so the list still renders.
func (s *APIStore) resolveUserProfiles(ctx context.Context, userIDs []uuid.UUID) map[uuid.UUID]identity.Profile {
	if len(userIDs) == 0 {
		return map[uuid.UUID]identity.Profile{}
	}

	resolved, err := s.identityService.ProfilesByUserID(ctx, userIDs)
	if err != nil {
		logger.L().Error(ctx, "failed to resolve user profiles", zap.Error(err))

		return map[uuid.UUID]identity.Profile{}
	}

	return resolved
}

func buildSuperAdminUser(
	id uuid.UUID,
	createdAt time.Time,
	teamIDs []uuid.UUID,
	teamNames []string,
	profiles map[uuid.UUID]identity.Profile,
) api.SuperAdminUser {
	entry := api.SuperAdminUser{
		UserID:    id,
		CreatedAt: createdAt,
	}
	// team_ids and team_names are aggregated in the same order.
	for i, teamID := range teamIDs {
		name := ""
		if i < len(teamNames) {
			name = teamNames[i]
		}
		entry.Teams = append(entry.Teams, struct {
			Name   string             `json:"name"`
			TeamID openapi_types.UUID `json:"teamID"`
		}{Name: name, TeamID: teamID})
	}
	if p, ok := profiles[id]; ok {
		if p.Email != "" {
			email := p.Email
			entry.Email = &email
		}
		if p.Name != "" {
			name := p.Name
			entry.Name = &name
		}
	}

	return entry
}
