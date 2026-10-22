package handlers

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/dashboard-api/internal/api"
	dashboardqueries "github.com/e2b-dev/infra/packages/db/pkg/dashboard/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

func (s *APIStore) GetSuperAdminTeams(c *gin.Context, params api.GetSuperAdminTeamsParams) {
	ctx := c.Request.Context()
	telemetry.ReportEvent(ctx, "list all teams (super-admin)")

	limit := normalizeBuildsLimit(params.Limit)
	cursorTime, cursorID, err := parseBuildsCursor(params.Cursor)
	if err != nil {
		s.sendAPIStoreError(c, http.StatusBadRequest, "invalid cursor")

		return
	}

	// Non-nil so pgx sends an empty array (not NULL); the query treats an empty
	// array as "no status filter". A nil slice would encode as NULL and make
	// cardinality()/ANY() yield NULL, excluding every row.
	statuses := []string{}
	if params.Statuses != nil {
		statuses = *params.Statuses
	}

	rows, err := s.db.Dashboard.ListAllTeams(ctx, dashboardqueries.ListAllTeamsParams{
		CursorCreatedAt: cursorTime,
		CursorID:        cursorID,
		Search:          strings.TrimSpace(utils.DerefOrDefault(params.Search, "")),
		Statuses:        statuses,
		LimitPlusOne:    limit + 1,
	})
	if err != nil {
		logger.L().Error(ctx, "Error getting all teams", zap.Error(err))
		s.sendAPIStoreError(c, http.StatusInternalServerError, "Error when getting teams")

		return
	}

	hasMore := len(rows) > int(limit)
	if hasMore {
		rows = rows[:limit]
	}

	teams := make([]api.SuperAdminTeam, 0, len(rows))
	for _, row := range rows {
		teams = append(teams, superAdminTeamToAPI(row))
	}

	var nextCursor *string
	if hasMore && len(rows) > 0 {
		last := rows[len(rows)-1]
		cursor := fmt.Sprintf("%s|%s", last.Team.CreatedAt.UTC().Format(time.RFC3339Nano), last.Team.ID.String())
		nextCursor = &cursor
	}

	c.JSON(http.StatusOK, api.SuperAdminTeamsResponse{
		Data:       teams,
		NextCursor: nextCursor,
	})
}

func superAdminTeamToAPI(row dashboardqueries.ListAllTeamsRow) api.SuperAdminTeam {
	return api.SuperAdminTeam{
		Id:                row.Team.ID,
		Name:              row.Team.Name,
		Slug:              row.Team.Slug,
		Tier:              row.Team.Tier,
		Email:             row.Team.Email,
		ProfilePictureUrl: row.Team.ProfilePictureUrl,
		IsBlocked:         row.Team.IsBlocked,
		IsBanned:          row.Team.IsBanned,
		BlockedReason:     row.Team.BlockedReason,
		CreatedAt:         row.Team.CreatedAt,
		Limits: api.UserTeamLimits{
			MaxLengthHours:           row.TeamLimit.MaxLengthHours,
			ConcurrentSandboxes:      row.TeamLimit.ConcurrentSandboxes,
			ConcurrentTemplateBuilds: row.TeamLimit.ConcurrentTemplateBuilds,
			MaxVcpu:                  row.TeamLimit.MaxVcpu,
			MaxRamMb:                 row.TeamLimit.MaxRamMb,
			DiskMb:                   row.TeamLimit.DiskMb,
			EventsTtlDays:            row.TeamLimit.EventsTtlDays,
			DefaultFreeDiskSizeMb:    &row.TeamLimit.DefaultFreeDiskSizeMb,
			MaxDiskSizeMb:            &row.TeamLimit.MaxDiskSizeMb,
		},
	}
}
