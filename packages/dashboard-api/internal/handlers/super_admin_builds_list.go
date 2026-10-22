package handlers

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/dashboard-api/internal/api"
	dashboardutils "github.com/e2b-dev/infra/packages/dashboard-api/internal/utils"
	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

func (s *APIStore) GetSuperAdminBuilds(c *gin.Context, params api.GetSuperAdminBuildsParams) {
	ctx := c.Request.Context()
	telemetry.ReportEvent(ctx, "list all builds (super-admin)")

	limit := normalizeBuildsLimit(params.Limit)
	cursorTime, cursorID, err := parseBuildsCursor(params.Cursor)
	if err != nil {
		s.sendAPIStoreError(c, http.StatusBadRequest, "invalid cursor")

		return
	}

	statusGroups := dashboardutils.MapBuildStatusesToDBStatusGroups(params.Statuses)
	rows, err := s.db.GetAllBuildsPage(ctx, queries.GetAllBuildsPageParams{
		CursorCreatedAt: cursorTime,
		CursorID:        cursorID,
		Statuses:        buildStatusGroupsToStrings(statusGroups),
		Search:          strings.TrimSpace(utils.DerefOrDefault(params.Search, "")),
		LimitPlusOne:    limit + 1,
	})
	if err != nil {
		logger.L().Error(ctx, "Error getting all builds", zap.Error(err))
		s.sendAPIStoreError(c, http.StatusInternalServerError, "Error when getting builds")

		return
	}

	hasMore := len(rows) > int(limit)
	if hasMore {
		rows = rows[:limit]
	}

	builds := make([]api.SuperAdminBuild, 0, len(rows))
	for _, row := range rows {
		builds = append(builds, superAdminBuildToAPI(row))
	}

	var nextCursor *string
	if hasMore && len(rows) > 0 {
		last := rows[len(rows)-1]
		cursor := fmt.Sprintf("%s|%s", last.CreatedAt.UTC().Format(time.RFC3339Nano), last.ID.String())
		nextCursor = &cursor
	}

	c.JSON(http.StatusOK, api.SuperAdminBuildsResponse{
		Data:       builds,
		NextCursor: nextCursor,
	})
}

func superAdminBuildToAPI(row queries.GetAllBuildsPageRow) api.SuperAdminBuild {
	template := row.TemplateAlias
	if template == "" {
		template = row.TemplateID
	}

	var teamID uuid.UUID
	if row.TeamID != nil {
		teamID = *row.TeamID
	}

	return api.SuperAdminBuild{
		Id:            row.ID,
		TeamID:        teamID,
		TeamName:      row.TeamName,
		Template:      template,
		TemplateId:    row.TemplateID,
		Status:        dashboardutils.MapBuildStatusFromDBStatusGroup(row.StatusGroup),
		StatusMessage: dashboardutils.MapBuildStatusMessageFromDBStatusGroup(row.StatusGroup, row.Reason),
		CreatedAt:     row.CreatedAt,
		FinishedAt:    row.FinishedAt,
		CpuCount:      row.Vcpu,
		MemoryMB:      row.RamMb,
		DiskSizeMB:    row.TotalDiskSizeMb,
		EnvdVersion:   row.EnvdVersion,
	}
}
