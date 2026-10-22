package handlers

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	openapi_types "github.com/oapi-codegen/runtime/types"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/dashboard-api/internal/api"
	dashboardqueries "github.com/e2b-dev/infra/packages/db/pkg/dashboard/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// superAdminTemplatesSort is fixed: the global view paginates by creation time descending only.
const superAdminTemplatesSort = templatesSortCreatedAtDesc

func (s *APIStore) GetSuperAdminTemplates(c *gin.Context, params api.GetSuperAdminTemplatesParams) {
	ctx := c.Request.Context()
	telemetry.ReportEvent(ctx, "list all templates (super-admin)")

	limit := normalizeTemplatesLimit(params.Limit)

	cursorValue, cursorID, err := parseTemplatesCursor(params.Cursor, superAdminTemplatesSort)
	if err != nil {
		if errors.Is(err, errTemplatesCursorSortMismatch) {
			s.sendAPIStoreError(c, http.StatusBadRequest, "Cursor was issued for a different sort; clear the cursor and restart")
		} else {
			s.sendAPIStoreError(c, http.StatusBadRequest, "Invalid cursor")
		}

		return
	}

	cursorTs, err := cursorTime(cursorValue)
	if err != nil {
		s.sendAPIStoreError(c, http.StatusBadRequest, "Invalid cursor")

		return
	}
	ct, cid := timeCursor(cursorTs, cursorID, true)

	rows, err := s.db.Dashboard.ListAllTemplatesByCreatedAtDesc(ctx, dashboardqueries.ListAllTemplatesByCreatedAtDescParams{
		Search:          strings.TrimSpace(utils.DerefOrDefault(params.Search, "")),
		Public:          params.Public,
		CursorCreatedAt: ct,
		CursorID:        cid,
		LimitPlusOne:    limit + 1,
	})
	if err != nil {
		logger.L().Error(ctx, "Error listing all templates", zap.Error(err))
		s.sendAPIStoreError(c, http.StatusInternalServerError, "Error when listing templates")

		return
	}

	hasMore := len(rows) > int(limit)
	if hasMore {
		rows = rows[:limit]
	}

	templates := make([]api.SuperAdminTemplate, 0, len(rows))
	for _, row := range rows {
		templates = append(templates, superAdminTemplateToAPI(row))
	}

	var nextCursor *string
	if hasMore && len(rows) > 0 {
		last := rows[len(rows)-1]
		cursor := formatTemplatesCursor(superAdminTemplatesSort, last.CreatedAt.UTC().Format(time.RFC3339Nano), last.TemplateID)
		nextCursor = &cursor
	}

	c.JSON(http.StatusOK, api.SuperAdminTemplatesResponse{
		Data:       templates,
		NextCursor: nextCursor,
	})
}

func superAdminTemplateToAPI(row dashboardqueries.ListAllTemplatesByCreatedAtDescRow) api.SuperAdminTemplate {
	var createdBy *struct {
		Email *string            `json:"email,omitempty"`
		Id    openapi_types.UUID `json:"id"`
	}
	if row.CreatorID != nil {
		createdBy = &struct {
			Email *string            `json:"email,omitempty"`
			Id    openapi_types.UUID `json:"id"`
		}{
			Id: *row.CreatorID,
		}
	}

	aliases := row.Aliases
	if aliases == nil {
		aliases = []string{}
	}
	names := row.Names
	if names == nil {
		names = []string{}
	}

	return api.SuperAdminTemplate{
		TemplateID:    row.TemplateID,
		TeamID:        row.TeamID,
		TeamName:      row.TeamName,
		BuildID:       row.BuildID,
		CpuCount:      row.CpuCount,
		MemoryMB:      row.MemoryMb,
		DiskSizeMB:    row.DiskSizeMb,
		Public:        row.Public,
		Aliases:       aliases,
		Names:         names,
		CreatedAt:     row.CreatedAt,
		UpdatedAt:     row.UpdatedAt,
		CreatedBy:     createdBy,
		LastSpawnedAt: row.LastSpawnedAt,
		SpawnCount:    row.SpawnCount,
		BuildCount:    row.BuildCount,
		EnvdVersion:   row.EnvdVersion,
		IsDefault:     false,
	}
}
