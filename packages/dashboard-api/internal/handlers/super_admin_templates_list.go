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

// parseSuperAdminTemplatesSort resolves the super-admin sort param. The global
// view supports only creation-time ordering (both directions); updated_at sorts
// would need an extra query pair and aren't needed here.
func parseSuperAdminTemplatesSort(value *api.GetSuperAdminTemplatesParamsSort) (templatesSort, error) {
	if value == nil {
		return templatesSortCreatedAtDesc, nil
	}

	switch templatesSort(*value) {
	case templatesSortCreatedAtAsc, templatesSortCreatedAtDesc:
		return templatesSort(*value), nil
	default:
		return "", errors.New("unsupported sort")
	}
}

func (s *APIStore) GetSuperAdminTemplates(c *gin.Context, params api.GetSuperAdminTemplatesParams) {
	ctx := c.Request.Context()
	telemetry.ReportEvent(ctx, "list all templates (super-admin)")

	sort, err := parseSuperAdminTemplatesSort(params.Sort)
	if err != nil {
		s.sendAPIStoreError(c, http.StatusBadRequest, "sort must be created_at_asc or created_at_desc")

		return
	}
	asc := sort == templatesSortCreatedAtAsc

	limit := normalizeTemplatesLimit(params.Limit)

	cursorValue, cursorID, err := parseTemplatesCursor(params.Cursor, sort)
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
	ct, cid := timeCursor(cursorTs, cursorID, !asc)

	search := strings.TrimSpace(utils.DerefOrDefault(params.Search, ""))

	var rows []dashboardqueries.ListAllTemplatesByCreatedAtDescRow
	if asc {
		ascRows, qErr := s.db.Dashboard.ListAllTemplatesByCreatedAtAsc(ctx, dashboardqueries.ListAllTemplatesByCreatedAtAscParams{
			Search:          search,
			Public:          params.Public,
			TeamID:          params.TeamId,
			CursorCreatedAt: ct,
			CursorID:        cid,
			LimitPlusOne:    limit + 1,
		})
		if qErr != nil {
			err = qErr
		} else {
			rows = make([]dashboardqueries.ListAllTemplatesByCreatedAtDescRow, len(ascRows))
			for i, r := range ascRows {
				rows[i] = dashboardqueries.ListAllTemplatesByCreatedAtDescRow(r)
			}
		}
	} else {
		rows, err = s.db.Dashboard.ListAllTemplatesByCreatedAtDesc(ctx, dashboardqueries.ListAllTemplatesByCreatedAtDescParams{
			Search:          search,
			Public:          params.Public,
			TeamID:          params.TeamId,
			CursorCreatedAt: ct,
			CursorID:        cid,
			LimitPlusOne:    limit + 1,
		})
	}
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
		cursor := formatTemplatesCursor(sort, last.CreatedAt.UTC().Format(time.RFC3339Nano), last.TemplateID)
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
