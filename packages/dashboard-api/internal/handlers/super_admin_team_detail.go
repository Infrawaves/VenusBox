package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/dashboard-api/internal/api"
	dashboardqueries "github.com/e2b-dev/infra/packages/db/pkg/dashboard/queries"
	"github.com/e2b-dev/infra/packages/db/pkg/dberrors"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// GetSuperAdminTeamsTeamID returns a single team with its limits, members, and templates.
func (s *APIStore) GetSuperAdminTeamsTeamID(c *gin.Context, teamID api.TeamID) {
	ctx := c.Request.Context()
	telemetry.ReportEvent(ctx, "get team detail (super-admin)")

	row, err := s.db.Dashboard.GetSuperAdminTeam(ctx, teamID)
	if err != nil {
		if dberrors.IsNotFoundError(err) {
			s.sendAPIStoreError(c, http.StatusNotFound, "Team not found")

			return
		}
		logger.L().Error(ctx, "Error getting team detail", zap.Error(err))
		s.sendAPIStoreError(c, http.StatusInternalServerError, "Error when getting team")

		return
	}

	team := superAdminTeamToAPI(dashboardqueries.ListAllTeamsRow{
		Team:      row.Team,
		TeamLimit: row.TeamLimit,
	})

	members := s.superAdminTeamMembers(c, teamID)
	templates := s.superAdminTeamTemplates(c, teamID, team.Name)

	c.JSON(http.StatusOK, api.SuperAdminTeamDetail{
		Team:      team,
		Members:   members,
		Templates: templates,
	})
}

// superAdminTeamMembers resolves the team's members with their identity
// profiles. A lookup failure is non-fatal and yields an empty list.
func (s *APIStore) superAdminTeamMembers(c *gin.Context, teamID uuid.UUID) []api.SuperAdminTeamMember {
	ctx := c.Request.Context()

	rows, err := s.db.GetTeamMembers(ctx, teamID)
	if err != nil {
		logger.L().Error(ctx, "Error getting team members", zap.Error(err))

		return []api.SuperAdminTeamMember{}
	}

	userIDs := make([]uuid.UUID, 0, len(rows))
	for _, row := range rows {
		userIDs = append(userIDs, row.UserID)
	}
	profiles := s.resolveUserProfiles(ctx, userIDs)

	members := make([]api.SuperAdminTeamMember, 0, len(rows))
	for _, row := range rows {
		member := api.SuperAdminTeamMember{
			UserID:    row.UserID,
			IsDefault: row.IsDefault,
			AddedBy:   row.AddedBy,
		}
		if p, ok := profiles[row.UserID]; ok {
			if p.Email != "" {
				email := p.Email
				member.Email = &email
			}
			if p.Name != "" {
				name := p.Name
				member.Name = &name
			}
		}
		if row.CreatedAt.Valid {
			t := row.CreatedAt.Time.UTC()
			member.CreatedAt = &t
		}
		members = append(members, member)
	}

	return members
}

// superAdminTeamTemplates returns the team's own templates. A lookup failure is
// non-fatal and yields an empty list.
func (s *APIStore) superAdminTeamTemplates(c *gin.Context, teamID uuid.UUID, teamName string) []api.SuperAdminTemplate {
	ctx := c.Request.Context()

	rows, err := s.db.GetTeamTemplates(ctx, teamID)
	if err != nil {
		logger.L().Error(ctx, "Error getting team templates", zap.Error(err))

		return []api.SuperAdminTemplate{}
	}

	templates := make([]api.SuperAdminTemplate, 0, len(rows))
	for _, row := range rows {
		var createdBy *struct {
			Email *string             `json:"email,omitempty"`
			Id    uuid.UUID           `json:"id"`
		}
		if row.CreatorID != nil {
			createdBy = &struct {
				Email *string   `json:"email,omitempty"`
				Id    uuid.UUID `json:"id"`
			}{Id: *row.CreatorID}
		}

		aliases := row.Aliases
		if aliases == nil {
			aliases = []string{}
		}
		names := row.Names
		if names == nil {
			names = []string{}
		}

		templates = append(templates, api.SuperAdminTemplate{
			TemplateID:    row.ActiveEnv.ID,
			TeamID:        teamID,
			TeamName:      teamName,
			BuildID:       row.BuildID,
			CpuCount:      row.BuildVcpu,
			MemoryMB:      row.BuildRamMb,
			DiskSizeMB:    row.BuildTotalDiskSizeMb,
			Public:        row.ActiveEnv.Public,
			Aliases:       aliases,
			Names:         names,
			CreatedAt:     row.ActiveEnv.CreatedAt,
			UpdatedAt:     row.ActiveEnv.UpdatedAt,
			CreatedBy:     createdBy,
			LastSpawnedAt: row.ActiveEnv.LastSpawnedAt,
			SpawnCount:    row.ActiveEnv.SpawnCount,
			BuildCount:    row.ActiveEnv.BuildCount,
			EnvdVersion:   row.BuildEnvdVersion,
			IsDefault:     false,
		})
	}

	return templates
}
