package handlers

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dashboardqueries "github.com/e2b-dev/infra/packages/db/pkg/dashboard/queries"
)

func TestSuperAdminTeamToAPI(t *testing.T) {
	t.Parallel()

	teamID := uuid.New()
	reason := "spam"
	row := dashboardqueries.ListAllTeamsRow{
		Team: dashboardqueries.Team{
			ID:            teamID,
			Name:          "acme",
			Slug:          "acme",
			Tier:          "base_v1",
			Email:         "team@acme.test",
			IsBlocked:     true,
			IsBanned:      false,
			BlockedReason: &reason,
		},
		TeamLimit: dashboardqueries.TeamLimit{
			ID:                  teamID,
			ConcurrentSandboxes: 20,
			MaxVcpu:             8,
		},
	}

	out := superAdminTeamToAPI(row)

	assert.Equal(t, teamID, out.Id)
	assert.Equal(t, "acme", out.Name)
	assert.Equal(t, "base_v1", out.Tier)
	assert.True(t, out.IsBlocked)
	require.NotNil(t, out.BlockedReason)
	assert.Equal(t, "spam", *out.BlockedReason)
	assert.Equal(t, int64(20), out.Limits.ConcurrentSandboxes)
	assert.Equal(t, int64(8), out.Limits.MaxVcpu)
}
