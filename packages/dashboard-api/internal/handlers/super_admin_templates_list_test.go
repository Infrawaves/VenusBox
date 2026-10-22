package handlers

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dashboardqueries "github.com/e2b-dev/infra/packages/db/pkg/dashboard/queries"
)

func TestSuperAdminTemplateToAPI(t *testing.T) {
	t.Parallel()

	teamID := uuid.New()
	creatorID := uuid.New()
	row := dashboardqueries.ListAllTemplatesByCreatedAtDescRow{
		TemplateID: "env-abc",
		TeamID:     teamID,
		TeamName:   "acme",
		CreatorID:  &creatorID,
		CpuCount:   2,
		MemoryMb:   1024,
		Aliases:    nil,
		Names:      nil,
	}

	out := superAdminTemplateToAPI(row)

	assert.Equal(t, "env-abc", out.TemplateID)
	assert.Equal(t, teamID, out.TeamID)
	assert.Equal(t, "acme", out.TeamName)
	assert.Equal(t, int64(2), out.CpuCount)
	assert.Equal(t, int64(1024), out.MemoryMB)
	assert.False(t, out.IsDefault)
	require.NotNil(t, out.CreatedBy)
	assert.Equal(t, creatorID, out.CreatedBy.Id)
	assert.NotNil(t, out.Aliases)
	assert.NotNil(t, out.Names)
}

func TestSuperAdminTemplateToAPINilCreator(t *testing.T) {
	t.Parallel()

	row := dashboardqueries.ListAllTemplatesByCreatedAtDescRow{
		TemplateID: "env-xyz",
		TeamID:     uuid.New(),
		TeamName:   "beta",
		CreatorID:  nil,
	}

	out := superAdminTemplateToAPI(row)
	assert.Nil(t, out.CreatedBy)
}
