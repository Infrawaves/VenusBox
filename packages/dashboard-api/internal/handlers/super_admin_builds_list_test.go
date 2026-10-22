package handlers

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/e2b-dev/infra/packages/dashboard-api/internal/api"
	dbtypes "github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/db/queries"
)

func TestSuperAdminBuildToAPI(t *testing.T) {
	t.Parallel()

	teamID := uuid.New()
	row := queries.GetAllBuildsPageRow{
		ID:            uuid.New(),
		TeamID:        &teamID,
		TeamName:      "acme",
		TemplateID:    "tmpl-1",
		TemplateAlias: "my-alias",
		StatusGroup:   dbtypes.BuildStatusGroupReady,
		Vcpu:          2,
		RamMb:         1024,
	}

	out := superAdminBuildToAPI(row)

	assert.Equal(t, teamID, out.TeamID)
	assert.Equal(t, "acme", out.TeamName)
	assert.Equal(t, "my-alias", out.Template)
	assert.Equal(t, "tmpl-1", out.TemplateId)
	assert.Equal(t, api.BuildStatus("success"), out.Status)
	assert.Equal(t, int64(2), out.CpuCount)
}

func TestSuperAdminBuildToAPITemplateFallback(t *testing.T) {
	t.Parallel()

	teamID := uuid.New()
	row := queries.GetAllBuildsPageRow{
		ID:            uuid.New(),
		TeamID:        &teamID,
		TemplateID:    "tmpl-2",
		TemplateAlias: "",
		StatusGroup:   dbtypes.BuildStatusGroupInProgress,
	}

	out := superAdminBuildToAPI(row)
	assert.Equal(t, "tmpl-2", out.Template)
}
