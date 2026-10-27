package handlers

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
)

func TestSuperAdminSandboxFromInstance(t *testing.T) {
	t.Parallel()

	teamID := uuid.New()
	alias := "my-alias"
	info := sandbox.Sandbox{
		SandboxID:       "sbx-1",
		TeamID:          teamID,
		BaseTemplateID:  "tmpl-1",
		ClientID:        "client-1",
		Alias:           &alias,
		StartTime:       time.Now(),
		EndTime:         time.Now().Add(time.Hour),
		VCpu:            2,
		RamMB:           1024,
		TotalDiskSizeMB: 4096,
		EnvdVersion:     "0.2.0",
		Metadata:        map[string]string{"k": "v"},
	}

	out := superAdminSandboxFromInstance(info)

	assert.Equal(t, teamID, out.TeamID)
	assert.Equal(t, "sbx-1", out.SandboxID)
	assert.Equal(t, "tmpl-1", out.TemplateID)
	assert.Equal(t, api.Running, out.State)
	assert.Equal(t, int32(2), out.CpuCount)
	assert.Equal(t, int32(1024), out.MemoryMB)
	assert.NotNil(t, out.Metadata)
}

func TestSuperAdminSandboxFromInstanceNilMetadata(t *testing.T) {
	t.Parallel()

	info := sandbox.Sandbox{
		SandboxID: "sbx-2",
		TeamID:    uuid.New(),
		Metadata:  nil,
	}

	out := superAdminSandboxFromInstance(info)
	assert.Nil(t, out.Metadata)
}
