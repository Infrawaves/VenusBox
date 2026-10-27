package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

func (a *APIStore) GetSuperAdminSandboxes(c *gin.Context) {
	ctx := c.Request.Context()
	telemetry.ReportEvent(ctx, "list all running sandboxes (super-admin)")

	sandboxes, err := a.orchestrator.GetAllSandboxes(ctx, []sandbox.State{sandbox.StateRunning})
	if err != nil {
		logger.L().Error(ctx, "Error getting all sandboxes", zap.Error(err))
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error when getting sandboxes")

		return
	}

	result := make([]api.SuperAdminSandbox, 0, len(sandboxes))
	for _, info := range sandboxes {
		result = append(result, superAdminSandboxFromInstance(info))
	}

	c.JSON(http.StatusOK, result)
}

func superAdminSandboxFromInstance(info sandbox.Sandbox) api.SuperAdminSandbox {
	sbx := api.SuperAdminSandbox{
		TeamID:       info.TeamID,
		ClientID:     info.ClientID,
		TemplateID:   info.BaseTemplateID,
		Alias:        info.Alias,
		SandboxID:    info.SandboxID,
		StartedAt:    info.StartTime,
		CpuCount:     api.CPUCount(info.VCpu),
		MemoryMB:     api.MemoryMB(info.RamMB),
		DiskSizeMB:   api.DiskSizeMB(info.TotalDiskSizeMB),
		EndAt:        info.EndTime,
		State:        api.Running,
		EnvdVersion:  info.EnvdVersion,
		VolumeMounts: convertFromDBMountsToAPIMounts(info.VolumeMounts),
	}

	if info.Metadata != nil {
		meta := api.SandboxMetadata(info.Metadata)
		sbx.Metadata = &meta
	}

	return sbx
}
