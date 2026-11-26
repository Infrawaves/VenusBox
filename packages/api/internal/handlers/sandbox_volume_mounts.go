package handlers

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/utils"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/shared/pkg/ginutils"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// maxAppendVolumeMounts matches the orchestrator's MaxAppendVolumeMounts and the
// spec's maxItems.
const maxAppendVolumeMounts = 2

// PostSandboxesSandboxIDVolumeMounts mounts existing team volumes into a running
// sandbox, next to the ones it has.
func (a *APIStore) PostSandboxesSandboxIDVolumeMounts(c *gin.Context, sandboxID string) {
	ctx := c.Request.Context()

	var err error
	sandboxID, err = utils.ShortID(sandboxID)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, "Invalid sandbox ID")

		return
	}

	teamID := auth.MustGetTeamID(c)

	body, err := ginutils.ParseBody[api.SandboxVolumeMountsAppend](ctx, c)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("Error when parsing request: %s", err))

		return
	}
	if len(body.VolumeMounts) == 0 {
		a.sendAPIStoreError(c, http.StatusBadRequest, "volumeMounts must not be empty")

		return
	}
	// The node holds the sandbox's update lock while envd mounts, up to 25s per
	// volume, and the call must end within the request deadline.
	if len(body.VolumeMounts) > maxAppendVolumeMounts {
		a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("at most %d volume mounts can be appended per request", maxAppendVolumeMounts))

		return
	}
	for index, m := range body.VolumeMounts {
		if m.Path == "/" {
			a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("volume mount #%d: path must not be '/'", index))

			return
		}
	}

	// No envd version check, not even create's: the recorded version is the
	// template's, which a live or offline envd upgrade leaves behind. An envd
	// without /volumes/mount answers 404, which the node reports and the API
	// turns into a 400.
	mounts, err := convertAPIVolumesToOrchestratorVolumes(ctx, a.sqlcDB, a.featureFlags, teamID, body.VolumeMounts, nil)
	if err != nil {
		apiErr := volumeMountsAPIError(ctx, err, sandboxID)
		a.sendAPIStoreError(c, apiErr.Code, apiErr.ClientMsg)

		return
	}

	if apiErr := a.orchestrator.AppendSandboxVolumeMounts(ctx, teamID, sandboxID, mounts); apiErr != nil {
		telemetry.ReportErrorByCode(ctx, apiErr.Code, "error appending sandbox volume mounts", apiErr.Err)
		a.sendAPIStoreError(c, apiErr.Code, apiErr.ClientMsg)

		return
	}

	c.Status(http.StatusNoContent)
}
