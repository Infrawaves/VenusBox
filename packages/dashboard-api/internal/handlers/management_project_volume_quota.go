package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/e2b-dev/infra/packages/dashboard-api/internal/api"
	"github.com/e2b-dev/infra/packages/db/pkg/volumequota"
	"github.com/e2b-dev/infra/packages/shared/pkg/ginutils"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// ManagementUpsertProjectVolumeQuota records only the desired volume quota.
// Its revision and reads are independent of the authentication limits cache.
func (s *APIStore) ManagementUpsertProjectVolumeQuota(c *gin.Context, projectID api.ProjectID) {
	ctx := c.Request.Context()
	// A raw field distinguishes the required explicit null (inherit) from an
	// omitted field, which must not accidentally clear a project's override.
	body, err := ginutils.ParseBody[struct {
		Revision int64           `json:"revision"`
		QuotaGiB json.RawMessage `json:"quota_gib"`
	}](ctx, c)
	if err != nil || len(body.QuotaGiB) == 0 {
		s.sendAPIStoreError(c, http.StatusBadRequest, "Invalid request body; revision and quota_gib are required")

		return
	}

	var quotaGiB *int64
	if err := json.Unmarshal(body.QuotaGiB, &quotaGiB); err != nil ||
		body.Revision < 1 || quotaGiB != nil && (*quotaGiB < 0 || *quotaGiB > math.MaxInt32) {
		s.sendAPIStoreError(c, http.StatusBadRequest,
			"revision must be positive; quota_gib must be null or an integer from 0 to 2147483647")

		return
	}

	err = volumequota.New(s.db).WithTeam(ctx, projectID, func(ctx context.Context, team *volumequota.LockedTeam) error {
		_, err := team.SetQuota(ctx, body.Revision, quotaGiB)

		return err
	})
	if err != nil {
		switch {
		case errors.Is(err, volumequota.ErrTeamNotFound):
			s.sendAPIStoreError(c, http.StatusNotFound, "Project not found")
		case errors.Is(err, volumequota.ErrInvalidQuota), errors.Is(err, volumequota.ErrInvalidRevision):
			s.sendAPIStoreError(c, http.StatusBadRequest, "Invalid volume quota")
		default:
			telemetry.ReportCriticalError(ctx, "upsert project volume quota failed", err,
				telemetry.WithTeamID(projectID.String()))
			s.sendAPIStoreError(c, http.StatusInternalServerError, "Error updating project volume quota")
		}

		return
	}

	c.Status(http.StatusNoContent)
}
