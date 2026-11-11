package handlers

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/dashboard-api/internal/api"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	"github.com/e2b-dev/infra/packages/db/pkg/volumequota"
)

func TestProjectVolumeQuotaHasIndependentRevisionsAndDefaultInheritance(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	teamID := testutils.CreateTestTeam(t, db)
	store, auth := newLimitsStore(db)
	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(),
		`UPDATE public.volume_quota_defaults SET quota_gib = 7`))

	// A large sandbox-limits revision cannot suppress the first quota update.
	limits := strings.Replace(validLimitsBody, `"revision": 2`, `"revision": 100`, 1)
	require.Equal(t, http.StatusNoContent, callUpsertProjectLimits(t, store, teamID, limits).Code)

	for index, step := range []struct {
		body         string
		wantQuota    int64
		wantRevision int64
	}{
		{`{"revision":1,"quota_gib":100}`, 100, 1},
		{`{"revision":2,"quota_gib":10}`, 10, 2},
		{`{"revision":1,"quota_gib":100}`, 10, 2},
		{`{"revision":2,"quota_gib":500}`, 10, 2},
		{`{"revision":3,"quota_gib":0}`, 0, 3},
		{`{"revision":4,"quota_gib":null}`, 7, 4},
		{`{"revision":3,"quota_gib":100}`, 7, 4},
	} {
		recorder := callUpsertProjectVolumeQuota(t, store, teamID, step.body)
		require.Equal(t, http.StatusNoContent, recorder.Code, recorder.Body.String())
		assertVolumeQuota(t, db, teamID, step.wantQuota, step.wantRevision)
		if index == 0 {
			limits = strings.Replace(validLimitsBody, `"revision": 2`, `"revision": 101`, 1)
			require.Equal(t, http.StatusNoContent, callUpsertProjectLimits(t, store, teamID, limits).Code)
			assertVolumeQuota(t, db, teamID, 100, 1)
		}
	}
	require.Equal(t, []uuid.UUID{teamID, teamID}, auth.invalidated,
		"quota updates have no dependency on the authentication cache")

	// A later full limits push cannot recreate or overwrite the quota override.
	limits = strings.Replace(validLimitsBody, `"revision": 2`, `"revision": 102`, 1)
	require.Equal(t, http.StatusNoContent, callUpsertProjectLimits(t, store, teamID, limits).Code)
	assertVolumeQuota(t, db, teamID, 7, 4)

	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(),
		`UPDATE public.volume_quota_defaults SET quota_gib = 9`))
	assertVolumeQuota(t, db, teamID, 9, 4)
}

func TestProjectVolumeQuotaRejectsInvalidRequestsBeforeWriting(t *testing.T) {
	t.Parallel()

	for name, body := range map[string]string{
		"malformed":        `{"revision":`,
		"missing quota":    `{"revision":1}`,
		"missing revision": `{"quota_gib":10}`,
		"zero revision":    `{"revision":0,"quota_gib":10}`,
		"negative quota":   `{"revision":1,"quota_gib":-1}`,
		"overflow quota":   `{"revision":1,"quota_gib":2147483648}`,
		"fractional quota": `{"revision":1,"quota_gib":1.5}`,
		"string quota":     `{"revision":1,"quota_gib":"10"}`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			recorder := callUpsertProjectVolumeQuota(t, &APIStore{}, uuid.New(), body)
			require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
		})
	}
}

func TestProjectVolumeQuotaRejectsAnUnknownProject(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store, _ := newLimitsStore(db)
	recorder := callUpsertProjectVolumeQuota(t, store, uuid.New(), `{"revision":1,"quota_gib":10}`)
	require.Equal(t, http.StatusNotFound, recorder.Code, recorder.Body.String())
}

func TestProjectVolumeQuotaContractRequiresServiceAuthAndExplicitQuota(t *testing.T) {
	t.Parallel()

	swagger, err := api.GetSwagger()
	require.NoError(t, err)
	path := swagger.Paths.Find("/v1/management/projects/{projectID}/volume-quota")
	require.NotNil(t, path)
	require.Equal(t, openapi3.SecurityRequirements{{"AdminJWTAuth": {}}}, *path.Put.Security)
	schema := swagger.Components.Schemas["ManagementProjectVolumeQuota"].Value
	for _, quota := range []any{nil, float64(0), float64(100), float64(math.MaxInt32)} {
		require.NoError(t, schema.VisitJSON(map[string]any{"revision": float64(1), "quota_gib": quota}))
	}
	for _, body := range []string{
		`{"revision":1}`, `{"quota_gib":100}`, `{"revision":0,"quota_gib":100}`,
		`{"revision":1,"quota_gib":-1}`, `{"revision":1,"quota_gib":2147483648}`,
	} {
		var value any
		require.NoError(t, json.Unmarshal([]byte(body), &value))
		require.Error(t, schema.VisitJSON(value), body)
	}
}

func assertVolumeQuota(t *testing.T, db *testutils.Database, teamID uuid.UUID, quotaGiB, revision int64) {
	t.Helper()

	require.NoError(t, volumequota.New(db.SqlcClient).WithTeam(t.Context(), teamID, func(_ context.Context, team *volumequota.LockedTeam) error {
		require.Equal(t, quotaGiB, team.QuotaGiB)
		require.Equal(t, revision, team.Revision)

		return nil
	}))
}

func callUpsertProjectVolumeQuota(t *testing.T, store *APIStore, teamID uuid.UUID, body string) *httptest.ResponseRecorder {
	t.Helper()

	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Request = httptest.NewRequestWithContext(t.Context(), http.MethodPut,
		"/v1/management/projects/"+teamID.String()+"/volume-quota", strings.NewReader(body))
	ginCtx.Request.Header.Set("Content-Type", "application/json")
	store.ManagementUpsertProjectVolumeQuota(ginCtx, teamID)
	ginCtx.Writer.WriteHeaderNow()

	return recorder
}
