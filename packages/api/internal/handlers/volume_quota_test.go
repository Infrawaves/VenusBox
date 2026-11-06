package handlers

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/volcengine/volcengine-go-sdk/service/vepfs"
	"github.com/volcengine/volcengine-go-sdk/volcengine"
	"github.com/volcengine/volcengine-go-sdk/volcengine/request"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/api/internal/cfg"
	"github.com/e2b-dev/infra/packages/api/internal/volumequota"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
)

type quotaVolumeClient struct {
	orchestrator.VolumeServiceClient

	calls       []string
	validateErr error
	createErr   error
	request     *orchestrator.CreateVolumeWithQuotaRequest
}

func (c *quotaVolumeClient) ValidateVolumeMount(_ context.Context, request *orchestrator.CreateVolumeWithQuotaRequest, _ ...grpc.CallOption) (*orchestrator.CreateVolumeResponse, error) {
	c.calls = append(c.calls, "validate")
	c.request = request

	return &orchestrator.CreateVolumeResponse{}, c.validateErr
}

func (c *quotaVolumeClient) CreateVolumeWithQuota(_ context.Context, request *orchestrator.CreateVolumeWithQuotaRequest, _ ...grpc.CallOption) (*orchestrator.CreateVolumeResponse, error) {
	c.calls = append(c.calls, "verified create")
	c.request = request

	return &orchestrator.CreateVolumeResponse{VolumeType: new(request.GetVolume().GetVolumeType())}, c.createErr
}

func (c *quotaVolumeClient) CreateVolume(_ context.Context, _ *orchestrator.CreateVolumeRequest, _ ...grpc.CallOption) (*orchestrator.CreateVolumeResponse, error) {
	c.calls = append(c.calls, "create")

	return &orchestrator.CreateVolumeResponse{}, nil
}

// fakeFilesets answers vePFS calls in memory: created filesets are Running at
// once. A non-nil createErr fails every CreateFileset.
type fakeFilesets struct {
	mu        sync.Mutex
	filesets  map[string]string // path -> fileset ID
	createErr error
}

func (f *fakeFilesets) DescribeFilesetsWithContext(_ volcengine.Context, in *vepfs.DescribeFilesetsInput, _ ...request.Option) (*vepfs.DescribeFilesetsOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	path := volcengine.StringValue(in.Filters[0].Value)
	id, ok := f.filesets[path]
	if !ok {
		return &vepfs.DescribeFilesetsOutput{}, nil
	}

	return &vepfs.DescribeFilesetsOutput{Filesets: []*vepfs.FilesetForDescribeFilesetsOutput{{
		FilesetId:   volcengine.String(id),
		FilesetPath: volcengine.String(path),
		Status:      volcengine.String(vepfs.EnumOfStatusForDescribeFilesetsOutputRunning),
	}}}, nil
}

func (f *fakeFilesets) CreateFilesetWithContext(_ volcengine.Context, in *vepfs.CreateFilesetInput, _ ...request.Option) (*vepfs.CreateFilesetOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.createErr != nil {
		return nil, f.createErr
	}
	if f.filesets == nil {
		f.filesets = map[string]string{}
	}
	id := "fset-" + uuid.NewString()
	f.filesets[volcengine.StringValue(in.FilesetPath)] = id

	return &vepfs.CreateFilesetOutput{FilesetId: volcengine.String(id)}, nil
}

func (f *fakeFilesets) SetFilesetQuotaWithContext(volcengine.Context, *vepfs.SetFilesetQuotaInput, ...request.Option) (*vepfs.SetFilesetQuotaOutput, error) {
	return &vepfs.SetFilesetQuotaOutput{}, nil
}

func quotaTestStore(t *testing.T, db *testutils.Database, api volumequota.FilesetAPI) *APIStore {
	t.Helper()

	return &APIStore{volumeQuota: volumequota.NewWithAPI(api, cfg.VolumeQuotaConfig{
		FileSystems: map[string]string{"vepfs": "vepfs-test"},
		Paths:       map[string]string{"vepfs": "/volumes"},
		Region:      "test", AccessKeyID: "test", SecretAccessKey: "test",
	}, db.SqlcClient)}
}

func TestManagedVolumeVerifiesMountBeforePinningQuota(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	teamID := testutils.CreateTestTeam(t, db)
	filesets := &fakeFilesets{}
	store := quotaTestStore(t, db, filesets)
	volume := queries.Volume{ID: uuid.New(), TeamID: teamID, VolumeType: "vepfs"}
	for _, code := range []codes.Code{codes.Unimplemented, codes.FailedPrecondition} {
		client := &quotaVolumeClient{validateErr: status.Error(code, "invalid mount")}
		_, err := store.createVolumeOnNode(t.Context(), client, volume)
		require.Equal(t, codes.Unavailable, status.Code(err))
		require.Equal(t, []string{"validate"}, client.calls)
		var count int
		require.NoError(t, db.SqlcClient.Pool().QueryRow(t.Context(),
			`SELECT count(*) FROM public.team_volume_quotas WHERE team_id = $1`, teamID).Scan(&count))
		require.Zero(t, count, "failed preflight must not permanently bind an incorrect path")
	}

	client := &quotaVolumeClient{}
	_, err := store.createVolumeOnNode(t.Context(), client, volume)
	require.NoError(t, err)
	require.Equal(t, []string{"validate", "verified create"}, client.calls)
	require.Equal(t, "vepfs-test", client.request.GetFileSystemId())
	require.Equal(t, "/volumes/team-"+teamID.String()+"/", client.request.GetFilesetPath())
	var fileSystemID, filesetPath string
	require.NoError(t, db.SqlcClient.Pool().QueryRow(t.Context(),
		`SELECT file_system_id, fileset_path FROM public.team_volume_quotas WHERE team_id = $1`, teamID).Scan(&fileSystemID, &filesetPath))
	require.Equal(t, client.request.GetFileSystemId(), fileSystemID)
	require.Equal(t, client.request.GetFilesetPath(), filesetPath)
	require.Contains(t, filesets.filesets, filesetPath, "an unbounded team still gets its fileset before its directory")

	client = &quotaVolumeClient{createErr: status.Error(codes.Unimplemented, "old node")}
	_, err = store.createVolumeOnNode(t.Context(), client, volume)
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Equal(t, []string{"validate", "verified create"}, client.calls, "no fallback to unverified create")
}

func TestManagedVolumeReportsUnmanagedTeamDirectory(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	teamID := testutils.CreateTestTeam(t, db)
	store := quotaTestStore(t, db, &fakeFilesets{
		createErr: errors.New("InvalidParameter.FilesetPathExist: The specified fileset path is exist."),
	})
	setQuota := func(quotaGiB int64) {
		t.Helper()
		_, err := db.SqlcClient.Pool().Exec(t.Context(), `
			INSERT INTO public.team_volume_quotas (team_id, quota_gib, revision) VALUES ($1, $2, 1)
			ON CONFLICT (team_id) DO UPDATE SET quota_gib = $2, revision = team_volume_quotas.revision + 1`,
			teamID, quotaGiB)
		require.NoError(t, err)
	}
	volume := queries.Volume{ID: uuid.New(), TeamID: teamID, VolumeType: "vepfs"}

	// A positive quota cannot be enforced on a plain directory: refuse.
	setQuota(10)
	client := &quotaVolumeClient{}
	_, err := store.createVolumeOnNode(t.Context(), client, volume)
	require.ErrorIs(t, err, volumequota.ErrUnmanagedTeamDirectory)
	require.Equal(t, []string{"validate"}, client.calls, "the volume is not created outside a fileset")

	// Without a quota the team keeps creating volumes as before the feature.
	setQuota(0)
	client = &quotaVolumeClient{}
	_, err = store.createVolumeOnNode(t.Context(), client, volume)
	require.NoError(t, err)
	require.Equal(t, []string{"validate", "verified create"}, client.calls)
}

func TestUnmanagedVolumeUsesOrdinaryCreate(t *testing.T) {
	t.Parallel()
	store := &APIStore{}
	client := &quotaVolumeClient{}
	_, err := store.createVolumeOnNode(t.Context(), client, queries.Volume{VolumeType: "local"})
	require.NoError(t, err)
	require.Equal(t, []string{"create"}, client.calls)
}
