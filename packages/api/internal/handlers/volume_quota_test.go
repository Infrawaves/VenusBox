package handlers

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
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

func TestManagedVolumeVerifiesMountBeforePinningQuota(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	teamID := testutils.CreateTestTeam(t, db)
	manager, err := volumequota.New(cfg.VolumeQuotaConfig{
		FileSystems: map[string]string{"vepfs": "vepfs-test"},
		Paths:       map[string]string{"vepfs": "/volumes"},
		Region:      "test", AccessKeyID: "test", SecretAccessKey: "test",
	}, db.SqlcClient)
	require.NoError(t, err)
	store := &APIStore{volumeQuota: manager}
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
	_, err = store.createVolumeOnNode(t.Context(), client, volume)
	require.NoError(t, err)
	require.Equal(t, []string{"validate", "verified create"}, client.calls)
	require.Equal(t, "vepfs-test", client.request.GetFileSystemId())
	require.Equal(t, "/volumes/team-"+teamID.String()+"/", client.request.GetFilesetPath())
	var fileSystemID, filesetPath string
	require.NoError(t, db.SqlcClient.Pool().QueryRow(t.Context(),
		`SELECT file_system_id, fileset_path FROM public.team_volume_quotas WHERE team_id = $1`, teamID).Scan(&fileSystemID, &filesetPath))
	require.Equal(t, client.request.GetFileSystemId(), fileSystemID)
	require.Equal(t, client.request.GetFilesetPath(), filesetPath)

	client = &quotaVolumeClient{createErr: status.Error(codes.Unimplemented, "old node")}
	_, err = store.createVolumeOnNode(t.Context(), client, volume)
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Equal(t, []string{"validate", "verified create"}, client.calls, "no fallback to unverified create")
}

func TestUnmanagedVolumeUsesOrdinaryCreate(t *testing.T) {
	t.Parallel()
	store := &APIStore{}
	client := &quotaVolumeClient{}
	_, err := store.createVolumeOnNode(t.Context(), client, queries.Volume{VolumeType: "local"})
	require.NoError(t, err)
	require.Equal(t, []string{"create"}, client.calls)
}
