package orchestrator

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/db/pkg/types"
	orchestratorgrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
)

// appendVolumeMountsClient answers AppendVolumeMounts with err and records the
// requests it got.
type appendVolumeMountsClient struct {
	orchestratorgrpc.SandboxServiceClient

	err      error
	requests []*orchestratorgrpc.SandboxAppendVolumeMountsRequest
}

func (c *appendVolumeMountsClient) AppendVolumeMounts(_ context.Context, req *orchestratorgrpc.SandboxAppendVolumeMountsRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
	c.requests = append(c.requests, req)
	if c.err != nil {
		return nil, c.err
	}

	return &emptypb.Empty{}, nil
}

var testVolumeMount = &orchestratorgrpc.SandboxVolumeMount{Id: "0b5c3f1e-9a4d-4b8e-8d34-6a0f1f0c2a11", Name: "data", Path: "/mnt/data", Type: "local"}

func TestAppendSandboxVolumeMountsRecordsMountsOnce(t *testing.T) {
	t.Parallel()

	client := &appendVolumeMountsClient{}
	o := newCreateSandboxTestOrchestrator(t, nodemanager.WithSandboxClient(client))
	sbx := addRunningSandbox(t, o)

	mounts := []*orchestratorgrpc.SandboxVolumeMount{testVolumeMount}
	require.Nil(t, o.AppendSandboxVolumeMounts(t.Context(), sbx.TeamID, sbx.SandboxID, mounts))
	require.Nil(t, o.AppendSandboxVolumeMounts(t.Context(), sbx.TeamID, sbx.SandboxID, mounts), "a retry succeeds")

	require.Len(t, client.requests, 2)
	assert.Equal(t, sbx.SandboxID, client.requests[0].GetSandboxId())

	stored, err := o.sandboxStore.Get(t.Context(), sbx.TeamID, sbx.SandboxID)
	require.NoError(t, err)
	assert.Equal(t, []*types.SandboxVolumeMountConfig{{ID: testVolumeMount.GetId(), Name: "data", Path: "/mnt/data", Type: "local"}}, stored.VolumeMounts)
}

func TestAppendSandboxVolumeMountsNodeErrors(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		err  error
		code int
	}{
		"mount failed":       {status.Error(codes.FailedPrecondition, `failed to mount volume at "/mnt/data"`), http.StatusBadRequest},
		"path conflict":      {status.Error(codes.InvalidArgument, "conflict"), http.StatusBadRequest},
		"checkpointing":      {status.Error(codes.Aborted, "checkpoint"), http.StatusConflict},
		"gone from the node": {status.Error(codes.NotFound, "sandbox not found"), http.StatusNotFound},
		"old orchestrator":   {status.Error(codes.Unimplemented, "unknown method"), http.StatusServiceUnavailable},
		"outcome unknown":    {status.Error(codes.Unavailable, "the outcome of mounting the volumes is unknown"), http.StatusServiceUnavailable},
		"node timed out":     {status.Error(codes.DeadlineExceeded, "deadline exceeded"), http.StatusServiceUnavailable},
		"internal":           {status.Error(codes.Internal, "boom"), http.StatusInternalServerError},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			o := newCreateSandboxTestOrchestrator(t, nodemanager.WithSandboxClient(&appendVolumeMountsClient{err: tc.err}))
			sbx := addRunningSandbox(t, o)

			apiErr := o.AppendSandboxVolumeMounts(t.Context(), sbx.TeamID, sbx.SandboxID, []*orchestratorgrpc.SandboxVolumeMount{testVolumeMount})
			require.NotNil(t, apiErr)
			assert.Equal(t, tc.code, apiErr.Code)

			stored, err := o.sandboxStore.Get(t.Context(), sbx.TeamID, sbx.SandboxID)
			require.NoError(t, err)
			assert.Empty(t, stored.VolumeMounts, "a failed append records nothing")
		})
	}
}

func TestAppendSandboxVolumeMountsRefusesANotRunningSandbox(t *testing.T) {
	t.Parallel()

	client := &appendVolumeMountsClient{}
	o := newCreateSandboxTestOrchestrator(t, nodemanager.WithSandboxClient(client))
	sbx := addSandboxInState(t, o, sandbox.StatePausing, "node-1")

	apiErr := o.AppendSandboxVolumeMounts(t.Context(), sbx.TeamID, sbx.SandboxID, []*orchestratorgrpc.SandboxVolumeMount{testVolumeMount})
	require.NotNil(t, apiErr)
	assert.Equal(t, http.StatusConflict, apiErr.Code)
	assert.Empty(t, client.requests, "the node is not asked")
}

// pausingAppendVolumeMountsClient replaces the sandbox's record with a new
// execution while it mounts, as a pause and resume finishing before its reply
// reaches the API would.
type pausingAppendVolumeMountsClient struct {
	orchestratorgrpc.SandboxServiceClient

	onAppend func()
}

func (c *pausingAppendVolumeMountsClient) AppendVolumeMounts(context.Context, *orchestratorgrpc.SandboxAppendVolumeMountsRequest, ...grpc.CallOption) (*emptypb.Empty, error) {
	c.onAppend()

	return &emptypb.Empty{}, nil
}

func TestAppendSandboxVolumeMountsDoesNotRecordOnALaterExecution(t *testing.T) {
	t.Parallel()

	client := &pausingAppendVolumeMountsClient{}
	o := newCreateSandboxTestOrchestrator(t, nodemanager.WithSandboxClient(client))
	sbx := addRunningSandbox(t, o)
	client.onAppend = func() {
		_, err := o.sandboxStore.Update(t.Context(), sbx.TeamID, sbx.SandboxID, func(s sandbox.Sandbox) (sandbox.Sandbox, error) {
			s.ExecutionID = "a-later-execution"

			return s, nil
		})
		assert.NoError(t, err)
	}

	apiErr := o.AppendSandboxVolumeMounts(t.Context(), sbx.TeamID, sbx.SandboxID, []*orchestratorgrpc.SandboxVolumeMount{testVolumeMount})
	require.NotNil(t, apiErr)
	assert.Equal(t, http.StatusConflict, apiErr.Code)

	stored, err := o.sandboxStore.Get(t.Context(), sbx.TeamID, sbx.SandboxID)
	require.NoError(t, err)
	assert.Empty(t, stored.VolumeMounts)
}

func TestAppendSandboxVolumeMountsAfterTheSandboxIsKilled(t *testing.T) {
	t.Parallel()

	client := &pausingAppendVolumeMountsClient{}
	o := newCreateSandboxTestOrchestrator(t, nodemanager.WithSandboxClient(client))
	sbx := addRunningSandbox(t, o)
	client.onAppend = func() { o.sandboxStore.Remove(t.Context(), sbx.TeamID, sbx.SandboxID) }

	apiErr := o.AppendSandboxVolumeMounts(t.Context(), sbx.TeamID, sbx.SandboxID, []*orchestratorgrpc.SandboxVolumeMount{testVolumeMount})
	require.NotNil(t, apiErr)
	assert.Equal(t, http.StatusNotFound, apiErr.Code)
}

func TestAppendSandboxVolumeMountsDuringASnapshotAsksForARetry(t *testing.T) {
	t.Parallel()

	client := &pausingAppendVolumeMountsClient{}
	o := newCreateSandboxTestOrchestrator(t, nodemanager.WithSandboxClient(client))
	sbx := addRunningSandbox(t, o)
	setState := func(state sandbox.State) {
		_, err := o.sandboxStore.Update(t.Context(), sbx.TeamID, sbx.SandboxID, func(s sandbox.Sandbox) (sandbox.Sandbox, error) {
			s.State = state

			return s, nil
		})
		require.NoError(t, err)
	}
	client.onAppend = func() { setState(sandbox.StateSnapshotting) }

	mounts := []*orchestratorgrpc.SandboxVolumeMount{testVolumeMount}
	apiErr := o.AppendSandboxVolumeMounts(t.Context(), sbx.TeamID, sbx.SandboxID, mounts)
	require.NotNil(t, apiErr)
	assert.Equal(t, http.StatusConflict, apiErr.Code)
	assert.Contains(t, apiErr.ClientMsg, "retry the same request once it is running")

	// The checkpoint ends with the same execution: the retry records the mount.
	client.onAppend = func() {}
	setState(sandbox.StateRunning)
	require.Nil(t, o.AppendSandboxVolumeMounts(t.Context(), sbx.TeamID, sbx.SandboxID, mounts))

	stored, err := o.sandboxStore.Get(t.Context(), sbx.TeamID, sbx.SandboxID)
	require.NoError(t, err)
	assert.Len(t, stored.VolumeMounts, 1)
}
