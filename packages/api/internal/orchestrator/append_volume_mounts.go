package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/api/internal/utils"
	"github.com/e2b-dev/infra/packages/db/pkg/types"
	orchestratorgrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// AppendSandboxVolumeMounts mounts volumes into a running sandbox and records
// them on its record, so a pause carries them into the snapshot and a resume
// mounts them again.
//
// The node mounts first and the record follows. The other order would let a
// pause that starts in between snapshot a mount that then fails, and every
// resume of that snapshot would fail on it. This order can only lose a mount:
// a pause that takes the record before the write below snapshots without it,
// and the resume's /init detaches the mount the guest still holds.
//
// The record is written only for the execution the node mounted into. A reply
// that arrives after a pause and resume would otherwise list the mount on an
// execution that never had it.
func (o *Orchestrator) AppendSandboxVolumeMounts(
	ctx context.Context,
	teamID uuid.UUID,
	sandboxID string,
	mounts []*orchestratorgrpc.SandboxVolumeMount,
) *api.APIError {
	ctx, span := tracer.Start(ctx, "append-sandbox-volume-mounts",
		trace.WithAttributes(
			attribute.String("instance.id", sandboxID),
			attribute.Int("volume_mounts", len(mounts)),
		),
	)
	defer span.End()

	sbx, err := o.sandboxStore.Get(ctx, teamID, sandboxID)
	if err != nil {
		if errors.Is(err, sandbox.ErrNotFound) {
			return &api.APIError{Code: http.StatusNotFound, ClientMsg: utils.SandboxNotFoundMsg(sandboxID), Err: err}
		}

		return &api.APIError{Code: http.StatusInternalServerError, ClientMsg: "Error getting sandbox", Err: err}
	}
	if sbx.State != sandbox.StateRunning {
		return &api.APIError{Code: http.StatusConflict, ClientMsg: utils.SandboxChangingStateMsg(sandboxID, sbx.State), Err: &sandbox.NotRunningError{SandboxID: sandboxID, State: sbx.State}}
	}

	if apiErr := o.appendVolumeMountsOnNode(ctx, sbx, mounts); apiErr != nil {
		return apiErr
	}

	executionID := sbx.ExecutionID
	added := nodemanager.ConvertOrchestratorMountsToDatabaseMounts(mounts)
	_, err = o.sandboxStore.Update(ctx, teamID, sandboxID, func(sbx sandbox.Sandbox) (sandbox.Sandbox, error) {
		if sbx.State != sandbox.StateRunning {
			return sbx, &sandbox.NotRunningError{SandboxID: sandboxID, State: sbx.State}
		}
		if sbx.ExecutionID != executionID {
			return sbx, errExecutionChanged
		}

		sbx.VolumeMounts = appendMissingVolumeMounts(sbx.VolumeMounts, added)

		return sbx, nil
	})
	if err != nil {
		notRunning, isNotRunning := errors.AsType[*sandbox.NotRunningError](err)
		switch {
		case errors.Is(err, sandbox.ErrNotFound):
			// Killed after the node mounted: the mounts went with it.
			return &api.APIError{Code: http.StatusNotFound, ClientMsg: utils.SandboxNotFoundMsg(sandboxID), Err: err}
		case isNotRunning && notRunning.State == sandbox.StateSnapshotting:
			// A checkpoint returns the sandbox to running with the same execution
			// and the mounts still in place, so the same request then records them.
			return &api.APIError{
				Code:      http.StatusConflict,
				ClientMsg: fmt.Sprintf("Sandbox '%s' is being snapshotted; retry the same request once it is running so the volume mounts are kept", sandboxID),
				Err:       err,
			}
		case isNotRunning || errors.Is(err, errExecutionChanged):
			telemetry.ReportError(ctx, "sandbox changed state after its volumes were mounted; the mounts are not kept", err)

			return &api.APIError{
				Code:      http.StatusConflict,
				ClientMsg: fmt.Sprintf("Sandbox '%s' changed state while the volumes were mounted; they will not be kept after a resume", sandboxID),
				Err:       err,
			}
		}

		return &api.APIError{Code: http.StatusInternalServerError, ClientMsg: "Error recording the volume mounts", Err: err}
	}

	return nil
}

// errExecutionChanged reports a sandbox record that belongs to a later
// execution than the one the node mounted the volumes into.
var errExecutionChanged = errors.New("sandbox execution changed")

// appendMissingVolumeMounts appends the mounts not yet recorded with the same
// path and volume, so a retried request records each mount once.
func appendMissingVolumeMounts(existing, added []*types.SandboxVolumeMountConfig) []*types.SandboxVolumeMountConfig {
	result := slices.Clone(existing)
	for _, m := range added {
		if !slices.ContainsFunc(result, func(e *types.SandboxVolumeMountConfig) bool {
			return e.Path == m.Path && e.ID == m.ID
		}) {
			result = append(result, m)
		}
	}

	return result
}

func (o *Orchestrator) appendVolumeMountsOnNode(ctx context.Context, sbx sandbox.Sandbox, mounts []*orchestratorgrpc.SandboxVolumeMount) *api.APIError {
	node := o.getOrConnectNode(ctx, sbx.ClusterID, sbx.NodeID)
	if node == nil {
		return &api.APIError{
			Code:      http.StatusServiceUnavailable,
			ClientMsg: fmt.Sprintf("Node hosting sandbox '%s' is not available, try again later", sbx.SandboxID),
			Err:       fmt.Errorf("node '%s' not found for cluster '%s'", sbx.NodeID, sbx.ClusterID),
		}
	}

	client, ctx := node.GetSandboxClient(ctx, sbx.TeamID.String())
	_, err := client.Sandbox.AppendVolumeMounts(ctx, &orchestratorgrpc.SandboxAppendVolumeMountsRequest{
		SandboxId:    sbx.SandboxID,
		VolumeMounts: mounts,
	})
	if err == nil {
		return nil
	}

	st, _ := status.FromError(err)
	switch st.Code() {
	case codes.NotFound:
		return &api.APIError{Code: http.StatusNotFound, ClientMsg: utils.SandboxNotFoundMsg(sbx.SandboxID), Err: err}
	case codes.InvalidArgument, codes.FailedPrecondition:
		// The node's message names the path: a conflict with another mount, a
		// volume envd could not mount, or an envd that cannot mount at runtime.
		return &api.APIError{Code: http.StatusBadRequest, ClientMsg: st.Message(), Err: err}
	case codes.Aborted:
		return &api.APIError{Code: http.StatusConflict, ClientMsg: fmt.Sprintf("Sandbox '%s' is being checkpointed, try again later", sbx.SandboxID), Err: err}
	case codes.Unavailable, codes.DeadlineExceeded, codes.Canceled:
		// The node may have mounted some of the volumes, or all of them. They are
		// not recorded, so a pause would drop them; the same request again
		// mounts what is missing and records them all.
		return &api.APIError{
			Code:      http.StatusServiceUnavailable,
			ClientMsg: fmt.Sprintf("Mounting the volumes into sandbox '%s' did not finish and some may be mounted; retry the same request", sbx.SandboxID),
			Err:       err,
		}
	case codes.Unimplemented:
		return &api.APIError{
			Code:      http.StatusServiceUnavailable,
			ErrorCode: errCodeFeatureUnsupported,
			ClientMsg: fmt.Sprintf("Sandbox '%s' runs on an orchestrator that cannot mount volumes into a running sandbox", sbx.SandboxID),
			Err:       err,
		}
	}

	err = utils.UnwrapGRPCError(err)
	telemetry.ReportCriticalError(ctx, "failed to append volume mounts on node", err)

	return &api.APIError{
		Code:      http.StatusInternalServerError,
		ClientMsg: "Error mounting the volumes into the sandbox",
		Err:       fmt.Errorf("failed to append volume mounts on node: %w", err),
	}
}
