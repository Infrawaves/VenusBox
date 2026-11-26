package server

import (
	"context"
	"errors"
	"path/filepath"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	sbxlogger "github.com/e2b-dev/infra/packages/shared/pkg/logger/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// AppendVolumeMounts mounts volumes into a running sandbox. The API validated
// the volumes against its database; this checks only what the node knows: the
// sandbox, its current mounts and its envd.
func (s *Server) AppendVolumeMounts(ctx context.Context, req *orchestrator.SandboxAppendVolumeMountsRequest) (*emptypb.Empty, error) {
	releaseWork := s.info.TrackWork()
	defer releaseWork()

	ctx, span := tracer.Start(ctx, "sandbox-append-volume-mounts")
	defer span.End()

	span.SetAttributes(telemetry.WithSandboxID(req.GetSandboxId()))

	if len(req.GetVolumeMounts()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "no volume mounts given")
	}
	if len(req.GetVolumeMounts()) > sandbox.MaxAppendVolumeMounts {
		return nil, status.Errorf(codes.InvalidArgument, "at most %d volume mounts can be appended at once", sandbox.MaxAppendVolumeMounts)
	}

	mounts, err := createVolumeMountModelsFromAPI(req.GetVolumeMounts())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid volume mounts: %s", err)
	}
	for _, m := range mounts {
		if m.Name == "" || m.Type == "" || !filepath.IsAbs(m.Path) || filepath.Clean(m.Path) != m.Path || m.Path == "/" {
			return nil, status.Errorf(codes.InvalidArgument, "invalid volume mount %q at %q", m.Name, m.Path)
		}
	}

	sbx, ok := s.sandboxFactory.Sandboxes.Get(req.GetSandboxId())
	if !ok {
		return nil, status.Error(codes.NotFound, "sandbox not found")
	}

	span.SetAttributes(
		telemetry.WithTeamID(sbx.Runtime.TeamID),
		telemetry.WithEnvdVersion(sbx.Config.Envd.Version),
	)

	err = sbx.AppendVolumeMounts(ctx, mounts)
	if err == nil {
		sbxlogger.I(sbx).Info(ctx, "appended volume mounts", zap.Int("count", len(mounts)))

		return &emptypb.Empty{}, nil
	}

	sbxlogger.I(sbx).Warn(ctx, "failed to append volume mounts", zap.Error(err))

	// FailedPrecondition is what the API reads as a refusal of the request, the
	// code a create uses for a volume envd could not mount.
	switch {
	case errors.Is(err, sandbox.ErrVolumeMountUnsettled):
		// Before the VolumeMountError a 500 from envd also carries: some mounts
		// may be in place, so this is not a refusal of the request.
		return nil, status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, sandbox.ErrSandboxUpdateClosed):
		return nil, status.Error(codes.NotFound, "sandbox not found")
	case errors.Is(err, sandbox.ErrCheckpointInFlight):
		return nil, status.Error(codes.Aborted, err.Error())
	case errors.Is(err, sandbox.ErrVolumeMountConflict):
		return nil, status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, sandbox.ErrEnvdVolumeAppendUnsupported):
		// Not Unimplemented: that is what an orchestrator predating this RPC
		// answers, and the API tells the two apart.
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	if volumeErr, ok := errors.AsType[sandbox.VolumeMountError](err); ok {
		return nil, status.Error(codes.FailedPrecondition, volumeErr.Error())
	}

	telemetry.ReportError(ctx, "failed to append volume mounts", err)

	return nil, status.Errorf(codes.Internal, "failed to append volume mounts: %s", err)
}
