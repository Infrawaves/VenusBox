package orchestrator

import (
	"context"
	"errors"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/placement"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

// The fault_domain values of a failed create, the label the orchestrator also
// puts on its killed and crashed sandbox counters.
const (
	createFaultDomainPlatform = "platform"
	createFaultDomainUnknown  = "unknown"
)

// createFailureFaultDomain says whose side a placement failure points at.
// Requests a user can fix are mostly rejected with a 4xx before placement, so
// the default is platform: capacity, a node failing to start the sandbox, a
// fleet that lacks a feature, or stored data a node refuses (a create's vcpu
// comes from the template build row, see refusalHTTPStatus).
//
// The exception is a volume envd could not mount, the only refusal the node
// sends as FailedPrecondition (volumeRefusal in the orchestrator). The mount
// path is the user's and the API checks only its form, so a path that clashes
// with a file in the guest gets here; so does a storage backend failing the
// mount. The node cannot tell the two apart, so the failure is unknown.
func createFailureFaultDomain(apiErr *api.APIError) string {
	if apiErr.ErrorCode != errCodeSandboxCreateRefused {
		return createFaultDomainPlatform
	}

	var createErr placement.SandboxCreateError
	if !errors.As(apiErr.Err, &createErr) {
		return createFaultDomainPlatform
	}

	if st, ok := status.FromError(createErr.LastErr); ok && st.Code() == codes.FailedPrecondition && createRefusal(st) != nil {
		return createFaultDomainUnknown
	}

	return createFaultDomainPlatform
}

// recordCreateFailure counts and logs a create or resume that failed at
// placement, so failures are visible outside of traces. It only observes:
// the error returned to the caller is unchanged.
func (o *Orchestrator) recordCreateFailure(ctx context.Context, apiErr *api.APIError, sandboxID, teamID string, isResume bool) {
	if apiErr == nil {
		return
	}

	faultDomain := createFailureFaultDomain(apiErr)

	if o.createFailedCounter != nil {
		o.createFailedCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("error_code", apiErr.ErrorCode),
			attribute.String("fault_domain", faultDomain),
			attribute.Bool("is_resume", isResume),
		))
	}

	logger.L().Warn(ctx, "sandbox create failed",
		logger.WithSandboxID(sandboxID),
		logger.WithTeamID(teamID),
		zap.String("error_code", apiErr.ErrorCode),
		zap.String("fault_domain", faultDomain),
		zap.Int("http_status", apiErr.Code),
		zap.Bool("is_resume", isResume),
		zap.Error(apiErr.Err),
	)
}
