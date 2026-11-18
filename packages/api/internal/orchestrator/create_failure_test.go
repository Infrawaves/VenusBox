package orchestrator

import (
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/placement"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

func TestRecordCreateFailure(t *testing.T) {
	t.Parallel()

	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")
	counter, err := telemetry.GetCounter(meter, telemetry.ApiOrchestratorCreateFailed)
	require.NoError(t, err)

	o := &Orchestrator{createFailedCounter: counter}
	o.recordCreateFailure(t.Context(), &api.APIError{
		Code:      http.StatusServiceUnavailable,
		ErrorCode: errCodeCapacityUnavailable,
		Err:       errors.New("no nodes"),
	}, "sbx-1", "team-1", true)
	o.recordCreateFailure(t.Context(), nil, "sbx-2", "team-1", false)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))

	var points []metricdata.DataPoint[int64]
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == string(telemetry.ApiOrchestratorCreateFailed) {
				points = append(points, m.Data.(metricdata.Sum[int64]).DataPoints...)
			}
		}
	}

	require.Len(t, points, 1)
	assert.Equal(t, int64(1), points[0].Value)
	assert.Equal(t, attribute.NewSet(
		attribute.String("error_code", errCodeCapacityUnavailable),
		attribute.String("fault_domain", createFaultDomainPlatform),
		attribute.Bool("is_resume", true),
	), points[0].Attributes)
}

// The domain is read off the real placement error, as placementAPIError
// builds it: only a volume mount refusal is unknown.
func TestCreateFailureFaultDomain(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{"no capacity", placement.NoNodesAvailableError{}, createFaultDomainPlatform},
		{"create failed", placement.SandboxCreateError{Attempts: 3, LastErr: status.Error(codes.Internal, "boom")}, createFaultDomainPlatform},
		{"vcpu refusal", placement.SandboxCreateError{Attempts: 1, LastErr: createRefusedStatusWith(t, codes.InvalidArgument, "vcpu 0 must be at least 1", http.StatusBadRequest)}, createFaultDomainPlatform},
		{"volume mount refusal", placement.SandboxCreateError{Attempts: 1, LastErr: createRefusedStatusWith(t, codes.FailedPrecondition, `failed to mount volume at "/data"`, http.StatusInternalServerError)}, createFaultDomainUnknown},
		{"files not found, no refusal detail", placement.SandboxCreateError{Attempts: 3, LastErr: status.Error(codes.FailedPrecondition, "sandbox files for 'abc' not found")}, createFaultDomainPlatform},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.want, createFailureFaultDomain(placementAPIError(tc.err)))
		})
	}
}

// An Orchestrator built without metrics must not panic on a failed create.
func TestRecordCreateFailureWithoutCounter(t *testing.T) {
	t.Parallel()

	assert.NotPanics(t, func() {
		(&Orchestrator{}).recordCreateFailure(t.Context(), &api.APIError{ErrorCode: errCodeInternalServer}, "sbx", "team", false)
	})
}
