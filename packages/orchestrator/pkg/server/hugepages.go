//go:build linux

package server

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	hostmetrics "github.com/e2b-dev/infra/packages/orchestrator/pkg/metrics"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/machineinfo"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// memoryMetricsSource is narrowed from *metrics.HostMetrics so tests can inject
// a pool snapshot.
type memoryMetricsSource interface {
	GetMemoryMetrics() (*hostmetrics.MemoryMetrics, error)
}

// checkHugePagesHeadroom refuses a create when this node's hugepage pool is at
// or above the configured utilization threshold.
//
// This backstops the identical check the API runs at placement time, which reads
// a node-info snapshot up to a full poll interval stale and which a resume
// pinned to its origin node can skip entirely. Exhausting the pool SIGBUSes a
// faulting Firecracker and kills an already-running sandbox, so the node that
// owns the pool gets the last word.
//
// ResourceExhausted is deliberate: the API's retry loop treats that code as
// "try another node" without consuming a placement attempt.
func (s *Server) checkHugePagesHeadroom(ctx context.Context) error {
	return checkHugePagesHeadroom(ctx, s.hostMetrics,
		s.featureFlags.IntFlag(ctx, featureflags.HugePagesMaxUsagePercentage))
}

func checkHugePagesHeadroom(ctx context.Context, source memoryMetricsSource, maxUsagePercentage int) error {
	if maxUsagePercentage <= 0 {
		return nil
	}

	// nil in tests that construct a Server directly.
	if source == nil {
		return nil
	}

	memory, err := source.GetMemoryMetrics()
	if err != nil {
		// No basis to reject on; the API-side check still applies.
		telemetry.ReportEvent(ctx, "hugepage headroom check skipped: memory metrics unavailable")

		return nil
	}

	pool := machineinfo.HugePagePool{
		Total:     memory.HugePagesTotal,
		Used:      memory.HugePagesUsed,
		Reserved:  memory.HugePagesReserved,
		SizeBytes: memory.HugePageSizeBytes,
	}

	if machineinfo.HasHugePagesHeadroom(pool, maxUsagePercentage) {
		return nil
	}

	telemetry.ReportEvent(ctx, "hugepage pool utilization threshold reached")

	return status.Errorf(codes.ResourceExhausted,
		"node hugepage pool at capacity (%d/%d pages used, %d reserved, limit %d%%), please retry",
		pool.Used, pool.Total, pool.Reserved, maxUsagePercentage)
}
