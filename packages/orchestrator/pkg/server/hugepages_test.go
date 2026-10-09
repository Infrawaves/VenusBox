//go:build linux

package server

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	hostmetrics "github.com/e2b-dev/infra/packages/orchestrator/pkg/metrics"
)

const testHugePageSize = 2 * 1024 * 1024

type stubMemoryMetrics struct {
	memory *hostmetrics.MemoryMetrics
	err    error
}

func (s stubMemoryMetrics) GetMemoryMetrics() (*hostmetrics.MemoryMetrics, error) {
	return s.memory, s.err
}

func pool(total, used, reserved uint64) stubMemoryMetrics {
	return stubMemoryMetrics{memory: &hostmetrics.MemoryMetrics{
		HugePagesTotal:    total,
		HugePagesUsed:     used,
		HugePagesReserved: reserved,
		HugePageSizeBytes: testHugePageSize,
	}}
}

func TestCheckHugePagesHeadroomAdmits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		source memoryMetricsSource
		max    int
	}{
		{name: "under the threshold", source: pool(1000, 100, 0), max: 80},
		{name: "just under the threshold", source: pool(1000, 799, 0), max: 80},
		{name: "node with no pool", source: pool(0, 0, 0), max: 80},
		{name: "threshold disabled", source: pool(1000, 1000, 0), max: 0},
		{name: "nil source", source: nil, max: 80},
		{
			name:   "sampling error fails open",
			source: stubMemoryMetrics{err: errors.New("read /proc/meminfo: boom")},
			max:    80,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.NoError(t, checkHugePagesHeadroom(t.Context(), tt.source, tt.max))
		})
	}
}

func TestCheckHugePagesHeadroomRejects(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		source memoryMetricsSource
		max    int
	}{
		{name: "exactly at the threshold", source: pool(1000, 800, 0), max: 80},
		{name: "over the threshold", source: pool(1000, 950, 0), max: 80},
		{name: "exhausted pool", source: pool(1000, 1000, 0), max: 80},
		{name: "reserved pages push it over", source: pool(1000, 700, 150), max: 80},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := checkHugePagesHeadroom(t.Context(), tt.source, tt.max)
			require.Error(t, err)

			// ResourceExhausted is what makes the API retry elsewhere without
			// burning a placement attempt.
			st, ok := status.FromError(err)
			require.True(t, ok, "error must carry a gRPC status")
			assert.Equal(t, codes.ResourceExhausted, st.Code())
		})
	}
}
