package placement

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/shared/pkg/machineinfo"
)

func TestIsNodeHugePagesAvailable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		total              uint64
		used               uint64
		reserved           uint64
		maxUsagePercentage int
		want               bool
	}{
		{
			name:               "empty pool is not a hugepage node",
			total:              0,
			used:               0,
			maxUsagePercentage: 80,
			want:               true,
		},
		{
			name:               "non-positive threshold disables the check",
			total:              1000,
			used:               999,
			maxUsagePercentage: 0,
			want:               true,
		},
		{
			name:               "well under the threshold",
			total:              1000,
			used:               137,
			maxUsagePercentage: 80,
			want:               true,
		},
		{
			name:               "just under the threshold",
			total:              1000,
			used:               799,
			maxUsagePercentage: 80,
			want:               true,
		},
		{
			name:               "exactly at the threshold is rejected",
			total:              1000,
			used:               800,
			maxUsagePercentage: 80,
			want:               false,
		},
		{
			name:               "over the threshold",
			total:              1000,
			used:               900,
			maxUsagePercentage: 80,
			want:               false,
		},
		{
			name:               "fully exhausted pool",
			total:              1000,
			used:               1000,
			maxUsagePercentage: 80,
			want:               false,
		},
		{
			name:               "used beyond total (surplus pages) is rejected",
			total:              1000,
			used:               1100,
			maxUsagePercentage: 80,
			want:               false,
		},
		{
			name:               "reserved pages count toward usage",
			total:              1000,
			used:               700,
			reserved:           150,
			maxUsagePercentage: 80,
			want:               false,
		},
		{
			name:               "reserved pages keep a node eligible when the sum fits",
			total:              1000,
			used:               700,
			reserved:           50,
			maxUsagePercentage: 80,
			want:               true,
		},
		{
			name:               "1536 GiB node at the real-world default",
			total:              611840, // ~1195 GiB of 2 MiB pages
			used:               14000,  // ~27 GiB faulted
			maxUsagePercentage: 80,
			want:               true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			node := nodemanager.NewTestNode("test-node", api.NodeStatusReady, 0, 8,
				nodemanager.WithHugePages(tt.total, tt.used, tt.reserved))

			assert.Equal(t, tt.want, isNodeHugePagesAvailable(node, tt.maxUsagePercentage))
		})
	}
}

// TestSampleSkipsHugePageSaturatedNodes asserts the filter is wired into
// sampling, even when the saturated node scores better.
func TestSampleSkipsHugePageSaturatedNodes(t *testing.T) {
	t.Parallel()

	saturated := nodemanager.NewTestNode("saturated", api.NodeStatusReady, 0, 64,
		nodemanager.WithHugePages(1000, 950, 0))
	healthy := nodemanager.NewTestNode("healthy", api.NodeStatusReady, 32, 64,
		nodemanager.WithHugePages(1000, 100, 0))

	algorithm := NewBestOfK(BestOfKConfig{R: 4, K: 3, Alpha: 0.5, HugePagesMaxUsagePercentage: 80}).(*BestOfK)

	nodes := []*nodemanager.Node{saturated, healthy}

	// Repeat: sampling is randomized, so a single run could miss the saturated node.
	for range 100 {
		chosen, err := algorithm.chooseNode(t.Context(), nodes, map[string]struct{}{},
			nodemanager.SandboxResources{CPUs: 1, MiBMemory: 2048}, machineinfo.MachineInfo{}, false, nil)

		assert.NoError(t, err)
		assert.Equal(t, healthy.ID, chosen.ID, "saturated node must never be chosen")
	}
}

// TestChooseNodeAllNodesHugePageSaturated asserts placement fails rather than
// overcommitting a pool when every node is over the threshold.
func TestChooseNodeAllNodesHugePageSaturated(t *testing.T) {
	t.Parallel()

	nodes := []*nodemanager.Node{
		nodemanager.NewTestNode("a", api.NodeStatusReady, 0, 64, nodemanager.WithHugePages(1000, 850, 0)),
		nodemanager.NewTestNode("b", api.NodeStatusReady, 0, 64, nodemanager.WithHugePages(1000, 1000, 0)),
	}

	algorithm := NewBestOfK(BestOfKConfig{R: 4, K: 3, Alpha: 0.5, HugePagesMaxUsagePercentage: 80}).(*BestOfK)

	chosen, err := algorithm.chooseNode(t.Context(), nodes, map[string]struct{}{},
		nodemanager.SandboxResources{CPUs: 1, MiBMemory: 2048}, machineinfo.MachineInfo{}, false, nil)

	assert.Nil(t, chosen)
	assert.ErrorAs(t, err, &FailedToPlaceSandboxError{})
}

// TestNodeWithoutMetricsSampleIsAvailable pins the fail-open choice for a node
// that has registered but not yet reported host metrics.
func TestNodeWithoutMetricsSampleIsAvailable(t *testing.T) {
	t.Parallel()

	node := nodemanager.NewTestNode("cold", api.NodeStatusReady, 0, 8,
		nodemanager.WithoutHugePagesSample())

	assert.True(t, isNodeHugePagesAvailable(node, 80))
}

// TestPreferredNodeWithoutHugePagesHeadroomIsNotPinned covers the resume path,
// where a preferred node bypasses chooseNode and its filters.
func TestPreferredNodeWithoutHugePagesHeadroomIsNotPinned(t *testing.T) {
	t.Parallel()

	saturated := nodemanager.NewTestNode("saturated", api.NodeStatusReady, 0, 64,
		nodemanager.WithHugePages(1000, 950, 0))
	healthy := nodemanager.NewTestNode("healthy", api.NodeStatusReady, 0, 64,
		nodemanager.WithHugePages(1000, 100, 0))
	healthy.SetSandboxClient(&nodemanager.MockSandboxClientCustom{
		CreateFunc: func() error { return nil },
	})

	algorithm := NewBestOfK(BestOfKConfig{R: 4, K: 3, Alpha: 0.5, HugePagesMaxUsagePercentage: 80})

	result, err := PlaceSandbox(
		t.Context(),
		algorithm,
		[]*nodemanager.Node{saturated, healthy},
		saturated, // e.g. the node a snapshot was taken on
		testSbxRequest("sbx-resume"),
		machineinfo.MachineInfo{},
		false,
		nil,
	)

	require.NoError(t, err)
	require.NotNil(t, result.Node)
	assert.Equal(t, healthy.ID, result.Node.ID, "a saturated preferred node must be dropped")
}

// TestPreferredNodeWithHugePagesHeadroomIsStillPinned guards against the check
// above turning into a blanket loss of resume locality.
func TestPreferredNodeWithHugePagesHeadroomIsStillPinned(t *testing.T) {
	t.Parallel()

	preferred := nodemanager.NewTestNode("preferred", api.NodeStatusReady, 60, 64,
		nodemanager.WithHugePages(1000, 100, 0))
	preferred.SetSandboxClient(&nodemanager.MockSandboxClientCustom{
		CreateFunc: func() error { return nil },
	})
	// Scores far better, so only the preference can explain choosing `preferred`.
	idle := nodemanager.NewTestNode("idle", api.NodeStatusReady, 0, 64,
		nodemanager.WithHugePages(1000, 10, 0))

	algorithm := NewBestOfK(BestOfKConfig{R: 4, K: 3, Alpha: 0.5, HugePagesMaxUsagePercentage: 80})

	result, err := PlaceSandbox(
		t.Context(),
		algorithm,
		[]*nodemanager.Node{preferred, idle},
		preferred,
		testSbxRequest("sbx-resume-2"),
		machineinfo.MachineInfo{},
		false,
		nil,
	)

	require.NoError(t, err)
	require.NotNil(t, result.Node)
	assert.Equal(t, preferred.ID, result.Node.ID)
}
