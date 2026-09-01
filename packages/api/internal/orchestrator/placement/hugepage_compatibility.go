package placement

import (
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/shared/pkg/machineinfo"
)

// isNodeHugePagesAvailable checks if a node's hugepage pool has room for another
// sandbox. See machineinfo.HasHugePagesHeadroom for the rules.
func isNodeHugePagesAvailable(node *nodemanager.Node, maxUsagePercentage int) bool {
	metrics := node.Metrics()

	return machineinfo.HasHugePagesHeadroom(machineinfo.HugePagePool{
		Total:     metrics.HugePagesTotal,
		Used:      metrics.HugePagesUsed,
		Reserved:  metrics.HugePagesReserved,
		SizeBytes: metrics.HugePageSizeBytes,
	}, maxUsagePercentage)
}
