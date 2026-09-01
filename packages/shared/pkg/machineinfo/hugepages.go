package machineinfo

// HugePagePool is a snapshot of a node's hugepage pool, in page counts, as
// reported by /proc/meminfo.
type HugePagePool struct {
	Total    uint64 // HugePages_Total: preallocated pool size
	Used     uint64 // HugePages_Total - HugePages_Free
	Reserved uint64 // HugePages_Rsvd: committed to a mapping but not yet faulted
	// SizeBytes is Hugepagesize, present on every Linux host whether or not a
	// pool exists. It therefore distinguishes "no pool" from "no sample yet".
	SizeBytes uint64
}

func (p HugePagePool) HasSample() bool {
	return p.SizeBytes > 0
}

// HasHugePagesHeadroom reports whether the pool has room to admit another sandbox.
//
// Guest RAM is faulted in on demand, so a node's real capacity is bounded by
// pool usage rather than by the RAM its sandboxes nominally declare. The
// headroom above the threshold is reserved for already-running sandboxes to
// fault in further pages: faulting against an exhausted pool delivers SIGBUS to
// Firecracker, killing that sandbox, so the reserve is not optional.
//
// A node with no sample yet is admitted — withholding placement from every
// freshly registered node would stall a cold control plane, and the
// orchestrator-side check backstops it with live data.
func HasHugePagesHeadroom(pool HugePagePool, maxUsagePercentage int) bool {
	if maxUsagePercentage <= 0 {
		return true
	}

	if !pool.HasSample() || pool.Total == 0 {
		return true
	}

	// Counting Reserved prevents a burst of placements from double-counting the
	// same free pages: a fault against a reserved page is already promised to
	// succeed.
	used := pool.Used + pool.Reserved
	if used >= pool.Total {
		return false
	}

	return used*100 < pool.Total*uint64(maxUsagePercentage)
}
