package scheduler

import (
	schedulerv1 "agentenv/services/api/proto"
	"agentenv/services/shared/config"
)

// FilterUnschedulable removes nodes whose own last heartbeat says they are not
// taking new work — today that means a node isolated through its admin API,
// which reports DRAINING.
//
// Only a status the node actually reported is acted on. A node with no snapshot
// yet, or one reporting UNSPECIFIED, is kept: it has just registered and has
// not had a chance to say anything about itself, and dropping it would leave a
// freshly started cluster with nothing to schedule onto until the first
// heartbeat lands. Fail open on what we do not know, fail closed on what a node
// told us.
//
// Statuses the scheduler derives rather than receives — LINGERING from pod
// termination, UNHEALTHY from a lost heartbeat — are not handled here. Those
// come from discovery and heartbeat expiry, and are filtered upstream of this
// call.
func FilterUnschedulable(nodes []RichNode) []RichNode {
	result := make([]RichNode, 0, len(nodes))
	for _, n := range nodes {
		status := n.Snapshot.GetStatus()
		if status != schedulerv1.NodeStatus_NODE_STATUS_UNSPECIFIED && !status.CanAcceptNewRequests() {
			continue
		}
		result = append(result, n)
	}
	return result
}

// FilterByResourceLimit removes nodes that exceed any configured resource
// threshold. Nodes without a heartbeat snapshot are always kept (they have no
// metrics to evaluate). A nil limit disables all filtering.
func FilterByResourceLimit(nodes []RichNode, limit *config.NodeResourceLimit) []RichNode {
	if limit == nil {
		return nodes
	}

	result := make([]RichNode, 0, len(nodes))
	for _, n := range nodes {
		if n.Snapshot == nil {
			// No heartbeat yet — cannot evaluate limits; keep the node.
			result = append(result, n)
			continue
		}
		if !withinLimit(n, limit) {
			continue
		}
		result = append(result, n)
	}
	return result
}

func withinLimit(n RichNode, limit *config.NodeResourceLimit) bool {
	s := n.Snapshot

	if limit.MaxSandboxCount != nil && s.GetSandboxCount() > *limit.MaxSandboxCount {
		return false
	}
	if limit.MaxSandboxStartingCount != nil && s.GetSandboxStartingCount() > *limit.MaxSandboxStartingCount {
		return false
	}
	if limit.MaxCPUUsedPercent != nil && s.GetCpuPercent() > *limit.MaxCPUUsedPercent {
		return false
	}
	if limit.MaxCPUAllocatedPercent != nil {
		if s.GetCpuCount() > 0 {
			allocatedPercent := s.GetAllocatedCpu() * 100 / s.GetCpuCount()
			if allocatedPercent > *limit.MaxCPUAllocatedPercent {
				return false
			}
		}
	}
	if limit.MaxMemoryUsedPercent != nil {
		if s.GetMemoryTotalBytes() > 0 {
			usedPercent := uint32(s.GetMemoryUsedBytes() * 100 / s.GetMemoryTotalBytes())
			if usedPercent > *limit.MaxMemoryUsedPercent {
				return false
			}
		}
	}
	if limit.MaxMemoryAllocatedPercent != nil {
		if s.GetMemoryTotalBytes() > 0 {
			allocatedPercent := uint32(s.GetAllocatedMemoryBytes() * 100 / s.GetMemoryTotalBytes())
			if allocatedPercent > *limit.MaxMemoryAllocatedPercent {
				return false
			}
		}
	}

	// "Including paused" ceilings sum the active running set with the paused
	// reservations reported in the snapshot. A node exceeding any of these is
	// dropped from scheduling candidates regardless of whether the active-only
	// counters are within limits.
	if limit.MaxSandboxCountIncludingPaused != nil {
		total := s.GetSandboxCount() + s.GetPausedSandboxCount()
		if total > *limit.MaxSandboxCountIncludingPaused {
			return false
		}
	}
	if limit.MaxAllocatedCPUIncludingPaused != nil {
		total := s.GetAllocatedCpu() + s.GetPausedAllocatedCpu()
		if total > *limit.MaxAllocatedCPUIncludingPaused {
			return false
		}
	}
	if limit.MaxAllocatedMemoryBytesIncludingPaused != nil {
		total := s.GetAllocatedMemoryBytes() + s.GetPausedAllocatedMemoryBytes()
		if total > *limit.MaxAllocatedMemoryBytesIncludingPaused {
			return false
		}
	}
	return true
}
