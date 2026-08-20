package scheduler

import (
	schedulerv1 "agentenv/services/api/proto"
	"agentenv/services/shared/routing"
)

// Node is the stored routing node, and it is the shared type verbatim.
//
// 🔴 An alias, not a copy. The gateway reads the same records out of the same
// Redis and cannot import this package, so the struct and its json tags have to
// live somewhere both can reach. A second declaration here that happened to
// match would be a second declaration that has to go on matching.
type Node = routing.Node

// RichNode combines discovery identity with observed runtime state.
// Strategy implementations can use Snapshot for load-aware scheduling.
type RichNode struct {
	Node
	Snapshot *schedulerv1.NodeSnapshot // nil if no heartbeat received yet
}

func NodeFromProto(node *schedulerv1.Node) Node {
	return routing.NodeFromProto(node)
}
