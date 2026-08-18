package scheduler

import schedulerv1 "agentenv/services/api/proto"

type Node struct {
	ID       string `json:"node_id"`
	Endpoint string `json:"endpoint"`
	// PodName is the name of the pod currently serving this node, which is the
	// identity a node still reports itself under until its own pod restarts
	// onto the build that reports the machine's name instead. Carrying it lets
	// the registry recognise such a heartbeat rather than rejecting it as an
	// unknown node.
	//
	// Only the *current* pod is aliased — a stale process reporting some
	// earlier pod's name is still refused — and the alias never shadows a real
	// node, so keeping it once the fleet is upgraded costs nothing and leaves
	// the next upgrade equally seamless.
	PodName string `json:"pod_name,omitempty"`
}

// RichNode combines discovery identity with observed runtime state.
// Strategy implementations can use Snapshot for load-aware scheduling.
type RichNode struct {
	Node
	Snapshot *schedulerv1.NodeSnapshot // nil if no heartbeat received yet
}

func (n Node) ToProto() *schedulerv1.Node {
	return &schedulerv1.Node{
		NodeId:   n.ID,
		Endpoint: n.Endpoint,
	}
}

func NodeFromProto(node *schedulerv1.Node) Node {
	if node == nil {
		return Node{}
	}
	return Node{
		ID:       node.GetNodeId(),
		Endpoint: node.GetEndpoint(),
	}
}
