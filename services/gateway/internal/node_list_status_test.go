package gateway

import (
	"testing"

	schedulerv1 "agentenv/services/api/proto"
)

// Every status the scheduler can report must render as itself. This is written
// as an exhaustive walk over the enum rather than a list of cases so that a
// status added later fails here instead of silently rendering as
// "unspecified" — which is how DRAINING and LINGERING both slipped through.
func TestNodeStatusToStringCoversEveryStatus(t *testing.T) {
	expected := map[schedulerv1.NodeStatus]string{
		schedulerv1.NodeStatus_NODE_STATUS_UNSPECIFIED: "unspecified",
		schedulerv1.NodeStatus_NODE_STATUS_READY:       "ready",
		schedulerv1.NodeStatus_NODE_STATUS_CONNECTING:  "connecting",
		schedulerv1.NodeStatus_NODE_STATUS_UNHEALTHY:   "unhealthy",
		schedulerv1.NodeStatus_NODE_STATUS_LINGERING:   "lingering",
		schedulerv1.NodeStatus_NODE_STATUS_DRAINING:    "draining",
	}

	for value, name := range schedulerv1.NodeStatus_name {
		status := schedulerv1.NodeStatus(value)
		want, ok := expected[status]
		if !ok {
			t.Fatalf("%s has no rendering in this test; add it here and in nodeStatusToString", name)
		}
		if got := nodeStatusToString(status); got != want {
			t.Errorf("%s rendered as %q, want %q", name, got, want)
		}
	}
}
