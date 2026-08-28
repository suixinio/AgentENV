package scheduler

import (
	"reflect"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"
)

func heartbeatWithRoster(t *testing.T, nodes *AtomicNodeRegistry, nodeID string, now time.Time, sandboxIDs ...string) {
	t.Helper()

	heartbeatWithClusterRoster(t, nodes, nodeID, "cluster-a", now, sandboxIDs...)
}

func heartbeatWithClusterRoster(t *testing.T, nodes *AtomicNodeRegistry, nodeID string, clusterID string, now time.Time, sandboxIDs ...string) {
	t.Helper()

	_, _, err := nodes.Heartbeat(&schedulerv1.HeartbeatRequest{
		NodeId:            nodeID,
		ClusterId:         clusterID,
		ServiceInstanceId: "svc-" + nodeID,
		Snapshot:          &schedulerv1.NodeSnapshot{Status: schedulerv1.NodeStatus_NODE_STATUS_READY},
		Roster:            sandboxRosterFromIDs(sandboxIDs),
	}, now)
	if err != nil {
		t.Fatalf("heartbeat for %s failed: %v", nodeID, err)
	}
}

// sandboxRosterFromIDs builds a roster with only the sandbox id set on each
// entry, one entry per id. This is the roster-shaped equivalent of the
// removed `SandboxIds` wire field: every other field lands on its zero
// value, which is byte-for-byte what the deleted legacy decode path used to
// produce (`RosterEntry{SandboxID: sandboxID}`).
func sandboxRosterFromIDs(sandboxIDs []string) []*schedulerv1.SandboxRosterEntry {
	roster := make([]*schedulerv1.SandboxRosterEntry, 0, len(sandboxIDs))
	for _, id := range sandboxIDs {
		roster = append(roster, &schedulerv1.SandboxRosterEntry{SandboxId: id})
	}
	return roster
}

// rosterIDs is the sandbox ids of a roster, for the assertions that predate
// the incarnation half and are still about the ids alone.
func rosterIDs(roster []RosterEntry) []string {
	ids := make([]string, 0, len(roster))
	for _, entry := range roster {
		ids = append(ids, entry.SandboxID)
	}
	return ids
}

func TestRosterOfKeepsTheHeartbeatRoster(t *testing.T) {
	nodes := NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, defaultObservedReportTTL)
	now := time.Unix(1_700_000_000, 0)

	if _, _, ok := nodes.RosterOf("node-a"); ok {
		t.Fatal("expected no roster before the first heartbeat")
	}

	heartbeatWithRoster(t, nodes, "node-a", now, "s1", "s2")

	roster, lastSeen, ok := nodes.RosterOf("node-a")
	if !ok {
		t.Fatal("expected a roster after a heartbeat")
	}
	if !reflect.DeepEqual(rosterIDs(roster), []string{"s1", "s2"}) {
		t.Fatalf("unexpected roster %v", roster)
	}
	if !lastSeen.Equal(now) {
		t.Fatalf("expected last seen %s, got %s", now, lastSeen)
	}

	// The returned slice is a copy: mutating it must not corrupt the registry.
	roster[0] = RosterEntry{SandboxID: "tampered"}
	if again, _, _ := nodes.RosterOf("node-a"); again[0].SandboxID != "s1" {
		t.Fatalf("expected the stored roster to be insulated from the caller, got %v", again)
	}
}

func TestRosterNormalisesBlanksAndDuplicates(t *testing.T) {
	nodes := NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, defaultObservedReportTTL)
	now := time.Unix(1_700_000_000, 0)

	heartbeatWithRoster(t, nodes, "node-a", now, " s1 ", "", "s1", "s2")

	roster, _, _ := nodes.RosterOf("node-a")
	if !reflect.DeepEqual(rosterIDs(roster), []string{"s1", "s2"}) {
		t.Fatalf("unexpected roster %v", roster)
	}
}

func TestNodesHoldingIsTheReverseIndex(t *testing.T) {
	nodes := NewAtomicNodeRegistry([]Node{
		{ID: "node-a", Endpoint: "http://node-a"},
		{ID: "node-b", Endpoint: "http://node-b"},
	}, defaultObservedReportTTL)
	now := time.Unix(1_700_000_000, 0)

	heartbeatWithRoster(t, nodes, "node-a", now, "s1", "s2")
	heartbeatWithRoster(t, nodes, "node-b", now, "s2", "s3")

	if got := nodes.NodesHolding("s1"); !reflect.DeepEqual(got, []string{"node-a"}) {
		t.Fatalf("unexpected holders of s1: %v", got)
	}
	// Both nodes reporting the same sandbox is the expected transient during a
	// cross-node takeover.
	if got := nodes.NodesHolding("s2"); !reflect.DeepEqual(got, []string{"node-a", "node-b"}) {
		t.Fatalf("unexpected holders of s2: %v", got)
	}
	if got := nodes.NodesHolding("nobody"); got != nil {
		t.Fatalf("expected no holders for an unknown sandbox, got %v", got)
	}

	// A later heartbeat replaces the roster rather than adding to it.
	heartbeatWithRoster(t, nodes, "node-a", now.Add(time.Second), "s1")
	if got := nodes.NodesHolding("s2"); !reflect.DeepEqual(got, []string{"node-b"}) {
		t.Fatalf("expected node-a to have dropped s2, got %v", got)
	}

	// An empty roster clears the node entirely.
	heartbeatWithRoster(t, nodes, "node-a", now.Add(2*time.Second))
	if got := nodes.NodesHolding("s1"); got != nil {
		t.Fatalf("expected an empty roster to clear the node, got %v", got)
	}
}

func TestRostersReturnsEveryObservedNodeSorted(t *testing.T) {
	nodes := NewAtomicNodeRegistry([]Node{
		{ID: "node-b", Endpoint: "http://node-b"},
		{ID: "node-a", Endpoint: "http://node-a"},
	}, defaultObservedReportTTL)
	now := time.Unix(1_700_000_000, 0)

	heartbeatWithRoster(t, nodes, "node-b", now, "s3")
	heartbeatWithRoster(t, nodes, "node-a", now.Add(time.Second), "s1", "s2")

	rosters := nodes.RostersInCluster("")
	if len(rosters) != 2 {
		t.Fatalf("expected 2 rosters, got %d", len(rosters))
	}
	if rosters[0].NodeID != "node-a" || rosters[1].NodeID != "node-b" {
		t.Fatalf("expected rosters sorted by node id, got %s then %s", rosters[0].NodeID, rosters[1].NodeID)
	}
	if !reflect.DeepEqual(rosters[0].SandboxIDs(), []string{"s1", "s2"}) {
		t.Fatalf("unexpected roster for node-a: %v", rosters[0].Entries)
	}
	if !rosters[0].LastSeen.Equal(now.Add(time.Second)) {
		t.Fatalf("unexpected last seen for node-a: %s", rosters[0].LastSeen)
	}
}

func TestUnregisterClearsTheRoster(t *testing.T) {
	nodes := NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, defaultObservedReportTTL)
	now := time.Unix(1_700_000_000, 0)

	heartbeatWithRoster(t, nodes, "node-a", now, "s1")
	if err := nodes.UnregisterObserved("node-a", "svc-node-a"); err != nil {
		t.Fatalf("unregister failed: %v", err)
	}

	if got := nodes.NodesHolding("s1"); got != nil {
		t.Fatalf("expected unregister to clear the reverse index, got %v", got)
	}
	if _, _, ok := nodes.RosterOf("node-a"); ok {
		t.Fatal("expected unregister to drop the roster")
	}
	if got := nodes.RostersInCluster(""); len(got) != 1 || !got[0].LastSeen.IsZero() {
		// The node is still in discovery, so it is still reported — as a node
		// that has told us nothing, which is the truth after an unregister.
		t.Fatalf("expected one empty roster after unregister, got %v", got)
	}
}

// Dropping out of discovery evicts the observation, and the reverse index has
// to go with it or a departed node keeps appearing as a holder forever.
func TestDiscoveryEvictionClearsTheRoster(t *testing.T) {
	nodes := NewAtomicNodeRegistry([]Node{
		{ID: "node-a", Endpoint: "http://node-a"},
		{ID: "node-b", Endpoint: "http://node-b"},
	}, defaultObservedReportTTL)
	now := time.Unix(1_700_000_000, 0)

	heartbeatWithRoster(t, nodes, "node-a", now, "s1")
	heartbeatWithRoster(t, nodes, "node-b", now, "s2")

	nodes.Set([]Node{{ID: "node-b", Endpoint: "http://node-b"}}, nil)

	if got := nodes.NodesHolding("s1"); got != nil {
		t.Fatalf("expected the evicted node to leave the reverse index, got %v", got)
	}
	if got := nodes.NodesHolding("s2"); !reflect.DeepEqual(got, []string{"node-b"}) {
		t.Fatalf("expected the surviving node to keep its roster, got %v", got)
	}
}

// 🔴 The rosters are compared against a registry read that is filtered by
// cluster, so they have to be filtered by the same one. A scheduler watching
// two clusters that skipped this would measure one cluster's rows against the
// other's nodes, and every healthy cross-node takeover in the second cluster
// would show up as a fault in the first.
func TestRostersInClusterScopesToOneCluster(t *testing.T) {
	nodes := NewAtomicNodeRegistry([]Node{
		{ID: "node-a", Endpoint: "http://node-a"},
		{ID: "node-b", Endpoint: "http://node-b"},
	}, defaultObservedReportTTL)
	now := time.Unix(1_700_000_000, 0)

	heartbeatWithClusterRoster(t, nodes, "node-a", "cluster-a", now, "s1")
	heartbeatWithClusterRoster(t, nodes, "node-b", "cluster-b", now, "s2")

	scoped := nodes.RostersInCluster("cluster-a")
	if len(scoped) != 1 || scoped[0].NodeID != "node-a" {
		t.Fatalf("expected only cluster-a's node, got %v", scoped)
	}
	// Cluster ids travel through a config file on one side and an environment
	// variable on the other; a difference in case must not empty the roster.
	if got := nodes.RostersInCluster("CLUSTER-A"); len(got) != 1 || got[0].NodeID != "node-a" {
		t.Fatalf("expected the cluster filter to ignore case, got %v", got)
	}
	// No filter means every cluster, matching a reader configured without one.
	if got := nodes.RostersInCluster(""); len(got) != 2 {
		t.Fatalf("expected both nodes without a filter, got %v", got)
	}
}

// 🔴 A node discovery knows about but has never heard from is the single most
// important thing this view has to report, and it is the one thing a list of
// heartbeats cannot contain. Reported as a roster with a zero LastSeen, it
// reads as stale downstream; omitted, it reads as if the node did not exist.
func TestRostersInClusterReportsNodesThatHaveNeverReported(t *testing.T) {
	nodes := NewAtomicNodeRegistry([]Node{
		{ID: "node-a", Endpoint: "http://node-a"},
		{ID: "node-silent", Endpoint: "http://node-silent"},
	}, defaultObservedReportTTL)
	now := time.Unix(1_700_000_000, 0)

	heartbeatWithRoster(t, nodes, "node-a", now, "s1")

	rosters := nodes.RostersInCluster("cluster-a")
	if len(rosters) != 2 {
		t.Fatalf("expected both nodes to be reported, got %v", rosters)
	}
	silent := rosters[1]
	if silent.NodeID != "node-silent" {
		t.Fatalf("expected the silent node to be reported, got %q", silent.NodeID)
	}
	if !silent.LastSeen.IsZero() {
		t.Fatalf("expected a node that never reported to carry no timestamp, got %s", silent.LastSeen)
	}
	if len(silent.Entries) != 0 {
		t.Fatalf("expected a node that never reported to hold nothing, got %v", silent.Entries)
	}

	// A node that reported to a *different* cluster is not silent, it is
	// somebody else's. Reporting it here as never-heard-from would be a lie in
	// the other direction.
	if got := nodes.RostersInCluster("cluster-z"); len(got) != 1 || got[0].NodeID != "node-silent" {
		t.Fatalf("expected only the node that never claimed a cluster, got %v", got)
	}

	// And once discovery drops a node it disappears from the view entirely —
	// which is exactly why the reconciliation cannot rely on this view alone to
	// notice a node's rows are stranded.
	nodes.Set([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, nil)
	if got := nodes.RostersInCluster("cluster-a"); len(got) != 1 || got[0].NodeID != "node-a" {
		t.Fatalf("expected the removed node to be gone from the view, got %v", got)
	}
}
