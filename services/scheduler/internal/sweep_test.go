package scheduler

import (
	"context"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// ─────────────────────────────────────────────────────────────────────────────
// The heartbeat-timeout sweep
// ─────────────────────────────────────────────────────────────────────────────
//
// 🔴 The tests that matter here are the ones that prove the sweep *refuses*.
// A sweep that fires is a convenience; a sweep that fires on a sandbox which is
// alive somewhere else is an outage it caused itself, and it is the shape e2b
// shipped (see the note at the top of sweep.go). So the refusal cases run
// against both stores, and each of them fails if the execution guard is removed
// from either implementation.
//
// The registry underneath is the real AtomicNodeRegistry driven by real
// Heartbeat calls, not a stub. Roster normalisation, the incarnation shape
// check and the cluster scoping are all part of what decides which pair the
// sweep hands to the guarded delete, and a stub would answer for none of them.

const sweepCluster = "cluster-sweep"

func sweepNodeA() Node { return Node{ID: "node-a", Endpoint: "http://node-a"} }
func sweepNodeB() Node { return Node{ID: "node-b", Endpoint: "http://node-b"} }

// sweepFixture is a registry, a store and a sweeper over both.
type sweepFixture struct {
	registry *AtomicNodeRegistry
	store    BindingStore
	sweeper  *bindingSweeper
	silence  time.Duration
}

func newSweepFixture(t *testing.T, store BindingStore, nodes ...Node) *sweepFixture {
	t.Helper()
	registry := NewAtomicNodeRegistry(nodes, defaultObservedReportTTL)
	silence := 5 * time.Minute
	return &sweepFixture{
		registry: registry,
		store:    store,
		sweeper:  newBindingSweeper(zap.NewNop(), registry, store, sweepCluster, silence),
		silence:  silence,
	}
}

// report drives one heartbeat through both halves the way Service.Heartbeat
// does: the roster into the node registry, and the same roster into the binding
// store. Anything that only did one of the two would be testing a state the
// scheduler cannot actually reach.
func (f *sweepFixture) report(t *testing.T, node Node, at time.Time, entries ...RosterEntry) {
	t.Helper()

	roster := make([]*schedulerv1.SandboxRosterEntry, 0, len(entries))
	for _, entry := range entries {
		roster = append(roster, &schedulerv1.SandboxRosterEntry{
			SandboxId:   entry.SandboxID,
			ExecutionId: entry.ExecutionID,
		})
	}
	req := &schedulerv1.HeartbeatRequest{
		NodeId:            node.ID,
		ClusterId:         sweepCluster,
		ServiceInstanceId: node.ID + "-instance",
		Roster:            roster,
	}
	// 🔴 Reconciled under the node the *registry* resolved, not the one the
	// heartbeat named, because that is what Service.Heartbeat does. The two
	// differ exactly once — while discovery is renaming a node mid-upgrade —
	// and that is the case TestBindingSweepDoesNotRetireANodeDiscoveryMerelyRenamed
	// is about.
	canonical, _, err := f.registry.Heartbeat(req, at)
	if err != nil {
		t.Fatalf("heartbeat for %s: %v", node.ID, err)
	}
	mustReconcile(t, f.store, canonical, withLongBudget(entries), at)
}

// reportLegacy is the same, through the pre-incarnation sandbox_ids field: no
// roster entries, so no incarnations, which is the input the "nothing to guard
// with" branch exists for.
func (f *sweepFixture) reportLegacy(t *testing.T, node Node, at time.Time, sandboxIDs ...string) {
	t.Helper()

	req := &schedulerv1.HeartbeatRequest{
		NodeId:            node.ID,
		ClusterId:         sweepCluster,
		ServiceInstanceId: node.ID + "-instance",
		SandboxIds:        sandboxIDs,
	}
	canonical, _, err := f.registry.Heartbeat(req, at)
	if err != nil {
		t.Fatalf("heartbeat for %s: %v", node.ID, err)
	}
	entries := make([]RosterEntry, 0, len(sandboxIDs))
	for _, sandboxID := range sandboxIDs {
		entries = append(entries, RosterEntry{SandboxID: sandboxID})
	}
	mustReconcile(t, f.store, canonical, entries, at)
}

// withLongBudget gives every entry the projection budget an authoritative
// scheduler would store it under. Without it the records would carry
// binding_ttl and the sweep would be racing an expiry rather than doing
// anything — which is the very configuration the sweep exists for the absence
// of.
func withLongBudget(entries []RosterEntry) []RosterEntry {
	out := make([]RosterEntry, 0, len(entries))
	for _, entry := range entries {
		entry.ProjectionTTL = 24 * time.Hour
		out = append(out, entry)
	}
	return out
}

// sweepStores runs one case against both binding stores, built the way a
// scheduler with the write switch on builds them.
func sweepStores(t *testing.T, run func(t *testing.T, store BindingStore)) {
	t.Helper()
	projectionStoreContract(t, func(t *testing.T, newStore projectionStoreFactory) {
		run(t, newStore(t, 30*time.Second, true, "enforce"))
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// It fires
// ─────────────────────────────────────────────────────────────────────────────

func TestBindingSweepRetiresTheRecordsOfANodeThatStoppedHeartbeating(t *testing.T) {
	sweepStores(t, func(t *testing.T, store BindingStore) {
		f := newSweepFixture(t, store, sweepNodeA(), sweepNodeB())
		start := time.Now()

		f.report(t, sweepNodeA(), start, RosterEntry{SandboxID: "sbx-1", ExecutionID: execOld})
		assertBinding(t, store, "sbx-1", "node-a", execOld)

		// node-b keeps reporting, so the whole-fleet guard has nothing to say.
		sweepAt := start.Add(f.silence + time.Second)
		f.report(t, sweepNodeB(), sweepAt.Add(-time.Second), RosterEntry{SandboxID: "sbx-2", ExecutionID: execNew})

		before := sweepCounts(t)
		f.sweeper.sweepOnce(sweepAt)
		delta := sweepCounts(t).since(before)

		if _, ok, err := store.Get("sbx-1", sweepAt); err != nil || ok {
			t.Fatalf("the dead node's record survived the sweep: ok=%v err=%v", ok, err)
		}
		if got := delta.sandbox[string(BindingDeleteDeleted)]; got != 1 {
			t.Fatalf("expected one deleted, got %v (all: %v)", got, delta.sandbox)
		}
		if got := delta.node[bindingSweepNodeSwept]; got != 1 {
			t.Fatalf("expected one swept node, got %v (all: %v)", got, delta.node)
		}
		// 🔴 The live node's own record must be untouched. A sweep keyed on
		// "the fleet is unhealthy" rather than on one node would take this too.
		assertBinding(t, store, "sbx-2", "node-b", execNew)
	})
}

// TestBindingSweepLeavesTheNodeRegistryAlone pins the sweep's other half: it
// removes routing records and nothing else.
//
// 🔴 Clearing the node's roster would be the easy way to stop re-sweeping it,
// and it would silently destroy the evidence every registry reconciliation
// gauge is derived from — a machine that died would read as a machine holding
// nothing, which is the same reading as a healthy idle node.
func TestBindingSweepLeavesTheNodeRegistryAlone(t *testing.T) {
	store := NewInMemoryBindingStoreWithModes(30*time.Second, arbitrateFenced, true)
	f := newSweepFixture(t, store, sweepNodeA())
	start := time.Now()

	f.report(t, sweepNodeA(), start, RosterEntry{SandboxID: "sbx-1", ExecutionID: execOld})
	f.sweeper.sweepOnce(start.Add(f.silence + time.Second))

	entries, lastSeen, ok := f.registry.RosterOf("node-a")
	if !ok || len(entries) != 1 || entries[0].SandboxID != "sbx-1" || entries[0].ExecutionID != execOld {
		t.Fatalf("the sweep mutated the node registry's roster: ok=%v entries=%+v", ok, entries)
	}
	if !lastSeen.Equal(start) {
		t.Fatalf("the sweep moved last_seen: got %s want %s", lastSeen, start)
	}
	if holders := f.registry.NodesHolding("sbx-1"); len(holders) != 1 || holders[0] != "node-a" {
		t.Fatalf("the sweep mutated the reverse holder index: %v", holders)
	}
}

func TestBindingSweepFiresOnASingleNodeCluster(t *testing.T) {
	sweepStores(t, func(t *testing.T, store BindingStore) {
		f := newSweepFixture(t, store, sweepNodeA())
		start := time.Now()

		f.report(t, sweepNodeA(), start, RosterEntry{SandboxID: "sbx-1", ExecutionID: execOld})

		before := sweepCounts(t)
		f.sweeper.sweepOnce(start.Add(f.silence + time.Second))
		delta := sweepCounts(t).since(before)

		// 🔴 The whole-fleet guard must not swallow this. On a one-node
		// deployment "every node is silent" and "the node died" are the same
		// sentence, and suppressing there would mean the sweep never fires on
		// the deployment shape it is developed against.
		if got := delta.node[bindingSweepNodeSwept]; got != 1 {
			t.Fatalf("a one-node cluster was not swept: %v", delta.node)
		}
		if got := delta.node[bindingSweepNodeSuppressedAllSilent]; got != 0 {
			t.Fatalf("the whole-fleet guard fired on a one-node cluster: %v", delta.node)
		}
		if _, ok, _ := store.Get("sbx-1", start); ok {
			t.Fatal("the record survived")
		}
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// 🔴 It refuses
// ─────────────────────────────────────────────────────────────────────────────

// TestBindingSweepRefusesToRetireARecordNamingALiveIncarnation is the test this
// whole change is judged by.
//
// The sequence is the one a hard node death actually produces: node-a is
// holding sbx-1 and stops answering; the sandbox is resumed on node-b under a
// new incarnation, which is the cluster working as designed; node-a's silence
// then crosses the threshold. The sweep is looking at node-a's last roster,
// which still names sbx-1 — under the *old* incarnation.
//
// 🔴 A sweep that deleted by sandbox id would delete the record of the sandbox
// that was successfully rescued. That is e2b's rule
// (packages/api/internal/sandbox/storage/redis/main.go:205-217) and it is the
// one thing this must not do.
func TestBindingSweepRefusesToRetireARecordNamingALiveIncarnation(t *testing.T) {
	sweepStores(t, func(t *testing.T, store BindingStore) {
		f := newSweepFixture(t, store, sweepNodeA(), sweepNodeB())
		start := time.Now()

		f.report(t, sweepNodeA(), start, RosterEntry{SandboxID: "sbx-1", ExecutionID: execOld})
		assertBinding(t, store, "sbx-1", "node-a", execOld)

		// The rescue: same sandbox id, new incarnation, new node.
		sweepAt := start.Add(f.silence + time.Second)
		f.report(t, sweepNodeB(), sweepAt.Add(-time.Second), RosterEntry{SandboxID: "sbx-1", ExecutionID: execNew})
		assertBinding(t, store, "sbx-1", "node-b", execNew)

		before := sweepCounts(t)
		f.sweeper.sweepOnce(sweepAt)
		delta := sweepCounts(t).since(before)

		// 🔴 The live record survives, on the live node, under the live
		// incarnation.
		assertBinding(t, store, "sbx-1", "node-b", execNew)
		if got := delta.sandbox[string(BindingDeleteRejectedStale)]; got != 1 {
			t.Fatalf("expected one rejected_stale, got %v (all: %v)", got, delta.sandbox)
		}
		if got := delta.sandbox[string(BindingDeleteDeleted)]; got != 0 {
			t.Fatalf("the sweep deleted a live incarnation's record: %v", delta.sandbox)
		}
	})
}

// TestBindingSweepSkipsARosterEntryWithNoIncarnation.
//
// 🔴 Skipped rather than deleted unguarded, which is the same decision
// ReportSandboxEvent makes for the same reason: an unguarded delete is the race
// the guard exists for. The cost of skipping is nil, because a node too old to
// report an incarnation is also too old to send a projection budget — its
// records carry binding_ttl and expire on their own.
func TestBindingSweepSkipsARosterEntryWithNoIncarnation(t *testing.T) {
	sweepStores(t, func(t *testing.T, store BindingStore) {
		f := newSweepFixture(t, store, sweepNodeA())
		start := time.Now()

		f.reportLegacy(t, sweepNodeA(), start, "sbx-legacy")
		assertBinding(t, store, "sbx-legacy", "node-a", "")

		before := sweepCounts(t)
		f.sweeper.sweepOnce(start.Add(f.silence + time.Second))
		delta := sweepCounts(t).since(before)

		if got := delta.sandbox[sandboxEventIgnoredUnknownExecution]; got != 1 {
			t.Fatalf("expected one ignored_unknown_execution, got %v (all: %v)", got, delta.sandbox)
		}
		if _, ok, _ := store.Get("sbx-legacy", start); !ok {
			t.Fatal("an unguardable record was deleted anyway")
		}
	})
}

func TestBindingSweepLeavesANodeThatIsStillReportingAlone(t *testing.T) {
	sweepStores(t, func(t *testing.T, store BindingStore) {
		f := newSweepFixture(t, store, sweepNodeA())
		start := time.Now()

		f.report(t, sweepNodeA(), start, RosterEntry{SandboxID: "sbx-1", ExecutionID: execOld})

		before := sweepCounts(t)
		// One second short of the threshold. 🔴 The boundary is the whole
		// subject: the default is chosen so a node in maximum backoff still
		// reports several times inside it, and an off-by-one that made the
		// comparison `>=` at report time would retire a node that just spoke.
		f.sweeper.sweepOnce(start.Add(f.silence))
		delta := sweepCounts(t).since(before)

		if len(delta.node) != 0 || len(delta.sandbox) != 0 {
			t.Fatalf("a node inside the threshold was acted on: nodes=%v sandboxes=%v", delta.node, delta.sandbox)
		}
		assertBinding(t, store, "sbx-1", "node-a", execOld)
	})
}

// TestBindingSweepSuppressesWhenEveryReportingNodeIsSilent.
//
// 🔴 A scheduler that cannot reach anything sees exactly what a scheduler
// watching a dead cluster sees. Of the two, the first is overwhelmingly more
// likely and the second is not made worse by waiting; retiring on that reading
// would empty the whole routing table for a fault that fixes itself.
func TestBindingSweepSuppressesWhenEveryReportingNodeIsSilent(t *testing.T) {
	store := NewInMemoryBindingStoreWithModes(30*time.Second, arbitrateFenced, true)
	f := newSweepFixture(t, store, sweepNodeA(), sweepNodeB())
	start := time.Now()

	f.report(t, sweepNodeA(), start, RosterEntry{SandboxID: "sbx-1", ExecutionID: execOld})
	f.report(t, sweepNodeB(), start, RosterEntry{SandboxID: "sbx-2", ExecutionID: execNew})

	before := sweepCounts(t)
	f.sweeper.sweepOnce(start.Add(f.silence + time.Second))
	delta := sweepCounts(t).since(before)

	if got := delta.node[bindingSweepNodeSuppressedAllSilent]; got != 2 {
		t.Fatalf("expected both silent nodes to be counted as suppressed, got %v", delta.node)
	}
	if got := delta.node[bindingSweepNodeSwept]; got != 0 {
		t.Fatalf("the sweep ran anyway: %v", delta.node)
	}
	if len(delta.sandbox) != 0 {
		t.Fatalf("records were retired during a suppressed round: %v", delta.sandbox)
	}
	assertBinding(t, store, "sbx-1", "node-a", execOld)
	assertBinding(t, store, "sbx-2", "node-b", execNew)
}

// TestBindingSweepHandlesAClusterThatDiesOneMachineAtATime.
//
// 🔴 The whole-fleet guard counts only the nodes a round could still act on. If
// it counted the ones it had already retired, the first death would prop up the
// denominator — so the second death, with the first node still listed by
// discovery, would read as "everything is silent at once" and be suppressed
// permanently. A cluster losing machines one at a time is a rolling hardware
// fault, not a partition, and each of them has to be handled on its own.
func TestBindingSweepHandlesAClusterThatDiesOneMachineAtATime(t *testing.T) {
	store := NewInMemoryBindingStoreWithModes(30*time.Second, arbitrateFenced, true)
	f := newSweepFixture(t, store, sweepNodeA(), sweepNodeB())
	start := time.Now()

	f.report(t, sweepNodeA(), start, RosterEntry{SandboxID: "sbx-1", ExecutionID: execOld})
	// node-b outlives node-a by ten minutes, then stops too.
	bLastSeen := start.Add(10 * time.Minute)
	f.report(t, sweepNodeB(), bLastSeen, RosterEntry{SandboxID: "sbx-2", ExecutionID: execNew})

	// node-a alone is past the threshold. node-b is still reporting.
	first := start.Add(f.silence + time.Minute)
	f.sweeper.sweepOnce(first)
	if _, ok, _ := store.Get("sbx-1", first); ok {
		t.Fatal("the first dead node was not swept")
	}
	assertBinding(t, store, "sbx-2", "node-b", execNew)

	// Now node-b is past it too, and node-a — still listed by discovery, since
	// a Pod object outlives the machine — has already been retired.
	second := bLastSeen.Add(f.silence + time.Minute)
	before := sweepCounts(t)
	f.sweeper.sweepOnce(second)
	delta := sweepCounts(t).since(before)

	if got := delta.node[bindingSweepNodeSuppressedAllSilent]; got != 0 {
		t.Fatalf("a staggered second death was read as a partition: %v", delta.node)
	}
	if got := delta.node[bindingSweepNodeSwept]; got != 1 {
		t.Fatalf("the second dead node was not swept: %v", delta.node)
	}
	if _, ok, _ := store.Get("sbx-2", second); ok {
		t.Fatal("the second dead node's record survived")
	}
}

// TestBindingSweepDoesNotCountANodeThatHasNeverReported.
//
// A machine discovery has just learned about has no heartbeat and holds
// nothing. 🔴 Counting it as silent would let adding one node to a two-node
// cluster suppress the sweep of a node that really did die.
func TestBindingSweepDoesNotCountANodeThatHasNeverReported(t *testing.T) {
	store := NewInMemoryBindingStoreWithModes(30*time.Second, arbitrateFenced, true)
	f := newSweepFixture(t, store, sweepNodeA(), sweepNodeB())
	start := time.Now()

	// Only node-a ever reports; node-b is known to discovery and silent from
	// birth.
	f.report(t, sweepNodeA(), start, RosterEntry{SandboxID: "sbx-1", ExecutionID: execOld})

	before := sweepCounts(t)
	f.sweeper.sweepOnce(start.Add(f.silence + time.Second))
	delta := sweepCounts(t).since(before)

	if got := delta.node[bindingSweepNodeSwept]; got != 1 {
		t.Fatalf("the never-reported node suppressed a real sweep: %v", delta.node)
	}
	if got := delta.node[bindingSweepNodeSuppressedAllSilent]; got != 0 {
		t.Fatalf("a never-reported node was counted as silent: %v", delta.node)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 🔴 It survives the node leaving discovery
// ─────────────────────────────────────────────────────────────────────────────

// TestBindingSweepRetiresANodeDiscoveryHasAlreadyDropped is the case the whole
// sweep would otherwise miss.
//
// Kubernetes discovery drops an endpoint as soon as its Serving condition goes
// false, which for a machine that died hard is one node-monitor grace period —
// tens of seconds, far inside any threshold worth having. AtomicNodeRegistry.Set
// then deletes that node's observed record and clears its roster. A sweep that
// read only the registry would find nothing to retire, in exactly the scenario
// it was built for.
func TestBindingSweepRetiresANodeDiscoveryHasAlreadyDropped(t *testing.T) {
	sweepStores(t, func(t *testing.T, store BindingStore) {
		f := newSweepFixture(t, store, sweepNodeA(), sweepNodeB())
		start := time.Now()

		f.report(t, sweepNodeA(), start, RosterEntry{SandboxID: "sbx-1", ExecutionID: execOld})
		f.report(t, sweepNodeB(), start, RosterEntry{SandboxID: "sbx-2", ExecutionID: execNew})

		// One round while both are known: the sweeper takes its own copy.
		f.sweeper.sweepOnce(start.Add(time.Minute))

		// The machine dies and its endpoint stops Serving.
		f.registry.Set([]Node{sweepNodeB()}, nil)
		if _, _, ok := f.registry.RosterOf("node-a"); ok {
			t.Fatal("the registry kept a roster for a node discovery dropped; this test no longer covers what it says")
		}
		// node-b keeps reporting, so the whole-fleet guard stays quiet.
		sweepAt := start.Add(f.silence + time.Minute)
		f.report(t, sweepNodeB(), sweepAt.Add(-time.Second), RosterEntry{SandboxID: "sbx-2", ExecutionID: execNew})

		before := sweepCounts(t)
		f.sweeper.sweepOnce(sweepAt)
		delta := sweepCounts(t).since(before)

		if got := delta.node[bindingSweepNodeSwept]; got != 1 {
			t.Fatalf("a node discovery had already dropped was never swept: %v", delta.node)
		}
		if _, ok, _ := store.Get("sbx-1", sweepAt); ok {
			t.Fatal("the dropped node's record survived")
		}
		assertBinding(t, store, "sbx-2", "node-b", execNew)
	})
}

// TestBindingSweepDoesNotRetireANodeDiscoveryMerelyRenamed.
//
// 🔴 The one shape the execution guard cannot catch. During a fleet upgrade
// discovery stops naming a node after its pod and starts naming it after the
// machine, so the old identity disappears and a new one appears for the same
// running host — the case AtomicNodeRegistry.aliasToID exists for. The shadow
// under the old name looks exactly like a dead node, and every sandbox on that
// host is still reporting under the *same* incarnation, so a delete would be
// accepted and a healthy machine would lose its whole routing table.
func TestBindingSweepDoesNotRetireANodeDiscoveryMerelyRenamed(t *testing.T) {
	sweepStores(t, func(t *testing.T, store BindingStore) {
		podName := Node{ID: "agentenv-node-abcde", Endpoint: "http://node-a"}
		machineName := Node{ID: "node-a", Endpoint: "http://node-a", PodName: podName.ID}

		f := newSweepFixture(t, store, podName, sweepNodeB())
		start := time.Now()

		f.report(t, podName, start, RosterEntry{SandboxID: "sbx-1", ExecutionID: execOld})
		f.report(t, sweepNodeB(), start, RosterEntry{SandboxID: "sbx-2", ExecutionID: execNew})
		f.sweeper.sweepOnce(start.Add(time.Minute))

		// Discovery renames it. The machine never stopped running, and it goes
		// on reporting the same sandbox under the same incarnation — under its
		// old node id, which is what a node mid-upgrade sends.
		f.registry.Set([]Node{machineName, sweepNodeB()}, nil)
		sweepAt := start.Add(f.silence + time.Minute)
		f.report(t, podName, sweepAt.Add(-time.Second), RosterEntry{SandboxID: "sbx-1", ExecutionID: execOld})
		f.report(t, sweepNodeB(), sweepAt.Add(-time.Second), RosterEntry{SandboxID: "sbx-2", ExecutionID: execNew})

		before := sweepCounts(t)
		f.sweeper.sweepOnce(sweepAt)
		delta := sweepCounts(t).since(before)

		if len(delta.node) != 0 || len(delta.sandbox) != 0 {
			t.Fatalf("a renamed node was swept: nodes=%v sandboxes=%v", delta.node, delta.sandbox)
		}
		// 🔴 The record is still routable, under the machine's new name.
		assertBinding(t, store, "sbx-1", "node-a", execOld)
	})
}

// TestBindingSweepForgetsANodeItHasRetiredAndDiscoveryHasDropped keeps the
// shadow map from being a leak. 🔴 Both conditions are needed: dropping on
// absence alone would forget the shadow of the very node the sweep exists for,
// since that node leaves discovery long before the threshold elapses.
func TestBindingSweepForgetsANodeItHasRetiredAndDiscoveryHasDropped(t *testing.T) {
	store := NewInMemoryBindingStoreWithModes(30*time.Second, arbitrateFenced, true)
	f := newSweepFixture(t, store, sweepNodeA(), sweepNodeB())
	start := time.Now()

	f.report(t, sweepNodeA(), start, RosterEntry{SandboxID: "sbx-1", ExecutionID: execOld})
	f.report(t, sweepNodeB(), start, RosterEntry{SandboxID: "sbx-2", ExecutionID: execNew})
	f.registry.Set([]Node{sweepNodeB()}, nil)

	sweepAt := start.Add(f.silence + time.Minute)
	f.report(t, sweepNodeB(), sweepAt.Add(-time.Second), RosterEntry{SandboxID: "sbx-2", ExecutionID: execNew})
	f.sweeper.sweepOnce(sweepAt)

	if _, held := f.sweeper.lastKnown["node-a"]; held {
		t.Fatalf("a swept, dropped node kept its shadow: %v", f.sweeper.lastKnown)
	}
	if _, held := f.sweeper.swept["node-a"]; held {
		t.Fatal("a swept, dropped node kept its bookkeeping")
	}
	// The live node keeps both, because it may report again.
	if _, held := f.sweeper.lastKnown["node-b"]; !held {
		t.Fatal("the live node lost its shadow")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// It acts once
// ─────────────────────────────────────────────────────────────────────────────

func TestBindingSweepRetiresOncePerReport(t *testing.T) {
	store := NewInMemoryBindingStoreWithModes(30*time.Second, arbitrateFenced, true)
	f := newSweepFixture(t, store, sweepNodeA())
	start := time.Now()

	f.report(t, sweepNodeA(), start, RosterEntry{SandboxID: "sbx-1", ExecutionID: execOld})

	sweepAt := start.Add(f.silence + time.Second)
	f.sweeper.sweepOnce(sweepAt)

	before := sweepCounts(t)
	f.sweeper.sweepOnce(sweepAt.Add(30 * time.Second))
	delta := sweepCounts(t).since(before)

	// 🔴 Nothing at all on the second round. Re-running the deletes would
	// answer noop_absent every thirty seconds for the life of the process,
	// which turns the metric that is supposed to prove the sweep fired into a
	// number that rises forever on a cluster where nothing is happening.
	if len(delta.node) != 0 || len(delta.sandbox) != 0 {
		t.Fatalf("the second round acted again: nodes=%v sandboxes=%v", delta.node, delta.sandbox)
	}
}

func TestBindingSweepActsAgainAfterTheNodeComesBackAndGoesSilentAgain(t *testing.T) {
	store := NewInMemoryBindingStoreWithModes(30*time.Second, arbitrateFenced, true)
	f := newSweepFixture(t, store, sweepNodeA())
	start := time.Now()

	f.report(t, sweepNodeA(), start, RosterEntry{SandboxID: "sbx-1", ExecutionID: execOld})
	f.sweeper.sweepOnce(start.Add(f.silence + time.Second))

	// It came back, under a new incarnation, and installed a record again.
	returned := start.Add(f.silence + 2*time.Minute)
	f.report(t, sweepNodeA(), returned, RosterEntry{SandboxID: "sbx-1", ExecutionID: execNew})
	assertBinding(t, store, "sbx-1", "node-a", execNew)

	before := sweepCounts(t)
	f.sweeper.sweepOnce(returned.Add(f.silence + time.Second))
	delta := sweepCounts(t).since(before)

	if got := delta.node[bindingSweepNodeSwept]; got != 1 {
		t.Fatalf("a node that came back and died again was never swept again: %v", delta.node)
	}
	if _, ok, _ := store.Get("sbx-1", returned); ok {
		t.Fatal("the second incarnation's record survived the second sweep")
	}
}

// TestBindingSweepRetriesAfterAStoreFailure.
//
// 🔴 A store that could not be reached must not consume the one chance this
// node's records had. The only other remover is a heartbeat the node will never
// send, so "swept, failed, never tried again" is permanent.
func TestBindingSweepRetriesAfterAStoreFailure(t *testing.T) {
	f := newSweepFixture(t, failingBindingStore{}, sweepNodeA())
	start := time.Now()

	// The registry half by hand: the store half would fail.
	if _, _, err := f.registry.Heartbeat(&schedulerv1.HeartbeatRequest{
		NodeId:            "node-a",
		ClusterId:         sweepCluster,
		ServiceInstanceId: "node-a-instance",
		Roster:            []*schedulerv1.SandboxRosterEntry{{SandboxId: "sbx-1", ExecutionId: execOld}},
	}, start); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}

	before := sweepCounts(t)
	f.sweeper.sweepOnce(start.Add(f.silence + time.Second))
	f.sweeper.sweepOnce(start.Add(f.silence + 31*time.Second))
	delta := sweepCounts(t).since(before)

	if got := delta.sandbox[sandboxEventStoreError]; got != 2 {
		t.Fatalf("expected the failed node to be retried, got %v store errors (all: %v)", got, delta.sandbox)
	}
	if got := delta.node[bindingSweepNodeSwept]; got != 0 {
		t.Fatalf("a round that deleted nothing was recorded as a completed sweep: %v", delta.node)
	}
}

// TestBindingSweepIgnoresAnotherClustersNodes pins the scoping. A scheduler
// answering for one cluster must not conclude anything about a node that
// reported to another.
func TestBindingSweepIgnoresAnotherClustersNodes(t *testing.T) {
	store := NewInMemoryBindingStoreWithModes(30*time.Second, arbitrateFenced, true)
	registry := NewAtomicNodeRegistry([]Node{sweepNodeA()}, defaultObservedReportTTL)
	start := time.Now()

	if _, _, err := registry.Heartbeat(&schedulerv1.HeartbeatRequest{
		NodeId:            "node-a",
		ClusterId:         "some-other-cluster",
		ServiceInstanceId: "node-a-instance",
		Roster:            []*schedulerv1.SandboxRosterEntry{{SandboxId: "sbx-1", ExecutionId: execOld}},
	}, start); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	mustReconcile(t, store, sweepNodeA(), withLongBudget([]RosterEntry{{SandboxID: "sbx-1", ExecutionID: execOld}}), start)

	sweeper := newBindingSweeper(zap.NewNop(), registry, store, sweepCluster, 5*time.Minute)
	before := sweepCounts(t)
	sweeper.sweepOnce(start.Add(6 * time.Minute))
	delta := sweepCounts(t).since(before)

	if len(delta.node) != 0 || len(delta.sandbox) != 0 {
		t.Fatalf("another cluster's node was swept: nodes=%v sandboxes=%v", delta.node, delta.sandbox)
	}
	assertBinding(t, store, "sbx-1", "node-a", execOld)
}

// ─────────────────────────────────────────────────────────────────────────────
// The switch
// ─────────────────────────────────────────────────────────────────────────────

// TestRunBindingSweepDoesNothingWithTheSwitchOff.
//
// 🔴 The switch defaults off and one thing decides it. A scheduler upgraded
// into a fleet that is already heartbeating must not begin deleting routing
// records because its binary changed.
func TestRunBindingSweepDoesNothingWithTheSwitchOff(t *testing.T) {
	store := NewInMemoryBindingStoreWithModes(30*time.Second, arbitrateFenced, true)
	service := NewService(zap.NewNop(), NewAtomicNodeRegistry([]Node{sweepNodeA()}, defaultObservedReportTTL),
		NewStrategy("round_robin"), store)
	if service.bindingSweep {
		t.Fatal("the sweep is on by default")
	}

	done := make(chan struct{})
	go func() {
		// A context that is never cancelled: the only way this returns is the
		// switch.
		service.RunBindingSweep(context.Background(), time.Millisecond)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("RunBindingSweep kept running with the switch off")
	}
}

func TestWithBindingSweepTurnsItOnAndKeepsTheDefaultSilence(t *testing.T) {
	service := NewService(zap.NewNop(), nil, NewStrategy("round_robin"),
		NewInMemoryBindingStore(30*time.Second), WithBindingSweep(0))
	if !service.bindingSweep {
		t.Fatal("WithBindingSweep did not turn the sweep on")
	}
	// 🔴 Zero is an unset field, not "retire everything that has not spoken in
	// this instant".
	if service.bindingSweepSilence != defaultBindingSweepSilence {
		t.Fatalf("a zero silence became %s, want the default", service.bindingSweepSilence)
	}

	service = NewService(zap.NewNop(), nil, NewStrategy("round_robin"),
		NewInMemoryBindingStore(30*time.Second), WithBindingSweep(90*time.Second))
	if service.bindingSweepSilence != 90*time.Second {
		t.Fatalf("silence = %s, want 90s", service.bindingSweepSilence)
	}
}

// TestBindingSweepDefaultIsAboveEverythingItHasToOutlast.
//
// 🔴 Four orderings, and each one is a way the sweep could be wrong rather than
// a tidy inequality:
//
//   - above report_ttl, or the roster fallback re-answers with the same dead
//     node the instant the record is retired;
//   - above the node's own report backoff ceiling, or a node that merely lost
//     the scheduler for a minute is declared dead;
//   - above the reconciliation's roster-stale threshold, so the sweep is the
//     last observer of silence and every gauge an operator watches has already
//     turned;
//   - far below the projection ceiling it exists to cut short, or it changes
//     nothing.
func TestBindingSweepDefaultIsAboveEverythingItHasToOutlast(t *testing.T) {
	if defaultBindingSweepSilence <= defaultObservedReportTTL {
		t.Fatalf("silence %s is not above report_ttl %s", defaultBindingSweepSilence, defaultObservedReportTTL)
	}
	// MAX_REPORT_BACKOFF, src/observability/reporter.rs.
	if defaultBindingSweepSilence <= 60*time.Second {
		t.Fatalf("silence %s does not outlast the node's report backoff ceiling", defaultBindingSweepSilence)
	}
	if defaultBindingSweepSilence <= rosterStaleMultiplier*defaultObservedReportTTL {
		t.Fatalf("silence %s fires before the reconciliation calls a roster stale", defaultBindingSweepSilence)
	}
	if defaultBindingSweepSilence >= defaultMaxProjectionTTL/10 {
		t.Fatalf("silence %s is not meaningfully shorter than the projection ceiling %s",
			defaultBindingSweepSilence, defaultMaxProjectionTTL)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

// sweepCountSet is both sweep counters read at one instant. They are read
// together and compared as deltas because the collectors are process-wide, so
// an absolute value would be whatever the tests before this one left behind.
type sweepCountSet struct {
	sandbox map[string]float64
	node    map[string]float64
}

func (s sweepCountSet) since(before sweepCountSet) sweepCountSet {
	return sweepCountSet{
		sandbox: deltaOf(before.sandbox, s.sandbox),
		node:    deltaOf(before.node, s.node),
	}
}

func deltaOf(before, after map[string]float64) map[string]float64 {
	delta := map[string]float64{}
	for label, value := range after {
		if diff := value - before[label]; diff != 0 {
			delta[label] = diff
		}
	}
	return delta
}

func sweepCounts(t *testing.T) sweepCountSet {
	t.Helper()
	return sweepCountSet{
		sandbox: counterSeriesByLabel(t, schedulerBindingSweep, "agentenv_scheduler_binding_sweep_total", "outcome"),
		node:    counterSeriesByLabel(t, schedulerBindingSweepNodes, "agentenv_scheduler_binding_sweep_nodes_total", "outcome"),
	}
}

// counterSeriesByLabel gathers one collector through a private registry, the
// same shape the arbitration and reconciliation metric tests use.
func counterSeriesByLabel(t *testing.T, collector prometheus.Collector, name string, label string) map[string]float64 {
	t.Helper()

	reg := prometheus.NewRegistry()
	reg.MustRegister(collector)
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather %s: %v", name, err)
	}
	series := map[string]float64{}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, pair := range metric.GetLabel() {
				if pair.GetName() == label {
					series[pair.GetValue()] = metric.GetCounter().GetValue()
				}
			}
		}
	}
	return series
}
