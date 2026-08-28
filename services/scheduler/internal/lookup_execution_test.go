package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"
	pausedregistry "agentenv/services/scheduler/internal/registry"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const lookupExecution = "00000001-0000-7000-8000-0000000000ff"

// TestLookupCarriesTheExecutionFromTheBinding: the hot path, where almost every
// answer comes from.
func TestLookupCarriesTheExecutionFromTheBinding(t *testing.T) {
	store := NewInMemoryBindingStore(time.Minute)
	if err := store.Record("sbx-1", Binding{Node: lookupTestNodes[0], ExecutionID: lookupExecution}, time.Now()); err != nil {
		t.Fatalf("record: %v", err)
	}
	svc := newLookupTestService(t, store, forbiddenRegistryReader{t: t}, testReportTTL)

	resp, err := lookup(t, svc, "sbx-1")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if resp.GetExecutionId() != lookupExecution {
		t.Fatalf("execution_id: got %q, want %q", resp.GetExecutionId(), lookupExecution)
	}
	if got := resp.GetExecutionAuthority(); got != schedulerv1.ExecutionAuthority_EXECUTION_AUTHORITY_REGISTRY {
		t.Fatalf("authority: got %v, want REGISTRY", got)
	}
}

// TestLookupReportsUnknownForABindingWithoutAnExecution.
//
// 🔴 Never REGISTRY with an empty value. "Authoritatively, no incarnation" is a
// sentence the caller would have to compare an empty string against.
func TestLookupReportsUnknownForABindingWithoutAnExecution(t *testing.T) {
	store := NewInMemoryBindingStore(time.Minute)
	if err := store.Record("sbx-1", Binding{Node: lookupTestNodes[0]}, time.Now()); err != nil {
		t.Fatalf("record: %v", err)
	}
	svc := newLookupTestService(t, store, forbiddenRegistryReader{t: t}, testReportTTL)

	resp, err := lookup(t, svc, "sbx-1")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if resp.GetExecutionId() != "" {
		t.Fatalf("execution_id: got %q, want empty", resp.GetExecutionId())
	}
	if got := resp.GetExecutionAuthority(); got != schedulerv1.ExecutionAuthority_EXECUTION_AUTHORITY_UNKNOWN {
		t.Fatalf("authority: got %v, want UNKNOWN", got)
	}
}

// TestPlacedAndPinnedAlwaysReportPending.
//
// 🔴 The `publishing` case is the hard one, and it is why this cannot be left
// to "the value happens to be empty". A publishing row *does* name an
// incarnation — and that incarnation belongs to a VM that was stopped before
// the upload started. Letting it flow through would make the caller compare
// live traffic against a dead VM's id and refuse every data-plane wake-up.
func TestPlacedAndPinnedAlwaysReportPending(t *testing.T) {
	cases := map[string]pausedregistry.Sandbox{
		"paused": {
			SandboxID: "sbx-1", State: pausedregistry.StatePaused,
			OriginNodeID: "node-a", SnapshotID: "snap",
		},
		"local_only": {
			SandboxID: "sbx-1", State: pausedregistry.StateLocalOnly,
			OriginNodeID: "node-a",
		},
		// 🔴 With a non-empty incarnation on the row, which is what makes this
		// a real assertion rather than a restatement of the schema.
		"publishing": {
			SandboxID: "sbx-1", State: pausedregistry.StatePublishing,
			OriginNodeID: "node-a", ExecutionID: lookupExecution,
		},
	}

	for name, entry := range cases {
		t.Run(name, func(t *testing.T) {
			reader := registryReaderOver(entry)
			svc := newLookupTestService(t, missingBindingStore{}, reader, testReportTTL)
			allNodesReady(t, svc)

			resp, err := lookup(t, svc, "sbx-1")
			if err != nil {
				t.Fatalf("lookup: %v", err)
			}
			if got := resp.GetExecutionAuthority(); got != schedulerv1.ExecutionAuthority_EXECUTION_AUTHORITY_PENDING {
				t.Fatalf("authority: got %v, want PENDING", got)
			}
			if resp.GetExecutionId() != "" {
				t.Fatalf("execution_id: got %q, want empty — the node is about to mint one", resp.GetExecutionId())
			}
		})
	}
}

// TestRunningRowReportsRegistry is the control for the test above.
//
// 🟢 Without it an implementation that answered PENDING everywhere would pass,
// and no answer would ever be usable for fencing.
func TestRunningRowReportsRegistry(t *testing.T) {
	reader := registryReaderOver(pausedregistry.Sandbox{
		SandboxID: "sbx-1", State: pausedregistry.StateRunning,
		OriginNodeID: "node-a", ExecutionID: lookupExecution,
	})
	svc := newLookupTestService(t, missingBindingStore{}, reader, testReportTTL)
	allNodesReady(t, svc)

	resp, err := lookup(t, svc, "sbx-1")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if got := resp.GetExecutionAuthority(); got != schedulerv1.ExecutionAuthority_EXECUTION_AUTHORITY_REGISTRY {
		t.Fatalf("authority: got %v, want REGISTRY", got)
	}
	if resp.GetExecutionId() != lookupExecution {
		t.Fatalf("execution_id: got %q, want %q", resp.GetExecutionId(), lookupExecution)
	}
}

// TestResumingRowReportsRegistryUnderPreallocation.
//
// The claim allocates the incarnation the resume will run under and writes it
// with the claim, so a `resuming` row can be answered for authoritatively —
// which is what lets the window between a claim and a live VM be defended
// rather than skipped.
func TestResumingRowReportsRegistryUnderPreallocation(t *testing.T) {
	reader := registryReaderOver(pausedregistry.Sandbox{
		SandboxID: "sbx-1", State: pausedregistry.StateResuming,
		OriginNodeID: "node-a", ClaimedByNodeID: "node-b", ExecutionID: lookupExecution,
	})
	svc := newLookupTestService(t, missingBindingStore{}, reader, testReportTTL)
	allNodesReady(t, svc)

	resp, err := lookup(t, svc, "sbx-1")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	// Holder() is always origin_node_id, never the claimant
	// (claimed_by_node_id): the claimant is an api-replica process under
	// aenv-api|node and structurally never reports a heartbeat, so routing
	// on it would always fail. See lookup_test.go's
	// TestLookupRoutesAResumingSandboxToItsOrigin for why.
	if got := resp.GetNode().GetNodeId(); got != "node-a" {
		t.Fatalf("a resuming row must route to its origin, got %q", got)
	}
	if got := resp.GetExecutionAuthority(); got != schedulerv1.ExecutionAuthority_EXECUTION_AUTHORITY_REGISTRY {
		t.Fatalf("authority: got %v, want REGISTRY", got)
	}
	if resp.GetExecutionId() != lookupExecution {
		t.Fatalf("execution_id: got %q, want %q", resp.GetExecutionId(), lookupExecution)
	}
}

// TestRosterHolderPrefersTheNewerExecution.
//
// 🔴 The node with the *newer* incarnation reported first, so freshness and
// incarnation point in opposite directions. Under the old "most recent report
// wins" rule the stale node wins, which is the roster-side half of the same
// defect the binding store had.
func TestRosterHolderPrefersTheNewerExecution(t *testing.T) {
	svc := newLookupTestService(t, missingBindingStore{}, forbiddenRegistryReader{t: t}, testReportTTL)

	heartbeatWithExecutions(t, svc, "node-b", RosterEntry{SandboxID: "sbx-1", ExecutionID: execNew})
	heartbeatWithExecutions(t, svc, "node-a", RosterEntry{SandboxID: "sbx-1", ExecutionID: execOld})

	resp, err := lookup(t, svc, "sbx-1")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if got := resp.GetNode().GetNodeId(); got != "node-b" {
		t.Fatalf("the roster fallback routed to the stale copy: got %q", got)
	}
	if resp.GetExecutionId() != execNew {
		t.Fatalf("execution_id: got %q, want %q", resp.GetExecutionId(), execNew)
	}
}

// TestRosterHolderStillPrefersTheFresherReportOnATie is the control.
//
// 🟢 One machine reporting under two names during a rollout has one
// incarnation and two entries, and freshness is the right answer there. Without
// this the old rule could be deleted outright and nothing would notice.
func TestRosterHolderStillPrefersTheFresherReportOnATie(t *testing.T) {
	svc := newLookupTestService(t, missingBindingStore{}, forbiddenRegistryReader{t: t}, testReportTTL)

	heartbeatWithExecutions(t, svc, "node-a", RosterEntry{SandboxID: "sbx-1", ExecutionID: execNew})
	time.Sleep(2 * time.Millisecond)
	heartbeatWithExecutions(t, svc, "node-b", RosterEntry{SandboxID: "sbx-1", ExecutionID: execNew})

	resp, err := lookup(t, svc, "sbx-1")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if got := resp.GetNode().GetNodeId(); got != "node-b" {
		t.Fatalf("on a tie the freshest report must win, got %q", got)
	}
}

// TestExecutionAxisAddsNoNewNotFound — S8.
//
// 🔴 NotFound is the one answer that must not spread. Downstream a 404 on a
// resume is how a platform decides a workspace may be rebuilt from nothing, so
// every branch that cannot answer says Unavailable instead. This walks the
// inputs the incarnation work added and asserts none of them found a new way to
// produce it.
func TestExecutionAxisAddsNoNewNotFound(t *testing.T) {
	// 🔴 The binding is seeded *after* the heartbeats. A heartbeat with an
	// empty roster reconciles that node's bindings away, so seeding first
	// would silently turn every binding case into a registry case — and the
	// test would still pass, for the wrong reason.
	bindTo := func(executionID string) func(BindingStore) {
		return func(store BindingStore) {
			_ = store.Record("sbx-1", Binding{Node: lookupTestNodes[0], ExecutionID: executionID}, time.Now())
		}
	}
	registryRow := func(state pausedregistry.State, executionID string) pausedregistry.Reader {
		return registryReaderOver(pausedregistry.Sandbox{
			SandboxID: "sbx-1", State: state, OriginNodeID: "node-a",
			SnapshotID: "snap", ExecutionID: executionID,
		})
	}

	cases := []struct {
		name   string
		store  BindingStore
		seed   func(BindingStore)
		reader pausedregistry.Reader
		roster []RosterEntry
	}{
		{name: "binding with an execution", store: NewInMemoryBindingStore(time.Minute), seed: bindTo(lookupExecution), reader: forbiddenRegistryReader{t: t}},
		{name: "binding without one", store: NewInMemoryBindingStore(time.Minute), seed: bindTo(""), reader: forbiddenRegistryReader{t: t}},
		{name: "roster with an execution", store: missingBindingStore{}, reader: forbiddenRegistryReader{t: t},
			roster: []RosterEntry{{SandboxID: "sbx-1", ExecutionID: lookupExecution}}},
		{name: "roster without one", store: missingBindingStore{}, reader: forbiddenRegistryReader{t: t},
			roster: []RosterEntry{{SandboxID: "sbx-1"}}},
		{name: "paused row", store: missingBindingStore{}, reader: registryRow(pausedregistry.StatePaused, "")},
		{name: "publishing row", store: missingBindingStore{}, reader: registryRow(pausedregistry.StatePublishing, lookupExecution)},
		{name: "local_only row", store: missingBindingStore{}, reader: registryRow(pausedregistry.StateLocalOnly, "")},
		{name: "running row", store: missingBindingStore{}, reader: registryRow(pausedregistry.StateRunning, lookupExecution)},
		{name: "running row without an execution", store: missingBindingStore{}, reader: registryRow(pausedregistry.StateRunning, "")},
		{name: "resuming row", store: missingBindingStore{}, reader: registryRow(pausedregistry.StateResuming, lookupExecution)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc := newLookupTestService(t, tc.store, tc.reader, testReportTTL)
			if len(tc.roster) > 0 {
				heartbeatWithExecutions(t, svc, "node-a", tc.roster...)
				heartbeatWithExecutions(t, svc, "node-b")
			} else {
				allNodesReady(t, svc)
			}
			if tc.seed != nil {
				tc.seed(tc.store)
			}

			_, err := lookup(t, svc, "sbx-1")
			if status.Code(err) == codes.NotFound {
				t.Fatalf("this input produced NotFound, which downstream reads as a sandbox that no longer exists: %v", err)
			}
		})
	}
}

// TestSchedulerServiceNeverReturnsPermissionDenied — the mechanical guarantee
// behind a code that belongs to a different service.
//
// 🔴 PermissionDenied is what the paused-registry service answers a fenced
// write with, and the gateway has no branch for it: it falls through to 502,
// "the upstream is broken". The upstream is not broken — a precise fact would
// become a wrong diagnosis. The Scheduler service must therefore never produce
// it, whatever else it grows.
func TestSchedulerServiceNeverReturnsPermissionDenied(t *testing.T) {
	seen := map[codes.Code]int{}
	record := func(err error) {
		seen[status.Code(err)]++
		if status.Code(err) == codes.PermissionDenied {
			t.Fatalf("a Scheduler method answered PermissionDenied; the gateway renders that as 502: %v", err)
		}
	}

	// Every failure shape reachable through LookupNode, which is the method
	// the incarnation work touched.
	for _, tc := range []struct {
		name   string
		store  BindingStore
		reader pausedregistry.Reader
		// silent skips the heartbeats: a binding store that fails every write
		// cannot be reconciled, and the failure this case is about is reached
		// on the first read anyway.
		silent bool
	}{
		{name: "binding store down", store: failingBindingStore{}, reader: nil, silent: true},
		{name: "registry unreadable", store: missingBindingStore{}, reader: &stubRegistryReader{err: errors.New("connection refused")}},
		{name: "registry empty", store: missingBindingStore{}, reader: &stubRegistryReader{ready: true}},
		{name: "registry cold", store: missingBindingStore{}, reader: &stubRegistryReader{}},
		{name: "unknown state", store: missingBindingStore{}, reader: registryReaderOver(
			pausedregistry.Sandbox{SandboxID: "sbx-1", State: "from-the-future", OriginNodeID: "node-a"})},
		{name: "origin not reporting", store: missingBindingStore{}, reader: registryReaderOver(
			pausedregistry.Sandbox{SandboxID: "sbx-1", State: pausedregistry.StateLocalOnly, OriginNodeID: "node-gone"})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc := newLookupTestService(t, tc.store, tc.reader, testReportTTL)
			if !tc.silent {
				allNodesReady(t, svc)
			}
			_, err := lookup(t, svc, "sbx-1")
			record(err)
		})
	}

	// And the other methods, in the shapes that fail.
	svc := newLookupTestService(t, missingBindingStore{}, nil, testReportTTL)
	_, err := svc.LookupNode(context.Background(), &schedulerv1.LookupNodeRequest{})
	record(err)
	_, err = svc.RecordAssignment(context.Background(), &schedulerv1.RecordAssignmentRequest{})
	record(err)
	_, err = svc.Heartbeat(context.Background(), &schedulerv1.HeartbeatRequest{})
	record(err)
	_, err = svc.Schedule(context.Background(), &schedulerv1.ScheduleRequest{})
	record(err)

	// 🟢 The control: this walk really did reach failing paths. Without it a
	// version of these calls that all succeeded would satisfy the assertion
	// above by never producing any code at all.
	if len(seen) < 2 {
		t.Fatalf("this test never reached more than one outcome, so it proves nothing about the code set: %v", seen)
	}
}

// TestArbitrationOffLeavesTheLookupAnswerUnchanged: the third leg of the
// rollback. Turning the arbitration off has to turn the *answer* off too — a
// caller acting on an incarnation this scheduler did not arbitrate is worse
// than one acting on none.
func TestArbitrationOffLeavesTheLookupAnswerUnchanged(t *testing.T) {
	store := NewInMemoryBindingStoreWithModes(time.Minute, InMemoryArbitrationFor("off"), false)
	if err := store.Record("sbx-1", Binding{Node: lookupTestNodes[0], ExecutionID: lookupExecution}, time.Now()); err != nil {
		t.Fatalf("record: %v", err)
	}
	svc := NewService(
		zap.NewNop(),
		NewAtomicNodeRegistry(lookupTestNodes, defaultObservedReportTTL),
		NewStrategy("round_robin"),
		store,
		WithSilentExecutionAxis(),
	)

	resp, err := svc.LookupNode(context.Background(), &schedulerv1.LookupNodeRequest{SandboxId: "sbx-1"})
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if resp.GetExecutionId() != "" {
		t.Fatalf("execution_id: got %q, want empty in the rollback mode", resp.GetExecutionId())
	}
	if got := resp.GetExecutionAuthority(); got != schedulerv1.ExecutionAuthority_EXECUTION_AUTHORITY_UNSPECIFIED {
		t.Fatalf("authority: got %v, want UNSPECIFIED in the rollback mode", got)
	}
	// The routing answer itself is untouched.
	if got := resp.GetNode().GetNodeId(); got != lookupTestNodes[0].ID {
		t.Fatalf("the rollback mode changed where the sandbox routes: %q", got)
	}
}

// TestARosterEntryWithABadExecutionKeepsItsRoute: narrowing is counted, and
// the route survives.
//
// Dropping the entry outright would trade a usable route for the absence of an
// unfenced one, and the sandbox would answer nothing at all.
func TestARosterEntryWithABadExecutionKeepsItsRoute(t *testing.T) {
	store := NewInMemoryBindingStore(time.Minute)
	svc := newLookupTestService(t, store, forbiddenRegistryReader{t: t}, testReportTTL)

	before := rosterDroppedCount(t, "bad_uuid")
	heartbeatWithExecutions(t, svc, "node-a", RosterEntry{SandboxID: "sbx-1", ExecutionID: "not-a-uuid"})

	binding, ok, err := store.Get("sbx-1", time.Now())
	if err != nil || !ok {
		t.Fatalf("a roster entry with an unusable incarnation lost its route entirely: ok=%v err=%v", ok, err)
	}
	if binding.ExecutionID != "" {
		t.Fatalf("an unusable incarnation was stored anyway: %q", binding.ExecutionID)
	}
	if after := rosterDroppedCount(t, "bad_uuid"); after <= before {
		t.Fatalf("the incarnation was discarded without saying so: %v -> %v", before, after)
	}
}

// registryReaderOver is a reader holding exactly one row, for the tests that
// are about what a lookup makes of a particular row rather than about the
// reader.
func registryReaderOver(sandbox pausedregistry.Sandbox) *stubRegistryReader {
	if sandbox.ClusterID == "" {
		sandbox.ClusterID = "cluster-1"
	}
	if sandbox.UpdatedAt.IsZero() {
		sandbox.UpdatedAt = time.Now()
	}
	return &stubRegistryReader{listing: pausedregistry.Listing{
		Now:       time.Now(),
		Sandboxes: []pausedregistry.Sandbox{sandbox},
	}}
}

// rosterDroppedCount reads the counter that makes a silent narrowing visible.
func rosterDroppedCount(t *testing.T, reason string) float64 {
	t.Helper()
	return counterSeriesValue(t, schedulerHeartbeatRosterDropped,
		"agentenv_scheduler_heartbeat_roster_dropped_total", map[string]string{"reason": reason})
}

func counterSeriesValue(t *testing.T, collector prometheus.Collector, name string, want map[string]string) float64 {
	t.Helper()

	reg := prometheus.NewRegistry()
	reg.MustRegister(collector)
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather %s: %v", name, err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
	metric:
		for _, metric := range family.GetMetric() {
			labels := map[string]string{}
			for _, label := range metric.GetLabel() {
				labels[label.GetName()] = label.GetValue()
			}
			for key, value := range want {
				if labels[key] != value {
					continue metric
				}
			}
			return metric.GetCounter().GetValue()
		}
	}
	return 0
}

// TestBothRowToProtoConversionsCarryTheExecution — S7 on the Go side.
//
// 🔴 One field, added in four places, and missing it in any one of them is
// silent: the column simply reads as empty on whichever path uses the list that
// was missed. Two of the four are column lists in the registry package and are
// covered there; these are the other two — the conversions to the wire.
//
// The one that matters most is registryEntryToProto: it is the copy a node
// reads back off a granted claim and starts its VM under. Empty there does not
// degrade anything visibly, it makes every cross-node resume fail mark_running.
func TestBothRowToProtoConversionsCarryTheExecution(t *testing.T) {
	t.Run("registryEntryToProto", func(t *testing.T) {
		out := registryEntryToProto(pausedregistry.Entry{Sandbox: pausedregistry.Sandbox{
			SandboxID: "sbx-1", State: pausedregistry.StateResuming, ExecutionID: lookupExecution,
		}})
		if out.GetExecutionId() != lookupExecution {
			t.Fatalf("execution_id: got %q, want %q — a node reading a granted claim would start the VM under nothing",
				out.GetExecutionId(), lookupExecution)
		}
	})

	t.Run("registrySandboxToProto", func(t *testing.T) {
		out := registrySandboxToProto(pausedregistry.Sandbox{
			SandboxID: "sbx-1", State: pausedregistry.StateRunning, ExecutionID: lookupExecution,
		})
		if out.GetExecutionId() != lookupExecution {
			t.Fatalf("execution_id: got %q, want %q — the only endpoint that shows a row's incarnation would show none",
				out.GetExecutionId(), lookupExecution)
		}
	})
}
