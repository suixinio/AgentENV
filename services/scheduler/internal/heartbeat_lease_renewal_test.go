// Tests for the impure half of the heartbeat-driven lease renewal:
// renewParkedLeasesFromHeartbeats and its wiring into reconcileRegistryOnce.
// The eligibility rule itself — which rows are candidates at all — is pinned
// against fixed inputs in reconcile_test.go; this file is about what happens
// to that candidate list once one exists: the switch, the restart-grace gate,
// the write, and their metrics.
package scheduler

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	pausedregistry "agentenv/services/scheduler/internal/registry"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.uber.org/zap"
)

// fakeParkedLeaseRenewer records what it was asked to renew and answers what
// the test told it to, the same discipline registry_service_test.go's
// fakeStore follows: the point of these tests is the wiring, and the wiring's
// failure mode is a write that silently never happens.
type fakeParkedLeaseRenewer struct {
	mu          sync.Mutex
	calls       int
	lastCluster string
	lastHolders []pausedregistry.ParkedLeaseHolder
	renewed     uint64
	err         error
}

func (f *fakeParkedLeaseRenewer) RenewParkedLeases(_ context.Context, clusterID string, holders []pausedregistry.ParkedLeaseHolder) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.lastCluster = clusterID
	f.lastHolders = holders
	if f.err != nil {
		return 0, f.err
	}
	return f.renewed, nil
}

func (f *fakeParkedLeaseRenewer) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// fakeGraceGate stands in for the write surface's restart-grace window,
// without waiting out a real lease TTL — see registryGraceGate's own doc for
// why the interface exists at all.
type fakeGraceGate struct {
	err error
}

func (g fakeGraceGate) RequireServing() error { return g.err }

// oneEligibleRowReader builds a stub registry reader carrying exactly one
// publishing row that a fresh node-a roster vouches for — the minimal shape
// renewParkedLeasesFromHeartbeats has anything to act on.
func oneEligibleRowReader(now time.Time) *stubRegistryReader {
	return &stubRegistryReader{
		clusterID: "cluster-a",
		listing: pausedregistry.Listing{
			Now: now,
			Sandboxes: []pausedregistry.Sandbox{
				{SandboxID: "s1", State: pausedregistry.StatePublishing, OriginNodeID: "node-a", SnapshotID: "snap", LeaseExpiresAt: at(now, time.Hour)},
			},
		},
	}
}

func serviceWithHeartbeatRenewal(reader *stubRegistryReader, nodes NodeRegistry, writer pausedregistry.ParkedLeaseRenewer, grace registryGraceGate, enabled bool) *Service {
	return NewService(
		zap.NewNop(),
		nodes,
		NewStrategy("round_robin"),
		NewInMemoryBindingStore(time.Minute),
		WithPausedRegistry(reader, testReportTTL, testReportTTL),
		WithHeartbeatLeaseRenewal(writer, grace, enabled),
	)
}

func nodeARoster(t *testing.T, now time.Time) NodeRegistry {
	t.Helper()
	nodes := NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, defaultObservedReportTTL)
	heartbeatWithClusterRoster(t, nodes, "node-a", "cluster-a", now, "s1")
	return nodes
}

// TestHeartbeatLeaseRenewalWritesWhenEnabledAndServing is the positive case
// every other test in this file contrasts against.
func TestHeartbeatLeaseRenewalWritesWhenEnabledAndServing(t *testing.T) {
	now := time.Now()
	reader := oneEligibleRowReader(now)
	writer := &fakeParkedLeaseRenewer{renewed: 1}
	svc := serviceWithHeartbeatRenewal(reader, nodeARoster(t, now), writer, fakeGraceGate{}, true)

	renewedBefore := testutil.ToFloat64(schedulerRegistryHeartbeatLeaseRenewed)

	if stop := svc.reconcileRegistryOnce(context.Background()); stop {
		t.Fatal("expected the loop to keep running")
	}

	if got := writer.callCount(); got != 1 {
		t.Fatalf("expected the writer to be called once, got %d", got)
	}
	if writer.lastCluster != "cluster-a" {
		t.Fatalf("expected the reader's own cluster scope, got %q", writer.lastCluster)
	}
	if !hasParkedLeaseRenewal(writer.lastHolders, "s1", "node-a") {
		t.Fatalf("expected s1/node-a to have been asked for, got %+v", writer.lastHolders)
	}
	if got := testutil.ToFloat64(schedulerRegistryHeartbeatLeaseRenewed); got != renewedBefore+1 {
		t.Fatalf("expected the renewed counter to advance by 1, went from %v to %v", renewedBefore, got)
	}
}

// TestHeartbeatLeaseRenewalSkippedWhenSwitchOff is the control for the switch
// itself: same candidate, same writer, same open grace — only `enabled`
// differs, and it alone must be what keeps the writer silent.
//
// This is the shape of every real deployment today: a write surface exists
// (registryWriter != nil) but scheduler.registry.heartbeat_lease_renewal has
// not been turned on.
func TestHeartbeatLeaseRenewalSkippedWhenSwitchOff(t *testing.T) {
	now := time.Now()
	reader := oneEligibleRowReader(now)
	writer := &fakeParkedLeaseRenewer{renewed: 1}
	svc := serviceWithHeartbeatRenewal(reader, nodeARoster(t, now), writer, fakeGraceGate{}, false)

	if stop := svc.reconcileRegistryOnce(context.Background()); stop {
		t.Fatal("expected the loop to keep running")
	}
	if got := writer.callCount(); got != 0 {
		t.Fatalf("expected the writer never to be called with the switch off, got %d calls", got)
	}
}

// TestHeartbeatLeaseRenewalNeverWiredIsANoOp covers the other real shape: no
// write surface at all (registryWriter stays nil because
// WithHeartbeatLeaseRenewal is never called), which is every query-only
// replica and every cluster with scheduler.registry.write_enabled=false.
//
// The candidates gauge is still asserted on, so this is not merely "did not
// panic": observability must not depend on the write path being wired at
// all.
func TestHeartbeatLeaseRenewalNeverWiredIsANoOp(t *testing.T) {
	now := time.Now()
	reader := oneEligibleRowReader(now)
	svc := NewService(
		zap.NewNop(),
		nodeARoster(t, now),
		NewStrategy("round_robin"),
		NewInMemoryBindingStore(time.Minute),
		WithPausedRegistry(reader, testReportTTL, testReportTTL),
	)

	if stop := svc.reconcileRegistryOnce(context.Background()); stop {
		t.Fatal("expected the loop to keep running")
	}
	if got := testutil.ToFloat64(schedulerRegistryParkedLeaseRenewalCandidates); got != 1 {
		t.Fatalf("expected the candidate gauge to still report 1, got %v", got)
	}
}

// TestHeartbeatLeaseRenewalSkipsDuringTheGraceWindow is the control for the
// restart-grace gate, contrasted against
// TestHeartbeatLeaseRenewalWritesWhenEnabledAndServing: everything else is
// identical, only the gate's answer differs.
func TestHeartbeatLeaseRenewalSkipsDuringTheGraceWindow(t *testing.T) {
	now := time.Now()
	reader := oneEligibleRowReader(now)
	writer := &fakeParkedLeaseRenewer{renewed: 1}
	svc := serviceWithHeartbeatRenewal(reader, nodeARoster(t, now), writer, fakeGraceGate{err: pausedregistry.ErrGracePeriod}, true)

	if stop := svc.reconcileRegistryOnce(context.Background()); stop {
		t.Fatal("expected the loop to keep running")
	}
	if got := writer.callCount(); got != 0 {
		t.Fatalf("expected the writer never to be called during the grace window, got %d calls", got)
	}
}

// TestHeartbeatLeaseRenewalSkipsDuringTheRealGracesRestartWindow is the same
// gate, but through the concrete *pausedregistry.Grace this process actually
// uses in production — proving registryGraceGate's structural contract holds
// for its one real implementation, not only for the fake above.
func TestHeartbeatLeaseRenewalSkipsDuringTheRealGracesRestartWindow(t *testing.T) {
	now := time.Now()
	reader := oneEligibleRowReader(now)
	writer := &fakeParkedLeaseRenewer{renewed: 1}

	// A window long enough that this test cannot outlast it by accident.
	grace := pausedregistry.NewGrace(time.Hour, zap.NewNop())
	if _, err := grace.Enter(context.Background(), openLeaseExtenderStub{}, "cluster-a"); err != nil {
		t.Fatalf("enter the restart grace pass: %v", err)
	}

	svc := serviceWithHeartbeatRenewal(reader, nodeARoster(t, now), writer, grace, true)

	if stop := svc.reconcileRegistryOnce(context.Background()); stop {
		t.Fatal("expected the loop to keep running")
	}
	if got := writer.callCount(); got != 0 {
		t.Fatalf("expected the writer never to be called while the real grace is still open, got %d calls", got)
	}
}

// openLeaseExtenderStub answers Grace.Enter without a database, mirroring
// registry_service_test.go's openExtender.
type openLeaseExtenderStub struct{}

func (openLeaseExtenderStub) ExtendLeases(context.Context, string, time.Duration) (time.Duration, int64, error) {
	return time.Minute, 0, nil
}

// TestHeartbeatLeaseRenewalFailureIsCountedAndLoopContinues is the failure
// path's control against the success path above: the write was attempted —
// the writer was called — but errored, and that must show up as a failure
// counted separately from a successful renewal rather than as a round that
// silently rewound nothing.
func TestHeartbeatLeaseRenewalFailureIsCountedAndLoopContinues(t *testing.T) {
	now := time.Now()
	reader := oneEligibleRowReader(now)
	writer := &fakeParkedLeaseRenewer{err: errors.New("connection refused")}
	svc := serviceWithHeartbeatRenewal(reader, nodeARoster(t, now), writer, fakeGraceGate{}, true)

	renewedBefore := testutil.ToFloat64(schedulerRegistryHeartbeatLeaseRenewed)
	failuresBefore := testutil.ToFloat64(schedulerRegistryHeartbeatLeaseRenewalFailures)

	if stop := svc.reconcileRegistryOnce(context.Background()); stop {
		t.Fatal("a failed renewal must not stop the reconcile loop")
	}

	if got := writer.callCount(); got != 1 {
		t.Fatalf("expected the write to have been attempted, got %d calls", got)
	}
	if got := testutil.ToFloat64(schedulerRegistryHeartbeatLeaseRenewalFailures); got != failuresBefore+1 {
		t.Fatalf("expected the failure counter to advance by 1, went from %v to %v", failuresBefore, got)
	}
	if got := testutil.ToFloat64(schedulerRegistryHeartbeatLeaseRenewed); got != renewedBefore {
		t.Fatalf("expected the renewed counter to stay put on a failed write, went from %v to %v", renewedBefore, got)
	}
}

// TestHeartbeatLeaseRenewalCandidatesGaugeTracksTheRoundNotTheSwitch proves
// the candidates gauge is a fact about the round rather than a shadow of the
// switch: it moves from a populated round to an empty one within the same
// test, so "the gauge reports 0" cannot be mistaken for "the gauge was never
// wired to anything".
func TestHeartbeatLeaseRenewalCandidatesGaugeTracksTheRoundNotTheSwitch(t *testing.T) {
	now := time.Now()

	populated := oneEligibleRowReader(now)
	writer := &fakeParkedLeaseRenewer{renewed: 1}
	svc := serviceWithHeartbeatRenewal(populated, nodeARoster(t, now), writer, fakeGraceGate{}, false)
	svc.reconcileRegistryOnce(context.Background())
	if got := testutil.ToFloat64(schedulerRegistryParkedLeaseRenewalCandidates); got != 1 {
		t.Fatalf("expected 1 candidate with an eligible row present, got %v", got)
	}

	empty := &stubRegistryReader{clusterID: "cluster-a", listing: pausedregistry.Listing{Now: now}}
	svc2 := serviceWithHeartbeatRenewal(empty, nodeARoster(t, now), writer, fakeGraceGate{}, false)
	svc2.reconcileRegistryOnce(context.Background())
	if got := testutil.ToFloat64(schedulerRegistryParkedLeaseRenewalCandidates); got != 0 {
		t.Fatalf("expected 0 candidates with no rows at all, got %v", got)
	}
}
