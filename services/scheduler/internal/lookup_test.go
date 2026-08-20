package scheduler

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"
	pausedregistry "agentenv/services/scheduler/internal/registry"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// missingBindingStore always misses. Every test below is about what happens
// after the binding does, and a store that answers would short-circuit all of
// them.
type missingBindingStore struct{}

func (missingBindingStore) Get(string, time.Time) (Binding, bool, error) {
	return Binding{}, false, nil
}

func (missingBindingStore) Record(string, Binding, time.Time) error { return nil }

func (missingBindingStore) ReconcileNode(Node, []RosterEntry, time.Time) error { return nil }

// forbiddenRegistryReader fails the test on any read. It pins the two answers
// that must be reachable without a database round trip: a binding hit, which
// every proxied request goes through, and a roster hit, which covers the window
// a heartbeat is late for. A counter checked afterwards would prove the same
// thing, but this way the failure names the call rather than a number.
type forbiddenRegistryReader struct{ t *testing.T }

func (r forbiddenRegistryReader) Get(context.Context, string) (pausedregistry.Sandbox, bool, error) {
	r.t.Fatal("the registry was read on a path that must not touch the database")
	return pausedregistry.Sandbox{}, false, nil
}

func (r forbiddenRegistryReader) List(context.Context) (pausedregistry.Listing, error) {
	r.t.Fatal("the registry was listed on a path that must not touch the database")
	return pausedregistry.Listing{}, nil
}

func (forbiddenRegistryReader) Ready() bool { return true }

func (forbiddenRegistryReader) ClusterID() string { return "" }

func (forbiddenRegistryReader) Close() {}

var lookupTestNodes = []Node{
	{ID: "node-a", Endpoint: "http://node-a"},
	{ID: "node-b", Endpoint: "http://node-b"},
}

// newLookupTestService wires a scheduler with the two nodes above. reportTTL is
// the roster freshness window; pass a tiny one to model rosters nobody has
// refreshed.
func newLookupTestService(t *testing.T, store BindingStore, reader pausedregistry.Reader, reportTTL time.Duration) *Service {
	t.Helper()

	opts := []ServiceOption{}
	if reader != nil {
		opts = append(opts, WithPausedRegistry(reader, reportTTL, testReportTTL))
	}
	return NewService(
		zap.NewNop(),
		NewAtomicNodeRegistry(lookupTestNodes, defaultObservedReportTTL),
		NewStrategy("round_robin"),
		store,
		opts...,
	)
}

// lookupHeartbeat makes a node report in with a status and a roster. It is also
// what takes the scheduler out of warm-up, so tests that want a cold scheduler
// simply do not call it.
func lookupHeartbeat(t *testing.T, svc *Service, nodeID string, nodeStatus schedulerv1.NodeStatus, sandboxIDs ...string) {
	t.Helper()

	_, err := svc.Heartbeat(context.Background(), &schedulerv1.HeartbeatRequest{
		NodeId:            nodeID,
		ClusterId:         "cluster-1",
		ServiceInstanceId: nodeID + "-1",
		Snapshot:          &schedulerv1.NodeSnapshot{Status: nodeStatus},
		SandboxIds:        sandboxIDs,
	})
	if err != nil {
		t.Fatalf("heartbeat for %s failed: %v", nodeID, err)
	}
}

// allNodesReady reports every node in with no sandboxes, which is the ordinary
// steady state: warm scheduler, fresh rosters, nothing held.
func allNodesReady(t *testing.T, svc *Service) {
	t.Helper()

	for _, node := range lookupTestNodes {
		lookupHeartbeat(t, svc, node.ID, schedulerv1.NodeStatus_NODE_STATUS_READY)
	}
}

// lookupResultCounts reads the lookup result counter back through a registry of
// this test's own. It is how a test sees which branch answered: the two
// refusals below share a gRPC code and a response shape, and differ only in
// this label and in the log line.
func lookupResultCounts(t *testing.T) map[string]float64 {
	t.Helper()

	reg := prometheus.NewRegistry()
	reg.MustRegister(schedulerLookupResults)
	return gaugeSeries(t, reg, "agentenv_scheduler_lookup_node_total")
}

func registryRow(sandboxID string, state pausedregistry.State, originNodeID string, claimedByNodeID string) pausedregistry.Listing {
	return pausedregistry.Listing{
		Now: time.Now(),
		Sandboxes: []pausedregistry.Sandbox{{
			SandboxID:       sandboxID,
			ClusterID:       "cluster-1",
			State:           state,
			Generation:      1,
			OriginNodeID:    originNodeID,
			ClaimedByNodeID: claimedByNodeID,
			SnapshotID:      "snap-1",
			UpdatedAt:       time.Now(),
		}},
	}
}

func lookup(t *testing.T, svc *Service, sandboxID string) (*schedulerv1.LookupNodeResponse, error) {
	t.Helper()

	return svc.LookupNode(context.Background(), &schedulerv1.LookupNodeRequest{SandboxId: sandboxID})
}

func requireLookupNode(t *testing.T, resp *schedulerv1.LookupNodeResponse, err error, nodeID string, location schedulerv1.SandboxLocation) {
	t.Helper()

	if err != nil {
		t.Fatalf("lookup failed: %v", err)
	}
	if got := resp.GetNode().GetNodeId(); got != nodeID {
		t.Fatalf("expected node %q, got %q", nodeID, got)
	}
	if got := resp.GetLocation(); got != location {
		t.Fatalf("expected location %s, got %s", location, got)
	}
}

// A binding is the hot path: every proxied request goes through it, so a hit
// must not reach the registry at all.
func TestLookupBindingHitNeverReadsTheRegistry(t *testing.T) {
	store := NewInMemoryBindingStore(time.Minute)
	svc := newLookupTestService(t, store, forbiddenRegistryReader{t: t}, testReportTTL)
	allNodesReady(t, svc)
	if err := store.Record("sbx-1", Binding{Node: lookupTestNodes[0]}, time.Now()); err != nil {
		t.Fatalf("record binding failed: %v", err)
	}

	resp, err := lookup(t, svc, "sbx-1")
	requireLookupNode(t, resp, err, "node-a", schedulerv1.SandboxLocation_SANDBOX_LOCATION_BOUND)
}

// §3.5: binding miss + roster hit ⇒ BOUND, and the database is not touched.
//
// A binding expires on its own TTL while the roster that seeded it stays as the
// node last reported it, so this is the window a late heartbeat opens. Reading
// the registry here would put the hot path's worst case on a database round
// trip for a question a node already answered.
func TestLookupFallsBackToTheRosterWithoutReadingTheRegistry(t *testing.T) {
	svc := newLookupTestService(t, missingBindingStore{}, forbiddenRegistryReader{t: t}, testReportTTL)
	lookupHeartbeat(t, svc, "node-a", schedulerv1.NodeStatus_NODE_STATUS_READY, "sbx-1")
	lookupHeartbeat(t, svc, "node-b", schedulerv1.NodeStatus_NODE_STATUS_READY)

	resp, err := lookup(t, svc, "sbx-1")
	// The roster names node-a. A registry read here would not only cost a
	// database round trip on the hot path, it would answer node-b — the origin
	// of a row the roster has already superseded.
	requireLookupNode(t, resp, err, "node-a", schedulerv1.SandboxLocation_SANDBOX_LOCATION_BOUND)
}

// A roster older than the report TTL is not evidence of anything: the node may
// have gone away with the sandbox on it. The lookup has to fall through to the
// registry rather than route to a node nobody has heard from.
func TestLookupIgnoresAStaleRoster(t *testing.T) {
	reader := &stubRegistryReader{listing: registryRow("sbx-1", pausedregistry.StatePaused, "node-b", "")}
	svc := newLookupTestService(t, missingBindingStore{}, reader, time.Nanosecond)
	lookupHeartbeat(t, svc, "node-a", schedulerv1.NodeStatus_NODE_STATUS_READY, "sbx-1")
	lookupHeartbeat(t, svc, "node-b", schedulerv1.NodeStatus_NODE_STATUS_READY)

	resp, err := lookup(t, svc, "sbx-1")
	requireLookupNode(t, resp, err, "node-b", schedulerv1.SandboxLocation_SANDBOX_LOCATION_PLACED)
	if reader.gets != 1 {
		t.Fatalf("expected exactly one registry read, got %d", reader.gets)
	}
}

// §3.5: paused + origin able to take work ⇒ the origin is chosen.
//
// 🔴 This is the affinity regression guard. Rebuilding on the machine that
// already has the layers is what keeps a cross-node resume from pulling the
// whole image back out of object storage. The loop matters: with two eligible
// nodes and no preference, round robin would alternate, so three answers in a
// row naming the origin cannot be luck.
func TestLookupPrefersTheOriginForAPausedSandbox(t *testing.T) {
	reader := &stubRegistryReader{listing: registryRow("sbx-1", pausedregistry.StatePaused, "node-a", "")}
	svc := newLookupTestService(t, missingBindingStore{}, reader, testReportTTL)
	allNodesReady(t, svc)

	for attempt := 0; attempt < 3; attempt++ {
		resp, err := lookup(t, svc, "sbx-1")
		requireLookupNode(t, resp, err, "node-a", schedulerv1.SandboxLocation_SANDBOX_LOCATION_PLACED)
		if got := resp.GetOriginNodeId(); got != "node-a" {
			t.Fatalf("expected the origin to be reported, got %q", got)
		}
	}
}

// §3.5: paused + origin isolated ⇒ some other node is chosen.
//
// The preference is soft precisely here: the snapshot is published, so any node
// can rebuild the sandbox, and insisting on a node that refuses new work would
// turn a slower resume into a failed one.
func TestLookupPlacesAPausedSandboxAwayFromADrainingOrigin(t *testing.T) {
	reader := &stubRegistryReader{listing: registryRow("sbx-1", pausedregistry.StatePaused, "node-a", "")}
	svc := newLookupTestService(t, missingBindingStore{}, reader, testReportTTL)
	lookupHeartbeat(t, svc, "node-a", schedulerv1.NodeStatus_NODE_STATUS_DRAINING)
	lookupHeartbeat(t, svc, "node-b", schedulerv1.NodeStatus_NODE_STATUS_READY)

	resp, err := lookup(t, svc, "sbx-1")
	requireLookupNode(t, resp, err, "node-b", schedulerv1.SandboxLocation_SANDBOX_LOCATION_PLACED)
	if got := resp.GetOriginNodeId(); got != "node-a" {
		t.Fatalf("expected the origin to be reported even when it was not chosen, got %q", got)
	}
}

// A paused sandbox with nowhere at all to go is Unavailable — retryable, and
// emphatically not "no such sandbox".
func TestLookupPausedWithNoEligibleNodeIsUnavailable(t *testing.T) {
	reader := &stubRegistryReader{listing: registryRow("sbx-1", pausedregistry.StatePaused, "node-a", "")}
	svc := newLookupTestService(t, missingBindingStore{}, reader, testReportTTL)
	lookupHeartbeat(t, svc, "node-a", schedulerv1.NodeStatus_NODE_STATUS_DRAINING)
	lookupHeartbeat(t, svc, "node-b", schedulerv1.NodeStatus_NODE_STATUS_DRAINING)

	_, err := lookup(t, svc, "sbx-1")
	requireCode(t, err, codes.Unavailable)
}

// publishing and local_only both mean the only copy of the sandbox is on the
// origin's disk, so the answer is that node and no other.
func TestLookupPinsAParkedSandboxToItsOrigin(t *testing.T) {
	for _, state := range []pausedregistry.State{pausedregistry.StatePublishing, pausedregistry.StateLocalOnly} {
		t.Run(string(state), func(t *testing.T) {
			reader := &stubRegistryReader{listing: registryRow("sbx-1", state, "node-a", "")}
			svc := newLookupTestService(t, missingBindingStore{}, reader, testReportTTL)
			allNodesReady(t, svc)

			resp, err := lookup(t, svc, "sbx-1")
			requireLookupNode(t, resp, err, "node-a", schedulerv1.SandboxLocation_SANDBOX_LOCATION_PINNED)
			if got := resp.GetNode().GetEndpoint(); got != "http://node-a" {
				t.Fatalf("expected the origin's endpoint, got %q", got)
			}
		})
	}
}

// §3.5: local_only + origin draining ⇒ FailedPrecondition, and no other node is
// named.
//
// 🔴 The origin is checked here rather than left to answer for itself. An
// isolated node replies to a resume it will not serve with a 503 asking for the
// request to be sent elsewhere — and for these two states there is nowhere
// else, since no snapshot ever reached shared storage. Forwarding would earn
// the caller a 503 whose stated reason is the opposite of the truth, so the
// refusal is made here where the reason can be said correctly.
//
// 🔴 The two reasons are reported apart. They share a gRPC code because the
// caller does the same thing either way — retry — but an operator does not:
// "not accepting work" is a node that said so and is answered at the admin API,
// while "not reporting" is a node that said nothing and is answered at its
// heartbeat. Reporting the second as the first sends whoever is on the incident
// to look at a subsystem that is behaving perfectly, and the cluster run of
// this change did exactly that seven times.
func TestLookupRefusesToPinToANodeThatWillNotServe(t *testing.T) {
	for _, tc := range []struct {
		name        string
		register    func(*testing.T, *Service)
		wantResult  lookupResult
		wantMessage string
		otherResult lookupResult
	}{
		{
			name: "origin is draining",
			register: func(t *testing.T, svc *Service) {
				lookupHeartbeat(t, svc, "node-a", schedulerv1.NodeStatus_NODE_STATUS_DRAINING)
				lookupHeartbeat(t, svc, "node-b", schedulerv1.NodeStatus_NODE_STATUS_READY)
			},
			wantResult:  lookupResultOriginUnschedulable,
			wantMessage: "which is not accepting work",
			otherResult: lookupResultOriginNotReporting,
		},
		{
			// A node nobody has heard from is not somewhere to send a sandbox
			// whose only copy is on its disk, however healthy it last looked.
			name: "origin is not reporting",
			register: func(t *testing.T, svc *Service) {
				lookupHeartbeat(t, svc, "node-b", schedulerv1.NodeStatus_NODE_STATUS_READY)
			},
			wantResult:  lookupResultOriginNotReporting,
			wantMessage: "which is not reporting",
			otherResult: lookupResultOriginUnschedulable,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader := &stubRegistryReader{listing: registryRow("sbx-1", pausedregistry.StateLocalOnly, "node-a", "")}
			svc := newLookupTestService(t, missingBindingStore{}, reader, testReportTTL)
			tc.register(t, svc)

			before := lookupResultCounts(t)
			resp, err := lookup(t, svc, "sbx-1")
			if resp != nil {
				t.Fatalf("expected no node, got %q", resp.GetNode().GetNodeId())
			}
			requireCode(t, err, codes.FailedPrecondition)

			if got := status.Convert(err).Message(); !strings.Contains(got, tc.wantMessage) {
				t.Fatalf("expected the message to say %q, got %q", tc.wantMessage, got)
			}

			after := lookupResultCounts(t)
			if got := after[string(tc.wantResult)] - before[string(tc.wantResult)]; got != 1 {
				t.Fatalf("expected one %s, got %v", tc.wantResult, got)
			}
			// And the other reason did not move. Collapsing the two back into
			// one label is exactly what this pins.
			if got := after[string(tc.otherResult)] - before[string(tc.otherResult)]; got != 0 {
				t.Fatalf("expected %s not to be counted, got %v", tc.otherResult, got)
			}
		})
	}
}

// A registry row names the node under the identity that node reported itself
// with, which during a fleet upgrade is its pod name. Resolving it the same way
// every other node identity is resolved keeps a pin from failing on a node that
// is right there.
func TestLookupPinResolvesAnOriginRecordedUnderItsPodName(t *testing.T) {
	reader := &stubRegistryReader{listing: registryRow("sbx-1", pausedregistry.StateLocalOnly, "pod-a", "")}
	registry := NewAtomicNodeRegistry(nil, defaultObservedReportTTL)
	registry.Set([]Node{{ID: "node-a", Endpoint: "http://node-a", PodName: "pod-a"}}, nil)
	svc := NewService(zap.NewNop(), registry, NewStrategy("round_robin"), missingBindingStore{},
		WithPausedRegistry(reader, testReportTTL, testReportTTL))
	lookupHeartbeat(t, svc, "node-a", schedulerv1.NodeStatus_NODE_STATUS_READY)

	resp, err := lookup(t, svc, "sbx-1")
	requireLookupNode(t, resp, err, "node-a", schedulerv1.SandboxLocation_SANDBOX_LOCATION_PINNED)
}

// §3.5: a resuming row is routed to its claimer, never to its origin.
//
// 🔴 A claim deliberately leaves origin_node_id pointing at whoever still holds
// the local artifacts, so reading origin here would send the caller to the node
// the sandbox is moving away from.
func TestLookupRoutesAResumingSandboxToItsClaimer(t *testing.T) {
	reader := &stubRegistryReader{listing: registryRow("sbx-1", pausedregistry.StateResuming, "node-a", "node-b")}
	svc := newLookupTestService(t, missingBindingStore{}, reader, testReportTTL)
	allNodesReady(t, svc)

	resp, err := lookup(t, svc, "sbx-1")
	requireLookupNode(t, resp, err, "node-b", schedulerv1.SandboxLocation_SANDBOX_LOCATION_BOUND)
	if got := resp.GetOriginNodeId(); got != "node-a" {
		t.Fatalf("expected the origin to be reported alongside the claimer, got %q", got)
	}
}

// The control for the test above: a running row has no claimer, so its origin
// is its holder.
func TestLookupRoutesARunningSandboxToItsOrigin(t *testing.T) {
	reader := &stubRegistryReader{listing: registryRow("sbx-1", pausedregistry.StateRunning, "node-a", "")}
	svc := newLookupTestService(t, missingBindingStore{}, reader, testReportTTL)
	allNodesReady(t, svc)

	resp, err := lookup(t, svc, "sbx-1")
	requireLookupNode(t, resp, err, "node-a", schedulerv1.SandboxLocation_SANDBOX_LOCATION_BOUND)
}

// A live sandbox on a node that stopped reporting cannot be routed to, and is
// certainly not absent. FailedPrecondition says which node is at fault.
func TestLookupLiveSandboxOnASilentNodeIsFailedPrecondition(t *testing.T) {
	reader := &stubRegistryReader{listing: registryRow("sbx-1", pausedregistry.StateRunning, "node-c", "")}
	svc := newLookupTestService(t, missingBindingStore{}, reader, testReportTTL)
	allNodesReady(t, svc)

	_, err := lookup(t, svc, "sbx-1")
	requireCode(t, err, codes.FailedPrecondition)
}

// The same row on the same silent node, but before any node has reported at
// all, is not a FailedPrecondition — it is a scheduler that has been told
// nothing yet.
//
// 🔴 The distinction is the whole point of the warm-up gate. FailedPrecondition
// says "the node holding this is not reporting", which a scheduler that has
// received no heartbeats cannot possibly know; Unavailable says "ask again",
// which is the truth. Getting this backwards fails every resume in the first
// seconds after a scheduler restart, and blames the nodes for it.
func TestLookupWithholdsALiveSandboxJudgementWhileBindingsAreCold(t *testing.T) {
	reader := &stubRegistryReader{listing: registryRow("sbx-1", pausedregistry.StateRunning, "node-a", "")}
	// No heartbeats: both nodes are known from discovery and neither has
	// reported, which is exactly the state a freshly started scheduler is in.
	svc := newLookupTestService(t, missingBindingStore{}, reader, testReportTTL)

	_, err := lookup(t, svc, "sbx-1")
	requireCode(t, err, codes.Unavailable)
}

// A state the table's CHECK constraint does not allow today is a row written by
// a newer build. Guessing which of the five known states it resembles is how a
// live sandbox gets rebuilt somewhere it already exists.
func TestLookupRefusesAnUnrecognisedRegistryState(t *testing.T) {
	reader := &stubRegistryReader{listing: registryRow("sbx-1", pausedregistry.State("hibernating"), "node-a", "")}
	svc := newLookupTestService(t, missingBindingStore{}, reader, testReportTTL)
	allNodesReady(t, svc)

	_, err := lookup(t, svc, "sbx-1")
	requireCode(t, err, codes.FailedPrecondition)
}

// 🔴 §3.5, the most important case: an unreadable or unread registry answers
// Unavailable. NotFound here would be the scheduler asserting a sandbox does
// not exist on the strength of a question it never got to ask.
func TestLookupNeverTurnsAnUnreadableRegistryIntoNotFound(t *testing.T) {
	for _, tc := range []struct {
		name   string
		reader *stubRegistryReader
	}{
		{
			name:   "the read failed",
			reader: &stubRegistryReader{err: errors.New("connection refused")},
		},
		{
			// A reader that has never completed a read has no idea what the
			// table holds, so its "no row" is not an observation.
			name:   "the reader has never read",
			reader: &stubRegistryReader{neverReady: true},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc := newLookupTestService(t, missingBindingStore{}, tc.reader, testReportTTL)
			allNodesReady(t, svc)

			_, err := lookup(t, svc, "sbx-1")
			requireCode(t, err, codes.Unavailable)
		})
	}
}

// With the registry readable and holding nothing, NotFound is finally the
// honest answer — and it is the only path that produces one.
func TestLookupAnswersNotFoundWhenTheRegistryIsReadableAndEmpty(t *testing.T) {
	reader := &stubRegistryReader{listing: pausedregistry.Listing{Now: time.Now()}}
	svc := newLookupTestService(t, missingBindingStore{}, reader, testReportTTL)
	allNodesReady(t, svc)

	_, err := lookup(t, svc, "sbx-1")
	requireCode(t, err, codes.NotFound)
	if reader.gets != 1 {
		t.Fatalf("expected exactly one registry read, got %d", reader.gets)
	}
}

// A scheduler that was never pointed at a registry behaves exactly as it did
// before there was one. Switching the feature off has to be a real way back.
func TestLookupWithoutARegistryKeepsTheOldAnswer(t *testing.T) {
	svc := newLookupTestService(t, missingBindingStore{}, nil, testReportTTL)
	allNodesReady(t, svc)

	_, err := lookup(t, svc, "sbx-1")
	requireCode(t, err, codes.NotFound)
}

// The warm-up gate still stands in front of every absence. A scheduler that has
// been told nothing yet withholds NotFound, because a sandbox that was never
// paused has no registry row by design and can only be found through a binding
// or a roster — neither of which exists yet.
func TestLookupWithholdsNotFoundWhileBindingsAreCold(t *testing.T) {
	for _, tc := range []struct {
		name   string
		reader pausedregistry.Reader
	}{
		{name: "no registry", reader: nil},
		{name: "empty registry", reader: &stubRegistryReader{listing: pausedregistry.Listing{Now: time.Now()}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// No heartbeats: nodes are known from discovery and none has
			// reported, which is the coldest state there is.
			svc := newLookupTestService(t, missingBindingStore{}, tc.reader, testReportTTL)

			_, err := lookup(t, svc, "sbx-1")
			requireCode(t, err, codes.Unavailable)
		})
	}
}

func TestLookupRejectsABlankSandboxID(t *testing.T) {
	svc := newLookupTestService(t, missingBindingStore{}, nil, testReportTTL)

	_, err := lookup(t, svc, "   ")
	requireCode(t, err, codes.InvalidArgument)
}

func TestLookupReportsAnUnreadableBindingStore(t *testing.T) {
	svc := newLookupTestService(t, failingBindingStore{}, nil, testReportTTL)

	_, err := lookup(t, svc, "sbx-1")
	requireCode(t, err, codes.Unavailable)
}

// 🔴 The query-only replica runs the same ladder. It has no discovery and no
// heartbeats, so it cannot place anything — but the answer that matters most,
// "the registry could not be read, so this is not a 404", needs nothing but the
// reader, and a gateway configured with query_only_scheduler_addr sends every
// sandbox lookup here.
func TestQueryOnlyLookupRunsTheSameLadder(t *testing.T) {
	ctx := context.Background()
	req := &schedulerv1.LookupNodeRequest{SandboxId: "sbx-1"}

	t.Run("binding hit", func(t *testing.T) {
		store := NewInMemoryBindingStore(time.Minute)
		if err := store.Record("sbx-1", Binding{Node: lookupTestNodes[0]}, time.Now()); err != nil {
			t.Fatalf("record binding failed: %v", err)
		}
		reader := &stubRegistryReader{listing: registryRow("sbx-1", pausedregistry.StatePaused, "node-a", "")}
		svc := NewQueryOnlyService(zap.NewNop(), store, WithQueryOnlyPausedRegistry(reader))

		resp, err := svc.LookupNode(ctx, req)
		if err != nil {
			t.Fatalf("lookup failed: %v", err)
		}
		if resp.GetNode().GetNodeId() != "node-a" || resp.GetLocation() != schedulerv1.SandboxLocation_SANDBOX_LOCATION_BOUND {
			t.Fatalf("unexpected answer: %v", resp)
		}
		if reader.gets != 0 {
			t.Fatalf("the registry was read %d times on a binding hit", reader.gets)
		}
	})

	t.Run("unreadable registry is unavailable", func(t *testing.T) {
		svc := NewQueryOnlyService(zap.NewNop(), missingBindingStore{},
			WithQueryOnlyPausedRegistry(&stubRegistryReader{err: errors.New("connection refused")}))

		_, err := svc.LookupNode(ctx, req)
		requireCode(t, err, codes.Unavailable)
	})

	t.Run("empty registry is not found", func(t *testing.T) {
		svc := NewQueryOnlyService(zap.NewNop(), missingBindingStore{},
			WithQueryOnlyPausedRegistry(&stubRegistryReader{listing: pausedregistry.Listing{Now: time.Now()}}))

		_, err := svc.LookupNode(ctx, req)
		requireCode(t, err, codes.NotFound)
	})

	t.Run("a registered sandbox it cannot place is unavailable", func(t *testing.T) {
		// Not NotFound: the row proves the sandbox exists. This replica just
		// cannot say which node should serve it.
		svc := NewQueryOnlyService(zap.NewNop(), missingBindingStore{},
			WithQueryOnlyPausedRegistry(&stubRegistryReader{
				listing: registryRow("sbx-1", pausedregistry.StatePaused, "node-a", ""),
			}))

		_, err := svc.LookupNode(ctx, req)
		requireCode(t, err, codes.Unavailable)
	})

	t.Run("without a registry it keeps the old answer", func(t *testing.T) {
		svc := NewQueryOnlyService(zap.NewNop(), missingBindingStore{})

		_, err := svc.LookupNode(ctx, req)
		requireCode(t, err, codes.NotFound)
	})
}

// Schedule is for a sandbox that does not exist yet, so it has no node it would
// rather be on. Sharing the placement pipeline with the lookup must not give it
// one.
func TestScheduleStillRoundRobinsWithoutAPreference(t *testing.T) {
	svc := newLookupTestService(t, missingBindingStore{}, nil, testReportTTL)
	allNodesReady(t, svc)

	seen := map[string]int{}
	for attempt := 0; attempt < 4; attempt++ {
		resp, err := svc.Schedule(context.Background(), &schedulerv1.ScheduleRequest{})
		if err != nil {
			t.Fatalf("schedule failed: %v", err)
		}
		seen[resp.GetNode().GetNodeId()]++
	}
	if seen["node-a"] != 2 || seen["node-b"] != 2 {
		t.Fatalf("expected an even split across both nodes, got %v", seen)
	}
}
