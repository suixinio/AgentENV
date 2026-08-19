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
)

// registryMetricNames are the series these tests read back. They are spelled
// out rather than gathered from the default registry so a rename shows up here
// as a failure instead of as a silently empty assertion.
const (
	metricRegistryRows              = "agentenv_scheduler_registry_rows"
	metricRegistryUntracked         = "agentenv_scheduler_registry_untracked"
	metricRegistryGhost             = "agentenv_scheduler_registry_ghost"
	metricRegistryStaleCopy         = "agentenv_scheduler_registry_stale_copy"
	metricRegistryRowsWithoutRoster = "agentenv_scheduler_registry_rows_without_roster"
	metricRegistryRosterStale       = "agentenv_scheduler_registry_roster_stale"
	metricRegistryParkedExpiring    = "agentenv_scheduler_registry_parked_lease_expiring"
	metricRegistryStranded          = "agentenv_scheduler_registry_stranded_rows"
	metricRegistryInvalid           = "agentenv_scheduler_registry_invalid_rows"
	metricRegistryLastSuccess       = "agentenv_scheduler_registry_last_success_timestamp_seconds"
	metricRegistryReadFailures      = "agentenv_scheduler_registry_read_failures_total"
	metricRegistryEnabled           = "agentenv_scheduler_registry_enabled"
)

// newRegistryGatherer collects the reconciliation series into a registry of
// this test's own. The collectors themselves are the process-wide ones — that
// is the point, since the behaviour under test is what those collectors hold
// after a round — but gathering through a private registry keeps the
// assertions away from whatever else has been registered globally.
func newRegistryGatherer(t *testing.T) prometheus.Gatherer {
	t.Helper()

	reg := prometheus.NewRegistry()
	for _, collector := range []prometheus.Collector{
		schedulerRegistryRows,
		schedulerRegistryUntracked,
		schedulerRegistryGhost,
		schedulerRegistryStaleCopy,
		schedulerRegistryRowsWithoutRoster,
		schedulerRegistryRosterStale,
		schedulerRegistryParkedLeaseExpiring,
		schedulerRegistryStrandedRows,
		schedulerRegistryInvalidRows,
		schedulerRegistryLastSuccess,
		schedulerRegistryReadFailures,
		schedulerRegistryEnabled,
	} {
		reg.MustRegister(collector)
	}
	return reg
}

// gaugeSeries reads one metric family back as {label value: value}. A vector
// with no children produces no family at all, so an absent series and a series
// reading zero are distinguishable here — which is the whole subject of
// TestRegistryMetricsKeepADepartedNodesRowsVisible.
func gaugeSeries(t *testing.T, gatherer prometheus.Gatherer, name string) map[string]float64 {
	t.Helper()

	families, err := gatherer.Gather()
	if err != nil {
		t.Fatalf("gather failed: %v", err)
	}
	series := map[string]float64{}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			key := ""
			for _, label := range metric.GetLabel() {
				key = label.GetValue()
			}
			switch {
			case metric.GetGauge() != nil:
				series[key] = metric.GetGauge().GetValue()
			case metric.GetCounter() != nil:
				series[key] = metric.GetCounter().GetValue()
			default:
				t.Fatalf("metric %q is neither a gauge nor a counter", name)
			}
		}
	}
	return series
}

func gaugeValue(t *testing.T, gatherer prometheus.Gatherer, name string) float64 {
	t.Helper()

	series := gaugeSeries(t, gatherer, name)
	value, ok := series[""]
	if !ok {
		t.Fatalf("expected an unlabelled series for %q, got %v", name, series)
	}
	return value
}

// newMetricsTestService wires a scheduler over a real node registry, so the
// rosters these tests reconcile against are the ones heartbeats actually
// produce.
func newMetricsTestService(t *testing.T, nodes *AtomicNodeRegistry, reader pausedregistry.Reader) *Service {
	t.Helper()

	return NewService(
		zap.NewNop(),
		nodes,
		NewStrategy("round_robin"),
		NewInMemoryBindingStore(time.Minute),
		WithPausedRegistry(reader, testReportTTL, testReportTTL),
	)
}

func metricsHeartbeat(t *testing.T, svc *Service, nodeID string, sandboxIDs ...string) {
	t.Helper()

	// Through the service, not the registry, so the round under test consumes
	// exactly what a heartbeat leaves behind.
	_, err := svc.Heartbeat(context.Background(), &schedulerv1.HeartbeatRequest{
		NodeId:            nodeID,
		ClusterId:         "cluster-a",
		ServiceInstanceId: "svc-" + nodeID,
		Snapshot:          &schedulerv1.NodeSnapshot{Status: schedulerv1.NodeStatus_NODE_STATUS_READY},
		SandboxIds:        sandboxIDs,
	})
	if err != nil {
		t.Fatalf("heartbeat for %s failed: %v", nodeID, err)
	}
}

// 🔴 The invariant the shadow reconciliation is built on: a round that could
// not read leaves every gauge exactly where the last successful round left it.
//
// Zeroing them instead would be indistinguishable from a cluster that just
// became perfectly healthy — the same shape as the outage this whole feature
// exists to make visible. Nothing but a test can hold this: the code that would
// break it is one line in a branch that is only reached when the database is
// down.
func TestRegistryMetricsSurviveAReadFailure(t *testing.T) {
	gatherer := newRegistryGatherer(t)
	now := time.Now()
	reader := &stubRegistryReader{listing: pausedregistry.Listing{
		Now: now,
		Sandboxes: []pausedregistry.Sandbox{
			{SandboxID: "s1", State: pausedregistry.StateRunning, OriginNodeID: "node-a", UpdatedAt: now, LeaseExpiresAt: at(now, time.Hour)},
			{SandboxID: "s2", State: pausedregistry.StateRunning, OriginNodeID: "node-gone", UpdatedAt: now, LeaseExpiresAt: at(now, time.Hour)},
			{SandboxID: "s3", State: pausedregistry.StatePaused, OriginNodeID: "node-a", UpdatedAt: now, LeaseExpiresAt: at(now, time.Hour)},
		},
	}}
	nodes := NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, defaultObservedReportTTL)
	svc := newMetricsTestService(t, nodes, reader)
	metricsHeartbeat(t, svc, "node-a", "s1", "never-paused")

	if stop := svc.reconcileRegistryOnce(context.Background()); stop {
		t.Fatal("expected the first round to leave the loop running")
	}

	before := map[string]map[string]float64{}
	for _, name := range []string{
		metricRegistryRows,
		metricRegistryUntracked,
		metricRegistryRowsWithoutRoster,
		metricRegistryRosterStale,
		metricRegistryInvalid,
		metricRegistryLastSuccess,
	} {
		before[name] = gaugeSeries(t, gatherer, name)
	}
	// A round that says nothing is not worth defending; check it said something.
	if got := before[metricRegistryUntracked]["node-a"]; got != 1 {
		t.Fatalf("expected the first round to record one untracked sandbox, got %v", before[metricRegistryUntracked])
	}
	if got := before[metricRegistryRowsWithoutRoster]["node-gone"]; got != 1 {
		t.Fatalf("expected the first round to record one row without a roster, got %v", before[metricRegistryRowsWithoutRoster])
	}
	if got := before[metricRegistryInvalid][""]; got != 1 {
		t.Fatalf("expected the paused row with no snapshot to be counted as invalid, got %v", before[metricRegistryInvalid])
	}
	failuresBefore := gaugeSeries(t, gatherer, metricRegistryReadFailures)[""]

	reader.err = errors.New("connection refused")
	if stop := svc.reconcileRegistryOnce(context.Background()); stop {
		t.Fatal("expected a read failure to leave the loop running")
	}

	for name, want := range before {
		got := gaugeSeries(t, gatherer, name)
		if len(got) != len(want) {
			t.Fatalf("%s changed shape across a failed read: %v then %v", name, want, got)
		}
		for label, value := range want {
			if got[label] != value {
				t.Fatalf("%s{%q} changed across a failed read: %v then %v", name, label, value, got[label])
			}
		}
	}
	if got := gaugeSeries(t, gatherer, metricRegistryReadFailures)[""]; got != failuresBefore+1 {
		t.Fatalf("expected the read failure to be counted once, got %v after %v", got, failuresBefore)
	}
}

// 🔴 P0: the series that must not vanish when a node does.
//
// A node leaving discovery takes its roster with it, and with the roster go
// every per-node series derived from one. That is correct for those series —
// there is no roster left to describe — but it means the moment a node's rows
// become permanently stranded is the moment the graphs about that node empty
// out and any alert on them resolves itself. rows_without_roster is labelled
// from the table, so it goes the other way.
func TestRegistryMetricsKeepADepartedNodesRowsVisible(t *testing.T) {
	gatherer := newRegistryGatherer(t)
	now := time.Now()
	reader := &stubRegistryReader{listing: pausedregistry.Listing{
		Now: now,
		Sandboxes: []pausedregistry.Sandbox{
			{SandboxID: "s1", State: pausedregistry.StateRunning, OriginNodeID: "node-a", UpdatedAt: now, LeaseExpiresAt: at(now, time.Hour)},
			{SandboxID: "s2", State: pausedregistry.StateLocalOnly, OriginNodeID: "node-a", SnapshotID: "snap", UpdatedAt: now, LeaseExpiresAt: at(now, time.Hour)},
		},
	}}
	nodes := NewAtomicNodeRegistry([]Node{
		{ID: "node-a", Endpoint: "http://node-a"},
		{ID: "node-b", Endpoint: "http://node-b"},
	}, defaultObservedReportTTL)
	svc := newMetricsTestService(t, nodes, reader)
	metricsHeartbeat(t, svc, "node-a", "s1")
	// node-b is in discovery and has never reported. It must be visible as
	// stale rather than absent — a machine that came up and never checked in is
	// exactly the one an operator is looking for.
	if stop := svc.reconcileRegistryOnce(context.Background()); stop {
		t.Fatal("expected the round to leave the loop running")
	}

	stale := gaugeSeries(t, gatherer, metricRegistryRosterStale)
	if stale["node-a"] != 0 {
		t.Fatalf("expected the reporting node to be fresh, got %v", stale)
	}
	if stale["node-b"] != 1 {
		t.Fatalf("expected the node that never reported to read as stale, got %v", stale)
	}
	// Seeded at zero for every reporting node, so the series is on the page
	// before it has anything to say.
	stranded := gaugeSeries(t, gatherer, metricRegistryRowsWithoutRoster)
	if stranded["node-a"] != 0 || stranded["node-b"] != 0 || len(stranded) != 2 {
		t.Fatalf("expected every row to have a roster while node-a reports, got %v", stranded)
	}

	// Now node-a leaves the cluster: discovery drops it, which evicts its
	// observation and its roster.
	nodes.Set([]Node{{ID: "node-b", Endpoint: "http://node-b"}}, nil)
	if stop := svc.reconcileRegistryOnce(context.Background()); stop {
		t.Fatal("expected the second round to leave the loop running")
	}

	stale = gaugeSeries(t, gatherer, metricRegistryRosterStale)
	if _, present := stale["node-a"]; present {
		t.Fatalf("expected the departed node to leave the roster-derived series, got %v", stale)
	}
	if got := gaugeSeries(t, gatherer, metricRegistryUntracked); len(got) != 1 {
		t.Fatalf("expected only the surviving node to report untracked sandboxes, got %v", got)
	}
	// And the rows it left behind are now the loudest thing on the page.
	if got := gaugeSeries(t, gatherer, metricRegistryRowsWithoutRoster)["node-a"]; got != 2 {
		t.Fatalf("expected both stranded rows to be counted against the departed node, got %v", got)
	}
}

// A state no build of this scheduler knows about still gets a series, because
// seeing it is the only way anybody finds out. Once the last such row is gone
// the series has to go with it, which needs the reset a fixed five-state write
// does not give.
func TestRegistryRowsForgetsStatesThatNoLongerExist(t *testing.T) {
	gatherer := newRegistryGatherer(t)
	now := time.Now()

	recordRegistryReconcile(computeRegistryReconcile(registryReconcileInput{
		listing: pausedregistry.Listing{Now: now, Sandboxes: []pausedregistry.Sandbox{
			{SandboxID: "s1", State: pausedregistry.State("hibernating"), OriginNodeID: "node-a", UpdatedAt: now},
		}},
		now:       now,
		reportTTL: testReportTTL,
	}), now)

	if got := gaugeSeries(t, gatherer, metricRegistryRows)["hibernating"]; got != 1 {
		t.Fatalf("expected an unknown state to be reported, got %v", got)
	}

	recordRegistryReconcile(computeRegistryReconcile(registryReconcileInput{
		listing:   pausedregistry.Listing{Now: now},
		now:       now,
		reportTTL: testReportTTL,
	}), now)

	rows := gaugeSeries(t, gatherer, metricRegistryRows)
	if _, present := rows["hibernating"]; present {
		t.Fatalf("expected the vanished state to stop being reported, got %v", rows)
	}
	if got, ok := rows[string(pausedregistry.StatePaused)]; !ok || got != 0 {
		t.Fatalf("expected the five known states to keep reporting zero, got %v", rows)
	}
}

// 🔴 Without this, a cluster that runs no registry is indistinguishable from
// one whose database has never answered: both publish the same zeroes and the
// same never-advancing last_success. Running no registry is a supported
// configuration — the deployment marks the Secret optional — so every alert
// over these series has to be able to exclude those clusters.
func TestRegistryEnabledGaugeSeparatesOffFromBroken(t *testing.T) {
	gatherer := newRegistryGatherer(t)

	SetRegistryEnabled(false)
	if got := gaugeValue(t, gatherer, metricRegistryEnabled); got != 0 {
		t.Fatalf("expected 0 with no registry configured, got %v", got)
	}

	SetRegistryEnabled(true)
	if got := gaugeValue(t, gatherer, metricRegistryEnabled); got != 1 {
		t.Fatalf("expected 1 with a registry configured, got %v", got)
	}
}
