package scheduler

import (
	"context"
	"strings"
	"time"

	schedulerv1 "agentenv/services/api/proto"
	"agentenv/services/shared/observability"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
)

var (
	schedulerRPCDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "agentenv_scheduler_rpc_duration_seconds",
			Help:    "Scheduler gRPC duration by RPC and status.",
			Buckets: observability.DurationBuckets,
		},
		[]string{"rpc", "status"},
	)
	schedulerScheduleDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "agentenv_scheduler_schedule_duration_seconds",
			Help:    "Scheduler Schedule duration by strategy and status.",
			Buckets: observability.DurationBuckets,
		},
		[]string{"strategy", "status"},
	)
	schedulerScheduleAssignments = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "agentenv_scheduler_schedule_assignments_total",
			Help: "Successful scheduler assignments by strategy.",
		},
		[]string{"strategy"},
	)
	schedulerObservedNodes = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "agentenv_scheduler_observed_nodes",
			Help: "Observed node count by derived status.",
		},
		[]string{"status"},
	)
	// One series per way a lookup can end. The RPC histogram only separates
	// gRPC codes, which collapses the three very different reasons a lookup
	// answers Unavailable and cannot see at all whether a resume was served
	// from a binding, placed onto a new node, or pinned to its origin.
	schedulerLookupResults = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "agentenv_scheduler_lookup_node_total",
			Help: "Sandbox lookups by how they were answered.",
		},
		[]string{"result"},
	)

	// Paused-registry shadow reconciliation. Every series below is an
	// observation of the node-owned registry against the heartbeat rosters;
	// nothing in the scheduler acts on any of them.
	//
	// 🔴 They are all registered unconditionally, so a cluster that runs no
	// registry publishes the same series reading zero as one whose database
	// has never answered. schedulerRegistryEnabled is what separates the two,
	// and every alerting rule over the series below has to be guarded by it:
	// not running a registry is a supported configuration (the Secret is
	// optional in the deployment), and an alert that fires forever on every
	// such cluster is an alert nobody will keep.
	schedulerRegistryEnabled = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "agentenv_scheduler_registry_enabled",
			Help: "1 when this scheduler is configured with a paused-registry DSN, 0 when the feature is off. Guard every other registry alert with this.",
		},
	)
	schedulerRegistryRows = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "agentenv_scheduler_registry_rows",
			Help: "Paused-registry row count by state.",
		},
		[]string{"state"},
	)
	schedulerRegistryUntracked = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "agentenv_scheduler_registry_untracked",
			Help: "Sandboxes a node reports that the registry has no row for. NOT an orphan count: a sandbox that was never paused has no row by design, so a healthy cluster's value is non-zero.",
		},
		[]string{"node"},
	)
	schedulerRegistryGhost = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "agentenv_scheduler_registry_ghost",
			Help: "Rows the registry says are running on a node whose own fresh roster does not list them.",
		},
		[]string{"node"},
	)
	schedulerRegistryRowsWithoutRoster = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "agentenv_scheduler_registry_rows_without_roster",
			Help: "Registry rows whose holder has no current roster, by holder. Unlike every other per-node series here this one is derived from the table, so a node leaving the cluster makes it rise rather than making it disappear.",
		},
		[]string{"node"},
	)
	schedulerRegistryStaleCopy = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "agentenv_scheduler_registry_stale_copy",
			Help: "Sandboxes a node reports that the registry attributes to another node. Briefly non-zero during a cross-node takeover.",
		},
		[]string{"node"},
	)
	schedulerRegistryHolderConflict = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "agentenv_scheduler_registry_holder_conflict",
			Help: "Sandboxes reported by two or more nodes with fresh rosters that the registry cannot attribute to one of them.",
		},
	)
	schedulerRegistryParkedLeaseExpiring = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "agentenv_scheduler_registry_parked_lease_expiring",
			Help: "publishing/local_only rows carrying a snapshot whose lease is about to lapse; once it does, another node may claim the sandbox and lose the work since that snapshot. Rows with no snapshot are not counted here — nobody can claim those — see registry_stranded_rows.",
		},
	)
	schedulerRegistryLiveLeaseLapsed = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "agentenv_scheduler_registry_live_lease_lapsed",
			Help: "running/resuming rows whose lease has lapsed. Informational: nothing takes a live row over on a timer.",
		},
	)
	schedulerRegistryReclaimableNow = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "agentenv_scheduler_registry_reclaimable_now",
			Help: "Live rows whose lease has lapsed and whose sandbox has outlived its own deadline: the rows the node-side reclaim will act on next tick.",
		},
	)
	schedulerRegistryRosterStale = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "agentenv_scheduler_registry_roster_stale",
			Help: "1 when a node has not reported a roster within the staleness threshold, or has never reported one at all. Covers every node discovery knows about, so a node that came up and never checked in reads 1 rather than being absent.",
		},
		[]string{"node"},
	)
	schedulerRegistryStrandedRows = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "agentenv_scheduler_registry_stranded_rows",
			Help: "publishing/local_only rows with no snapshot: nobody but the origin node can ever resume them and nothing can ever clear them. Every pause passes through this briefly, so alert on it lasting rather than on it appearing.",
		},
	)
	schedulerRegistryInvalidRows = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "agentenv_scheduler_registry_invalid_rows",
			Help: "paused rows carrying no snapshot reference. One of these fails a whole node-side batch read, so this should always be zero.",
		},
	)
	schedulerRegistryReadFailures = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "agentenv_scheduler_registry_read_failures_total",
			Help: "Failed paused-registry reads.",
		},
	)
	schedulerRegistryReconcileDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "agentenv_scheduler_registry_reconcile_duration_seconds",
			Help:    "Duration of one paused-registry reconciliation round.",
			Buckets: observability.DurationBuckets,
		},
	)
	schedulerRegistryLastSuccess = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "agentenv_scheduler_registry_last_success_timestamp_seconds",
			Help: "Unix time of the last successful paused-registry reconciliation.",
		},
	)
)

// SetRegistryEnabled publishes whether this process was configured with a
// paused registry at all. Called once at start-up, by both the primary and the
// query-only replica.
func SetRegistryEnabled(enabled bool) {
	value := 0.0
	if enabled {
		value = 1
	}
	schedulerRegistryEnabled.Set(value)
}

func recordSchedulerLookup(result lookupResult) {
	schedulerLookupResults.WithLabelValues(string(result)).Inc()
}

func recordRegistryReadFailure() {
	schedulerRegistryReadFailures.Inc()
}

func recordRegistryReconcileDuration(start time.Time) {
	schedulerRegistryReconcileDuration.Observe(time.Since(start).Seconds())
}

// recordRegistryReconcile publishes one successful round.
//
// The per-node vectors are reset first so a node that left the cluster stops
// reporting the counts it had when it did. Only a successful round resets
// anything: a failed read leaves every gauge exactly where it was, because
// zeroing them would be indistinguishable from a cluster that just became
// perfectly healthy.
func recordRegistryReconcile(result registryReconcileResult, now time.Time) {
	// Reset here too, even though the five known states are written every
	// round: the map also carries whatever state a newer node build writes, and
	// that label would otherwise stay on /metrics forever once the last such
	// row is gone.
	schedulerRegistryRows.Reset()
	for state, count := range result.rowsByState {
		schedulerRegistryRows.WithLabelValues(string(state)).Set(float64(count))
	}

	schedulerRegistryUntracked.Reset()
	for node, count := range result.untracked {
		schedulerRegistryUntracked.WithLabelValues(node).Set(float64(count))
	}

	schedulerRegistryGhost.Reset()
	for node, count := range result.ghost {
		schedulerRegistryGhost.WithLabelValues(node).Set(float64(count))
	}

	schedulerRegistryStaleCopy.Reset()
	for node, count := range result.staleCopy {
		schedulerRegistryStaleCopy.WithLabelValues(node).Set(float64(count))
	}

	// Labelled from the registry rather than from the roster list, so a node
	// that has left the cluster keeps its series here after losing every other
	// one.
	schedulerRegistryRowsWithoutRoster.Reset()
	for node, count := range result.rowsWithoutRoster {
		schedulerRegistryRowsWithoutRoster.WithLabelValues(node).Set(float64(count))
	}

	schedulerRegistryRosterStale.Reset()
	for node, stale := range result.rosterStale {
		value := 0.0
		if stale {
			value = 1
		}
		schedulerRegistryRosterStale.WithLabelValues(node).Set(value)
	}

	schedulerRegistryHolderConflict.Set(float64(result.holderConflict))
	schedulerRegistryParkedLeaseExpiring.Set(float64(result.parkedLeaseExpiring))
	schedulerRegistryLiveLeaseLapsed.Set(float64(result.liveLeaseLapsed))
	schedulerRegistryReclaimableNow.Set(float64(result.reclaimableNow))
	schedulerRegistryStrandedRows.Set(float64(result.strandedRows))
	schedulerRegistryInvalidRows.Set(float64(result.invalidRows))
	schedulerRegistryLastSuccess.Set(float64(now.Unix()))
}

func MetricsUnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		rpc := schedulerRPCLabel(info.FullMethod)
		if rpc == "" {
			return handler(ctx, req)
		}
		start := time.Now()
		resp, err := handler(ctx, req)
		recordSchedulerRPC(rpc, start, err)
		return resp, err
	}
}

func recordSchedulerRPC(rpc string, start time.Time, err error) {
	status := observability.GRPCStatusLabel(err)
	schedulerRPCDuration.WithLabelValues(rpc, status).Observe(time.Since(start).Seconds())
}

func recordSchedulerSchedule(strategy string, start time.Time, err error) {
	strategy = schedulerStrategyLabel(strategy)
	status := observability.GRPCStatusLabel(err)
	schedulerScheduleDuration.WithLabelValues(strategy, status).Observe(time.Since(start).Seconds())
	if err == nil {
		schedulerScheduleAssignments.WithLabelValues(strategy).Inc()
	}
}

func recordObservedNodes(nodes []*schedulerv1.ObservedNode) {
	counts := map[string]int{
		"ready":       0,
		"connecting":  0,
		"unhealthy":   0,
		"lingering":   0,
		"draining":    0,
		"unspecified": 0,
	}
	for _, node := range nodes {
		counts[schedulerNodeStatusLabel(node.GetSnapshot().GetStatus())]++
	}
	for label, count := range counts {
		schedulerObservedNodes.WithLabelValues(label).Set(float64(count))
	}
}

func schedulerRPCLabel(fullMethod string) string {
	switch fullMethod[strings.LastIndex(fullMethod, "/")+1:] {
	case "Schedule":
		return "Schedule"
	case "ListNodes":
		return "ListNodes"
	case "LookupNode":
		return "LookupNode"
	case "RecordAssignment":
		return "RecordAssignment"
	case "Heartbeat":
		return "Heartbeat"
	case "ListObservedNodes":
		return "ListObservedNodes"
	case "ReportSandboxEvent":
		return "ReportSandboxEvent"
	case "GetNode":
		return "GetNode"
	case "UnregisterNode":
		return "UnregisterNode"
	case "ListRegistrySandboxes":
		return "ListRegistrySandboxes"
	default:
		return ""
	}
}

func schedulerStrategyLabel(strategy string) string {
	switch strings.ToLower(strings.TrimSpace(strategy)) {
	case "round_robin":
		return "round_robin"
	case "random":
		return "random"
	default:
		return "unknown"
	}
}

func schedulerNodeStatusLabel(status schedulerv1.NodeStatus) string {
	switch status {
	case schedulerv1.NodeStatus_NODE_STATUS_READY:
		return "ready"
	case schedulerv1.NodeStatus_NODE_STATUS_CONNECTING:
		return "connecting"
	case schedulerv1.NodeStatus_NODE_STATUS_UNHEALTHY:
		return "unhealthy"
	case schedulerv1.NodeStatus_NODE_STATUS_LINGERING:
		return "lingering"
	case schedulerv1.NodeStatus_NODE_STATUS_DRAINING:
		return "draining"
	default:
		return "unspecified"
	}
}
