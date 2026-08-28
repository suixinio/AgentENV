package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const defaultSchedulerArtifactStoreCapacity = 1_000_000

// defaultSchedulerMaxProjectionTTL caps how long a node may ask this scheduler
// to keep a routing projection.
//
// 🔴 25 hours, not the node's own 24-hour lifetime ceiling, and the extra hour
// is the point. A node computes its budget as "time left on the sandbox's life,
// plus a grace period so the record outlives the sandbox rather than dying just
// before it". Capping at exactly 24 hours would clamp every record by precisely
// that grace — undoing what the grace is for, and pinning the "clamped" counter
// at 100% so it could never signal a real misconfiguration.
//
// 🔴 It is a storage owner's limit on writers. It is not a second definition of
// how long a sandbox lives; that stays on the node, in one place, because a
// second copy would drift and the drift would look exactly like a cold cache.
const defaultSchedulerMaxProjectionTTL = 25 * time.Hour

// defaultGatewaySchedulerFallbackTimeout bounds the query-only-scheduler
// LookupNode call a projection miss and an undecided wake-up both fall
// through to (services/gateway/internal/server.go's lookupNodeFallback).
//
// 🔴 Mirrors gateway.defaultSchedulerFallbackTimeout, the value NewServer
// falls back to when a caller constructs ServerOptions directly (every test,
// and any embedder that does not go through config.Load). This package
// cannot import the gateway package to share one constant, so the two are
// declared independently and must be kept equal by hand — if they drift,
// config.Load's callers see one value and a caller building ServerOptions
// directly sees the other.
const defaultGatewaySchedulerFallbackTimeout = 3 * time.Second

// The heartbeat-timeout sweep's two cadences.
//
// 🔴 defaultSchedulerBindingSweepSilence is how long a node may say nothing
// before the scheduler retires the routing records that node's last heartbeat
// installed. Five minutes, and every term of that is measured rather than
// guessed:
//
//   - The node reports every 5s (AENV_OBSERVABILITY_REPORT_INTERVAL_SECS), so
//     this is sixty consecutive missed reports.
//   - A node that cannot reach the scheduler backs off, doubling to a ceiling
//     of 60s (MAX_REPORT_BACKOFF, src/observability/reporter.rs). Even pinned
//     at the ceiling a live node reports five times inside this window.
//   - A node missed a single heartbeat during the phase-1 rollout and backed
//     off 5s. A rolling DaemonSet restart is a pod coming back in seconds, not
//     minutes, so a routine rollout cannot reach this.
//   - It is an order of magnitude above scheduler.report_ttl (30s), which is
//     when a node reads UNHEALTHY and its roster stops being usable as a
//     routing fallback. That ordering is load-bearing rather than tidy: retire
//     a record while the roster is still fresh and the roster fallback simply
//     re-answers with the same dead node, so the sweep has to be the *last*
//     observer of silence, not the first.
//
// 🔴 It is deliberately not derived from report_ttl. The two answer different
// questions — "may I still route to what this node said" versus "has this node
// gone for good" — and tying them would move the second every time somebody
// tuned the first.
const (
	defaultSchedulerBindingSweepSilence  = 5 * time.Minute
	defaultSchedulerBindingSweepInterval = 30 * time.Second
)

const (
	defaultSchedulerRegistryMaxConnections    = 4
	defaultSchedulerRegistryReconcileInterval = 30 * time.Second
	defaultSchedulerRegistryQueryTimeout      = 5 * time.Second
	defaultSchedulerRegistryLeaseWarnWindow   = 30 * time.Second

	// The node's own lease default (`lease_ttl_secs`, src/cfg.rs). Matching it
	// means a fleet that configures neither side still agrees.
	defaultSchedulerRegistryLeaseTTL = 90 * time.Second
	// Far below any real lease, on purpose: it is there to catch a reported
	// zero or a milliseconds/seconds mix-up, not to second-guess a node whose
	// own configuration already checks its lease against its renewal cadence.
	defaultSchedulerRegistryLeaseTTLFloor = 30 * time.Second
	// The node ran this pass on its reconcile cadence; keeping the same one
	// means the changeover does not also change how quickly a decommissioned
	// machine's rows are collected.
	defaultSchedulerRegistryReclaimInterval     = 30 * time.Second
	defaultSchedulerRegistryWriteMaxConnections = 8
	defaultSchedulerRegistryDiscardMaxRows      = 10
	defaultSchedulerRegistryDiscardMaxRatio     = 0.10
)

// The snapshot catalog's build queue. Three numbers, and the relationship
// between two of them is what keeps the queue from becoming an outage.
const (
	// defaultSchedulerCatalogMaxConcurrentBuilds is the cluster-wide ceiling.
	// e2b hangs the equivalent off its tier table at 20; we have no tenants to
	// hang it off, so the subject of the quota is the cluster and the number is
	// the same one.
	defaultSchedulerCatalogMaxConcurrentBuilds = 20

	// defaultSchedulerCatalogBuildHeartbeatTTL is how long a build may go
	// without being heard from before it is treated as gone.
	//
	// Five minutes because a build legitimately takes a long time — it boots a
	// VM and runs the user's steps — while its heartbeat is a tick that has no
	// reason to stop for five minutes unless the process running it has. The
	// number this is really paired with lives on the node: the builder renews
	// at a third of this, so two consecutive misses still leave a margin.
	defaultSchedulerCatalogBuildHeartbeatTTL = 5 * time.Minute

	// defaultSchedulerCatalogBuildReapInterval is how often the pass runs. Far
	// below the TTL, because it decides how long a template stays shut *after*
	// the build holding it is already known to be gone, and that wait costs a
	// user a refused build for no further benefit.
	defaultSchedulerCatalogBuildReapInterval = 30 * time.Second

	// defaultSchedulerCatalogNodeBuildHeartbeatInterval is how often a node's
	// builder says it is still running — the *other* half of the pair, which
	// lives outside this module.
	//
	// 🔴 It mirrors `snapshot.catalog.build_heartbeat_interval_secs` in
	// `src/cfg.rs`, whose default is 100 seconds. It is stated here because
	// the floor below is meaningless without it and the scheduler has no way
	// to ask: nodes do not report their renewal cadence, and by the time one
	// is reaped it is too late to find out. Change one and change the other.
	defaultSchedulerCatalogNodeBuildHeartbeatInterval = 100 * time.Second

	// schedulerCatalogBuildHeartbeatTTLMissedRenewals is how many renewals in
	// a row may be lost before a build is treated as gone.
	//
	// Two, plus the one that has to elapse to notice: a scheduler rollout, a
	// slow network or a paused VM can eat a renewal or two without the build
	// being in any trouble.
	schedulerCatalogBuildHeartbeatTTLMissedRenewals = 3

	// schedulerCatalogBuildHeartbeatTTLFloor is the shortest TTL this process
	// will run a reaper against, whatever the renewal cadence is said to be.
	//
	// 🔴 A floor and not a default, because the two directions are not
	// symmetrical. A TTL too long leaks a template until somebody notices; a
	// TTL too short ends builds that are still running, over and over, and the
	// error the user sees says the heartbeat lapsed when it did not. One
	// mistyped unit — 30 for 30 seconds where 30s was meant, or milliseconds
	// where a duration was meant — lands on the second side, so it is refused
	// at start-up rather than run.
	//
	// 🔴 This absolute floor is the *weaker* of the two checks and used to be
	// the only one. At 30 seconds against a node renewing every 100 it let
	// through every TTL in [30s, 100s] — each of which reaps every healthy
	// build in the cluster, cluster-wide, on schedule. The cluster default of
	// 5 minutes meant nothing was ever armed, which is luck and not design.
	// The check that has the relationship in it is
	// schedulerCatalogBuildHeartbeatTTLMissedRenewals above; this one stays as
	// the guard on a renewal interval that has itself been mistyped.
	schedulerCatalogBuildHeartbeatTTLFloor = 30 * time.Second
)

// SchedulerCatalogConfig bounds the snapshot catalog's build queue.
//
// It is read only where the catalog is served, which is where the registry's
// write surface is: the catalog shares that pool, because the two halves of a
// pause have to be able to reach one transaction.
type SchedulerCatalogConfig struct {
	// MaxConcurrentBuilds is the cluster-wide ceiling on builds that are
	// pending or in progress. Zero takes the default. 🔴 A negative value
	// removes the ceiling — and with it the advisory lock and the count that
	// exist only to enforce one — which is a thing to do knowingly and never by
	// leaving a field unset, hence negative rather than zero.
	MaxConcurrentBuilds int `json:"max_concurrent_builds"`

	// BuildHeartbeatTTL is how long a build may go unheard from before the
	// reaper ends it.
	//
	// 🔴 This number is half of a pair whose other half is on the node. The
	// builder renews on a cadence derived from it; a TTL shorter than that
	// cadence reaps every build in the cluster on schedule. The floor above is
	// what catches the unit slip that produces one.
	BuildHeartbeatTTL time.Duration `json:"build_heartbeat_ttl"`

	// BuildReapInterval is how often the reaping pass runs.
	BuildReapInterval time.Duration `json:"build_reap_interval"`

	// NodeBuildHeartbeatInterval is how often this cluster's nodes renew a
	// running build's lease — `snapshot.catalog.build_heartbeat_interval_secs`
	// on the node side.
	//
	// 🔴 The scheduler does not use this to do anything; it uses it to refuse
	// a TTL that would reap healthy builds. It is a field rather than a
	// constant so that lowering the TTL is possible at all, and it is a field
	// the operator has to *say* rather than something inferred, because the
	// number it stands for lives in a different process's configuration and
	// nothing on the wire carries it. Lower the TTL without lowering this and
	// start-up refuses; lower this without lowering the node's and every build
	// in the cluster is reaped while it runs.
	NodeBuildHeartbeatInterval time.Duration `json:"node_build_heartbeat_interval"`
}

func (s *SchedulerCatalogConfig) UnmarshalJSON(data []byte) error {
	type wire struct {
		MaxConcurrentBuilds        *int            `json:"max_concurrent_builds"`
		BuildHeartbeatTTL          json.RawMessage `json:"build_heartbeat_ttl"`
		BuildReapInterval          json.RawMessage `json:"build_reap_interval"`
		NodeBuildHeartbeatInterval json.RawMessage `json:"node_build_heartbeat_interval"`
	}

	parsed := wire{}
	if err := json.Unmarshal(data, &parsed); err != nil {
		return err
	}
	if parsed.MaxConcurrentBuilds != nil {
		s.MaxConcurrentBuilds = *parsed.MaxConcurrentBuilds
	}
	if len(bytes.TrimSpace(parsed.BuildHeartbeatTTL)) > 0 {
		d, err := parseSchedulerDuration(parsed.BuildHeartbeatTTL, "scheduler.catalog.build_heartbeat_ttl")
		if err != nil {
			return err
		}
		s.BuildHeartbeatTTL = d
	}
	if len(bytes.TrimSpace(parsed.BuildReapInterval)) > 0 {
		d, err := parseSchedulerDuration(parsed.BuildReapInterval, "scheduler.catalog.build_reap_interval")
		if err != nil {
			return err
		}
		s.BuildReapInterval = d
	}
	if len(bytes.TrimSpace(parsed.NodeBuildHeartbeatInterval)) > 0 {
		d, err := parseSchedulerDuration(parsed.NodeBuildHeartbeatInterval, "scheduler.catalog.node_build_heartbeat_interval")
		if err != nil {
			return err
		}
		s.NodeBuildHeartbeatInterval = d
	}
	return nil
}

// validateSchedulerCatalog checks the build queue's bounds.
//
// 🔴 Checked whether or not the catalog is switched on, unlike the registry
// block. Every field has a default, so an invalid value takes somebody writing
// one, and the one that costs work — a TTL below the floor — is worth refusing
// at start-up on a process that has not yet been given a DSN as much as on one
// that has.
func validateSchedulerCatalog(c SchedulerCatalogConfig) error {
	if c.BuildHeartbeatTTL <= 0 {
		return errors.New("scheduler.catalog.build_heartbeat_ttl must be greater than zero")
	}
	if c.BuildHeartbeatTTL < schedulerCatalogBuildHeartbeatTTLFloor {
		return fmt.Errorf(
			"scheduler.catalog.build_heartbeat_ttl is %s, below the floor of %s: a TTL this short ends builds that are still running",
			c.BuildHeartbeatTTL, schedulerCatalogBuildHeartbeatTTLFloor)
	}
	if c.NodeBuildHeartbeatInterval <= 0 {
		return errors.New("scheduler.catalog.node_build_heartbeat_interval must be greater than zero")
	}
	// 🔴 The check with the relationship in it, and the one the absolute floor
	// above cannot make. The reaper ends a build it has not heard from within
	// the TTL, and what it hears from is a node renewing on its own cadence —
	// so a TTL below that cadence reaps *every* build in the cluster, on
	// schedule, and reports each as a lapsed heartbeat. That is a
	// configuration whose effect is total and whose symptom names the wrong
	// cause, which is the pair of properties that makes it worth refusing at
	// start-up.
	if floor := time.Duration(schedulerCatalogBuildHeartbeatTTLMissedRenewals) * c.NodeBuildHeartbeatInterval; c.BuildHeartbeatTTL < floor {
		return fmt.Errorf(
			"scheduler.catalog.build_heartbeat_ttl (%s) is below %d × scheduler.catalog.node_build_heartbeat_interval (%s = %s): "+
				"a build is reaped when it has gone unheard from for the TTL, and this cluster's nodes are declared to renew every %s, "+
				"so this TTL ends builds that are running perfectly well. Raise the TTL, or lower the node's "+
				"snapshot.catalog.build_heartbeat_interval_secs and say so here",
			c.BuildHeartbeatTTL, schedulerCatalogBuildHeartbeatTTLMissedRenewals,
			c.NodeBuildHeartbeatInterval, floor, c.NodeBuildHeartbeatInterval)
	}
	if c.BuildReapInterval <= 0 {
		return errors.New("scheduler.catalog.build_reap_interval must be greater than zero")
	}
	// 🔴 The one relation between the two, and it is not cosmetic. The reaper
	// holds itself back for a full TTL after it can first hear a heartbeat, so
	// an interval longer than the TTL means the first pass after that window
	// opens is one whole interval later still — and a template stays shut for
	// that long after everyone already knows its build is gone.
	if c.BuildReapInterval > c.BuildHeartbeatTTL {
		return fmt.Errorf(
			"scheduler.catalog.build_reap_interval (%s) must not exceed scheduler.catalog.build_heartbeat_ttl (%s)",
			c.BuildReapInterval, c.BuildHeartbeatTTL)
	}
	return nil
}

type Node struct {
	ID       string `json:"id"`
	Endpoint string `json:"endpoint"`
}

type SchedulerDiscoveryKubernetesConfig struct {
	Namespace             string `json:"namespace"`
	ServiceName           string `json:"service_name"`
	Port                  int32  `json:"port"`
	Scheme                string `json:"scheme"`
	IgnorePodSelector     string `json:"ignore_pod_selector"`
	NoSchedulePodSelector string `json:"no_schedule_pod_selector"`
}

type SchedulerDiscoveryConfig struct {
	Mode       string                             `json:"mode"`
	Kubernetes SchedulerDiscoveryKubernetesConfig `json:"kubernetes"`
}

// NodeResourceLimit defines per-node resource thresholds for scheduling
// eligibility. A node exceeding any configured limit is excluded from
// scheduling candidates. Nil (absent) fields impose no limit.
//
// Allocated-percent limits (CPU and memory) can legitimately exceed 100%
// because allocated resources reflect the sum of all sandbox reservations,
// which may overcommit the physical capacity of the node.
type NodeResourceLimit struct {
	MaxSandboxCount           *uint32 `json:"max_sandbox_count"`
	MaxSandboxStartingCount   *uint32 `json:"max_sandbox_starting_count"`
	MaxCPUUsedPercent         *uint32 `json:"max_cpu_used_percent"`
	MaxCPUAllocatedPercent    *uint32 `json:"max_cpu_allocated_percent"` // can exceed 100 (overcommit)
	MaxMemoryUsedPercent      *uint32 `json:"max_memory_used_percent"`
	MaxMemoryAllocatedPercent *uint32 `json:"max_memory_allocated_percent"` // can exceed 100 (overcommit)

	// Limits that apply to the sum of the active running set plus paused
	// sandboxes. Paused sandboxes have released their VM-side CPU / memory
	// but still occupy persisted state on the node, so operators may want a
	// separate ceiling on total node footprint (including paused) on top of
	// the active-only ceilings above.
	MaxSandboxCountIncludingPaused         *uint32 `json:"max_sandbox_count_including_paused"`
	MaxAllocatedCPUIncludingPaused         *uint32 `json:"max_allocated_cpu_including_paused"`
	MaxAllocatedMemoryBytesIncludingPaused *uint64 `json:"max_allocated_memory_bytes_including_paused"`
}

// SchedulerRegistryConfig points the scheduler at the node-owned
// `paused_sandboxes` table, read-only.
//
// An empty DSN switches the whole thing off: no pool, no reconciliation, and
// the read-only registry API answers FailedPrecondition. That is the default,
// and it must stay a supported configuration — a cluster that never sets this
// behaves exactly as it did before this existed.
type SchedulerRegistryConfig struct {
	// DSN is supplied through the environment or a Secret, never through the
	// config file: it carries credentials and the config file is a ConfigMap.
	DSN string `json:"dsn"`
	// ClusterID scopes every query. Empty means "read every row in the
	// database", which is only correct when this database serves one cluster.
	ClusterID         string        `json:"cluster_id"`
	MaxConnections    int32         `json:"max_connections"`
	ReconcileInterval time.Duration `json:"reconcile_interval"`
	QueryTimeout      time.Duration `json:"query_timeout"`
	// LeaseWarnWindow is how far ahead a parked row's lease is looked at
	// before it is counted as expiring.
	LeaseWarnWindow time.Duration `json:"lease_warn_window"`

	// WriteEnabled turns on the write surface: the migration, the
	// PausedRegistry gRPC service, and the reclamation timer.
	//
	// Off by default, and it has to stay that way. Switching it on makes this
	// process the owner of a table the nodes are still writing themselves; the
	// two are only safe together in the changeover window the rollout notes
	// describe, and defaulting to on would put every existing deployment into
	// that window on an upgrade nobody asked for.
	WriteEnabled bool `json:"write_enabled"`
	// WriteFencing adds the identity-axis predicates to begin_pause and
	// mark_running: a write claiming to come from an incarnation is refused
	// unless the row names that incarnation.
	//
	// 🔴 On by default, and the rollback is this setting rather than a code
	// path. It is also *not* the same switch as
	// scheduler.routing.execution_arbitration or
	// gateway.routing.execution_fencing — the three names carry their scope for
	// exactly that reason, because "fencing is off" otherwise has three
	// possible meanings and only this one loses a workspace.
	//
	// 🔴 It turns the checking off, never the column. The CHECK constraint is
	// DDL and does not follow the setting, so nodes must go on sending an
	// execution id however this is set; with it false their writes are simply
	// not compared against the row.
	WriteFencing bool `json:"write_fencing"`
	// WriteMaxConnections caps the writable pool. This is the number the whole
	// fleet's writes now share, where each node used to hold its own.
	WriteMaxConnections int32 `json:"write_max_connections"`
	// LeaseTTL is the lease length stamped for callers that report none of
	// their own, and the length of the restart grace window.
	//
	// It is the node's configuration that governs a real lease — see
	// registry.Store.WithLeaseTTL — so this value is a fallback and a yardstick
	// rather than a policy. It defaults to the node's own default for that
	// reason.
	LeaseTTL time.Duration `json:"lease_ttl"`
	// ReclaimInterval is how often the cluster's backstop pass runs.
	ReclaimInterval time.Duration `json:"reclaim_interval"`
	// LeaseTTLFloor is the shortest lease this process will stamp, however
	// short a value a node reports.
	//
	// It is a guard against an unset field or a unit confused for another, not
	// a policy: the invariant that a lease outlives the cadence renewing it is
	// checked on the node, which is the only place that knows both numbers.
	// Values below it are raised, never refused — a longer lease is harder to
	// take over, and failing a pause over an advisory number is not a trade
	// worth making.
	LeaseTTLFloor time.Duration `json:"lease_ttl_floor"`
	// DiscardMaxRows and DiscardMaxRatio bound how much one reclamation pass
	// may delete before it is refused outright. Both apply; the stricter wins.
	DiscardMaxRows  int64   `json:"discard_max_rows"`
	DiscardMaxRatio float64 `json:"discard_max_ratio"`

	// HeartbeatLeaseRenewal turns on the scheduler's own renewal of
	// publishing/local_only leases, driven by the same heartbeat rosters
	// RunRegistryReconcile already reads: a row whose holder has a fresh
	// roster that still lists the sandbox gets lease_expires_at pushed out
	// directly by this process.
	//
	// 🔴 Off by default, and unlike WriteFencing that is not a rollback
	// posture — it is the honest starting point. The api half's own renewal
	// call (renew_paused_leases) keys its match on the calling process's own
	// identity, which stopped being the row's holder for these two states once
	// origin_node_id was changed to name the machine that actually holds the
	// bytes; this switch is a second, independent way to keep those rows
	// alive, added to a `paused_sandboxes` table both the EKS and the pve-sg
	// trees still read from the same tree. Turning it on must be a value
	// change an operator makes deliberately, on a build both sides are ready
	// for — never a default an upgrade acquires on its own.
	//
	// It does nothing unless this process also has a write surface: no DSN,
	// write_enabled=false, and every query-only replica all leave it inert
	// however this is set, the same as WriteFencing.
	HeartbeatLeaseRenewal bool `json:"heartbeat_lease_renewal"`
}

func (s *SchedulerRegistryConfig) UnmarshalJSON(data []byte) error {
	type wire struct {
		DSN               *string         `json:"dsn"`
		ClusterID         *string         `json:"cluster_id"`
		MaxConnections    *int32          `json:"max_connections"`
		ReconcileInterval json.RawMessage `json:"reconcile_interval"`
		QueryTimeout      json.RawMessage `json:"query_timeout"`
		LeaseWarnWindow   json.RawMessage `json:"lease_warn_window"`

		WriteEnabled          *bool           `json:"write_enabled"`
		WriteFencing          *bool           `json:"write_fencing"`
		WriteMaxConnections   *int32          `json:"write_max_connections"`
		LeaseTTL              json.RawMessage `json:"lease_ttl"`
		LeaseTTLFloor         json.RawMessage `json:"lease_ttl_floor"`
		ReclaimInterval       json.RawMessage `json:"reclaim_interval"`
		DiscardMaxRows        *int64          `json:"discard_max_rows"`
		DiscardMaxRatio       *float64        `json:"discard_max_ratio"`
		HeartbeatLeaseRenewal *bool           `json:"heartbeat_lease_renewal"`
	}

	parsed := wire{}
	if err := json.Unmarshal(data, &parsed); err != nil {
		return err
	}

	if parsed.DSN != nil {
		s.DSN = *parsed.DSN
	}
	if parsed.ClusterID != nil {
		s.ClusterID = *parsed.ClusterID
	}
	if parsed.MaxConnections != nil {
		s.MaxConnections = *parsed.MaxConnections
	}

	if len(bytes.TrimSpace(parsed.ReconcileInterval)) > 0 {
		d, err := parseSchedulerDuration(parsed.ReconcileInterval, "scheduler.registry.reconcile_interval")
		if err != nil {
			return err
		}
		s.ReconcileInterval = d
	}
	if len(bytes.TrimSpace(parsed.QueryTimeout)) > 0 {
		d, err := parseSchedulerDuration(parsed.QueryTimeout, "scheduler.registry.query_timeout")
		if err != nil {
			return err
		}
		s.QueryTimeout = d
	}
	if len(bytes.TrimSpace(parsed.LeaseWarnWindow)) > 0 {
		d, err := parseSchedulerDuration(parsed.LeaseWarnWindow, "scheduler.registry.lease_warn_window")
		if err != nil {
			return err
		}
		s.LeaseWarnWindow = d
	}

	if parsed.WriteEnabled != nil {
		s.WriteEnabled = *parsed.WriteEnabled
	}
	if parsed.WriteFencing != nil {
		s.WriteFencing = *parsed.WriteFencing
	}
	if parsed.WriteMaxConnections != nil {
		s.WriteMaxConnections = *parsed.WriteMaxConnections
	}
	if len(bytes.TrimSpace(parsed.LeaseTTL)) > 0 {
		d, err := parseSchedulerDuration(parsed.LeaseTTL, "scheduler.registry.lease_ttl")
		if err != nil {
			return err
		}
		s.LeaseTTL = d
	}
	if len(bytes.TrimSpace(parsed.LeaseTTLFloor)) > 0 {
		d, err := parseSchedulerDuration(parsed.LeaseTTLFloor, "scheduler.registry.lease_ttl_floor")
		if err != nil {
			return err
		}
		s.LeaseTTLFloor = d
	}
	if len(bytes.TrimSpace(parsed.ReclaimInterval)) > 0 {
		d, err := parseSchedulerDuration(parsed.ReclaimInterval, "scheduler.registry.reclaim_interval")
		if err != nil {
			return err
		}
		s.ReclaimInterval = d
	}
	if parsed.DiscardMaxRows != nil {
		s.DiscardMaxRows = *parsed.DiscardMaxRows
	}
	if parsed.DiscardMaxRatio != nil {
		s.DiscardMaxRatio = *parsed.DiscardMaxRatio
	}
	if parsed.HeartbeatLeaseRenewal != nil {
		s.HeartbeatLeaseRenewal = *parsed.HeartbeatLeaseRenewal
	}

	return nil
}

type SchedulerConfig struct {
	GRPCListenAddr          string                   `json:"grpc_listen_addr"`
	MetricsListenAddr       string                   `json:"metrics_listen_addr"`
	Strategy                string                   `json:"strategy"`
	ReportTTL               time.Duration            `json:"report_ttl"`
	BindingTTL              time.Duration            `json:"binding_ttl"`
	WarmupTimeout           time.Duration            `json:"warmup_timeout"`
	RedisAddr               string                   `json:"redis_addr"`
	MaxProjectionTTL        time.Duration            `json:"max_projection_ttl"`
	BindingSweepSilence     time.Duration            `json:"binding_sweep_silence"`
	BindingSweepInterval    time.Duration            `json:"binding_sweep_interval"`
	ArtifactStoreCapacity   int                      `json:"artifact_store_capacity"`
	ArtifactLookupNodeLimit int                      `json:"artifact_lookup_node_limit"`
	Nodes                   []Node                   `json:"nodes"`
	Discovery               SchedulerDiscoveryConfig `json:"discovery"`
	NodeResourceLimit       *NodeResourceLimit       `json:"node_resource_limit"`
	Registry                SchedulerRegistryConfig  `json:"registry"`
	Catalog                 SchedulerCatalogConfig   `json:"catalog"`
	Routing                 SchedulerRoutingConfig   `json:"routing"`
}

func (s *SchedulerConfig) UnmarshalJSON(data []byte) error {
	type wire struct {
		GRPCListenAddr          *string                   `json:"grpc_listen_addr"`
		MetricsListenAddr       *string                   `json:"metrics_listen_addr"`
		Strategy                *string                   `json:"strategy"`
		ReportTTL               json.RawMessage           `json:"report_ttl"`
		BindingTTL              json.RawMessage           `json:"binding_ttl"`
		WarmupTimeout           json.RawMessage           `json:"warmup_timeout"`
		RedisAddr               *string                   `json:"redis_addr"`
		MaxProjectionTTL        json.RawMessage           `json:"max_projection_ttl"`
		BindingSweepSilence     json.RawMessage           `json:"binding_sweep_silence"`
		BindingSweepInterval    json.RawMessage           `json:"binding_sweep_interval"`
		ArtifactStoreCapacity   *int                      `json:"artifact_store_capacity"`
		ArtifactLookupNodeLimit *int                      `json:"artifact_lookup_node_limit"`
		Nodes                   *[]Node                   `json:"nodes"`
		Discovery               *SchedulerDiscoveryConfig `json:"discovery"`
		NodeResourceLimit       *NodeResourceLimit        `json:"node_resource_limit"`
		// Decoded into the existing value rather than through a pointer,
		// so a config that names only one registry key keeps the defaults
		// for the others instead of zeroing them.
		Registry json.RawMessage `json:"registry"`
		// Decoded into the existing value for the same reason the registry
		// block is: a config naming one catalog key must keep the defaults for
		// the others, and one of those others is a TTL whose zero value would
		// be refused at start-up.
		Catalog json.RawMessage `json:"catalog"`
		// Nested one pointer deep on each side, so a config file that names
		// the block without naming the key inside it leaves the default alone
		// rather than blanking it.
		Routing *struct {
			ExecutionArbitration    *string `json:"execution_arbitration"`
			ProjectionAuthoritative *bool   `json:"projection_authoritative"`
			BindingSweep            *bool   `json:"binding_sweep"`
		} `json:"routing"`
	}

	parsed := wire{}
	if err := json.Unmarshal(data, &parsed); err != nil {
		return err
	}

	if parsed.GRPCListenAddr != nil {
		s.GRPCListenAddr = *parsed.GRPCListenAddr
	}
	if parsed.MetricsListenAddr != nil {
		s.MetricsListenAddr = *parsed.MetricsListenAddr
	}
	if parsed.Strategy != nil {
		s.Strategy = *parsed.Strategy
	}
	if parsed.Nodes != nil {
		s.Nodes = *parsed.Nodes
	}
	if parsed.Discovery != nil {
		s.Discovery = *parsed.Discovery
	}
	if parsed.NodeResourceLimit != nil {
		s.NodeResourceLimit = parsed.NodeResourceLimit
	}
	if parsed.RedisAddr != nil {
		s.RedisAddr = *parsed.RedisAddr
	}
	if parsed.ArtifactStoreCapacity != nil {
		s.ArtifactStoreCapacity = *parsed.ArtifactStoreCapacity
	}
	if parsed.ArtifactLookupNodeLimit != nil {
		s.ArtifactLookupNodeLimit = *parsed.ArtifactLookupNodeLimit
	}

	if len(bytes.TrimSpace(parsed.Registry)) > 0 {
		if err := json.Unmarshal(parsed.Registry, &s.Registry); err != nil {
			return err
		}
	}
	if len(bytes.TrimSpace(parsed.Catalog)) > 0 {
		if err := json.Unmarshal(parsed.Catalog, &s.Catalog); err != nil {
			return err
		}
	}
	if parsed.Routing != nil && parsed.Routing.ExecutionArbitration != nil {
		// Carried through verbatim rather than parsed here: an unrecognised
		// value has to reach validate() and stop the process, and parsing at
		// this depth would turn it into an unmarshal error whose message names
		// the JSON rather than the setting.
		s.Routing.ExecutionArbitration = SchedulerExecutionArbitration(*parsed.Routing.ExecutionArbitration)
	}
	if parsed.Routing != nil && parsed.Routing.ProjectionAuthoritative != nil {
		s.Routing.ProjectionAuthoritative = *parsed.Routing.ProjectionAuthoritative
	}
	if parsed.Routing != nil && parsed.Routing.BindingSweep != nil {
		s.Routing.BindingSweep = *parsed.Routing.BindingSweep
	}

	if len(bytes.TrimSpace(parsed.MaxProjectionTTL)) > 0 {
		d, err := parseSchedulerDuration(parsed.MaxProjectionTTL, "scheduler.max_projection_ttl")
		if err != nil {
			return err
		}
		s.MaxProjectionTTL = d
	}

	if len(bytes.TrimSpace(parsed.BindingSweepSilence)) > 0 {
		d, err := parseSchedulerDuration(parsed.BindingSweepSilence, "scheduler.binding_sweep_silence")
		if err != nil {
			return err
		}
		s.BindingSweepSilence = d
	}
	if len(bytes.TrimSpace(parsed.BindingSweepInterval)) > 0 {
		d, err := parseSchedulerDuration(parsed.BindingSweepInterval, "scheduler.binding_sweep_interval")
		if err != nil {
			return err
		}
		s.BindingSweepInterval = d
	}

	if len(bytes.TrimSpace(parsed.ReportTTL)) > 0 {
		d, err := parseSchedulerDuration(parsed.ReportTTL, "scheduler.report_ttl")
		if err != nil {
			return err
		}
		s.ReportTTL = d
	}
	if len(bytes.TrimSpace(parsed.BindingTTL)) > 0 {
		d, err := parseSchedulerDuration(parsed.BindingTTL, "scheduler.binding_ttl")
		if err != nil {
			return err
		}
		s.BindingTTL = d
	}
	if len(bytes.TrimSpace(parsed.WarmupTimeout)) > 0 {
		d, err := parseSchedulerDuration(parsed.WarmupTimeout, "scheduler.warmup_timeout")
		if err != nil {
			return err
		}
		s.WarmupTimeout = d
	}

	return nil
}

func parseSchedulerDuration(raw json.RawMessage, field string) (time.Duration, error) {
	var asString string
	if err := json.Unmarshal(raw, &asString); err == nil {
		d, parseErr := time.ParseDuration(strings.TrimSpace(asString))
		if parseErr != nil {
			return 0, fmt.Errorf("%s must be a duration string like \"30s\": %w", field, parseErr)
		}
		return d, nil
	}

	var asNumber json.Number
	if err := json.Unmarshal(raw, &asNumber); err == nil {
		return 0, fmt.Errorf("%s must be a duration string like \"30s\", got numeric value %s", field, asNumber.String())
	}

	return 0, fmt.Errorf("%s must be a duration string like \"30s\"", field)
}

// SchedulerExecutionArbitration is the three-state switch over what the
// scheduler's routing half does with an incarnation: whether a binding written
// by an older one may displace a newer one, and whether LookupNode answers with
// an incarnation at all.
//
// 🔴 It governs the routing half alone. scheduler.registry.write_fencing turns
// off the registry's SQL predicates and gateway.routing.execution_fencing turns
// off the gateway's refusal; the three are deliberately separate settings with
// their scope in their names. Merging any two would let one panicked flip
// switch off a half nobody meant to — and the half that goes quiet is not the
// one whose failure is visible.
type SchedulerExecutionArbitration string

const (
	// Off is the complete rollback: bindings are overwritten by whoever
	// reported last, exactly as before, and the two new LookupNode fields stay
	// at their zero values.
	SchedulerExecutionArbitrationOff SchedulerExecutionArbitration = "off"
	// Observe works out what arbitration would have decided and counts it, but
	// writes the way Off does. The release runs a round of this first.
	SchedulerExecutionArbitrationObserve SchedulerExecutionArbitration = "observe"
	// Enforce is the default: an older incarnation cannot take a binding back.
	SchedulerExecutionArbitrationEnforce SchedulerExecutionArbitration = "enforce"
)

// ParseSchedulerExecutionArbitration is the one place a mode string becomes a
// mode.
//
// 🔴 An unrecognised value is an error, never a fallback. A fallback makes one
// mistyped letter switch arbitration off without saying so, and the resulting
// behaviour is indistinguishable from the value having been meant. The empty
// string is not a mistyped value: it is the absence of a setting, and it
// resolves to the documented default.
func ParseSchedulerExecutionArbitration(raw string) (SchedulerExecutionArbitration, error) {
	switch SchedulerExecutionArbitration(strings.ToLower(strings.TrimSpace(raw))) {
	case "":
		return SchedulerExecutionArbitrationEnforce, nil
	case SchedulerExecutionArbitrationOff:
		return SchedulerExecutionArbitrationOff, nil
	case SchedulerExecutionArbitrationObserve:
		return SchedulerExecutionArbitrationObserve, nil
	case SchedulerExecutionArbitrationEnforce:
		return SchedulerExecutionArbitrationEnforce, nil
	default:
		return "", fmt.Errorf("scheduler.routing.execution_arbitration must be one of %s, %s, %s, got %q",
			SchedulerExecutionArbitrationOff, SchedulerExecutionArbitrationObserve, SchedulerExecutionArbitrationEnforce, raw)
	}
}

// SchedulerRoutingConfig groups the switches over what the scheduler does with
// a routing answer, as opposed to what it does with the registry table.
type SchedulerRoutingConfig struct {
	ExecutionArbitration SchedulerExecutionArbitration `json:"execution_arbitration"`
	// ProjectionAuthoritative promotes the routing projection from a cache the
	// heartbeat rewrites every five seconds into a record with a lifetime of
	// its own: node-supplied TTLs are honoured, pause and delete events remove
	// records, and a heartbeat that finds nothing changed stops resetting the
	// deadline.
	//
	// 🔴 Off by default, which is the opposite of the switch above it, and the
	// reason is not caution for its own sake. Every node in the fleet is
	// already sending the lifecycle events this makes act; a cluster that
	// upgraded the scheduler binary without setting anything would start
	// mutating its own routing table the moment a pod restarted, having asked
	// for nothing. The switches above default to their end state because that
	// release existed in order to turn them on.
	ProjectionAuthoritative bool `json:"projection_authoritative"`
	// BindingSweep lets the scheduler retire the routing records a node
	// installed once that node has stopped heartbeating for longer than
	// scheduler.binding_sweep_silence.
	//
	// 🔴 Off by default, for the same reason as the switch above it and not
	// merely by imitation. Every node in the fleet is already heartbeating, so
	// a scheduler that acquired this by being upgraded would start deleting
	// routing records on a timer having been asked for nothing — and the one
	// failure mode that matters here is deleting the record of a sandbox that
	// is alive.
	//
	// 🔴 Independent of ProjectionAuthoritative rather than implied by it. The
	// sweep only *matters* when records are long-lived, but a switch that is
	// two switches cannot be turned on alone, and this one has to be
	// exercisable — and revertible — without moving the write switch under a
	// running cluster.
	BindingSweep bool `json:"binding_sweep"`
}

// GatewayExecutionFencing is the three-state switch over the gateway's routing
// layer refusal: whether it stamps the incarnation it routed against onto the
// request, and whether a mismatch is refused or only counted.
//
// 🔴 It governs the gateway alone. Two other switches carry names that read the
// same way and turn off different halves — scheduler.registry.write_fencing
// guards the registry's SQL predicates, scheduler.routing.execution_arbitration
// guards binding arbitration — so "is fencing off" has no single answer and
// each has to be named. They are deliberately not merged: sharing one would let
// a single panic-flip switch off a half nobody meant to, silently.
type GatewayExecutionFencing string

const (
	// Off is the complete rollback: the gateway behaves byte for byte as it did
	// before execution fencing existed.
	GatewayExecutionFencingOff GatewayExecutionFencing = "off"
	// Observe compares and counts, and stamps nothing.
	//
	// 🔴 The "stamps nothing" is the load-bearing half, not a detail: stamping
	// the expect header is what arms the node's own refusal, and a 412 the node
	// has already produced cannot be withdrawn by a gateway that was only meant
	// to be watching — it can only be translated into a 409 the client did not
	// get before. So observe delegates nothing, and takes its whole reading off
	// the echo the node sends regardless of what was expected of it, which costs
	// it no observability at all. (This comment previously read "stamps and
	// compares and counts"; corrected 2026-08-20 — observe never stamped after
	// the adjudication that made it a real dry run.)
	GatewayExecutionFencingObserve GatewayExecutionFencing = "observe"
	// Enforce is the default. Both gates are live.
	GatewayExecutionFencingEnforce GatewayExecutionFencing = "enforce"
)

// ParseGatewayExecutionFencing is the one place a mode string becomes a mode.
//
// 🔴 An unrecognised value is an error, never a fallback. A fallback would make
// one mistyped letter switch fencing off — or on — without saying so, and the
// resulting behaviour is indistinguishable from the value having been meant.
// The empty string is not a mistyped value: it is the absence of a setting, and
// it resolves to the documented default.
func ParseGatewayExecutionFencing(raw string) (GatewayExecutionFencing, error) {
	switch GatewayExecutionFencing(strings.ToLower(strings.TrimSpace(raw))) {
	case "":
		return GatewayExecutionFencingEnforce, nil
	case GatewayExecutionFencingOff:
		return GatewayExecutionFencingOff, nil
	case GatewayExecutionFencingObserve:
		return GatewayExecutionFencingObserve, nil
	case GatewayExecutionFencingEnforce:
		return GatewayExecutionFencingEnforce, nil
	default:
		return "", fmt.Errorf("gateway.routing.execution_fencing must be one of %s, %s, %s, got %q",
			GatewayExecutionFencingOff, GatewayExecutionFencingObserve, GatewayExecutionFencingEnforce, raw)
	}
}

// ParseRoutingProjectionSwitch is the one place an on/off setting becomes a
// bool.
//
// 🔴 An unrecognised value is an error rather than a false. These switches
// change what a running cluster does to its own routing table, and the failure
// mode of guessing is a rollout that reports success while the switch it was
// for never moved. "true"/"false"/"1"/"0" are accepted alongside "on"/"off"
// because operators reach for all of them and a typo should be the only thing
// that stops the process.
func ParseRoutingProjectionSwitch(raw string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "on", "true", "1":
		return true, nil
	case "off", "false", "0":
		return false, nil
	default:
		return false, fmt.Errorf("must be one of on, off (got %q)", raw)
	}
}

// ParseRestUpstream turns the configured REST upstream into the base URL the
// gateway forwards to, or reports that there is none.
//
// The empty string — the switch off — is not an error: it answers "" with a nil
// error, and the caller reads that as "fan out to the nodes, as before".
//
// 🔴 Anything else must be a usable absolute address or the process stops. A
// REST upstream that cannot be parsed is not a degraded upstream, it is a
// gateway that answers every sandbox create with a 502 while reporting the
// switch as on — and "the switch is on and nothing works" is the state an
// operator will spend the incident staring at the api half for. A bare
// `host:port` is accepted and read as http, because that is the shape every
// other address in this config file has and an operator copying the style of
// `scheduler_addr` should not get a refusal for it.
func ParseRestUpstream(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", nil
	}

	candidate := trimmed
	if !strings.Contains(candidate, "://") {
		candidate = "http://" + candidate
	}
	parsed, err := url.Parse(candidate)
	if err != nil {
		return "", fmt.Errorf("gateway.rest_upstream_addr %q is not an address: %w", raw, err)
	}
	switch parsed.Scheme {
	case "http", "https":
	default:
		return "", fmt.Errorf("gateway.rest_upstream_addr %q must be http or https, got scheme %q", raw, parsed.Scheme)
	}
	if parsed.Host == "" {
		return "", fmt.Errorf("gateway.rest_upstream_addr %q names no host", raw)
	}
	// 🔴 A path would be silently prepended to every forwarded route, so a
	// value like `http://agentenv-api:8000/` — which an operator pasting from a
	// browser produces without thinking — must not turn `/sandboxes` into
	// `//sandboxes`. A bare "/" is the one path that means nothing, so it is
	// dropped rather than refused; anything longer is a real prefix and this
	// switch does not carry one.
	if path := strings.Trim(parsed.Path, "/"); path != "" {
		return "", fmt.Errorf("gateway.rest_upstream_addr %q must not carry a path (%q)", raw, parsed.Path)
	}
	parsed.Path = ""
	parsed.RawPath = ""
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String(), nil
}

// GatewayRoutingConfig groups the switches over what the gateway does with a
// routing answer, as opposed to where it listens or who it talks to.
type GatewayRoutingConfig struct {
	ExecutionFencing GatewayExecutionFencing `json:"execution_fencing"`
	// ProjectionRead lets the gateway answer a sandbox route from the routing
	// projection directly, falling back to the scheduler on a miss or an error.
	// Off leaves every request going through LookupNode, which is what shipped
	// before this existed.
	//
	// 🔴 Turning this on must be paired with routing.execution_arbitration
	// staying anything other than "off". The scheduler's rollback for the
	// incarnation axis works by blanking the two incarnation fields on the way
	// out of LookupNode; a gateway reading the projection itself never sees
	// that blanking and would go on fencing against incarnations the scheduler
	// has stopped arbitrating. There is no mechanism for it — the pairing is an
	// operational rule, written here because this is where somebody reads it.
	ProjectionRead bool `json:"projection_read"`
	// ProjectionAuthoritative is the gateway's half of the write-side switch:
	// it makes resume and connect record an assignment, and makes the gateway
	// forward the incarnation and TTL a node reports. Off means it forwards
	// neither, which is a projection write identical to today's.
	//
	// 🔴 The write side is one logical switch across two processes and each
	// holds half of it. Neither ordering is unsafe — see the note in the stage
	// plan — so they do not have to be flipped together.
	ProjectionAuthoritative bool `json:"projection_authoritative"`
}

type GatewayConfig struct {
	HTTPListenAddr         string `json:"http_listen_addr"`
	MetricsListenAddr      string `json:"metrics_listen_addr"`
	SchedulerAddr          string `json:"scheduler_addr"`
	QueryOnlySchedulerAddr string `json:"query_only_scheduler_addr"`
	// ResumeAddr is the api half's wake-up surface, asked when the routing
	// projection has no answer for a sandbox.
	//
	// 🔴 No longer optional. Empty used to be the switch off — every
	// projection miss fell through to the scheduler, and the node the request
	// landed on woke the sandbox itself — while 阶段 3a's rollback was a
	// ConfigMap change and a gateway restart rather than a DaemonSet roll
	// (`_sd-impl-phase3-role.md` §11.1, §11.2). That premise is retired: nodes
	// run `aenv-node` now and have no wake-up surface of their own under any
	// configuration, so `Validate` refuses a config with this empty rather
	// than letting a cluster discover it as every resume attempt silently
	// falling back to the scheduler. See rest_upstream.go and
	// `TestTheRestUpstreamIsAlwaysSetBecauseNodesNeverServeRest`.
	ResumeAddr string `json:"resume_addr"`
	// RestUpstreamAddr is where user-facing REST goes: sandbox, snapshot and
	// template calls, the routes `aenv-node` answers 404 on.
	//
	// 🔴 No longer optional, for the same reason as ResumeAddr above. Empty
	// used to be the switch off — the gateway asked the scheduler which node
	// should serve the call and forwarded it there, because the nodes were
	// still the pre-split single process and never stopped being able to
	// serve it (`_sd-impl-phase3-role.md` §11.1, §11.2). That premise is
	// retired: nodes run `aenv-node` now and answer 404 on every user-facing
	// REST route under any configuration, so `Validate` refuses a config with
	// this empty rather than letting a cluster discover it as REST 404s. Set —
	// `http://agentenv-api:8000`, or a bare `agentenv-api:8000` which is read
	// as http — the calls go to the api half, which owns sandboxes and drives
	// the machines itself. See rest_upstream.go and
	// `TestTheRestUpstreamIsAlwaysSetBecauseNodesNeverServeRest`.
	//
	// 🔴 Never carries data-plane traffic. A request routed by proxy headers or
	// by a sandbox proxy domain goes to the node holding the sandbox whatever
	// this says: the api half runs no sandboxes and has nothing to proxy to.
	RestUpstreamAddr string `json:"rest_upstream_addr"`
	// RedisAddr is where the routing projection lives. Read-only from here:
	// the gateway never writes a record, and the scheduler is the only process
	// that arbitrates one.
	RedisAddr           string        `json:"redis_addr"`
	RequestTimeout      time.Duration `json:"request_timeout"`
	ForwardResponseSize int64         `json:"forward_response_size"`
	SandboxProxyDomains []string      `json:"sandbox_proxy_domains"`
	// DebugMode enables debug-only behaviors in the gateway such as exposing
	// the backend node id on proxied responses. It is off by default.
	DebugMode bool                 `json:"debug_mode"`
	Routing   GatewayRoutingConfig `json:"routing"`
	// ControlPlaneToken is the shared secret the gateway stamps on every request
	// it forwards to a node, so the node can tell control-plane traffic from
	// anything that reached it another way.
	//
	// It is a credential, so it arrives through the environment or a Secret and
	// never through the config file, which is a ConfigMap — the same rule the
	// registry DSN follows, and the reason it is absent from the JSON shape
	// below rather than merely undocumented. Empty is an ordinary value: the
	// gateway stamps nothing and the node's gate stays open, which is what makes
	// the rollout config-driven instead of deploy-driven.
	ControlPlaneToken string `json:"-"`
	// SchedulerFallbackDisabled turns off the query-only-scheduler LookupNode
	// call that a projection miss and an undecided wake-up both fall through
	// to. False — the default, and today's behaviour exactly — still asks the
	// scheduler on that cold path.
	//
	// 🔴 Phase 4's decommissioning lever for that one call, in the same shape
	// as ResumeAddr and RestUpstreamAddr above: a ConfigMap edit and a
	// restart, not a code change. See gateway/internal/server.go's
	// ServerOptions.SchedulerFallbackDisabled.
	SchedulerFallbackDisabled bool `json:"scheduler_fallback_disabled"`
	// SchedulerFallbackTimeout bounds that same call on its own, separately
	// from RequestTimeout, so a scheduler that is merely unreachable cannot
	// hold it open for whatever of the request's overall budget is left. Zero
	// (the default when unset) is resolved to a fixed fallback inside
	// gateway.NewServer, never left as "no timeout".
	SchedulerFallbackTimeout time.Duration `json:"scheduler_fallback_timeout"`
}

func (g *GatewayConfig) UnmarshalJSON(data []byte) error {
	type wire struct {
		HTTPListenAddr         *string         `json:"http_listen_addr"`
		MetricsListenAddr      *string         `json:"metrics_listen_addr"`
		SchedulerAddr          *string         `json:"scheduler_addr"`
		QueryOnlySchedulerAddr *string         `json:"query_only_scheduler_addr"`
		ResumeAddr             *string         `json:"resume_addr"`
		RestUpstreamAddr       *string         `json:"rest_upstream_addr"`
		RedisAddr              *string         `json:"redis_addr"`
		RequestTimeout         json.RawMessage `json:"request_timeout"`
		ForwardResponseSize    *int64          `json:"forward_response_size"`
		SandboxProxyDomains    *[]string       `json:"sandbox_proxy_domains"`
		DebugMode              *bool           `json:"debug_mode"`
		// Nested one pointer deep on each side, so a config file that names the
		// block without naming the key inside it leaves the default alone rather
		// than blanking it.
		Routing *struct {
			ExecutionFencing        *string `json:"execution_fencing"`
			ProjectionRead          *bool   `json:"projection_read"`
			ProjectionAuthoritative *bool   `json:"projection_authoritative"`
		} `json:"routing"`
		SchedulerFallbackDisabled *bool           `json:"scheduler_fallback_disabled"`
		SchedulerFallbackTimeout  json.RawMessage `json:"scheduler_fallback_timeout"`
	}

	parsed := wire{}
	if err := json.Unmarshal(data, &parsed); err != nil {
		return err
	}

	if parsed.HTTPListenAddr != nil {
		g.HTTPListenAddr = *parsed.HTTPListenAddr
	}
	if parsed.MetricsListenAddr != nil {
		g.MetricsListenAddr = *parsed.MetricsListenAddr
	}
	if parsed.SchedulerAddr != nil {
		g.SchedulerAddr = *parsed.SchedulerAddr
	}
	if parsed.QueryOnlySchedulerAddr != nil {
		g.QueryOnlySchedulerAddr = *parsed.QueryOnlySchedulerAddr
	}
	if parsed.ResumeAddr != nil {
		g.ResumeAddr = *parsed.ResumeAddr
	}
	if parsed.RestUpstreamAddr != nil {
		g.RestUpstreamAddr = *parsed.RestUpstreamAddr
	}
	if parsed.RedisAddr != nil {
		g.RedisAddr = *parsed.RedisAddr
	}
	if parsed.ForwardResponseSize != nil {
		g.ForwardResponseSize = *parsed.ForwardResponseSize
	}
	if parsed.SandboxProxyDomains != nil {
		g.SandboxProxyDomains = *parsed.SandboxProxyDomains
	}
	if parsed.DebugMode != nil {
		g.DebugMode = *parsed.DebugMode
	}
	if parsed.Routing != nil && parsed.Routing.ExecutionFencing != nil {
		// Carried through verbatim rather than parsed here: an unrecognised
		// value has to reach validate() and stop the process, and parsing at
		// this depth would turn it into an unmarshal error whose message names
		// the JSON rather than the setting.
		g.Routing.ExecutionFencing = GatewayExecutionFencing(*parsed.Routing.ExecutionFencing)
	}
	if parsed.Routing != nil && parsed.Routing.ProjectionRead != nil {
		g.Routing.ProjectionRead = *parsed.Routing.ProjectionRead
	}
	if parsed.Routing != nil && parsed.Routing.ProjectionAuthoritative != nil {
		g.Routing.ProjectionAuthoritative = *parsed.Routing.ProjectionAuthoritative
	}

	if len(bytes.TrimSpace(parsed.RequestTimeout)) > 0 {
		d, err := parseGatewayRequestTimeout(parsed.RequestTimeout)
		if err != nil {
			return err
		}
		g.RequestTimeout = d
	}
	if parsed.SchedulerFallbackDisabled != nil {
		g.SchedulerFallbackDisabled = *parsed.SchedulerFallbackDisabled
	}
	if len(bytes.TrimSpace(parsed.SchedulerFallbackTimeout)) > 0 {
		d, err := parseGatewayDuration("gateway.scheduler_fallback_timeout", parsed.SchedulerFallbackTimeout)
		if err != nil {
			return err
		}
		g.SchedulerFallbackTimeout = d
	}

	return nil
}

func parseGatewayRequestTimeout(raw json.RawMessage) (time.Duration, error) {
	return parseGatewayDuration("gateway.request_timeout", raw)
}

// parseGatewayDuration parses a gateway duration field carried through
// UnmarshalJSON as json.RawMessage, so a config file may write a bare
// duration string ("30s") and a numeric value is rejected with a message
// naming the field rather than becoming a silently-wrong nanosecond count.
func parseGatewayDuration(field string, raw json.RawMessage) (time.Duration, error) {
	var asString string
	if err := json.Unmarshal(raw, &asString); err == nil {
		d, parseErr := time.ParseDuration(strings.TrimSpace(asString))
		if parseErr != nil {
			return 0, fmt.Errorf("%s must be a duration string like \"30s\": %w", field, parseErr)
		}
		return d, nil
	}

	var asNumber json.Number
	if err := json.Unmarshal(raw, &asNumber); err == nil {
		return 0, fmt.Errorf("%s must be a duration string like \"30s\", got numeric value %s", field, asNumber.String())
	}

	return 0, fmt.Errorf("%s must be a duration string like \"30s\"", field)
}

type Config struct {
	Service   string          `json:"service"`
	LogLevel  string          `json:"log_level"`
	LogFormat string          `json:"log_format"`
	Scheduler SchedulerConfig `json:"scheduler"`
	Gateway   GatewayConfig   `json:"gateway"`
}

func Load(path string, service string) (Config, error) {
	return load(path, service, false)
}

func LoadScheduler(path string, queryOnly bool) (Config, error) {
	return load(path, "scheduler", queryOnly)
}

func load(path string, service string, schedulerQueryOnly bool) (Config, error) {
	cfg := defaultConfig(service)
	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return Config{}, fmt.Errorf("read config file: %w", err)
		}
		if err := json.Unmarshal(data, &cfg); err != nil {
			return Config{}, fmt.Errorf("unmarshal config json: %w", err)
		}
	}
	if err := overrideWithEnv(&cfg); err != nil {
		return Config{}, err
	}
	cfg.Service = service
	cfg.applyDefaults()
	if err := cfg.validate(schedulerQueryOnly); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func defaultConfig(service string) Config {
	return Config{
		Service:   service,
		LogLevel:  "info",
		LogFormat: "auto",
		Scheduler: SchedulerConfig{
			GRPCListenAddr:          ":9090",
			MetricsListenAddr:       ":9101",
			Strategy:                "round_robin",
			ReportTTL:               30 * time.Second,
			BindingTTL:              30 * time.Second,
			WarmupTimeout:           15 * time.Second,
			ArtifactStoreCapacity:   defaultSchedulerArtifactStoreCapacity,
			ArtifactLookupNodeLimit: 0,
			Nodes: []Node{
				{ID: "local-node", Endpoint: "http://127.0.0.1:8000"},
			},
			Discovery: SchedulerDiscoveryConfig{
				Mode: "static",
				Kubernetes: SchedulerDiscoveryKubernetesConfig{
					Scheme: "http",
				},
			},
			Registry: SchedulerRegistryConfig{
				MaxConnections:      defaultSchedulerRegistryMaxConnections,
				ReconcileInterval:   defaultSchedulerRegistryReconcileInterval,
				QueryTimeout:        defaultSchedulerRegistryQueryTimeout,
				LeaseWarnWindow:     defaultSchedulerRegistryLeaseWarnWindow,
				WriteMaxConnections: defaultSchedulerRegistryWriteMaxConnections,
				LeaseTTL:            defaultSchedulerRegistryLeaseTTL,
				LeaseTTLFloor:       defaultSchedulerRegistryLeaseTTLFloor,
				ReclaimInterval:     defaultSchedulerRegistryReclaimInterval,
				DiscardMaxRows:      defaultSchedulerRegistryDiscardMaxRows,
				DiscardMaxRatio:     defaultSchedulerRegistryDiscardMaxRatio,
				// 🔴 On by default. This is the half whose absence costs a
				// workspace, so a cluster that says nothing gets it; the
				// runbook's caution belongs in the runbook.
				WriteFencing: true,
			},
			// Same reasoning as the gateway's: the default names the end
			// state, and starting a release on observe is release discipline.
			// A default of observe leaves clusters parked there with nobody
			// aware they were never flipped.
			Catalog: SchedulerCatalogConfig{
				MaxConcurrentBuilds:        defaultSchedulerCatalogMaxConcurrentBuilds,
				BuildHeartbeatTTL:          defaultSchedulerCatalogBuildHeartbeatTTL,
				BuildReapInterval:          defaultSchedulerCatalogBuildReapInterval,
				NodeBuildHeartbeatInterval: defaultSchedulerCatalogNodeBuildHeartbeatInterval,
			},
			Routing:              SchedulerRoutingConfig{ExecutionArbitration: SchedulerExecutionArbitrationEnforce},
			MaxProjectionTTL:     defaultSchedulerMaxProjectionTTL,
			BindingSweepSilence:  defaultSchedulerBindingSweepSilence,
			BindingSweepInterval: defaultSchedulerBindingSweepInterval,
		},
		Gateway: GatewayConfig{
			HTTPListenAddr:           ":8080",
			MetricsListenAddr:        ":9102",
			SchedulerAddr:            "127.0.0.1:9090",
			RequestTimeout:           30 * time.Second,
			ForwardResponseSize:      4 << 20,
			SandboxProxyDomains:      []string{},
			SchedulerFallbackTimeout: defaultGatewaySchedulerFallbackTimeout,
			// The default points at the end state rather than at the cautious
			// first step. Starting a release on observe is release discipline,
			// which belongs in the runbook; putting it in the default leaves
			// clusters parked there with nobody aware they were never flipped.
			Routing: GatewayRoutingConfig{ExecutionFencing: GatewayExecutionFencingEnforce},
		},
	}
}

func overrideWithEnv(cfg *Config) error {
	set := func(key string, target *string) {
		if v := strings.TrimSpace(os.Getenv(key)); v != "" {
			*target = v
		}
	}
	set("LOG_LEVEL", &cfg.LogLevel)
	set("LOG_FORMAT", &cfg.LogFormat)
	set("SCHEDULER_GRPC_LISTEN_ADDR", &cfg.Scheduler.GRPCListenAddr)
	set("SCHEDULER_METRICS_LISTEN_ADDR", &cfg.Scheduler.MetricsListenAddr)
	set("SCHEDULER_STRATEGY", &cfg.Scheduler.Strategy)
	set("SCHEDULER_REDIS_ADDR", &cfg.Scheduler.RedisAddr)
	set("GATEWAY_HTTP_LISTEN_ADDR", &cfg.Gateway.HTTPListenAddr)
	set("GATEWAY_METRICS_LISTEN_ADDR", &cfg.Gateway.MetricsListenAddr)
	set("GATEWAY_SCHEDULER_ADDR", &cfg.Gateway.SchedulerAddr)
	set("GATEWAY_QUERY_ONLY_SCHEDULER_ADDR", &cfg.Gateway.QueryOnlySchedulerAddr)
	set("GATEWAY_RESUME_ADDR", &cfg.Gateway.ResumeAddr)
	set("GATEWAY_REST_UPSTREAM_ADDR", &cfg.Gateway.RestUpstreamAddr)
	set("GATEWAY_REDIS_ADDR", &cfg.Gateway.RedisAddr)
	// A shared secret, so it arrives the same way the DSN does and never through
	// the ConfigMap.
	set("GATEWAY_CONTROL_PLANE_TOKEN", &cfg.Gateway.ControlPlaneToken)
	// The DSN carries credentials, so it only ever arrives this way — never
	// through the config file, which is a ConfigMap.
	set("SCHEDULER_REGISTRY_DSN", &cfg.Scheduler.Registry.DSN)
	set("SCHEDULER_REGISTRY_CLUSTER_ID", &cfg.Scheduler.Registry.ClusterID)

	if v := strings.TrimSpace(os.Getenv("SCHEDULER_REGISTRY_WRITE_ENABLED")); v != "" {
		enabled, err := strconv.ParseBool(v)
		if err != nil {
			return fmt.Errorf("invalid SCHEDULER_REGISTRY_WRITE_ENABLED %q: %w", v, err)
		}
		cfg.Scheduler.Registry.WriteEnabled = enabled
	}

	if v := strings.TrimSpace(os.Getenv("SCHEDULER_REGISTRY_LEASE_TTL")); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("invalid SCHEDULER_REGISTRY_LEASE_TTL %q: %w", v, err)
		}
		cfg.Scheduler.Registry.LeaseTTL = d
	}

	if v := strings.TrimSpace(os.Getenv("SCHEDULER_REGISTRY_LEASE_TTL_FLOOR")); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("invalid SCHEDULER_REGISTRY_LEASE_TTL_FLOOR %q: %w", v, err)
		}
		cfg.Scheduler.Registry.LeaseTTLFloor = d
	}

	if v := strings.TrimSpace(os.Getenv("SCHEDULER_REGISTRY_RECLAIM_INTERVAL")); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("invalid SCHEDULER_REGISTRY_RECLAIM_INTERVAL %q: %w", v, err)
		}
		cfg.Scheduler.Registry.ReclaimInterval = d
	}

	// The catalog's build queue. Overridable from the environment for the
	// reason recon gives about the ConfigMap: a value edited in the file is
	// rolled back by the next apply, and the two numbers below are the ones an
	// operator reaches for during an incident.
	if v := strings.TrimSpace(os.Getenv("SCHEDULER_CATALOG_MAX_CONCURRENT_BUILDS")); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("invalid SCHEDULER_CATALOG_MAX_CONCURRENT_BUILDS %q: %w", v, err)
		}
		cfg.Scheduler.Catalog.MaxConcurrentBuilds = n
	}

	if v := strings.TrimSpace(os.Getenv("SCHEDULER_CATALOG_BUILD_HEARTBEAT_TTL")); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("invalid SCHEDULER_CATALOG_BUILD_HEARTBEAT_TTL %q: %w", v, err)
		}
		cfg.Scheduler.Catalog.BuildHeartbeatTTL = d
	}

	if v := strings.TrimSpace(os.Getenv("SCHEDULER_CATALOG_BUILD_REAP_INTERVAL")); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("invalid SCHEDULER_CATALOG_BUILD_REAP_INTERVAL %q: %w", v, err)
		}
		cfg.Scheduler.Catalog.BuildReapInterval = d
	}

	if v := strings.TrimSpace(os.Getenv("SCHEDULER_CATALOG_NODE_BUILD_HEARTBEAT_INTERVAL")); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("invalid SCHEDULER_CATALOG_NODE_BUILD_HEARTBEAT_INTERVAL %q: %w", v, err)
		}
		cfg.Scheduler.Catalog.NodeBuildHeartbeatInterval = d
	}

	if v := strings.TrimSpace(os.Getenv("GATEWAY_SANDBOX_PROXY_DOMAINS")); v != "" {
		cfg.Gateway.SandboxProxyDomains = splitCommaSeparated(v)
	}

	if v := strings.TrimSpace(os.Getenv("SCHEDULER_BINDING_TTL")); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("invalid SCHEDULER_BINDING_TTL %q: %w", v, err)
		}
		cfg.Scheduler.BindingTTL = d
	}

	if v := strings.TrimSpace(os.Getenv("SCHEDULER_WARMUP_TIMEOUT")); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("invalid SCHEDULER_WARMUP_TIMEOUT %q: %w", v, err)
		}
		cfg.Scheduler.WarmupTimeout = d
	}

	if v := strings.TrimSpace(os.Getenv("SCHEDULER_REGISTRY_RECONCILE_INTERVAL")); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("invalid SCHEDULER_REGISTRY_RECONCILE_INTERVAL %q: %w", v, err)
		}
		cfg.Scheduler.Registry.ReconcileInterval = d
	}

	if v := strings.TrimSpace(os.Getenv("SCHEDULER_ARTIFACT_STORE_CAPACITY")); v != "" {
		capacity, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("invalid SCHEDULER_ARTIFACT_STORE_CAPACITY %q: %w", v, err)
		}
		cfg.Scheduler.ArtifactStoreCapacity = capacity
	}

	if v := strings.TrimSpace(os.Getenv("SCHEDULER_ARTIFACT_LOOKUP_NODE_LIMIT")); v != "" {
		limit, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("invalid SCHEDULER_ARTIFACT_LOOKUP_NODE_LIMIT %q: %w", v, err)
		}
		cfg.Scheduler.ArtifactLookupNodeLimit = limit
	}

	if v := strings.TrimSpace(os.Getenv("GATEWAY_REQUEST_TIMEOUT")); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("invalid GATEWAY_REQUEST_TIMEOUT %q: %w", v, err)
		}
		cfg.Gateway.RequestTimeout = d
	}

	if v := strings.TrimSpace(os.Getenv("GATEWAY_DEBUG_MODE")); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return fmt.Errorf("invalid GATEWAY_DEBUG_MODE %q: %w", v, err)
		}
		cfg.Gateway.DebugMode = b
	}

	// 🔴 Phase 4's decommissioning lever — see GatewayConfig.
	// SchedulerFallbackDisabled — so it follows GATEWAY_DEBUG_MODE's shape
	// rather than the mounted-file pattern the projection switches use below:
	// this one is meant to be flipped by `kubectl set env` and a restart, the
	// same way ResumeAddr and RestUpstreamAddr are.
	if v := strings.TrimSpace(os.Getenv("GATEWAY_SCHEDULER_FALLBACK_DISABLED")); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return fmt.Errorf("invalid GATEWAY_SCHEDULER_FALLBACK_DISABLED %q: %w", v, err)
		}
		cfg.Gateway.SchedulerFallbackDisabled = b
	}

	if v := strings.TrimSpace(os.Getenv("GATEWAY_SCHEDULER_FALLBACK_TIMEOUT")); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("invalid GATEWAY_SCHEDULER_FALLBACK_TIMEOUT %q: %w", v, err)
		}
		cfg.Gateway.SchedulerFallbackTimeout = d
	}

	if v := strings.TrimSpace(os.Getenv("SCHEDULER_REGISTRY_WRITE_FENCING")); v != "" {
		fencing, err := strconv.ParseBool(v)
		if err != nil {
			return fmt.Errorf("invalid SCHEDULER_REGISTRY_WRITE_FENCING %q: %w", v, err)
		}
		cfg.Scheduler.Registry.WriteFencing = fencing
	}

	if v := strings.TrimSpace(os.Getenv("SCHEDULER_REGISTRY_HEARTBEAT_LEASE_RENEWAL")); v != "" {
		enabled, err := strconv.ParseBool(v)
		if err != nil {
			return fmt.Errorf("invalid SCHEDULER_REGISTRY_HEARTBEAT_LEASE_RENEWAL %q: %w", v, err)
		}
		cfg.Scheduler.Registry.HeartbeatLeaseRenewal = enabled
	}

	if v := strings.TrimSpace(os.Getenv("SCHEDULER_ROUTING_EXECUTION_ARBITRATION")); v != "" {
		mode, err := ParseSchedulerExecutionArbitration(v)
		if err != nil {
			return fmt.Errorf("invalid SCHEDULER_ROUTING_EXECUTION_ARBITRATION: %w", err)
		}
		cfg.Scheduler.Routing.ExecutionArbitration = mode
	}

	if v := strings.TrimSpace(os.Getenv("GATEWAY_ROUTING_EXECUTION_FENCING")); v != "" {
		mode, err := ParseGatewayExecutionFencing(v)
		if err != nil {
			return fmt.Errorf("invalid GATEWAY_ROUTING_EXECUTION_FENCING: %w", err)
		}
		cfg.Gateway.Routing.ExecutionFencing = mode
	}

	// 🔴 The three projection switches arrive as environment variables and
	// never as a mounted file. The two are not interchangeable here: a mounted
	// value that cannot be read is deliberately held at its last good value by
	// the consumer that reads one, and kubelet refreshes volumes minutes apart
	// and unevenly across nodes — so deleting a key and writing an empty string
	// have opposite effects and the failing side is silent. A configMapKeyRef
	// is read once at start-up: deleting the key falls back to the code
	// default, writing an empty string leaves the value alone, and neither
	// takes effect until the pod restarts. Flipping one is therefore `kubectl
	// set env`, which rolls the deployment and is loud.
	if v := strings.TrimSpace(os.Getenv("SCHEDULER_ROUTING_PROJECTION_AUTHORITATIVE")); v != "" {
		on, err := ParseRoutingProjectionSwitch(v)
		if err != nil {
			return fmt.Errorf("invalid SCHEDULER_ROUTING_PROJECTION_AUTHORITATIVE: %w", err)
		}
		cfg.Scheduler.Routing.ProjectionAuthoritative = on
	}

	if v := strings.TrimSpace(os.Getenv("GATEWAY_ROUTING_PROJECTION_READ")); v != "" {
		on, err := ParseRoutingProjectionSwitch(v)
		if err != nil {
			return fmt.Errorf("invalid GATEWAY_ROUTING_PROJECTION_READ: %w", err)
		}
		cfg.Gateway.Routing.ProjectionRead = on
	}

	if v := strings.TrimSpace(os.Getenv("GATEWAY_ROUTING_PROJECTION_AUTHORITATIVE")); v != "" {
		on, err := ParseRoutingProjectionSwitch(v)
		if err != nil {
			return fmt.Errorf("invalid GATEWAY_ROUTING_PROJECTION_AUTHORITATIVE: %w", err)
		}
		cfg.Gateway.Routing.ProjectionAuthoritative = on
	}

	if v := strings.TrimSpace(os.Getenv("SCHEDULER_ROUTING_BINDING_SWEEP")); v != "" {
		on, err := ParseRoutingProjectionSwitch(v)
		if err != nil {
			return fmt.Errorf("invalid SCHEDULER_ROUTING_BINDING_SWEEP: %w", err)
		}
		cfg.Scheduler.Routing.BindingSweep = on
	}

	if v := strings.TrimSpace(os.Getenv("SCHEDULER_BINDING_SWEEP_SILENCE")); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("invalid SCHEDULER_BINDING_SWEEP_SILENCE %q: %w", v, err)
		}
		cfg.Scheduler.BindingSweepSilence = d
	}

	if v := strings.TrimSpace(os.Getenv("SCHEDULER_BINDING_SWEEP_INTERVAL")); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("invalid SCHEDULER_BINDING_SWEEP_INTERVAL %q: %w", v, err)
		}
		cfg.Scheduler.BindingSweepInterval = d
	}

	if v := strings.TrimSpace(os.Getenv("SCHEDULER_MAX_PROJECTION_TTL")); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("invalid SCHEDULER_MAX_PROJECTION_TTL %q: %w", v, err)
		}
		cfg.Scheduler.MaxProjectionTTL = d
	}

	return nil
}

func splitCommaSeparated(raw string) []string {
	parts := strings.Split(raw, ",")
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			values = append(values, part)
		}
	}
	return values
}

func (c *Config) applyDefaults() {
	if strings.TrimSpace(c.Scheduler.MetricsListenAddr) == "" {
		c.Scheduler.MetricsListenAddr = ":9101"
	}
	if c.Scheduler.ReportTTL <= 0 {
		c.Scheduler.ReportTTL = 30 * time.Second
	}
	if c.Scheduler.BindingTTL <= 0 {
		c.Scheduler.BindingTTL = 30 * time.Second
	}
	if c.Scheduler.MaxProjectionTTL <= 0 {
		c.Scheduler.MaxProjectionTTL = defaultSchedulerMaxProjectionTTL
	}
	if c.Scheduler.Registry.MaxConnections <= 0 {
		c.Scheduler.Registry.MaxConnections = defaultSchedulerRegistryMaxConnections
	}
	if c.Scheduler.Registry.ReconcileInterval <= 0 {
		c.Scheduler.Registry.ReconcileInterval = defaultSchedulerRegistryReconcileInterval
	}
	if c.Scheduler.Registry.QueryTimeout <= 0 {
		c.Scheduler.Registry.QueryTimeout = defaultSchedulerRegistryQueryTimeout
	}
	if c.Scheduler.Registry.LeaseWarnWindow <= 0 {
		c.Scheduler.Registry.LeaseWarnWindow = defaultSchedulerRegistryLeaseWarnWindow
	}
	if c.Scheduler.Registry.WriteMaxConnections <= 0 {
		c.Scheduler.Registry.WriteMaxConnections = defaultSchedulerRegistryWriteMaxConnections
	}
	if c.Scheduler.Registry.LeaseTTL <= 0 {
		c.Scheduler.Registry.LeaseTTL = defaultSchedulerRegistryLeaseTTL
	}
	if c.Scheduler.Registry.LeaseTTLFloor <= 0 {
		c.Scheduler.Registry.LeaseTTLFloor = defaultSchedulerRegistryLeaseTTLFloor
	}
	if c.Scheduler.Registry.ReclaimInterval <= 0 {
		c.Scheduler.Registry.ReclaimInterval = defaultSchedulerRegistryReclaimInterval
	}
	if c.Scheduler.Registry.DiscardMaxRows <= 0 {
		c.Scheduler.Registry.DiscardMaxRows = defaultSchedulerRegistryDiscardMaxRows
	}
	if c.Scheduler.Registry.DiscardMaxRatio <= 0 {
		c.Scheduler.Registry.DiscardMaxRatio = defaultSchedulerRegistryDiscardMaxRatio
	}
	if strings.TrimSpace(c.Scheduler.Discovery.Mode) == "" {
		c.Scheduler.Discovery.Mode = "static"
	}
	if strings.TrimSpace(c.Scheduler.Discovery.Kubernetes.Scheme) == "" {
		c.Scheduler.Discovery.Kubernetes.Scheme = "http"
	}
	// An explicitly empty value in the config file means the same thing as the
	// key being absent. Anything else is left alone for validate() to refuse:
	// defaulting a value it could not read would be the silent fallback this
	// setting must not have.
	if strings.TrimSpace(string(c.Scheduler.Routing.ExecutionArbitration)) == "" {
		c.Scheduler.Routing.ExecutionArbitration = SchedulerExecutionArbitrationEnforce
	}
	if strings.TrimSpace(c.Gateway.MetricsListenAddr) == "" {
		c.Gateway.MetricsListenAddr = ":9102"
	}
	// An explicitly empty value in the config file means the same thing as the
	// key being absent. Anything else is left alone for validate() to refuse:
	// defaulting a value it could not read would be the silent fallback this
	// setting must not have.
	if strings.TrimSpace(string(c.Gateway.Routing.ExecutionFencing)) == "" {
		c.Gateway.Routing.ExecutionFencing = GatewayExecutionFencingEnforce
	}
}

// validateSchedulerRegistry checks the registry block only when it is switched
// on. An empty DSN is the default and always valid: the feature is off and
// every other field is irrelevant.
func validateSchedulerRegistry(registry SchedulerRegistryConfig) error {
	if strings.TrimSpace(registry.DSN) == "" {
		return nil
	}
	if registry.MaxConnections <= 0 {
		return errors.New("scheduler.registry.max_connections must be greater than zero")
	}
	if registry.ReconcileInterval <= 0 {
		return errors.New("scheduler.registry.reconcile_interval must be greater than zero")
	}
	if registry.QueryTimeout <= 0 {
		return errors.New("scheduler.registry.query_timeout must be greater than zero")
	}
	if registry.LeaseWarnWindow <= 0 {
		return errors.New("scheduler.registry.lease_warn_window must be greater than zero")
	}
	// The column is a uuid, so a cluster id that is not one makes every query
	// fail forever. Catching it here turns a permanently silent read failure
	// into a start-up error.
	if clusterID := strings.TrimSpace(registry.ClusterID); clusterID != "" && !looksLikeUUID(clusterID) {
		return fmt.Errorf("scheduler.registry.cluster_id must be a uuid, got %q", clusterID)
	}

	if !registry.WriteEnabled {
		return nil
	}

	// 🔴 A missing cluster id is *not* rejected here, and the asymmetry is
	// deliberate.
	//
	// The write surface does need one — reclaiming every cluster in a shared
	// database deletes rows this controller was never given, and the restart
	// grace pass would extend one cluster's leases while serving another's
	// writes with none of theirs extended. But the cluster id arrives from an
	// optional Secret key, so "missing" is a thing that happens on a rollout,
	// and rejecting it here means the scheduler will not start at all: routing,
	// discovery and bindings would all stop over a registry that was switched
	// on last week. The write surface is left cold instead, loudly, and the
	// rest of the process carries on (see openRegistryWriteSurface).
	//
	// The checks below are different in kind: every one of them has a default,
	// so reaching an invalid value takes somebody explicitly writing one, and
	// no absent Secret can produce it.
	if registry.WriteMaxConnections <= 0 {
		return errors.New("scheduler.registry.write_max_connections must be greater than zero")
	}
	if registry.LeaseTTL <= 0 {
		return errors.New("scheduler.registry.lease_ttl must be greater than zero")
	}
	if registry.LeaseTTLFloor <= 0 {
		return errors.New("scheduler.registry.lease_ttl_floor must be greater than zero")
	}
	if registry.ReclaimInterval <= 0 {
		return errors.New("scheduler.registry.reclaim_interval must be greater than zero")
	}
	if registry.DiscardMaxRows <= 0 {
		return errors.New("scheduler.registry.discard_max_rows must be greater than zero")
	}
	if registry.DiscardMaxRatio <= 0 || registry.DiscardMaxRatio > 1 {
		return fmt.Errorf("scheduler.registry.discard_max_ratio must be in (0, 1], got %v", registry.DiscardMaxRatio)
	}
	return nil
}

// looksLikeUUID reports whether s has the canonical 8-4-4-4-12 hexadecimal
// shape. It is a shape check, not a version check: the registry only needs the
// value to survive a cast to uuid.
func looksLikeUUID(s string) bool {
	groups := strings.Split(s, "-")
	if len(groups) != 5 {
		return false
	}
	for i, want := range []int{8, 4, 4, 4, 12} {
		if len(groups[i]) != want {
			return false
		}
		for _, r := range groups[i] {
			switch {
			case r >= '0' && r <= '9':
			case r >= 'a' && r <= 'f':
			case r >= 'A' && r <= 'F':
			default:
				return false
			}
		}
	}
	return true
}

func (c Config) Validate() error {
	return c.validate(false)
}

func (c Config) validate(schedulerQueryOnly bool) error {
	if c.Service == "" {
		return errors.New("service is required")
	}
	if c.LogLevel == "" {
		return errors.New("log_level is required")
	}
	if c.LogFormat == "" {
		return errors.New("log_format is required")
	}
	switch strings.ToLower(c.LogFormat) {
	case "auto", "console", "json":
	default:
		return errors.New("log_format must be one of auto, console, json")
	}
	if c.Service == "scheduler" {
		if c.Scheduler.GRPCListenAddr == "" {
			return errors.New("scheduler.grpc_listen_addr is required")
		}
		if c.Scheduler.MetricsListenAddr == "" {
			return errors.New("scheduler.metrics_listen_addr is required")
		}
		if c.Scheduler.ReportTTL <= 0 {
			return errors.New("scheduler.report_ttl must be greater than zero")
		}
		if c.Scheduler.BindingTTL <= 0 {
			return errors.New("scheduler.binding_ttl must be greater than zero")
		}
		// Refused rather than defaulted, and refused on the query-only replica
		// too: that replica is the one serving data-plane lookups, so a typo
		// there is a typo on the path that matters most.
		if _, err := ParseSchedulerExecutionArbitration(string(c.Scheduler.Routing.ExecutionArbitration)); err != nil {
			return err
		}
		// Checked before the query-only early return: a query-only replica is
		// given the same registry reader, so a bad registry config has to fail
		// there too rather than only on the primary.
		if err := validateSchedulerRegistry(c.Scheduler.Registry); err != nil {
			return err
		}
		// Before the query-only early return as well: a replica is given the
		// same config, and a TTL below the floor is a typo somebody should be
		// told about wherever it is read.
		if err := validateSchedulerCatalog(c.Scheduler.Catalog); err != nil {
			return err
		}
		if schedulerQueryOnly {
			if strings.TrimSpace(c.Scheduler.RedisAddr) == "" {
				return errors.New("scheduler --query-only requires scheduler.redis_addr")
			}
			return nil
		}
		if c.Scheduler.ArtifactStoreCapacity <= 0 {
			return errors.New("scheduler.artifact_store_capacity must be greater than zero")
		}
		switch strings.ToLower(strings.TrimSpace(c.Scheduler.Discovery.Mode)) {
		case "static":
			if len(c.Scheduler.Nodes) == 0 {
				return errors.New("scheduler.nodes must not be empty")
			}
			for _, n := range c.Scheduler.Nodes {
				if n.ID == "" || n.Endpoint == "" {
					return errors.New("scheduler.nodes require id and endpoint")
				}
			}
		case "kubernetes":
			kube := c.Scheduler.Discovery.Kubernetes
			if strings.TrimSpace(kube.Namespace) == "" {
				return errors.New("scheduler.discovery.kubernetes.namespace is required")
			}
			if strings.TrimSpace(kube.ServiceName) == "" {
				return errors.New("scheduler.discovery.kubernetes.service_name is required")
			}
			if kube.Port <= 0 {
				return errors.New("scheduler.discovery.kubernetes.port must be greater than zero")
			}
			if strings.TrimSpace(kube.Scheme) == "" {
				return errors.New("scheduler.discovery.kubernetes.scheme is required")
			}
		default:
			return errors.New("scheduler.discovery.mode must be one of static, kubernetes")
		}
	}
	if c.Service == "gateway" {
		if c.Gateway.HTTPListenAddr == "" {
			return errors.New("gateway.http_listen_addr is required")
		}
		if c.Gateway.MetricsListenAddr == "" {
			return errors.New("gateway.metrics_listen_addr is required")
		}
		if c.Gateway.SchedulerAddr == "" {
			return errors.New("gateway.scheduler_addr is required")
		}
		if _, err := ParseGatewayExecutionFencing(string(c.Gateway.Routing.ExecutionFencing)); err != nil {
			return err
		}
		// 🔴 Refused rather than quietly ignored. A read switch with nowhere to
		// read from is a switch that reports as on and does nothing, and the
		// symptom — every request still going to the scheduler — is exactly
		// what the switch being off looks like.
		if c.Gateway.Routing.ProjectionRead && strings.TrimSpace(c.Gateway.RedisAddr) == "" {
			return errors.New("gateway.routing.projection_read requires gateway.redis_addr")
		}
		// Same reasoning, one step earlier: a REST upstream nobody can parse
		// stops the process here rather than turning into a 502 per request.
		if _, err := ParseRestUpstream(c.Gateway.RestUpstreamAddr); err != nil {
			return err
		}
		// 🔴 阶段 3a no longer has an "off" position for either of its two
		// addresses. Nodes run aenv-node now and answer 404 on every
		// user-facing REST route and have no wake-up surface of their own, so
		// an empty rest_upstream_addr or resume_addr is not a rollback — it is
		// an outage with no matching half, and refusing it here turns that into
		// a startup failure instead of a 404/502 discovered per request. See
		// rest_upstream.go and TestTheRestUpstreamIsAlwaysSetBecauseNodesNeverServeRest.
		if strings.TrimSpace(c.Gateway.RestUpstreamAddr) == "" {
			return errors.New("gateway.rest_upstream_addr is required: aenv-node answers 404 on " +
				"user-facing REST, so the gateway has nowhere else to send it")
		}
		if strings.TrimSpace(c.Gateway.ResumeAddr) == "" {
			return errors.New("gateway.resume_addr is required: aenv-node has no wake-up surface " +
				"of its own, so the gateway has nowhere else to ask a paused sandbox to be woken")
		}
	}
	return nil
}
