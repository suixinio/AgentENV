package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
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
}

func (s *SchedulerRegistryConfig) UnmarshalJSON(data []byte) error {
	type wire struct {
		DSN               *string         `json:"dsn"`
		ClusterID         *string         `json:"cluster_id"`
		MaxConnections    *int32          `json:"max_connections"`
		ReconcileInterval json.RawMessage `json:"reconcile_interval"`
		QueryTimeout      json.RawMessage `json:"query_timeout"`
		LeaseWarnWindow   json.RawMessage `json:"lease_warn_window"`

		WriteEnabled        *bool           `json:"write_enabled"`
		WriteFencing        *bool           `json:"write_fencing"`
		WriteMaxConnections *int32          `json:"write_max_connections"`
		LeaseTTL            json.RawMessage `json:"lease_ttl"`
		LeaseTTLFloor       json.RawMessage `json:"lease_ttl_floor"`
		ReclaimInterval     json.RawMessage `json:"reclaim_interval"`
		DiscardMaxRows      *int64          `json:"discard_max_rows"`
		DiscardMaxRatio     *float64        `json:"discard_max_ratio"`
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
	ArtifactStoreCapacity   int                      `json:"artifact_store_capacity"`
	ArtifactLookupNodeLimit int                      `json:"artifact_lookup_node_limit"`
	Nodes                   []Node                   `json:"nodes"`
	Discovery               SchedulerDiscoveryConfig `json:"discovery"`
	NodeResourceLimit       *NodeResourceLimit       `json:"node_resource_limit"`
	Registry                SchedulerRegistryConfig  `json:"registry"`
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
		ArtifactStoreCapacity   *int                      `json:"artifact_store_capacity"`
		ArtifactLookupNodeLimit *int                      `json:"artifact_lookup_node_limit"`
		Nodes                   *[]Node                   `json:"nodes"`
		Discovery               *SchedulerDiscoveryConfig `json:"discovery"`
		NodeResourceLimit       *NodeResourceLimit        `json:"node_resource_limit"`
		// Decoded into the existing value rather than through a pointer,
		// so a config that names only one registry key keeps the defaults
		// for the others instead of zeroing them.
		Registry json.RawMessage `json:"registry"`
		// Nested one pointer deep on each side, so a config file that names
		// the block without naming the key inside it leaves the default alone
		// rather than blanking it.
		Routing *struct {
			ExecutionArbitration    *string `json:"execution_arbitration"`
			ProjectionAuthoritative *bool   `json:"projection_authoritative"`
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

	if len(bytes.TrimSpace(parsed.MaxProjectionTTL)) > 0 {
		d, err := parseSchedulerDuration(parsed.MaxProjectionTTL, "scheduler.max_projection_ttl")
		if err != nil {
			return err
		}
		s.MaxProjectionTTL = d
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
}

func (g *GatewayConfig) UnmarshalJSON(data []byte) error {
	type wire struct {
		HTTPListenAddr         *string         `json:"http_listen_addr"`
		MetricsListenAddr      *string         `json:"metrics_listen_addr"`
		SchedulerAddr          *string         `json:"scheduler_addr"`
		QueryOnlySchedulerAddr *string         `json:"query_only_scheduler_addr"`
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

	return nil
}

func parseGatewayRequestTimeout(raw json.RawMessage) (time.Duration, error) {
	var asString string
	if err := json.Unmarshal(raw, &asString); err == nil {
		d, parseErr := time.ParseDuration(strings.TrimSpace(asString))
		if parseErr != nil {
			return 0, fmt.Errorf("gateway.request_timeout must be a duration string like \"30s\": %w", parseErr)
		}
		return d, nil
	}

	var asNumber json.Number
	if err := json.Unmarshal(raw, &asNumber); err == nil {
		return 0, fmt.Errorf("gateway.request_timeout must be a duration string like \"30s\", got numeric value %s", asNumber.String())
	}

	return 0, errors.New("gateway.request_timeout must be a duration string like \"30s\"")
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
			Routing:          SchedulerRoutingConfig{ExecutionArbitration: SchedulerExecutionArbitrationEnforce},
			MaxProjectionTTL: defaultSchedulerMaxProjectionTTL,
		},
		Gateway: GatewayConfig{
			HTTPListenAddr:      ":8080",
			MetricsListenAddr:   ":9102",
			SchedulerAddr:       "127.0.0.1:9090",
			RequestTimeout:      30 * time.Second,
			ForwardResponseSize: 4 << 20,
			SandboxProxyDomains: []string{},
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

	if v := strings.TrimSpace(os.Getenv("SCHEDULER_REGISTRY_WRITE_FENCING")); v != "" {
		fencing, err := strconv.ParseBool(v)
		if err != nil {
			return fmt.Errorf("invalid SCHEDULER_REGISTRY_WRITE_FENCING %q: %w", v, err)
		}
		cfg.Scheduler.Registry.WriteFencing = fencing
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
	}
	return nil
}
