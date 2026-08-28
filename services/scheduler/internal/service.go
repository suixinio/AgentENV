package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	schedulerv1 "agentenv/services/api/proto"
	pausedregistry "agentenv/services/scheduler/internal/registry"
	"agentenv/services/shared/config"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Service struct {
	schedulerv1.UnimplementedSchedulerServer
	logger        *zap.Logger
	nodes         NodeRegistry
	strategy      Strategy
	store         BindingStore
	artifacts     ArtifactStore
	resourceLimit *config.NodeResourceLimit
	warmup        *warmupGate
	// registry is the read-only view of the node-owned paused registry. Never
	// nil: an unconfigured scheduler gets the disabled reader, which answers
	// ErrDisabled rather than an empty result, so "switched off" can never be
	// mistaken for "the table is empty".
	registry pausedregistry.Reader
	// reportTTL is how long a heartbeat stays current. It decides both the
	// roster freshness the shadow reconciliation judges against and the
	// freshness a lookup requires before routing to a node the registry names,
	// which is why it is one value rather than two.
	reportTTL               time.Duration
	registryLeaseWarnWindow time.Duration
	// silentExecution blanks the two incarnation fields in every lookup
	// answer. Set only in the rollback mode, where a response from this build
	// has to be indistinguishable from one before it.
	silentExecution bool
	// projectionAuthoritative is the write-side switch: whether the routing
	// projection is a record with its own lifetime, or the 30-second cache it
	// has always been.
	//
	// 🔴 It defaults to false, unlike the three incarnation switches next to
	// it, and the difference is not timidity. Turning it on makes
	// ReportSandboxEvent — an RPC every node in the fleet is already sending —
	// stop being a guaranteed no-op and start deleting records. A cluster that
	// upgrades this binary without setting anything would acquire that
	// behaviour at the moment a pod restarted, having asked for nothing.
	projectionAuthoritative bool
	// maxProjectionTTL caps what a node may ask this scheduler to store.
	//
	// 🔴 A storage owner's limit on writers, not a second source of truth for
	// how long a sandbox lives. The definition stays on the node; a second copy
	// of it in a ConfigMap would drift, and the drift would show up as records
	// expiring before their sandboxes — indistinguishable from a cold cache.
	maxProjectionTTL time.Duration
	// bindingSweep is the switch over the heartbeat-timeout sweep: whether a
	// node that stops heartbeating has the routing records it installed
	// retired, or keeps them until their own TTL lapses.
	//
	// 🔴 Off by default and independent of projectionAuthoritative. See the
	// argument on config.SchedulerRoutingConfig.BindingSweep, and the one at
	// the top of sweep.go for what the sweep is and is not.
	bindingSweep bool
	// bindingSweepSilence is how long a node may say nothing first.
	bindingSweepSilence time.Duration
	// registryWriter is the narrow write surface the heartbeat-driven lease
	// renewal uses, nil unless WithHeartbeatLeaseRenewal wired one in. Every
	// other reconciliation path — including this one when the switch below is
	// off — never touches it.
	//
	// HeartbeatLeaseRenewer, not the narrower ParkedLeaseRenewer: this one
	// write surface backs both renewParkedLeasesFromHeartbeats (publishing/
	// local_only) and renewLiveLeasesFromHeartbeats (running) — see
	// RenewLiveLeases' own doc for why the second exists.
	registryWriter pausedregistry.HeartbeatLeaseRenewer
	// registryGrace gates registryWriter through the write surface's own
	// restart grace window. See renewParkedLeasesFromHeartbeats and
	// renewLiveLeasesFromHeartbeats.
	registryGrace registryGraceGate
	// heartbeatLeaseRenewal is the switch itself.
	//
	// 🔴 Off by default, unlike registry write_fencing: this is a new write
	// path added to a table two deployments already share, not a rollback for
	// one that shipped on. See WithHeartbeatLeaseRenewal.
	heartbeatLeaseRenewal bool
}

func NewService(logger *zap.Logger, nodes NodeRegistry, strategy Strategy, store BindingStore, opts ...ServiceOption) *Service {
	if logger == nil {
		logger = zap.NewNop()
	}
	if nodes == nil {
		nodes = NewAtomicNodeRegistry(nil, defaultObservedReportTTL)
	}
	s := &Service{
		logger:                  logger,
		nodes:                   nodes,
		strategy:                strategy,
		store:                   store,
		artifacts:               NewInMemoryArtifactStore(defaultArtifactStoreCapacity, 0),
		registry:                pausedregistry.Disabled(),
		reportTTL:               defaultObservedReportTTL,
		registryLeaseWarnWindow: defaultRegistryLeaseWarnWindow,
		maxProjectionTTL:        defaultMaxProjectionTTL,
		bindingSweepSilence:     defaultBindingSweepSilence,
	}
	for _, opt := range opts {
		opt(s)
	}
	if s.warmup == nil {
		s.warmup = newWarmupGate(nodes, defaultWarmupTimeout, time.Now())
	}
	return s
}

// WithWarmupTimeout bounds how long a freshly started scheduler withholds
// "sandbox assignment not found" while its bindings are still being seeded by
// node heartbeats.
func WithWarmupTimeout(timeout time.Duration) ServiceOption {
	return func(s *Service) {
		s.warmup = newWarmupGate(s.nodes, timeout, time.Now())
	}
}

// ServiceOption configures optional Service behaviour.
type ServiceOption func(*Service)

// WithNodeResourceLimit sets per-node resource thresholds for scheduling.
func WithNodeResourceLimit(limit *config.NodeResourceLimit) ServiceOption {
	return func(s *Service) {
		s.resourceLimit = limit
	}
}

func WithArtifactStore(store ArtifactStore) ServiceOption {
	return func(s *Service) {
		s.artifacts = store
	}
}

// WithSilentExecutionAxis makes lookups answer with the two incarnation fields
// left at their zero values.
//
// 🔴 Paired with the binding store's own arbiter, never on its own: the
// rollback has to turn off the arbitration *and* the answer, because a caller
// acting on an incarnation this scheduler did not arbitrate is worse than one
// acting on none.
func WithSilentExecutionAxis() ServiceOption {
	return func(s *Service) {
		s.silentExecution = true
	}
}

// WithAuthoritativeProjection turns on the write side of the routing
// projection: node-supplied TTLs are honoured, and pause/delete events remove
// records instead of being logged and dropped.
//
// 🔴 Opt-in. See the note on Service.projectionAuthoritative for why this one
// switch defaults the other way from the three beside it.
func WithAuthoritativeProjection(maxProjectionTTL time.Duration) ServiceOption {
	return func(s *Service) {
		s.projectionAuthoritative = true
		if maxProjectionTTL > 0 {
			s.maxProjectionTTL = maxProjectionTTL
		}
	}
}

// WithBindingSweep turns on the heartbeat-timeout sweep, with the silence a
// node is allowed before the records it installed are retired.
//
// 🔴 A non-positive silence takes the default rather than meaning "immediately".
// Zero arriving here is an unset config field, and the reading of it that costs
// something is the one where every node that has not reported in this instant
// loses its routing.
func WithBindingSweep(silence time.Duration) ServiceOption {
	return func(s *Service) {
		s.bindingSweep = true
		if silence > 0 {
			s.bindingSweepSilence = silence
		}
	}
}

// WithPausedRegistry installs the read-only paused-registry reader, together
// with the two intervals the shadow reconciliation judges rosters and leases
// against.
func WithPausedRegistry(reader pausedregistry.Reader, reportTTL time.Duration, leaseWarnWindow time.Duration) ServiceOption {
	return func(s *Service) {
		if reader == nil {
			reader = pausedregistry.Disabled()
		}
		s.registry = reader
		if reportTTL > 0 {
			s.reportTTL = reportTTL
		}
		if leaseWarnWindow > 0 {
			s.registryLeaseWarnWindow = leaseWarnWindow
		}
	}
}

// WithHeartbeatLeaseRenewal turns on the scheduler-driven renewal of
// publishing/local_only/running leases: each reconcile round, a row whose
// holder's own fresh heartbeat roster still lists the sandbox has
// lease_expires_at pushed out directly by this process.
//
// 🔴 Why this exists at all. The api half's own renewal call
// (renew_paused_leases, `src/api/impls/paused_recovery.rs`) renews under its
// own process identity, which for these three states is no longer the row's
// holder now that origin_node_id names the machine that actually holds the
// bytes rather than the api replica that wrote the row — see the commit that
// changed that. This round already has both facts this needs — the registry
// row and a fresh roster naming the same sandbox — so acting on them here is
// authority actually held by the party asserting it: the node's own
// heartbeat, which it cannot forge, rather than a caller renewing on a row it
// does not hold. See computeRegistryReconcile's parkedLeaseRenewals and
// liveLeaseRenewals for the exact eligibility rules.
//
// 🔴 `running` shares this one switch with publishing/local_only rather than
// getting its own: both are the same feature — "keep alive whatever this
// node's own heartbeat vouches for" — and a deployment with a reason to want
// one half without the other has none yet. See renewLiveLeasesFromHeartbeats.
//
// 🔴 Off by default, unlike scheduler.registry.write_fencing. That switch
// defaults on because it is the rollback for a predicate that already shipped
// serving every deployment; this one is a new write path added to a table
// both the EKS and the pve-sg trees still share, and turning it on for either
// without the other agreeing is exactly the zero-behaviour-change guarantee
// this switch exists to preserve until both sides are ready.
//
// writer nil disables the option outright, mirroring WithPausedRegistry —
// there is nothing to gate if there is nothing to write through. grace is
// accepted separately, and may be nil: registryGraceGate's one implementation
// today, *pausedregistry.Grace, tolerates a nil receiver by treating it as
// always serving, and any other implementation is asked to keep that
// property (see registryGraceGate's own doc).
func WithHeartbeatLeaseRenewal(writer pausedregistry.HeartbeatLeaseRenewer, grace registryGraceGate, enabled bool) ServiceOption {
	return func(s *Service) {
		if writer == nil {
			return
		}
		s.registryWriter = writer
		s.registryGrace = grace
		s.heartbeatLeaseRenewal = enabled
	}
}

type QueryOnlyService struct {
	schedulerv1.UnimplementedSchedulerServer
	logger          *zap.Logger
	store           BindingStore
	registry        pausedregistry.Reader
	silentExecution bool
}

// QueryOnlyServiceOption configures optional QueryOnlyService behaviour.
type QueryOnlyServiceOption func(*QueryOnlyService)

// WithQueryOnlyPausedRegistry gives a query-only replica the same read-only
// registry reader as the primary.
//
// 🔴 This is not optional in practice. A gateway configured with
// gateway.query_only_scheduler_addr sends every sandbox data-plane lookup to
// this replica, so a registry wired only into the primary would be consulted by
// nothing that matters — and the deployment where that shows up is not the one
// this is developed against.
func WithQueryOnlyPausedRegistry(reader pausedregistry.Reader) QueryOnlyServiceOption {
	return func(s *QueryOnlyService) {
		if reader == nil {
			reader = pausedregistry.Disabled()
		}
		s.registry = reader
	}
}

// WithQueryOnlySilentExecutionAxis is WithSilentExecutionAxis for the replica.
//
// 🔴 It has to exist separately. The replica is the one serving data-plane
// lookups, so a rollback applied only to the primary would leave the answers
// that matter unchanged.
func WithQueryOnlySilentExecutionAxis() QueryOnlyServiceOption {
	return func(s *QueryOnlyService) {
		s.silentExecution = true
	}
}

func NewQueryOnlyService(logger *zap.Logger, store BindingStore, opts ...QueryOnlyServiceOption) *QueryOnlyService {
	if logger == nil {
		logger = zap.NewNop()
	}
	s := &QueryOnlyService{logger: logger, store: store, registry: pausedregistry.Disabled()}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func (s *QueryOnlyService) ListRegistrySandboxes(ctx context.Context, req *schedulerv1.ListRegistrySandboxesRequest) (*schedulerv1.ListRegistrySandboxesResponse, error) {
	return listRegistrySandboxes(ctx, s.logger, s.registry, nil, req)
}

func (s *QueryOnlyService) LookupNode(ctx context.Context, req *schedulerv1.LookupNodeRequest) (*schedulerv1.LookupNodeResponse, error) {
	// No warm-up gate: this mode requires Redis, so its bindings outlive the
	// replica's own restart and a miss is a real miss rather than a cold cache.
	//
	// No placer either. This replica runs no discovery and receives no
	// heartbeats, so it cannot tell a draining node from a healthy one and has
	// no way to resolve a node id to an endpoint. It still consults the
	// registry, because the answer that matters most here — "unreadable, so
	// this is not a 404" — needs nothing but the reader.
	return lookupNode(ctx, lookupDeps{
		logger:          s.logger,
		store:           s.store,
		registry:        s.registry,
		silentExecution: s.silentExecution,
	}, req)
}

func (s *Service) Schedule(_ context.Context, req *schedulerv1.ScheduleRequest) (resp *schedulerv1.ScheduleResponse, err error) {
	start := time.Now()
	defer func() {
		recordSchedulerSchedule(s.strategy.Name(), start, err)
	}()

	// A Schedule call is for a sandbox that does not exist yet, so there is no
	// node it would rather be on.
	result, selectErr := s.selectNode(req.GetHint(), "")
	if selectErr != nil {
		if errors.Is(selectErr, ErrNoNodes) {
			err = status.Error(codes.Unavailable, "no nodes available")
			return nil, err
		}
		err = status.Error(codes.Internal, selectErr.Error())
		return nil, err
	}
	return &schedulerv1.ScheduleResponse{Node: result.node.Node.ToProto()}, nil
}

// placement is one selection, together with the candidate counts the decision
// was taken over. The counts travel with the node because they are the only
// thing that explains a selection after the fact.
type placement struct {
	node       RichNode
	candidates int
	eligible   int
}

// selectNode runs the placement pipeline.
//
// preferNodeID is a soft affinity, not a constraint: a node that survives every
// filter is taken as-is, and one that does not is simply forgotten. It exists
// for the sandbox that is being rebuilt from a snapshot — the machine that
// paused it still has the layers on disk, and pulling them again from object
// storage instead is the single largest avoidable cost in a cross-node resume.
//
// It is applied after filtering, never before: a preference that could bring
// back a node the filters removed would let an isolated or overloaded node be
// selected by the one path that never asked the strategy.
func (s *Service) selectNode(hint *schedulerv1.ScheduleRequestHint, preferNodeID string) (placement, error) {
	discovered := s.nodes.Snapshot( /* allowLingering */ false)
	rich := make([]RichNode, 0, len(discovered))
	for _, n := range discovered {
		rich = append(rich, RichNode{
			Node:     n,
			Snapshot: s.nodes.PeekObserved(n.ID),
		})
	}

	// Discovery already dropped lingering nodes; this drops the ones that
	// reported themselves isolated in their own heartbeat.
	eligible := FilterByResourceLimit(FilterUnschedulable(rich), s.resourceLimit)

	// Resolve the preference the way every other node identity is resolved: a
	// registry row was written by the node itself and may name the identity it
	// reported under before a fleet upgrade renamed it.
	preferNodeID = strings.TrimSpace(preferNodeID)
	if preferNodeID != "" {
		if resolved, ok := s.nodes.Resolve(preferNodeID); ok {
			preferNodeID = resolved.ID
		}
		for _, candidate := range eligible {
			if candidate.ID != preferNodeID {
				continue
			}
			s.logger.Debug("scheduler selected the preferred node",
				zap.String("node_id", candidate.ID),
				zap.String("endpoint", candidate.Endpoint),
				zap.Int("candidate_nodes", len(rich)),
				zap.Int("eligible_nodes", len(eligible)),
			)
			return placement{node: candidate, candidates: len(rich), eligible: len(eligible)}, nil
		}
	}

	node, err := s.strategy.Select(eligible, hint)
	if err != nil {
		s.logger.Debug("scheduler selection failed",
			zap.String("strategy", s.strategy.Name()),
			zap.String("hint", summarizeScheduleHint(hint)),
			zap.String("prefer_node_id", preferNodeID),
			zap.Int("candidate_nodes", len(rich)),
			zap.Int("eligible_nodes", len(eligible)),
			zap.Error(err),
		)
		return placement{}, err
	}
	s.logger.Debug("scheduler selected node",
		zap.String("strategy", s.strategy.Name()),
		zap.String("hint", summarizeScheduleHint(hint)),
		zap.String("prefer_node_id", preferNodeID),
		zap.String("node_id", node.ID),
		zap.String("endpoint", node.Endpoint),
		zap.Int("candidate_nodes", len(rich)),
		zap.Int("eligible_nodes", len(eligible)),
	)
	return placement{node: node, candidates: len(rich), eligible: len(eligible)}, nil
}

// summarizeScheduleHint renders a compact, log-friendly description of a
// scheduling hint.
func summarizeScheduleHint(hint *schedulerv1.ScheduleRequestHint) string {
	switch k := hint.GetKind().(type) {
	case *schedulerv1.ScheduleRequestHint_NewColdSandbox:
		c := k.NewColdSandbox
		return fmt.Sprintf("new_cold_sandbox cpu=%d memory_mb=%d images=%v", c.GetCpuCount(), c.GetMemoryMb(), c.GetImages())
	case *schedulerv1.ScheduleRequestHint_NewSandbox:
		return "new_sandbox"
	default:
		return "none"
	}
}

func (s *Service) ListNodes(_ context.Context, _ *schedulerv1.ListNodesRequest) (*schedulerv1.ListNodesResponse, error) {
	snapshot := s.nodes.Snapshot( /* allowLingering */ true)
	nodes := make([]*schedulerv1.Node, 0, len(snapshot))
	for _, node := range snapshot {
		nodes = append(nodes, node.ToProto())
	}

	s.logger.Debug("scheduler listed nodes", zap.Int("node_count", len(nodes)))

	return &schedulerv1.ListNodesResponse{Nodes: nodes}, nil
}

func (s *Service) LookupNode(ctx context.Context, req *schedulerv1.LookupNodeRequest) (*schedulerv1.LookupNodeResponse, error) {
	return lookupNode(ctx, lookupDeps{
		logger:          s.logger,
		store:           s.store,
		registry:        s.registry,
		warmup:          s.warmup,
		placer:          s,
		silentExecution: s.silentExecution,
	}, req)
}

func (s *Service) RecordAssignment(_ context.Context, req *schedulerv1.RecordAssignmentRequest) (*schedulerv1.RecordAssignmentResponse, error) {
	if strings.TrimSpace(req.GetSandboxId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "sandbox_id is required")
	}
	node := NodeFromProto(req.GetNode())
	if strings.TrimSpace(node.ID) == "" || strings.TrimSpace(node.Endpoint) == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id and endpoint are required")
	}
	if known, ok := s.nodes.Resolve(node.ID); ok {
		// Heartbeats reconcile bindings under the node's current identity; a
		// binding written under a previous one would be dropped by the next
		// heartbeat as belonging to nobody.
		node.ID = known.ID
	}
	if !s.isKnownNode(node) {
		s.logger.Warn("scheduler rejected assignment for unknown node",
			zap.String("sandbox_id", req.GetSandboxId()),
			zap.String("node_id", node.ID),
			zap.String("endpoint", node.Endpoint),
		)
		return nil, status.Error(codes.InvalidArgument, "node is not in scheduler node list")
	}
	// 🔴 Through the store's arbitration, like a heartbeat. The incarnation is
	// optional here — the gateway reads it off the node's response and may not
	// have one — and an assignment for a sandbox that has just been created or
	// forked has no incumbent to lose to, because both mint a fresh sandbox id.
	// What is not optional is going through the same rule: a write that skipped
	// it would be the way around everything the heartbeat path enforces.
	execution := normalizeExecutionID(req.GetExecutionId())
	projectionTTL, ttlSource := s.resolveProjectionTTL(projectionTTLFromSecs(req.GetProjectionTtlSecs()))
	recordProjectionTTLSource(ttlSource)
	binding := Binding{Node: node, ExecutionID: execution, ProjectionTTL: projectionTTL}
	if err := s.store.Record(req.GetSandboxId(), binding, time.Now()); err != nil {
		s.logger.Warn("scheduler record assignment binding store failed",
			zap.String("sandbox_id", req.GetSandboxId()),
			zap.String("node_id", node.ID),
			zap.Error(err),
		)
		return nil, status.Error(codes.Unavailable, "binding store unavailable")
	}
	s.logger.Debug("scheduler recorded sandbox assignment",
		zap.String("sandbox_id", req.GetSandboxId()),
		zap.String("node_id", node.ID),
		zap.String("endpoint", node.Endpoint),
		zap.String("execution_id", execution),
		zap.Duration("projection_ttl", projectionTTL),
		zap.String("projection_ttl_source", ttlSource),
	)
	return &schedulerv1.RecordAssignmentResponse{}, nil
}

func (s *Service) Heartbeat(_ context.Context, req *schedulerv1.HeartbeatRequest) (*schedulerv1.HeartbeatResponse, error) {
	nodeID := strings.TrimSpace(req.GetNodeId())
	serviceInstanceID := strings.TrimSpace(req.GetServiceInstanceId())
	if nodeID == "" || serviceInstanceID == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id and service_instance_id are required")
	}

	now := time.Now()
	node, cpuConfigJSON, err := s.nodes.Heartbeat(req, now)
	if err != nil {
		if errors.Is(err, ErrNodeNotInRegistry) {
			s.logger.Warn("scheduler rejected observed registration for unknown node",
				zap.String("node_id", nodeID),
			)
			return nil, status.Error(codes.InvalidArgument, "node is not in scheduler node list")
		}
		return nil, status.Error(codes.Internal, "node registry heartbeat failed")
	}
	roster := rosterFromHeartbeat(req)
	roster = s.resolveRosterProjectionTTLs(roster)
	if err := s.store.ReconcileNode(node, roster, now); err != nil {
		s.logger.Warn("scheduler heartbeat binding reconcile failed",
			zap.String("node_id", nodeID),
			zap.Error(err),
		)
		return nil, status.Error(codes.Unavailable, "binding store unavailable")
	}
	// Only now, with the roster actually applied to the store: warm-up is about
	// the bindings being seeded, not about the node having said hello.
	s.warmup.reportedIn(now)
	return &schedulerv1.HeartbeatResponse{CpuConfigJson: cpuConfigJSON}, nil
}

// ReportSandboxEvent applies the two event types that end a sandbox's presence
// on a node, and watches the rest go by.
//
// 🔴 Never an error to the caller, whatever happens inside. Events are best
// effort from the node's side — the reporter drops them on a lagging channel
// and only warns on a failed send — and the heartbeat reconciliation is the
// repair path for every one that is lost. Failing this RPC would put a retry
// obligation on a caller that has no queue to retry from.
//
// 🔴 What PAUSE actually buys here is small, and it is worth being honest about
// it: a node's heartbeat roster is derived from every sandbox it holds
// regardless of state, so a paused sandbox is still in the roster and the next
// reconciliation reinstalls the record this deleted. It is not harmful — the
// record names the node that holds the paused sandbox, which is where the data
// plane wants to go — but the load-bearing half is DELETE, where the sandbox
// leaves the roster for good and the event buys both the five seconds until the
// next heartbeat and the case where that heartbeat never comes because the node
// died.
func (s *Service) ReportSandboxEvent(_ context.Context, req *schedulerv1.ReportSandboxEventRequest) (*schedulerv1.ReportSandboxEventResponse, error) {
	now := time.Now()
	applied := 0
	for _, event := range req.GetEvents() {
		switch event.GetEventType() {
		case schedulerv1.SandboxEventType_SANDBOX_EVENT_TYPE_PAUSE,
			schedulerv1.SandboxEventType_SANDBOX_EVENT_TYPE_DELETE:
			if s.applyProjectionDelete(event, now) {
				applied++
			}
		default:
			recordSandboxEvent(sandboxEventTypeLabel(event.GetEventType()), sandboxEventObservedOnly)
		}
	}
	s.logger.Debug("scheduler handled sandbox event batch",
		zap.String("node_id", req.GetNodeId()),
		zap.String("service_instance_id", req.GetServiceInstanceId()),
		zap.Int("event_count", len(req.GetEvents())),
		zap.Int("projection_deletes", applied),
		zap.Bool("projection_authoritative", s.projectionAuthoritative),
	)
	return &schedulerv1.ReportSandboxEventResponse{}, nil
}

// applyProjectionDelete runs one pause or delete event against the projection,
// and says whether a record went away.
func (s *Service) applyProjectionDelete(event *schedulerv1.SandboxEvent, now time.Time) bool {
	label := sandboxEventTypeLabel(event.GetEventType())
	if !s.projectionAuthoritative {
		recordSandboxEvent(label, sandboxEventIgnoredSwitchOff)
		return false
	}
	sandboxID := strings.TrimSpace(event.GetSandboxId())
	if sandboxID == "" {
		recordSandboxEvent(label, sandboxEventIgnoredNoSandbox)
		return false
	}
	// 🔴 The quiet normaliser, not the roster one. The roster's counts a drop
	// into a series whose name and help both say "roster"; an event path
	// incrementing it would put two unrelated facts into one number.
	execution, _ := normalizeExecutionIDReason(event.GetExecutionId())
	if execution == "" {
		recordSandboxEvent(label, sandboxEventIgnoredUnknownExecution)
		return false
	}
	outcome, err := s.store.Delete(sandboxID, execution, now)
	if err != nil {
		recordSandboxEvent(label, sandboxEventStoreError)
		s.logger.Warn("scheduler sandbox event binding delete failed",
			zap.String("sandbox_id", sandboxID),
			zap.String("event_type", label),
			zap.Error(err),
		)
		return false
	}
	recordSandboxEvent(label, string(outcome))
	if outcome == BindingDeleteRejectedStale {
		// Worth a line: it means an event arrived for an incarnation that is no
		// longer the one running, which is the guard doing its job and also the
		// only visible sign of out-of-order delivery.
		s.logger.Debug("scheduler refused a stale sandbox event delete",
			zap.String("sandbox_id", sandboxID),
			zap.String("event_type", label),
			zap.String("event_execution_id", execution),
		)
	}
	return outcome == BindingDeleteDeleted || outcome == BindingDeleteUnknownIncumbent
}

// defaultMaxProjectionTTL is the ceiling a scheduler stores a projection under
// when nothing configures one.
//
// 🔴 25 hours, not 24, and the extra hour is load-bearing. A node's own default
// ceiling is 24 hours and it adds a grace period on top so the record outlives
// the sandbox rather than dying just before it. A 24-hour cap here would clamp
// every single record by exactly that grace — cancelling the thing the grace is
// for, and pinning the "clamped" counter at 100% so it can never signal
// anything.
const defaultMaxProjectionTTL = 25 * time.Hour

// resolveProjectionTTL turns a node-reported budget into what the store should
// write, and says where the number came from.
//
// 🔴 Zero — which is what an absent field and an older node both produce — is
// "use the store's binding_ttl". It is never "no expiry". The wire type is
// unsigned so nothing below zero can arrive here, but the same rule is enforced
// at every place a signed value is parsed, because the one implementation of
// this that got it wrong did so by dividing to zero and then handing that
// straight to a Redis SET with no expiry.
func (s *Service) resolveProjectionTTL(raw time.Duration) (time.Duration, string) {
	if !s.projectionAuthoritative || raw <= 0 {
		return 0, projectionTTLSourceDefault
	}
	if s.maxProjectionTTL > 0 && raw > s.maxProjectionTTL {
		return s.maxProjectionTTL, projectionTTLSourceClamped
	}
	return raw, projectionTTLSourceNode
}

// projectionTTLFromSecs is the one conversion from the wire's whole seconds.
// A non-positive value becomes zero, which every reader of this treats as
// "no budget offered" and never as "no expiry".
func projectionTTLFromSecs(secs uint32) time.Duration {
	if secs == 0 {
		return 0
	}
	return time.Duration(secs) * time.Second
}

// resolveRosterProjectionTTLs applies the same rule across a heartbeat roster.
// It does not count: the store writes a TTL for only some of these entries — the
// refreshes keep the deadline they already have — and counting all of them would
// report a decision that was not made, once per sandbox every five seconds.
func (s *Service) resolveRosterProjectionTTLs(roster []RosterEntry) []RosterEntry {
	if len(roster) == 0 {
		return roster
	}
	for i := range roster {
		ttl, _ := s.resolveProjectionTTL(roster[i].ProjectionTTL)
		roster[i].ProjectionTTL = ttl
	}
	return roster
}

func (s *Service) RunObservedNodesMetrics(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 15 * time.Second
	}
	s.refreshObservedNodesMetrics(time.Now())

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			s.refreshObservedNodesMetrics(now)
		}
	}
}

func (s *Service) refreshObservedNodesMetrics(now time.Time) {
	recordObservedNodes(s.nodes.ListObserved("", now))
}

func (s *Service) ListObservedNodes(_ context.Context, req *schedulerv1.ListObservedNodesRequest) (*schedulerv1.ListObservedNodesResponse, error) {
	nodes := s.nodes.ListObserved(req.GetClusterId(), time.Now())
	return &schedulerv1.ListObservedNodesResponse{
		Nodes: nodes,
	}, nil
}

func (s *Service) ListP2PPeers(_ context.Context, req *schedulerv1.ListP2PPeersRequest) (*schedulerv1.ListP2PPeersResponse, error) {
	peers := s.nodes.ListP2pPeers(
		req.GetClusterId(),
		req.GetBackend(),
		req.GetExcludeNodeId(),
		time.Now(),
	)
	return &schedulerv1.ListP2PPeersResponse{Peers: peers}, nil
}

func (s *Service) RecordP2PArtifact(_ context.Context, req *schedulerv1.RecordP2PArtifactRequest) (*schedulerv1.RecordP2PArtifactResponse, error) {
	if strings.TrimSpace(req.GetClusterId()) == "" || strings.TrimSpace(req.GetBackend()) == "" || strings.TrimSpace(req.GetKey()) == "" || strings.TrimSpace(req.GetNodeId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "cluster_id, backend, key, and node_id are required")
	}
	// Index under the node's current identity, not the one it reported itself
	// under: peers are listed by the identity heartbeats are recorded against,
	// so a node mid-upgrade would otherwise file its artifacts under a name no
	// lookup ever matches, and the index would quietly stop accelerating.
	node, ok := s.nodes.Resolve(req.GetNodeId())
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "node is not in scheduler node list")
	}

	s.artifacts.Record(req.GetClusterId(), req.GetBackend(), req.GetKey(), node.ID)
	s.logger.Debug("scheduler recorded P2P artifact",
		zap.String("cluster_id", req.GetClusterId()),
		zap.String("backend", req.GetBackend()),
		zap.String("key", req.GetKey()),
		zap.String("node_id", node.ID),
	)
	return &schedulerv1.RecordP2PArtifactResponse{}, nil
}

func (s *Service) ForgetP2PArtifact(_ context.Context, req *schedulerv1.ForgetP2PArtifactRequest) (*schedulerv1.ForgetP2PArtifactResponse, error) {
	if strings.TrimSpace(req.GetClusterId()) == "" || strings.TrimSpace(req.GetBackend()) == "" || strings.TrimSpace(req.GetKey()) == "" || strings.TrimSpace(req.GetNodeId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "cluster_id, backend, key, and node_id are required")
	}

	// Must resolve exactly as Record did, or a node that filed an artifact under
	// one identity could never withdraw it under the other.
	nodeID := req.GetNodeId()
	if node, ok := s.nodes.Resolve(nodeID); ok {
		nodeID = node.ID
	}

	s.artifacts.Forget(req.GetClusterId(), req.GetBackend(), req.GetKey(), nodeID)
	s.logger.Debug("scheduler forgot P2P artifact",
		zap.String("cluster_id", req.GetClusterId()),
		zap.String("backend", req.GetBackend()),
		zap.String("key", req.GetKey()),
		zap.String("node_id", nodeID),
	)
	return &schedulerv1.ForgetP2PArtifactResponse{}, nil
}

func (s *Service) LookupP2PArtifact(_ context.Context, req *schedulerv1.LookupP2PArtifactRequest) (*schedulerv1.LookupP2PArtifactResponse, error) {
	if strings.TrimSpace(req.GetClusterId()) == "" || strings.TrimSpace(req.GetBackend()) == "" || strings.TrimSpace(req.GetKey()) == "" {
		return nil, status.Error(codes.InvalidArgument, "cluster_id, backend, and key are required")
	}

	nodeIDs := s.artifacts.Lookup(req.GetClusterId(), req.GetBackend(), req.GetKey())
	peers := s.nodes.FilterP2pPeers(
		req.GetClusterId(),
		req.GetBackend(),
		nodeIDs,
		req.GetExcludeNodeId(),
		time.Now(),
	)
	return &schedulerv1.LookupP2PArtifactResponse{Peers: peers}, nil
}

func (s *Service) GetNode(_ context.Context, req *schedulerv1.GetNodeRequest) (*schedulerv1.GetNodeResponse, error) {
	nodeID := strings.TrimSpace(req.GetNodeId())
	if nodeID == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id is required")
	}

	node, ok := s.nodes.GetObserved(nodeID, req.GetClusterId(), time.Now())
	if !ok {
		return nil, status.Error(codes.NotFound, "observed node not found")
	}

	return &schedulerv1.GetNodeResponse{Node: node}, nil
}

func (s *Service) UnregisterNode(_ context.Context, req *schedulerv1.UnregisterNodeRequest) (*schedulerv1.UnregisterNodeResponse, error) {
	nodeID := strings.TrimSpace(req.GetNodeId())
	serviceInstanceID := strings.TrimSpace(req.GetServiceInstanceId())
	if nodeID == "" || serviceInstanceID == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id and service_instance_id are required")
	}

	if node, ok := s.nodes.Resolve(nodeID); ok {
		// Bindings and observations are held under the node's current identity,
		// so an unregister sent under the previous one has to be resolved or it
		// clears nothing.
		nodeID = node.ID
	}

	unregisterErr := s.nodes.UnregisterObserved(nodeID, serviceInstanceID)
	if unregisterErr != nil {
		if errors.Is(unregisterErr, ErrServiceInstanceMismatch) {
			return nil, status.Error(codes.FailedPrecondition, "service instance mismatch")
		}
		return nil, status.Error(codes.Internal, unregisterErr.Error())
	}

	now := time.Now()
	if err := s.store.ReconcileNode(Node{ID: nodeID}, nil, now); err != nil {
		s.logger.Warn("scheduler unregister binding reconcile failed",
			zap.String("node_id", nodeID),
			zap.Error(err),
		)
		return nil, status.Error(codes.Unavailable, "binding store unavailable")
	}
	s.artifacts.ForgetNode(nodeID)

	return &schedulerv1.UnregisterNodeResponse{}, nil
}

func (s *Service) ListRegistrySandboxes(ctx context.Context, req *schedulerv1.ListRegistrySandboxesRequest) (*schedulerv1.ListRegistrySandboxesResponse, error) {
	return listRegistrySandboxes(ctx, s.logger, s.registry, s.canonicalNodeID, req)
}

// listRegistrySandboxes serves the read-only view of the paused registry.
//
// The two failure answers are deliberately different codes. FailedPrecondition
// means this scheduler was never pointed at a registry, so there is nothing to
// read and never will be until it is reconfigured. Unavailable means there is
// one and it could not be read — retryable, and above all *not* an empty list:
// an empty answer from this call reads as "the registry holds nothing", which
// is the one conclusion an unreadable registry must never produce.
//
// resolveNodeID canonicalises the node_id filter and each row's holder the same
// way the reconciliation does. A node that reports itself under a new identity
// keeps writing rows under the old one for as long as the alias lives, so
// comparing the two raw would answer "no rows on that node" for a node that has
// plenty. Nil means no aliases are known, which is the query-only replica.
func listRegistrySandboxes(
	ctx context.Context,
	logger *zap.Logger,
	reader pausedregistry.Reader,
	resolveNodeID func(string) string,
	req *schedulerv1.ListRegistrySandboxesRequest,
) (*schedulerv1.ListRegistrySandboxesResponse, error) {
	if req.GetPageSize() < 0 {
		return nil, status.Error(codes.InvalidArgument, "page_size must not be negative")
	}
	// 🔴 Rejected here rather than filtered on, and rejected before the reader
	// is consulted at all. A state outside the five matches no row, so filtering
	// on it would answer a typo with an empty list — which reads as "the
	// registry holds none of those" rather than as "there is no such state".
	// Argument validation comes first so that answer is the same whether or not
	// this deployment happens to run a registry.
	stateFilter, err := parseRegistryStateFilter(req.GetState())
	if err != nil {
		return nil, err
	}
	if reader == nil {
		return nil, status.Error(codes.FailedPrecondition, "paused registry is not configured")
	}

	listing, err := reader.List(ctx)
	if err != nil {
		if errors.Is(err, pausedregistry.ErrDisabled) {
			return nil, status.Error(codes.FailedPrecondition, "paused registry is not configured")
		}
		logger.Warn("scheduler registry list failed", zap.Error(err))
		return nil, status.Error(codes.Unavailable, "paused registry unavailable")
	}

	nodeFilter := strings.TrimSpace(req.GetNodeId())
	if resolveNodeID == nil {
		resolveNodeID = func(nodeID string) string { return nodeID }
	}
	nodeFilter = resolveNodeID(nodeFilter)
	pageToken := strings.TrimSpace(req.GetPageToken())

	matched := make([]pausedregistry.Sandbox, 0, len(listing.Sandboxes))
	for _, sandbox := range listing.Sandboxes {
		if stateFilter != "" && sandbox.State != stateFilter {
			continue
		}
		if nodeFilter != "" && resolveNodeID(sandbox.Holder()) != nodeFilter {
			continue
		}
		if pageToken != "" && sandbox.SandboxID <= pageToken {
			continue
		}
		matched = append(matched, sandbox)
	}
	// The reader promises no ordering; paging over an unordered list would
	// silently skip rows.
	sort.Slice(matched, func(i, j int) bool {
		return matched[i].SandboxID < matched[j].SandboxID
	})

	nextPageToken := ""
	if pageSize := int(req.GetPageSize()); pageSize > 0 && pageSize < len(matched) {
		matched = matched[:pageSize]
		nextPageToken = matched[len(matched)-1].SandboxID
	}

	sandboxes := make([]*schedulerv1.RegistrySandbox, 0, len(matched))
	for _, sandbox := range matched {
		sandboxes = append(sandboxes, registrySandboxToProto(sandbox))
	}

	return &schedulerv1.ListRegistrySandboxesResponse{
		Sandboxes:         sandboxes,
		NextPageToken:     nextPageToken,
		DatabaseNowUnixMs: listing.Now.UTC().UnixMilli(),
	}, nil
}

// parseRegistryStateFilter resolves the optional state filter. An empty filter
// means every state; anything the table cannot hold is an error naming the five
// values that it can, so the caller is told what to type rather than handed an
// empty page.
func parseRegistryStateFilter(raw string) (pausedregistry.State, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", nil
	}
	state, ok := pausedregistry.ParseState(trimmed)
	if !ok {
		known := make([]string, 0, len(pausedregistry.KnownStates()))
		for _, candidate := range pausedregistry.KnownStates() {
			known = append(known, string(candidate))
		}
		return "", status.Errorf(codes.InvalidArgument,
			"unknown state %q, must be one of %s", trimmed, strings.Join(known, ", "))
	}
	return state, nil
}

func registrySandboxToProto(sandbox pausedregistry.Sandbox) *schedulerv1.RegistrySandbox {
	return &schedulerv1.RegistrySandbox{
		SandboxId:              sandbox.SandboxID,
		ClusterId:              sandbox.ClusterID,
		State:                  string(sandbox.State),
		Generation:             sandbox.Generation,
		OriginNodeId:           sandbox.OriginNodeID,
		ClaimedByNodeId:        sandbox.ClaimedByNodeID,
		SnapshotId:             sandbox.SnapshotID,
		PausedAtUnixMs:         sandbox.PausedAt.UTC().UnixMilli(),
		UpdatedAtUnixMs:        sandbox.UpdatedAt.UTC().UnixMilli(),
		LeaseExpiresAtUnixMs:   optionalUnixMilli(sandbox.LeaseExpiresAt),
		SandboxExpiresAtUnixMs: optionalUnixMilli(sandbox.SandboxExpiresAt),
		HolderNodeId:           sandbox.Holder(),
		// The only endpoint on which an operator can see, row by row, which
		// incarnation the cluster believes is alive. That is the view a
		// suspected double-live is reconciled against, so an empty field here
		// is not cosmetic.
		ExecutionId: sandbox.ExecutionID,
	}
}

// optionalUnixMilli renders a nullable timestamp. Zero stands for NULL, which
// each of the two lease columns gives its own meaning to — see the field
// comments in scheduler.proto.
func optionalUnixMilli(t *time.Time) int64 {
	if t == nil {
		return 0
	}
	return t.UTC().UnixMilli()
}

func (s *Service) isKnownNode(node Node) bool {
	return s.nodes.Contains(node)
}
