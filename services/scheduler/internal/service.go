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

type QueryOnlyService struct {
	schedulerv1.UnimplementedSchedulerServer
	logger   *zap.Logger
	store    BindingStore
	registry pausedregistry.Reader
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
		logger:   s.logger,
		store:    s.store,
		registry: s.registry,
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
		logger:   s.logger,
		store:    s.store,
		registry: s.registry,
		warmup:   s.warmup,
		placer:   s,
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
	if err := s.store.Record(req.GetSandboxId(), node, time.Now()); err != nil {
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
	if err := s.store.ReconcileNode(node, req.GetSandboxIds(), now); err != nil {
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

func (s *Service) ReportSandboxEvent(_ context.Context, req *schedulerv1.ReportSandboxEventRequest) (*schedulerv1.ReportSandboxEventResponse, error) {
	s.logger.Debug("scheduler ignored sandbox event batch",
		zap.String("node_id", req.GetNodeId()),
		zap.String("service_instance_id", req.GetServiceInstanceId()),
		zap.Int("event_count", len(req.GetEvents())),
	)
	return &schedulerv1.ReportSandboxEventResponse{}, nil
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
