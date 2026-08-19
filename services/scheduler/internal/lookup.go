package scheduler

import (
	"context"
	"errors"
	"strings"
	"time"

	schedulerv1 "agentenv/services/api/proto"
	pausedregistry "agentenv/services/scheduler/internal/registry"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// lookupResult labels one lookup answer for metrics. The set is closed: every
// return below maps onto exactly one of these, so the sum of the series is the
// call count and a new branch that forgets to classify itself shows up as a
// gap rather than as silence.
type lookupResult string

const (
	lookupResultBinding  lookupResult = "bound_binding"
	lookupResultRoster   lookupResult = "bound_roster"
	lookupResultRegistry lookupResult = "bound_registry"
	lookupResultPlaced   lookupResult = "placed"
	lookupResultPinned   lookupResult = "pinned"
	lookupResultNotFound lookupResult = "not_found"

	lookupResultInvalidArgument lookupResult = "invalid_argument"
	// The three ways an answer can be withheld because something could not be
	// consulted. Each is Unavailable, and none of them is ever NotFound.
	lookupResultStoreUnavailable    lookupResult = "unavailable_binding_store"
	lookupResultRegistryUnavailable lookupResult = "unavailable_registry"
	lookupResultRegistryCold        lookupResult = "unavailable_registry_cold"
	lookupResultColdBindings        lookupResult = "unavailable_cold_bindings"
	lookupResultNoNodes             lookupResult = "unavailable_no_nodes"
	lookupResultNoPlacer            lookupResult = "unavailable_no_placer"
	lookupResultPlacementFailed     lookupResult = "internal_placement_failed"
	// The three ways the registry names a node that cannot serve the request.
	//
	// 🔴 The first two are the same refusal for opposite reasons and are kept
	// apart on purpose. "Not accepting work" sends an operator to the admin API
	// to look at the node's DRAINING status; "not reporting" sends them to the
	// node's heartbeat. Reporting the second as the first is a signpost pointing
	// at a healthy subsystem, and it costs however long it takes to stop
	// believing it.
	lookupResultOriginUnschedulable lookupResult = "origin_unschedulable"
	lookupResultOriginNotReporting  lookupResult = "origin_not_reporting"
	lookupResultHolderUnreachable   lookupResult = "holder_unreachable"
	lookupResultUnknownState        lookupResult = "unknown_state"
)

// nodeSchedulability is why a node may not be pinned to. The two failures are
// reached through the same call and mean entirely different things, so they are
// never collapsed into a bool.
type nodeSchedulability int

const (
	// nodeSchedulable: discovery knows the node, its heartbeat is fresh, and it
	// has not said it is going away.
	nodeSchedulable nodeSchedulability = iota
	// nodeNotReporting: discovery does not know the node, or its last heartbeat
	// is older than the report TTL. Nothing is known about it either way — this
	// is the fail-closed answer, not an observation.
	nodeNotReporting
	// nodeNotAcceptingWork: the node is reporting, and what it reported is that
	// it will not take new work (DRAINING).
	nodeNotAcceptingWork
)

// nodePlacer is the half of a lookup that has to know about nodes: which of
// them reported holding a sandbox, which of them still take work, and which one
// should rebuild a sandbox nobody holds.
//
// A query-only replica has none of that — no discovery, no heartbeats, no
// strategy — so it carries a nil placer and answers Unavailable for anything it
// would have to place. That is deliberate. The one answer it must never invent
// is an authoritative "no such sandbox", and a replica that cannot see the
// nodes has no standing to give one.
type nodePlacer interface {
	// rosterHolder returns the node that most recently reported holding this
	// sandbox in a heartbeat, provided that report is still fresh.
	rosterHolder(sandboxID string, now time.Time) (Node, bool)
	// liveNode resolves a node id to a discovered node with a fresh heartbeat.
	// It says nothing about whether that node accepts new work.
	liveNode(nodeID string, now time.Time) (Node, bool)
	// schedulableNode is liveNode plus the node not having reported itself
	// unable to take new work. It says which of the two it failed on, because
	// the caller reports them as different faults.
	schedulableNode(nodeID string, now time.Time) (Node, nodeSchedulability)
	// place picks a node for a sandbox any node could rebuild, preferring
	// preferNodeID when it survives filtering.
	place(preferNodeID string) (Node, error)
}

// lookupDeps is everything a lookup may consult. Only store is mandatory.
type lookupDeps struct {
	logger *zap.Logger
	store  BindingStore
	// registry is the read-only paused registry. A disabled reader answers
	// ErrDisabled, which means "not configured" and restores the behaviour of a
	// scheduler that never had one — as opposed to a read failure, which means
	// "configured and unreadable" and must never produce an absence.
	registry pausedregistry.Reader
	// warmup withholds absence answers while bindings are still being seeded by
	// node heartbeats. Nil where a miss is a real miss from the first request
	// (the query-only replica requires Redis, whose bindings outlive it).
	warmup *warmupGate
	placer nodePlacer
}

// lookupNode answers a sandbox lookup by walking, in order, everything that can
// know where a sandbox is:
//
//  1. the binding a node's heartbeat wrote                    → BOUND
//  2. the roster of that heartbeat, which outlives the binding → BOUND
//  3. the paused registry the nodes agreed on among themselves → PLACED / PINNED / BOUND
//  4. nothing knew of it                                       → NOT_FOUND
//
// 🔴 The ordering exists to keep one specific answer honest. NOT_FOUND here
// means "the registry is readable and holds no row", never "we could not look":
// downstream a 404 on a resume is the end of that sandbox's life, so every step
// that could not be completed answers Unavailable instead and lets the caller
// try again.
func lookupNode(ctx context.Context, deps lookupDeps, req *schedulerv1.LookupNodeRequest) (_ *schedulerv1.LookupNodeResponse, err error) {
	result := lookupResultInvalidArgument
	defer func() { recordSchedulerLookup(result) }()

	logger := deps.logger
	if logger == nil {
		logger = zap.NewNop()
	}

	sandboxID := strings.TrimSpace(req.GetSandboxId())
	if sandboxID == "" {
		return nil, status.Error(codes.InvalidArgument, "sandbox_id is required")
	}

	now := time.Now()

	// 1. The binding. This is the hot path — every proxied request lands here —
	// so nothing below it may run on a hit.
	node, ok, getErr := deps.store.Get(sandboxID, now)
	if getErr != nil {
		result = lookupResultStoreUnavailable
		logger.Warn("scheduler lookup binding store failed", zap.String("sandbox_id", sandboxID), zap.Error(getErr))
		return nil, status.Error(codes.Unavailable, "binding store unavailable")
	}
	if ok {
		result = lookupResultBinding
		logger.Debug("scheduler lookup resolved sandbox assignment",
			zap.String("sandbox_id", sandboxID),
			zap.String("node_id", node.ID),
			zap.String("endpoint", node.Endpoint),
			zap.String("source", string(lookupResultBinding)),
		)
		return lookupResponse(node, schedulerv1.SandboxLocation_SANDBOX_LOCATION_BOUND, ""), nil
	}

	// 2. The roster. A binding expires on its own TTL while the roster that
	// wrote it stays as the node last reported it, so this covers the window a
	// heartbeat is late for, and the one where another node's reconciliation
	// dropped a binding this node still lists.
	if deps.placer != nil {
		if holder, held := deps.placer.rosterHolder(sandboxID, now); held {
			result = lookupResultRoster
			logger.Debug("scheduler lookup resolved sandbox from a heartbeat roster",
				zap.String("sandbox_id", sandboxID),
				zap.String("node_id", holder.ID),
				zap.String("endpoint", holder.Endpoint),
				zap.String("source", string(lookupResultRoster)),
			)
			return lookupResponse(holder, schedulerv1.SandboxLocation_SANDBOX_LOCATION_BOUND, ""), nil
		}
	}

	// Bindings and rosters are written by the same heartbeat, so a scheduler
	// that has not been told anything yet knows nothing about either — and an
	// absence derived from that is a cold cache, not a fact. Evaluated here
	// rather than earlier so a roster hit costs nothing.
	warm := deps.warmup == nil || deps.warmup.warmedUp(now)

	// 3. The registry.
	reader := deps.registry
	if reader == nil {
		reader = pausedregistry.Disabled()
	}
	entry, found, registryErr := reader.Get(ctx, sandboxID)
	switch {
	case errors.Is(registryErr, pausedregistry.ErrDisabled):
		// No registry configured. Nothing else can be consulted, so the answer
		// is exactly what a scheduler without one has always given.
		return lookupAbsent(logger, sandboxID, warm, &result)
	case registryErr != nil:
		result = lookupResultRegistryUnavailable
		logger.Warn("scheduler lookup registry read failed",
			zap.String("sandbox_id", sandboxID),
			zap.Error(registryErr),
		)
		return nil, status.Error(codes.Unavailable, "paused registry unavailable")
	case !found:
		// A reader that has never completed a read cannot be quoted as saying a
		// row is absent. The Postgres reader marks itself ready on the read that
		// just succeeded, so this guards the readers that could answer without
		// having looked rather than that one.
		if !reader.Ready() {
			result = lookupResultRegistryCold
			logger.Info("scheduler lookup withheld while the paused registry is still cold",
				zap.String("sandbox_id", sandboxID),
			)
			return nil, status.Error(codes.Unavailable, "paused registry is not ready")
		}
		return lookupAbsent(logger, sandboxID, warm, &result)
	}

	// 4. There is a row, so the sandbox exists. From here the only question is
	// which node may serve it, and every failure to answer that is a 503.
	if deps.placer == nil {
		result = lookupResultNoPlacer
		logger.Warn("scheduler lookup cannot place a registered sandbox on this replica",
			zap.String("sandbox_id", sandboxID),
			zap.String("state", string(entry.State)),
			zap.String("origin_node_id", entry.OriginNodeID),
		)
		return nil, status.Error(codes.Unavailable, "this scheduler replica cannot place sandboxes")
	}

	switch entry.State {
	case pausedregistry.StatePaused:
		// The snapshot is published, so any node can rebuild it. Origin is only
		// a preference — but a strong one: rebuilding on the machine that
		// already has the layers is what makes a cross-node resume avoid a full
		// pull from object storage.
		placed, placeErr := deps.placer.place(entry.OriginNodeID)
		if placeErr != nil {
			if errors.Is(placeErr, ErrNoNodes) {
				result = lookupResultNoNodes
				return nil, status.Error(codes.Unavailable, "no nodes available")
			}
			result = lookupResultPlacementFailed
			return nil, status.Error(codes.Internal, placeErr.Error())
		}
		result = lookupResultPlaced
		logger.Info("scheduler placed a paused sandbox",
			zap.String("sandbox_id", sandboxID),
			zap.String("node_id", placed.ID),
			zap.String("origin_node_id", entry.OriginNodeID),
			zap.Bool("origin_preferred", placed.ID == entry.OriginNodeID),
		)
		return lookupResponse(placed, schedulerv1.SandboxLocation_SANDBOX_LOCATION_PLACED, entry.OriginNodeID), nil

	case pausedregistry.StatePublishing, pausedregistry.StateLocalOnly:
		// No snapshot in shared storage: the only copy is on origin's disk, so
		// this is that node or nothing.
		//
		// 🔴 Origin's schedulability is checked here rather than left to the
		// node. An isolated node answers a resume it cannot serve with a 503
		// asking for the request to go somewhere else, and there is nowhere else
		// for these two states — so sending it would earn the caller a 503 whose
		// message says the opposite of what happened.
		origin, schedulability := deps.placer.schedulableNode(entry.OriginNodeID, now)
		switch schedulability {
		case nodeNotReporting:
			// The node has not been heard from. Nothing is known about whether
			// it would serve this — it is being refused because it is silent,
			// which is a heartbeat problem and not a scheduling decision the
			// node made.
			result = lookupResultOriginNotReporting
			logger.Warn("scheduler cannot pin a sandbox to an origin node that is not reporting",
				zap.String("sandbox_id", sandboxID),
				zap.String("state", string(entry.State)),
				zap.String("origin_node_id", entry.OriginNodeID),
			)
			return nil, status.Errorf(codes.FailedPrecondition,
				"sandbox is %s on node %q, which is not reporting", entry.State, entry.OriginNodeID)
		case nodeNotAcceptingWork:
			result = lookupResultOriginUnschedulable
			logger.Warn("scheduler cannot pin a sandbox to an origin node that is not accepting work",
				zap.String("sandbox_id", sandboxID),
				zap.String("state", string(entry.State)),
				zap.String("origin_node_id", entry.OriginNodeID),
			)
			return nil, status.Errorf(codes.FailedPrecondition,
				"sandbox is %s on node %q, which is not accepting work", entry.State, entry.OriginNodeID)
		}
		result = lookupResultPinned
		logger.Info("scheduler pinned a sandbox to its origin node",
			zap.String("sandbox_id", sandboxID),
			zap.String("state", string(entry.State)),
			zap.String("node_id", origin.ID),
		)
		return lookupResponse(origin, schedulerv1.SandboxLocation_SANDBOX_LOCATION_PINNED, entry.OriginNodeID), nil

	case pausedregistry.StateRunning, pausedregistry.StateResuming:
		// 🔴 Holder(), not origin_node_id. A claim leaves origin pointing at
		// whoever still holds the local artifacts, so a resuming row read by
		// origin would route the caller to the node the sandbox is moving away
		// from.
		holderID := entry.Holder()
		holder, live := deps.placer.liveNode(holderID, now)
		if !live {
			if !warm {
				result = lookupResultColdBindings
				logger.Info("scheduler lookup withheld while bindings are still being seeded",
					zap.String("sandbox_id", sandboxID),
				)
				return nil, status.Error(codes.Unavailable, "scheduler is still seeding sandbox assignments")
			}
			result = lookupResultHolderUnreachable
			logger.Warn("scheduler cannot reach the node holding a live sandbox",
				zap.String("sandbox_id", sandboxID),
				zap.String("state", string(entry.State)),
				zap.String("holder_node_id", holderID),
			)
			return nil, status.Errorf(codes.FailedPrecondition,
				"sandbox is %s on node %q, which is not reporting", entry.State, holderID)
		}
		result = lookupResultRegistry
		logger.Debug("scheduler lookup resolved sandbox from the paused registry",
			zap.String("sandbox_id", sandboxID),
			zap.String("state", string(entry.State)),
			zap.String("node_id", holder.ID),
			zap.String("source", string(lookupResultRegistry)),
		)
		return lookupResponse(holder, schedulerv1.SandboxLocation_SANDBOX_LOCATION_BOUND, entry.OriginNodeID), nil

	default:
		// The table's CHECK constraint pins the five states above, so this is a
		// row written by a build newer than this one. Refusing is the only safe
		// answer: guessing which of the five it resembles is how a live sandbox
		// gets rebuilt somewhere it already exists.
		result = lookupResultUnknownState
		logger.Error("scheduler lookup found an unrecognised registry state",
			zap.String("sandbox_id", sandboxID),
			zap.String("state", string(entry.State)),
		)
		return nil, status.Errorf(codes.FailedPrecondition, "sandbox is in an unrecognised registry state %q", entry.State)
	}
}

// lookupAbsent answers the one case where nothing knows about the sandbox.
//
// It is the only place NOT_FOUND is produced, and it is gated on the bindings
// having been seeded: a scheduler that has just started has been told nothing,
// and a sandbox that has never been paused has no registry row by design, so
// the two together would 404 a perfectly healthy running sandbox.
func lookupAbsent(logger *zap.Logger, sandboxID string, warm bool, result *lookupResult) (*schedulerv1.LookupNodeResponse, error) {
	if !warm {
		*result = lookupResultColdBindings
		// Retryable on purpose: the gateway maps Unavailable to 503, and a 503
		// on a resume is a request that can be made again, where a 404 is a
		// sandbox the caller is told no longer exists.
		logger.Info("scheduler lookup withheld while bindings are still being seeded",
			zap.String("sandbox_id", sandboxID),
		)
		return nil, status.Error(codes.Unavailable, "scheduler is still seeding sandbox assignments")
	}
	*result = lookupResultNotFound
	logger.Debug("scheduler lookup missed sandbox assignment", zap.String("sandbox_id", sandboxID))
	return nil, status.Error(codes.NotFound, "sandbox assignment not found")
}

func lookupResponse(node Node, location schedulerv1.SandboxLocation, originNodeID string) *schedulerv1.LookupNodeResponse {
	return &schedulerv1.LookupNodeResponse{
		Node:         node.ToProto(),
		Location:     location,
		OriginNodeId: originNodeID,
	}
}

// rosterHolder implements nodePlacer.
//
// More than one node listing the same sandbox is normal mid-takeover: the
// origin keeps its paused record until its own reconciliation drops it. The
// most recent report wins, which is the closest thing a one-way heartbeat can
// offer to "who has it now".
func (s *Service) rosterHolder(sandboxID string, now time.Time) (Node, bool) {
	var (
		best     Node
		bestSeen time.Time
		found    bool
	)
	for _, nodeID := range s.nodes.NodesHolding(sandboxID) {
		node, resolved := s.nodes.Resolve(nodeID)
		if !resolved {
			continue
		}
		_, lastSeen, ok := s.nodes.RosterOf(node.ID)
		if !ok || !s.rosterFresh(lastSeen, now) {
			continue
		}
		if !found || lastSeen.After(bestSeen) {
			best, bestSeen, found = node, lastSeen, true
		}
	}
	return best, found
}

// liveNode implements nodePlacer.
func (s *Service) liveNode(nodeID string, now time.Time) (Node, bool) {
	node, ok := s.nodes.Resolve(strings.TrimSpace(nodeID))
	if !ok {
		return Node{}, false
	}
	_, lastSeen, ok := s.nodes.RosterOf(node.ID)
	if !ok || !s.rosterFresh(lastSeen, now) {
		return Node{}, false
	}
	return node, true
}

// schedulableNode implements nodePlacer.
//
// FilterUnschedulable fails open on a node that has said nothing about itself,
// which is right for scheduling a brand-new sandbox onto a freshly started
// cluster. Requiring a fresh roster on top of it turns that around here: a node
// nobody has heard from is not somewhere to pin a sandbox whose only copy is on
// its disk.
//
// 🔴 That fail-closed half has a cost worth knowing before anybody calls it a
// bug. Every scheduler restart begins with no rosters at all, so for the gap
// until each node's next heartbeat lands, a publishing/local_only sandbox on a
// perfectly healthy node is refused. The gap is one node report interval —
// 5s by default (AENV_OBSERVABILITY_REPORT_INTERVAL_SECS) — and up to 60s
// (MAX_REPORT_BACKOFF) if the scheduler was down long enough for the nodes'
// report backoff to have grown. The refusal is FailedPrecondition, which the
// gateway renders as a retryable 503, so the window costs a retry rather than a
// sandbox. Failing open instead would mean pinning to a machine that may have
// been gone for hours, which costs the sandbox.
//
// Note this is not symmetric with a `paused` row, which is placed rather than
// pinned and so stays serviceable in the same window. That asymmetry is the
// design: a paused sandbox has a published snapshot and any node can rebuild
// it, while these two states have one copy in the world.
func (s *Service) schedulableNode(nodeID string, now time.Time) (Node, nodeSchedulability) {
	node, ok := s.liveNode(nodeID, now)
	if !ok {
		return Node{}, nodeNotReporting
	}
	rich := RichNode{Node: node, Snapshot: s.nodes.PeekObserved(node.ID)}
	if len(FilterUnschedulable([]RichNode{rich})) == 0 {
		return Node{}, nodeNotAcceptingWork
	}
	return node, nodeSchedulable
}

// place implements nodePlacer.
func (s *Service) place(preferNodeID string) (Node, error) {
	result, err := s.selectNode(nil, preferNodeID)
	if err != nil {
		return Node{}, err
	}
	return result.node.Node, nil
}

// rosterFresh judges a heartbeat's age against the same report TTL the observed
// node view derives UNHEALTHY from, so "fresh enough to route to" and "healthy"
// cannot drift apart.
func (s *Service) rosterFresh(lastSeen time.Time, now time.Time) bool {
	if lastSeen.IsZero() {
		return false
	}
	ttl := s.reportTTL
	if ttl <= 0 {
		ttl = defaultObservedReportTTL
	}
	return now.Sub(lastSeen) <= ttl
}
