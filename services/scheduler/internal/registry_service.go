package scheduler

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	schedulerv1 "agentenv/services/api/proto"
	pausedregistry "agentenv/services/scheduler/internal/registry"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var registryLeaseTTLClamped = promauto.NewCounter(prometheus.CounterOpts{
	Name: "agentenv_scheduler_registry_write_lease_ttl_clamped_total",
	Help: "Requests whose reported lease TTL was below the floor and was raised to it.",
})

// registryReleaseClaimUnmatched and registryRemoveUnmatched count the
// conditional writes that matched nothing.
//
// Both are successes: the row the caller meant to act on has already moved on,
// which is the state it was trying to reach. They are counted because the node
// used to run these statements itself and could see the zero-row tag; once they
// moved behind an RPC, a node quoting generations it has already lost became
// invisible from both sides.
var registryReleaseClaimUnmatched = promauto.NewCounter(prometheus.CounterOpts{
	Name: "agentenv_scheduler_registry_write_release_claim_unmatched_total",
	Help: "release_claim requests whose quoted generation matched no row.",
})

var registryRemoveUnmatched = promauto.NewCounter(prometheus.CounterOpts{
	Name: "agentenv_scheduler_registry_write_remove_unmatched_total",
	Help: "remove requests whose quoted generation matched no row.",
})

// registryLeaseTTLTooShort counts nodes reporting a lease TTL that leaves no
// room for two missed renewals.
//
// 🔴 Counted and logged, never refused. The invariant is ttl >= 3*interval, and
// a node that gets it wrong will have its own rows expire underneath it — but
// refusing the renewal is how that becomes certain instead of merely likely.
// The node enforces this locally against its own config; this is the only place
// that can see both numbers when those two knobs have drifted apart.
var registryLeaseTTLTooShort = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "agentenv_scheduler_registry_write_lease_ttl_too_short_total",
		Help: "Lease renewals whose reported TTL leaves no room for two missed renewals.",
	},
	[]string{"node"},
)

var registryWriteRPCs = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "agentenv_scheduler_registry_write_rpc_total",
		Help: "Paused-registry write RPCs by method and gRPC code.",
	},
	[]string{"rpc", "code"},
)

// PausedRegistryService serves the registry to the nodes.
//
// It is a translation layer and nothing else: every decision about what a
// sandbox's row may become lives in the store, next to the statement that makes
// it. The one thing decided here is what a failure looks like on the wire, and
// that is decided the same way every time —
//
// 🔴 no error is ever answered with an empty result. A sandbox missing from
// GetSandboxes means the row does not exist, and the node deletes that
// sandbox's local artifacts and tears down its VM on the strength of that. An
// empty map is an assertion about the table, not an admission that this process
// could not read it.
type PausedRegistryService struct {
	schedulerv1.UnimplementedPausedRegistryServer

	store pausedregistry.Store
	grace *pausedregistry.Grace
	log   *zap.Logger

	// clusterID is the one cluster this process owns the table for.
	//
	// Requests naming a different one are refused rather than served. The scope
	// still travels in every statement, exactly as it does on the node — this
	// is a second check, not a replacement for the first. It exists because the
	// restart grace pass extends the leases of one cluster, and serving a
	// second cluster's writes would mean serving them with none of their leases
	// extended, which is the one state this whole gate exists to avoid.
	clusterID string

	// defaultLeaseTTL is what the store stamps when a request reports no TTL of
	// its own. Kept here only to notice when a node disagrees with it.
	defaultLeaseTTL time.Duration
	// leaseTTLFloor is the shortest lease this process will stamp, however
	// short a value a caller reports. See clampLeaseTTL.
	leaseTTLFloor time.Duration

	mu        sync.Mutex
	warnedTTL map[time.Duration]struct{}
}

// NewPausedRegistryService wires the store to the five RPCs.
func NewPausedRegistryService(
	log *zap.Logger,
	store pausedregistry.Store,
	grace *pausedregistry.Grace,
	clusterID string,
	defaultLeaseTTL time.Duration,
	leaseTTLFloor time.Duration,
) *PausedRegistryService {
	if leaseTTLFloor <= 0 {
		leaseTTLFloor = defaultLeaseTTLFloor
	}
	return &PausedRegistryService{
		store:           store,
		grace:           grace,
		log:             log,
		clusterID:       strings.TrimSpace(clusterID),
		defaultLeaseTTL: defaultLeaseTTL,
		leaseTTLFloor:   leaseTTLFloor,
		warnedTTL:       make(map[time.Duration]struct{}),
	}
}

// defaultLeaseTTLFloor is deliberately far below any real lease.
const defaultLeaseTTLFloor = 30 * time.Second

// ─────────────────────────────────────────────────────────────────────────────
// GetSandboxes
// ─────────────────────────────────────────────────────────────────────────────

// GetSandboxes reads rows in bulk, without metadata.
//
// No bulk consumer reads metadata — the node's two batch callers look only at
// state, the two node ids and the generation — and leaving it out keeps the
// largest column in the table off a response whose size is the roster's.
func (s *PausedRegistryService) GetSandboxes(ctx context.Context, req *schedulerv1.GetSandboxesRequest) (*schedulerv1.GetSandboxesResponse, error) {
	if err := s.admit(req.GetClusterId()); err != nil {
		return nil, s.fail("GetSandboxes", err)
	}

	rows, err := s.store.GetMany(ctx, req.GetClusterId(), req.GetSandboxIds())
	if err != nil {
		return nil, s.fail("GetSandboxes", err)
	}

	out := make([]*schedulerv1.RegistryEntry, 0, len(rows.Entries))
	for _, entry := range rows.Entries {
		out = append(out, registryEntryToProto(entry))
	}
	registryWriteRPCs.WithLabelValues("GetSandboxes", codes.OK.String()).Inc()
	resp := &schedulerv1.GetSandboxesResponse{
		Sandboxes: out,
		// 🔴 Say which ids this answer covers. The caller treats an id's
		// absence from Sandboxes as authority to tear down a running VM and
		// delete its artifacts, and it has no other way to tell that apart from
		// a response that lost rows on the way here.
		CoveredSandboxIds: rows.Covered,
	}
	if !rows.Now.IsZero() {
		resp.NowUnixMicros = rows.Now.UnixMicro()
	}
	return resp, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// TransitionSandbox
// ─────────────────────────────────────────────────────────────────────────────

// TransitionSandbox moves a sandbox between states.
//
// The six node-side operations it covers differ in which of the request's
// fields mean anything, and a field that means nothing for the requested kind
// is refused rather than dropped. Dropping is how a caller ends up believing it
// fenced a write that was never conditional, or that it recorded metadata that
// was never stored.
func (s *PausedRegistryService) TransitionSandbox(ctx context.Context, req *schedulerv1.TransitionSandboxRequest) (*schedulerv1.TransitionSandboxResponse, error) {
	if err := s.admit(req.GetClusterId()); err != nil {
		return nil, s.fail("TransitionSandbox", err)
	}

	store := s.leaseStore(req.GetLeaseTtlMillis())

	switch req.GetKind() {
	case schedulerv1.TransitionKind_TRANSITION_KIND_BEGIN_PAUSE:
		if err := rejectFields(req, fieldGeneration|fieldSnapshot); err != nil {
			return nil, s.fail("TransitionSandbox", err)
		}
		if len(req.GetMetadataJson()) == 0 {
			return nil, s.fail("TransitionSandbox", fmt.Errorf("%w: begin_pause carries no metadata", pausedregistry.ErrInvalidArgument))
		}
		began, err := store.BeginPause(ctx, pausedregistry.BeginPauseInput{
			ClusterID:    req.GetClusterId(),
			SandboxID:    req.GetSandboxId(),
			OriginNodeID: req.GetNodeId(),
			Metadata:     req.GetMetadataJson(),
		})
		if err != nil {
			return nil, s.fail("TransitionSandbox", err)
		}
		registryWriteRPCs.WithLabelValues("TransitionSandbox", codes.OK.String()).Inc()
		return &schedulerv1.TransitionSandboxResponse{
			Generation:         began.Generation,
			PreviousSnapshotId: began.PreviousSnapshotID,
		}, nil

	case schedulerv1.TransitionKind_TRANSITION_KIND_COMPLETE_PAUSE:
		if err := rejectFields(req, fieldMetadata); err != nil {
			return nil, s.fail("TransitionSandbox", err)
		}
		generation, err := requireGeneration(req)
		if err != nil {
			return nil, s.fail("TransitionSandbox", err)
		}
		if err := store.CompletePause(ctx, req.GetClusterId(), req.GetSandboxId(), generation, req.GetSnapshotId()); err != nil {
			return nil, s.fail("TransitionSandbox", err)
		}
		return s.ok("TransitionSandbox")

	case schedulerv1.TransitionKind_TRANSITION_KIND_MARK_LOCAL_ONLY:
		if err := rejectFields(req, fieldMetadata|fieldSnapshot); err != nil {
			return nil, s.fail("TransitionSandbox", err)
		}
		generation, err := requireGeneration(req)
		if err != nil {
			return nil, s.fail("TransitionSandbox", err)
		}
		if err := store.MarkLocalOnly(ctx, req.GetClusterId(), req.GetSandboxId(), generation); err != nil {
			return nil, s.fail("TransitionSandbox", err)
		}
		return s.ok("TransitionSandbox")

	case schedulerv1.TransitionKind_TRANSITION_KIND_RELEASE_CLAIM:
		if err := rejectFields(req, fieldMetadata|fieldSnapshot); err != nil {
			return nil, s.fail("TransitionSandbox", err)
		}
		generation, err := requireGeneration(req)
		if err != nil {
			return nil, s.fail("TransitionSandbox", err)
		}
		matched, err := store.ReleaseClaim(ctx, req.GetClusterId(), req.GetSandboxId(), generation)
		if err != nil {
			return nil, s.fail("TransitionSandbox", err)
		}
		if !matched {
			// Still a success — the claim this caller meant to release is no
			// longer its own, which is the state it was trying to reach. But
			// the node cannot see the zero-row tag from here, and a node that
			// keeps quoting generations it has already lost is worth finding.
			registryReleaseClaimUnmatched.Inc()
			s.log.Info("release_claim matched no row: the caller's generation is stale",
				zap.String("sandbox_id", req.GetSandboxId()),
				zap.String("node_id", req.GetNodeId()),
				zap.Int64("expect_generation", generation),
			)
		}
		registryWriteRPCs.WithLabelValues("TransitionSandbox", codes.OK.String()).Inc()
		return &schedulerv1.TransitionSandboxResponse{Matched: matched}, nil

	case schedulerv1.TransitionKind_TRANSITION_KIND_MARK_RUNNING:
		if err := rejectFields(req, fieldGeneration|fieldMetadata|fieldSnapshot); err != nil {
			return nil, s.fail("TransitionSandbox", err)
		}
		var expiresAt *time.Time
		if req.SandboxExpiresAtUnixMicros != nil {
			// Absent is not zero. Absent means the sandbox was asked never to
			// expire and reclamation leaves it alone forever; zero would be a
			// deadline in 1970, which reclamation acts on.
			deadline := time.UnixMicro(req.GetSandboxExpiresAtUnixMicros()).UTC()
			expiresAt = &deadline
		}
		outcome, err := store.MarkRunning(ctx, req.GetClusterId(), req.GetSandboxId(), req.GetNodeId(), expiresAt)
		if err != nil {
			return nil, s.fail("TransitionSandbox", err)
		}
		// 🔴 Not being adopted is a successful answer, not a failure. Untracked
		// says the cluster does not track this sandbox — which is what a
		// sandbox that has never been paused looks like, and by far the common
		// case. Reporting it as an error would make the node treat its own
		// healthy sandboxes as an outage.
		//
		// Held-elsewhere is also a success on the wire and something else
		// entirely in meaning: two nodes believe they are bringing the same
		// sandbox up. It used to be indistinguishable from untracked because
		// both arrived as `tracked: false`.
		registryWriteRPCs.WithLabelValues("TransitionSandbox", codes.OK.String()).Inc()
		return &schedulerv1.TransitionSandboxResponse{
			Tracked:            outcome == pausedregistry.MarkRunningAdopted,
			MarkRunningOutcome: markRunningOutcomeToProto(outcome),
		}, nil

	case schedulerv1.TransitionKind_TRANSITION_KIND_REMOVE:
		if err := rejectFields(req, fieldMetadata|fieldSnapshot); err != nil {
			return nil, s.fail("TransitionSandbox", err)
		}
		generation, err := requireGeneration(req)
		if err != nil {
			return nil, s.fail("TransitionSandbox", err)
		}
		removed, err := s.store.Remove(ctx, req.GetClusterId(), req.GetSandboxId(), generation)
		if err != nil {
			return nil, s.fail("TransitionSandbox", err)
		}
		if !removed {
			// The row moved since the caller read it, so this deleted nothing —
			// which is what the caller wanted, since the row it meant to delete
			// is already gone. Counted because the alternative reading is that
			// a node is operating on a view from before a partition.
			registryRemoveUnmatched.Inc()
			s.log.Info("remove matched no row: the caller's generation is stale",
				zap.String("sandbox_id", req.GetSandboxId()),
				zap.String("node_id", req.GetNodeId()),
				zap.Int64("expect_generation", generation),
			)
		}
		registryWriteRPCs.WithLabelValues("TransitionSandbox", codes.OK.String()).Inc()
		return &schedulerv1.TransitionSandboxResponse{Removed: removed}, nil

	case schedulerv1.TransitionKind_TRANSITION_KIND_REMOVE_UNCONDITIONAL:
		// 🔴 Refused, not served. This build has no unconditional delete: the
		// guard used to be the caller reading the row first, and a node that
		// still asks for this one is a node still deciding for itself whether
		// somebody else's sandbox may be destroyed.
		return nil, s.fail("TransitionSandbox",
			fmt.Errorf("%w: the unconditional remove is no longer served; quote a generation",
				pausedregistry.ErrInvalidArgument))

	default:
		return nil, s.fail("TransitionSandbox",
			fmt.Errorf("%w: transition kind %q is not one this build serves", pausedregistry.ErrInvalidArgument, req.GetKind()))
	}
}

// markRunningOutcomeToProto maps the store's answer onto the wire enum.
//
// An outcome this build does not know maps to UNSPECIFIED rather than to a
// plausible neighbour: the caller's response to "untracked" is to carry on and
// its response to "held elsewhere" is to tear a sandbox down, and guessing
// between them is worse than saying nothing.
func markRunningOutcomeToProto(outcome pausedregistry.MarkRunningOutcome) schedulerv1.MarkRunningOutcome {
	switch outcome {
	case pausedregistry.MarkRunningUntracked:
		return schedulerv1.MarkRunningOutcome_MARK_RUNNING_OUTCOME_UNTRACKED
	case pausedregistry.MarkRunningAdopted:
		return schedulerv1.MarkRunningOutcome_MARK_RUNNING_OUTCOME_ADOPTED
	case pausedregistry.MarkRunningHeldElsewhere:
		return schedulerv1.MarkRunningOutcome_MARK_RUNNING_OUTCOME_HELD_ELSEWHERE
	default:
		return schedulerv1.MarkRunningOutcome_MARK_RUNNING_OUTCOME_UNSPECIFIED
	}
}

// conflictReasonToProto maps the store's conflict classification onto the wire.
func conflictReasonToProto(reason pausedregistry.ConflictReason) schedulerv1.ConflictReason {
	switch reason {
	case pausedregistry.ConflictReasonLiveElsewhere:
		return schedulerv1.ConflictReason_CONFLICT_REASON_LIVE_ELSEWHERE
	case pausedregistry.ConflictReasonClaimLost:
		return schedulerv1.ConflictReason_CONFLICT_REASON_CLAIM_LOST
	default:
		return schedulerv1.ConflictReason_CONFLICT_REASON_UNSPECIFIED
	}
}

// renewalsPerLease is how many renewals a lease must outlive: a TTL has to
// survive two missed ones, so it must be at least three intervals long.
const renewalsPerLease = 3

// checkRenewalCadence notices a node whose lease TTL and renewal interval
// disagree, and does nothing about it beyond saying so.
//
// 🔴 The reason it can only observe: the TTL belongs to the node (see
// Store.WithLeaseTTL) and so does the interval, and this process sees them only
// because the node reports them. Refusing a renewal on the strength of that
// would expire the rows of the one node already at risk of exactly that — it
// would convert a configuration smell into the outage the invariant exists to
// prevent. A node that does not report its interval at all (an older build)
// is not judged.
func (s *PausedRegistryService) checkRenewalCadence(nodeID string, leaseTTLMillis, intervalMillis int64) {
	if leaseTTLMillis <= 0 || intervalMillis <= 0 {
		return
	}
	if leaseTTLMillis >= renewalsPerLease*intervalMillis {
		return
	}

	registryLeaseTTLTooShort.WithLabelValues(nodeID).Inc()
	s.log.Warn("node reports a lease TTL that leaves no room for two missed renewals",
		zap.String("node_id", nodeID),
		zap.Duration("lease_ttl", time.Duration(leaseTTLMillis)*time.Millisecond),
		zap.Duration("reconcile_interval", time.Duration(intervalMillis)*time.Millisecond),
		zap.Duration("minimum_ttl", time.Duration(renewalsPerLease*intervalMillis)*time.Millisecond),
	)
}

// requestField names one optional field of a transition request.
type requestField uint8

const (
	fieldGeneration requestField = 1 << iota
	fieldMetadata
	fieldSnapshot
)

// rejectFields refuses a request that carries a field its kind never writes.
//
// 🔴 Refused, not ignored. A caller that quotes a generation believes the write
// is fenced; one that sends metadata believes it was stored. Neither is true
// for the kinds listed here, and both mistakes are invisible from the caller's
// side — the response says the transition succeeded, because it did.
func rejectFields(req *schedulerv1.TransitionSandboxRequest, unused requestField) error {
	if unused&fieldGeneration != 0 && req.ExpectGeneration != nil {
		return fmt.Errorf("%w: %v quotes a generation, but it is not a conditional write",
			pausedregistry.ErrInvalidArgument, req.GetKind())
	}
	if unused&fieldMetadata != 0 && len(req.GetMetadataJson()) != 0 {
		return fmt.Errorf("%w: %v carries metadata, which only begin_pause records",
			pausedregistry.ErrInvalidArgument, req.GetKind())
	}
	if unused&fieldSnapshot != 0 && strings.TrimSpace(req.GetSnapshotId()) != "" {
		return fmt.Errorf("%w: %v names a snapshot, which only complete_pause records",
			pausedregistry.ErrInvalidArgument, req.GetKind())
	}
	return nil
}

func requireGeneration(req *schedulerv1.TransitionSandboxRequest) (int64, error) {
	if req.ExpectGeneration == nil {
		return 0, fmt.Errorf("%w: %v is a conditional write and quotes no generation",
			pausedregistry.ErrInvalidArgument, req.GetKind())
	}
	return req.GetExpectGeneration(), nil
}

// ─────────────────────────────────────────────────────────────────────────────
// AcquireSandbox
// ─────────────────────────────────────────────────────────────────────────────

// AcquireSandbox takes a sandbox for a resume.
//
// All four outcomes are successful responses. Only the first hands the caller
// anything to do; the other three tell it where the sandbox actually is, which
// is what lets a node redirect rather than report a bare failure — and none of
// them may be confused with this process being unable to answer, which is the
// only thing that produces an error here.
func (s *PausedRegistryService) AcquireSandbox(ctx context.Context, req *schedulerv1.AcquireSandboxRequest) (*schedulerv1.AcquireSandboxResponse, error) {
	if err := s.admit(req.GetClusterId()); err != nil {
		return nil, s.fail("AcquireSandbox", err)
	}

	store := s.leaseStore(req.GetLeaseTtlMillis())
	claim, err := store.ClaimForResume(ctx, req.GetClusterId(), req.GetSandboxId(), req.GetNodeId())
	if err != nil {
		return nil, s.fail("AcquireSandbox", err)
	}

	resp := &schedulerv1.AcquireSandboxResponse{}
	switch claim.Outcome {
	case pausedregistry.ClaimOutcomeClaimed:
		if claim.Entry == nil {
			// Unreachable unless the store's own contract broke. Reported as a
			// failure rather than as an empty claim: a claim message with no
			// entry would be read as "granted" by anything that checks the
			// oneof and not its contents.
			return nil, s.fail("AcquireSandbox",
				fmt.Errorf("%w: claim granted without an entry", pausedregistry.ErrInvalidRecord))
		}
		resp.Outcome = &schedulerv1.AcquireSandboxResponse_Claimed{
			Claimed: &schedulerv1.AcquiredSandbox{
				Entry:         registryEntryToProto(*claim.Entry),
				MetadataJson:  claim.Entry.Metadata,
				PreviousState: string(claim.PreviousState),
			},
		}
	case pausedregistry.ClaimOutcomeNotFound:
		resp.Outcome = &schedulerv1.AcquireSandboxResponse_NotFound{NotFound: &schedulerv1.AcquireNotFound{}}
	case pausedregistry.ClaimOutcomeNotReady:
		resp.Outcome = &schedulerv1.AcquireSandboxResponse_NotReady{
			NotReady: &schedulerv1.AcquireOriginRef{OriginNodeId: claim.OriginNodeID},
		}
	case pausedregistry.ClaimOutcomeConflict:
		resp.Outcome = &schedulerv1.AcquireSandboxResponse_Conflict{
			Conflict: &schedulerv1.AcquireOriginRef{
				OriginNodeId: claim.OriginNodeID,
				Reason:       conflictReasonToProto(claim.ConflictReason),
			},
		}
	default:
		return nil, s.fail("AcquireSandbox",
			fmt.Errorf("%w: claim outcome %q is not one this build serves", pausedregistry.ErrInvalidRecord, claim.Outcome))
	}

	registryWriteRPCs.WithLabelValues("AcquireSandbox", codes.OK.String()).Inc()
	return resp, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// RenewNodeLease / ReleaseNodeHoldings
// ─────────────────────────────────────────────────────────────────────────────

// RenewNodeLease extends the lease on everything a node reports holding.
//
// The whole roster is passed straight through. Which of those the node has
// standing to renew is decided by the statement's predicate, not here and not
// by the caller: a node that listed a sandbox it does not hold must not thereby
// extend somebody else's lease.
func (s *PausedRegistryService) RenewNodeLease(ctx context.Context, req *schedulerv1.RenewNodeLeaseRequest) (*schedulerv1.RenewNodeLeaseResponse, error) {
	if err := s.admit(req.GetClusterId()); err != nil {
		return nil, s.fail("RenewNodeLease", err)
	}

	held := make([]pausedregistry.HeldSandbox, 0, len(req.GetHeld()))
	for _, h := range req.GetHeld() {
		entry := pausedregistry.HeldSandbox{SandboxID: h.GetSandboxId()}
		if h.ExpiresAtUnixMicros != nil {
			// Absent is not zero. Absent means the sandbox was asked never to
			// expire and reclamation leaves it alone forever; zero would be a
			// deadline in 1970, which reclamation acts on.
			deadline := time.UnixMicro(h.GetExpiresAtUnixMicros()).UTC()
			entry.ExpiresAt = &deadline
		}
		held = append(held, entry)
	}

	s.checkRenewalCadence(req.GetNodeId(), req.GetLeaseTtlMillis(), req.GetReconcileIntervalMillis())

	store := s.leaseStore(req.GetLeaseTtlMillis())
	renewed, err := store.RenewLease(ctx, req.GetClusterId(), req.GetNodeId(), held)
	if err != nil {
		return nil, s.fail("RenewNodeLease", err)
	}

	registryWriteRPCs.WithLabelValues("RenewNodeLease", codes.OK.String()).Inc()
	return &schedulerv1.RenewNodeLeaseResponse{Renewed: renewed}, nil
}

// ReleaseNodeHoldings frees what a previous process on the same machine held.
//
// 🔴 Served through the grace period, unlike reclamation. The evidence here is
// not a clock: a row saying "running on this node", read on behalf of a process
// that has just started and holds nothing, can only have been written by a
// previous process on that same machine, whose sandboxes were its children.
// Nothing about this process having been away weakens that, and withholding it
// would leave a restarting node's sandboxes stranded for a full lease.
func (s *PausedRegistryService) ReleaseNodeHoldings(ctx context.Context, req *schedulerv1.ReleaseNodeHoldingsRequest) (*schedulerv1.ReleaseNodeHoldingsResponse, error) {
	if err := s.admit(req.GetClusterId()); err != nil {
		return nil, s.fail("ReleaseNodeHoldings", err)
	}

	freed, err := s.store.ReleaseNodeHoldings(ctx, req.GetClusterId(), req.GetNodeId())
	if err != nil {
		return nil, s.fail("ReleaseNodeHoldings", err)
	}

	registryWriteRPCs.WithLabelValues("ReleaseNodeHoldings", codes.OK.String()).Inc()
	return &schedulerv1.ReleaseNodeHoldingsResponse{Released: freed.Released, Discarded: freed.Discarded}, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Plumbing
// ─────────────────────────────────────────────────────────────────────────────

// admit refuses everything until this process may speak for the table, and
// refuses any request naming a cluster it does not own.
func (s *PausedRegistryService) admit(clusterID string) error {
	if err := s.grace.Require(); err != nil {
		return err
	}
	requested := strings.TrimSpace(clusterID)
	if requested == "" {
		return fmt.Errorf("%w: cluster_id is required", pausedregistry.ErrInvalidArgument)
	}
	if s.clusterID != "" && !strings.EqualFold(requested, s.clusterID) {
		return fmt.Errorf("%w: this controller owns cluster %s, not %s",
			pausedregistry.ErrInvalidArgument, s.clusterID, requested)
	}
	return nil
}

// leaseStore is the store stamping the lease length the caller reported.
func (s *PausedRegistryService) leaseStore(leaseTTLMillis int64) pausedregistry.Store {
	if leaseTTLMillis <= 0 {
		return s.store
	}
	ttl := s.clampLeaseTTL(time.Duration(leaseTTLMillis) * time.Millisecond)
	s.warnOnLeaseDisagreement(ttl)
	return s.store.WithLeaseTTL(ttl)
}

// clampLeaseTTL raises a reported lease to the floor, never lowers it and never
// refuses it.
//
// 🔴 Why a floor exists at all. The invariant that keeps a lease longer than
// the cadence renewing it — "at least three reconcile intervals, so two missed
// renewals are survivable" — is checked in the *node's* configuration, because
// the node is the only place that knows both numbers. Once the TTL travels
// per-call, nothing on this side can re-check it: this process never learns any
// node's reconcile interval.
//
// 🔴 Why it does not need to be accurate. The bug this catches is a reported 0
// or 1ms — an unset field, or milliseconds confused with seconds. A value that
// is merely somewhat shorter than three intervals is already refused by the
// node's own configuration check, so a floor precise enough to catch *that*
// would duplicate a check that already exists, using a number this side would
// have to guess. Please do not make it precise; make it obviously absurd.
//
// 🔴 Why raising rather than refusing. A longer lease is harder to take over,
// which is the fail-safe direction. Refusing would fail the sandbox operation
// the request belongs to — a pause, a resume — over a number that was only ever
// advisory. The costs are not comparable.
func (s *PausedRegistryService) clampLeaseTTL(ttl time.Duration) time.Duration {
	if ttl >= s.leaseTTLFloor {
		return ttl
	}
	registryLeaseTTLClamped.Inc()
	s.log.Warn("a node reported a lease shorter than this controller will stamp; raising it",
		zap.Duration("node_lease_ttl", ttl),
		zap.Duration("floor", s.leaseTTLFloor),
		zap.String("effect", "the lease is stamped at the floor, so rows on that node take longer to become claimable rather than sooner"),
	)
	return s.leaseTTLFloor
}

// warnOnLeaseDisagreement says so, once per distinct value, when a node reports
// a lease length this process was not configured for.
//
// Not an error — the node's value is the one that governs, because the node is
// what renews on a cadence its own configuration keeps in step with the TTL.
// But a fleet where the two disagree is one where every graph drawn against the
// configured value is wrong, including the restart grace window's.
func (s *PausedRegistryService) warnOnLeaseDisagreement(ttl time.Duration) {
	if s.defaultLeaseTTL <= 0 || ttl == s.defaultLeaseTTL {
		return
	}
	s.mu.Lock()
	_, seen := s.warnedTTL[ttl]
	if !seen {
		s.warnedTTL[ttl] = struct{}{}
	}
	s.mu.Unlock()
	if seen {
		return
	}
	s.log.Warn("a node reports a lease length this controller was not configured for; the node's value is used",
		zap.Duration("node_lease_ttl", ttl),
		zap.Duration("configured_lease_ttl", s.defaultLeaseTTL),
	)
}

func (s *PausedRegistryService) ok(rpc string) (*schedulerv1.TransitionSandboxResponse, error) {
	registryWriteRPCs.WithLabelValues(rpc, codes.OK.String()).Inc()
	return &schedulerv1.TransitionSandboxResponse{}, nil
}

// fail turns a store error into a gRPC status.
//
// 🔴 The default arm is Unavailable, not Internal and never a nil error with an
// empty body. Whatever this process failed at, the one thing it must not do is
// let the answer be mistaken for the table's.
func (s *PausedRegistryService) fail(rpc string, err error) error {
	code := registryErrorCode(err)
	registryWriteRPCs.WithLabelValues(rpc, code.String()).Inc()
	if code == codes.Unavailable || code == codes.FailedPrecondition {
		s.log.Warn("paused registry write refused", zap.String("rpc", rpc), zap.String("code", code.String()), zap.Error(err))
	}
	return status.Error(code, err.Error())
}

// registryErrorCode maps a store error to the code the node's client keys off.
//
// Aborted and FailedPrecondition are deliberately different codes. A generation
// conflict means somebody else wrote first and the caller's view is stale,
// which its own code handles by re-reading; an invalid record means a row this
// build cannot make sense of, which nothing on the node can repair. Collapsing
// them would send a node into a re-read loop over a row that will never change.
func registryErrorCode(err error) codes.Code {
	switch {
	case err == nil:
		return codes.OK
	case errors.Is(err, pausedregistry.ErrInvalidArgument):
		return codes.InvalidArgument
	case errors.Is(err, pausedregistry.ErrGenerationConflict):
		return codes.Aborted
	case errors.Is(err, pausedregistry.ErrInvalidRecord):
		return codes.FailedPrecondition
	case errors.Is(err, pausedregistry.ErrNotReady), errors.Is(err, pausedregistry.ErrGracePeriod):
		return codes.Unavailable
	case errors.Is(err, context.Canceled):
		return codes.Canceled
	case errors.Is(err, context.DeadlineExceeded):
		return codes.DeadlineExceeded
	default:
		return codes.Unavailable
	}
}

// registryEntryToProto converts a row for the wire, without metadata.
//
// Times are microseconds. PostgreSQL stores timestamptz to the microsecond and
// the node's clock type is nanoseconds, so nanoseconds would round-trip a write
// as a silently truncated read; seconds would make two transitions within the
// same second unorderable.
func registryEntryToProto(entry pausedregistry.Entry) *schedulerv1.RegistryEntry {
	return &schedulerv1.RegistryEntry{
		SandboxId:           entry.SandboxID,
		ClusterId:           entry.ClusterID,
		State:               string(entry.State),
		Generation:          entry.Generation,
		OriginNodeId:        entry.OriginNodeID,
		ClaimedByNodeId:     entry.ClaimedByNodeID,
		SnapshotId:          entry.SnapshotID,
		PausedAtUnixMicros:  entry.PausedAt.UnixMicro(),
		UpdatedAtUnixMicros: entry.UpdatedAt.UnixMicro(),
	}
}

// RunReclaim drives the cluster's backstop pass until ctx is done.
//
// 🔴 A timer, not an RPC. This pass was never any node's business: it is the
// database owner acting on rows whose holder has stopped renewing *and* whose
// sandbox has outlived the deadline its own user set. Exposing it would let a
// node ask for a cluster-wide sweep, and the next stage would have to take that
// back.
//
// It is held back through the restart grace window. Nothing about it is urgent
// — every row it collects has been stranded for at least a sandbox lifetime
// already — and the leases it reads are ones this process is the reason nobody
// renewed.
func (s *PausedRegistryService) RunReclaim(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		s.log.Warn("paused registry reclamation disabled: no interval configured")
		return
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		if err := s.grace.RequireServing(); err != nil {
			s.log.Debug("skipping paused registry reclamation", zap.Error(err))
			continue
		}

		freed, err := s.store.ReclaimExpiredHoldings(ctx, s.clusterID)
		switch {
		case errors.Is(err, pausedregistry.ErrDiscardBreakerTripped):
			// Already logged at error level with the counts, by the breaker.
			// Nothing is retried and nothing is escalated: the next tick will
			// try again, and until an operator looks the rows stay where they
			// are, which is the recoverable half of the two outcomes.
			continue
		case err != nil:
			s.log.Warn("paused registry reclamation failed", zap.Error(err))
			continue
		}

		if freed.Released > 0 || freed.Discarded > 0 {
			s.log.Info("paused registry reclamation pass",
				zap.Uint64("released", freed.Released),
				zap.Uint64("discarded", freed.Discarded),
			)
		}
	}
}

// Phase reports the write surface's phase, for a health endpoint.
func (s *PausedRegistryService) Phase() (string, time.Duration, time.Duration) {
	phase, remaining, downtime := s.grace.Observation()
	return phase.String(), remaining, downtime
}
