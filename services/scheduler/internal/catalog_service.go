package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	schedulerv1 "agentenv/services/api/proto"
	"agentenv/services/scheduler/internal/catalog"
	pausedregistry "agentenv/services/scheduler/internal/registry"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var catalogRPCs = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "agentenv_scheduler_catalog_rpc_total",
		Help: "Snapshot catalog RPCs by method and gRPC code.",
	},
	[]string{"rpc", "code"},
)

// catalogRejections counts the refusals that are answered as successes.
//
// 🔴 Separate from the code label above, because they are OK on the wire and
// invisible in it. A cluster refusing every build for its ceiling and a cluster
// serving them all look identical in agentenv_scheduler_catalog_rpc_total, and
// the first is the one somebody has to be told about.
var catalogRejections = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "agentenv_scheduler_catalog_rejected_total",
		Help: "Snapshot catalog writes refused with a reason the caller acts on, by method and reason.",
	},
	[]string{"rpc", "reason"},
)

// catalogGate is the part of the registry's restart grace this service needs:
// whether the schema this build expects has been applied yet.
//
// 🔴 Shared with the paused registry deliberately. Both migrations run in one
// goroutine, one after the other, and the gate opens after both — so a catalog
// RPC answered before the gate is one whose tables may not exist. A refusal is
// the only honest answer there: an empty page reads as "this cluster has no
// snapshots", and a caller acts on that by deciding a snapshot is gone.
type catalogGate interface{ Require() error }

// SnapshotCatalogService serves the catalog to the nodes.
//
// It is a translation layer and nothing else — every decision about what a row
// may become lives in the store, next to the statement that makes it. What is
// decided here is what a failure looks like on the wire, and it is decided the
// same way every time: 🔴 no error is ever answered with an empty result. A
// snapshot missing from a listing means it is not there, and a caller collects
// artifacts on the strength of that.
//
// 🔴 The payload stays opaque all the way through. `committed_payload` and
// `build_error_json` arrive as bytes, are stored as bytes and go back as bytes;
// nothing in this file decodes either, and the day one of them grows a Go type
// is the day the node's domain model becomes a cross-process contract.
type SnapshotCatalogService struct {
	schedulerv1.UnimplementedSnapshotCatalogServer

	store catalog.Store
	gate  catalogGate
	log   *zap.Logger

	// clusterID is the one cluster this process owns the catalog for. Requests
	// naming another are refused rather than served; the scope still travels in
	// every statement, so this is a second check and not a replacement.
	clusterID string
}

// NewSnapshotCatalogService wires the store to the ten RPCs.
func NewSnapshotCatalogService(log *zap.Logger, store catalog.Store, gate catalogGate, clusterID string) *SnapshotCatalogService {
	if log == nil {
		log = zap.NewNop()
	}
	return &SnapshotCatalogService{
		store:     store,
		gate:      gate,
		log:       log,
		clusterID: strings.TrimSpace(clusterID),
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Writes
// ─────────────────────────────────────────────────────────────────────────────

// BeginSnapshot opens the row before any bytes exist.
func (s *SnapshotCatalogService) BeginSnapshot(ctx context.Context, req *schedulerv1.BeginSnapshotRequest) (*schedulerv1.BeginSnapshotResponse, error) {
	const rpc = "BeginSnapshot"
	if err := s.admit(req.GetClusterId()); err != nil {
		return nil, s.fail(rpc, err)
	}

	in := catalog.BeginInput{
		ClusterID:             req.GetClusterId(),
		NodeID:                req.GetNodeId(),
		SnapshotID:            req.GetSnapshotId(),
		SourceKind:            req.GetSourceKind(),
		SourceSandboxID:       req.GetSourceSandboxId(),
		CPUCount:              req.GetCpuCount(),
		MemoryMiB:             req.GetMemoryMib(),
		DiskSizeMiB:           req.GetDiskSizeMib(),
		Alias:                 req.GetAlias(),
		CreatedAtMs:           req.GetCreatedAtUnixMs(),
		SandboxStartedAtMs:    req.SandboxStartedAtUnixMs,
		Status:                req.GetStatus(),
		PublishingExecutionID: req.GetPublishingExecutionId(),
		Published:             req.GetPublished(),
		OriginNodeID:          req.GetOriginNodeId(),
	}
	if req.PausedTransition != nil {
		begun, err := pausedBeginFromProto(req.GetPausedTransition())
		if err != nil {
			return nil, s.fail(rpc, err)
		}
		in.Paused = begun
	}

	out, err := s.store.BeginSnapshot(ctx, in)
	if err != nil {
		return nil, s.fail(rpc, err)
	}
	if out.Rejected != nil {
		s.countRejection(rpc, out.Rejected)
		return &schedulerv1.BeginSnapshotResponse{
			Outcome: &schedulerv1.BeginSnapshotResponse_Rejected{Rejected: rejectedToProto(out.Rejected)},
		}, nil
	}
	if out.Row == nil {
		return nil, s.fail(rpc, fmt.Errorf("%w: the store reported neither a row nor a refusal", catalog.ErrInvalidRecord))
	}

	catalogRPCs.WithLabelValues(rpc, codes.OK.String()).Inc()
	return &schedulerv1.BeginSnapshotResponse{
		Outcome: &schedulerv1.BeginSnapshotResponse_Began{Began: &schedulerv1.BeganSnapshot{
			Row:                snapshotRowToProto(out.Row),
			Generation:         out.Generation,
			PreviousSnapshotId: out.PreviousSnapshotID,
		}},
	}, nil
}

// CommitSnapshot flips a row to `ready`.
func (s *SnapshotCatalogService) CommitSnapshot(ctx context.Context, req *schedulerv1.CommitSnapshotRequest) (*schedulerv1.CommitSnapshotResponse, error) {
	const rpc = "CommitSnapshot"
	if err := s.admit(req.GetClusterId()); err != nil {
		return nil, s.fail(rpc, err)
	}

	in := catalog.CommitInput{
		ClusterID:             req.GetClusterId(),
		NodeID:                req.GetNodeId(),
		SnapshotID:            req.GetSnapshotId(),
		CommittedPayload:      req.GetCommittedPayload(),
		CommittedSchema:       req.GetCommittedSchema(),
		CPUCount:              req.CpuCount,
		MemoryMiB:             req.MemoryMib,
		DiskSizeMiB:           req.DiskSizeMib,
		Alias:                 req.GetAlias(),
		UpdatedAtMs:           req.GetUpdatedAtUnixMs(),
		PublishingExecutionID: req.GetPublishingExecutionId(),
		Published:             req.GetPublished(),
		OriginNodeID:          req.GetOriginNodeId(),
	}
	if req.PausedTransition != nil {
		finish, err := pausedFinishFromProto(req.GetPausedTransition(), true)
		if err != nil {
			return nil, s.fail(rpc, err)
		}
		// 🔴 The two halves have to agree about who can start this sandbox.
		// complete_pause with published=false hands the registry row to any
		// node while the catalog says only the origin has the bytes, and a
		// resume elsewhere is then granted a claim it cannot honour.
		// mark_local_only with published=true is the same disagreement written
		// the other way round. Neither is a state to record.
		if finish.LocalOnly == req.GetPublished() {
			return nil, s.fail(rpc, fmt.Errorf(
				"%w: the registry transition and `published` disagree: %v says the sandbox is parked on its origin, published=%v says any node can start it",
				catalog.ErrInvalidArgument, req.GetPausedTransition().GetKind(), req.GetPublished()))
		}
		in.Paused = finish
	}

	out, err := s.store.CommitSnapshot(ctx, in)
	if err != nil {
		return nil, s.fail(rpc, err)
	}
	if out.Rejected != nil {
		s.countRejection(rpc, out.Rejected)
		return &schedulerv1.CommitSnapshotResponse{
			Outcome: &schedulerv1.CommitSnapshotResponse_Rejected{Rejected: rejectedToProto(out.Rejected)},
		}, nil
	}
	if out.Row == nil {
		return nil, s.fail(rpc, fmt.Errorf("%w: the store reported neither a row nor a refusal", catalog.ErrInvalidRecord))
	}

	catalogRPCs.WithLabelValues(rpc, codes.OK.String()).Inc()
	return &schedulerv1.CommitSnapshotResponse{
		Outcome: &schedulerv1.CommitSnapshotResponse_Committed{Committed: snapshotRowToProto(out.Row)},
	}, nil
}

// FailSnapshot records that a capture or a build produced nothing to run.
func (s *SnapshotCatalogService) FailSnapshot(ctx context.Context, req *schedulerv1.FailSnapshotRequest) (*schedulerv1.FailSnapshotResponse, error) {
	const rpc = "FailSnapshot"
	if err := s.admit(req.GetClusterId()); err != nil {
		return nil, s.fail(rpc, err)
	}

	in := catalog.FailInput{
		ClusterID:       req.GetClusterId(),
		NodeID:          req.GetNodeId(),
		SnapshotID:      req.GetSnapshotId(),
		BuildError:      json.RawMessage(req.GetBuildErrorJson()),
		UpdatedAtMs:     req.GetUpdatedAtUnixMs(),
		FailActiveBuild: req.GetFailActiveBuild(),
	}
	if req.PausedTransition != nil {
		finish, err := pausedFinishFromProto(req.GetPausedTransition(), false)
		if err != nil {
			return nil, s.fail(rpc, err)
		}
		in.Paused = finish
	}

	out, err := s.store.FailSnapshot(ctx, in)
	if err != nil {
		return nil, s.fail(rpc, err)
	}
	if out.Rejected != nil {
		s.countRejection(rpc, out.Rejected)
		return &schedulerv1.FailSnapshotResponse{
			Outcome: &schedulerv1.FailSnapshotResponse_Rejected{Rejected: rejectedToProto(out.Rejected)},
		}, nil
	}
	if out.Row == nil {
		return nil, s.fail(rpc, fmt.Errorf("%w: the store reported neither a row nor a refusal", catalog.ErrInvalidRecord))
	}

	catalogRPCs.WithLabelValues(rpc, codes.OK.String()).Inc()
	return &schedulerv1.FailSnapshotResponse{
		Outcome: &schedulerv1.FailSnapshotResponse_Failed{Failed: snapshotRowToProto(out.Row)},
	}, nil
}

// DeleteSnapshot soft-deletes one row.
func (s *SnapshotCatalogService) DeleteSnapshot(ctx context.Context, req *schedulerv1.DeleteSnapshotRequest) (*schedulerv1.DeleteSnapshotResponse, error) {
	const rpc = "DeleteSnapshot"
	if err := s.admit(req.GetClusterId()); err != nil {
		return nil, s.fail(rpc, err)
	}

	deleted, err := s.store.DeleteSnapshot(ctx, req.GetClusterId(), req.GetIdOrAlias(), req.GetDeletedAtUnixMs())
	if err != nil {
		return nil, s.fail(rpc, err)
	}

	catalogRPCs.WithLabelValues(rpc, codes.OK.String()).Inc()
	resp := &schedulerv1.DeleteSnapshotResponse{Deleted: deleted.Deleted}
	if deleted.Row != nil {
		resp.Row = snapshotRowToProto(deleted.Row)
	}
	return resp, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Reads
// ─────────────────────────────────────────────────────────────────────────────

// GetSnapshot reads one row by id or alias.
func (s *SnapshotCatalogService) GetSnapshot(ctx context.Context, req *schedulerv1.GetSnapshotRequest) (*schedulerv1.GetSnapshotResponse, error) {
	const rpc = "GetSnapshot"
	if err := s.admit(req.GetClusterId()); err != nil {
		return nil, s.fail(rpc, err)
	}

	row, err := s.store.GetSnapshot(ctx, req.GetClusterId(), req.GetIdOrAlias(), catalog.ReadOptions{
		// 🔴 Inverted here, and only here. The wire field is negative so that
		// its zero value — what a caller that forgot it sends — is the safe
		// reading; the store's option is positive so that a statement reads the
		// way its predicate does. The negation is the seam between the two.
		OnlyReady: !req.GetAllowAnyStatus(),
		WithBuild: req.GetWithBuild(),
	})
	if err != nil {
		return nil, s.fail(rpc, err)
	}

	catalogRPCs.WithLabelValues(rpc, codes.OK.String()).Inc()
	resp := &schedulerv1.GetSnapshotResponse{}
	if row != nil {
		resp.Row = snapshotRowToProto(row)
	}
	return resp, nil
}

// ListSnapshots reads one keyset page.
func (s *SnapshotCatalogService) ListSnapshots(ctx context.Context, req *schedulerv1.ListSnapshotsRequest) (*schedulerv1.ListSnapshotsResponse, error) {
	const rpc = "ListSnapshots"
	if err := s.admit(req.GetClusterId()); err != nil {
		return nil, s.fail(rpc, err)
	}

	in := catalog.ListInput{
		ClusterID: req.GetClusterId(),
		Filter:    filterFromProto(req.GetFilter()),
		Limit:     req.GetLimit(),
		ReadOptions: catalog.ReadOptions{
			// Inverted; see GetSnapshot.
			OnlyReady: !req.GetAllowAnyStatus(),
			WithBuild: req.GetWithBuild(),
		},
	}
	if req.Cursor != nil {
		in.Cursor = &catalog.Cursor{
			CreatedAtMs: req.GetCursor().GetCreatedAtUnixMs(),
			SnapshotID:  req.GetCursor().GetSnapshotId(),
		}
	}

	page, err := s.store.ListSnapshots(ctx, in)
	if err != nil {
		return nil, s.fail(rpc, err)
	}

	catalogRPCs.WithLabelValues(rpc, codes.OK.String()).Inc()
	resp := &schedulerv1.ListSnapshotsResponse{Rows: make([]*schedulerv1.SnapshotRow, 0, len(page.Rows))}
	for i := range page.Rows {
		resp.Rows = append(resp.Rows, snapshotRowToProto(&page.Rows[i]))
	}
	if page.Next != nil {
		resp.NextCursor = &schedulerv1.SnapshotCursor{
			CreatedAtUnixMs: page.Next.CreatedAtMs,
			SnapshotId:      page.Next.SnapshotID,
		}
	}
	return resp, nil
}

// ResolveAlias answers which snapshot an alias names.
func (s *SnapshotCatalogService) ResolveAlias(ctx context.Context, req *schedulerv1.ResolveAliasRequest) (*schedulerv1.ResolveAliasResponse, error) {
	const rpc = "ResolveAlias"
	if err := s.admit(req.GetClusterId()); err != nil {
		return nil, s.fail(rpc, err)
	}

	// Inverted; see GetSnapshot.
	target, err := s.store.ResolveAlias(ctx, req.GetClusterId(), req.GetAlias(), !req.GetAllowAnyStatus())
	if err != nil {
		return nil, s.fail(rpc, err)
	}

	catalogRPCs.WithLabelValues(rpc, codes.OK.String()).Inc()
	if target == nil {
		// An alias that names nothing, and one whose snapshot the ready
		// predicate excluded, are the same answer here and deliberately so:
		// both mean there is nothing this caller may act on.
		return &schedulerv1.ResolveAliasResponse{}, nil
	}
	return &schedulerv1.ResolveAliasResponse{
		SnapshotId:   target.SnapshotID,
		Published:    target.Published,
		OriginNodeId: target.OriginNodeID,
	}, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Builds
// ─────────────────────────────────────────────────────────────────────────────

// StartBuild admits one build.
func (s *SnapshotCatalogService) StartBuild(ctx context.Context, req *schedulerv1.StartBuildRequest) (*schedulerv1.StartBuildResponse, error) {
	const rpc = "StartBuild"
	if err := s.admit(req.GetClusterId()); err != nil {
		return nil, s.fail(rpc, err)
	}

	out, err := s.store.StartBuild(ctx, catalog.StartBuildInput{
		ClusterID:     req.GetClusterId(),
		NodeID:        req.GetNodeId(),
		BuildID:       req.GetBuildId(),
		TemplateID:    req.GetTemplateId(),
		StartedAtMs:   req.GetStartedAtUnixMs(),
		HeartbeatAtMs: req.GetHeartbeatAtUnixMs(),
	})
	if err != nil {
		return nil, s.fail(rpc, err)
	}
	if out.Rejected != nil {
		s.countRejection(rpc, out.Rejected)
		return &schedulerv1.StartBuildResponse{
			Outcome: &schedulerv1.StartBuildResponse_Rejected{Rejected: rejectedToProto(out.Rejected)},
		}, nil
	}
	if out.Build == nil || out.Snapshot == nil {
		return nil, s.fail(rpc, fmt.Errorf("%w: a build was admitted without a row to show for it", catalog.ErrInvalidRecord))
	}

	catalogRPCs.WithLabelValues(rpc, codes.OK.String()).Inc()
	return &schedulerv1.StartBuildResponse{
		Outcome: &schedulerv1.StartBuildResponse_Started{Started: &schedulerv1.StartedBuild{
			Build:    buildRowToProto(out.Build),
			Snapshot: snapshotRowToProto(out.Snapshot),
		}},
	}, nil
}

// RenewBuildLease records that the builder is still alive.
func (s *SnapshotCatalogService) RenewBuildLease(ctx context.Context, req *schedulerv1.RenewBuildLeaseRequest) (*schedulerv1.RenewBuildLeaseResponse, error) {
	const rpc = "RenewBuildLease"
	if err := s.admit(req.GetClusterId()); err != nil {
		return nil, s.fail(rpc, err)
	}

	live, err := s.store.RenewBuildLease(ctx, catalog.RenewBuildLeaseInput{
		ClusterID:     req.GetClusterId(),
		NodeID:        req.GetNodeId(),
		BuildID:       req.GetBuildId(),
		HeartbeatAtMs: req.GetHeartbeatAtUnixMs(),
	})
	if err != nil {
		return nil, s.fail(rpc, err)
	}
	if !live {
		// 🔴 Logged, because this is the only notice a builder gets that its
		// template has been handed to somebody else, and the shape of the bug
		// it prevents — two builders publishing into one template — is not one
		// anybody finds afterwards.
		s.log.Info("build lease renewal refused: this build is no longer the live one",
			zap.String("build_id", req.GetBuildId()),
			zap.String("node_id", req.GetNodeId()),
		)
	}

	catalogRPCs.WithLabelValues(rpc, codes.OK.String()).Inc()
	return &schedulerv1.RenewBuildLeaseResponse{Live: live}, nil
}

// GetBuild reads one build row, without the ready predicate.
func (s *SnapshotCatalogService) GetBuild(ctx context.Context, req *schedulerv1.GetBuildRequest) (*schedulerv1.GetBuildResponse, error) {
	const rpc = "GetBuild"
	if err := s.admit(req.GetClusterId()); err != nil {
		return nil, s.fail(rpc, err)
	}

	row, err := s.store.GetBuild(ctx, req.GetClusterId(), req.GetBuildId())
	if err != nil {
		return nil, s.fail(rpc, err)
	}

	catalogRPCs.WithLabelValues(rpc, codes.OK.String()).Inc()
	resp := &schedulerv1.GetBuildResponse{}
	if row != nil {
		resp.Build = buildRowToProto(row)
	}
	return resp, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// The reaper's driver
// ─────────────────────────────────────────────────────────────────────────────

// RunBuildReaper drives the reaping pass until ctx is done.
//
// 🔴 A timer, not an RPC. Which builds have stopped reporting is the database
// owner's business, and exposing it would let one node ask for a cluster-wide
// sweep that ends another node's builds.
//
// 🔴 It is not optional wherever `builds_one_active_per_template` exists. That
// index makes one stranded build a template nobody can build again, so shipping
// the index without this loop converts today's leak into an outage for that
// template. Nothing here starts it; whoever wires the build path is who must.
func (s *SnapshotCatalogService) RunBuildReaper(ctx context.Context, interval, ttl time.Duration) {
	if interval <= 0 || ttl <= 0 {
		s.log.Warn("snapshot catalog build reaper disabled: no interval or no TTL configured",
			zap.Duration("interval", interval), zap.Duration("ttl", ttl))
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
		if err := s.gate.Require(); err != nil {
			continue
		}
		reaped, err := s.store.ReapExpiredBuilds(ctx, catalog.ReapInput{
			ClusterID: s.clusterID,
			NowMs:     time.Now().UnixMilli(),
			TTLMs:     ttl.Milliseconds(),
		})
		if err != nil {
			s.log.Warn("snapshot catalog build reaping pass failed", zap.Error(err))
			continue
		}
		if len(reaped) > 0 {
			s.log.Info("snapshot catalog build reaping pass", zap.Int("reaped", len(reaped)))
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Plumbing
// ─────────────────────────────────────────────────────────────────────────────

// admit refuses everything until this process may speak for the catalog, and
// refuses any request naming a cluster it does not own.
func (s *SnapshotCatalogService) admit(clusterID string) error {
	if s.store == nil {
		return fmt.Errorf("%w: this process serves no snapshot catalog", pausedregistry.ErrNotReady)
	}
	if s.gate != nil {
		if err := s.gate.Require(); err != nil {
			return err
		}
	}
	requested := strings.TrimSpace(clusterID)
	if requested == "" {
		return fmt.Errorf("%w: cluster_id is required", catalog.ErrInvalidArgument)
	}
	if s.clusterID != "" && !strings.EqualFold(requested, s.clusterID) {
		return fmt.Errorf("%w: this controller owns cluster %s, not %s",
			catalog.ErrInvalidArgument, s.clusterID, requested)
	}
	return nil
}

func (s *SnapshotCatalogService) countRejection(rpc string, rejected *catalog.Rejected) {
	catalogRPCs.WithLabelValues(rpc, codes.OK.String()).Inc()
	catalogRejections.WithLabelValues(rpc, string(rejected.Reason)).Inc()
}

// fail turns a store error into a gRPC status.
//
// 🔴 The default arm is Unavailable, never Internal and never a nil error with
// an empty body. Whatever this process failed at, the one thing it must not do
// is let the answer be mistaken for the table's.
func (s *SnapshotCatalogService) fail(rpc string, err error) error {
	code := catalogErrorCode(err)
	catalogRPCs.WithLabelValues(rpc, code.String()).Inc()
	if code == codes.Unavailable || code == codes.FailedPrecondition {
		s.log.Warn("snapshot catalog request refused",
			zap.String("rpc", rpc), zap.String("code", code.String()), zap.Error(err))
	}
	return status.Error(code, err.Error())
}

// catalogErrorCode maps a store error to the code the node's client keys off.
//
// The refusals a caller acts on are not here: they travel in the response as a
// CatalogRejected, because a status code cannot say which row won or what the
// status is now. What is left are the failures nobody can act on.
func catalogErrorCode(err error) codes.Code {
	switch {
	case err == nil:
		return codes.OK
	case errors.Is(err, catalog.ErrInvalidArgument):
		return codes.InvalidArgument
	case errors.Is(err, catalog.ErrInvalidRecord):
		// A row this build cannot make sense of: an operator with a psql
		// prompt, not a retry. Without this arm it falls to the default and
		// reads as "unavailable", sending every node into a retry loop over a
		// write that can never succeed.
		return codes.FailedPrecondition
	case errors.Is(err, catalog.ErrNoPausedHalf):
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

// ─────────────────────────────────────────────────────────────────────────────
// Wire conversions
// ─────────────────────────────────────────────────────────────────────────────

func snapshotRowToProto(row *catalog.SnapshotRow) *schedulerv1.SnapshotRow {
	out := &schedulerv1.SnapshotRow{
		SnapshotId:             row.SnapshotID,
		ClusterId:              row.ClusterID,
		SourceKind:             row.SourceKind,
		SourceSandboxId:        row.SourceSandboxID,
		CpuCount:               row.CPUCount,
		MemoryMib:              row.MemoryMiB,
		DiskSizeMib:            row.DiskSizeMiB,
		Status:                 row.Status,
		StatusGroup:            row.StatusGroup,
		Alias:                  row.Alias,
		CreatedAtUnixMs:        row.CreatedAtMs,
		UpdatedAtUnixMs:        row.UpdatedAtMs,
		SandboxStartedAtUnixMs: row.SandboxStartedAtMs,
		CommittedPayload:       row.CommittedPayload,
		CommittedSchema:        row.CommittedSchema,
		BuildErrorJson:         row.BuildError,
		BuildStartedAtUnixMs:   row.BuildStartedAtMs,
		BuildFinishedAtUnixMs:  row.BuildFinishedAtMs,
		// 🔴 Projected, never a filter. See catalog.PinOriginIfUnpublished,
		// which is the one place either of these decides anything.
		Published:    row.Published,
		OriginNodeId: row.OriginNodeID,
	}
	return out
}

func buildRowToProto(row *catalog.BuildRow) *schedulerv1.BuildRow {
	return &schedulerv1.BuildRow{
		BuildId:           row.BuildID,
		TemplateId:        row.TemplateID,
		ClusterId:         row.ClusterID,
		Status:            row.Status,
		StatusGroup:       row.StatusGroup,
		NodeId:            row.NodeID,
		HeartbeatAtUnixMs: row.HeartbeatAtMs,
		CreatedAtUnixMs:   row.CreatedAtMs,
		StartedAtUnixMs:   row.StartedAtMs,
		FinishedAtUnixMs:  row.FinishedAtMs,
		ErrorReasonJson:   row.ErrorReason,
	}
}

func filterFromProto(f *schedulerv1.SnapshotFilter) catalog.Filter {
	if f == nil {
		return catalog.Filter{}
	}
	return catalog.Filter{
		SourceKinds:       f.GetSourceKinds(),
		AliasPrefix:       f.AliasPrefix,
		SnapshotIDs:       f.GetSnapshotIds(),
		SnapshotIDOrAlias: f.SnapshotIdOrAlias,
		SourceSandboxID:   f.SourceSandboxId,
		TemplateStatuses:  f.GetTemplateStatuses(),
	}
}

// rejectedToProto maps a refusal onto the wire.
//
// 🔴 An unknown reason maps to UNSPECIFIED rather than to a plausible
// neighbour. The caller's response to "the alias is taken" and to "you are a
// superseded incarnation" are opposite — retry under another name, versus stop
// and touch nothing — and guessing between them is worse than saying nothing.
func rejectedToProto(r *catalog.Rejected) *schedulerv1.CatalogRejected {
	out := &schedulerv1.CatalogRejected{
		ObservedStatus:        r.ObservedStatus,
		AliasHolderSnapshotId: r.AliasHolder,
		ActiveBuildId:         r.ActiveBuildID,
		ObservedGeneration:    r.ObservedGeneration,
	}
	switch r.Reason {
	case catalog.RejectionNotFound:
		out.Reason = schedulerv1.CatalogRejection_CATALOG_REJECTION_NOT_FOUND
	case catalog.RejectionStatusMismatch:
		out.Reason = schedulerv1.CatalogRejection_CATALOG_REJECTION_STATUS_MISMATCH
	case catalog.RejectionAliasTaken:
		out.Reason = schedulerv1.CatalogRejection_CATALOG_REJECTION_ALIAS_TAKEN
	case catalog.RejectionGenerationMismatch:
		out.Reason = schedulerv1.CatalogRejection_CATALOG_REJECTION_GENERATION_MISMATCH
	case catalog.RejectionExecutionSuperseded:
		out.Reason = schedulerv1.CatalogRejection_CATALOG_REJECTION_EXECUTION_SUPERSEDED
	case catalog.RejectionBuildInProgress:
		out.Reason = schedulerv1.CatalogRejection_CATALOG_REJECTION_BUILD_IN_PROGRESS
	case catalog.RejectionBuildQueueFull:
		out.Reason = schedulerv1.CatalogRejection_CATALOG_REJECTION_BUILD_QUEUE_FULL
	case catalog.RejectionAlreadyExists:
		out.Reason = schedulerv1.CatalogRejection_CATALOG_REJECTION_ALREADY_EXISTS
	default:
		out.Reason = schedulerv1.CatalogRejection_CATALOG_REJECTION_UNSPECIFIED
	}
	return out
}

// pausedBeginFromProto validates the registry half of a BeginSnapshot.
//
// 🔴 A field that means nothing for the requested kind is refused rather than
// dropped, exactly as on TransitionSandbox. Dropping is how a caller ends up
// believing it fenced a write that was never conditional.
func pausedBeginFromProto(t *schedulerv1.CatalogPausedTransition) (*catalog.PausedBegin, error) {
	if t.GetKind() != schedulerv1.TransitionKind_TRANSITION_KIND_BEGIN_PAUSE {
		return nil, fmt.Errorf("%w: opening a snapshot row carries begin_pause, not %v", catalog.ErrInvalidArgument, t.GetKind())
	}
	if t.ExpectGeneration != nil {
		return nil, fmt.Errorf("%w: begin_pause quotes no generation: it creates or takes over a row and has nothing to quote", catalog.ErrInvalidArgument)
	}
	if len(t.GetMetadataJson()) == 0 {
		return nil, fmt.Errorf("%w: begin_pause carries no metadata", catalog.ErrInvalidArgument)
	}
	if strings.TrimSpace(t.GetExecutionId()) == "" {
		return nil, fmt.Errorf("%w: begin_pause carries no execution id, and every write this build fences does", catalog.ErrInvalidArgument)
	}
	if err := rejectSandboxDeadline(t); err != nil {
		return nil, err
	}
	return &catalog.PausedBegin{
		SandboxID:      t.GetSandboxId(),
		Metadata:       json.RawMessage(t.GetMetadataJson()),
		ExecutionID:    t.GetExecutionId(),
		LeaseTTLMillis: t.GetLeaseTtlMillis(),
	}, nil
}

// pausedFinishFromProto validates the registry half of a commit or a failure.
//
// allowComplete is false on FailSnapshot: complete_pause names a snapshot the
// sandbox can come back from, and that call is the statement that there is
// none.
func pausedFinishFromProto(t *schedulerv1.CatalogPausedTransition, allowComplete bool) (*catalog.PausedFinish, error) {
	var localOnly bool
	switch t.GetKind() {
	case schedulerv1.TransitionKind_TRANSITION_KIND_COMPLETE_PAUSE:
		if !allowComplete {
			return nil, fmt.Errorf("%w: a failed snapshot can only park its sandbox as local-only", catalog.ErrInvalidArgument)
		}
	case schedulerv1.TransitionKind_TRANSITION_KIND_MARK_LOCAL_ONLY:
		localOnly = true
	default:
		return nil, fmt.Errorf("%w: finishing a snapshot row carries complete_pause or mark_local_only, not %v", catalog.ErrInvalidArgument, t.GetKind())
	}
	if t.ExpectGeneration == nil {
		return nil, fmt.Errorf("%w: %v is a conditional write and quotes no generation", catalog.ErrInvalidArgument, t.GetKind())
	}
	if len(t.GetMetadataJson()) != 0 {
		return nil, fmt.Errorf("%w: %v carries metadata, which only begin_pause records", catalog.ErrInvalidArgument, t.GetKind())
	}
	if strings.TrimSpace(t.GetExecutionId()) != "" {
		return nil, fmt.Errorf("%w: %v names an incarnation, which only begin_pause is fenced on", catalog.ErrInvalidArgument, t.GetKind())
	}
	if err := rejectSandboxDeadline(t); err != nil {
		return nil, err
	}
	return &catalog.PausedFinish{
		SandboxID:        t.GetSandboxId(),
		ExpectGeneration: t.GetExpectGeneration(),
		LeaseTTLMillis:   t.GetLeaseTtlMillis(),
		LocalOnly:        localOnly,
	}, nil
}

// rejectSandboxDeadline refuses a deadline none of these three transitions
// writes.
//
// Refused rather than ignored: mark_running and the lease renewal are what
// record a sandbox's deadline, and a caller that sent one here would believe it
// had been stored while reclamation went on treating the sandbox as having
// none.
func rejectSandboxDeadline(t *schedulerv1.CatalogPausedTransition) error {
	if t.SandboxExpiresAtUnixMicros != nil {
		return fmt.Errorf("%w: %v does not record a sandbox deadline; mark_running and the lease renewal do",
			catalog.ErrInvalidArgument, t.GetKind())
	}
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// The paused half
// ─────────────────────────────────────────────────────────────────────────────

// PausedHalfAdapter runs the paused registry's transitions inside the catalog's
// transaction.
//
// 🔴 It lives here, at the wiring layer, rather than in either package. The
// catalog must not import the registry — its whole contract is that it knows
// only scalar columns — and the registry must not import the catalog, because
// the catalog is a phase newer and will outlive it. What is left is a
// translation, and translations belong where the two are assembled.
type PausedHalfAdapter struct {
	store *pausedregistry.PostgresStore
}

// NewPausedHalfAdapter wires a registry store as the catalog's paused half.
// A nil store yields a nil adapter, which the catalog reads as "this process
// serves no registry" and refuses such writes rather than half-applying them.
func NewPausedHalfAdapter(store *pausedregistry.PostgresStore) catalog.PausedHalf {
	if store == nil {
		return nil
	}
	return &PausedHalfAdapter{store: store}
}

func (a *PausedHalfAdapter) Begin(ctx context.Context, tx pgx.Tx, in catalog.PausedBegin) (catalog.PausedBegan, error) {
	began, err := a.view(in.LeaseTTLMillis).BeginPauseTx(ctx, tx, pausedregistry.BeginPauseInput{
		ClusterID:    in.ClusterID,
		SandboxID:    in.SandboxID,
		OriginNodeID: in.OriginNodeID,
		Metadata:     in.Metadata,
		ExecutionID:  in.ExecutionID,
	})
	return catalog.PausedBegan{Generation: began.Generation, PreviousSnapshotID: began.PreviousSnapshotID}, translatePausedError(err)
}

func (a *PausedHalfAdapter) Complete(ctx context.Context, tx pgx.Tx, in catalog.PausedFinish, snapshotID string) error {
	return translatePausedError(a.view(in.LeaseTTLMillis).CompletePauseTx(ctx, tx,
		in.ClusterID, in.SandboxID, in.ExpectGeneration, snapshotID))
}

func (a *PausedHalfAdapter) MarkLocalOnly(ctx context.Context, tx pgx.Tx, in catalog.PausedFinish) error {
	return translatePausedError(a.view(in.LeaseTTLMillis).MarkLocalOnlyTx(ctx, tx,
		in.ClusterID, in.SandboxID, in.ExpectGeneration))
}

func (a *PausedHalfAdapter) ObserveGeneration(ctx context.Context, tx pgx.Tx, clusterID, sandboxID string) (int64, bool, error) {
	return a.store.ObserveGenerationTx(ctx, tx, clusterID, sandboxID)
}

func (a *PausedHalfAdapter) view(leaseTTLMillis int64) *pausedregistry.PostgresStore {
	if leaseTTLMillis <= 0 {
		return a.store
	}
	return a.store.TxView(time.Duration(leaseTTLMillis) * time.Millisecond)
}

// translatePausedError turns the registry's sentinels into the catalog's, so
// the catalog can classify a refusal without importing the registry.
func translatePausedError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, pausedregistry.ErrExecutionFenced):
		return fmt.Errorf("%w: %v", catalog.ErrPausedExecutionFenced, err)
	case errors.Is(err, pausedregistry.ErrGenerationConflict):
		return fmt.Errorf("%w: %v", catalog.ErrPausedGenerationMismatch, err)
	case errors.Is(err, pausedregistry.ErrInvalidArgument):
		return fmt.Errorf("%w: %v", catalog.ErrInvalidArgument, err)
	case errors.Is(err, pausedregistry.ErrInvalidRecord):
		return fmt.Errorf("%w: %v", catalog.ErrInvalidRecord, err)
	default:
		return err
	}
}
