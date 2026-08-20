package registry

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// The paused registry's three pause transitions, run inside somebody else's
// transaction.
//
// 🔴 They exist because the snapshot catalog needs them, and they are here
// rather than there for one reason: there is exactly one owner of the
// statements that move a `paused_sandboxes` row, and it is this package. The
// catalog owning a second copy would be one build's worth of drift away from a
// sandbox that is paused according to one table and running according to the
// other — which is the state the whole registry exists to make impossible.
//
// What the catalog owns instead is the transaction. A pause writes two facts —
// the sandbox is parked, and here is the snapshot it parked into — and putting
// them in one transaction is the entire reason the catalog is served from this
// process. These methods take the caller's pgx.Tx and never open one of their
// own, so the two halves cannot be committed apart.
//
// The statements themselves are the same constants the pool-based methods use.
// Nothing about the fencing, the generation CAS or the CHECK constraints
// changes because the caller brought its own transaction.

// TxView returns this store stamping the given lease length.
//
// 🔴 The lease TTL belongs to the node, not to this process: the node renews on
// its own cadence and its configuration is what keeps the TTL longer than that
// cadence. A TTL chosen here would expire rows underneath a node doing exactly
// what it was told. A non-positive value keeps this store's own default.
func (s *PostgresStore) TxView(ttl time.Duration) *PostgresStore {
	if ttl <= 0 || ttl == s.leaseTTL {
		return s
	}
	view := *s
	view.leaseTTL = ttl
	view.ownsPool = false
	return &view
}

// BeginPauseTx is BeginPause without the transaction.
//
// The classification of a zero-row upsert runs on the caller's transaction too,
// which is what makes it meaningful: it reports the version of the row this
// write did not match, not whatever a third party has committed since.
func (s *PostgresStore) BeginPauseTx(ctx context.Context, tx pgx.Tx, in BeginPauseInput) (BeganPause, error) {
	cluster, err := requireUUID("cluster_id", in.ClusterID)
	if err != nil {
		return BeganPause{}, err
	}
	sandbox, err := requireUUID("sandbox_id", in.SandboxID)
	if err != nil {
		return BeganPause{}, err
	}
	if strings.TrimSpace(in.OriginNodeID) == "" {
		return BeganPause{}, fmt.Errorf("%w: origin_node_id is required", ErrInvalidArgument)
	}
	// Checked for shape, never decoded — see BeginPause for why `null` in
	// particular has to be refused here rather than discovered on the node.
	if !isJSONObject(in.Metadata) {
		return BeganPause{}, fmt.Errorf("%w: sandbox %s has no metadata object", ErrInvalidRecord, sandbox)
	}
	execution, err := requireExecutionUUID(in.ExecutionID)
	if err != nil {
		return BeganPause{}, err
	}

	var (
		generation       int64
		previousSnapshot *string
	)
	err = tx.QueryRow(ctx, s.beginPauseSQL,
		sandbox, cluster, in.OriginNodeID, []byte(in.Metadata), s.ttlSeconds(), execution,
	).Scan(&generation, &previousSnapshot)
	if errors.Is(err, pgx.ErrNoRows) {
		return BeganPause{}, s.classifyRefusedPause(ctx, tx, cluster, sandbox, execution)
	}
	if err != nil {
		return BeganPause{}, fmt.Errorf("registry begin_pause: %w", err)
	}

	began := BeganPause{Generation: generation}
	if previousSnapshot != nil {
		began.PreviousSnapshotID = *previousSnapshot
	}
	return began, nil
}

// CompletePauseTx is CompletePause without the transaction.
func (s *PostgresStore) CompletePauseTx(ctx context.Context, tx pgx.Tx, clusterID, sandboxID string, expectGeneration int64, snapshotID string) error {
	cluster, err := requireUUID("cluster_id", clusterID)
	if err != nil {
		return err
	}
	sandbox, err := requireUUID("sandbox_id", sandboxID)
	if err != nil {
		return err
	}
	snapshot, err := requireUUID("snapshot_id", snapshotID)
	if err != nil {
		return err
	}

	tag, err := tx.Exec(ctx, completePauseSQL, sandbox, expectGeneration, snapshot, s.ttlSeconds(), cluster)
	if err != nil {
		return fmt.Errorf("registry complete_pause: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: sandbox %s is not publishing at generation %d", ErrGenerationConflict, sandbox, expectGeneration)
	}
	return nil
}

// MarkLocalOnlyTx is MarkLocalOnly without the transaction.
//
// 🔴 The row is kept, exactly as the pool-based one keeps it. The sandbox
// really is paused; only nobody but its origin node can bring it back. Deleting
// it would make "parked on its own node" indistinguishable from "resumed
// elsewhere, or destroyed", and reconciliation answers the second by throwing
// away what is by then the only copy.
func (s *PostgresStore) MarkLocalOnlyTx(ctx context.Context, tx pgx.Tx, clusterID, sandboxID string, expectGeneration int64) error {
	cluster, err := requireUUID("cluster_id", clusterID)
	if err != nil {
		return err
	}
	sandbox, err := requireUUID("sandbox_id", sandboxID)
	if err != nil {
		return err
	}

	tag, err := tx.Exec(ctx, markLocalOnlySQL, sandbox, expectGeneration, s.ttlSeconds(), cluster)
	if err != nil {
		return fmt.Errorf("registry mark_local_only: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: sandbox %s is not publishing at generation %d", ErrGenerationConflict, sandbox, expectGeneration)
	}
	return nil
}

// ObserveGenerationTx reads the generation a row carries now, so a caller whose
// conditional write lost can re-read against a number instead of blind.
//
// The bool is false when there is no row — which is a different thing from a
// generation of zero, and the caller's next move differs between them.
func (s *PostgresStore) ObserveGenerationTx(ctx context.Context, tx pgx.Tx, clusterID, sandboxID string) (int64, bool, error) {
	cluster, err := requireUUID("cluster_id", clusterID)
	if err != nil {
		return 0, false, err
	}
	sandbox, err := requireUUID("sandbox_id", sandboxID)
	if err != nil {
		return 0, false, err
	}

	var generation int64
	err = tx.QueryRow(ctx,
		`SELECT generation FROM paused_sandboxes WHERE sandbox_id = $1::uuid AND cluster_id = $2::uuid`,
		sandbox, cluster).Scan(&generation)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("registry observe_generation: %w", err)
	}
	return generation, true, nil
}
