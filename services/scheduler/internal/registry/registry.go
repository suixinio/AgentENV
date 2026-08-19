// Package registry is a read-only view of the `paused_sandboxes` table that the
// AgentENV nodes own.
//
// The table's schema and every write to it belong to the node-side Rust
// registry (`src/orchestrator/paused_registry/`). This package never writes:
// it exists so the control plane can see what the nodes have agreed on, and
// reconcile that against what the nodes report over heartbeats. The read-only
// discipline is enforced at the database (see the pool's AfterConnect and the
// read-only transactions in postgres.go), not by review.
package registry

import (
	"context"
	"errors"
	"strings"
	"time"
)

// State is the row's lifecycle state. The five values are pinned by a CHECK
// constraint on the table, so anything else is a row this build does not
// understand rather than a state to handle.
type State string

const (
	// StatePublishing means the snapshot upload is in flight. The VM is
	// stopped and the row cannot yet be rebuilt anywhere else.
	StatePublishing State = "publishing"
	// StatePaused means a snapshot has been published and any node may claim
	// the sandbox.
	StatePaused State = "paused"
	// StateResuming means a node has taken the claim and is bringing the
	// sandbox back up. claimed_by_node_id names that node; origin_node_id
	// still names whoever holds the local artifacts.
	StateResuming State = "resuming"
	// StateLocalOnly means the snapshot upload failed and the only copy is on
	// the origin node's disk.
	StateLocalOnly State = "local_only"
	// StateRunning means the sandbox is live on origin_node_id.
	StateRunning State = "running"
)

// KnownStates lists the five states the table's CHECK constraint pins, in the
// order a reader is expected to see them explained. It exists so that anything
// validating a state against the set names the same five values this package
// defines, rather than repeating a list that can fall behind.
func KnownStates() []State {
	return []State{StatePublishing, StatePaused, StateResuming, StateLocalOnly, StateRunning}
}

// ParseState resolves user input to one of the five known states, case
// insensitively. The bool is false for anything else.
//
// 🔴 Callers must not fall back to filtering on the raw input when this fails.
// A filter on a state no row can hold matches nothing, and an empty list reads
// as "the registry holds no such rows" — which is how a mistyped state becomes
// a confident wrong answer instead of an error.
func ParseState(raw string) (State, bool) {
	trimmed := strings.TrimSpace(raw)
	for _, state := range KnownStates() {
		if strings.EqualFold(string(state), trimmed) {
			return state, true
		}
	}
	return "", false
}

// Sandbox is one registry row.
//
// It carries the two lease columns deliberately. The node-side read path
// (`ENTRY_COLUMNS` in postgres.rs) leaves them out, so no Rust consumer can see
// them — yet they are what decides whether a parked row can be taken over, and
// central reconciliation is the first place they can be observed at all.
type Sandbox struct {
	SandboxID       string
	ClusterID       string
	State           State
	Generation      int64
	OriginNodeID    string
	ClaimedByNodeID string
	SnapshotID      string
	PausedAt        time.Time
	UpdatedAt       time.Time
	// LeaseExpiresAt is nil when the column is NULL, which the node treats as
	// already expired — see LeaseExpired.
	LeaseExpiresAt *time.Time
	// SandboxExpiresAt is the deadline the sandbox's own owner set. Only
	// renew_lease writes it, and a NULL never matches a reclaim condition.
	SandboxExpiresAt *time.Time
}

// Holder returns the node this row makes authoritative for the sandbox.
//
// Only a resuming row is held by its claimer; every other state is held by
// origin_node_id. A claim deliberately leaves origin_node_id pointing at
// whoever still has the local artifacts, so comparing origin_node_id against a
// heartbeat roster gives the wrong answer for exactly the rows that are moving
// between nodes.
func (s Sandbox) Holder() string {
	if s.State == StateResuming && s.ClaimedByNodeID != "" {
		return s.ClaimedByNodeID
	}
	return s.OriginNodeID
}

// LeaseExpired mirrors the node's LEASE_EXPIRED predicate verbatim:
//
//	COALESCE(lease_expires_at, updated_at) < now()
//
// `now` must be the database clock the row was read against, not this process's
// wall clock: the two are already known to drift (begin_pause stamps updated_at
// from the node's clock) and adding a third clock would make the answer
// unfalsifiable.
func (s Sandbox) LeaseExpired(now time.Time) bool {
	return s.LeaseDeadline().Before(now)
}

// LeaseDeadline is the COALESCE half of LeaseExpired, exposed on its own so
// callers can ask about a lease that is about to lapse rather than one that
// already has.
func (s Sandbox) LeaseDeadline() time.Time {
	if s.LeaseExpiresAt != nil {
		return *s.LeaseExpiresAt
	}
	return s.UpdatedAt
}

// Invalid reports whether the row is one the node-side read path refuses.
//
// A paused row promises a cross-node resume from a snapshot; without the
// snapshot reference it cannot deliver one, and decoding it fails the *whole*
// get_many batch (postgres.rs) — so a single bad row silently freezes one
// machine's reconciliation. This should be zero at all times.
func (s Sandbox) Invalid() bool {
	return s.State == StatePaused && s.SnapshotID == ""
}

// Listing is one read of the table, together with the database clock it was
// read against. The two travel as a pair because every lease judgement has to
// be made against the same clock the rows were written by.
type Listing struct {
	Sandboxes []Sandbox
	Now       time.Time
}

var (
	// ErrDisabled is returned by every read when no DSN is configured. It is
	// how a caller tells "the feature is off" from "the feature is broken":
	// the first keeps today's behaviour, the second must be reported as
	// unavailable and never as an authoritative "no such row".
	ErrDisabled = errors.New("paused registry is not configured")
)

// Reader is the read-only registry surface.
type Reader interface {
	// Get returns one row. The bool is false only when the query succeeded and
	// matched nothing.
	Get(ctx context.Context, sandboxID string) (Sandbox, bool, error)
	// List returns every row in scope, plus the database clock.
	List(ctx context.Context) (Listing, error)
	// Ready reports whether this reader has ever completed a read. A false
	// answer means the caller must not treat a missing row as authoritative —
	// it has no idea yet what the table holds.
	Ready() bool
	// ClusterID is the cluster every read is scoped to, or empty when the
	// reader is unscoped and returns every cluster's rows.
	//
	// It is exposed so that anything comparing this table against another view
	// of the fleet can narrow that view to the same set of nodes. Carrying the
	// scope as a second setting alongside the reader is how the two halves of
	// such a comparison drift apart.
	ClusterID() string
	// Close releases the underlying resources. Safe to call on a disabled
	// reader.
	Close()
}

type disabledReader struct{}

// Disabled returns a Reader that is off: it never touches a database, is never
// ready, and answers every read with ErrDisabled.
func Disabled() Reader { return disabledReader{} }

func (disabledReader) Get(_ context.Context, _ string) (Sandbox, bool, error) {
	return Sandbox{}, false, ErrDisabled
}

func (disabledReader) List(_ context.Context) (Listing, error) {
	return Listing{}, ErrDisabled
}

func (disabledReader) Ready() bool { return false }

func (disabledReader) ClusterID() string { return "" }

func (disabledReader) Close() {}
