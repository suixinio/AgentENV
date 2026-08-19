package registry

import (
	"context"
	"errors"
	"testing"
	"time"
)

func timePtr(t time.Time) *time.Time { return &t }

func TestHolderPrefersClaimerOnlyWhileResuming(t *testing.T) {
	cases := []struct {
		name    string
		sandbox Sandbox
		want    string
	}{
		{
			name:    "running is held by its origin",
			sandbox: Sandbox{State: StateRunning, OriginNodeID: "node-a", ClaimedByNodeID: "node-b"},
			want:    "node-a",
		},
		{
			// The claim deliberately leaves origin_node_id pointing at the node
			// that still holds the local artifacts, so origin is the wrong
			// answer for exactly the rows that are moving.
			name:    "resuming is held by its claimer",
			sandbox: Sandbox{State: StateResuming, OriginNodeID: "node-a", ClaimedByNodeID: "node-b"},
			want:    "node-b",
		},
		{
			name:    "resuming without a claimer falls back to origin",
			sandbox: Sandbox{State: StateResuming, OriginNodeID: "node-a"},
			want:    "node-a",
		},
		{
			name:    "paused is held by its origin",
			sandbox: Sandbox{State: StatePaused, OriginNodeID: "node-a"},
			want:    "node-a",
		},
		{
			name:    "local_only is held by its origin",
			sandbox: Sandbox{State: StateLocalOnly, OriginNodeID: "node-a"},
			want:    "node-a",
		},
		{
			name:    "publishing is held by its origin",
			sandbox: Sandbox{State: StatePublishing, OriginNodeID: "node-a"},
			want:    "node-a",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.sandbox.Holder(); got != tc.want {
				t.Fatalf("expected holder %q, got %q", tc.want, got)
			}
		})
	}
}

func TestLeaseExpiredMatchesTheNodePredicate(t *testing.T) {
	now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)

	cases := []struct {
		name    string
		sandbox Sandbox
		want    bool
	}{
		{
			name:    "lease in the future has not expired",
			sandbox: Sandbox{UpdatedAt: now.Add(-time.Hour), LeaseExpiresAt: timePtr(now.Add(time.Minute))},
			want:    false,
		},
		{
			name:    "lease in the past has expired",
			sandbox: Sandbox{UpdatedAt: now, LeaseExpiresAt: timePtr(now.Add(-time.Second))},
			want:    true,
		},
		{
			// COALESCE(lease_expires_at, updated_at): a row written before the
			// column existed reads as already expired, which is the safe
			// direction.
			name:    "null lease falls back to updated_at",
			sandbox: Sandbox{UpdatedAt: now.Add(-time.Second)},
			want:    true,
		},
		{
			name:    "null lease with a fresh updated_at has not expired",
			sandbox: Sandbox{UpdatedAt: now.Add(time.Second)},
			want:    false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.sandbox.LeaseExpired(now); got != tc.want {
				t.Fatalf("expected lease expired %v, got %v", tc.want, got)
			}
		})
	}
}

func TestInvalidOnlyCoversPausedWithoutSnapshot(t *testing.T) {
	if !(Sandbox{State: StatePaused}).Invalid() {
		t.Fatal("expected a paused row without a snapshot to be invalid")
	}
	if (Sandbox{State: StatePaused, SnapshotID: "snap"}).Invalid() {
		t.Fatal("expected a paused row with a snapshot to be valid")
	}
	// publishing has no snapshot yet by design, and local_only has none after a
	// failed upload; neither is a broken row.
	if (Sandbox{State: StatePublishing}).Invalid() {
		t.Fatal("expected a publishing row without a snapshot to be valid")
	}
	if (Sandbox{State: StateLocalOnly}).Invalid() {
		t.Fatal("expected a local_only row without a snapshot to be valid")
	}
}

// A disabled reader has to be distinguishable from a reader that simply has not
// managed a read yet: the first means "this cluster does not run a registry" and
// keeps today's behaviour, the second means "we do not know" and must never be
// answered as an authoritative absence.
func TestDisabledReaderIsDistinguishableFromCold(t *testing.T) {
	disabled := Disabled()
	if disabled.Ready() {
		t.Fatal("expected a disabled reader never to be ready")
	}
	if _, err := disabled.List(context.Background()); !errors.Is(err, ErrDisabled) {
		t.Fatalf("expected ErrDisabled from List, got %v", err)
	}
	if _, _, err := disabled.Get(context.Background(), "sandbox-a"); !errors.Is(err, ErrDisabled) {
		t.Fatalf("expected ErrDisabled from Get, got %v", err)
	}
	disabled.Close()

	// A configured reader pointed at a port nothing listens on: not ready
	// either, but its failure is a backend failure and not ErrDisabled.
	cold, err := New(context.Background(), Config{
		DSN:          "postgres://agentenv@127.0.0.1:1/agentenv?sslmode=disable&connect_timeout=1",
		QueryTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("expected a parseable dsn to build a reader, got %v", err)
	}
	defer cold.Close()

	if cold.Ready() {
		t.Fatal("expected a reader that has never read to report not ready")
	}
	_, err = cold.List(context.Background())
	if err == nil {
		t.Fatal("expected a read against an unreachable database to fail")
	}
	if errors.Is(err, ErrDisabled) {
		t.Fatalf("expected a backend failure rather than ErrDisabled, got %v", err)
	}
	if cold.Ready() {
		t.Fatal("expected a failed read to leave the reader not ready")
	}
}

func TestNewRejectsMissingAndMalformedDSN(t *testing.T) {
	if _, err := New(context.Background(), Config{}); err == nil {
		t.Fatal("expected an empty dsn to be rejected")
	}
	if _, err := New(context.Background(), Config{DSN: "://not a dsn"}); err == nil {
		t.Fatal("expected a malformed dsn to be rejected")
	}
}
