package registry

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func timePtr(t time.Time) *time.Time { return &t }

// TestHolderIsAlwaysOriginNeverTheClaimant pins Holder() to origin_node_id in
// every one of the five states, including resuming with a claimant set.
//
// 🔴 This used to branch: resuming with a non-empty claimed_by_node_id
// answered the claimant, on the theory that a claim's origin_node_id "still
// names whoever holds the local artifacts" and so is "the wrong answer for
// exactly the rows that are moving" — true under --role all, where the
// process that calls claim_for_resume is the same machine that will run the
// VM, so the claimant is a legitimate routing target. Under --role api|node
// the claimant is an api-replica process (a Pod name, e.g.
// "agentenv-api-57f67787-dj9th") that structurally never reports a heartbeat,
// so routing to it always fails; origin_node_id, meanwhile, is the honest
// continuation of "where the bytes are right now" until mark_running repoints
// it at the new holder. The resuming/claimer case below is deliberately given
// a Pod-name-shaped claimant, not another origin-shaped string, so a
// regression back to the old branch cannot pass by accident with a
// same-shaped fixture.
func TestHolderIsAlwaysOriginNeverTheClaimant(t *testing.T) {
	const claimant = "agentenv-api-57f67787-dj9th"

	cases := []struct {
		name    string
		sandbox Sandbox
		want    string
	}{
		{
			name:    "running is held by its origin",
			sandbox: Sandbox{State: StateRunning, OriginNodeID: "aenv-master-01", ClaimedByNodeID: claimant},
			want:    "aenv-master-01",
		},
		{
			name:    "resuming with a claimant is still held by its origin, not the claimant",
			sandbox: Sandbox{State: StateResuming, OriginNodeID: "aenv-master-01", ClaimedByNodeID: claimant},
			want:    "aenv-master-01",
		},
		{
			name:    "resuming without a claimant is held by its origin",
			sandbox: Sandbox{State: StateResuming, OriginNodeID: "aenv-master-01"},
			want:    "aenv-master-01",
		},
		{
			name:    "paused is held by its origin",
			sandbox: Sandbox{State: StatePaused, OriginNodeID: "aenv-master-01"},
			want:    "aenv-master-01",
		},
		{
			name:    "local_only is held by its origin",
			sandbox: Sandbox{State: StateLocalOnly, OriginNodeID: "aenv-master-01"},
			want:    "aenv-master-01",
		},
		{
			name:    "publishing is held by its origin",
			sandbox: Sandbox{State: StatePublishing, OriginNodeID: "aenv-master-01"},
			want:    "aenv-master-01",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.sandbox.Holder(); got != tc.want {
				t.Fatalf("expected holder %q, got %q", tc.want, got)
			}
			// 🔴 The negative half: never the claimant, in the one state where
			// the old code could return it.
			if got := tc.sandbox.Holder(); tc.sandbox.ClaimedByNodeID != "" && got == tc.sandbox.ClaimedByNodeID {
				t.Fatalf("Holder() returned the claimant %q — routing this would always fail, "+
					"the claimant never reports a heartbeat", got)
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

// ParseState is the only place a caller-supplied state is turned into one of
// the five. It answers false rather than passing the input through, so a filter
// built on it cannot end up matching nothing and calling that an answer.
func TestParseStateAcceptsOnlyTheFiveKnownStates(t *testing.T) {
	for _, state := range KnownStates() {
		for _, spelling := range []string{string(state), strings.ToUpper(string(state)), "  " + string(state) + "\t"} {
			got, ok := ParseState(spelling)
			if !ok {
				t.Fatalf("expected %q to parse", spelling)
			}
			if got != state {
				t.Fatalf("expected %q to parse to %q, got %q", spelling, state, got)
			}
		}
	}

	for _, raw := range []string{"", "bogus", "pause", "paused ish", "local-only", "hibernating"} {
		if got, ok := ParseState(raw); ok {
			t.Fatalf("expected %q to be rejected, got %q", raw, got)
		}
	}
}

// The five are the ones the table's CHECK constraint pins. A build that grows a
// sixth state without adding it here would filter it out of every listing while
// still counting it in the metrics, so the two lists are held together.
func TestKnownStatesCoversEveryDeclaredState(t *testing.T) {
	want := map[State]bool{
		StatePublishing: false,
		StatePaused:     false,
		StateResuming:   false,
		StateLocalOnly:  false,
		StateRunning:    false,
	}
	for _, state := range KnownStates() {
		seen, declared := want[state]
		if !declared {
			t.Fatalf("KnownStates lists %q, which is not one of the declared states", state)
		}
		if seen {
			t.Fatalf("KnownStates lists %q twice", state)
		}
		want[state] = true
	}
	for state, seen := range want {
		if !seen {
			t.Fatalf("KnownStates is missing %q", state)
		}
	}
}
