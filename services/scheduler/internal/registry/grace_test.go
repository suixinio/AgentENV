package registry

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.uber.org/zap"
)

// fakeExtender stands in for the database so the window's own arithmetic can be
// tested without one.
type fakeExtender struct {
	downtime time.Duration
	extended int64
	err      error
	calls    int
	cluster  string
	ttl      time.Duration
}

func (f *fakeExtender) ExtendLeases(_ context.Context, clusterID string, ttl time.Duration) (time.Duration, int64, error) {
	f.calls++
	f.cluster = clusterID
	f.ttl = ttl
	return f.downtime, f.extended, f.err
}

// TestNothingIsServedBeforeTheGateOpens is guard §3.1 at its narrowest point.
//
// 🔴 The failure being prevented is not "an error is missed". It is that a
// registry which cannot answer produces an *empty* answer, and the node reads
// absence as "the cluster has moved past this sandbox" and deletes its local
// artifacts. Cold has to be an error, and it has to be a different error from
// "no such row".
func TestNothingIsServedBeforeTheGateOpens(t *testing.T) {
	grace := NewGrace(time.Minute, zap.NewNop())

	if grace.Ready() {
		t.Fatal("a gate that has not run its restart pass reported ready")
	}
	if grace.Serving() {
		t.Fatal("a cold gate reported serving")
	}
	if err := grace.Require(); !errors.Is(err, ErrNotReady) {
		t.Fatalf("expected ErrNotReady, got %v", err)
	}
	if err := grace.RequireServing(); !errors.Is(err, ErrNotReady) {
		t.Fatalf("expected ErrNotReady, got %v", err)
	}
	if grace.allowsLeaseTakeover() {
		t.Fatal("a cold gate allowed a lapsed-lease takeover")
	}
}

func TestTheGateOpensInTwoStages(t *testing.T) {
	const window = 80 * time.Millisecond

	grace := NewGrace(window, zap.NewNop())
	ext := &fakeExtender{downtime: 5 * time.Minute, extended: 3}

	observed, err := grace.Enter(context.Background(), ext, stCluster)
	if err != nil {
		t.Fatalf("enter failed: %v", err)
	}
	if ext.calls != 1 || ext.cluster != stCluster || ext.ttl != window {
		t.Fatalf("the restart pass was not run as configured: %+v", ext)
	}
	if observed.Downtime != 5*time.Minute || observed.Extended != 3 {
		t.Fatalf("unexpected observation: %+v", observed)
	}

	// Stage one: reads and ordinary writes are served, and the two things that
	// take a sandbox away from the node holding it are not.
	if !grace.Ready() {
		t.Fatal("expected ready once the restart pass has run")
	}
	if grace.Serving() {
		t.Fatal("the grace window should not be over yet")
	}
	if err := grace.Require(); err != nil {
		t.Fatalf("ordinary requests must be served during the grace window: %v", err)
	}
	if err := grace.RequireServing(); !errors.Is(err, ErrGracePeriod) {
		t.Fatalf("expected ErrGracePeriod, got %v", err)
	}
	if grace.allowsLeaseTakeover() {
		t.Fatal("a lapsed-lease takeover was allowed during the grace window")
	}

	// Stage two.
	time.Sleep(window + 20*time.Millisecond)

	if !grace.Serving() {
		t.Fatal("the grace window never closed")
	}
	if err := grace.RequireServing(); err != nil {
		t.Fatalf("expected the window to be over, got %v", err)
	}
	if !grace.allowsLeaseTakeover() {
		t.Fatal("takeovers are still withheld after the window closed")
	}
}

func TestAFailedRestartPassLeavesTheGateCold(t *testing.T) {
	grace := NewGrace(time.Minute, zap.NewNop())
	ext := &fakeExtender{err: errors.New("database is on fire")}

	if _, err := grace.Enter(context.Background(), ext, stCluster); err == nil {
		t.Fatal("expected the failure to surface")
	}
	if grace.Ready() {
		t.Fatal("a gate whose restart pass failed must stay cold")
	}
	if err := grace.Require(); !errors.Is(err, ErrNotReady) {
		t.Fatalf("expected ErrNotReady, got %v", err)
	}
}

// TestANilGateServesEverything: a store constructed without one — which is what
// the contract tests and any direct user get — is not gated at all.
func TestANilGateServesEverything(t *testing.T) {
	var grace *Grace

	if !grace.Ready() || !grace.Serving() {
		t.Fatal("a nil gate should not be a closed one")
	}
	if err := grace.Require(); err != nil {
		t.Fatalf("nil gate refused a request: %v", err)
	}
	if err := grace.RequireServing(); err != nil {
		t.Fatalf("nil gate refused a serving request: %v", err)
	}
	if !grace.allowsLeaseTakeover() {
		t.Fatal("nil gate withheld a takeover")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// The restart pass itself
// ─────────────────────────────────────────────────────────────────────────────

// setLease rewrites the two timestamps a test needs to place a row relative to
// an outage. Seeding cannot do it: every row is written with now().
func (f *storeFixture) setLease(sandboxID, updatedAt, leaseExpires string) {
	f.t.Helper()

	sql := "UPDATE paused_sandboxes SET updated_at = " + updatedAt +
		", lease_expires_at = " + leaseExpires + " WHERE sandbox_id = $1::uuid"
	if _, err := f.pool.Exec(context.Background(), sql, sandboxID); err != nil {
		f.t.Fatalf("place %s in time failed: %v", sandboxID, err)
	}
}

// TestTheRestartPassExtendsALeaseTheOutageLapsed is the case the whole gate
// exists for.
//
// The node was renewing right up to the moment this process went away, and
// could not renew again until it came back — because every renewal now goes
// through here. Without the pass its parked row is claimable by anybody the
// instant the service opens, and the sandbox comes back one snapshot behind.
func TestTheRestartPassExtendsALeaseTheOutageLapsed(t *testing.T) {
	f := newStoreFixture(t)
	id := sandboxUUID(100)

	f.seed(seedRow{sandboxID: id, state: "publishing", originNode: stNodeA, snapshotID: snapshotUUID(100)})
	// Renewed 10 minutes ago with a 90s lease: healthy right up to the outage.
	f.setLease(id, "now() - interval '10 minutes'", "now() - interval '8 minutes 30 seconds'")

	downtime, extended, err := f.store.ExtendLeases(context.Background(), f.cluster, 90*time.Second)
	if err != nil {
		t.Fatalf("restart pass failed: %v", err)
	}
	if extended != 1 {
		t.Fatalf("expected the row to be extended, got %d", extended)
	}
	if downtime < 9*time.Minute || downtime > 11*time.Minute {
		t.Fatalf("downtime should be about ten minutes, got %s", downtime)
	}

	row := f.raw(id)
	if row.leaseExpires == nil || !row.leaseExpires.After(f.dbNow()) {
		t.Fatalf("the lease is still lapsed after the restart pass: %v", row.leaseExpires)
	}

	// And the row is no longer takeable, which is the whole point.
	claim, err := f.store.ClaimForResume(context.Background(), f.cluster, id, stNodeB)
	if err != nil {
		t.Fatalf("claim failed: %v", err)
	}
	if claim.Outcome != ClaimOutcomeNotReady {
		t.Fatalf("a row the restart pass repaired was still taken over: %s", claim.Outcome)
	}
}

// TestTheRestartPassLeavesARowThatWasAlreadyDeadAlone is why the extension is
// additive rather than `now() + downtime + lease`.
//
// The same amount is added to a value that much further in the past, so a row
// whose holder had already been gone for longer than the outage stays lapsed —
// and stays reclaimable. The absolute form would have deferred a
// decommissioned machine's rows by the whole length of an outage that had
// nothing to do with them.
func TestTheRestartPassLeavesARowThatWasAlreadyDeadAlone(t *testing.T) {
	f := newStoreFixture(t)
	recent, longDead := sandboxUUID(101), sandboxUUID(102)

	f.seed(seedRow{sandboxID: recent, state: "publishing", originNode: stNodeA, snapshotID: snapshotUUID(101)})
	f.seed(seedRow{sandboxID: longDead, state: "publishing", originNode: stNodeB, snapshotID: snapshotUUID(102)})

	// The outage was ten minutes; this row's holder stopped renewing a week
	// before it started.
	f.setLease(recent, "now() - interval '10 minutes'", "now() - interval '8 minutes'")
	f.setLease(longDead, "now() - interval '7 days'", "now() - interval '7 days'")

	if _, _, err := f.store.ExtendLeases(context.Background(), f.cluster, 90*time.Second); err != nil {
		t.Fatalf("restart pass failed: %v", err)
	}

	if row := f.raw(recent); row.leaseExpires == nil || !row.leaseExpires.After(f.dbNow()) {
		t.Fatalf("the healthy holder's lease was not repaired: %v", row.leaseExpires)
	}
	if row := f.raw(longDead); row.leaseExpires == nil || row.leaseExpires.After(f.dbNow()) {
		t.Fatalf("a week-dead holder's lease was pushed into the future: %v", row.leaseExpires)
	}
}

// TestTheRestartPassAssumesAFullLeaseWhenItCannotMeasure: no rows means no
// max(updated_at) to measure against, and the conservative answer is a whole
// lease rather than zero.
func TestTheRestartPassAssumesAFullLeaseWhenItCannotMeasure(t *testing.T) {
	f := newStoreFixture(t)

	downtime, extended, err := f.store.ExtendLeases(context.Background(), f.cluster, 90*time.Second)
	if err != nil {
		t.Fatalf("restart pass failed: %v", err)
	}
	if extended != 0 {
		t.Fatalf("expected nothing to extend, got %d", extended)
	}
	if downtime != 90*time.Second {
		t.Fatalf("expected one full lease, got %s", downtime)
	}
}

// TestTheRestartPassDoesNotRewriteTheEvidenceItReads: updated_at is what the
// downtime is inferred from, so stamping it with the time of the repair would
// leave the next restart measuring its outage against this one's clean-up.
func TestTheRestartPassDoesNotRewriteTheEvidenceItReads(t *testing.T) {
	f := newStoreFixture(t)
	id := sandboxUUID(103)

	f.seed(seedRow{sandboxID: id, state: "publishing", originNode: stNodeA, snapshotID: snapshotUUID(103)})
	f.setLease(id, "now() - interval '10 minutes'", "now() - interval '8 minutes'")
	before := f.raw(id).updatedAt

	if _, _, err := f.store.ExtendLeases(context.Background(), f.cluster, 90*time.Second); err != nil {
		t.Fatalf("restart pass failed: %v", err)
	}
	if after := f.raw(id).updatedAt; !after.Equal(before) {
		t.Fatalf("the restart pass rewrote updated_at: %s -> %s", before, after)
	}
}

func TestTheRestartPassIsScopedToItsCluster(t *testing.T) {
	f := newStoreFixture(t)
	id := sandboxUUID(104)

	f.seed(seedRow{sandboxID: id, cluster: f.other, state: "publishing", originNode: "node-z", snapshotID: snapshotUUID(104)})
	f.setLease(id, "now() - interval '10 minutes'", "now() - interval '8 minutes'")
	before := f.raw(id).leaseExpires

	if _, extended, err := f.store.ExtendLeases(context.Background(), f.cluster, 90*time.Second); err != nil || extended != 0 {
		t.Fatalf("the restart pass reached another cluster: %d, %v", extended, err)
	}
	if after := f.raw(id).leaseExpires; !after.Equal(*before) {
		t.Fatalf("another cluster's lease was extended: %s -> %s", before, after)
	}
}

// TestTheGraceWindowWithholdsOnlyTheTakeoverArm covers guard §3.2's third
// clause end to end.
//
// A `paused` row's claim never consults the lease, so withholding it would
// stall every ordinary cross-node resume for a full lease after each restart,
// for nothing. The arm that is withheld is the one that takes a sandbox off a
// node still uploading its snapshot — precisely the decision a lease this
// process failed to renew cannot support.
func TestTheGraceWindowWithholdsOnlyTheTakeoverArm(t *testing.T) {
	f := newStoreFixture(t)

	grace := NewGrace(time.Hour, zap.NewNop())
	if _, err := grace.Enter(context.Background(), f.store, f.cluster); err != nil {
		t.Fatalf("enter failed: %v", err)
	}
	f.store.WithGuards(grace, nil)

	parked, durable := sandboxUUID(105), sandboxUUID(106)
	f.seed(seedRow{sandboxID: parked, state: "publishing", originNode: stNodeA, snapshotID: snapshotUUID(105)})
	f.seed(seedRow{sandboxID: durable, state: "paused", originNode: stNodeA, snapshotID: snapshotUUID(106)})
	// Placed after the restart pass ran, so the parked row's lease is lapsed
	// exactly as it would be if it had been written during the outage.
	f.setLease(parked, "now() - interval '1 hour'", "now() - interval '1 hour'")

	claim, err := f.store.ClaimForResume(context.Background(), f.cluster, parked, stNodeB)
	if err != nil {
		t.Fatalf("claim failed: %v", err)
	}
	if claim.Outcome != ClaimOutcomeNotReady {
		t.Fatalf("a parked row was taken over during the grace window: %s", claim.Outcome)
	}
	if row := f.raw(parked); row.state != "publishing" {
		t.Fatalf("the parked row was modified: %+v", row)
	}

	claim, err = f.store.ClaimForResume(context.Background(), f.cluster, durable, stNodeB)
	if err != nil {
		t.Fatalf("claim failed: %v", err)
	}
	if claim.Outcome != ClaimOutcomeClaimed {
		t.Fatalf("an ordinary cross-node resume was refused during the grace window: %s", claim.Outcome)
	}
	if claim.PreviousState != StatePaused {
		t.Fatalf("previous_state: got %q", claim.PreviousState)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Discard breaker
// ─────────────────────────────────────────────────────────────────────────────

func TestTheDiscardBreakerTakesTheStricterOfItsTwoLimits(t *testing.T) {
	cases := []struct {
		name       string
		maxRows    int64
		maxRatio   float64
		candidates int64
		total      int64
		wantTrip   bool
	}{
		{name: "nothing to discard", maxRows: 1, maxRatio: 0.01, candidates: 0, total: 1000},
		{name: "under both", maxRows: 10, maxRatio: 0.5, candidates: 3, total: 100},
		{name: "at both limits exactly", maxRows: 10, maxRatio: 0.1, candidates: 10, total: 100},
		{name: "over the count on a large cluster", maxRows: 10, maxRatio: 0.9, candidates: 11, total: 1000, wantTrip: true},
		{name: "over the ratio on a small cluster", maxRows: 100, maxRatio: 0.1, candidates: 3, total: 10, wantTrip: true},
		// A ratio needs a denominator; with no rows at all the absolute limit is
		// the only one that means anything.
		{name: "no denominator, under the count", maxRows: 10, maxRatio: 0.1, candidates: 5, total: 0},
		{name: "no denominator, over the count", maxRows: 10, maxRatio: 0.1, candidates: 11, total: 0, wantTrip: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			breaker := NewDiscardBreaker(tc.maxRows, tc.maxRatio, zap.NewNop())
			err := breaker.Allow(tc.candidates, tc.total)
			if tc.wantTrip && !errors.Is(err, ErrDiscardBreakerTripped) {
				t.Fatalf("expected the breaker to trip, got %v", err)
			}
			if !tc.wantTrip && err != nil {
				t.Fatalf("expected the pass to be allowed, got %v", err)
			}
		})
	}
}

func TestANilBreakerAllowsEverything(t *testing.T) {
	var breaker *DiscardBreaker

	if err := breaker.Allow(1_000_000, 1_000_001); err != nil {
		t.Fatalf("a nil breaker refused a pass: %v", err)
	}
}

func TestTheDiscardBreakerFillsInWhateverWasLeftAtZero(t *testing.T) {
	breaker := NewDiscardBreaker(0, 0, nil)

	if breaker.MaxRows != defaultDiscardMaxRows || breaker.MaxRatio != defaultDiscardMaxRatio {
		t.Fatalf("defaults were not applied: %+v", breaker)
	}
	// A zero-valued breaker built by hand must behave the same way rather than
	// letting everything through.
	bare := &DiscardBreaker{}
	if err := bare.Allow(defaultDiscardMaxRows+1, 0); !errors.Is(err, ErrDiscardBreakerTripped) {
		t.Fatalf("a zero-valued breaker allowed a pass past the default limit: %v", err)
	}
}
