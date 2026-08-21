// Contract tests for the paused registry write path.
//
// These are a port of the node's own behavioural suite
// (`tests/paused_registry.rs`) plus the fail-closed cases that suite never
// had. They are written against the `Store` interface and the Rust semantics
// spec (`docs/proposals/_recon-R2-registry-spec.md`) alone — deliberately not
// against the Go implementation, because a translation checked by tests that
// were read off the translation proves nothing.
//
// Everything interesting about this registry lives in which rows a predicate
// matches and which it deliberately does not, so none of it can be checked
// without a real database. Point SCHEDULER_REGISTRY_TEST_DSN at a throwaway
// PostgreSQL to run them:
//
//	docker run -d --rm --name aenv-pg -e POSTGRES_PASSWORD=verify \
//	    -e POSTGRES_DB=aenv -p 15499:5432 postgres:16-alpine
//	SCHEDULER_REGISTRY_TEST_DSN=postgres://postgres:verify@127.0.0.1:15499/aenv \
//	    go test ./services/scheduler/internal/registry/ -run Contract
package registry

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	// contractLeaseTTL is short enough that a test can outlive a lease
	// without dragging. It mirrors TEST_LEASE_SECS in the Rust suite.
	contractLeaseTTL = 1 * time.Second
	// contractPastLease is comfortably past contractLeaseTTL on a loaded
	// machine. It mirrors PAST_LEASE in the Rust suite.
	contractPastLease = 1600 * time.Millisecond

	contractNodeA = "node-a"
	contractNodeB = "node-b"
)

// contractDSN returns the test database DSN, or skips the calling test when
// none is configured.
//
// A skipped test reports as passing, so a runner that is *supposed* to have a
// database — CI, or a verification run — sets SCHEDULER_REGISTRY_TEST_REQUIRED
// and gets a loud failure instead of a file full of green no-ops. Without that
// escape hatch the one job meant to be enforcing this suite is also the one
// that can silently skip it.
func contractDSN(t *testing.T) string {
	t.Helper()

	dsn := os.Getenv("SCHEDULER_REGISTRY_TEST_DSN")
	if dsn == "" {
		if os.Getenv("SCHEDULER_REGISTRY_TEST_REQUIRED") != "" {
			t.Fatal("SCHEDULER_REGISTRY_TEST_REQUIRED is set but SCHEDULER_REGISTRY_TEST_DSN is not: these contract tests would have been skipped")
		}
		t.Skip("SCHEDULER_REGISTRY_TEST_DSN is not set; skipping the registry contract tests")
	}

	return dsn
}

// contractStore builds the Store under test and brings the table to the shape
// this build expects.
//
// 🔴 This is the *only* place in these tests that names the implementation.
// Everything above it is derived from the Rust specification, so a defect in
// the translation shows up as a failing assertion rather than as a test that
// was written to match it.
//
// Migrate runs against a table the caller has already created from the node's
// own DDL, which is the mixed-mode case the switchover has to survive: while
// both a node and this process may bootstrap the same table, a migration that
// is not idempotent over the node's shape stops one of them from starting.
func contractStore(t *testing.T, dsn string) Store {
	t.Helper()

	store, err := NewStore(context.Background(), StoreConfig{
		DSN:      dsn,
		LeaseTTL: contractLeaseTTL,
		// 🔴 Fenced, because that is what a deployment runs. These are the
		// semantics tests — the port of the node's own behavioural suite — and
		// running them against the rollback statements would leave the
		// statements production actually uses covered only by the tests written
		// for them, with nothing checking that fencing left the rest of the
		// contract intact.
		WriteFencing: true,
	})
	if err != nil {
		t.Fatalf("build the store under test: %v", err)
	}
	t.Cleanup(store.Close)

	if err := migrateRetryingDeadlock(func() error { return store.Migrate(context.Background()) }); err != nil {
		t.Fatalf("migrate a table the node already created: %v", err)
	}

	return store
}

// contractEnv is one test's view of the database: the store under test, its
// own schema and cluster id, and a direct connection for the handful of
// assertions that have to look at, or write, a row the Store itself cannot
// produce.
type contractEnv struct {
	store   Store
	conn    *pgx.Conn
	schema  string
	cluster string

	// executions is the incarnation each sandbox in this test is currently
	// living under, so the helpers below can send the *same* one through a
	// pause that a resume installed — which is what the node does, and what
	// the fenced statements require.
	mu         sync.Mutex
	executions map[string]string
}

// executionOf is the incarnation this sandbox is running under, minting one
// the first time it is asked for.
func (e *contractEnv) executionOf(sandboxID string) string {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.executions == nil {
		e.executions = make(map[string]string)
	}
	if id, ok := e.executions[sandboxID]; ok {
		return id
	}
	id := newExecutionID()
	e.executions[sandboxID] = id
	return id
}

// nextExecutionOf mints a *new* incarnation for a sandbox and remembers it.
// A claim is where one is allocated, so that is where this is called.
func (e *contractEnv) nextExecutionOf(sandboxID string) string {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.executions == nil {
		e.executions = make(map[string]string)
	}
	id := newExecutionID()
	e.executions[sandboxID] = id
	return id
}

// contractSetup gives the calling test a private schema holding a table of
// its own, and returns an environment scoped to a fresh random cluster id.
//
// 🔴 The schema is the isolation, not the cluster id. Several suites share one
// test database, and they all want a table called `paused_sandboxes` — the
// node's DDL names it without a schema qualifier and is copied verbatim, so
// `search_path` is the only seam available. Partitioning by cluster id alone
// would still leave every suite creating, migrating and truncating the same
// table underneath each other.
//
// The cluster id is kept on top of that because it is what the Rust suite uses
// and because the scoping tests need two of them, not because it isolates
// anything here.
func contractSetup(t *testing.T) *contractEnv {
	t.Helper()

	dsn := contractDSN(t)
	ctx := context.Background()

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect to the test database: %v", err)
	}

	schema := contractSchemaName(t)
	quoted := pgx.Identifier{schema}.Sanitize()
	if _, err := conn.Exec(ctx, "CREATE SCHEMA "+quoted); err != nil {
		_ = conn.Close(ctx)
		t.Fatalf("create the private schema %s: %v", schema, err)
	}
	t.Cleanup(func() {
		cleanup := context.Background()
		if _, err := conn.Exec(cleanup, "DROP SCHEMA "+quoted+" CASCADE"); err != nil {
			t.Logf("drop the private schema %s: %v", schema, err)
		}
		_ = conn.Close(cleanup)
	})

	// This connection is used for the direct SQL a couple of tests need, so it
	// has to resolve the same unqualified table name the store does.
	if _, err := conn.Exec(ctx, "SET search_path TO "+quoted); err != nil {
		t.Fatalf("point the test connection at %s: %v", schema, err)
	}
	if _, err := conn.Exec(ctx, legacyNodeSchemaDDL); err != nil {
		t.Fatalf("create the pre-phase-3 table in %s: %v", schema, err)
	}

	env := &contractEnv{conn: conn, schema: schema, cluster: contractUUID(t), executions: make(map[string]string)}
	env.store = contractStore(t, contractDSNInSchema(t, dsn, schema))

	return env
}

// contractSchemaName is a schema name unique to this test, short enough to
// survive PostgreSQL's 63-byte identifier limit — beyond it names are
// truncated, and two of these test names share a long enough prefix to
// collide once truncated.
func contractSchemaName(t *testing.T) string {
	t.Helper()

	name := []rune(t.Name())
	if len(name) > 24 {
		name = name[:24]
	}
	cleaned := make([]rune, 0, len(name))
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			cleaned = append(cleaned, r)
		default:
			cleaned = append(cleaned, '_')
		}
	}

	var unique [8]byte
	if _, err := rand.Read(unique[:]); err != nil {
		t.Fatalf("generate a schema suffix: %v", err)
	}

	// Lower case throughout. The schema is created as a quoted identifier, so
	// its name keeps whatever case it is given, but `search_path` carries it
	// unquoted — and PostgreSQL folds unquoted identifiers to lower case before
	// resolving them. A name with a capital in it therefore creates one schema
	// and looks for another, and every statement lands as "no schema has been
	// selected to create in".
	return strings.ToLower(fmt.Sprintf("regtest_%s_%x", string(cleaned), unique))
}

// contractDSNInSchema points a DSN at one schema.
//
// It has to travel in the DSN because that is the whole of what NewStore
// accepts: there is no seam for handing the store a prepared pool, so the
// connection string is the only way to reach its RuntimeParams.
func contractDSNInSchema(t *testing.T, dsn, schema string) string {
	t.Helper()

	if !strings.HasPrefix(dsn, "postgres://") && !strings.HasPrefix(dsn, "postgresql://") {
		// Keyword/value form: pgx reads search_path here as a runtime param too.
		return dsn + " search_path=" + schema
	}

	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse the test DSN: %v", err)
	}
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()

	return parsed.String()
}

// otherCluster returns a second cluster id in the same schema, for the scoping
// tests. It needs no cleanup of its own: the schema takes everything with it.
func (e *contractEnv) otherCluster(t *testing.T) string {
	t.Helper()

	return contractUUID(t)
}

// contractUUID returns a random v4 UUID. The sandbox, cluster and snapshot
// columns are all UUID typed, so anything else is rejected by the database.
func contractUUID(t *testing.T) string {
	t.Helper()

	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatalf("generate a uuid: %v", err)
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80

	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// contractMetadata is a stand-in for the node's SandboxMetadata blob. Its
// contents are never interpreted by the control plane; what matters is that
// whatever went in comes back.
func contractMetadata(origin string) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(`{"origin":%q,"vcpu":2,"nested":{"kept":true}}`, origin))
}

func (e *contractEnv) beginPause(t *testing.T, cluster, sandboxID, origin string) BeganPause {
	t.Helper()

	began, err := e.store.BeginPause(context.Background(), BeginPauseInput{
		ClusterID:    cluster,
		SandboxID:    sandboxID,
		OriginNodeID: origin,
		Metadata:     contractMetadata(origin),
		ExecutionID:  e.executionOf(sandboxID),
	})
	if err != nil {
		t.Fatalf("begin pause on %s: %v", sandboxID, err)
	}

	return began
}

// pauseAndPublish drives a sandbox to a durable `paused` row and returns the
// snapshot it names.
func (e *contractEnv) pauseAndPublish(t *testing.T, cluster, sandboxID, origin string) string {
	t.Helper()

	began := e.beginPause(t, cluster, sandboxID, origin)
	snapshot := contractUUID(t)
	if err := e.store.CompletePause(context.Background(), cluster, sandboxID, began.Generation, snapshot); err != nil {
		t.Fatalf("complete pause on %s: %v", sandboxID, err)
	}

	return snapshot
}

func (e *contractEnv) markRunning(t *testing.T, cluster, sandboxID, node string) bool {
	t.Helper()

	return e.markRunningOutcome(t, cluster, sandboxID, node) == MarkRunningAdopted
}

func (e *contractEnv) markRunningOutcome(t *testing.T, cluster, sandboxID, node string) MarkRunningOutcome {
	t.Helper()

	outcome, err := e.store.MarkRunning(context.Background(), cluster, sandboxID, node, e.executionOf(sandboxID), nil)
	if err != nil {
		t.Fatalf("mark %s running on %s: %v", sandboxID, node, err)
	}

	return outcome
}

func (e *contractEnv) claim(t *testing.T, cluster, sandboxID, node string) ResumeClaim {
	t.Helper()

	claim, err := e.store.ClaimForResume(context.Background(), cluster, sandboxID, node, e.nextExecutionOf(sandboxID))
	if err != nil {
		t.Fatalf("claim %s for %s: %v", sandboxID, node, err)
	}

	return claim
}

func (e *contractEnv) renew(t *testing.T, cluster, node string, held ...HeldSandbox) uint64 {
	t.Helper()

	renewed, err := e.store.RenewLease(context.Background(), cluster, node, held)
	if err != nil {
		t.Fatalf("renew leases for %s: %v", node, err)
	}

	return renewed
}

func (e *contractEnv) reclaim(t *testing.T, cluster string) ReleasedHoldings {
	t.Helper()

	reclaimed, err := e.store.ReclaimExpiredHoldings(context.Background(), cluster)
	if err != nil {
		t.Fatalf("reclaim expired holdings: %v", err)
	}

	return reclaimed
}

func (e *contractEnv) releaseHoldings(t *testing.T, cluster, node string) ReleasedHoldings {
	t.Helper()

	released, err := e.store.ReleaseNodeHoldings(context.Background(), cluster, node)
	if err != nil {
		t.Fatalf("release holdings of %s: %v", node, err)
	}

	return released
}

// requireRow reads a row that must exist.
func (e *contractEnv) requireRow(t *testing.T, cluster, sandboxID string) Entry {
	t.Helper()

	entry, found, err := e.store.Get(context.Background(), cluster, sandboxID)
	if err != nil {
		t.Fatalf("read %s: %v", sandboxID, err)
	}
	if !found {
		t.Fatalf("expected a row for %s", sandboxID)
	}

	return entry
}

// requireNoRow asserts that a read succeeded and matched nothing, which is the
// answer the caller acts on by deleting local artifacts.
func (e *contractEnv) requireNoRow(t *testing.T, cluster, sandboxID string) {
	t.Helper()

	_, found, err := e.store.Get(context.Background(), cluster, sandboxID)
	if err != nil {
		t.Fatalf("read %s: %v", sandboxID, err)
	}
	if found {
		t.Fatalf("expected no row for %s", sandboxID)
	}
}

// held reports a sandbox with no deadline at all — the shape most of these
// tests want, since they exercise the lease rather than reclamation.
func contractHeld(sandboxID string) HeldSandbox {
	return HeldSandbox{SandboxID: sandboxID}
}

// heldDue reports a sandbox whose deadline is `offset` from now. A negative
// offset means the sandbox has already outlived it.
func contractHeldDue(sandboxID string, offset time.Duration) HeldSandbox {
	due := time.Now().Add(offset)
	return HeldSandbox{SandboxID: sandboxID, ExpiresAt: &due}
}

// 🔴 The failure this registry exists to survive, and the one it used to make
// worse: an upload that fails must not cost the sandbox the snapshot it
// already had. Clearing the reference on BeginPause left the row naming
// nothing while a perfectly good snapshot sat in the repository unreferenced,
// so losing the origin node lost the sandbox outright.
//
// Ported from a_failed_publish_keeps_the_snapshot_the_sandbox_already_had.
func TestContractAFailedPublishKeepsTheSnapshotTheSandboxAlreadyHad(t *testing.T) {
	env := contractSetup(t)
	sandboxID := contractUUID(t)

	first := env.pauseAndPublish(t, env.cluster, sandboxID, contractNodeA)

	// Resume, then pause again — and this time the upload never lands.
	env.markRunning(t, env.cluster, sandboxID, contractNodeA)
	began := env.beginPause(t, env.cluster, sandboxID, contractNodeA)
	if began.PreviousSnapshotID != first {
		t.Fatalf("the pause must report the snapshot it is superseding so the caller can retire it later: got %q, want %q", began.PreviousSnapshotID, first)
	}
	if err := env.store.MarkLocalOnly(context.Background(), env.cluster, sandboxID, began.Generation); err != nil {
		t.Fatalf("downgrade to local-only: %v", err)
	}

	row := env.requireRow(t, env.cluster, sandboxID)
	if row.State != StateLocalOnly {
		t.Fatalf("unexpected state %q, want %q", row.State, StateLocalOnly)
	}
	if row.SnapshotID != first {
		t.Fatalf("a failed publish must leave the previous snapshot referenced rather than orphan it: got %q, want %q", row.SnapshotID, first)
	}

	// And once the origin node stops renewing, that snapshot is what the
	// sandbox comes back from — degraded to the previous pause, but alive.
	time.Sleep(contractPastLease)
	claim := env.claim(t, env.cluster, sandboxID, contractNodeB)
	if claim.Outcome != ClaimOutcomeClaimed {
		t.Fatalf("a lapsed local-only row must be recoverable from its last snapshot: got %q", claim.Outcome)
	}
	if claim.Entry == nil {
		t.Fatal("a granted claim must carry the row the caller has to rebuild from")
	} else if claim.Entry.SnapshotID != first {
		t.Fatalf("unexpected snapshot on the claimed row: got %q, want %q", claim.Entry.SnapshotID, first)
	}
	if claim.PreviousState != StateLocalOnly {
		t.Fatalf("this claim overrode a node that never finished uploading, which is the one event on this path an operator has to be able to find: got %q, want %q", claim.PreviousState, StateLocalOnly)
	}
}

// A downgrade that quietly matches nothing leaves the row stuck in
// `publishing`, and every resume from another node then reports an upload that
// gave up long ago as still in progress.
//
// Ported from a_downgrade_that_matches_nothing_is_reported.
func TestContractADowngradeThatMatchesNothingIsReported(t *testing.T) {
	env := contractSetup(t)
	sandboxID := contractUUID(t)

	began := env.beginPause(t, env.cluster, sandboxID, contractNodeA)

	stale := env.store.MarkLocalOnly(context.Background(), env.cluster, sandboxID, began.Generation-1)
	if !errors.Is(stale, ErrGenerationConflict) {
		t.Fatalf("a downgrade against a superseded generation must be reported: got %v, want %v", stale, ErrGenerationConflict)
	}
	if row := env.requireRow(t, env.cluster, sandboxID); row.State != StatePublishing {
		t.Fatalf("a refused downgrade must leave the row alone: got state %q", row.State)
	}

	if err := env.store.MarkLocalOnly(context.Background(), env.cluster, sandboxID, began.Generation); err != nil {
		t.Fatalf("downgrade with the right generation: %v", err)
	}
}

// 🔴 Not in the Rust suite. CompletePause is a real compare-and-swap, and its
// failure has to be distinguishable from success: the caller responds to it by
// deciding whether the snapshot it just uploaded is referenced by anybody, and
// a swallowed conflict deletes a snapshot the row still points at.
func TestContractCompletePauseRefusesAStaleGeneration(t *testing.T) {
	env := contractSetup(t)
	sandboxID := contractUUID(t)

	began := env.beginPause(t, env.cluster, sandboxID, contractNodeA)
	snapshot := contractUUID(t)

	stale := env.store.CompletePause(context.Background(), env.cluster, sandboxID, began.Generation-1, snapshot)
	if !errors.Is(stale, ErrGenerationConflict) {
		t.Fatalf("publishing against a superseded generation must be reported: got %v, want %v", stale, ErrGenerationConflict)
	}

	row := env.requireRow(t, env.cluster, sandboxID)
	if row.State != StatePublishing {
		t.Fatalf("a refused publish must leave the row publishing: got %q", row.State)
	}
	if row.SnapshotID != "" {
		t.Fatalf("a refused publish must not write the snapshot it was carrying: got %q", row.SnapshotID)
	}

	if err := env.store.CompletePause(context.Background(), env.cluster, sandboxID, began.Generation, snapshot); err != nil {
		t.Fatalf("publish with the right generation: %v", err)
	}
	if row := env.requireRow(t, env.cluster, sandboxID); row.SnapshotID != snapshot {
		t.Fatalf("unexpected snapshot after publishing: got %q, want %q", row.SnapshotID, snapshot)
	}
}

// 🔴 Not in the Rust suite. The predicate carries `AND state = 'publishing'`
// as well as the generation, so a second publish against a row that has
// already moved on is a conflict rather than a silent rewrite. Without the
// state half, a late-arriving publish would drag a running sandbox back to
// `paused` and hand it to the first node that asked for it.
func TestContractCompletePauseRefusesARowThatIsNoLongerPublishing(t *testing.T) {
	env := contractSetup(t)
	sandboxID := contractUUID(t)

	began := env.beginPause(t, env.cluster, sandboxID, contractNodeA)
	first := contractUUID(t)
	if err := env.store.CompletePause(context.Background(), env.cluster, sandboxID, began.Generation, first); err != nil {
		t.Fatalf("complete pause: %v", err)
	}

	// The generation is unchanged — CompletePause does not bump it — so only
	// the state half of the predicate can refuse this.
	second := env.store.CompletePause(context.Background(), env.cluster, sandboxID, began.Generation, contractUUID(t))
	if !errors.Is(second, ErrGenerationConflict) {
		t.Fatalf("publishing onto a row that is no longer publishing must be refused: got %v, want %v", second, ErrGenerationConflict)
	}
	if row := env.requireRow(t, env.cluster, sandboxID); row.SnapshotID != first {
		t.Fatalf("the refused publish must not have replaced the snapshot: got %q, want %q", row.SnapshotID, first)
	}
}

// Reconciliation asks about a node's whole roster at once. The batch has to
// answer exactly what a row-at-a-time read would: present means present,
// absent means "no row" — never "not looked at".
//
// Ported from a_batch_read_reports_only_the_sandboxes_that_have_rows.
func TestContractABatchReadReportsOnlyTheSandboxesThatHaveRows(t *testing.T) {
	env := contractSetup(t)
	tracked := contractUUID(t)
	alsoTracked := contractUUID(t)
	untracked := contractUUID(t)

	env.pauseAndPublish(t, env.cluster, tracked, contractNodeA)
	env.pauseAndPublish(t, env.cluster, alsoTracked, contractNodeB)

	rows, err := env.store.GetMany(context.Background(), env.cluster, []string{tracked, untracked, alsoTracked})
	if err != nil {
		t.Fatalf("batch read: %v", err)
	}

	if len(rows.Entries) != 2 {
		t.Fatalf("expected exactly the two rows that exist, got %d", len(rows.Entries))
	}
	if got := rows.Entries[tracked].OriginNodeID; got != contractNodeA {
		t.Fatalf("unexpected origin for the first row: got %q, want %q", got, contractNodeA)
	}
	if got := rows.Entries[alsoTracked].OriginNodeID; got != contractNodeB {
		t.Fatalf("unexpected origin for the second row: got %q, want %q", got, contractNodeB)
	}
	if _, ok := rows.Entries[untracked]; ok {
		t.Fatal("a sandbox with no row must be absent, which is how the caller reads 'the cluster does not track it'")
	}

	// 🔴 The absence above is only usable as "no row" because the answer says
	// it looked. Without this the caller cannot tell that reading from a
	// response that lost rows on the way, and its response to "no row" is to
	// delete the sandbox.
	covered := make(map[string]bool, len(rows.Covered))
	for _, id := range rows.Covered {
		covered[id] = true
	}
	for _, id := range []string{tracked, untracked, alsoTracked} {
		if !covered[id] {
			t.Fatalf("the answer does not claim to have looked up %s, so its absence proves nothing", id)
		}
	}
	if rows.Now.IsZero() {
		t.Fatal("a batch read must carry the database's clock: lease arithmetic on these rows must not use the reader's own")
	}
}

// 🔴 The batch is what reconciliation tears sandboxes down on, so it must be
// scoped to this cluster just as tightly as every other read. Without the
// filter, one cluster's reconciliation reads another cluster's rows and
// concludes its own sandboxes have moved on.
//
// Ported from a_batch_read_cannot_see_another_clusters_sandboxes.
func TestContractABatchReadCannotSeeAnotherClustersSandboxes(t *testing.T) {
	env := contractSetup(t)
	theirs := env.otherCluster(t)
	sandboxID := contractUUID(t)

	env.pauseAndPublish(t, theirs, sandboxID, contractNodeA)

	rows, err := env.store.GetMany(context.Background(), env.cluster, []string{sandboxID})
	if err != nil {
		t.Fatalf("batch read: %v", err)
	}
	if len(rows.Entries) != 0 {
		t.Fatalf("another cluster's row must be invisible, got %d rows", len(rows.Entries))
	}
	// Covered still names the id: it was looked up, in this cluster, and has no
	// row here. That is a different statement from "not looked at", and it is
	// the one the caller is entitled to act on.
	if len(rows.Covered) != 1 || rows.Covered[0] != sandboxID {
		t.Fatalf("a scoped miss must still report what it looked up: got %v", rows.Covered)
	}

	env.requireRow(t, theirs, sandboxID)
}

// Ported from a_batch_read_of_nothing_asks_nothing.
func TestContractABatchReadOfNothingAsksNothing(t *testing.T) {
	env := contractSetup(t)

	rows, err := env.store.GetMany(context.Background(), env.cluster, nil)
	if err != nil {
		t.Fatalf("batch read of nothing: %v", err)
	}
	if len(rows.Entries) != 0 {
		t.Fatalf("expected an empty map, got %d rows", len(rows.Entries))
	}
	if len(rows.Covered) != 0 {
		t.Fatalf("a batch that asked for nothing looked up nothing: got %v", rows.Covered)
	}
}

// 🔴 Two clusters pointed at one registry database must not see each other's
// sandboxes. Without the scope a node claims a foreign row, fails to find the
// snapshot in its own repository, and deletes the row as dangling — taking the
// other cluster's sandbox with it.
//
// Ported from one_cluster_cannot_reach_anothers_sandboxes.
func TestContractOneClusterCannotReachAnothersSandboxes(t *testing.T) {
	env := contractSetup(t)
	theirs := env.otherCluster(t)
	sandboxID := contractUUID(t)

	env.pauseAndPublish(t, env.cluster, sandboxID, contractNodeA)

	env.requireNoRow(t, theirs, sandboxID)

	if claim := env.claim(t, theirs, sandboxID, contractNodeB); claim.Outcome != ClaimOutcomeNotFound {
		t.Fatalf("another cluster's sandbox must not be claimable: got %q, want %q", claim.Outcome, ClaimOutcomeNotFound)
	}

	// The generation quoted is our row's, so a cluster filter that failed open
	// would match and delete it. That is what makes this assertion have teeth.
	ours := env.requireRow(t, env.cluster, sandboxID)
	removed, err := env.store.Remove(context.Background(), theirs, sandboxID, ours.Generation)
	if err != nil {
		t.Fatalf("remove from the other cluster: %v", err)
	}
	if removed {
		t.Fatal("a delete scoped to another cluster must not match our row")
	}
	if row := env.requireRow(t, env.cluster, sandboxID); row.OriginNodeID != contractNodeA {
		t.Fatalf("another cluster's delete must not touch our row: got origin %q", row.OriginNodeID)
	}
}

// 🔴 Not in the Rust suite as a case of its own, though its last few lines
// cover the same ground: a foreign cluster must not be able to take the row
// over by pausing the same sandbox id. The upsert carries
// `WHERE paused_sandboxes.cluster_id = EXCLUDED.cluster_id`, and a mismatch is
// an error rather than a silent no-op — a pause that reports success while
// having written nothing leaves the caller quoting a generation that does not
// exist, and its snapshot ends up referenced by nobody.
func TestContractBeginPauseRefusesAnotherClustersRow(t *testing.T) {
	env := contractSetup(t)
	theirs := env.otherCluster(t)
	sandboxID := contractUUID(t)

	snapshot := env.pauseAndPublish(t, env.cluster, sandboxID, contractNodeA)
	before := env.requireRow(t, env.cluster, sandboxID)

	_, hijack := env.store.BeginPause(context.Background(), BeginPauseInput{
		ClusterID:    theirs,
		SandboxID:    sandboxID,
		OriginNodeID: contractNodeB,
		Metadata:     contractMetadata(contractNodeB),
		ExecutionID:  newExecutionID(),
	})
	if !errors.Is(hijack, ErrInvalidRecord) {
		t.Fatalf("a foreign cluster must not be able to rewrite the row: got %v, want %v", hijack, ErrInvalidRecord)
	}

	after := env.requireRow(t, env.cluster, sandboxID)
	if after.OriginNodeID != contractNodeA {
		t.Fatalf("the refused pause must not have moved the origin: got %q, want %q", after.OriginNodeID, contractNodeA)
	}
	if after.State != before.State || after.Generation != before.Generation {
		t.Fatalf("the refused pause must not have touched the row: got state %q generation %d, want %q/%d", after.State, after.Generation, before.State, before.Generation)
	}
	if after.SnapshotID != snapshot {
		t.Fatalf("the refused pause must not have touched the snapshot: got %q, want %q", after.SnapshotID, snapshot)
	}
}

// 🔴 Not in the Rust suite. A `paused` row with no snapshot promises a
// cross-node resume it cannot deliver, and the node-side decoder refuses it
// rather than skipping it — because a skipped row is an absent row, and an
// absent row is what the caller deletes local artifacts on.
//
// The batch case matters most: one unreadable row must fail the whole read,
// never quietly shorten the map.
func TestContractAPausedRowWithNoSnapshotIsRefusedNotSkipped(t *testing.T) {
	env := contractSetup(t)
	broken := contractUUID(t)
	healthy := contractUUID(t)

	env.pauseAndPublish(t, env.cluster, healthy, contractNodeA)

	// Only a direct write can produce this row: every path through the Store
	// that reaches `paused` writes a snapshot on the way.
	if _, err := env.conn.Exec(context.Background(), `
INSERT INTO paused_sandboxes (
    sandbox_id, cluster_id, state, generation, origin_node_id, claimed_by_node_id,
    snapshot_id, metadata, paused_at, updated_at, lease_expires_at, sandbox_expires_at
) VALUES ($1, $2, 'paused', 3, $3, NULL, NULL, '{}'::jsonb, now(), now(), now() + interval '90 seconds', NULL)`,
		broken, env.cluster, contractNodeA); err != nil {
		t.Fatalf("seed the invalid row: %v", err)
	}

	_, _, err := env.store.Get(context.Background(), env.cluster, broken)
	if !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("a paused row with no snapshot must be reported as invalid, never as an absence: got %v, want %v", err, ErrInvalidRecord)
	}

	rows, err := env.store.GetMany(context.Background(), env.cluster, []string{healthy, broken})
	if !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("one unreadable row must fail the whole batch: got %v (%d rows), want %v", err, len(rows.Entries), ErrInvalidRecord)
	}
}

// Remove is destructive and conditional, so what it does has to be exactly
// what it says: this row, this cluster, this generation, gone.
func TestContractRemoveDeletesOnlyTheRowItNames(t *testing.T) {
	env := contractSetup(t)
	doomed := contractUUID(t)
	bystander := contractUUID(t)

	env.pauseAndPublish(t, env.cluster, doomed, contractNodeA)
	env.pauseAndPublish(t, env.cluster, bystander, contractNodeA)

	row := env.requireRow(t, env.cluster, doomed)
	removed, err := env.store.Remove(context.Background(), env.cluster, doomed, row.Generation)
	if err != nil {
		t.Fatalf("remove: %v", err)
	}
	if !removed {
		t.Fatal("a delete quoting the row's own generation must match it")
	}

	env.requireNoRow(t, env.cluster, doomed)
	env.requireRow(t, env.cluster, bystander)

	// Removing a row that is already gone is not an error: the contract is
	// "only correct once the sandbox itself is gone", and a retry after a
	// timeout has to be able to say so twice. It reports that it matched
	// nothing, which is how a retry tells itself apart from a first attempt.
	removed, err = env.store.Remove(context.Background(), env.cluster, doomed, row.Generation)
	if err != nil {
		t.Fatalf("removing an absent row must not be an error: %v", err)
	}
	if removed {
		t.Fatal("removing an absent row cannot have matched anything")
	}
}

// 🔴 The window this closes: a node reads a row, decides the sandbox is not
// live elsewhere, and deletes it — while somebody else resumes it in between.
// The delete used to succeed and take the snapshot the other node is running
// from with it, silently, from both sides.
func TestContractRemoveRefusesAStaleGeneration(t *testing.T) {
	env := contractSetup(t)
	sandboxID := contractUUID(t)

	env.pauseAndPublish(t, env.cluster, sandboxID, contractNodeA)
	stale := env.requireRow(t, env.cluster, sandboxID).Generation

	// Somebody else moves the row on — exactly what a resume elsewhere does.
	if claim := env.claim(t, env.cluster, sandboxID, contractNodeB); claim.Outcome != ClaimOutcomeClaimed {
		t.Fatalf("seed the race: got %q, want %q", claim.Outcome, ClaimOutcomeClaimed)
	}

	removed, err := env.store.Remove(context.Background(), env.cluster, sandboxID, stale)
	if err != nil {
		t.Fatalf("a stale delete is not an error, it is a no-op: %v", err)
	}
	if removed {
		t.Fatal("a delete quoting a generation the row has moved past must match nothing")
	}
	env.requireRow(t, env.cluster, sandboxID)
}

// The Go read model carries the two lease columns the Rust ENTRY_COLUMNS
// leaves out (see the Sandbox doc comment), and they are what decides whether
// a parked row can be taken over. A read that drops them silently is a read
// that makes every lease judgement above it unfalsifiable.
//
// The metadata assertion is semantic rather than byte-for-byte on purpose:
// JSONB normalises whitespace and key order, so identical bytes are not
// something the database can promise. What it must promise is that no field
// is lost.
func TestContractAReadCarriesTheLeaseColumnsAndTheMetadataItWasGiven(t *testing.T) {
	env := contractSetup(t)
	sandboxID := contractUUID(t)

	env.pauseAndPublish(t, env.cluster, sandboxID, contractNodeA)
	env.markRunning(t, env.cluster, sandboxID, contractNodeA)

	deadline := time.Now().Add(time.Hour)
	if renewed := env.renew(t, env.cluster, contractNodeA, HeldSandbox{SandboxID: sandboxID, ExpiresAt: &deadline}); renewed != 1 {
		t.Fatalf("expected the holder's renewal to land, got %d", renewed)
	}

	row := env.requireRow(t, env.cluster, sandboxID)
	if row.LeaseExpiresAt == nil {
		t.Fatal("expected the lease column to be read back")
	}
	if row.SandboxExpiresAt == nil {
		t.Fatal("expected the sandbox deadline to be read back")
	} else if drift := row.SandboxExpiresAt.Sub(deadline); drift > time.Second || drift < -time.Second {
		t.Fatalf("the deadline must be recorded as reported, drifted by %s", drift)
	}
	if row.PausedAt.IsZero() || row.UpdatedAt.IsZero() {
		t.Fatal("expected both timestamps to be read back")
	}

	var want, got map[string]any
	if err := json.Unmarshal(contractMetadata(contractNodeA), &want); err != nil {
		t.Fatalf("decode the metadata we wrote: %v", err)
	}
	if err := json.Unmarshal(row.Metadata, &got); err != nil {
		t.Fatalf("decode the metadata we read back: %v", err)
	}
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("metadata must round-trip with nothing dropped: got %v, want %v", got, want)
	}
}
