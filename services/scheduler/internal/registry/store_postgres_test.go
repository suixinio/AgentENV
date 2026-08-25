package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

// storeFixture is one test's private copy of the table.
//
// It creates the table through Migrate rather than through a copy of the DDL,
// so every test in this file is also a test that the migration produces a table
// the node would recognise: a paraphrase would pass the store's own tests while
// failing against a table a node created.
type storeFixture struct {
	t       *testing.T
	pool    *pgxpool.Pool
	store   *PostgresStore
	cluster string
	other   string

	// executions is the incarnation each sandbox in this test is living
	// under. The fenced statements compare against the row, so a pause has to
	// send the value the resume installed — the same coupling the node has.
	mu         sync.Mutex
	executions map[string]string
}

// executionFor is the incarnation this sandbox is running under, minting one
// the first time it is asked for.
func (f *storeFixture) executionFor(sandboxID string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.executions == nil {
		f.executions = make(map[string]string)
	}
	if id, ok := f.executions[sandboxID]; ok {
		return id
	}
	id := newExecutionID()
	f.executions[sandboxID] = id
	return id
}

// nextExecutionFor mints a new incarnation for a sandbox and remembers it. A
// claim is where one is allocated, so that is where this belongs.
func (f *storeFixture) nextExecutionFor(sandboxID string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.executions == nil {
		f.executions = make(map[string]string)
	}
	id := newExecutionID()
	f.executions[sandboxID] = id
	return id
}

const (
	stCluster = "11111111-aaaa-4aaa-8aaa-111111111111"
	stOther   = "22222222-bbbb-4bbb-8bbb-222222222222"

	stNodeA = "node-a"
	stNodeB = "node-b"

	// stLeaseTTL is short enough that a test can watch a lease it stamped, and
	// long enough that a slow machine does not expire one mid-test.
	stLeaseTTL = 90 * time.Second
)

// stMetadata is a stand-in for the node's sandbox record. Its shape does not
// matter here — nothing in this package is allowed to have an opinion about it
// — but it is deliberately not flat, so a round trip that lost structure would
// show up.
const stMetadata = `{"id":"s","nested":{"list":[1,2,3],"big":9007199254740993},"unknown_to_this_build":true}`

// migrateRetryingDeadlock applies the schema, retrying a deadlock the test
// suite inflicted on itself.
//
// 🔴 Not papering over a fault in the migration. Migrate takes a cluster-wide
// advisory lock, and the whole suite shares one database: while this session
// holds that lock and issues DDL, another package's per-test
// `DROP SCHEMA ... CASCADE` can be waiting on the same system-catalog rows, and
// PostgreSQL breaks the three-way wait by killing whichever session asked last.
// That is this one, and what it was killed for has nothing to do with what the
// calling test asserts.
//
// It cannot happen in a deployment — there is one schema there and nothing
// drops it — and the process that migrates for real already retries every
// failure forever, so that a schema it cannot apply does not stop the scheduler
// routing traffic. This is that loop, bounded.
//
// 🔴 It hands the error back rather than failing the test itself, which is
// where it differs from the copy in the catalog suite. Two tests here assert on
// a refusal Migrate is *supposed* to produce, and they need the same deadlock
// retry without losing the error they are about to inspect. Anything that is
// not a deadlock is returned on the first attempt, so a genuine migration fault
// still surfaces as itself rather than as five attempts and a timeout.
func migrateRetryingDeadlock(migrate func() error) error {
	var err error
	for attempt := 0; attempt < 5; attempt++ {
		if err = migrate(); err == nil {
			return nil
		}
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "40P01" {
			return err
		}
		time.Sleep(time.Duration(attempt+1) * 200 * time.Millisecond)
	}
	return err
}

// TestMigrateRetryingDeadlockRefusesEverythingElse pins the one property that
// keeps the retry above from hiding a real migration fault.
//
// 🔴 Drop the SQLSTATE check and the helper turns every genuine failure — a
// refused preflight, a syntax error, a connection reset — into five attempts
// and a slow, misattributed timeout. Nothing else in this package would notice:
// the two tests that assert on a refusal Migrate is *supposed* to produce would
// still pass, just three seconds later. So the distinction is asserted here
// directly, on the helper, with no database involved.
func TestMigrateRetryingDeadlockRefusesEverythingElse(t *testing.T) {
	deadlock := &pgconn.PgError{Code: "40P01"}
	for _, tc := range []struct {
		name  string
		err   error
		calls int
	}{
		{"a deadlock is retried to the bound", deadlock, 5},
		{"an undefined table is not", &pgconn.PgError{Code: "42P01"}, 1},
		{"nor is a plain error", errors.New("connection reset by peer"), 1},
		{"and success costs one attempt", nil, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			err := migrateRetryingDeadlock(func() error {
				calls++
				return tc.err
			})
			if !errors.Is(err, tc.err) {
				t.Fatalf("the helper must hand back the error it gave up on: got %v, want %v", err, tc.err)
			}
			if calls != tc.calls {
				t.Fatalf("got %d attempts, want %d", calls, tc.calls)
			}
		})
	}
}

// newStoreFixture gives the test its own schema rather than its own database.
//
// The table's name is unqualified in every statement — the node's script says
// `paused_sandboxes` and this side copies it verbatim — so a private schema on
// the search path is the only isolation available without a second database.
// It also means these tests do not have to take turns with anything else
// pointed at the same PostgreSQL, which is otherwise a source of failures that
// look like the code and are not.
func newStoreFixture(t *testing.T) *storeFixture {
	t.Helper()

	f := newSchemaFixture(t)

	// Created through Migrate rather than through a copy of the DDL, so every
	// test using this fixture is also a test that the migration produces a
	// table the node would recognise.
	if err := migrateRetryingDeadlock(func() error { return Migrate(context.Background(), f.pool) }); err != nil {
		t.Fatalf("migrate failed: %v", err)
	}
	f.store = newStoreWithPool(f.pool, StoreConfig{LeaseTTL: stLeaseTTL, Logger: zap.NewNop(), WriteFencing: true})
	return f
}

// newUnfencedStoreFixture is the same table with the rollback statements
// selected, so the behaviour the setting restores is covered by tests of its
// own rather than only by the absence of the fenced ones.
func newUnfencedStoreFixture(t *testing.T) *storeFixture {
	t.Helper()

	f := newSchemaFixture(t)
	if err := migrateRetryingDeadlock(func() error { return Migrate(context.Background(), f.pool) }); err != nil {
		t.Fatalf("migrate failed: %v", err)
	}
	f.store = newStoreWithPool(f.pool, StoreConfig{LeaseTTL: stLeaseTTL, Logger: zap.NewNop(), WriteFencing: false})
	return f
}

// newStoreFixtureWithoutMigration gives the test an empty private schema and a
// pool pointed at it, for the cases that have to build the table some other
// way — above all, the way a node builds it.
func newStoreFixtureWithoutMigration(t *testing.T) *storeFixture {
	t.Helper()
	return newSchemaFixture(t)
}

// newSchemaFixture creates the private schema and the pool, and nothing else.
func newSchemaFixture(t *testing.T) *storeFixture {
	t.Helper()

	dsn := requireTestDSN(t)
	ctx := context.Background()

	schema := testSchemaName(t)

	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect to test database failed: %v", err)
	}
	defer admin.Close()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatalf("create test schema %s failed: %v", schema, err)
	}

	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse dsn failed: %v", err)
	}
	poolCfg.ConnConfig.RuntimeParams["search_path"] = schema

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		t.Fatalf("connect to test schema failed: %v", err)
	}
	t.Cleanup(func() {
		pool.Close()
		cleanup, err := pgxpool.New(context.Background(), dsn)
		if err != nil {
			t.Logf("reconnect to drop test schema failed: %v", err)
			return
		}
		defer cleanup.Close()
		if _, err := cleanup.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+schema+" CASCADE"); err != nil {
			t.Logf("drop test schema %s failed: %v", schema, err)
		}
	})

	return &storeFixture{t: t, pool: pool, cluster: stCluster, other: stOther, executions: make(map[string]string)}
}

// testSchemaName is a legal, unique, obviously disposable identifier.
func testSchemaName(t *testing.T) string {
	t.Helper()

	var b strings.Builder
	b.WriteString("regtest_")
	for _, r := range strings.ToLower(t.Name()) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	name := b.String()
	if len(name) > 40 {
		name = name[:40]
	}
	return fmt.Sprintf("%s_%d", name, time.Now().UnixNano()%1_000_000_000)
}

// seed writes a row directly, so a test can start from a state no sequence of
// store calls reaches — an expired lease, a row from another cluster, a deadline
// in the past.
type seedRow struct {
	sandboxID     string
	cluster       string
	state         string
	generation    int64
	originNode    string
	claimedBy     string
	snapshotID    string
	metadata      string
	leaseExpires  string // SQL expression, empty means NULL
	sandboxExpiry string // SQL expression, empty means NULL
	// executionID is the incarnation on the row. Empty means "let the fixture
	// choose": a live state gets the one this test is already using for that
	// sandbox, a parked state gets NULL, because that is what the CHECK
	// constraint allows. Set it explicitly to seed a row belonging to somebody
	// else's incarnation.
	executionID string
}

func (f *storeFixture) seed(row seedRow) {
	f.t.Helper()

	if row.cluster == "" {
		row.cluster = f.cluster
	}
	if row.generation == 0 {
		row.generation = 1
	}
	if row.metadata == "" {
		row.metadata = stMetadata
	}
	lease := row.leaseExpires
	if lease == "" {
		lease = "NULL"
	}
	expiry := row.sandboxExpiry
	if expiry == "" {
		expiry = "NULL"
	}

	var claimedBy, snapshot, execution any
	if row.claimedBy != "" {
		claimedBy = row.claimedBy
	}
	if row.snapshotID != "" {
		snapshot = row.snapshotID
	}
	switch {
	case row.executionID != "":
		execution = row.executionID
	case row.state == "running" || row.state == "publishing" || row.state == "resuming":
		execution = f.executionFor(row.sandboxID)
	}
	// execution_started_at travels with execution_id: the CHECK ties them
	// together, so a seed that set one and not the other would be refused.
	started := "NULL"
	if execution != nil {
		started = "now()"
	}

	sql := `INSERT INTO paused_sandboxes (
        sandbox_id, cluster_id, state, generation, origin_node_id, claimed_by_node_id,
        snapshot_id, metadata, paused_at, updated_at, lease_expires_at, sandbox_expires_at,
        execution_id, execution_started_at
    ) VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7::uuid, $8::jsonb, now(), now(), ` +
		lease + `, ` + expiry + `, $9::uuid, ` + started + `)`

	if _, err := f.pool.Exec(context.Background(), sql,
		row.sandboxID, row.cluster, row.state, row.generation, row.originNode,
		claimedBy, snapshot, row.metadata, execution,
	); err != nil {
		f.t.Fatalf("seed %s failed: %v", row.sandboxID, err)
	}
}

// raw reads a row without going through the decoder, so a test can look at a
// row the decoder would refuse.
type rawRow struct {
	state          string
	generation     int64
	originNode     string
	claimedBy      *string
	snapshotID     *string
	pausedAt       time.Time
	updatedAt      time.Time
	leaseExpires   *time.Time
	sandboxExpiry  *time.Time
	executionID    *string
	executionStart *time.Time
	found          bool
}

// execution renders the incarnation column for a comparison, "none" standing
// for NULL — the difference between "somebody else holds this" and "the cluster
// believes nobody does" is the whole reason the column is nullable, so it must
// not collapse into an empty string.
func (r rawRow) execution() string {
	if r.executionID == nil {
		return "none"
	}
	return *r.executionID
}

func (f *storeFixture) raw(sandboxID string) rawRow {
	f.t.Helper()

	var row rawRow
	err := f.pool.QueryRow(context.Background(), `
        SELECT state, generation, origin_node_id, claimed_by_node_id, snapshot_id::text,
               paused_at, updated_at, lease_expires_at, sandbox_expires_at,
               execution_id::text, execution_started_at
          FROM paused_sandboxes WHERE sandbox_id = $1::uuid`, sandboxID).
		Scan(&row.state, &row.generation, &row.originNode, &row.claimedBy, &row.snapshotID,
			&row.pausedAt, &row.updatedAt, &row.leaseExpires, &row.sandboxExpiry,
			&row.executionID, &row.executionStart)
	if err != nil {
		if strings.Contains(err.Error(), "no rows") {
			return rawRow{}
		}
		f.t.Fatalf("read back %s failed: %v", sandboxID, err)
	}
	row.found = true
	return row
}

// seedCleanup empties the table between sub-tests that reuse a sandbox id.
func (f *storeFixture) seedCleanup() {
	f.t.Helper()
	if _, err := f.pool.Exec(context.Background(), "DELETE FROM paused_sandboxes"); err != nil {
		f.t.Fatalf("clear table failed: %v", err)
	}
}

func (f *storeFixture) dbNow() time.Time {
	f.t.Helper()
	var now time.Time
	if err := f.pool.QueryRow(context.Background(), "SELECT now()").Scan(&now); err != nil {
		f.t.Fatalf("read database clock failed: %v", err)
	}
	return now
}

func sandboxUUID(n int) string {
	const digits = "0123456789abcdef"
	return "aaaaaaaa-0000-4000-8000-0000000000" + string([]byte{digits[(n>>4)&0xf], digits[n&0xf]})
}

func snapshotUUID(n int) string {
	const digits = "0123456789abcdef"
	return "cccccccc-0000-4000-8000-0000000000" + string([]byte{digits[(n>>4)&0xf], digits[n&0xf]})
}

// ─────────────────────────────────────────────────────────────────────────────
// Migration
// ─────────────────────────────────────────────────────────────────────────────

// TestMigrateOverAnEmptyPrePhase3Table is the upgrade path that still has to
// work.
//
// Every cluster that ran the old node-side backend has a table of exactly that
// shape. Once the runbook's DROP TABLE has emptied it — or on a cluster that
// only ever created it and never wrote — migrating has to bring it forward
// rather than fail: the ALTERs below the CREATE TABLE are the only route it
// has, which is why they are kept in a script that is otherwise a fresh build's.
//
// The table is created from a verbatim copy of the node's own DDL
// (legacyNodeSchemaDDL) rather than from Migrate. That is the point: a
// paraphrase would pass here and fail against a real one.
func TestMigrateOverAnEmptyPrePhase3Table(t *testing.T) {
	f := newStoreFixtureWithoutMigration(t)
	ctx := context.Background()

	if _, err := f.pool.Exec(ctx, legacyNodeSchemaDDL); err != nil {
		t.Fatalf("the pre-phase-3 bootstrap failed: %v", err)
	}
	if err := migrateRetryingDeadlock(func() error { return Migrate(ctx, f.pool) }); err != nil {
		t.Fatalf("migrating an empty pre-phase-3 table failed: %v", err)
	}
	// And again, because a rollout is not one step.
	if err := migrateRetryingDeadlock(func() error { return Migrate(ctx, f.pool) }); err != nil {
		t.Fatalf("second migration failed: %v", err)
	}

	store := newStoreWithPool(f.pool, StoreConfig{LeaseTTL: stLeaseTTL, Logger: zap.NewNop(), WriteFencing: true})
	if _, err := store.BeginPause(ctx, BeginPauseInput{
		ClusterID:    f.cluster,
		SandboxID:    sandboxUUID(8),
		OriginNodeID: stNodeA,
		Metadata:     json.RawMessage(stMetadata),
		ExecutionID:  newExecutionID(),
	}); err != nil {
		t.Fatalf("the table is unusable after the round trip: %v", err)
	}
}

// TestMigrateRefusesAPrePhase3Table is the other half, and the one an operator
// meets.
//
// A populated pre-phase-3 table cannot be brought forward: every live row in it
// names no incarnation, so the ADD CONSTRAINT would scan the table and fail
// naming a constraint that was born a second ago. The refusal has to arrive
// before that, say how many rows are in the way, and name the command that
// fixes it.
//
// 🔴 The assertion that this is *not* the raw PostgreSQL message is the whole
// test. Dropping the preflight leaves a build that still fails — just with the
// message this exists to replace.
func TestMigrateRefusesAPrePhase3Table(t *testing.T) {
	f := newStoreFixtureWithoutMigration(t)
	ctx := context.Background()

	if _, err := f.pool.Exec(ctx, legacyNodeSchemaDDL); err != nil {
		t.Fatalf("the pre-phase-3 bootstrap failed: %v", err)
	}
	if _, err := f.pool.Exec(ctx, `INSERT INTO paused_sandboxes (
        sandbox_id, cluster_id, state, generation, origin_node_id, metadata, paused_at, updated_at
    ) VALUES ($1::uuid, $2::uuid, 'running', 3, $3, '{}'::jsonb, now(), now())`,
		sandboxUUID(9), f.cluster, stNodeA); err != nil {
		t.Fatalf("seed a pre-phase-3 row: %v", err)
	}

	err := migrateRetryingDeadlock(func() error { return Migrate(ctx, f.pool) })
	if err == nil {
		t.Fatal("migrating a populated pre-phase-3 table succeeded; the live row it holds has no incarnation and nothing would ever give it one")
	}
	message := err.Error()
	for _, want := range []string{"DROP TABLE", "paused_sandboxes", "1"} {
		if !strings.Contains(message, want) {
			t.Fatalf("the refusal does not mention %q, so it does not tell an operator what to do: %s", want, message)
		}
	}
	// 🔴 The negative half. PostgreSQL's own wording names a constraint nobody
	// there created and says nothing about what to do; if that is what comes
	// out, the preflight did not run.
	if strings.Contains(message, "paused_sandboxes_execution_check") {
		t.Fatalf("the refusal is PostgreSQL's constraint violation rather than the preflight's: %s", message)
	}
}

// TestMigrateAcceptsATableItAlreadyOwns: the check is about pre-phase-3 rows,
// not about rows.
//
// 🟢 The control for the test above. Without it a preflight that refused every
// non-empty table would look correct — and would stop every restart of a
// healthy controller.
func TestMigrateAcceptsATableItAlreadyOwns(t *testing.T) {
	f := newStoreFixture(t)
	ctx := context.Background()

	id := sandboxUUID(10)
	if _, err := f.store.BeginPause(ctx, BeginPauseInput{
		ClusterID:    f.cluster,
		SandboxID:    id,
		OriginNodeID: stNodeA,
		Metadata:     json.RawMessage(stMetadata),
		ExecutionID:  f.executionFor(id),
	}); err != nil {
		t.Fatalf("begin pause: %v", err)
	}

	if err := migrateRetryingDeadlock(func() error { return Migrate(ctx, f.pool) }); err != nil {
		t.Fatalf("migrating a table this build populated was refused: %v", err)
	}
}

func TestMigrateIsIdempotent(t *testing.T) {
	f := newStoreFixture(t)

	// newStoreFixture already ran it once; a second run is what a restart does.
	if err := migrateRetryingDeadlock(func() error { return Migrate(context.Background(), f.pool) }); err != nil {
		t.Fatalf("second migrate failed: %v", err)
	}
	if err := migrateRetryingDeadlock(func() error { return Migrate(context.Background(), f.pool) }); err != nil {
		t.Fatalf("third migrate failed: %v", err)
	}
}

// TestMigratePinsTheTableShape is the drift alarm on the schema.
//
// It used to pin the column set to the node's, on the grounds that this side
// must add nothing the node does not know about. The node no longer writes
// here, so what is pinned now is this build's own declared shape — columns and
// constraint definitions both. Anybody adding a column to SchemaDDL without
// adding it here turns this red immediately, which is the point: the two column
// lists the read and write paths keep are already easy to miss one of, and a
// schema that grows silently is how the third gets missed too.
func TestMigratePinsTheTableShape(t *testing.T) {
	f := newStoreFixture(t)

	rows, err := f.pool.Query(context.Background(), `
        SELECT column_name FROM information_schema.columns
         WHERE table_schema = current_schema() AND table_name = 'paused_sandboxes'
         ORDER BY column_name`)
	if err != nil {
		t.Fatalf("read columns failed: %v", err)
	}
	defer rows.Close()

	got := []string{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan column failed: %v", err)
		}
		got = append(got, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read columns failed: %v", err)
	}

	want := []string{
		"claimed_by_node_id", "cluster_id", "execution_id", "execution_started_at",
		"generation", "lease_expires_at",
		"metadata", "origin_node_id", "paused_at", "sandbox_expires_at",
		"sandbox_id", "snapshot_id", "state", "updated_at",
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("column set changed:\n got %v\nwant %v", got, want)
	}

	// The constraint definitions, not merely their names. A CHECK that lost
	// one of the three live states, or one of its two conjuncts, would still
	// be present under the same name and would fence nothing.
	constraints := map[string]string{}
	crows, err := f.pool.Query(context.Background(), `
        SELECT c.conname, pg_get_constraintdef(c.oid) FROM pg_constraint c
          JOIN pg_namespace n ON n.oid = c.connamespace
         WHERE n.nspname = current_schema() AND c.contype = 'c'`)
	if err != nil {
		t.Fatalf("read constraints failed: %v", err)
	}
	defer crows.Close()
	for crows.Next() {
		var name, def string
		if err := crows.Scan(&name, &def); err != nil {
			t.Fatalf("scan constraint failed: %v", err)
		}
		constraints[name] = def
	}
	if err := crows.Err(); err != nil {
		t.Fatalf("read constraints failed: %v", err)
	}

	execution, ok := constraints["paused_sandboxes_execution_check"]
	if !ok {
		t.Fatalf("paused_sandboxes_execution_check is missing; nothing then stops a live row from naming no incarnation: %v", constraints)
	}
	for _, fragment := range []string{"running", "publishing", "resuming", "execution_id", "execution_started_at"} {
		if !strings.Contains(execution, fragment) {
			t.Fatalf("the execution CHECK no longer mentions %q: %s", fragment, execution)
		}
	}
}

func TestMigrateCreatesTheStateConstraintTheNodeExpects(t *testing.T) {
	f := newStoreFixture(t)

	var definition string
	if err := f.pool.QueryRow(context.Background(), `
        SELECT pg_get_constraintdef(c.oid) FROM pg_constraint c
          JOIN pg_namespace n ON n.oid = c.connamespace
         WHERE c.conname = 'paused_sandboxes_state_check'
           AND n.nspname = current_schema()`).Scan(&definition); err != nil {
		t.Fatalf("read constraint failed: %v", err)
	}
	for _, state := range KnownStates() {
		if !strings.Contains(definition, "'"+string(state)+"'") {
			t.Fatalf("state %q is missing from the CHECK constraint: %s", state, definition)
		}
	}

	// And nothing beyond the five: a sixth would be the exact drift that stops
	// a node from starting.
	if strings.Count(definition, "'") != 2*len(KnownStates()) {
		t.Fatalf("the CHECK constraint names something other than the five known states: %s", definition)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// BeginPause
// ─────────────────────────────────────────────────────────────────────────────

func (f *storeFixture) beginPause(sandboxID, node string) BeganPause {
	f.t.Helper()

	began, err := f.store.BeginPause(context.Background(), BeginPauseInput{
		ClusterID:    f.cluster,
		SandboxID:    sandboxID,
		OriginNodeID: node,
		Metadata:     json.RawMessage(stMetadata),
		ExecutionID:  f.executionFor(sandboxID),
	})
	if err != nil {
		f.t.Fatalf("begin_pause %s failed: %v", sandboxID, err)
	}
	return began
}

// TestAFailedPublishKeepsTheSnapshotTheSandboxAlreadyHad is the node's first
// integration test, and the reason snapshot_id is absent from the upsert's
// update list.
//
// Clearing it would leave a pause whose upload then failed pointing at no
// snapshot at all, while the perfectly good previous one sat in the repository
// with nothing referencing it — and losing the origin node at that moment would
// lose the sandbox outright.
func TestAFailedPublishKeepsTheSnapshotTheSandboxAlreadyHad(t *testing.T) {
	f := newStoreFixture(t)
	ctx := context.Background()
	id := sandboxUUID(1)

	first := f.beginPause(id, stNodeA)
	if err := f.store.CompletePause(ctx, f.cluster, id, first.Generation, snapshotUUID(1)); err != nil {
		t.Fatalf("complete_pause failed: %v", err)
	}

	// Resume it, then pause it again — and let that second pause fail before it
	// publishes anything.
	if _, err := f.store.MarkRunning(ctx, f.cluster, id, stNodeA, stNodeA, f.executionFor(id), nil); err != nil {
		t.Fatalf("mark_running failed: %v", err)
	}
	second := f.beginPause(id, stNodeA)

	if second.PreviousSnapshotID != snapshotUUID(1) {
		t.Fatalf("begin_pause should report the snapshot it superseded, got %q", second.PreviousSnapshotID)
	}
	row := f.raw(id)
	if row.snapshotID == nil || *row.snapshotID != snapshotUUID(1) {
		t.Fatalf("a pause that has not published yet must keep pointing at the last snapshot, got %v", row.snapshotID)
	}
	if second.Generation <= first.Generation {
		t.Fatalf("a re-pause must bump the generation: %d -> %d", first.Generation, second.Generation)
	}
}

func TestBeginPauseOnAFreshSandboxReportsNoPreviousSnapshot(t *testing.T) {
	f := newStoreFixture(t)
	id := sandboxUUID(2)

	began := f.beginPause(id, stNodeA)
	if began.Generation != 1 {
		t.Fatalf("a new row starts at generation 1, got %d", began.Generation)
	}
	if began.PreviousSnapshotID != "" {
		t.Fatalf("a sandbox with no history supersedes nothing, got %q", began.PreviousSnapshotID)
	}
	row := f.raw(id)
	if row.state != "publishing" || row.originNode != stNodeA || row.claimedBy != nil {
		t.Fatalf("unexpected row after begin_pause: %+v", row)
	}
}

// TestBeginPauseRefusesToTakeOverAnotherClustersRow covers the upsert's
// cluster guard, which the node's own test suite does not.
//
// Two clusters pointed at one database is an ordinary accident — the DSN is
// just a connection string. Without the guard, one would rewrite the other's
// row, fail to find the snapshot in its own repository, and then delete the row
// as dangling.
func TestBeginPauseRefusesToTakeOverAnotherClustersRow(t *testing.T) {
	f := newStoreFixture(t)
	id := sandboxUUID(3)

	f.seed(seedRow{sandboxID: id, cluster: f.other, state: "paused", originNode: "node-z", snapshotID: snapshotUUID(3)})

	_, err := f.store.BeginPause(context.Background(), BeginPauseInput{
		ClusterID:    f.cluster,
		SandboxID:    id,
		OriginNodeID: stNodeA,
		Metadata:     json.RawMessage(stMetadata),
		ExecutionID:  f.executionFor(id),
	})
	if !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("expected ErrInvalidRecord for another cluster's row, got %v", err)
	}

	row := f.raw(id)
	if row.state != "paused" || row.originNode != "node-z" {
		t.Fatalf("the other cluster's row was modified: %+v", row)
	}
}

// TestBeginPauseStampsTheDatabaseClock is the one deliberate change to the
// node's statement.
//
// The node bound its own wall clock here and the database's now() everywhere
// else, which left updated_at carrying two clocks depending on which write
// touched the row last. The restart grace period infers how long this process
// was gone from max(updated_at), so a node whose clock runs fast made the
// outage look shorter than it was — shortening the very window that exists to
// stop a mass takeover.
func TestBeginPauseStampsTheDatabaseClock(t *testing.T) {
	f := newStoreFixture(t)
	id := sandboxUUID(4)

	before := f.dbNow()
	f.beginPause(id, stNodeA)
	after := f.dbNow()

	row := f.raw(id)
	for name, stamp := range map[string]time.Time{"paused_at": row.pausedAt, "updated_at": row.updatedAt} {
		if stamp.Before(before) || stamp.After(after) {
			t.Fatalf("%s (%s) is outside the database's own clock window [%s, %s]; it came from somewhere else",
				name, stamp, before, after)
		}
	}
}

// TestBeginPauseRefusesMetadataThatIsNotAnObject.
//
// 🔴 `null` is the case worth spelling out. It is a perfectly legal JSON
// document, so a validity check accepts it and JSONB stores it happily — but
// the node's SandboxMetadata has ten required fields, so that row afterwards
// fails to decode *on the node*. The node's decoder is shared by get, get_many
// and claim_for_resume, so one such row fails its whole batch and freezes that
// machine's reconciliation entirely. One poisoned row, one stalled node, and
// the cause is a write this side let through.
func TestBeginPauseRefusesMetadataThatIsNotAnObject(t *testing.T) {
	f := newStoreFixture(t)

	for name, metadata := range map[string]string{
		"empty":       ``,
		"truncated":   `{"id":`,
		"not json":    `certainly not json`,
		"nul in text": "{\"id\":\"\x00\"}",
		"json null":   `null`,
		"whitespace":  "   \n\t ",
		"array":       `[{"id":"s"}]`,
		"string":      `"metadata"`,
		"number":      `42`,
		"bool":        `true`,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := f.store.BeginPause(context.Background(), BeginPauseInput{
				ClusterID:    f.cluster,
				SandboxID:    sandboxUUID(5),
				OriginNodeID: stNodeA,
				Metadata:     json.RawMessage(metadata),
				ExecutionID:  f.executionFor(sandboxUUID(5)),
			})
			if !errors.Is(err, ErrInvalidRecord) {
				t.Fatalf("expected ErrInvalidRecord, got %v", err)
			}
		})
	}
}

// TestBeginPauseAcceptsAnyObject: which fields belong in there is the node's
// business. An empty object is a document this side has no standing to refuse —
// refusing it would be this package forming an opinion about a schema it
// deliberately cannot see.
func TestBeginPauseAcceptsAnyObject(t *testing.T) {
	f := newStoreFixture(t)

	for name, metadata := range map[string]string{
		"empty object": `{}`,
		"unknown keys": `{"a_field_this_build_has_never_heard_of": {"nested": true}}`,
		"nested nulls": `{"id":"s","snapshot_id":null}`,
	} {
		t.Run(name, func(t *testing.T) {
			f.seedCleanup()
			if _, err := f.store.BeginPause(context.Background(), BeginPauseInput{
				ClusterID:    f.cluster,
				SandboxID:    sandboxUUID(7),
				OriginNodeID: stNodeA,
				Metadata:     json.RawMessage(metadata),
				ExecutionID:  f.executionFor(sandboxUUID(7)),
			}); err != nil {
				t.Fatalf("a JSON object was refused: %v", err)
			}
		})
	}
}

func TestBeginPauseStoresMetadataVerbatim(t *testing.T) {
	f := newStoreFixture(t)
	id := sandboxUUID(6)

	f.beginPause(id, stNodeA)

	entry, found, err := f.store.Get(context.Background(), f.cluster, id)
	if err != nil || !found {
		t.Fatalf("get failed: %v (found=%v)", err, found)
	}

	var stored, original map[string]any
	decode := func(raw []byte, into *map[string]any) {
		d := json.NewDecoder(strings.NewReader(string(raw)))
		d.UseNumber()
		if err := d.Decode(into); err != nil {
			t.Fatalf("decode %s: %v", raw, err)
		}
	}
	decode(entry.Metadata, &stored)
	decode([]byte(stMetadata), &original)

	// The large integer is the one that matters: anything that round-trips
	// through a float loses it, and nothing here would report that it had.
	nested := stored["nested"].(map[string]any)
	if got := nested["big"].(json.Number).String(); got != "9007199254740993" {
		t.Fatalf("metadata lost precision: big = %s", got)
	}
	if _, ok := stored["unknown_to_this_build"]; !ok {
		t.Fatal("metadata lost a field this build does not know about, which is exactly the field it must not lose")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// CompletePause / MarkLocalOnly
// ─────────────────────────────────────────────────────────────────────────────

func TestCompletePauseRefusesAStaleGeneration(t *testing.T) {
	f := newStoreFixture(t)
	ctx := context.Background()
	id := sandboxUUID(10)

	first := f.beginPause(id, stNodeA)
	// Somebody re-paused it, so the generation the first pause holds is stale.
	f.beginPause(id, stNodeA)

	err := f.store.CompletePause(ctx, f.cluster, id, first.Generation, snapshotUUID(10))
	if !errors.Is(err, ErrGenerationConflict) {
		t.Fatalf("expected ErrGenerationConflict, got %v", err)
	}
	if row := f.raw(id); row.snapshotID != nil {
		t.Fatalf("a refused complete_pause must not publish a snapshot, got %v", row.snapshotID)
	}
}

func TestCompletePauseRefusesARowThatIsNoLongerPublishing(t *testing.T) {
	f := newStoreFixture(t)
	ctx := context.Background()
	id := sandboxUUID(11)

	began := f.beginPause(id, stNodeA)
	if err := f.store.CompletePause(ctx, f.cluster, id, began.Generation, snapshotUUID(11)); err != nil {
		t.Fatalf("complete_pause failed: %v", err)
	}

	// The generation is still right — completing does not bump it — but the row
	// has left `publishing`, and a second publish must not overwrite the first.
	err := f.store.CompletePause(ctx, f.cluster, id, began.Generation, snapshotUUID(12))
	if !errors.Is(err, ErrGenerationConflict) {
		t.Fatalf("expected ErrGenerationConflict, got %v", err)
	}
	if row := f.raw(id); row.snapshotID == nil || *row.snapshotID != snapshotUUID(11) {
		t.Fatalf("the published snapshot was overwritten: %v", row.snapshotID)
	}
}

// TestADowngradeThatMatchesNothingIsReported is the node's own test, and its
// reasoning is the whole point of the error.
//
// A downgrade that quietly matches nothing leaves the row stuck in
// `publishing`, and every resume from another node then answers "still
// uploading" about an upload that gave up long ago. The caller cannot repair
// it, but it can say so.
func TestADowngradeThatMatchesNothingIsReported(t *testing.T) {
	f := newStoreFixture(t)
	ctx := context.Background()
	id := sandboxUUID(12)

	began := f.beginPause(id, stNodeA)
	f.beginPause(id, stNodeA)

	err := f.store.MarkLocalOnly(ctx, f.cluster, id, began.Generation)
	if !errors.Is(err, ErrGenerationConflict) {
		t.Fatalf("expected ErrGenerationConflict, got %v", err)
	}
}

// TestMarkLocalOnlyKeepsTheRow covers the reason the downgrade is not a delete.
//
// The sandbox really is paused; only nobody but its origin node can bring it
// back. Deleting the row would make "still parked on its own node"
// indistinguishable from "resumed elsewhere, or destroyed", and reconciliation
// answers the second by throwing away what is now the only copy.
func TestMarkLocalOnlyKeepsTheRow(t *testing.T) {
	f := newStoreFixture(t)
	ctx := context.Background()
	id := sandboxUUID(13)

	began := f.beginPause(id, stNodeA)
	if err := f.store.MarkLocalOnly(ctx, f.cluster, id, began.Generation); err != nil {
		t.Fatalf("mark_local_only failed: %v", err)
	}

	row := f.raw(id)
	if !row.found {
		t.Fatal("mark_local_only deleted the row; the origin node's copy is now unfindable")
	}
	if row.state != "local_only" || row.originNode != stNodeA {
		t.Fatalf("unexpected row: %+v", row)
	}
	if row.generation != began.Generation {
		t.Fatalf("mark_local_only must not bump the generation: %d -> %d", began.Generation, row.generation)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Get / GetMany
// ─────────────────────────────────────────────────────────────────────────────

// TestABatchReadReportsOnlyTheSandboxesThatHaveRows is the contract the node's
// reconciliation reads as a delete order, so it is also the contract every
// failure mode below has to stay clear of.
func TestABatchReadReportsOnlyTheSandboxesThatHaveRows(t *testing.T) {
	f := newStoreFixture(t)
	present, absent := sandboxUUID(20), sandboxUUID(21)

	f.beginPause(present, stNodeA)

	rows, err := f.store.GetMany(context.Background(), f.cluster, []string{present, absent})
	if err != nil {
		t.Fatalf("get_many failed: %v", err)
	}
	if len(rows.Entries) != 1 {
		t.Fatalf("expected exactly the one sandbox with a row, got %d", len(rows.Entries))
	}
	if _, ok := rows.Entries[present]; !ok {
		t.Fatal("the sandbox with a row is missing from the batch")
	}
	if _, ok := rows.Entries[absent]; ok {
		t.Fatal("a sandbox with no row appeared in the batch")
	}
	// The absent one is only readable as "no row" because the answer says it
	// was looked up.
	if len(rows.Covered) != 2 {
		t.Fatalf("both ids were looked up, so both must be covered: got %v", rows.Covered)
	}
}

func TestABatchReadCannotSeeAnotherClustersSandboxes(t *testing.T) {
	f := newStoreFixture(t)
	id := sandboxUUID(22)

	f.seed(seedRow{sandboxID: id, cluster: f.other, state: "paused", originNode: "node-z", snapshotID: snapshotUUID(22)})

	rows, err := f.store.GetMany(context.Background(), f.cluster, []string{id})
	if err != nil {
		t.Fatalf("get_many failed: %v", err)
	}
	if len(rows.Entries) != 0 {
		t.Fatalf("one cluster read another's row: %+v", rows.Entries)
	}
}

// TestABatchReadFailsWholeOnARowItCannotDecode is the fail-closed direction.
//
// 🔴 The tempting alternative — skip the row and return the rest — turns one
// undecodable row into a delete order for that sandbox. The node's own decoder
// fails the whole batch for the same reason, and the cost of matching it is
// that one bad row freezes a node's reconciliation, which is recoverable.
func TestABatchReadFailsWholeOnARowItCannotDecode(t *testing.T) {
	f := newStoreFixture(t)
	good, bad := sandboxUUID(23), sandboxUUID(24)

	f.beginPause(good, stNodeA)
	// `paused` with no snapshot: the row promises a cross-node resume it cannot
	// deliver. Seeded directly because no sequence of store calls produces it.
	f.seed(seedRow{sandboxID: bad, state: "paused", originNode: stNodeA})

	rows, err := f.store.GetMany(context.Background(), f.cluster, []string{good, bad})
	if !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("expected ErrInvalidRecord, got %v", err)
	}
	if rows.Entries != nil || rows.Covered != nil {
		t.Fatalf("a failed batch must return nothing at all, got %+v", rows)
	}
}

// TestABatchReadFailsRatherThanShorteningOnAMalformedID is the same property
// one step earlier: an id that never reached a WHERE clause must not be
// reported as a WHERE clause that matched nothing.
func TestABatchReadFailsRatherThanShorteningOnAMalformedID(t *testing.T) {
	f := newStoreFixture(t)
	good := sandboxUUID(25)
	f.beginPause(good, stNodeA)

	for _, malformed := range []string{"not-a-uuid", "", "aaaaaaaa-0000-4000-8000-00000000000", "{aaaaaaaa-0000-4000-8000-000000000001}"} {
		rows, err := f.store.GetMany(context.Background(), f.cluster, []string{good, malformed})
		if !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("id %q: expected ErrInvalidArgument, got %v", malformed, err)
		}
		if rows.Entries != nil || rows.Covered != nil {
			t.Fatalf("id %q: expected nothing at all, got %+v", malformed, rows)
		}
	}
}

func TestABatchReadOfNothingAsksNothing(t *testing.T) {
	f := newStoreFixture(t)

	rows, err := f.store.GetMany(context.Background(), f.cluster, nil)
	if err != nil {
		t.Fatalf("get_many of nothing failed: %v", err)
	}
	if len(rows.Entries) != 0 {
		t.Fatalf("expected an empty map, got %+v", rows.Entries)
	}
	if len(rows.Covered) != 0 {
		t.Fatalf("a batch that asked for nothing looked up nothing: got %v", rows.Covered)
	}
}

// TestGetRefusesAMalformedIDRatherThanAnsweringNoRow is the deliberate
// difference from the read-only Reader in this package.
//
// That one answers "no row" for a malformed id, because nothing downstream of
// it deletes anything. On the write path absence is what makes a caller throw
// away the only copy of a workspace.
func TestGetRefusesAMalformedIDRatherThanAnsweringNoRow(t *testing.T) {
	f := newStoreFixture(t)

	_, found, err := f.store.Get(context.Background(), f.cluster, "definitely-not-a-uuid")
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("expected ErrInvalidArgument, got %v", err)
	}
	if found {
		t.Fatal("a refused read must not report a row")
	}
}

func TestGetRefusesARowItCannotDecode(t *testing.T) {
	f := newStoreFixture(t)

	f.seed(seedRow{sandboxID: sandboxUUID(26), state: "paused", originNode: stNodeA})
	_, _, err := f.store.Get(context.Background(), f.cluster, sandboxUUID(26))
	if !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("expected ErrInvalidRecord for a paused row with no snapshot, got %v", err)
	}
}

// TestTheReadModelCarriesTheLeaseColumns is the one place this side reads more
// than the node does.
//
// The node's ENTRY_COLUMNS leaves them out, so no Rust consumer can see a
// lease. The Go Entry type declares them and Sandbox.LeaseExpired falls back to
// UpdatedAt when LeaseExpiresAt is nil — so a decoder that did not select them
// would have every entry claiming a lease that expired at the zero time.
func TestTheReadModelCarriesTheLeaseColumns(t *testing.T) {
	f := newStoreFixture(t)
	id := sandboxUUID(27)

	f.seed(seedRow{
		sandboxID: id, state: "running", originNode: stNodeA,
		leaseExpires: "now() + interval '5 minutes'", sandboxExpiry: "now() + interval '2 hours'",
	})

	entry, found, err := f.store.Get(context.Background(), f.cluster, id)
	if err != nil || !found {
		t.Fatalf("get failed: %v (found=%v)", err, found)
	}
	if entry.LeaseExpiresAt == nil {
		t.Fatal("lease_expires_at was not read; every lease judgement on this entry would be wrong")
	}
	if entry.SandboxExpiresAt == nil {
		t.Fatal("sandbox_expires_at was not read")
	}
	if entry.LeaseExpired(f.dbNow()) {
		t.Fatal("a lease five minutes in the future was read as expired")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// ClaimForResume
// ─────────────────────────────────────────────────────────────────────────────

// TestAPausedSandboxIsClaimableImmediately covers the first arm of the
// three-way test, and the reason it does not consult the lease: making every
// ordinary cross-node resume wait out a lease would cost latency for nothing.
//
// It also pins previous_state, which can only come from the `previous` CTE.
// Reading it off the returned row always says `resuming`, which is how this
// claim came to report every ordinary resume as a lease takeover for months.
func TestAPausedSandboxIsClaimableImmediately(t *testing.T) {
	f := newStoreFixture(t)
	ctx := context.Background()
	id := sandboxUUID(30)

	began := f.beginPause(id, stNodeA)
	if err := f.store.CompletePause(ctx, f.cluster, id, began.Generation, snapshotUUID(30)); err != nil {
		t.Fatalf("complete_pause failed: %v", err)
	}

	claim, err := f.store.ClaimForResume(ctx, f.cluster, id, stNodeB, f.nextExecutionFor(id))
	if err != nil {
		t.Fatalf("claim failed: %v", err)
	}
	if claim.Outcome != ClaimOutcomeClaimed {
		t.Fatalf("expected a claim, got %s", claim.Outcome)
	}
	if claim.PreviousState != StatePaused {
		t.Fatalf("an ordinary resume must not be reported as a takeover; previous_state = %q", claim.PreviousState)
	}
	if claim.Entry == nil || claim.Entry.State != StateResuming {
		t.Fatalf("the returned entry should be the row the claim produced: %+v", claim.Entry)
	}
	if claim.Entry.Generation != began.Generation+1 {
		t.Fatalf("a claim bumps the generation: %d -> %d", began.Generation, claim.Entry.Generation)
	}

	row := f.raw(id)
	if row.claimedBy == nil || *row.claimedBy != stNodeB {
		t.Fatalf("claimed_by_node_id should name the claimer, got %v", row.claimedBy)
	}
	// origin_node_id deliberately still names whoever holds the local
	// artifacts; without that the origin node cannot tell a resume happening
	// elsewhere from its own row.
	if row.originNode != stNodeA {
		t.Fatalf("a claim must not move origin_node_id, got %q", row.originNode)
	}
}

// TestALiveSandboxIsNeverTakenOverOnALapsedLease is the invariant the whole
// module exists for.
//
// A lapsed lease proves the holder cannot reach the database. It does not prove
// the holder is dead, and a partitioned node — still running every sandbox it
// has, still being routed traffic — satisfies it exactly as well as a dead one.
func TestALiveSandboxIsNeverTakenOverOnALapsedLease(t *testing.T) {
	for _, state := range []string{"running", "resuming"} {
		t.Run(state, func(t *testing.T) {
			f := newStoreFixture(t)
			id := sandboxUUID(31)

			row := seedRow{
				sandboxID: id, state: state, originNode: stNodeA, snapshotID: snapshotUUID(31),
				leaseExpires: "now() - interval '10 years'",
			}
			if state == "resuming" {
				row.claimedBy = stNodeA
			}
			f.seed(row)

			claim, err := f.store.ClaimForResume(context.Background(), f.cluster, id, stNodeB, f.nextExecutionFor(id))
			if err != nil {
				t.Fatalf("claim failed: %v", err)
			}
			if claim.Outcome != ClaimOutcomeConflict {
				t.Fatalf("a live sandbox must stay with its holder however long the lease has lapsed, got %s", claim.Outcome)
			}
			if claim.OriginNodeID != stNodeA {
				t.Fatalf("the conflict should say where the sandbox is, got %q", claim.OriginNodeID)
			}
			if after := f.raw(id); after.state != state {
				t.Fatalf("the row was modified: %s -> %s", state, after.state)
			}
		})
	}
}

// TestAParkedSandboxMovesOnOnceItsHolderStopsRenewing is the second arm.
//
// publishing and local_only name a node that paused the sandbox but never got
// its snapshot into the repository. The VM is already stopped, so rebuilding
// elsewhere cannot duplicate it — it only rewinds to the snapshot the previous
// pause left behind. That is a real loss, so it waits for a full lease.
func TestAParkedSandboxMovesOnOnceItsHolderStopsRenewing(t *testing.T) {
	for _, state := range []string{"publishing", "local_only"} {
		t.Run(state, func(t *testing.T) {
			f := newStoreFixture(t)
			id := sandboxUUID(32)

			f.seed(seedRow{
				sandboxID: id, state: state, originNode: stNodeA, snapshotID: snapshotUUID(32),
				leaseExpires: "now() - interval '1 hour'",
			})

			claim, err := f.store.ClaimForResume(context.Background(), f.cluster, id, stNodeB, f.nextExecutionFor(id))
			if err != nil {
				t.Fatalf("claim failed: %v", err)
			}
			if claim.Outcome != ClaimOutcomeClaimed {
				t.Fatalf("expected a claim, got %s", claim.Outcome)
			}
			// 🔴 The one event on this path where a user loses work, and the
			// only thing that makes it findable.
			if string(claim.PreviousState) != state {
				t.Fatalf("previous_state must name what the claim replaced, got %q", claim.PreviousState)
			}
		})
	}
}

func TestAParkedSandboxWithALiveLeaseStaysWithItsOrigin(t *testing.T) {
	f := newStoreFixture(t)
	id := sandboxUUID(33)

	f.seed(seedRow{
		sandboxID: id, state: "publishing", originNode: stNodeA, snapshotID: snapshotUUID(33),
		leaseExpires: "now() + interval '1 hour'",
	})

	claim, err := f.store.ClaimForResume(context.Background(), f.cluster, id, stNodeB, f.nextExecutionFor(id))
	if err != nil {
		t.Fatalf("claim failed: %v", err)
	}
	if claim.Outcome != ClaimOutcomeNotReady {
		t.Fatalf("expected NotReady while the holder is still renewing, got %s", claim.Outcome)
	}
	if claim.OriginNodeID != stNodeA {
		t.Fatalf("NotReady must name the origin node, got %q", claim.OriginNodeID)
	}
}

// TestASandboxThatNeverPublishedIsNeverClaimable covers the snapshot
// precondition: without one there is nothing to rebuild from, so the claim
// would hand out an authorisation its holder cannot act on.
func TestASandboxThatNeverPublishedIsNeverClaimable(t *testing.T) {
	f := newStoreFixture(t)
	id := sandboxUUID(34)

	f.seed(seedRow{
		sandboxID: id, state: "local_only", originNode: stNodeA,
		leaseExpires: "now() - interval '1 hour'",
	})

	claim, err := f.store.ClaimForResume(context.Background(), f.cluster, id, stNodeB, f.nextExecutionFor(id))
	if err != nil {
		t.Fatalf("claim failed: %v", err)
	}
	if claim.Outcome != ClaimOutcomeNotReady {
		t.Fatalf("a sandbox with no snapshot stays with its origin, got %s", claim.Outcome)
	}
	if after := f.raw(id); after.state != "local_only" {
		t.Fatalf("the row was claimed anyway: %+v", after)
	}
}

func TestAClaimOnAnUnknownSandboxIsNotFound(t *testing.T) {
	f := newStoreFixture(t)

	claim, err := f.store.ClaimForResume(context.Background(), f.cluster, sandboxUUID(35), stNodeB, f.nextExecutionFor(sandboxUUID(35)))
	if err != nil {
		t.Fatalf("claim failed: %v", err)
	}
	if claim.Outcome != ClaimOutcomeNotFound {
		t.Fatalf("expected NotFound, got %s", claim.Outcome)
	}
}

func TestAClaimCannotReachAnotherClustersSandbox(t *testing.T) {
	f := newStoreFixture(t)
	id := sandboxUUID(36)

	f.seed(seedRow{sandboxID: id, cluster: f.other, state: "paused", originNode: "node-z", snapshotID: snapshotUUID(36)})

	claim, err := f.store.ClaimForResume(context.Background(), f.cluster, id, stNodeB, f.nextExecutionFor(id))
	if err != nil {
		t.Fatalf("claim failed: %v", err)
	}
	if claim.Outcome != ClaimOutcomeNotFound {
		t.Fatalf("one cluster reached another's sandbox: %s", claim.Outcome)
	}
	if after := f.raw(id); after.state != "paused" || after.claimedBy != nil {
		t.Fatalf("the other cluster's row was modified: %+v", after)
	}
}

func TestAClaimCarriesTheMetadataTheResumeWillRebuildFrom(t *testing.T) {
	f := newStoreFixture(t)
	ctx := context.Background()
	id := sandboxUUID(37)

	began := f.beginPause(id, stNodeA)
	if err := f.store.CompletePause(ctx, f.cluster, id, began.Generation, snapshotUUID(37)); err != nil {
		t.Fatalf("complete_pause failed: %v", err)
	}

	claim, err := f.store.ClaimForResume(ctx, f.cluster, id, stNodeB, f.nextExecutionFor(id))
	if err != nil || claim.Outcome != ClaimOutcomeClaimed {
		t.Fatalf("claim failed: %v (%s)", err, claim.Outcome)
	}
	if len(claim.Entry.Metadata) == 0 {
		t.Fatal("the claim carried no metadata; this is the only path that reads it, so the resume has nothing to rebuild from")
	}
	var doc map[string]any
	if err := json.Unmarshal(claim.Entry.Metadata, &doc); err != nil {
		t.Fatalf("claim metadata is not a document: %v", err)
	}
	if _, ok := doc["unknown_to_this_build"]; !ok {
		t.Fatal("the claim dropped a metadata field this build does not know about")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// ReleaseClaim / MarkRunning
// ─────────────────────────────────────────────────────────────────────────────

func TestReleasingAClaimPutsTheSandboxBack(t *testing.T) {
	f := newStoreFixture(t)
	ctx := context.Background()
	id := sandboxUUID(40)

	began := f.beginPause(id, stNodeA)
	if err := f.store.CompletePause(ctx, f.cluster, id, began.Generation, snapshotUUID(40)); err != nil {
		t.Fatalf("complete_pause failed: %v", err)
	}
	claim, err := f.store.ClaimForResume(ctx, f.cluster, id, stNodeB, f.nextExecutionFor(id))
	if err != nil || claim.Outcome != ClaimOutcomeClaimed {
		t.Fatalf("claim failed: %v (%s)", err, claim.Outcome)
	}

	matched, err := f.store.ReleaseClaim(ctx, f.cluster, id, claim.Entry.Generation)
	if err != nil {
		t.Fatalf("release_claim failed: %v", err)
	}
	if !matched {
		t.Fatal("a release quoting the claim's own generation must match")
	}
	row := f.raw(id)
	if row.state != "paused" || row.claimedBy != nil {
		t.Fatalf("a released claim goes back to paused with no claimer: %+v", row)
	}
	if row.generation != claim.Entry.Generation {
		t.Fatalf("release_claim must not bump the generation: %d -> %d", claim.Entry.Generation, row.generation)
	}
}

// TestAReleaseThatMatchesNothingSucceedsAndSaysSo is the one conditional write
// whose no-op is a success, and it is the node's behaviour.
//
// A release that matches nothing means somebody else already moved the row on,
// which is the outcome the release was trying to produce. Reporting it as a
// failure would give its caller — an error path already unwinding a failed
// resume — a second failure it has no way to act on.
//
// 🔴 Success, but it must not be silent. The node used to run this statement
// itself and could see the zero-row tag; once it moved behind an RPC, that
// evidence stops at the controller unless it is carried back, and it is the
// only thing that says a node is quoting generations it has already lost.
func TestAReleaseThatMatchesNothingSucceedsAndSaysSo(t *testing.T) {
	f := newStoreFixture(t)
	ctx := context.Background()
	id := sandboxUUID(41)

	f.seed(seedRow{sandboxID: id, state: "paused", generation: 9, originNode: stNodeA, snapshotID: snapshotUUID(41)})

	matched, err := f.store.ReleaseClaim(ctx, f.cluster, id, 3)
	if err != nil {
		t.Fatalf("a release that matches nothing must not be an error, got %v", err)
	}
	if matched {
		t.Fatal("a stale generation cannot have matched the row")
	}

	matched, err = f.store.ReleaseClaim(ctx, f.cluster, sandboxUUID(42), 1)
	if err != nil {
		t.Fatalf("releasing a sandbox with no row must not be an error, got %v", err)
	}
	if matched {
		t.Fatal("a sandbox with no row cannot have matched anything")
	}
}

// TestMarkingAnUntrackedSandboxRunningReportsThatItIsUntracked pins the half of
// mark_running that matters most.
//
// A sandbox the cluster was never told about must stay that way. Creating a row
// here would mean every resume on a node with a node-local history starts
// publishing rows for sandboxes that have no snapshot behind them — and a row
// with no snapshot can never be claimed and never be cleared.
func TestMarkingAnUntrackedSandboxRunningReportsThatItIsUntracked(t *testing.T) {
	f := newStoreFixture(t)
	id := sandboxUUID(43)

	outcome, err := f.store.MarkRunning(context.Background(), f.cluster, id, stNodeA, stNodeA, f.executionFor(id), nil)
	if err != nil {
		t.Fatalf("mark_running failed: %v", err)
	}
	if outcome != MarkRunningUntracked {
		t.Fatalf("the cluster does not track this sandbox, and said otherwise: got %q", outcome)
	}
	if row := f.raw(id); row.found {
		t.Fatalf("mark_running created a row: %+v", row)
	}
}

// TestMarkingRunningCannotEraseAnotherNodesClaim covers the claim guard.
//
// Without it a blind write would clear claimed_by_node_id mid-claim, and both
// nodes would go on to bring the same sandbox up believing they held it — a
// second live copy, and a row naming only whichever wrote last.
func TestMarkingRunningCannotEraseAnotherNodesClaim(t *testing.T) {
	f := newStoreFixture(t)
	id := sandboxUUID(44)

	f.seed(seedRow{
		sandboxID: id, state: "resuming", generation: 4, originNode: stNodeA,
		claimedBy: stNodeB, snapshotID: snapshotUUID(44),
	})

	outcome, err := f.store.MarkRunning(context.Background(), f.cluster, id, "node-c", "node-c", f.executionFor(id), nil)
	if err != nil {
		t.Fatalf("mark_running failed: %v", err)
	}
	if outcome == MarkRunningAdopted {
		t.Fatal("a node claimed a sandbox another node holds the claim on")
	}
	// 🔴 And it says *which* refusal this is. Untracked means the cluster has
	// no opinion and the caller carries on; held-elsewhere means two nodes are
	// bringing the same sandbox up. They arrived as the same `false` until the
	// re-read that tells them apart moved to the server.
	if outcome != MarkRunningHeldElsewhere {
		t.Fatalf("a refusal caused by another node's claim must say so: got %q", outcome)
	}
	row := f.raw(id)
	if row.state != "resuming" || row.claimedBy == nil || *row.claimedBy != stNodeB {
		t.Fatalf("the other node's claim was erased: %+v", row)
	}
}

func TestMarkingATrackedSandboxRunningReportsTheNodeAsHolder(t *testing.T) {
	f := newStoreFixture(t)
	ctx := context.Background()
	id := sandboxUUID(45)

	f.seed(seedRow{
		sandboxID: id, state: "resuming", generation: 4, originNode: stNodeA,
		claimedBy: stNodeB, snapshotID: snapshotUUID(45),
	})

	outcome, err := f.store.MarkRunning(ctx, f.cluster, id, stNodeB, stNodeB, f.executionFor(id), nil)
	if err != nil {
		t.Fatalf("mark_running failed: %v", err)
	}
	if outcome != MarkRunningAdopted {
		t.Fatalf("the claimer must be able to mark its own claim running: got %q", outcome)
	}
	row := f.raw(id)
	if row.state != "running" || row.originNode != stNodeB || row.claimedBy != nil {
		t.Fatalf("unexpected row: %+v", row)
	}
	if row.generation != 5 {
		t.Fatalf("mark_running bumps the generation, got %d", row.generation)
	}
}

// 🔴 The permanent-orphan window.
//
// Reclamation requires a lapsed lease *and* a deadline that has passed, and
// NULL is not a deadline that has passed — it never matches, at any point in
// the future. Until D11 only renew_lease wrote the column, so every row spent
// its first reconcile interval carrying none. A node lost inside that window
// left a row that nothing could ever act on: not claimable (it is live), not
// reclaimable (no deadline), not removable (nobody owns it).
func TestMarkRunningStampsTheDeadlineReclamationNeeds(t *testing.T) {
	f := newStoreFixture(t)
	ctx := context.Background()
	id := sandboxUUID(47)

	f.seed(seedRow{
		sandboxID: id, state: "resuming", generation: 4, originNode: stNodeA,
		claimedBy: stNodeB, snapshotID: snapshotUUID(47),
	})
	if before := f.raw(id); before.sandboxExpiry != nil {
		t.Fatalf("the fixture already carries a deadline, so this proves nothing: %+v", before)
	}

	deadline := f.dbNow().Add(2 * time.Hour)
	if outcome, err := f.store.MarkRunning(ctx, f.cluster, id, stNodeB, stNodeB, f.executionFor(id), &deadline); err != nil || outcome != MarkRunningAdopted {
		t.Fatalf("mark_running failed: %v (%s)", err, outcome)
	}

	row := f.raw(id)
	if row.sandboxExpiry == nil {
		t.Fatal("a row marked running with a deadline must carry it before the first renewal")
	}
	if got := row.sandboxExpiry.Sub(deadline).Abs(); got > time.Second {
		t.Fatalf("the deadline drifted by %s", got)
	}
}

// A sandbox asked never to expire has to stay that way. Absent is not zero:
// zero is a deadline in 1970, which reclamation acts on immediately.
func TestMarkRunningLeavesAnAbsentDeadlineAbsent(t *testing.T) {
	f := newStoreFixture(t)
	ctx := context.Background()
	id := sandboxUUID(48)

	f.seed(seedRow{
		sandboxID: id, state: "resuming", generation: 4, originNode: stNodeA,
		claimedBy: stNodeB, snapshotID: snapshotUUID(48),
	})

	if _, err := f.store.MarkRunning(ctx, f.cluster, id, stNodeB, stNodeB, f.executionFor(id), nil); err != nil {
		t.Fatalf("mark_running failed: %v", err)
	}
	if row := f.raw(id); row.sandboxExpiry != nil {
		t.Fatalf("a sandbox with no deadline was given one: %v", row.sandboxExpiry)
	}
}

// TestMarkRunningPropagatesARowItCannotDecode keeps "untracked" meaning one
// thing.
//
// Untracked tells the caller the registry has no say over this sandbox, and its
// reconciliation is built on that. A row that exists but cannot be decoded is
// not that, so it has to arrive as an error.
func TestMarkRunningPropagatesARowItCannotDecode(t *testing.T) {
	f := newStoreFixture(t)
	id := sandboxUUID(46)

	// Claimed by somebody else so the guard refuses, and undecodable so the
	// re-read that follows has something to complain about.
	f.seed(seedRow{sandboxID: id, state: "paused", originNode: stNodeA, claimedBy: stNodeB})

	_, err := f.store.MarkRunning(context.Background(), f.cluster, id, "node-c", "node-c", f.executionFor(id), nil)
	if !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("expected ErrInvalidRecord, got %v", err)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// RenewSandboxDeadline
// ─────────────────────────────────────────────────────────────────────────────

// TestRenewSandboxDeadlineWritesTheExactValue is the propagation test itself:
// this is what closes the gap where POST /timeout's new deadline never
// reached paused_sandboxes.sandbox_expires_at. It has to pin the exact value
// carried in, not merely that the column stopped being NULL — a test that
// only checked non-null would pass even if the wrong deadline (say, the
// user's raw unclamped request instead of keep_alive_for's clamped one)
// reached the row.
func TestRenewSandboxDeadlineWritesTheExactValue(t *testing.T) {
	f := newStoreFixture(t)
	ctx := context.Background()
	id := sandboxUUID(5001)

	f.seed(seedRow{sandboxID: id, state: "running", originNode: stNodeA})
	if before := f.raw(id); before.sandboxExpiry != nil {
		t.Fatalf("the fixture already carries a deadline, so this proves nothing: %+v", before)
	}

	deadline := f.dbNow().Add(90 * time.Minute)
	outcome, err := f.store.RenewSandboxDeadline(ctx, f.cluster, id, f.executionFor(id), &deadline)
	if err != nil {
		t.Fatalf("renew_sandbox_deadline failed: %v", err)
	}
	if outcome != DeadlineRenewalRenewed {
		t.Fatalf("outcome: got %q, want %q", outcome, DeadlineRenewalRenewed)
	}

	row := f.raw(id)
	if row.sandboxExpiry == nil {
		t.Fatal("the deadline was not written")
	}
	if got := row.sandboxExpiry.Sub(deadline).Abs(); got > time.Second {
		t.Fatalf("the deadline drifted by %s: got %s, want %s", got, row.sandboxExpiry, deadline)
	}
}

// TestRenewSandboxDeadlineCanClearToNeverExpire proves nil is carried through
// as a real value and not read as "leave it alone" — a sandbox whose owner
// asked for no timeout at all must be able to clear a deadline an earlier
// renewal wrote.
func TestRenewSandboxDeadlineCanClearToNeverExpire(t *testing.T) {
	f := newStoreFixture(t)
	ctx := context.Background()
	id := sandboxUUID(5002)

	f.seed(seedRow{sandboxID: id, state: "running", originNode: stNodeA, sandboxExpiry: "now() + interval '1 hour'"})
	if before := f.raw(id); before.sandboxExpiry == nil {
		t.Fatal("the fixture does not carry a deadline, so clearing it proves nothing")
	}

	outcome, err := f.store.RenewSandboxDeadline(ctx, f.cluster, id, f.executionFor(id), nil)
	if err != nil {
		t.Fatalf("renew_sandbox_deadline failed: %v", err)
	}
	if outcome != DeadlineRenewalRenewed {
		t.Fatalf("outcome: got %q, want %q", outcome, DeadlineRenewalRenewed)
	}
	if row := f.raw(id); row.sandboxExpiry != nil {
		t.Fatalf("a sandbox asked never to expire still carries a deadline: %v", row.sandboxExpiry)
	}
}

// TestRenewSandboxDeadlineTouchesOnlyItsOwnColumn is the isolation half: this
// write must not become a second, uncoordinated way to renew the liveness
// lease or move any identity column. Those are RenewLiveLeases' and
// MarkRunning's jobs respectively, each with its own guard.
func TestRenewSandboxDeadlineTouchesOnlyItsOwnColumn(t *testing.T) {
	f := newStoreFixture(t)
	ctx := context.Background()
	id := sandboxUUID(5003)

	f.seed(seedRow{
		sandboxID: id, state: "running", originNode: stNodeA, generation: 7,
		leaseExpires: "now() + interval '30 seconds'",
	})
	before := f.raw(id)

	deadline := f.dbNow().Add(time.Hour)
	if _, err := f.store.RenewSandboxDeadline(ctx, f.cluster, id, f.executionFor(id), &deadline); err != nil {
		t.Fatalf("renew_sandbox_deadline failed: %v", err)
	}

	after := f.raw(id)
	if after.leaseExpires == nil || !after.leaseExpires.Equal(*before.leaseExpires) {
		t.Fatalf("lease_expires_at moved: got %v, want %v", after.leaseExpires, before.leaseExpires)
	}
	if after.generation != before.generation {
		t.Fatalf("generation moved: got %d, want %d", after.generation, before.generation)
	}
	if after.originNode != before.originNode {
		t.Fatalf("origin_node_id moved: got %q, want %q", after.originNode, before.originNode)
	}
	if after.state != before.state {
		t.Fatalf("state moved: got %q, want %q", after.state, before.state)
	}
}

// TestRenewSandboxDeadlineRefusesARowThatIsNotRunning covers a claimed
// (`resuming`) row: the incarnation on it is the one the claim allocated, but
// the row is not `running` yet, and this write only ever touches `running`
// rows. Reporting Superseded here, and leaving the row untouched, is what
// keeps a stale deadline observed against a since-superseded local view from
// ever reaching a row this call did not mean to act on.
func TestRenewSandboxDeadlineRefusesARowThatIsNotRunning(t *testing.T) {
	f := newStoreFixture(t)
	ctx := context.Background()
	id := sandboxUUID(5004)

	f.seed(seedRow{
		sandboxID: id, state: "resuming", generation: 3, originNode: stNodeA,
		claimedBy: stNodeB, snapshotID: snapshotUUID(5004),
	})
	before := f.raw(id)

	deadline := f.dbNow().Add(time.Hour)
	outcome, err := f.store.RenewSandboxDeadline(ctx, f.cluster, id, f.executionFor(id), &deadline)
	if err != nil {
		t.Fatalf("renew_sandbox_deadline failed: %v", err)
	}
	if outcome != DeadlineRenewalSuperseded {
		t.Fatalf("outcome: got %q, want %q", outcome, DeadlineRenewalSuperseded)
	}
	if after := f.raw(id); after.sandboxExpiry != nil {
		t.Fatalf("a superseded write must not touch the row: %+v (was %+v)", after, before)
	}
}

// TestRenewSandboxDeadlineRefusesADifferentIncarnation is the guard's whole
// point: a deadline computed against a `running` sandbox under incarnation A
// must never attach to the row once it has moved on to incarnation B — a
// fresh resume after a pause, running under a fresh execution_id. Superseded,
// not an error, and not written.
func TestRenewSandboxDeadlineRefusesADifferentIncarnation(t *testing.T) {
	f := newStoreFixture(t)
	ctx := context.Background()
	id := sandboxUUID(5005)

	f.seed(seedRow{sandboxID: id, state: "running", originNode: stNodeA})
	// The row's own incarnation, seeded by f.seed via f.executionFor. A
	// caller quoting anything else is quoting a stale, since-replaced one.
	staleExecution := f.nextExecutionFor(id)

	deadline := f.dbNow().Add(time.Hour)
	outcome, err := f.store.RenewSandboxDeadline(ctx, f.cluster, id, staleExecution, &deadline)
	if err != nil {
		t.Fatalf("renew_sandbox_deadline failed: %v", err)
	}
	if outcome != DeadlineRenewalSuperseded {
		t.Fatalf("outcome: got %q, want %q", outcome, DeadlineRenewalSuperseded)
	}
	if after := f.raw(id); after.sandboxExpiry != nil {
		t.Fatalf("a fenced write must not touch the row: %+v", after)
	}
}

// TestRenewSandboxDeadlineOnAnUntrackedSandboxReportsNotTracked is the common,
// healthy case: a sandbox this cluster has no row for at all — a disabled
// registry, or a resume whose own mark_running has not landed yet.
func TestRenewSandboxDeadlineOnAnUntrackedSandboxReportsNotTracked(t *testing.T) {
	f := newStoreFixture(t)
	id := sandboxUUID(5006)

	deadline := f.dbNow().Add(time.Hour)
	outcome, err := f.store.RenewSandboxDeadline(context.Background(), f.cluster, id, f.executionFor(id), &deadline)
	if err != nil {
		t.Fatalf("renew_sandbox_deadline failed: %v", err)
	}
	if outcome != DeadlineRenewalNotTracked {
		t.Fatalf("outcome: got %q, want %q", outcome, DeadlineRenewalNotTracked)
	}
	if row := f.raw(id); row.found {
		t.Fatalf("renew_sandbox_deadline created a row: %+v", row)
	}
}

// TestRenewSandboxDeadlineCannotReachAnotherClustersRow is the multi-tenancy
// guard every write on this interface carries.
func TestRenewSandboxDeadlineCannotReachAnotherClustersRow(t *testing.T) {
	f := newStoreFixture(t)
	ctx := context.Background()
	id := sandboxUUID(5007)

	f.seed(seedRow{sandboxID: id, cluster: f.other, state: "running", originNode: "node-z"})
	if before := f.raw(id); before.sandboxExpiry != nil {
		t.Fatalf("the fixture already carries a deadline, so this proves nothing: %+v", before)
	}

	deadline := f.dbNow().Add(time.Hour)
	outcome, err := f.store.RenewSandboxDeadline(ctx, f.cluster, id, f.executionFor(id), &deadline)
	if err != nil {
		t.Fatalf("renew_sandbox_deadline failed: %v", err)
	}
	if outcome != DeadlineRenewalNotTracked {
		t.Fatalf("one cluster reached another's row: outcome %q", outcome)
	}
	if after := f.raw(id); after.sandboxExpiry != nil {
		t.Fatalf("another cluster's row was touched: %+v", after)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// RenewLease
// ─────────────────────────────────────────────────────────────────────────────

// TestOnlyTheHolderCanRenewItsLease is why the whole roster is passed and the
// predicate decides.
//
// A node cannot extend another node's lease by listing a sandbox it does not
// hold — which is what lets the caller pass its entire local roster without
// having to work out first which of those it has standing to renew.
func TestOnlyTheHolderCanRenewItsLease(t *testing.T) {
	f := newStoreFixture(t)
	ctx := context.Background()

	mine, theirs := sandboxUUID(50), sandboxUUID(51)
	f.seed(seedRow{sandboxID: mine, state: "running", originNode: stNodeA, leaseExpires: "now() - interval '1 hour'"})
	f.seed(seedRow{sandboxID: theirs, state: "running", originNode: stNodeB, leaseExpires: "now() - interval '1 hour'"})

	renewed, err := f.store.RenewLease(ctx, f.cluster, stNodeA, []HeldSandbox{
		{SandboxID: mine}, {SandboxID: theirs},
	})
	if err != nil {
		t.Fatalf("renew_lease failed: %v", err)
	}
	if renewed != 1 {
		t.Fatalf("expected exactly the caller's own row to renew, got %d", renewed)
	}
	if row := f.raw(mine); row.leaseExpires == nil || row.leaseExpires.Before(f.dbNow()) {
		t.Fatalf("the holder's own lease was not renewed: %+v", row.leaseExpires)
	}
	if row := f.raw(theirs); row.leaseExpires == nil || row.leaseExpires.After(f.dbNow()) {
		t.Fatalf("a node renewed a lease on a sandbox it does not hold: %+v", row.leaseExpires)
	}
}

// TestAParkedSandboxIsNotRenewed pins the state list.
//
// `paused` means nobody holds the sandbox, so there is nothing to keep alive
// and a renewal would only be noise on a row whose claimability does not
// consult the lease at all.
func TestAParkedSandboxIsNotRenewed(t *testing.T) {
	f := newStoreFixture(t)
	id := sandboxUUID(52)

	f.seed(seedRow{sandboxID: id, state: "paused", originNode: stNodeA, snapshotID: snapshotUUID(52), leaseExpires: "now() - interval '1 hour'"})

	renewed, err := f.store.RenewLease(context.Background(), f.cluster, stNodeA, []HeldSandbox{{SandboxID: id}})
	if err != nil {
		t.Fatalf("renew_lease failed: %v", err)
	}
	if renewed != 0 {
		t.Fatalf("a paused row has no holder to renew for, got %d", renewed)
	}
}

func TestAClaimerRenewsTheSandboxItIsBringingUp(t *testing.T) {
	f := newStoreFixture(t)
	id := sandboxUUID(53)

	// origin is A, but B holds the claim — so B is the one with standing.
	f.seed(seedRow{
		sandboxID: id, state: "resuming", originNode: stNodeA, claimedBy: stNodeB,
		snapshotID: snapshotUUID(53), leaseExpires: "now() - interval '1 hour'",
	})

	if renewed, err := f.store.RenewLease(context.Background(), f.cluster, stNodeA, []HeldSandbox{{SandboxID: id}}); err != nil || renewed != 0 {
		t.Fatalf("the origin node must not renew a claim it does not hold: %d, %v", renewed, err)
	}
	if renewed, err := f.store.RenewLease(context.Background(), f.cluster, stNodeB, []HeldSandbox{{SandboxID: id}}); err != nil || renewed != 1 {
		t.Fatalf("the claimer must renew its own claim: %d, %v", renewed, err)
	}
}

// TestARenewalMovesTheDeadlineTheRowIsJudgedAgainst is why the deadline travels
// with the renewal instead of being read off the row.
//
// The metadata is whatever the sandbox looked like when it was paused; a resume
// may set a different timeout, and callers extend timeouts on live sandboxes
// all the time. Reading the deadline out of the row would reclaim sandboxes
// that still had hours to run.
func TestARenewalMovesTheDeadlineTheRowIsJudgedAgainst(t *testing.T) {
	f := newStoreFixture(t)
	ctx := context.Background()
	id := sandboxUUID(54)

	f.seed(seedRow{
		sandboxID: id, state: "running", originNode: stNodeA, snapshotID: snapshotUUID(54),
		leaseExpires: "now() - interval '1 hour'", sandboxExpiry: "now() - interval '1 hour'",
	})

	// The sandbox was given more time, and the holder says so on its next
	// renewal. It must stop being reclaimable.
	extended := f.dbNow().Add(2 * time.Hour)
	if _, err := f.store.RenewLease(ctx, f.cluster, stNodeA, []HeldSandbox{{SandboxID: id, ExpiresAt: &extended}}); err != nil {
		t.Fatalf("renew_lease failed: %v", err)
	}

	row := f.raw(id)
	if row.sandboxExpiry == nil || row.sandboxExpiry.Before(f.dbNow()) {
		t.Fatalf("the deadline did not move: %v", row.sandboxExpiry)
	}

	freed, err := f.store.ReclaimExpiredHoldings(ctx, f.cluster)
	if err != nil {
		t.Fatalf("reclaim failed: %v", err)
	}
	if freed.Released != 0 || freed.Discarded != 0 {
		t.Fatalf("a sandbox whose deadline moved must not be reclaimed: %+v", freed)
	}
}

// TestANilDeadlineMeansNeverExpire, not "unknown".
func TestANilDeadlineMeansNeverExpire(t *testing.T) {
	f := newStoreFixture(t)
	ctx := context.Background()
	id := sandboxUUID(55)

	f.seed(seedRow{
		sandboxID: id, state: "running", originNode: stNodeA, snapshotID: snapshotUUID(55),
		leaseExpires: "now() + interval '1 hour'", sandboxExpiry: "now() - interval '1 hour'",
	})

	if _, err := f.store.RenewLease(ctx, f.cluster, stNodeA, []HeldSandbox{{SandboxID: id, ExpiresAt: nil}}); err != nil {
		t.Fatalf("renew_lease failed: %v", err)
	}
	if row := f.raw(id); row.sandboxExpiry != nil {
		t.Fatalf("a renewal reporting no deadline must clear it, got %v", row.sandboxExpiry)
	}
}

func TestARenewalOfNothingAsksNothing(t *testing.T) {
	f := newStoreFixture(t)

	renewed, err := f.store.RenewLease(context.Background(), f.cluster, stNodeA, nil)
	if err != nil {
		t.Fatalf("renew_lease of nothing failed: %v", err)
	}
	if renewed != 0 {
		t.Fatalf("expected 0, got %d", renewed)
	}
}

// TestTheCallersLeaseLengthWins is why WithLeaseTTL exists.
//
// The floor that keeps a lease longer than the cadence renewing it is computed
// on the node, from the node's own reconcile interval. A controller stamping a
// shorter one would expire the leases of nodes that are renewing exactly as
// they were told to — and a parked row with an expired lease is one another
// node may take, rewinding the sandbox to an older snapshot.
func TestTheCallersLeaseLengthWins(t *testing.T) {
	f := newStoreFixture(t)
	id := sandboxUUID(56)

	f.seed(seedRow{sandboxID: id, state: "running", originNode: stNodeA, leaseExpires: "now() - interval '1 hour'"})

	long := 6 * time.Hour
	if _, err := f.store.WithLeaseTTL(long).RenewLease(context.Background(), f.cluster, stNodeA, []HeldSandbox{{SandboxID: id}}); err != nil {
		t.Fatalf("renew_lease failed: %v", err)
	}

	row := f.raw(id)
	if row.leaseExpires == nil {
		t.Fatal("no lease was written")
	}
	// The store's own TTL is 90s, so anything near that means the caller's
	// value was dropped.
	if row.leaseExpires.Sub(f.dbNow()) < 5*time.Hour {
		t.Fatalf("the caller's lease length was ignored: expires in %s", row.leaseExpires.Sub(f.dbNow()))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// RenewParkedLeases
// ─────────────────────────────────────────────────────────────────────────────
//
// RenewLease's narrower sibling for the scheduler's own heartbeat-driven
// reconciliation: a caller-asserted (sandbox, node) list rather than one
// node's own roster under its own identity, restricted to the two parked
// states a takeover can act on, and never touching sandbox_expires_at.

// TestRenewParkedLeasesOnlyTheAssertedHolderMatches is RenewLease's own
// TestOnlyTheHolderCanRenewItsLease, for the statement that takes a caller-
// supplied holder instead of trusting the caller's own identity.
//
// The predicate still has to do the checking, because the caller here is the
// scheduler's own reconciliation asserting what a node's roster told it — and
// an assertion that were trusted outright would let a stale or malicious
// roster entry renew a row it does not actually describe.
func TestRenewParkedLeasesOnlyTheAssertedHolderMatches(t *testing.T) {
	f := newStoreFixture(t)
	ctx := context.Background()
	id := sandboxUUID(60)

	f.seed(seedRow{sandboxID: id, state: "publishing", originNode: stNodeA, leaseExpires: "now() - interval '1 hour'"})

	// The control: asserting the wrong holder must renew nothing.
	if renewed, err := f.store.RenewParkedLeases(ctx, f.cluster, []ParkedLeaseHolder{{SandboxID: id, NodeID: stNodeB}}); err != nil || renewed != 0 {
		t.Fatalf("asserting the wrong holder must renew nothing: renewed=%d err=%v", renewed, err)
	}
	if row := f.raw(id); row.leaseExpires == nil || row.leaseExpires.After(f.dbNow()) {
		t.Fatalf("the row must still read as lapsed after a mismatched assertion: %+v", row.leaseExpires)
	}

	// The row's own holder does renew it.
	if renewed, err := f.store.RenewParkedLeases(ctx, f.cluster, []ParkedLeaseHolder{{SandboxID: id, NodeID: stNodeA}}); err != nil || renewed != 1 {
		t.Fatalf("the row's own holder must renew it: renewed=%d err=%v", renewed, err)
	}
	if row := f.raw(id); row.leaseExpires == nil || !row.leaseExpires.After(f.dbNow()) {
		t.Fatalf("the correct holder's renewal must land: %+v", row.leaseExpires)
	}
}

// TestRenewParkedLeasesTouchesOnlyPublishingAndLocalOnly pins the state list
// against all five states in one round, the same way
// TestContractReclamationLeavesParkedRowsAlone pins reclamation's.
//
// `paused` has no holder to renew for. `running` and `resuming` lapsing
// invites no takeover on a timer — see liveLeaseLapsed's own doc in
// reconcile.go — so this statement has no business touching them, unlike
// RenewLease which the node itself uses for all four live/parked states.
func TestRenewParkedLeasesTouchesOnlyPublishingAndLocalOnly(t *testing.T) {
	f := newStoreFixture(t)
	ctx := context.Background()

	paused := sandboxUUID(61)
	running := sandboxUUID(62)
	resuming := sandboxUUID(63)
	publishing := sandboxUUID(64)
	localOnly := sandboxUUID(65)

	f.seed(seedRow{sandboxID: paused, state: "paused", originNode: stNodeA, snapshotID: snapshotUUID(61), leaseExpires: "now() - interval '1 hour'"})
	f.seed(seedRow{sandboxID: running, state: "running", originNode: stNodeA, leaseExpires: "now() - interval '1 hour'"})
	f.seed(seedRow{sandboxID: resuming, state: "resuming", originNode: stNodeB, claimedBy: stNodeA, snapshotID: snapshotUUID(63), leaseExpires: "now() - interval '1 hour'"})
	f.seed(seedRow{sandboxID: publishing, state: "publishing", originNode: stNodeA, leaseExpires: "now() - interval '1 hour'"})
	f.seed(seedRow{sandboxID: localOnly, state: "local_only", originNode: stNodeA, snapshotID: snapshotUUID(65), leaseExpires: "now() - interval '1 hour'"})

	renewed, err := f.store.RenewParkedLeases(ctx, f.cluster, []ParkedLeaseHolder{
		{SandboxID: paused, NodeID: stNodeA},
		{SandboxID: running, NodeID: stNodeA},
		{SandboxID: resuming, NodeID: stNodeA},
		{SandboxID: publishing, NodeID: stNodeA},
		{SandboxID: localOnly, NodeID: stNodeA},
	})
	if err != nil {
		t.Fatalf("renew_parked_leases failed: %v", err)
	}
	if renewed != 2 {
		t.Fatalf("expected exactly the publishing and local_only rows to renew, got %d", renewed)
	}

	for _, tc := range []struct {
		name string
		id   string
		want bool // true: lease must have moved to the future
	}{
		{"paused", paused, false},
		{"running", running, false},
		{"resuming", resuming, false},
		{"publishing", publishing, true},
		{"local_only", localOnly, true},
	} {
		row := f.raw(tc.id)
		moved := row.leaseExpires != nil && row.leaseExpires.After(f.dbNow())
		if moved != tc.want {
			t.Fatalf("%s: lease moved=%v, want %v (%+v)", tc.name, moved, tc.want, row.leaseExpires)
		}
	}
}

// TestRenewParkedLeasesLeavesSandboxExpiresAtAlone is the one property
// renewLeaseSQL's own doc warns is easy to get wrong by writing the two
// clocks at different moments — this statement's answer is to not write the
// second clock at all.
//
// The lease gap is asserted as a measured duration rather than merely
// "not expired", so a statement that renewed nothing but merely raced past
// its own seeded deadline could not pass this by accident.
func TestRenewParkedLeasesLeavesSandboxExpiresAtAlone(t *testing.T) {
	f := newStoreFixture(t)
	ctx := context.Background()
	id := sandboxUUID(66)

	f.seed(seedRow{
		sandboxID: id, state: "local_only", originNode: stNodeA, snapshotID: snapshotUUID(66),
		leaseExpires: "now() - interval '1 hour'", sandboxExpiry: "now() + interval '2 hours'",
	})
	before := f.raw(id)
	if before.leaseExpires == nil || before.sandboxExpiry == nil {
		t.Fatal("the seed must have written both clocks")
	}

	if renewed, err := f.store.RenewParkedLeases(ctx, f.cluster, []ParkedLeaseHolder{{SandboxID: id, NodeID: stNodeA}}); err != nil || renewed != 1 {
		t.Fatalf("renew_parked_leases failed: renewed=%d err=%v", renewed, err)
	}

	after := f.raw(id)
	if after.sandboxExpiry == nil || !after.sandboxExpiry.Equal(*before.sandboxExpiry) {
		t.Fatalf("sandbox_expires_at must be untouched: before=%v after=%v", before.sandboxExpiry, after.sandboxExpiry)
	}
	// A measured gap, not merely "later than now": the seeded lease was an hour
	// in the past and stLeaseTTL pushes a fresh one out from now, so the two
	// must differ by comfortably more than an hour — a margin no clock
	// granularity or scheduling jitter on a test machine could produce by
	// accident.
	if gap := after.leaseExpires.Sub(*before.leaseExpires); gap < 55*time.Minute {
		t.Fatalf("expected the lease to move forward by roughly an hour, moved by %s", gap)
	}
}

// TestRenewParkedLeasesCannotReachAnotherClustersRows mirrors RenewLease's own
// TestContractARenewalCannotReachAnotherClustersRows: node ids name machines
// and repeat across clusters, so the scope has to be checked, not merely
// carried.
func TestRenewParkedLeasesCannotReachAnotherClustersRows(t *testing.T) {
	f := newStoreFixture(t)
	ctx := context.Background()
	id := sandboxUUID(67)

	f.seed(seedRow{sandboxID: id, cluster: f.other, state: "publishing", originNode: stNodeA, leaseExpires: "now() - interval '1 hour'"})

	if renewed, err := f.store.RenewParkedLeases(ctx, f.cluster, []ParkedLeaseHolder{{SandboxID: id, NodeID: stNodeA}}); err != nil || renewed != 0 {
		t.Fatalf("another cluster's row must not be renewable: renewed=%d err=%v", renewed, err)
	}
	// Control: the same call scoped to the row's actual cluster does renew it,
	// proving the zero above is the cluster check and not some other mistake.
	if renewed, err := f.store.RenewParkedLeases(ctx, f.other, []ParkedLeaseHolder{{SandboxID: id, NodeID: stNodeA}}); err != nil || renewed != 1 {
		t.Fatalf("the row's own cluster must be able to renew it: renewed=%d err=%v", renewed, err)
	}
}

// TestRenewParkedLeasesOfNothingAsksNothing mirrors RenewLease's own
// TestARenewalOfNothingAsksNothing, with the positive control RenewLease's
// version does not need: a nil or empty list has to mean "nothing was asked",
// not "everything matched", and the only way to tell those apart is to show
// the same row *can* be renewed when it is actually named.
func TestRenewParkedLeasesOfNothingAsksNothing(t *testing.T) {
	f := newStoreFixture(t)
	ctx := context.Background()
	id := sandboxUUID(68)
	f.seed(seedRow{sandboxID: id, state: "publishing", originNode: stNodeA, leaseExpires: "now() - interval '1 hour'"})

	if renewed, err := f.store.RenewParkedLeases(ctx, f.cluster, nil); err != nil || renewed != 0 {
		t.Fatalf("renew_parked_leases of nothing failed: renewed=%d err=%v", renewed, err)
	}
	if row := f.raw(id); row.leaseExpires == nil || row.leaseExpires.After(f.dbNow()) {
		t.Fatalf("an empty request must not have touched the row: %+v", row.leaseExpires)
	}

	if renewed, err := f.store.RenewParkedLeases(ctx, f.cluster, []ParkedLeaseHolder{{SandboxID: id, NodeID: stNodeA}}); err != nil || renewed != 1 {
		t.Fatalf("the same row named explicitly must renew: renewed=%d err=%v", renewed, err)
	}
}

// TestRenewParkedLeasesRefusesAnEmptyNodeID pins the validation RenewLease
// gets for free from taking one node id for the whole call: this statement
// takes one per pair, so each has to be checked on its own.
func TestRenewParkedLeasesRefusesAnEmptyNodeID(t *testing.T) {
	f := newStoreFixture(t)
	ctx := context.Background()
	id := sandboxUUID(69)
	f.seed(seedRow{sandboxID: id, state: "publishing", originNode: stNodeA, leaseExpires: "now() - interval '1 hour'"})

	if _, err := f.store.RenewParkedLeases(ctx, f.cluster, []ParkedLeaseHolder{{SandboxID: id, NodeID: ""}}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("expected ErrInvalidArgument for an empty node id, got %v", err)
	}
	// Control: the same sandbox with a real node id succeeds.
	if renewed, err := f.store.RenewParkedLeases(ctx, f.cluster, []ParkedLeaseHolder{{SandboxID: id, NodeID: stNodeA}}); err != nil || renewed != 1 {
		t.Fatalf("a real node id must succeed: renewed=%d err=%v", renewed, err)
	}
}

// TestRenewParkedLeasesRefusesAMalformedSandboxID is requireUUID's own guard,
// pinned here rather than assumed: a malformed id failing the whole call
// loudly is what stops one bad id from being silently dropped out of a batch.
func TestRenewParkedLeasesRefusesAMalformedSandboxID(t *testing.T) {
	f := newStoreFixture(t)
	ctx := context.Background()

	if _, err := f.store.RenewParkedLeases(ctx, f.cluster, []ParkedLeaseHolder{{SandboxID: "not-a-uuid", NodeID: stNodeA}}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("expected ErrInvalidArgument for a malformed sandbox id, got %v", err)
	}

	// Control: a well-formed id in the same call shape succeeds.
	id := sandboxUUID(70)
	f.seed(seedRow{sandboxID: id, state: "publishing", originNode: stNodeA, leaseExpires: "now() - interval '1 hour'"})
	if renewed, err := f.store.RenewParkedLeases(ctx, f.cluster, []ParkedLeaseHolder{{SandboxID: id, NodeID: stNodeA}}); err != nil || renewed != 1 {
		t.Fatalf("a well-formed id must succeed: renewed=%d err=%v", renewed, err)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// RenewLiveLeases
//
// RenewParkedLeases' own test suite above pins that a `running` row's lease is
// never touched by that statement (TestRenewParkedLeasesTouchesOnlyPublishingAndLocalOnly).
// The tests below are that statement's mirror image for RenewLiveLeases:
// pinning that *only* `running` rows move, with the same shape of assertions
// RenewParkedLeases' own suite uses — a measured lease movement, not merely a
// nil error, since a zero-row UPDATE also returns nil.
// ─────────────────────────────────────────────────────────────────────────────

// stPodClaimant and stMachineHolder are deliberately shaped like the two
// different identity spaces the split-role deployment actually produces — a
// Kubernetes Deployment pod name and a real machine's hostname — rather than
// reusing stNodeA/stNodeB, which are both machine-shaped generic strings and
// would let a broken origin_node_id comparison pass by coincidence (compare
// two "node-a"-shaped values and a bug that accidentally matches on a
// substring, a prefix, or drops the comparison to a truthiness check would
// never show up). RenewLiveLeases exists specifically because a pod-shaped
// caller identity can never equal a running row's machine-shaped
// origin_node_id — see RenewLiveLeases' own doc — so its own tests should not
// use fixture values where that structural mismatch is invisible.
const (
	stPodClaimant   = "agentenv-api-7f9c8d5b6-x2kpq"
	stMachineHolder = "aenv-node-07.prod.internal"
)

// TestRenewLiveLeasesOnlyTheAssertedHolderMatches mirrors
// TestRenewParkedLeasesOnlyTheAssertedHolderMatches, with shapes distinct
// enough that a comparison silently reduced to "non-empty" or "same length"
// could not pass this test by accident.
func TestRenewLiveLeasesOnlyTheAssertedHolderMatches(t *testing.T) {
	f := newStoreFixture(t)
	ctx := context.Background()
	id := sandboxUUID(71)

	f.seed(seedRow{sandboxID: id, state: "running", originNode: stMachineHolder, leaseExpires: "now() - interval '1 hour'"})

	// The control: asserting the api replica's own pod identity — exactly
	// what the split-role api half's periodic renew_paused_leases call
	// reports today — must renew nothing. This is the bug RenewLiveLeases
	// exists to route around, pinned as a negative here so a regression that
	// silently widens the predicate back to accepting any caller shows up as
	// a test failure rather than as a reclaimed sandbox on a live node.
	if renewed, err := f.store.RenewLiveLeases(ctx, f.cluster, []ParkedLeaseHolder{{SandboxID: id, NodeID: stPodClaimant}}); err != nil || renewed != 0 {
		t.Fatalf("asserting a pod-shaped non-holder identity must renew nothing: renewed=%d err=%v", renewed, err)
	}
	if row := f.raw(id); row.leaseExpires == nil || row.leaseExpires.After(f.dbNow()) {
		t.Fatalf("the row must still read as lapsed after a mismatched assertion: %+v", row.leaseExpires)
	}

	// The row's own holder does renew it.
	if renewed, err := f.store.RenewLiveLeases(ctx, f.cluster, []ParkedLeaseHolder{{SandboxID: id, NodeID: stMachineHolder}}); err != nil || renewed != 1 {
		t.Fatalf("the row's own holder must renew it: renewed=%d err=%v", renewed, err)
	}
	if row := f.raw(id); row.leaseExpires == nil || !row.leaseExpires.After(f.dbNow()) {
		t.Fatalf("the correct holder's renewal must land: %+v", row.leaseExpires)
	}
}

// TestRenewLiveLeasesTouchesOnlyRunning is RenewLiveLeases' mirror of
// TestRenewParkedLeasesTouchesOnlyPublishingAndLocalOnly: every other state,
// including `resuming` — which RenewLease's own resuming branch already
// renews correctly under the claimant identity, see paused_recovery.rs — must
// come back untouched.
func TestRenewLiveLeasesTouchesOnlyRunning(t *testing.T) {
	f := newStoreFixture(t)
	ctx := context.Background()

	paused := sandboxUUID(72)
	running := sandboxUUID(73)
	resuming := sandboxUUID(74)
	publishing := sandboxUUID(75)
	localOnly := sandboxUUID(76)

	f.seed(seedRow{sandboxID: paused, state: "paused", originNode: stMachineHolder, snapshotID: snapshotUUID(72), leaseExpires: "now() - interval '1 hour'"})
	f.seed(seedRow{sandboxID: running, state: "running", originNode: stMachineHolder, leaseExpires: "now() - interval '1 hour'"})
	// originNode (not claimedBy) is deliberately set to stMachineHolder here,
	// matching every other row in this test: if the statement's state scope
	// were ever accidentally widened to include `resuming`, origin_node_id
	// would already match and this row would renew — the identity check must
	// not be the thing masking a state-scope regression.
	f.seed(seedRow{sandboxID: resuming, state: "resuming", originNode: stMachineHolder, claimedBy: stNodeB, snapshotID: snapshotUUID(74), leaseExpires: "now() - interval '1 hour'"})
	f.seed(seedRow{sandboxID: publishing, state: "publishing", originNode: stMachineHolder, leaseExpires: "now() - interval '1 hour'"})
	f.seed(seedRow{sandboxID: localOnly, state: "local_only", originNode: stMachineHolder, snapshotID: snapshotUUID(76), leaseExpires: "now() - interval '1 hour'"})

	renewed, err := f.store.RenewLiveLeases(ctx, f.cluster, []ParkedLeaseHolder{
		{SandboxID: paused, NodeID: stMachineHolder},
		{SandboxID: running, NodeID: stMachineHolder},
		{SandboxID: resuming, NodeID: stMachineHolder},
		{SandboxID: publishing, NodeID: stMachineHolder},
		{SandboxID: localOnly, NodeID: stMachineHolder},
	})
	if err != nil {
		t.Fatalf("renew_live_leases failed: %v", err)
	}
	if renewed != 1 {
		t.Fatalf("expected only the running row to renew, got %d", renewed)
	}

	for _, tc := range []struct {
		name string
		id   string
		want bool // true: lease must have moved to the future
	}{
		{"paused", paused, false},
		{"running", running, true},
		{"resuming", resuming, false},
		{"publishing", publishing, false},
		{"local_only", localOnly, false},
	} {
		row := f.raw(tc.id)
		moved := row.leaseExpires != nil && row.leaseExpires.After(f.dbNow())
		if moved != tc.want {
			t.Fatalf("%s: expected moved=%v, got leaseExpires=%v", tc.name, tc.want, row.leaseExpires)
		}
	}
}

// TestRenewLiveLeasesLeavesSandboxExpiresAtAlone mirrors
// TestRenewParkedLeasesLeavesSandboxExpiresAtAlone: the api half's own
// authority over the deadline is not something this heartbeat-driven write
// may touch, even for the one state it does renew.
func TestRenewLiveLeasesLeavesSandboxExpiresAtAlone(t *testing.T) {
	f := newStoreFixture(t)
	ctx := context.Background()
	id := sandboxUUID(77)

	f.seed(seedRow{
		sandboxID: id, state: "running", originNode: stMachineHolder,
		leaseExpires: "now() - interval '1 hour'", sandboxExpiry: "now() + interval '2 hours'",
	})
	before := f.raw(id)
	if before.leaseExpires == nil || before.sandboxExpiry == nil {
		t.Fatal("the seed must have written both clocks")
	}

	if renewed, err := f.store.RenewLiveLeases(ctx, f.cluster, []ParkedLeaseHolder{{SandboxID: id, NodeID: stMachineHolder}}); err != nil || renewed != 1 {
		t.Fatalf("renew_live_leases failed: renewed=%d err=%v", renewed, err)
	}

	after := f.raw(id)
	if after.sandboxExpiry == nil || !after.sandboxExpiry.Equal(*before.sandboxExpiry) {
		t.Fatalf("sandbox_expires_at must be untouched: before=%v after=%v", before.sandboxExpiry, after.sandboxExpiry)
	}
	if gap := after.leaseExpires.Sub(*before.leaseExpires); gap < 55*time.Minute {
		t.Fatalf("expected the lease to move forward by roughly an hour, moved by %s", gap)
	}
}

// TestRenewLiveLeasesCannotReachAnotherClustersRows mirrors
// TestRenewParkedLeasesCannotReachAnotherClustersRows.
func TestRenewLiveLeasesCannotReachAnotherClustersRows(t *testing.T) {
	f := newStoreFixture(t)
	ctx := context.Background()
	id := sandboxUUID(78)

	f.seed(seedRow{sandboxID: id, cluster: f.other, state: "running", originNode: stMachineHolder, leaseExpires: "now() - interval '1 hour'"})

	if renewed, err := f.store.RenewLiveLeases(ctx, f.cluster, []ParkedLeaseHolder{{SandboxID: id, NodeID: stMachineHolder}}); err != nil || renewed != 0 {
		t.Fatalf("another cluster's row must not be renewable: renewed=%d err=%v", renewed, err)
	}
	if renewed, err := f.store.RenewLiveLeases(ctx, f.other, []ParkedLeaseHolder{{SandboxID: id, NodeID: stMachineHolder}}); err != nil || renewed != 1 {
		t.Fatalf("the row's own cluster must be able to renew it: renewed=%d err=%v", renewed, err)
	}
}

// TestRenewLiveLeasesOfNothingAsksNothing mirrors
// TestRenewParkedLeasesOfNothingAsksNothing.
func TestRenewLiveLeasesOfNothingAsksNothing(t *testing.T) {
	f := newStoreFixture(t)
	ctx := context.Background()
	id := sandboxUUID(79)
	f.seed(seedRow{sandboxID: id, state: "running", originNode: stMachineHolder, leaseExpires: "now() - interval '1 hour'"})

	if renewed, err := f.store.RenewLiveLeases(ctx, f.cluster, nil); err != nil || renewed != 0 {
		t.Fatalf("renew_live_leases of nothing failed: renewed=%d err=%v", renewed, err)
	}
	if row := f.raw(id); row.leaseExpires == nil || row.leaseExpires.After(f.dbNow()) {
		t.Fatalf("an empty request must not have touched the row: %+v", row.leaseExpires)
	}

	if renewed, err := f.store.RenewLiveLeases(ctx, f.cluster, []ParkedLeaseHolder{{SandboxID: id, NodeID: stMachineHolder}}); err != nil || renewed != 1 {
		t.Fatalf("the same row named explicitly must renew: renewed=%d err=%v", renewed, err)
	}
}

// TestRenewLiveLeasesRefusesAnEmptyNodeID mirrors
// TestRenewParkedLeasesRefusesAnEmptyNodeID.
func TestRenewLiveLeasesRefusesAnEmptyNodeID(t *testing.T) {
	f := newStoreFixture(t)
	ctx := context.Background()
	id := sandboxUUID(80)
	f.seed(seedRow{sandboxID: id, state: "running", originNode: stMachineHolder, leaseExpires: "now() - interval '1 hour'"})

	if _, err := f.store.RenewLiveLeases(ctx, f.cluster, []ParkedLeaseHolder{{SandboxID: id, NodeID: ""}}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("expected ErrInvalidArgument for an empty node id, got %v", err)
	}
	if renewed, err := f.store.RenewLiveLeases(ctx, f.cluster, []ParkedLeaseHolder{{SandboxID: id, NodeID: stMachineHolder}}); err != nil || renewed != 1 {
		t.Fatalf("a real node id must succeed: renewed=%d err=%v", renewed, err)
	}
}

// TestRenewLiveLeasesRefusesAMalformedSandboxID mirrors
// TestRenewParkedLeasesRefusesAMalformedSandboxID.
func TestRenewLiveLeasesRefusesAMalformedSandboxID(t *testing.T) {
	f := newStoreFixture(t)
	ctx := context.Background()

	if _, err := f.store.RenewLiveLeases(ctx, f.cluster, []ParkedLeaseHolder{{SandboxID: "not-a-uuid", NodeID: stMachineHolder}}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("expected ErrInvalidArgument for a malformed sandbox id, got %v", err)
	}

	id := sandboxUUID(81)
	f.seed(seedRow{sandboxID: id, state: "running", originNode: stMachineHolder, leaseExpires: "now() - interval '1 hour'"})
	if renewed, err := f.store.RenewLiveLeases(ctx, f.cluster, []ParkedLeaseHolder{{SandboxID: id, NodeID: stMachineHolder}}); err != nil || renewed != 1 {
		t.Fatalf("a well-formed id must succeed: renewed=%d err=%v", renewed, err)
	}
}

// TestALeaseViewDoesNotOwnThePool: a view is what a per-request handler holds,
// and closing it must not drain the pool underneath the store it came from.
func TestALeaseViewDoesNotOwnThePool(t *testing.T) {
	f := newStoreFixture(t)

	view := f.store.WithLeaseTTL(6 * time.Hour)
	view.Close()

	if _, err := f.store.GetMany(context.Background(), f.cluster, nil); err != nil {
		t.Fatalf("closing a lease view took the original store down: %v", err)
	}
	if _, _, err := f.store.Get(context.Background(), f.cluster, sandboxUUID(57)); err != nil {
		t.Fatalf("closing a lease view took the original store down: %v", err)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// ReclaimExpiredHoldings
// ─────────────────────────────────────────────────────────────────────────────

// reclaimCase is one row and what the backstop pass should do with it.
//
// 🔴 Both conditions, never one. A lapsed lease alone says only that the holder
// cannot reach the database, which is the mistake ClaimForResume exists to
// avoid. A passed deadline alone would race the node's own eviction, which
// pauses the sandbox properly and publishes a fresh snapshot — by far the
// better outcome. Together they describe a sandbox that has outlived the
// deadline its own user set, on a node not heard from since before it did.
func TestReclamationNeedsBothClocks(t *testing.T) {
	cases := []struct {
		name          string
		state         string
		snapshot      bool
		lease         string
		deadline      string
		wantReleased  uint64
		wantDiscarded uint64
		wantState     string // empty means the row should be gone
	}{
		{
			name: "lease lapsed and deadline passed", state: "running", snapshot: true,
			lease: "now() - interval '1 hour'", deadline: "now() - interval '1 hour'",
			wantReleased: 1, wantState: "paused",
		},
		{
			name: "still within its deadline", state: "running", snapshot: true,
			lease: "now() - interval '1 hour'", deadline: "now() + interval '1 hour'",
			wantState: "running",
		},
		{
			name: "node is still reporting", state: "running", snapshot: true,
			lease: "now() + interval '1 hour'", deadline: "now() - interval '1 hour'",
			wantState: "running",
		},
		{
			name: "no deadline at all", state: "running", snapshot: true,
			lease: "now() - interval '1 hour'", deadline: "",
			wantState: "running",
		},
		{
			name: "nothing to rebuild from", state: "running", snapshot: false,
			lease: "now() - interval '1 hour'", deadline: "now() - interval '1 hour'",
			wantDiscarded: 1, wantState: "",
		},
		{
			name: "an interrupted resume", state: "resuming", snapshot: true,
			lease: "now() - interval '1 hour'", deadline: "now() - interval '1 hour'",
			wantReleased: 1, wantState: "paused",
		},
		{
			name: "a parked row is not live", state: "publishing", snapshot: true,
			lease: "now() - interval '1 hour'", deadline: "now() - interval '1 hour'",
			wantState: "publishing",
		},
		{
			name: "a paused row is not live", state: "paused", snapshot: true,
			lease: "now() - interval '1 hour'", deadline: "now() - interval '1 hour'",
			wantState: "paused",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newStoreFixture(t)
			id := sandboxUUID(60)

			row := seedRow{
				sandboxID: id, state: tc.state, originNode: stNodeA,
				leaseExpires: tc.lease, sandboxExpiry: tc.deadline,
			}
			if tc.snapshot {
				row.snapshotID = snapshotUUID(60)
			}
			if tc.state == "resuming" {
				row.claimedBy = stNodeB
			}
			f.seed(row)

			freed, err := f.store.ReclaimExpiredHoldings(context.Background(), f.cluster)
			if err != nil {
				t.Fatalf("reclaim failed: %v", err)
			}
			if freed.Released != tc.wantReleased || freed.Discarded != tc.wantDiscarded {
				t.Fatalf("counts: got %+v, want released=%d discarded=%d", freed, tc.wantReleased, tc.wantDiscarded)
			}

			after := f.raw(id)
			if tc.wantState == "" {
				if after.found {
					t.Fatalf("expected the row to be gone, got %+v", after)
				}
				return
			}
			if !after.found {
				t.Fatal("the row was deleted")
			}
			if after.state != tc.wantState {
				t.Fatalf("state: got %q, want %q", after.state, tc.wantState)
			}
			if tc.wantReleased > 0 {
				if after.claimedBy != nil {
					t.Fatalf("a released row keeps a claimer: %v", after.claimedBy)
				}
				// `paused` means nobody holds it; leaving a live-looking lease
				// behind would only confuse the next reader.
				if after.leaseExpires == nil || after.leaseExpires.After(f.dbNow()) {
					t.Fatalf("a released row should not carry a live lease: %v", after.leaseExpires)
				}
			}
		})
	}
}

func TestReclamationIsScopedToItsCluster(t *testing.T) {
	f := newStoreFixture(t)
	id := sandboxUUID(61)

	f.seed(seedRow{
		sandboxID: id, cluster: f.other, state: "running", originNode: "node-z", snapshotID: snapshotUUID(61),
		leaseExpires: "now() - interval '1 hour'", sandboxExpiry: "now() - interval '1 hour'",
	})

	freed, err := f.store.ReclaimExpiredHoldings(context.Background(), f.cluster)
	if err != nil {
		t.Fatalf("reclaim failed: %v", err)
	}
	if freed.Released != 0 || freed.Discarded != 0 {
		t.Fatalf("one cluster reclaimed another's rows: %+v", freed)
	}
	if row := f.raw(id); row.state != "running" {
		t.Fatalf("the other cluster's row was modified: %+v", row)
	}
}

// TestTheDiscardBreakerAbandonsTheWholePass covers guard §3.3.
//
// 🔴 The releases go back too, not just the deletes. A release is safe on its
// own, but a discard count this far out of band says the premise underneath
// both halves is wrong — they share a predicate over two clocks and a state
// column — and half of a pass nobody trusts is worse than none of it.
func TestTheDiscardBreakerAbandonsTheWholePass(t *testing.T) {
	f := newStoreFixture(t)
	f.store.WithGuards(nil, NewDiscardBreaker(2, 1.0, zap.NewNop()))

	// Three rows with nothing to rebuild from, over a limit of two.
	for i := 0; i < 3; i++ {
		f.seed(seedRow{
			sandboxID: sandboxUUID(70 + i), state: "running", originNode: stNodeA,
			leaseExpires: "now() - interval '1 hour'", sandboxExpiry: "now() - interval '1 hour'",
		})
	}
	// And one that would have been released.
	f.seed(seedRow{
		sandboxID: sandboxUUID(79), state: "running", originNode: stNodeA, snapshotID: snapshotUUID(79),
		leaseExpires: "now() - interval '1 hour'", sandboxExpiry: "now() - interval '1 hour'",
	})

	freed, err := f.store.ReclaimExpiredHoldings(context.Background(), f.cluster)
	if !errors.Is(err, ErrDiscardBreakerTripped) {
		t.Fatalf("expected ErrDiscardBreakerTripped, got %v", err)
	}
	if freed.Released != 0 || freed.Discarded != 0 {
		t.Fatalf("a refused pass must report nothing done, got %+v", freed)
	}
	for i := 0; i < 3; i++ {
		if row := f.raw(sandboxUUID(70 + i)); !row.found {
			t.Fatalf("row %d was deleted by a pass that was refused", i)
		}
	}
	if row := f.raw(sandboxUUID(79)); row.state != "running" {
		t.Fatalf("the release half ran even though the pass was refused: %+v", row)
	}
}

func TestTheDiscardBreakerLetsAnOrdinaryPassThrough(t *testing.T) {
	f := newStoreFixture(t)
	f.store.WithGuards(nil, NewDiscardBreaker(2, 1.0, zap.NewNop()))

	f.seed(seedRow{
		sandboxID: sandboxUUID(80), state: "running", originNode: stNodeA,
		leaseExpires: "now() - interval '1 hour'", sandboxExpiry: "now() - interval '1 hour'",
	})

	freed, err := f.store.ReclaimExpiredHoldings(context.Background(), f.cluster)
	if err != nil {
		t.Fatalf("reclaim failed: %v", err)
	}
	if freed.Discarded != 1 {
		t.Fatalf("expected the one row under the limit to be discarded, got %+v", freed)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// ReleaseNodeHoldings
// ─────────────────────────────────────────────────────────────────────────────

// TestASuccessorProcessReleasesWhatThePreviousOneWasRunning is the strongest
// evidence in the system, and the only thing that can free a live row.
//
// A node's id names the machine, not the process, so a row saying "running on
// this node" read by a process that has just started and holds nothing can only
// have been written by a previous process on that same machine. That process is
// gone and its sandboxes went with it — they were its children, in its PID
// namespace. No timeout can establish that.
func TestASuccessorProcessReleasesWhatThePreviousOneWasRunning(t *testing.T) {
	f := newStoreFixture(t)

	recoverable, unrecoverable := sandboxUUID(90), sandboxUUID(91)
	f.seed(seedRow{sandboxID: recoverable, state: "running", originNode: stNodeA, snapshotID: snapshotUUID(90)})
	f.seed(seedRow{sandboxID: unrecoverable, state: "running", originNode: stNodeA})

	freed, err := f.store.ReleaseNodeHoldings(context.Background(), f.cluster, stNodeA)
	if err != nil {
		t.Fatalf("release_node_holdings failed: %v", err)
	}
	if freed.Released != 1 || freed.Discarded != 1 {
		t.Fatalf("expected one of each, got %+v", freed)
	}

	// A snapshot outlives the process that was running the sandbox, so the row
	// goes back to being claimable by anyone.
	row := f.raw(recoverable)
	if row.state != "paused" || row.claimedBy != nil {
		t.Fatalf("a recoverable row should come back as paused: %+v", row)
	}
	if row.leaseExpires == nil || row.leaseExpires.After(f.dbNow()) {
		t.Fatalf("a released row should not carry a live lease: %v", row.leaseExpires)
	}
	// Without one there is nothing left to bring back: the local artifacts were
	// consumed by the resume that started it, and a row that can never be
	// claimed and never be cleared would just accumulate.
	if f.raw(unrecoverable).found {
		t.Fatal("a live row that never published should be discarded, not kept")
	}
}

// TestReleasingHoldingsTouchesNothingButThisNodesLiveRows is the double
// narrowing: another node's live rows, and this node's parked ones, are both
// out of scope.
func TestReleasingHoldingsTouchesNothingButThisNodesLiveRows(t *testing.T) {
	f := newStoreFixture(t)

	othersLive := sandboxUUID(92)
	ownParked := sandboxUUID(93)
	otherCluster := sandboxUUID(94)

	f.seed(seedRow{sandboxID: othersLive, state: "running", originNode: stNodeB, snapshotID: snapshotUUID(92)})
	f.seed(seedRow{sandboxID: ownParked, state: "paused", originNode: stNodeA, snapshotID: snapshotUUID(93)})
	f.seed(seedRow{sandboxID: otherCluster, cluster: f.other, state: "running", originNode: stNodeA, snapshotID: snapshotUUID(94)})

	freed, err := f.store.ReleaseNodeHoldings(context.Background(), f.cluster, stNodeA)
	if err != nil {
		t.Fatalf("release_node_holdings failed: %v", err)
	}
	if freed.Released != 0 || freed.Discarded != 0 {
		t.Fatalf("expected nothing in scope, got %+v", freed)
	}
	if row := f.raw(othersLive); row.state != "running" || row.originNode != stNodeB {
		t.Fatalf("another node's live row was released: %+v", row)
	}
	if row := f.raw(ownParked); row.state != "paused" {
		t.Fatalf("this node's parked row was touched: %+v", row)
	}
	if row := f.raw(otherCluster); row.state != "running" {
		t.Fatalf("another cluster's row was released: %+v", row)
	}
}

// TestAnInterruptedResumeIsReleasedByTheNodeThatClaimedIt: a resuming row is
// held by its claimer, not by origin_node_id — the claim deliberately leaves
// that pointing at whoever holds the local artifacts.
func TestAnInterruptedResumeIsReleasedByTheNodeThatClaimedIt(t *testing.T) {
	f := newStoreFixture(t)
	id := sandboxUUID(95)

	f.seed(seedRow{
		sandboxID: id, state: "resuming", originNode: stNodeA, claimedBy: stNodeB,
		snapshotID: snapshotUUID(95),
	})

	// The origin node restarting must not free a resume another node is driving.
	if freed, err := f.store.ReleaseNodeHoldings(context.Background(), f.cluster, stNodeA); err != nil || freed.Released != 0 {
		t.Fatalf("the origin node released a claim it does not hold: %+v, %v", freed, err)
	}
	if freed, err := f.store.ReleaseNodeHoldings(context.Background(), f.cluster, stNodeB); err != nil || freed.Released != 1 {
		t.Fatalf("the claimer's successor should release it: %+v, %v", freed, err)
	}
	if row := f.raw(id); row.state != "paused" || row.claimedBy != nil {
		t.Fatalf("unexpected row: %+v", row)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Remove
// ─────────────────────────────────────────────────────────────────────────────

func TestRemoveIsScopedToItsCluster(t *testing.T) {
	f := newStoreFixture(t)
	id := sandboxUUID(96)

	f.seed(seedRow{
		sandboxID: id, cluster: f.other, state: "paused", generation: 5,
		originNode: "node-z", snapshotID: snapshotUUID(96),
	})

	// The generation quoted is the other cluster's own, so only the cluster
	// filter can be what stops this.
	removed, err := f.store.Remove(context.Background(), f.cluster, id, 5)
	if err != nil {
		t.Fatalf("remove failed: %v", err)
	}
	if removed {
		t.Fatal("a delete scoped to this cluster matched another cluster's row")
	}
	if !f.raw(id).found {
		t.Fatal("one cluster deleted another's row")
	}
}

func TestRemoveOfSomethingThatIsNotThereIsNotAnError(t *testing.T) {
	f := newStoreFixture(t)

	removed, err := f.store.Remove(context.Background(), f.cluster, sandboxUUID(97), 1)
	if err != nil {
		t.Fatalf("remove of an absent row failed: %v", err)
	}
	if removed {
		t.Fatal("an absent row cannot have been deleted")
	}
}

// TestRemoveRefusesAGenerationTheRowHasMovedPast is the window the caller-side
// guard could not close.
//
// The old shape was: read the row, decide the sandbox is not live elsewhere,
// delete it. A node returning from a partition holds a view from before it
// left, and takes exactly that path against a sandbox somebody else has since
// resumed. The delete matched, and the snapshot the other node is running from
// went with it — no error on either side.
func TestRemoveRefusesAGenerationTheRowHasMovedPast(t *testing.T) {
	f := newStoreFixture(t)
	id := sandboxUUID(99)

	f.seed(seedRow{
		sandboxID: id, state: "running", generation: 7,
		originNode: stNodeB, snapshotID: snapshotUUID(99),
	})

	removed, err := f.store.Remove(context.Background(), f.cluster, id, 6)
	if err != nil {
		t.Fatalf("a stale delete is a no-op, not an error: %v", err)
	}
	if removed {
		t.Fatal("a delete quoting a generation the row has moved past must match nothing")
	}
	if !f.raw(id).found {
		t.Fatal("a stale delete destroyed a row somebody else is holding")
	}

	// The same call with the row's actual generation does delete it, so the
	// refusal above is the condition working rather than the statement being
	// broken.
	removed, err = f.store.Remove(context.Background(), f.cluster, id, 7)
	if err != nil {
		t.Fatalf("remove failed: %v", err)
	}
	if !removed {
		t.Fatal("a delete quoting the row's own generation must match it")
	}
	if f.raw(id).found {
		t.Fatal("the row survived a delete that reported it matched")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Cluster scope, end to end
// ─────────────────────────────────────────────────────────────────────────────

// TestOneClusterCannotReachAnothersSandboxes sweeps every write.
//
// Two clusters pointed at one database is an ordinary accident: the DSN is just
// a connection string. Without the scope a node would claim another cluster's
// sandbox, fail to find its snapshot in its own repository, and then delete the
// other cluster's row as dangling.
func TestOneClusterCannotReachAnothersSandboxes(t *testing.T) {
	f := newStoreFixture(t)
	ctx := context.Background()
	id := sandboxUUID(98)

	f.seed(seedRow{
		sandboxID: id, cluster: f.other, state: "publishing", generation: 3, originNode: "node-z",
		leaseExpires: "now() + interval '1 hour'",
	})
	before := f.raw(id)

	if err := f.store.CompletePause(ctx, f.cluster, id, 3, snapshotUUID(98)); !errors.Is(err, ErrGenerationConflict) {
		t.Fatalf("complete_pause reached another cluster: %v", err)
	}
	if err := f.store.MarkLocalOnly(ctx, f.cluster, id, 3); !errors.Is(err, ErrGenerationConflict) {
		t.Fatalf("mark_local_only reached another cluster: %v", err)
	}
	if matched, err := f.store.ReleaseClaim(ctx, f.cluster, id, 3); err != nil || matched {
		t.Fatalf("release_claim reached another cluster: %v, %v", matched, err)
	}
	if outcome, err := f.store.MarkRunning(ctx, f.cluster, id, stNodeA, stNodeA, f.executionFor(id), nil); err != nil || outcome != MarkRunningUntracked {
		t.Fatalf("mark_running reached another cluster: %v, %v", outcome, err)
	}
	if renewed, err := f.store.RenewLease(ctx, f.cluster, "node-z", []HeldSandbox{{SandboxID: id}}); err != nil || renewed != 0 {
		t.Fatalf("renew_lease reached another cluster: %d, %v", renewed, err)
	}
	if _, found, err := f.store.Get(ctx, f.cluster, id); err != nil || found {
		t.Fatalf("get reached another cluster: %v, %v", found, err)
	}

	after := f.raw(id)
	if after.state != before.state || after.generation != before.generation || after.originNode != before.originNode {
		t.Fatalf("the other cluster's row changed:\nbefore %+v\nafter  %+v", before, after)
	}
	if after.snapshotID != nil {
		t.Fatalf("the other cluster's row was given a snapshot: %v", after.snapshotID)
	}
}

// TestARowWrittenBeforeTheLeaseColumnExistedCountsAsExpired covers the COALESCE
// half of the lease predicate.
//
// A NULL lease_expires_at is a row written by a build that did not have the
// column. Treating it as expired is the safe direction: a live holder refreshes
// it within one interval, a dead one never does — and the alternative leaves
// those rows permanently unclaimable by anyone but a node that may be gone.
func TestARowWrittenBeforeTheLeaseColumnExistedCountsAsExpired(t *testing.T) {
	f := newStoreFixture(t)
	id := sandboxUUID(110)

	f.seed(seedRow{sandboxID: id, state: "local_only", originNode: stNodeA, snapshotID: snapshotUUID(110)})
	// No lease column at all, and last written long ago.
	f.setLease(id, "now() - interval '1 hour'", "NULL")

	claim, err := f.store.ClaimForResume(context.Background(), f.cluster, id, stNodeB, f.nextExecutionFor(id))
	if err != nil {
		t.Fatalf("claim failed: %v", err)
	}
	if claim.Outcome != ClaimOutcomeClaimed {
		t.Fatalf("a row with no lease at all should read as expired, got %s", claim.Outcome)
	}
	if claim.PreviousState != StateLocalOnly {
		t.Fatalf("previous_state: got %q", claim.PreviousState)
	}
}

// TestAStateThisBuildDoesNotKnowIsRefusedRatherThanSkipped.
//
// The CHECK constraint pins five states, so a sixth means the table has moved
// on from this build. 🔴 Skipping the row would make it missing, and missing is
// what tells the node to delete the sandbox's local artifacts — so a row this
// build cannot read has to arrive as an error on every path that reads it.
func TestAStateThisBuildDoesNotKnowIsRefusedRatherThanSkipped(t *testing.T) {
	f := newStoreFixture(t)
	ctx := context.Background()
	good, unknown := sandboxUUID(111), sandboxUUID(112)

	f.beginPause(good, stNodeA)

	// The constraint is what stops this being writable, which is also what
	// makes it a future build's row rather than a corrupt one.
	if _, err := f.pool.Exec(ctx, "ALTER TABLE paused_sandboxes DROP CONSTRAINT paused_sandboxes_state_check"); err != nil {
		t.Fatalf("drop constraint failed: %v", err)
	}
	f.seed(seedRow{sandboxID: unknown, state: "hibernating", originNode: stNodeA, snapshotID: snapshotUUID(112)})

	if _, _, err := f.store.Get(ctx, f.cluster, unknown); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("get: expected ErrInvalidRecord, got %v", err)
	}
	rows, err := f.store.GetMany(ctx, f.cluster, []string{good, unknown})
	if !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("get_many: expected ErrInvalidRecord, got %v", err)
	}
	if rows.Entries != nil || rows.Covered != nil {
		t.Fatalf("get_many returned rows alongside its error: %+v", rows)
	}
	if _, err := f.store.ClaimForResume(ctx, f.cluster, unknown, stNodeB, f.nextExecutionFor(unknown)); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("claim: expected ErrInvalidRecord, got %v", err)
	}
}

// TestTheSchemaLockIsTheOneTheNodesTake.
//
// CREATE TABLE IF NOT EXISTS is not atomic against a concurrent creator: two
// writers booting together both pass the existence check and then collide in
// the system catalogs, which is fatal for whichever loses. During the
// changeover a node and this controller are exactly that pair, so a lock taken
// on a different key serialises this process against itself and nothing else.
func TestTheSchemaLockIsTheOneTheNodesTake(t *testing.T) {
	// The literal from PostgresPausedSandboxRegistry::ensure_schema.
	const nodeKey int64 = 0x0A6E_7653_4348_4D41

	if schemaLockKey != nodeKey {
		t.Fatalf("the schema advisory lock has drifted from the node's: %#x vs %#x", schemaLockKey, nodeKey)
	}
}

// TestTheSchemaLockIsReleased: a lock left held blocks every other writer's
// bootstrap until this process exits, which on a rolling update is a deadlock
// between the old pod and the new one.
func TestTheSchemaLockIsReleased(t *testing.T) {
	f := newStoreFixture(t)

	var held bool
	if err := f.pool.QueryRow(context.Background(),
		"SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND ((classid::bigint << 32) | objid::bigint) = $1)",
		schemaLockKey).Scan(&held); err != nil {
		t.Fatalf("read advisory locks failed: %v", err)
	}
	if held {
		t.Fatal("the schema advisory lock is still held after Migrate returned")
	}
}
