package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

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
	if err := Migrate(context.Background(), f.pool); err != nil {
		t.Fatalf("migrate failed: %v", err)
	}
	f.store = newStoreWithPool(f.pool, StoreConfig{LeaseTTL: stLeaseTTL, Logger: zap.NewNop()})
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

	return &storeFixture{t: t, pool: pool, cluster: stCluster, other: stOther}
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

	var claimedBy, snapshot any
	if row.claimedBy != "" {
		claimedBy = row.claimedBy
	}
	if row.snapshotID != "" {
		snapshot = row.snapshotID
	}

	sql := `INSERT INTO paused_sandboxes (
        sandbox_id, cluster_id, state, generation, origin_node_id, claimed_by_node_id,
        snapshot_id, metadata, paused_at, updated_at, lease_expires_at, sandbox_expires_at
    ) VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7::uuid, $8::jsonb, now(), now(), ` +
		lease + `, ` + expiry + `)`

	if _, err := f.pool.Exec(context.Background(), sql,
		row.sandboxID, row.cluster, row.state, row.generation, row.originNode,
		claimedBy, snapshot, row.metadata,
	); err != nil {
		f.t.Fatalf("seed %s failed: %v", row.sandboxID, err)
	}
}

// raw reads a row without going through the decoder, so a test can look at a
// row the decoder would refuse.
type rawRow struct {
	state         string
	generation    int64
	originNode    string
	claimedBy     *string
	snapshotID    *string
	pausedAt      time.Time
	updatedAt     time.Time
	leaseExpires  *time.Time
	sandboxExpiry *time.Time
	found         bool
}

func (f *storeFixture) raw(sandboxID string) rawRow {
	f.t.Helper()

	var row rawRow
	err := f.pool.QueryRow(context.Background(), `
        SELECT state, generation, origin_node_id, claimed_by_node_id, snapshot_id::text,
               paused_at, updated_at, lease_expires_at, sandbox_expires_at
          FROM paused_sandboxes WHERE sandbox_id = $1::uuid`, sandboxID).
		Scan(&row.state, &row.generation, &row.originNode, &row.claimedBy, &row.snapshotID,
			&row.pausedAt, &row.updatedAt, &row.leaseExpires, &row.sandboxExpiry)
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

// TestMigrationIsTheNodesScriptVerbatim is the guard the whole switchover rests
// on.
//
// Every node configured with the `postgres` backend runs its own copy of this
// script on every start, and two of its statements drop and re-add the state
// CHECK unconditionally. The moment the two copies disagree about the set of
// states — and one row exists carrying a state only this side knows — the next
// node to start cannot add its constraint back and never comes up, with the
// cause in a different process on a different machine.
func TestMigrationIsTheNodesScriptVerbatim(t *testing.T) {
	if SchemaDDL != schemaDDL {
		t.Fatalf("the controller's migration has drifted from the node's SCHEMA_DDL.\ncontroller:\n%s\nnode:\n%s", SchemaDDL, schemaDDL)
	}
}

// TestMigrateIsIdempotentOverATableTheNodeCreated is the switchover's hard
// gate, made executable.
//
// During the changeover both a node and this controller bootstrap the same
// table, and the node's script is unconditional about the state constraint:
// every `postgres`-backend node runs `DROP CONSTRAINT IF EXISTS` +
// `ADD CONSTRAINT` on every start. So this migration has to be a no-op over a
// table the node built — and, since the node will run its own script again
// afterwards, the shape this leaves behind has to be one the node's script
// still applies cleanly to.
//
// The table here is created from a copy of the node's DDL rather than from
// Migrate, which is the whole point: a paraphrase would pass against a table
// this side built and fail against a real one.
func TestMigrateIsIdempotentOverATableTheNodeCreated(t *testing.T) {
	f := newStoreFixtureWithoutMigration(t)
	ctx := context.Background()

	// 1. The node comes up first and builds the table.
	if _, err := f.pool.Exec(ctx, schemaDDL); err != nil {
		t.Fatalf("the node's own bootstrap failed: %v", err)
	}
	// 2. The controller migrates over it.
	if err := Migrate(ctx, f.pool); err != nil {
		t.Fatalf("migrating a table the node created failed: %v", err)
	}
	// 3. The node restarts and runs its script again. This is the direction
	//    that strands a machine: it fails on *that* node, whose configuration
	//    nobody changed, naming a constraint nobody there touched.
	if _, err := f.pool.Exec(ctx, schemaDDL); err != nil {
		t.Fatalf("the node could not start after the controller migrated: %v", err)
	}
	// 4. And once more each way, because a rollout is not two steps.
	if err := Migrate(ctx, f.pool); err != nil {
		t.Fatalf("second controller migration failed: %v", err)
	}
	if _, err := f.pool.Exec(ctx, schemaDDL); err != nil {
		t.Fatalf("second node bootstrap failed: %v", err)
	}

	// The table still works for both sides afterwards.
	store := newStoreWithPool(f.pool, StoreConfig{LeaseTTL: stLeaseTTL, Logger: zap.NewNop()})
	if _, err := store.BeginPause(ctx, BeginPauseInput{
		ClusterID:    f.cluster,
		SandboxID:    sandboxUUID(8),
		OriginNodeID: stNodeA,
		Metadata:     json.RawMessage(stMetadata),
	}); err != nil {
		t.Fatalf("the table is unusable after the round trip: %v", err)
	}
}

// TestATableTheControllerCreatedAcceptsTheNodesBootstrap is the same gate from
// the other side: the controller came up first on a fresh database.
func TestATableTheControllerCreatedAcceptsTheNodesBootstrap(t *testing.T) {
	f := newStoreFixture(t) // already migrated

	if _, err := f.pool.Exec(context.Background(), schemaDDL); err != nil {
		t.Fatalf("a node could not start against a table the controller created: %v", err)
	}
}

func TestMigrateIsIdempotent(t *testing.T) {
	f := newStoreFixture(t)

	// newStoreFixture already ran it once; a second run is what a restart does.
	if err := Migrate(context.Background(), f.pool); err != nil {
		t.Fatalf("second migrate failed: %v", err)
	}
	if err := Migrate(context.Background(), f.pool); err != nil {
		t.Fatalf("third migrate failed: %v", err)
	}
}

// TestMigrateAddsNoColumnOfItsOwn pins the column set to the node's.
//
// A column added here that the node does not know about is not neutral: a NOT
// NULL one without a default makes every insert the node still issues fail, and
// the node's insert names its columns explicitly, so it would never populate it
// anyway.
func TestMigrateAddsNoColumnOfItsOwn(t *testing.T) {
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
		"claimed_by_node_id", "cluster_id", "generation", "lease_expires_at",
		"metadata", "origin_node_id", "paused_at", "sandbox_expires_at",
		"sandbox_id", "snapshot_id", "state", "updated_at",
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("column set changed:\n got %v\nwant %v", got, want)
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
	if _, err := f.store.MarkRunning(ctx, f.cluster, id, stNodeA); err != nil {
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
	if len(rows) != 1 {
		t.Fatalf("expected exactly the one sandbox with a row, got %d", len(rows))
	}
	if _, ok := rows[present]; !ok {
		t.Fatal("the sandbox with a row is missing from the batch")
	}
	if _, ok := rows[absent]; ok {
		t.Fatal("a sandbox with no row appeared in the batch")
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
	if len(rows) != 0 {
		t.Fatalf("one cluster read another's row: %+v", rows)
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
	if rows != nil {
		t.Fatalf("a failed batch must return no map at all, got %+v", rows)
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
		if rows != nil {
			t.Fatalf("id %q: expected no map at all, got %+v", malformed, rows)
		}
	}
}

func TestABatchReadOfNothingAsksNothing(t *testing.T) {
	f := newStoreFixture(t)

	rows, err := f.store.GetMany(context.Background(), f.cluster, nil)
	if err != nil {
		t.Fatalf("get_many of nothing failed: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("expected an empty map, got %+v", rows)
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

	claim, err := f.store.ClaimForResume(ctx, f.cluster, id, stNodeB)
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

			claim, err := f.store.ClaimForResume(context.Background(), f.cluster, id, stNodeB)
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

			claim, err := f.store.ClaimForResume(context.Background(), f.cluster, id, stNodeB)
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

	claim, err := f.store.ClaimForResume(context.Background(), f.cluster, id, stNodeB)
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

	claim, err := f.store.ClaimForResume(context.Background(), f.cluster, id, stNodeB)
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

	claim, err := f.store.ClaimForResume(context.Background(), f.cluster, sandboxUUID(35), stNodeB)
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

	claim, err := f.store.ClaimForResume(context.Background(), f.cluster, id, stNodeB)
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

	claim, err := f.store.ClaimForResume(ctx, f.cluster, id, stNodeB)
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
	claim, err := f.store.ClaimForResume(ctx, f.cluster, id, stNodeB)
	if err != nil || claim.Outcome != ClaimOutcomeClaimed {
		t.Fatalf("claim failed: %v (%s)", err, claim.Outcome)
	}

	if err := f.store.ReleaseClaim(ctx, f.cluster, id, claim.Entry.Generation); err != nil {
		t.Fatalf("release_claim failed: %v", err)
	}
	row := f.raw(id)
	if row.state != "paused" || row.claimedBy != nil {
		t.Fatalf("a released claim goes back to paused with no claimer: %+v", row)
	}
	if row.generation != claim.Entry.Generation {
		t.Fatalf("release_claim must not bump the generation: %d -> %d", claim.Entry.Generation, row.generation)
	}
}

// TestAReleaseThatMatchesNothingIsSilent is the one conditional write whose
// no-op is a success, and it is the node's behaviour.
//
// A release that matches nothing means somebody else already moved the row on,
// which is the outcome the release was trying to produce. Reporting it would
// give its caller — an error path that is already unwinding a failed resume —
// a second failure it has no way to act on.
func TestAReleaseThatMatchesNothingIsSilent(t *testing.T) {
	f := newStoreFixture(t)
	ctx := context.Background()
	id := sandboxUUID(41)

	f.seed(seedRow{sandboxID: id, state: "paused", generation: 9, originNode: stNodeA, snapshotID: snapshotUUID(41)})

	if err := f.store.ReleaseClaim(ctx, f.cluster, id, 3); err != nil {
		t.Fatalf("a release that matches nothing must not be an error, got %v", err)
	}
	if err := f.store.ReleaseClaim(ctx, f.cluster, sandboxUUID(42), 1); err != nil {
		t.Fatalf("releasing a sandbox with no row must not be an error, got %v", err)
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

	tracked, err := f.store.MarkRunning(context.Background(), f.cluster, id, stNodeA)
	if err != nil {
		t.Fatalf("mark_running failed: %v", err)
	}
	if tracked {
		t.Fatal("the cluster does not track this sandbox, and said it did")
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

	tracked, err := f.store.MarkRunning(context.Background(), f.cluster, id, "node-c")
	if err != nil {
		t.Fatalf("mark_running failed: %v", err)
	}
	if tracked {
		t.Fatal("a node claimed a sandbox another node holds the claim on")
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

	tracked, err := f.store.MarkRunning(ctx, f.cluster, id, stNodeB)
	if err != nil {
		t.Fatalf("mark_running failed: %v", err)
	}
	if !tracked {
		t.Fatal("the claimer must be able to mark its own claim running")
	}
	row := f.raw(id)
	if row.state != "running" || row.originNode != stNodeB || row.claimedBy != nil {
		t.Fatalf("unexpected row: %+v", row)
	}
	if row.generation != 5 {
		t.Fatalf("mark_running bumps the generation, got %d", row.generation)
	}
}

// TestMarkRunningPropagatesARowItCannotDecode keeps `false` meaning one thing.
//
// False tells the caller the registry has no say over this sandbox, and its
// reconciliation is built on that. A row that exists but cannot be decoded is
// not that, so it has to arrive as an error.
func TestMarkRunningPropagatesARowItCannotDecode(t *testing.T) {
	f := newStoreFixture(t)
	id := sandboxUUID(46)

	// Claimed by somebody else so the guard refuses, and undecodable so the
	// re-read that follows has something to complain about.
	f.seed(seedRow{sandboxID: id, state: "paused", originNode: stNodeA, claimedBy: stNodeB})

	_, err := f.store.MarkRunning(context.Background(), f.cluster, id, "node-c")
	if !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("expected ErrInvalidRecord, got %v", err)
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

	f.seed(seedRow{sandboxID: id, cluster: f.other, state: "paused", originNode: "node-z", snapshotID: snapshotUUID(96)})

	if err := f.store.Remove(context.Background(), f.cluster, id); err != nil {
		t.Fatalf("remove failed: %v", err)
	}
	if !f.raw(id).found {
		t.Fatal("one cluster deleted another's row")
	}
}

func TestRemoveOfSomethingThatIsNotThereIsNotAnError(t *testing.T) {
	f := newStoreFixture(t)

	if err := f.store.Remove(context.Background(), f.cluster, sandboxUUID(97)); err != nil {
		t.Fatalf("remove of an absent row failed: %v", err)
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
	if err := f.store.ReleaseClaim(ctx, f.cluster, id, 3); err != nil {
		t.Fatalf("release_claim failed: %v", err)
	}
	if tracked, err := f.store.MarkRunning(ctx, f.cluster, id, stNodeA); err != nil || tracked {
		t.Fatalf("mark_running reached another cluster: %v, %v", tracked, err)
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

	claim, err := f.store.ClaimForResume(context.Background(), f.cluster, id, stNodeB)
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
	if rows != nil {
		t.Fatalf("get_many returned a map alongside its error: %+v", rows)
	}
	if _, err := f.store.ClaimForResume(ctx, f.cluster, unknown, stNodeB); !errors.Is(err, ErrInvalidRecord) {
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
