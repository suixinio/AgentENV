package catalog

import (
	"context"
	"crypto/rand"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// These tests assert the schema itself: the constraints are the contract the
// query layer will be written against, so each one is stated here as a row the
// database refuses rather than as a rule somebody has to remember.

func newUUID(t *testing.T) string {
	t.Helper()

	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatalf("generate a uuid: %v", err)
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func migratedPool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	pool := newTestPool(t)
	if err := Migrate(context.Background(), pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return pool
}

type snapshotSeed struct {
	id           string
	clusterID    string
	sourceKind   string
	sandboxID    any
	status       string
	published    any
	originNodeID any
	payload      any
	schemaVer    any
	buildError   any
	createdAtMs  int64
	updatedAtMs  any
}

// insertSnapshot writes one row, filling in whatever the caller left blank with
// a value that satisfies every constraint. Tests that are about a constraint
// set the one field they mean and leave the rest alone.
func insertSnapshot(ctx context.Context, pool *pgxpool.Pool, s snapshotSeed) (string, error) {
	if s.id == "" {
		var b [16]byte
		if _, err := rand.Read(b[:]); err != nil {
			return "", err
		}
		b[6] = (b[6] & 0x0f) | 0x40
		b[8] = (b[8] & 0x3f) | 0x80
		s.id = fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
	}
	if s.clusterID == "" {
		s.clusterID = "11111111-1111-1111-1111-111111111111"
	}
	if s.sourceKind == "" {
		s.sourceKind = "template"
	}
	if s.status == "" {
		s.status = "waiting"
	}
	if s.published == nil {
		s.published = true
	}
	if s.createdAtMs == 0 {
		s.createdAtMs = time.Now().UnixMilli()
	}

	_, err := pool.Exec(ctx, `
INSERT INTO snapshots (
    id, cluster_id, source_kind, source_sandbox_id,
    cpu_count, memory_mib, disk_size_mib,
    status, status_group,
    published, origin_node_id,
    created_at_ms, updated_at_ms,
    committed_payload, committed_schema, build_error
) VALUES (
    $1, $2, $3, $4,
    1, 128, 1024,
    $5, 'pending',
    $6, $7,
    $8, $9,
    $10, $11, $12
)`,
		s.id, s.clusterID, s.sourceKind, s.sandboxID,
		s.status,
		s.published, s.originNodeID,
		s.createdAtMs, s.updatedAtMs,
		s.payload, s.schemaVer, s.buildError)
	return s.id, err
}

func seedSnapshot(t *testing.T, pool *pgxpool.Pool, s snapshotSeed) string {
	t.Helper()

	id, err := insertSnapshot(context.Background(), pool, s)
	if err != nil {
		t.Fatalf("seed a snapshot row: %v", err)
	}
	return id
}

func refuses(t *testing.T, constraint string, err error) {
	t.Helper()

	if err == nil {
		t.Fatalf("expected %s to refuse the row, it was accepted", constraint)
	}
	if !strings.Contains(err.Error(), constraint) {
		t.Fatalf("refused by something other than %s: %v", constraint, err)
	}
}

func TestStatusGroupIsDerivedAndNotWritable(t *testing.T) {
	pool := migratedPool(t)
	ctx := context.Background()

	readyPayload := []byte(`{"payload":true}`)
	cases := []struct {
		status string
		group  string
	}{
		{"waiting", "pending"},
		{"building", "in_progress"},
		{"ready", "ready"},
		{"error", "failed"},
	}

	for _, tc := range cases {
		seed := snapshotSeed{status: tc.status}
		switch tc.status {
		case "ready":
			seed.payload = readyPayload
			seed.schemaVer = 1
		case "error":
			seed.buildError = `{"message":"nope","step":null}`
		}
		// The insert above always writes 'pending' into status_group. Anything
		// other than the mapped value here means the trigger did not run.
		id := seedSnapshot(t, pool, seed)

		var group string
		if err := pool.QueryRow(ctx, "SELECT status_group FROM snapshots WHERE id = $1", id).Scan(&group); err != nil {
			t.Fatalf("read status_group: %v", err)
		}
		if group != tc.group {
			t.Fatalf("status %q produced status_group %q, want %q", tc.status, group, tc.group)
		}
	}

	// And on update, including one that tries to set the derived column by hand.
	id := seedSnapshot(t, pool, snapshotSeed{status: "waiting"})
	if _, err := pool.Exec(ctx,
		"UPDATE snapshots SET status = 'building', status_group = 'ready' WHERE id = $1", id); err != nil {
		t.Fatalf("update: %v", err)
	}
	var group string
	if err := pool.QueryRow(ctx, "SELECT status_group FROM snapshots WHERE id = $1", id).Scan(&group); err != nil {
		t.Fatalf("read status_group: %v", err)
	}
	if group != "in_progress" {
		t.Fatalf("a hand-written status_group survived: %q", group)
	}
}

func TestUpdatedAtFollowsTheCallerWhenTheCallerStatesIt(t *testing.T) {
	pool := migratedPool(t)
	ctx := context.Background()

	// Stated on insert: kept exactly. This is what makes a row and its
	// object-store twin comparable field by field during the double-write phase.
	const stated int64 = 1_700_000_000_123
	id := seedSnapshot(t, pool, snapshotSeed{updatedAtMs: stated})
	var got int64
	if err := pool.QueryRow(ctx, "SELECT updated_at_ms FROM snapshots WHERE id = $1", id).Scan(&got); err != nil {
		t.Fatalf("read updated_at_ms: %v", err)
	}
	if got != stated {
		t.Fatalf("updated_at_ms was overwritten: %d, want %d", got, stated)
	}

	// Left out on insert: stamped, because the column is NOT NULL and a caller
	// that omits it still has to produce a row.
	other := seedSnapshot(t, pool, snapshotSeed{})
	if err := pool.QueryRow(ctx, "SELECT updated_at_ms FROM snapshots WHERE id = $1", other).Scan(&got); err != nil {
		t.Fatalf("read updated_at_ms: %v", err)
	}
	if got <= 0 {
		t.Fatalf("an omitted updated_at_ms was not stamped: %d", got)
	}

	// Not mentioned in an update: refreshed, so a row cannot silently keep a
	// timestamp from before the change.
	if _, err := pool.Exec(ctx, "UPDATE snapshots SET origin_node_id = 'node-a' WHERE id = $1", id); err != nil {
		t.Fatalf("update: %v", err)
	}
	if err := pool.QueryRow(ctx, "SELECT updated_at_ms FROM snapshots WHERE id = $1", id).Scan(&got); err != nil {
		t.Fatalf("read updated_at_ms: %v", err)
	}
	if got == stated {
		t.Fatal("an update that did not mention updated_at_ms left it stale")
	}

	// Mentioned in an update: kept.
	const restated int64 = 1_700_000_000_456
	if _, err := pool.Exec(ctx, "UPDATE snapshots SET updated_at_ms = $2 WHERE id = $1", id, restated); err != nil {
		t.Fatalf("update: %v", err)
	}
	if err := pool.QueryRow(ctx, "SELECT updated_at_ms FROM snapshots WHERE id = $1", id).Scan(&got); err != nil {
		t.Fatalf("read updated_at_ms: %v", err)
	}
	if got != restated {
		t.Fatalf("a stated updated_at_ms was overwritten: %d, want %d", got, restated)
	}
}

func TestSnapshotAxesRefuseIncoherentRows(t *testing.T) {
	pool := migratedPool(t)
	ctx := context.Background()

	t.Run("a template may not carry a sandbox id", func(t *testing.T) {
		_, err := insertSnapshot(ctx, pool, snapshotSeed{sourceKind: "template", sandboxID: "sbx-1"})
		refuses(t, "snapshots_source_axis", err)
	})

	t.Run("a sandbox row must carry one", func(t *testing.T) {
		_, err := insertSnapshot(ctx, pool, snapshotSeed{sourceKind: "sandbox"})
		refuses(t, "snapshots_source_axis", err)
	})

	t.Run("ready without a payload is refused", func(t *testing.T) {
		// 🔴 This is the constraint that stops a snapshot whose bytes are still
		// being written from being started: `ready` is exactly the state in
		// which a payload exists.
		_, err := insertSnapshot(ctx, pool, snapshotSeed{status: "ready"})
		refuses(t, "snapshots_ready_is_committed", err)
	})

	t.Run("a payload without a version is refused", func(t *testing.T) {
		_, err := insertSnapshot(ctx, pool, snapshotSeed{
			status: "ready", payload: []byte(`{"a":1}`),
		})
		refuses(t, "snapshots_committed_axis", err)
	})

	t.Run("error without a reason is refused", func(t *testing.T) {
		_, err := insertSnapshot(ctx, pool, snapshotSeed{status: "error"})
		refuses(t, "snapshots_error_axis", err)
	})

	t.Run("unpublished without an origin is refused", func(t *testing.T) {
		_, err := insertSnapshot(ctx, pool, snapshotSeed{published: false})
		refuses(t, "snapshots_origin_axis", err)
	})

	t.Run("published with an origin is a hint and is allowed", func(t *testing.T) {
		// A hint that may be empty and may be wrong; the axis constraint only
		// binds the unpublished direction.
		seedSnapshot(t, pool, snapshotSeed{published: true, originNodeID: "node-a"})
	})

	t.Run("a failed publish is ready, unpublished, and pinned", func(t *testing.T) {
		// The terminal state of a publish that never reached shared storage.
		// It is `ready` on purpose: the snapshot is complete and its origin can
		// start it, so calling it unfinished would tell the user it does not
		// exist.
		id := seedSnapshot(t, pool, snapshotSeed{
			status: "ready", payload: []byte(`{"a":1}`), schemaVer: 1,
			published: false, originNodeID: "node-a",
		})
		var group string
		if err := pool.QueryRow(ctx, "SELECT status_group FROM snapshots WHERE id = $1", id).Scan(&group); err != nil {
			t.Fatalf("read status_group: %v", err)
		}
		if group != "ready" {
			t.Fatalf("an unpublished snapshot fell out of the ready group: %q", group)
		}
	})

	t.Run("resources must be positive", func(t *testing.T) {
		_, err := pool.Exec(ctx, `
INSERT INTO snapshots (id, cluster_id, source_kind, cpu_count, memory_mib, disk_size_mib,
                       status, status_group, created_at_ms, updated_at_ms)
VALUES ($1, '11111111-1111-1111-1111-111111111111', 'template', 0, 128, 1024,
        'waiting', 'pending', 1, 1)`, newUUID(t))
		refuses(t, "cpu_count", err)
	})
}

// TestOriginColumnsStayRemovable pins the six rules that make the origin block
// droppable in one migration file. Each assertion below is one of them, checked
// against the catalogs rather than against the DDL text, so a later change that
// breaks one fails here rather than three phases from now.
func TestOriginColumnsStayRemovable(t *testing.T) {
	pool := migratedPool(t)
	ctx := context.Background()

	// V1: not in any primary key, unique constraint or foreign key.
	// V3: in exactly one index, and that index is the dedicated partial one.
	rows, err := pool.Query(ctx, `
SELECT i.relname, ix.indisunique, ix.indisprimary
  FROM pg_index ix
  JOIN pg_class  i ON i.oid = ix.indexrelid
  JOIN pg_class  t ON t.oid = ix.indrelid
  JOIN pg_namespace n ON n.oid = t.relnamespace
  JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY (ix.indkey)
 WHERE t.relname = 'snapshots'
   AND n.nspname = current_schema()
   AND a.attname IN ('published', 'origin_node_id')`)
	if err != nil {
		t.Fatalf("inspect indexes: %v", err)
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		var unique, primary bool
		if err := rows.Scan(&name, &unique, &primary); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if unique || primary {
			t.Fatalf("index %s over the origin block is unique or primary: dropping the columns would rebuild it", name)
		}
		names = append(names, name)
	}
	for _, name := range names {
		if name != "snapshots_unpublished_idx" {
			t.Fatalf("the origin block appears in %s as well as snapshots_unpublished_idx: "+
				"dropping the columns would have to rebuild that index too", name)
		}
	}
	if len(names) == 0 {
		t.Fatal("snapshots_unpublished_idx does not cover the origin block")
	}

	// The key columns only: `published` appears in that index's predicate, and
	// origin_node_id in its key, so exactly one index name may come back.

	// V4: the constraint tying them together is named, so the drop script can
	// use DROP CONSTRAINT IF EXISTS and stay idempotent.
	var named bool
	if err := pool.QueryRow(ctx, `
SELECT EXISTS (
  SELECT 1 FROM pg_constraint c
    JOIN pg_class t ON t.oid = c.conrelid
    JOIN pg_namespace n ON n.oid = t.relnamespace
   WHERE t.relname = 'snapshots' AND n.nspname = current_schema()
     AND c.conname = 'snapshots_origin_axis')`).Scan(&named); err != nil {
		t.Fatalf("look for the named constraint: %v", err)
	}
	if !named {
		t.Fatal("snapshots_origin_axis is not there under that name")
	}

	// No foreign key touches either column, from either side.
	var fkCount int
	if err := pool.QueryRow(ctx, `
SELECT count(*)
  FROM pg_constraint c
  JOIN pg_class t ON t.oid = c.conrelid
  JOIN pg_namespace n ON n.oid = t.relnamespace
  JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY (c.conkey)
 WHERE c.contype = 'f' AND t.relname = 'snapshots' AND n.nspname = current_schema()
   AND a.attname IN ('published', 'origin_node_id')`).Scan(&fkCount); err != nil {
		t.Fatalf("inspect foreign keys: %v", err)
	}
	if fkCount != 0 {
		t.Fatalf("%d foreign key columns touch the origin block", fkCount)
	}

	// V6: origin_node_id stays nullable. A later SET NOT NULL has to be unwound
	// before the column can go, and the intermediate state rejects writes.
	var nullable string
	if err := pool.QueryRow(ctx, `
SELECT is_nullable FROM information_schema.columns
 WHERE table_schema = current_schema() AND table_name = 'snapshots'
   AND column_name = 'origin_node_id'`).Scan(&nullable); err != nil {
		t.Fatalf("read the column: %v", err)
	}
	if nullable != "YES" {
		t.Fatal("origin_node_id became NOT NULL")
	}

	// And the whole point: the drop really is one file. Run it here, on a
	// database that has the schema, and check nothing else needed touching.
	seedSnapshot(t, pool, snapshotSeed{published: false, originNodeID: "node-a"})
	if _, err := pool.Exec(ctx, `DELETE FROM snapshots WHERE NOT published`); err != nil {
		t.Fatalf("clear unpublished rows: %v", err)
	}
	for _, statement := range []string{
		`DROP INDEX IF EXISTS snapshots_unpublished_idx`,
		`ALTER TABLE snapshots DROP CONSTRAINT IF EXISTS snapshots_origin_axis`,
		`ALTER TABLE snapshots DROP COLUMN IF EXISTS origin_node_id`,
		`ALTER TABLE snapshots DROP COLUMN IF EXISTS published`,
	} {
		if _, err := pool.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	// The listing index — the one thing a botched removal would take with it —
	// is still there, and the table still writes.
	var listIdx bool
	if err := pool.QueryRow(ctx, `
SELECT EXISTS (SELECT 1 FROM pg_indexes
                WHERE schemaname = current_schema() AND indexname = 'snapshots_list_idx')`).Scan(&listIdx); err != nil {
		t.Fatalf("look for snapshots_list_idx: %v", err)
	}
	if !listIdx {
		t.Fatal("dropping the origin block took snapshots_list_idx with it")
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO snapshots (id, cluster_id, source_kind, cpu_count, memory_mib, disk_size_mib,
                       status, status_group, created_at_ms, updated_at_ms)
VALUES ($1, '11111111-1111-1111-1111-111111111111', 'template', 1, 128, 1024,
        'waiting', 'pending', 1, 1)`, newUUID(t)); err != nil {
		t.Fatalf("the table stopped accepting rows after the drop: %v", err)
	}
}

func TestBuildsHoldOneLiveBuildPerTemplate(t *testing.T) {
	pool := migratedPool(t)
	ctx := context.Background()

	template := seedSnapshot(t, pool, snapshotSeed{status: "building"})
	now := time.Now().UnixMilli()

	insertBuild := func(id, status string, started, finished any) error {
		_, err := pool.Exec(ctx, `
INSERT INTO builds (id, template_id, cluster_id, status, status_group, node_id,
                    heartbeat_at_ms, created_at_ms, started_at_ms, finished_at_ms)
VALUES ($1, $2, '11111111-1111-1111-1111-111111111111', $3, 'pending', 'node-a',
        $4, $4, $5, $6)`, id, template, status, now, started, finished)
		return err
	}

	first := newUUID(t)
	if err := insertBuild(first, "building", now, nil); err != nil {
		t.Fatalf("admit the first build: %v", err)
	}

	// 🔴 The exclusion is the index, not a check in the caller: two concurrent
	// requests both reading `waiting` and both starting a build VM is what this
	// closes.
	err := insertBuild(newUUID(t), "building", now, nil)
	refuses(t, "builds_one_active_per_template", err)

	// The control, and the reason heartbeat_at_ms and a reaper ship with this
	// index: once the stuck build leaves the live group, the template is free
	// again. Without the reaper that transition never happens and the template
	// is blocked forever.
	if _, err := pool.Exec(ctx, `
UPDATE builds SET status = 'error', finished_at_ms = $2,
                  error_reason = '{"message":"build heartbeat lapsed","step":null}'::jsonb
 WHERE id = $1`, first, now); err != nil {
		t.Fatalf("reap the first build: %v", err)
	}
	if err := insertBuild(newUUID(t), "building", now, nil); err != nil {
		t.Fatalf("a template whose stuck build was reaped is still blocked: %v", err)
	}
}

func TestBuildTimestampAxes(t *testing.T) {
	pool := migratedPool(t)
	ctx := context.Background()

	template := seedSnapshot(t, pool, snapshotSeed{status: "waiting"})
	now := time.Now().UnixMilli()

	insertBuild := func(status string, started, finished any) error {
		_, err := pool.Exec(ctx, `
INSERT INTO builds (id, template_id, cluster_id, status, status_group,
                    created_at_ms, started_at_ms, finished_at_ms, error_reason)
VALUES ($1, $2, '11111111-1111-1111-1111-111111111111', $3, 'pending',
        $4, $5, $6, NULL)`, newUUID(t), template, status, now, started, finished)
		return err
	}

	refuses(t, "builds_started_axis", insertBuild("waiting", now, nil))
	refuses(t, "builds_started_axis", insertBuild("building", nil, nil))
	refuses(t, "builds_finished_axis", insertBuild("building", now, now))
	refuses(t, "builds_finished_axis", insertBuild("error", now, nil))

	if err := insertBuild("waiting", nil, nil); err != nil {
		t.Fatalf("a queued build was refused: %v", err)
	}
}

func TestAliasesAreUniqueBothWays(t *testing.T) {
	pool := migratedPool(t)
	ctx := context.Background()

	const cluster = "11111111-1111-1111-1111-111111111111"
	first := seedSnapshot(t, pool, snapshotSeed{})
	second := seedSnapshot(t, pool, snapshotSeed{})
	now := time.Now().UnixMilli()

	bind := func(alias, snapshot string) error {
		_, err := pool.Exec(ctx,
			"INSERT INTO aliases (cluster_id, alias, snapshot_id, created_at_ms) VALUES ($1, $2, $3, $4)",
			cluster, alias, snapshot, now)
		return err
	}

	if err := bind("prod", first); err != nil {
		t.Fatalf("bind an alias: %v", err)
	}

	// One alias, one snapshot: this is the conflict the API reports, and it
	// replaces ninety lines of read-modify-write-reread.
	refuses(t, "aliases_pkey", bind("prod", second))

	// One snapshot, one alias: SnapshotRecord.alias is an Option, and a second
	// row here would duplicate this snapshot inside a listing page.
	refuses(t, "aliases_one_per_snapshot", bind("staging", first))

	// Another cluster is another scope.
	if _, err := pool.Exec(ctx,
		"INSERT INTO aliases (cluster_id, alias, snapshot_id, created_at_ms) VALUES ($1, $2, $3, $4)",
		"22222222-2222-2222-2222-222222222222", "prod", second, now); err != nil {
		t.Fatalf("the same alias in another cluster was refused: %v", err)
	}

	// A hard delete takes the alias with it, so "an alias pointing at a row
	// that is not there" is not a state this schema can be in. The catalog's
	// own delete is soft and has to remove the alias itself; this is the
	// backstop for whatever eventually removes the row for real.
	if _, err := pool.Exec(ctx, "DELETE FROM snapshots WHERE id = $1", first); err != nil {
		t.Fatalf("hard delete: %v", err)
	}
	var remaining int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM aliases WHERE snapshot_id = $1", first).Scan(&remaining); err != nil {
		t.Fatalf("count aliases: %v", err)
	}
	if remaining != 0 {
		t.Fatalf("%d aliases outlived their snapshot", remaining)
	}
}

func TestActiveTemplatesViewHidesTheDeletionColumn(t *testing.T) {
	pool := migratedPool(t)
	ctx := context.Background()

	const cluster = "11111111-1111-1111-1111-111111111111"
	live := seedSnapshot(t, pool, snapshotSeed{})
	gone := seedSnapshot(t, pool, snapshotSeed{})
	now := time.Now().UnixMilli()

	for id, deleted := range map[string]any{live: nil, gone: now} {
		if _, err := pool.Exec(ctx,
			"INSERT INTO templates (id, cluster_id, created_at_ms, updated_at_ms, deleted_at_ms) VALUES ($1, $2, $3, $3, $4)",
			id, cluster, now, deleted); err != nil {
			t.Fatalf("seed a template: %v", err)
		}
	}

	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM active_templates").Scan(&count); err != nil {
		t.Fatalf("read the view: %v", err)
	}
	if count != 1 {
		t.Fatalf("the view returned %d rows, want 1", count)
	}

	// The column is not on the view, so a caller reading through it cannot
	// forget the filter — and cannot write one that reads deleted rows.
	if _, err := pool.Exec(ctx, "SELECT deleted_at_ms FROM active_templates"); err == nil {
		t.Fatal("active_templates exposes deleted_at_ms")
	}
}
