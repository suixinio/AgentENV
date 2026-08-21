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

	// 🔴 disk_size_mib is the one resource a row may open without, because a
	// v3 template has no disk size until its build has produced a rootfs; the
	// node writes that as 0. See migration 0003 — the rule is gated on `ready`
	// instead, exactly the way snapshots_ready_is_committed above is.
	t.Run("a row that is not ready may have no disk size yet", func(t *testing.T) {
		if _, err := pool.Exec(ctx, `
INSERT INTO snapshots (id, cluster_id, source_kind, cpu_count, memory_mib, disk_size_mib,
                       status, status_group, created_at_ms, updated_at_ms)
VALUES ($1, '11111111-1111-1111-1111-111111111111', 'template', 1, 128, 0,
        'waiting', 'pending', 1, 1)`, newUUID(t)); err != nil {
			t.Fatalf("a template that does not know its disk size yet was refused: %v", err)
		}
	})

	t.Run("ready with no disk size is refused", func(t *testing.T) {
		id := newUUID(t)
		if _, err := pool.Exec(ctx, `
INSERT INTO snapshots (id, cluster_id, source_kind, cpu_count, memory_mib, disk_size_mib,
                       status, status_group, created_at_ms, updated_at_ms)
VALUES ($1, '11111111-1111-1111-1111-111111111111', 'template', 1, 128, 0,
        'building', 'in_progress', 1, 1)`, id); err != nil {
			t.Fatalf("open the row: %v", err)
		}
		// The flip commit_snapshot makes, with nothing filling the size in.
		_, err := pool.Exec(ctx, `
UPDATE snapshots
   SET status = 'ready', committed_payload = $2, committed_schema = 1
 WHERE id = $1`, id, []byte(`{"a":1}`))
		refuses(t, "snapshots_ready_has_disk_size", err)
	})

	// The column is a signed INTEGER and the store casts a uint32 into it, so
	// below zero is a value that arrived corrupted rather than one that means
	// "not known yet". Dropping the old positivity CHECK must not have opened
	// that door.
	t.Run("a negative disk size is refused", func(t *testing.T) {
		_, err := pool.Exec(ctx, `
INSERT INTO snapshots (id, cluster_id, source_kind, cpu_count, memory_mib, disk_size_mib,
                       status, status_group, created_at_ms, updated_at_ms)
VALUES ($1, '11111111-1111-1111-1111-111111111111', 'template', 1, 128, -1,
        'waiting', 'pending', 1, 1)`, newUUID(t))
		refuses(t, "snapshots_disk_size_floor", err)
	})

	// And the column CHECK 0003 replaced is gone rather than merely shadowed:
	// while it is on the table nothing else matters, because it refuses the
	// insert before either rule above is consulted.
	t.Run("the column-level positivity check is gone", func(t *testing.T) {
		var leftover []string
		rows, err := pool.Query(ctx, `
SELECT c.conname
  FROM pg_constraint c
  JOIN pg_class t ON t.oid = c.conrelid
  JOIN pg_namespace n ON n.oid = t.relnamespace
 WHERE t.relname = 'snapshots' AND n.nspname = current_schema()
   AND c.contype = 'c'
   AND c.conname NOT IN ('snapshots_disk_size_floor', 'snapshots_ready_has_disk_size')
   AND pg_get_constraintdef(c.oid) LIKE '%disk_size_mib%'`)
		if err != nil {
			t.Fatalf("read the table's check constraints: %v", err)
		}
		defer rows.Close()
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				t.Fatalf("scan a constraint name: %v", err)
			}
			leftover = append(leftover, name)
		}
		if len(leftover) != 0 {
			t.Fatalf("disk_size_mib is still constrained by %v: 0003 dropped the wrong name", leftover)
		}
	})
}

// TestOriginColumnsStayRemovable pins the rules that make the origin block
// droppable in one migration file, checked against the catalogs rather than
// against the DDL text, so a later change that breaks one fails here rather
// than three phases from now.
//
// Five of the six are here. V5 — neither column ever appears in a WHERE clause
// — is a rule about the statements this package sends rather than about the
// schema, and nothing in the database can see it; it is pinned by
// TestPinIsTheOnlyPlaceTheOriginBlockDecidesAnything in pin_test.go.
func TestOriginColumnsStayRemovable(t *testing.T) {
	pool := migratedPool(t)
	ctx := context.Background()

	// V1, for indexes, and V3: the origin block appears in exactly one index,
	// that index is the dedicated partial one, and it is neither unique nor
	// primary — so dropping the columns drops one whole index and rebuilds
	// nothing.
	//
	// 🔴 The predicate is read as well as the key columns, and that is the
	// whole of this check rather than a refinement of it. `a.attnum = ANY
	// (ix.indkey)` covers key and INCLUDE columns; a partial index's predicate
	// lives in indpred and matches none of them. Our one index over the block
	// is `(cluster_id, origin_node_id) WHERE NOT published`, so `published`
	// contributed no rows at all — this block was testing origin_node_id and
	// nothing else, and `AND published` bolted onto any other index's predicate
	// passed it. Which is exactly the thing V3 exists to prevent: a partial
	// predicate is where a column gets tangled into an index without appearing
	// to be part of it, and DROP COLUMN takes the whole index with it.
	rows, err := pool.Query(ctx, `
SELECT i.relname,
       ix.indisunique,
       ix.indisprimary,
       COALESCE(pg_get_expr(ix.indpred, ix.indrelid), ''),
       COALESCE((SELECT string_agg(a.attname, ',')
                   FROM pg_attribute a
                  WHERE a.attrelid = t.oid
                    AND a.attnum = ANY (ix.indkey)), '')
  FROM pg_index ix
  JOIN pg_class  i ON i.oid = ix.indexrelid
  JOIN pg_class  t ON t.oid = ix.indrelid
  JOIN pg_namespace n ON n.oid = t.relnamespace
 WHERE t.relname = 'snapshots'
   AND n.nspname = current_schema()`)
	if err != nil {
		t.Fatalf("inspect indexes: %v", err)
	}
	defer rows.Close()

	var names []string
	scanned := 0
	for rows.Next() {
		var name, predicate, keyColumns string
		var unique, primary bool
		if err := rows.Scan(&name, &unique, &primary, &predicate, &keyColumns); err != nil {
			t.Fatalf("scan: %v", err)
		}
		scanned++

		var where []string
		for _, column := range originBlockColumns {
			if columnListHas(keyColumns, column) {
				where = append(where, "key column "+column)
			}
			if mentionsIdentifier(predicate, column) {
				where = append(where, "predicate column "+column)
			}
		}
		if len(where) == 0 {
			continue
		}
		if unique || primary {
			t.Fatalf("index %s over the origin block (%s) is unique or primary: dropping the columns would rebuild it",
				name, strings.Join(where, ", "))
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read indexes: %v", err)
	}
	// A query that returned nothing would pass every assertion above it.
	if scanned == 0 {
		t.Fatal("no indexes on snapshots came back: this check was inspecting nothing")
	}
	for _, name := range names {
		if name != "snapshots_unpublished_idx" {
			t.Fatalf("the origin block appears in %s as well as snapshots_unpublished_idx: "+
				"dropping the columns would have to rebuild that index too", name)
		}
	}
	if len(names) != 1 {
		t.Fatalf("the origin block is covered by %d indexes (%v), want exactly snapshots_unpublished_idx",
			len(names), names)
	}

	// V2: `published` is never merged into status_group. They answer different
	// questions — "did the capture finish" and "did the bytes reach shared
	// storage" — and folding them turns the drop into a backfill.
	//
	// The trigger that derives status_group is the only place the merge could
	// be written, so the assertion is that its body does not mention either
	// column. The behavioural half is next door: a failed publish is `ready`
	// and unpublished, which is the pair a merge would make impossible.
	var triggerBody string
	if err := pool.QueryRow(ctx, `
SELECT p.prosrc
  FROM pg_proc p
  JOIN pg_namespace n ON n.oid = p.pronamespace
 WHERE p.proname = 'catalog_status_group_trg' AND n.nspname = current_schema()`).Scan(&triggerBody); err != nil {
		t.Fatalf("read the status_group trigger: %v", err)
	}
	for _, column := range originBlockColumns {
		if mentionsIdentifier(triggerBody, column) {
			t.Fatalf("the status_group trigger reads %s:\n%s\n\n"+
				"status_group is derived from `status` alone. Folding the origin block into it "+
				"makes dropping those columns a backfill of every row rather than one DDL file.",
				column, triggerBody)
		}
	}
	// And the other way it could be merged: a generated column, whose
	// expression is a second place a dependency can hide.
	var generated string
	if err := pool.QueryRow(ctx, `
SELECT is_generated FROM information_schema.columns
 WHERE table_schema = current_schema() AND table_name = 'snapshots'
   AND column_name = 'status_group'`).Scan(&generated); err != nil {
		t.Fatalf("read status_group: %v", err)
	}
	if generated != "NEVER" {
		t.Fatalf("status_group became a generated column (%s): its expression is a dependency "+
			"the origin block can be pulled into without appearing in any index", generated)
	}

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

	// V1, for foreign keys: none touches either column, from either side.
	//
	// 🔴 Both sides, and the second half was missing. conkey is the referencing
	// side — an FK on `snapshots` whose column is one of these. confkey is the
	// referenced side: another table pointing *at* one of these columns, which
	// needs a unique index over it to exist at all and would therefore break
	// V1's first half too — but by way of an error message about a constraint
	// on a table this test never looks at.
	var fkCount int
	if err := pool.QueryRow(ctx, `
SELECT (SELECT count(*)
          FROM pg_constraint c
          JOIN pg_class t ON t.oid = c.conrelid
          JOIN pg_namespace n ON n.oid = t.relnamespace
          JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY (c.conkey)
         WHERE c.contype = 'f' AND t.relname = 'snapshots' AND n.nspname = current_schema()
           AND a.attname IN ('published', 'origin_node_id'))
     + (SELECT count(*)
          FROM pg_constraint c
          JOIN pg_class ft ON ft.oid = c.confrelid
          JOIN pg_namespace fn ON fn.oid = ft.relnamespace
          JOIN pg_attribute a ON a.attrelid = ft.oid AND a.attnum = ANY (c.confkey)
         WHERE c.contype = 'f' AND ft.relname = 'snapshots' AND fn.nspname = current_schema()
           AND a.attname IN ('published', 'origin_node_id'))`).Scan(&fkCount); err != nil {
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
	//
	// 🔴 Everything else, not one named index. The rehearsal used to re-check
	// `snapshots_list_idx` alone, which pinned the one index somebody happened
	// to name in 2026-08 and let a botched removal take any of the others. What
	// makes the rehearsal worth running is that it does not need to know which
	// index a future rule-breaker will have tangled the columns into: it takes
	// the whole inventory before the drop and requires all of it back after,
	// minus the two objects the drop is supposed to remove.
	seedSnapshot(t, pool, snapshotSeed{published: false, originNodeID: "node-a"})
	if _, err := pool.Exec(ctx, `DELETE FROM snapshots WHERE NOT published`); err != nil {
		t.Fatalf("clear unpublished rows: %v", err)
	}

	indexesBefore := snapshotIndexNames(t, ctx, pool)
	constraintsBefore := snapshotConstraintNames(t, ctx, pool)
	// A rehearsal over an empty inventory would pass whatever the drop did.
	if len(indexesBefore) < 3 || len(constraintsBefore) < 3 {
		t.Fatalf("only %d indexes and %d constraints to check: this rehearsal is inspecting almost nothing",
			len(indexesBefore), len(constraintsBefore))
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

	indexesAfter := snapshotIndexNames(t, ctx, pool)
	for name := range indexesBefore {
		if name == "snapshots_unpublished_idx" {
			continue
		}
		if !indexesAfter[name] {
			t.Fatalf("dropping the origin block took index %s with it", name)
		}
	}
	if indexesAfter["snapshots_unpublished_idx"] {
		t.Fatal("snapshots_unpublished_idx survived the drop: it is the one index that should have gone")
	}

	constraintsAfter := snapshotConstraintNames(t, ctx, pool)
	for name := range constraintsBefore {
		if name == "snapshots_origin_axis" {
			continue
		}
		if !constraintsAfter[name] {
			t.Fatalf("dropping the origin block took constraint %s with it", name)
		}
	}

	// And the table still writes.
	if _, err := pool.Exec(ctx, `
INSERT INTO snapshots (id, cluster_id, source_kind, cpu_count, memory_mib, disk_size_mib,
                       status, status_group, created_at_ms, updated_at_ms)
VALUES ($1, '11111111-1111-1111-1111-111111111111', 'template', 1, 128, 1024,
        'waiting', 'pending', 1, 1)`, newUUID(t)); err != nil {
		t.Fatalf("the table stopped accepting rows after the drop: %v", err)
	}
}

// originBlockColumns are the two columns the rules above keep untangled.
var originBlockColumns = []string{"published", "origin_node_id"}

// columnListHas answers whether a comma-separated column list contains a name.
// Compared whole, so `published` does not match `unpublished_at`.
func columnListHas(list, column string) bool {
	for _, name := range strings.Split(list, ",") {
		if name == column {
			return true
		}
	}
	return false
}

// mentionsIdentifier answers whether an expression names a column, as a word.
//
// pg_get_expr renders a partial index's predicate as SQL text — `(NOT
// published)` — so this is a text search over a normalised rendering rather
// than over anything anybody typed.
func mentionsIdentifier(expr, identifier string) bool {
	rest := expr
	for {
		idx := strings.Index(rest, identifier)
		if idx < 0 {
			return false
		}
		before := idx == 0 || !isSQLIdentifierByte(rest[idx-1])
		end := idx + len(identifier)
		after := end == len(rest) || !isSQLIdentifierByte(rest[end])
		if before && after {
			return true
		}
		rest = rest[idx+len(identifier):]
	}
}

func isSQLIdentifierByte(b byte) bool {
	return b == '_' || b == '$' ||
		(b >= '0' && b <= '9') ||
		(b >= 'A' && b <= 'Z') ||
		(b >= 'a' && b <= 'z')
}

func snapshotIndexNames(t *testing.T, ctx context.Context, pool *pgxpool.Pool) map[string]bool {
	t.Helper()
	return namesOf(t, ctx, pool, `
SELECT indexname FROM pg_indexes
 WHERE schemaname = current_schema() AND tablename = 'snapshots'`)
}

func snapshotConstraintNames(t *testing.T, ctx context.Context, pool *pgxpool.Pool) map[string]bool {
	t.Helper()
	return namesOf(t, ctx, pool, `
SELECT c.conname
  FROM pg_constraint c
  JOIN pg_class t ON t.oid = c.conrelid
  JOIN pg_namespace n ON n.oid = t.relnamespace
 WHERE t.relname = 'snapshots' AND n.nspname = current_schema()`)
}

func namesOf(t *testing.T, ctx context.Context, pool *pgxpool.Pool, query string) map[string]bool {
	t.Helper()

	rows, err := pool.Query(ctx, query)
	if err != nil {
		t.Fatalf("read names: %v", err)
	}
	defer rows.Close()

	out := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan a name: %v", err)
		}
		out[name] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read names: %v", err)
	}
	return out
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
