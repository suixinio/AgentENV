package catalog

import (
	"context"
	"crypto/rand"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ─────────────────────────────────────────────────────────────────────────────
// The parts that need no database.
// ─────────────────────────────────────────────────────────────────────────────

func TestEmbeddedMigrationsParse(t *testing.T) {
	a, err := newApplier(migrationsFS, migrationsDir)
	if err != nil {
		t.Fatalf("parse the embedded migrations: %v", err)
	}
	if len(a.migrations) < 2 {
		t.Fatalf("expected at least the two creation migrations, got %d", len(a.migrations))
	}
	for i, m := range a.migrations {
		if m.version != i+1 {
			t.Fatalf("migration %s has version %d at position %d: versions must be dense and ordered",
				m.name, m.version, i+1)
		}
	}

	latest, err := LatestVersion()
	if err != nil {
		t.Fatalf("LatestVersion: %v", err)
	}
	if want := a.migrations[len(a.migrations)-1].version; latest != want {
		t.Fatalf("LatestVersion returned %d, want %d", latest, want)
	}
}

func TestNewApplierRefusesUnusableSets(t *testing.T) {
	cases := []struct {
		name  string
		files map[string]*fstest.MapFile
		want  string
	}{
		{
			name: "duplicate version",
			files: map[string]*fstest.MapFile{
				"m/0001_a.sql": {Data: []byte("SELECT 1;")},
				"m/0001_b.sql": {Data: []byte("SELECT 1;")},
			},
			want: "would never run",
		},
		{
			name:  "no version prefix",
			files: map[string]*fstest.MapFile{"m/create.sql": {Data: []byte("SELECT 1;")}},
			want:  "not named",
		},
		{
			name:  "non numeric version",
			files: map[string]*fstest.MapFile{"m/abcd_x.sql": {Data: []byte("SELECT 1;")}},
			want:  "no numeric version",
		},
		{
			// 0 is what an empty ledger reads as, so a migration numbered 0
			// could never be told from one that has not run.
			name:  "version zero",
			files: map[string]*fstest.MapFile{"m/0000_x.sql": {Data: []byte("SELECT 1;")}},
			want:  "versions start at 1",
		},
		{
			name:  "nothing to apply",
			files: map[string]*fstest.MapFile{"m/README.md": {Data: []byte("not sql")}},
			want:  "no catalog migrations found",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := newApplier(fstest.MapFS(tc.files), "m")
			if err == nil {
				t.Fatal("expected a refusal, got none")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not mention %q", err, tc.want)
			}
		})
	}
}

func TestNoTransactionDirectiveOnlyCountsOnTheFirstLine(t *testing.T) {
	if !hasNoTransactionDirective(noTransactionDirective + "\nCREATE INDEX CONCURRENTLY x ON y (z);\n") {
		t.Fatal("a file opening with the directive was not recognised")
	}
	// Buried in the middle it is a comment about the directive, not the
	// directive: a file whose transaction behaviour depends on line 40 is a
	// file nobody reads correctly.
	if hasNoTransactionDirective("-- notes\n" + noTransactionDirective + "\nSELECT 1;\n") {
		t.Fatal("the directive was honoured away from the first line")
	}
	if hasNoTransactionDirective("CREATE TABLE t (id INT);\n") {
		t.Fatal("an ordinary file was treated as NO TRANSACTION")
	}
}

func TestSplitStatementsKeepsSemicolonsWhereTheyBelong(t *testing.T) {
	body := `
-- a comment with a ; in it
CREATE TABLE t (id INT);
CREATE FUNCTION f() RETURNS TRIGGER AS $$
BEGIN
    NEW.x := 'a;b';
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
INSERT INTO t (id) VALUES (1);
/* block ; comment */
SELECT 'it''s ok; really' AS "weird;name";
`
	statements, err := splitStatements(body)
	if err != nil {
		t.Fatalf("split: %v", err)
	}
	if len(statements) != 4 {
		for i, s := range statements {
			t.Logf("statement %d: %s", i+1, s)
		}
		t.Fatalf("expected 4 statements, got %d", len(statements))
	}
	if !strings.Contains(statements[1], "LANGUAGE plpgsql") {
		t.Fatalf("the function body was cut short: %q", statements[1])
	}
	if !strings.Contains(statements[1], "RETURN NEW;") {
		t.Fatalf("the function body lost its interior semicolons: %q", statements[1])
	}
	if !strings.Contains(statements[3], "weird;name") {
		t.Fatalf("a quoted identifier was split: %q", statements[3])
	}
}

func TestSplitStatementsRefusesUnterminatedText(t *testing.T) {
	for _, body := range []string{
		"SELECT 'unterminated",
		"SELECT $$unterminated",
		"/* unterminated",
	} {
		if _, err := splitStatements(body); err == nil {
			t.Fatalf("expected a refusal for %q", body)
		}
	}
}

func TestDollarTagReadsOpeners(t *testing.T) {
	cases := map[string]string{
		"$$body$$":       "$$",
		"$fn$body$fn$":   "$fn$",
		"$a1$body$a1$":   "$a1$",
		"$ not a tag":    "",
		"$1 is a param":  "",
		"$1$ is a param": "",
	}
	for input, want := range cases {
		got, ok := dollarTag(input)
		if want == "" {
			if ok {
				t.Fatalf("%q was read as the dollar tag %q", input, got)
			}
			continue
		}
		if !ok || got != want {
			t.Fatalf("%q gave (%q, %v), want %q", input, got, ok, want)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// The parts that need PostgreSQL.
// ─────────────────────────────────────────────────────────────────────────────

// requireTestDSN mirrors the registry suite's helper, including the escape
// hatch that turns a missing database into a failure.
//
// A skipped test reports as passing, so a runner that is supposed to have a
// database — CI, or a verification run — sets SCHEDULER_REGISTRY_TEST_REQUIRED
// and gets a loud failure instead of a file full of green no-ops.
func requireTestDSN(t *testing.T) string {
	t.Helper()

	dsn := os.Getenv("SCHEDULER_REGISTRY_TEST_DSN")
	if dsn == "" {
		if os.Getenv("SCHEDULER_REGISTRY_TEST_REQUIRED") != "" {
			t.Fatal("SCHEDULER_REGISTRY_TEST_REQUIRED is set but SCHEDULER_REGISTRY_TEST_DSN is not: these tests would have been skipped")
		}
		t.Skip("SCHEDULER_REGISTRY_TEST_DSN is not set; skipping the real PostgreSQL test")
	}
	return dsn
}

// newTestPool gives each test its own schema, so two of them can assert on an
// unfiltered read of the same table name without seeing each other's rows — and
// so the cleanup can drop everything by dropping one schema.
func newTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	dsn := requireTestDSN(t)
	ctx := context.Background()

	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect to the test database: %v", err)
	}
	defer func() { _ = admin.Close(ctx) }()

	schema := testSchemaName(t)
	quoted := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+quoted); err != nil {
		t.Fatalf("create the private schema %s: %v", schema, err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		conn, err := pgx.Connect(cleanup, dsn)
		if err != nil {
			t.Logf("reconnect to drop %s: %v", schema, err)
			return
		}
		defer func() { _ = conn.Close(cleanup) }()
		if _, err := conn.Exec(cleanup, "DROP SCHEMA "+quoted+" CASCADE"); err != nil {
			t.Logf("drop the private schema %s: %v", schema, err)
		}
	})

	pool, err := pgxpool.New(ctx, dsnInSchema(t, dsn, schema))
	if err != nil {
		t.Fatalf("open a pool against %s: %v", schema, err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func testSchemaName(t *testing.T) string {
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
	// Lower case throughout: search_path carries the name unquoted, and
	// PostgreSQL folds unquoted identifiers before resolving them, so a capital
	// creates one schema and looks for another.
	return strings.ToLower(fmt.Sprintf("cattest_%s_%x", string(cleaned), unique))
}

func dsnInSchema(t *testing.T, dsn, schema string) string {
	t.Helper()

	if !strings.HasPrefix(dsn, "postgres://") && !strings.HasPrefix(dsn, "postgresql://") {
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

func TestMigrateCreatesTheCatalogOnAnEmptyDatabase(t *testing.T) {
	pool := newTestPool(t)
	ctx := context.Background()

	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate an empty database: %v", err)
	}

	for _, relation := range ownedRelations {
		var found *string
		if err := pool.QueryRow(ctx, "SELECT to_regclass($1)::text", relation).Scan(&found); err != nil {
			t.Fatalf("look for %s: %v", relation, err)
		}
		if found == nil {
			t.Fatalf("%s was not created", relation)
		}
	}

	// The view, the two trigger functions, and every index the queries were
	// planned against. Named individually rather than counted: a count passes
	// when the wrong index exists.
	for _, indexName := range []string{
		"snapshots_list_idx",
		"snapshots_source_sandbox_idx",
		"snapshots_unpublished_idx",
		"templates_cluster_live_idx",
		"builds_one_active_per_template",
		"builds_active_idx",
		"aliases_one_per_snapshot",
	} {
		var exists bool
		if err := pool.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM pg_indexes
			                 WHERE schemaname = current_schema() AND indexname = $1)`,
			indexName).Scan(&exists); err != nil {
			t.Fatalf("look for index %s: %v", indexName, err)
		}
		if !exists {
			t.Fatalf("index %s was not created", indexName)
		}
	}

	var viewExists bool
	if err := pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_views
		                 WHERE schemaname = current_schema() AND viewname = 'active_templates')`,
	).Scan(&viewExists); err != nil {
		t.Fatalf("look for the active_templates view: %v", err)
	}
	if !viewExists {
		t.Fatal("active_templates was not created")
	}

	latest, err := LatestVersion()
	if err != nil {
		t.Fatalf("LatestVersion: %v", err)
	}
	var recorded int
	if err := pool.QueryRow(ctx, "SELECT max(version) FROM catalog_schema_migrations").Scan(&recorded); err != nil {
		t.Fatalf("read the ledger: %v", err)
	}
	if recorded != latest {
		t.Fatalf("the ledger records version %d, this build carries %d", recorded, latest)
	}
}

func TestMigrateTwiceChangesNothing(t *testing.T) {
	pool := newTestPool(t)
	ctx := context.Background()

	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("first migration: %v", err)
	}

	type ledgerRow struct {
		version   int
		appliedAt int64
	}
	read := func() []ledgerRow {
		rows, err := pool.Query(ctx, "SELECT version, applied_at_ms FROM catalog_schema_migrations ORDER BY version")
		if err != nil {
			t.Fatalf("read the ledger: %v", err)
		}
		defer rows.Close()
		var out []ledgerRow
		for rows.Next() {
			var r ledgerRow
			if err := rows.Scan(&r.version, &r.appliedAt); err != nil {
				t.Fatalf("scan the ledger: %v", err)
			}
			out = append(out, r)
		}
		return out
	}

	before := read()
	if len(before) == 0 {
		t.Fatal("the first migration recorded nothing")
	}

	// A row that survives the second run untouched is the evidence: an applier
	// that re-ran a file would either fail on a CREATE or restamp applied_at_ms.
	time.Sleep(2 * time.Millisecond)
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("second migration: %v", err)
	}

	after := read()
	if len(after) != len(before) {
		t.Fatalf("the ledger grew from %d rows to %d", len(before), len(after))
	}
	for i := range before {
		if before[i] != after[i] {
			t.Fatalf("ledger row %d changed: %+v -> %+v", i, before[i], after[i])
		}
	}

	// And the schema is still usable, not just present: a table that got
	// re-created empty would pass every existence check above.
	seedSnapshot(t, pool, snapshotSeed{status: "waiting"})
}

func TestMigrateRefusesATableItDidNotCreate(t *testing.T) {
	pool := newTestPool(t)
	ctx := context.Background()

	// Somebody else's `snapshots`. Every statement in 0001 is IF NOT EXISTS, so
	// without the preflight this migrates "successfully" and leaves a schema
	// that rejects every insert.
	if _, err := pool.Exec(ctx, "CREATE TABLE snapshots (id TEXT PRIMARY KEY)"); err != nil {
		t.Fatalf("plant a foreign table: %v", err)
	}

	err := Migrate(ctx, pool)
	if err == nil {
		t.Fatal("expected a refusal, got success")
	}
	// 🔴 Asserted on the preflight's own words, not on the table name.
	//
	// "snapshots" appears in almost every failure this migration can produce —
	// the first file is called 0001_snapshots.sql, so a run that skipped the
	// preflight entirely, proceeded, and died on `column "deleted_at_ms" does
	// not exist` names it too. A test that only looked for the table name
	// passed with the preflight deleted, which is the one thing it was there
	// to hold down.
	if !strings.Contains(err.Error(), "不是本进程建的") {
		t.Fatalf("this is not the preflight's refusal — the migration got past it and failed later: %v", err)
	}
	if !strings.Contains(err.Error(), "snapshots") {
		t.Fatalf("the refusal does not name the table: %v", err)
	}
	// And it stopped before writing anything: a preflight that refused after
	// applying 0001 would leave the ledger claiming a version.
	var recorded int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM catalog_schema_migrations`).Scan(&recorded); err != nil {
		t.Fatalf("read the ledger: %v", err)
	}
	if recorded != 0 {
		t.Fatalf("the refused run recorded %d versions", recorded)
	}

	// The control: the same call against a database whose ledger has an entry
	// proceeds, because then the table is ours by construction.
	if _, err := pool.Exec(ctx, "DROP TABLE snapshots"); err != nil {
		t.Fatalf("remove the foreign table: %v", err)
	}
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate after removing the foreign table: %v", err)
	}
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("a second run must not trip the preflight: %v", err)
	}
}

// TestMigrateRefusesALedgerWhoseTablesAreGone is the rollback foot-gun, run.
//
// 🔴 Rolling 2a back means dropping four tables. The ledger is a fifth table
// nobody thinks of, because it is not one of the tables the change was about —
// so the plausible half-done rollback is `DROP TABLE aliases, builds,
// templates, snapshots` with catalog_schema_migrations left behind. What makes
// that dangerous is that it is silent: the next upgrade is told both versions
// are applied, skips both files, returns success, and opens the write gate over
// a database with no catalog in it. Every RPC then fails on a missing relation,
// forever, and re-running the upgrade cannot fix it because re-running is what
// is already happening.
//
// So the refusal has to come from the applier, and it has to name the command
// that finishes the job.
func TestMigrateRefusesALedgerWhoseTablesAreGone(t *testing.T) {
	pool := newTestPool(t)
	ctx := context.Background()

	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("first migration: %v", err)
	}

	// The half-done rollback, exactly as somebody would type it.
	if _, err := pool.Exec(ctx,
		`DROP TABLE IF EXISTS aliases, builds, templates, snapshots CASCADE`); err != nil {
		t.Fatalf("drop the catalog tables: %v", err)
	}

	err := Migrate(ctx, pool)
	if err == nil {
		t.Fatal("a ledger with no tables under it migrated 'successfully': " +
			"that is the silent no-op this check exists to stop")
	}
	// The message has to carry all three: which versions the ledger claims,
	// which relations are missing, and the command that fixes it. An operator
	// reading only the first two would repeat the same half-rollback.
	for _, want := range []string{"1", "2", "snapshots", "aliases", "catalog_schema_migrations"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("the refusal does not mention %q: %v", want, err)
		}
	}
	if !strings.Contains(err.Error(), rollbackCommand) {
		t.Fatalf("the refusal does not quote the rollback command: %v", err)
	}

	// 🔴 And it is a refusal, not a repair: the tables are still absent. A
	// re-apply would be right for this cause and wrong for the two that look
	// identical from here — a partial restore, and a search_path pointing
	// somewhere other than where the tables live.
	for _, relation := range []string{"snapshots", "aliases"} {
		var found *string
		if err := pool.QueryRow(ctx, "SELECT to_regclass($1)::text", relation).Scan(&found); err != nil {
			t.Fatalf("look for %s: %v", relation, err)
		}
		if found != nil {
			t.Fatalf("%s was re-created: this check refuses, it does not repair", relation)
		}
	}

	// The control, and the documented way out: finish the rollback, and the
	// same call migrates from scratch.
	if _, err := pool.Exec(ctx, rollbackCommand); err != nil {
		t.Fatalf("run the rollback the error quotes: %v", err)
	}
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate after a completed rollback: %v", err)
	}
	seedSnapshot(t, pool, snapshotSeed{status: "waiting"})
}

// TestMigrateAcceptsALedgerFromANewerBuild is the other side of the check
// above: a version this build has never heard of names relations it cannot
// know, so it is not evidence of anything and must not be refused.
func TestMigrateAcceptsALedgerFromANewerBuild(t *testing.T) {
	pool := newTestPool(t)
	ctx := context.Background()

	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("first migration: %v", err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO catalog_schema_migrations (version, applied_at_ms) VALUES (99, 1)`); err != nil {
		t.Fatalf("record a version from the future: %v", err)
	}

	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("a rolled-back build refused to start against a newer ledger: %v", err)
	}
}

// TestNoTransactionDirectiveIsWhatMakesConcurrentIndexesWork carries its own
// control: the same file without the directive must fail, or the directive is
// not what the first half proved.
func TestNoTransactionDirectiveIsWhatMakesConcurrentIndexesWork(t *testing.T) {
	base := "CREATE TABLE IF NOT EXISTS notx_probe (id INTEGER PRIMARY KEY, tag TEXT);\n"
	concurrent := "CREATE INDEX CONCURRENTLY IF NOT EXISTS notx_probe_id_idx ON notx_probe (id);\n" +
		"CREATE INDEX CONCURRENTLY IF NOT EXISTS notx_probe_tag_idx ON notx_probe (tag);\n"

	t.Run("with the directive", func(t *testing.T) {
		pool := newTestPool(t)
		files := fstest.MapFS{
			"m/0001_base.sql":       {Data: []byte(base)},
			"m/0002_concurrent.sql": {Data: []byte(noTransactionDirective + "\n" + concurrent)},
		}
		a, err := newApplier(files, "m")
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		if err := a.run(context.Background(), pool); err != nil {
			t.Fatalf("apply: %v", err)
		}

		// Both statements ran, so the file really was split rather than sent
		// whole and stopped at the first semicolon.
		var count int
		if err := pool.QueryRow(context.Background(),
			`SELECT count(*) FROM pg_indexes
			  WHERE schemaname = current_schema()
			    AND indexname IN ('notx_probe_id_idx', 'notx_probe_tag_idx')`,
		).Scan(&count); err != nil {
			t.Fatalf("count indexes: %v", err)
		}
		if count != 2 {
			t.Fatalf("expected both concurrent indexes, got %d", count)
		}
	})

	t.Run("without the directive", func(t *testing.T) {
		pool := newTestPool(t)
		files := fstest.MapFS{
			"m/0001_base.sql":       {Data: []byte(base)},
			"m/0002_concurrent.sql": {Data: []byte(concurrent)},
		}
		a, err := newApplier(files, "m")
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		err = a.run(context.Background(), pool)
		if err == nil {
			t.Fatal("expected CREATE INDEX CONCURRENTLY inside a transaction to fail")
		}
		if !strings.Contains(err.Error(), "transaction") {
			t.Fatalf("failed for some other reason: %v", err)
		}
	})
}
