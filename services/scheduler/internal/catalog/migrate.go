// Package catalog owns the snapshot catalog's schema: the `snapshots`,
// `templates`, `builds` and `aliases` tables, and the versioned applier that
// creates them.
//
// 🔴 Why this does not reuse the mechanism next door. `paused_sandboxes` is
// migrated by a single Go string constant of CREATE TABLE IF NOT EXISTS and
// ALTER … ADD COLUMN IF NOT EXISTS, applied in one multi-statement Exec under
// an advisory lock, with no version table at all (registry/migrate.go). That is
// the right shape for that table: a few hundred rows of live coordination state
// which can be dropped and rebuilt, and whose runbook says exactly that.
//
// The catalog is the opposite kind of data. It is the record of what snapshots
// exist, it cannot be reconstructed from anywhere else, and it is the most
// expensive thing the system holds. Four operations it will need are not
// expressible in the other mechanism:
//
//   - CREATE INDEX CONCURRENTLY, which cannot run inside a transaction, while a
//     multi-statement Exec is wrapped in one implicit transaction by the server;
//   - a column changing type or constraint, which is add-NOT-VALID, VALIDATE,
//     SET NOT NULL — three steps that have to survive being interrupted between
//     any two of them;
//   - a backfill in batches, which needs a COMMIT inside a loop;
//   - undoing a change, which needs a version number to undo *to*.
//
// So the catalog gets a version table and one file per change, and
// `paused_sandboxes` keeps what it has. Two mechanisms, because the two tables
// are two different kinds of thing — not because one is newer.
//
// 🔴 What is deliberately not adopted is a migration library. This module's ten
// direct dependencies are all libraries; it has no build-time tooling and no
// `go tool` step, and adding goose would change the build chain and the CI
// image for four files that will not move for months. The two conventions worth
// having are borrowed instead: a version table, and a per-file escape hatch for
// statements that cannot run in a transaction. If catalog changes ever become
// frequent — more than about one a month — this judgement should be revisited
// and goose is the right answer then.
//
// # Rolling it back
//
// There is no down migration, and for this phase there does not need to be: a
// rollback is dropping what was created, which is one command. It is written
// once, as `rollbackCommand` below, and quoted verbatim by verifyApplied's
// refusal, so that the process tells an operator the whole of it at the moment
// they need it:
//
//	DROP TABLE IF EXISTS aliases, builds, templates, snapshots CASCADE;
//	DROP TABLE IF EXISTS catalog_schema_migrations;
//
// 🔴 Both lines, always. The second is the one that gets forgotten — it is not
// one of the tables the change was about — and forgetting it is silent: a
// later upgrade is told every version is applied, creates nothing, opens the
// write gate, and leaves every catalog RPC failing on a missing relation with
// no way forward. verifyApplied refuses that state at startup rather than
// letting it become a schema nobody can repair by re-running anything.
//
// This does not touch `paused_sandboxes`, which is a different mechanism with
// a different runbook — see the note above.
package catalog

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// migrationsDir is the directory inside migrationsFS holding the files.
const migrationsDir = "migrations"

// noTransactionDirective marks a file that must not be wrapped in a
// transaction: CREATE INDEX CONCURRENTLY, and batched backfills that commit as
// they go. It has to be on the first line, so that reading the top of a file
// tells you how it runs.
//
// 🔴 A file marked this way is applied statement by statement and its version
// is recorded afterwards, outside any transaction — so an interruption can
// leave it half applied and unrecorded, and the next run starts it again from
// the top. Every statement in such a file must therefore be written to tolerate
// that: IF EXISTS, IF NOT EXISTS, or otherwise idempotent. That is the price of
// the escape hatch and it is why the default is the other way round.
const noTransactionDirective = "-- +aenv NO TRANSACTION"

// versionTableDDL creates the ledger. Idempotent, and applied before the lock's
// work begins so that a first run and a hundredth look the same.
const versionTableDDL = `
CREATE TABLE IF NOT EXISTS catalog_schema_migrations (
    version       INTEGER PRIMARY KEY,
    applied_at_ms BIGINT  NOT NULL
)`

// schemaLockKey is the advisory lock registry/migrate.go takes.
//
// 🔴 The same key on purpose, not a second one. Two schedulers rolling over
// each other must not migrate concurrently, and that is true across both
// schemas — but the stronger reason is local: both migrations run from the same
// goroutine in the same process, one after the other. Two keys would create an
// ordering between two locks that a future caller could take the other way
// round, which is a deadlock that only appears under a rollover. One key cannot
// be ordered wrongly.
const schemaLockKey int64 = 0x0A6E_7653_4348_4D41

// unlockTimeout bounds the advisory unlock, which runs on a context detached
// from the caller's.
const unlockTimeout = 5 * time.Second

// relationsByVersion is what each migration file creates, keyed by its version.
//
// It is read by both halves of the schema self-check, which are complementary:
// preflight refuses a relation that exists with an empty ledger, and
// verifyApplied refuses a ledger entry whose relations are gone. Between them,
// "the ledger and the database agree" is checked in both directions before any
// DDL runs.
//
// 🔴 When a later migration drops one of these, move it out of this map in the
// same file — otherwise verifyApplied refuses every start after that migration
// applies, on a database that is perfectly correct.
//
// 🔴 A migration that creates nothing still gets an entry, empty. An *absent*
// version is how verifyApplied recognises a ledger written by a build newer
// than this one — it skips it, because it cannot know what those files created
// — and a version this build ships must never look like that. Leaving 3 out
// would work today only because it happens to own no relations; it would be a
// version this build carries and cannot describe, which is the state the map
// exists to make impossible. TestEveryMigrationDeclaresItsRelations holds it
// down.
var relationsByVersion = map[int][]string{
	1: {"snapshots"},
	2: {"templates", "builds", "aliases", "active_templates"},
	3: {}, // moves a CHECK from the column to `ready`; creates no relation
}

// ownedRelations is relationsByVersion flattened in version order, which is
// also creation order — so a message listing them reads the way the files run.
var ownedRelations = flattenRelations()

func flattenRelations() []string {
	versions := make([]int, 0, len(relationsByVersion))
	for version := range relationsByVersion {
		versions = append(versions, version)
	}
	sort.Ints(versions)

	out := make([]string, 0, len(relationsByVersion))
	for _, version := range versions {
		out = append(out, relationsByVersion[version]...)
	}
	return out
}

// migration is one file.
type migration struct {
	version       int
	name          string
	body          string
	noTransaction bool
}

// applier holds a parsed, ordered set of migrations.
type applier struct {
	migrations []migration
}

// newApplier reads and validates the migrations in dir.
//
// Validation is strict on the two mistakes that are invisible at runtime: a
// duplicate version, where one of the two files would silently never run, and a
// version of zero, which the ledger cannot distinguish from "nothing applied".
func newApplier(fsys fs.FS, dir string) (*applier, error) {
	entries, err := fs.ReadDir(fsys, dir)
	if err != nil {
		return nil, fmt.Errorf("read catalog migrations: %w", err)
	}

	migrations := make([]migration, 0, len(entries))
	seen := make(map[int]string, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}

		version, err := parseVersion(entry.Name())
		if err != nil {
			return nil, err
		}
		if previous, duplicate := seen[version]; duplicate {
			return nil, fmt.Errorf(
				"catalog migrations %s and %s share version %d: one of them would never run",
				previous, entry.Name(), version)
		}
		seen[version] = entry.Name()

		raw, err := fs.ReadFile(fsys, path.Join(dir, entry.Name()))
		if err != nil {
			return nil, fmt.Errorf("read catalog migration %s: %w", entry.Name(), err)
		}
		body := string(raw)

		migrations = append(migrations, migration{
			version:       version,
			name:          entry.Name(),
			body:          body,
			noTransaction: hasNoTransactionDirective(body),
		})
	}

	if len(migrations) == 0 {
		return nil, errors.New("no catalog migrations found: the applier would report success having done nothing")
	}

	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].version < migrations[j].version
	})

	return &applier{migrations: migrations}, nil
}

// parseVersion reads the leading NNNN_ of a migration file name.
func parseVersion(name string) (int, error) {
	prefix, _, found := strings.Cut(name, "_")
	if !found {
		return 0, fmt.Errorf("catalog migration %s is not named <version>_<description>.sql", name)
	}
	version, err := strconv.Atoi(prefix)
	if err != nil {
		return 0, fmt.Errorf("catalog migration %s has no numeric version: %w", name, err)
	}
	if version <= 0 {
		return 0, fmt.Errorf("catalog migration %s has version %d: versions start at 1, because 0 is what an empty ledger reads as", name, version)
	}
	return version, nil
}

// hasNoTransactionDirective reports whether the file's first line asks to be
// applied outside a transaction.
func hasNoTransactionDirective(body string) bool {
	firstLine, _, _ := strings.Cut(body, "\n")
	return strings.TrimSpace(firstLine) == noTransactionDirective
}

// LatestVersion is the highest version this build carries. Exposed so a health
// surface can report the schema it expects.
func LatestVersion() (int, error) {
	a, err := newApplier(migrationsFS, migrationsDir)
	if err != nil {
		return 0, err
	}
	return a.migrations[len(a.migrations)-1].version, nil
}

// Migrate brings the catalog schema to the shape this build expects.
//
// It follows registry.Migrate's three settled shapes rather than inventing new
// ones: the lock is taken on one pinned connection because advisory locks are
// session-scoped; the preflight runs inside the lock and before any DDL; and
// the unlock runs on a detached context, destroying the connection when even
// that fails, because returning a connection to the pool while it still holds a
// cluster-wide lock is a deadlock nobody can see.
//
// 🔴 Callers retry rather than exit. A catalog that cannot be migrated must not
// stop this process from routing traffic or serving bindings; the catalog RPCs
// answer UNAVAILABLE until this has succeeded, which is a refusal a caller can
// act on. An empty answer would not be.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	a, err := newApplier(migrationsFS, migrationsDir)
	if err != nil {
		return err
	}
	return a.run(ctx, pool)
}

func (a *applier) run(ctx context.Context, pool *pgxpool.Pool) error {
	if pool == nil {
		return errors.New("migrate snapshot catalog schema: no pool")
	}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection for catalog schema bootstrap: %w", err)
	}
	released := false
	defer func() {
		if !released {
			conn.Release()
		}
	}()

	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", schemaLockKey); err != nil {
		return fmt.Errorf("lock catalog schema: %w", err)
	}

	applyErr := a.apply(ctx, conn.Conn())

	// Released before the outcome is reported, on a context detached from the
	// caller's: the most likely reason to be here with a failure is that the
	// caller's context was cancelled, and an unlock skipped for that reason
	// leaves the lock held until the session ends.
	unlockCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), unlockTimeout)
	defer cancel()
	if _, err := conn.Exec(unlockCtx, "SELECT pg_advisory_unlock($1)", schemaLockKey); err != nil {
		hijacked := conn.Hijack()
		released = true
		_ = hijacked.Close(unlockCtx)
		if applyErr != nil {
			return fmt.Errorf("ensure catalog schema: %w (releasing the schema lock also failed: %v)", applyErr, err)
		}
		return fmt.Errorf("release catalog schema lock: %w", err)
	}

	if applyErr != nil {
		return fmt.Errorf("ensure catalog schema: %w", applyErr)
	}
	return nil
}

// apply runs every migration this database has not recorded.
func (a *applier) apply(ctx context.Context, conn *pgx.Conn) error {
	if _, err := conn.Exec(ctx, versionTableDDL); err != nil {
		return fmt.Errorf("ensure catalog migration ledger: %w", err)
	}

	applied, err := appliedVersions(ctx, conn)
	if err != nil {
		return err
	}

	if err := preflight(ctx, conn, applied); err != nil {
		return err
	}
	if err := verifyApplied(ctx, conn, applied); err != nil {
		return err
	}

	for _, m := range a.migrations {
		if applied[m.version] {
			continue
		}
		if err := applyOne(ctx, conn, m); err != nil {
			return err
		}
	}
	return nil
}

// appliedVersions reads the ledger.
func appliedVersions(ctx context.Context, conn *pgx.Conn) (map[int]bool, error) {
	rows, err := conn.Query(ctx, "SELECT version FROM catalog_schema_migrations")
	if err != nil {
		return nil, fmt.Errorf("read catalog migration ledger: %w", err)
	}
	defer rows.Close()

	applied := make(map[int]bool)
	for rows.Next() {
		var version int
		if err := rows.Scan(&version); err != nil {
			return nil, fmt.Errorf("read catalog migration ledger: %w", err)
		}
		applied[version] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read catalog migration ledger: %w", err)
	}
	return applied, nil
}

// preflight refuses to create a table that is already there and was not created
// by this ledger.
//
// 🔴 Why refusing beats proceeding. Every statement in 0001 and 0002 is written
// IF NOT EXISTS, so running against somebody else's `snapshots` table would
// succeed, silently, and leave a schema that looks migrated and rejects every
// insert. The failure would surface later as a column that does not exist, in a
// query, on a machine whose own configuration is unchanged — which is the shape
// of error registry/migrate.go's preflight exists to replace.
//
// It only fires on a database with an empty ledger. Once anything is recorded,
// these relations are ours by construction and their existence is expected.
func preflight(ctx context.Context, conn *pgx.Conn, applied map[int]bool) error {
	if len(applied) > 0 {
		return nil
	}

	existing := make([]string, 0, len(ownedRelations))
	for _, relation := range ownedRelations {
		found, err := relationExists(ctx, conn, relation)
		if err != nil {
			return err
		}
		if found {
			existing = append(existing, relation)
		}
	}
	if len(existing) == 0 {
		return nil
	}

	return fmt.Errorf(
		"目录表 %s 已经存在，但 catalog_schema_migrations 里没有任何记录 —— "+
			"这张表不是本进程建的。本 build 不会在别人的表上继续建（每条 DDL 都是 IF NOT EXISTS，"+
			"继续下去会得到一个看着已迁移、却拒绝每一次写入的 schema）。"+
			"按 runbook 处置：确认这些表的归属，dev/test 环境可 DROP TABLE 后重启本进程",
		strings.Join(existing, ", "))
}

// verifyApplied is preflight's other half: it refuses a ledger that claims work
// the database no longer has.
//
// 🔴 What it catches, and why nothing else can. Rolling 2a back means dropping
// the four tables — and the ledger is a fifth table nobody thinks of, because
// it is not one of the tables the change was about. Drop the four and leave it,
// and this applier is told every version is applied: `applied[m.version]` skips
// both files, apply() returns nil, the write gate opens, and every catalog RPC
// then fails on a relation that is not there — permanently, because the next
// start is told the same thing. The symptom is at the far end of the process
// from the cause, the process reports a clean startup, and re-running the
// upgrade is the one thing that cannot fix it.
//
// So the ledger is checked against the catalogs before any DDL runs. A rollback
// that depends on somebody remembering a fifth table name is not a rollback;
// this turns forgetting it into a refusal at startup, naming the command that
// finishes the job.
//
// 🔴 Refused rather than repaired. Re-applying the missing files would be right
// for the half-finished rollback and wrong for the two other ways to reach this
// state — a dump restored without its tables, and a search_path that resolves
// somewhere other than where the tables were created — where it would create a
// second copy of the catalog in the wrong schema and start serving from it.
//
// Versions this build does not know are skipped: that is a database a newer
// build migrated, and this one has no idea what those files created.
func verifyApplied(ctx context.Context, conn *pgx.Conn, applied map[int]bool) error {
	if len(applied) == 0 {
		return nil
	}

	versions := make([]int, 0, len(applied))
	for version := range applied {
		if _, known := relationsByVersion[version]; known {
			versions = append(versions, version)
		}
	}
	sort.Ints(versions)

	missing := make([]string, 0, len(ownedRelations))
	claimed := make([]string, 0, len(versions))
	for _, version := range versions {
		gone := false
		for _, relation := range relationsByVersion[version] {
			found, err := relationExists(ctx, conn, relation)
			if err != nil {
				return err
			}
			if !found {
				missing = append(missing, relation)
				gone = true
			}
		}
		if gone {
			claimed = append(claimed, strconv.Itoa(version))
		}
	}
	if len(missing) == 0 {
		return nil
	}

	return fmt.Errorf(
		"catalog_schema_migrations 记录了版本 %s 已应用，但它建的关系 %s 不在库里 —— "+
			"这两件事不可能同时为真。最常见的成因是回滚只删了表、没删版本表："+
			"那样下一次升级会「成功」但什么也不建，闸门照常打开，之后每一个目录 RPC 都会永久性地"+
			"栽在一张不存在的表上。本 build 拒绝在这种状态下继续（重建缺失的表在另外两种成因下是错的："+
			"dump 没恢复全、search_path 指到了别处 —— 那会在错误的 schema 里再建一份目录并开始服务）。"+
			"把回滚做完，再重启本进程：\n"+
			"    %s",
		strings.Join(claimed, ", "), strings.Join(missing, ", "), rollbackCommand)
}

// rollbackCommand is the whole of a 2a rollback, in one place, quoted by
// verifyApplied's refusal.
//
// 🔴 Not quoted by preflight, deliberately. That one fires when these names
// belong to somebody else, and this command would tell an operator to drop
// their tables.
//
// 🔴 The ledger is on this line for the same reason the check above exists: it
// is the table the four in front of it make people forget, and dropping the
// four without it is the failure mode with no symptom at the time and no way
// back afterwards. CASCADE is what takes `active_templates` and the foreign
// keys with the tables.
const rollbackCommand = "DROP TABLE IF EXISTS aliases, builds, templates, snapshots CASCADE; " +
	"DROP TABLE IF EXISTS catalog_schema_migrations;"

// relationExists asks whether a name resolves to a table or view in this
// session's search_path.
//
// to_regclass answers NULL rather than raising when the name is free, so this
// is one round trip per name and no error handling for the ordinary case.
func relationExists(ctx context.Context, conn *pgx.Conn, relation string) (bool, error) {
	var found *string
	if err := conn.QueryRow(ctx, "SELECT to_regclass($1)::text", relation).Scan(&found); err != nil {
		return false, fmt.Errorf("inspect the catalog schema: %w", err)
	}
	return found != nil, nil
}

// applyOne runs one migration and records it.
func applyOne(ctx context.Context, conn *pgx.Conn, m migration) error {
	if m.noTransaction {
		return applyOutsideTransaction(ctx, conn, m)
	}
	return applyInTransaction(ctx, conn, m)
}

// applyInTransaction is the default: the file's statements and the ledger entry
// commit together, so a version is recorded exactly when its change is durable.
func applyInTransaction(ctx context.Context, conn *pgx.Conn, m migration) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin catalog migration %s: %w", m.name, err)
	}
	defer func() {
		// Detached, for the same reason the advisory unlock is: the usual way
		// to reach here is a cancelled context, and a rollback that inherits it
		// does not happen.
		rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), unlockTimeout)
		defer cancel()
		_ = tx.Rollback(rollbackCtx)
	}()

	// One Exec for the whole file. With no arguments pgx sends this over the
	// simple protocol, which accepts multiple statements; the explicit
	// transaction around it is what makes the ledger entry atomic with them.
	if _, err := tx.Exec(ctx, m.body); err != nil {
		return fmt.Errorf("apply catalog migration %s: %w", m.name, err)
	}
	if err := recordVersion(ctx, tx, m); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit catalog migration %s: %w", m.name, err)
	}
	return nil
}

// applyOutsideTransaction runs a NO TRANSACTION file one statement at a time.
//
// 🔴 One at a time, not one Exec. A multi-statement simple query is wrapped by
// the server in a single implicit transaction, so sending the file whole would
// fail CREATE INDEX CONCURRENTLY with "cannot run inside a transaction block" —
// the exact statement the directive exists to allow.
func applyOutsideTransaction(ctx context.Context, conn *pgx.Conn, m migration) error {
	statements, err := splitStatements(m.body)
	if err != nil {
		return fmt.Errorf("split catalog migration %s: %w", m.name, err)
	}
	for i, statement := range statements {
		if _, err := conn.Exec(ctx, statement); err != nil {
			return fmt.Errorf("apply catalog migration %s (statement %d): %w", m.name, i+1, err)
		}
	}
	return recordVersion(ctx, conn, m)
}

// execer is the part of pgx.Tx and pgx.Conn recordVersion needs.
type execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

func recordVersion(ctx context.Context, db execer, m migration) error {
	_, err := db.Exec(ctx,
		`INSERT INTO catalog_schema_migrations (version, applied_at_ms) VALUES ($1, $2)
		 ON CONFLICT (version) DO NOTHING`,
		m.version, time.Now().UnixMilli())
	if err != nil {
		return fmt.Errorf("record catalog migration %s: %w", m.name, err)
	}
	return nil
}

// splitStatements cuts a SQL file into top-level statements on semicolons.
//
// It is not a SQL parser and does not need to be. It needs to know the four
// places a semicolon does not end a statement — inside a single-quoted string,
// inside a quoted identifier, inside a dollar-quoted body, and inside a comment
// — because a function body written with $$ … $$ contains semicolons and
// splitting on them produces fragments that do not parse.
func splitStatements(body string) ([]string, error) {
	var (
		statements []string
		current    strings.Builder
		i          int
	)
	flush := func() {
		statement := strings.TrimSpace(current.String())
		if statement != "" {
			statements = append(statements, statement)
		}
		current.Reset()
	}

	for i < len(body) {
		switch {
		case strings.HasPrefix(body[i:], "--"):
			end := strings.IndexByte(body[i:], '\n')
			if end < 0 {
				i = len(body)
				continue
			}
			current.WriteString(body[i : i+end+1])
			i += end + 1

		case strings.HasPrefix(body[i:], "/*"):
			end := strings.Index(body[i+2:], "*/")
			if end < 0 {
				return nil, errors.New("unterminated block comment")
			}
			current.WriteString(body[i : i+2+end+2])
			i += 2 + end + 2

		case body[i] == '\'' || body[i] == '"':
			quote := body[i]
			j := i + 1
			for j < len(body) {
				if body[j] == quote {
					// A doubled quote is an escaped one, not the end.
					if j+1 < len(body) && body[j+1] == quote {
						j += 2
						continue
					}
					break
				}
				j++
			}
			if j >= len(body) {
				return nil, fmt.Errorf("unterminated %c-quoted literal", quote)
			}
			current.WriteString(body[i : j+1])
			i = j + 1

		case body[i] == '$':
			tag, ok := dollarTag(body[i:])
			if !ok {
				current.WriteByte(body[i])
				i++
				continue
			}
			end := strings.Index(body[i+len(tag):], tag)
			if end < 0 {
				return nil, fmt.Errorf("unterminated dollar-quoted body %s", tag)
			}
			stop := i + len(tag) + end + len(tag)
			current.WriteString(body[i:stop])
			i = stop

		case body[i] == ';':
			flush()
			i++

		default:
			current.WriteByte(body[i])
			i++
		}
	}
	flush()

	if len(statements) == 0 {
		return nil, errors.New("no statements found")
	}
	return statements, nil
}

// dollarTag reads a dollar-quote opener — $$ or $tag$ — from the head of s.
func dollarTag(s string) (string, bool) {
	if len(s) < 2 || s[0] != '$' {
		return "", false
	}
	for i := 1; i < len(s); i++ {
		c := s[i]
		if c == '$' {
			return s[:i+1], true
		}
		isTagChar := c == '_' ||
			(c >= 'a' && c <= 'z') ||
			(c >= 'A' && c <= 'Z') ||
			(i > 1 && c >= '0' && c <= '9')
		if !isTagChar {
			return "", false
		}
	}
	return "", false
}
