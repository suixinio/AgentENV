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

// ownedRelations are the relations migration 0001 and 0002 create. The
// preflight refuses to run when one of these already exists and this database
// has no record of ever having created it.
var ownedRelations = []string{"snapshots", "templates", "builds", "aliases"}

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
		var found *string
		// to_regclass resolves through search_path and answers NULL rather than
		// raising when the name is free, so this is one round trip per name and
		// no error handling for the ordinary case.
		if err := conn.QueryRow(ctx, "SELECT to_regclass($1)::text", relation).Scan(&found); err != nil {
			return fmt.Errorf("inspect the catalog schema before migrating: %w", err)
		}
		if found != nil {
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
