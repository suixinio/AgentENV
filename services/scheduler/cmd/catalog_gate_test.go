package main

import (
	"context"
	"errors"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pausedregistry "agentenv/services/scheduler/internal/registry"
	"agentenv/services/shared/config"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

// TestACatalogMigrationFailureHoldsThePausedRegistryShut pins what a failure in
// the catalog's schema actually costs, because the coupling is easy to state
// wrongly and was.
//
// 🔴 The gate is shared, and shared in one direction more than people expect.
// openRegistryWriteSurface runs the registry's migration, then the catalog's,
// and only then grace.Enter. So a catalog migration that fails never reaches
// Enter, the phase stays cold, and Grace.Require refuses — which means every
// paused-registry *write* answers UNAVAILABLE: begin_pause, complete_pause,
// mark_local_only, claim_for_resume. That is phase 1's live functionality, held
// shut by a schema this process had never applied before.
//
// The design is deliberate and this test does not argue with it: refusing is
// the only honest answer from a process whose catalog tables may not exist, and
// an empty listing reads as "this cluster has no snapshots", which a caller
// acts on by deciding a snapshot is gone. What the test fixes is that nothing
// said so. The comment above migrateCatalog claimed a failure "leaves routing,
// discovery and bindings alone" — true, and only half of it: those three do
// keep working, and the paused registry does not.
//
// So both halves are asserted here, and so is the recovery: the loop retries,
// and the gate opens by itself once the schema can be applied.
func TestACatalogMigrationFailureHoldsThePausedRegistryShut(t *testing.T) {
	pool := catalogGateTestPool(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 🔴 Somebody else's `snapshots`, which is what the catalog's preflight
	// refuses — a real failure of the real migration rather than a stub that
	// returns an error, so the coupling is tested through the code that would
	// actually produce it.
	if _, err := pool.Exec(ctx, "CREATE TABLE snapshots (id TEXT PRIMARY KEY)"); err != nil {
		t.Fatalf("plant a foreign table: %v", err)
	}

	store := &gateTestStore{pool: pool}
	extender := &gateTestExtender{}
	grace := pausedregistry.NewGrace(time.Minute, zap.NewNop())
	cfg := config.Config{}
	cfg.Scheduler.Registry.ClusterID = "11111111-1111-1111-1111-111111111111"

	go openRegistryWriteSurface(ctx, zap.NewNop(), cfg, store, grace, extender)

	// Wait until the loop has finished one whole attempt, so what follows is an
	// assertion about a settled state rather than a race with a goroutine.
	//
	// Either of two things ends an attempt: it backs off and migrates again,
	// which is what a refused catalog schema should produce, or it reaches
	// grace.Enter, which is what a broken coupling produces. Waiting for either
	// means the wrong one is reported by the assertion that names it rather
	// than by this loop timing out.
	waitFor(t, "the surface to finish its first attempt", func() bool {
		return store.migrations.Load() > 1 || extender.calls.Load() > 0
	})

	// 🔴 The gate did not open, and every registry write is refused with it.
	if err := grace.Require(); !errors.Is(err, pausedregistry.ErrNotReady) {
		t.Fatalf("Require() = %v, want ErrNotReady: a catalog schema that could not be applied "+
			"must hold the paused registry's writes shut, not let them through onto a half-built database", err)
	}
	if grace.Ready() {
		t.Fatal("the write surface reports ready with the catalog migration failing")
	}
	if phase, _, _ := grace.Observation(); phase != pausedregistry.PhaseCold {
		t.Fatalf("phase = %v, want cold", phase)
	}
	// And the restart pass never ran: Enter is downstream of both migrations,
	// so a lease extended here would mean the gate opened on a schema that is
	// not there.
	if extender.calls.Load() != 0 {
		t.Fatalf("the restart pass ran %d times with the catalog migration failing", extender.calls.Load())
	}

	// It kept trying, rather than giving up or taking the process down. That is
	// the other half of the design: this process routes traffic, discovers
	// nodes and serves bindings, all of which work with the catalog's database
	// on fire.
	if store.migrations.Load() < 2 {
		t.Fatalf("the surface attempted the migration %d times: a refused schema has to be retried, "+
			"or the cluster needs a restart to recover from a database that has already come back",
			store.migrations.Load())
	}

	// The recovery, unassisted: remove the obstruction and the same loop opens
	// the gate on its own.
	if _, err := pool.Exec(ctx, "DROP TABLE snapshots"); err != nil {
		t.Fatalf("remove the foreign table: %v", err)
	}
	waitFor(t, "the write surface to open", grace.Ready)
	if err := grace.Require(); err != nil {
		t.Fatalf("Require() = %v after the schema became applicable", err)
	}
	if extender.calls.Load() == 0 {
		t.Fatal("the gate opened without the restart pass running")
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// gateTestStore is a paused-registry store that migrates successfully and hands
// out a pool, which is all openRegistryWriteSurface asks of one.
//
// The interface is embedded rather than implemented: nothing else on it is
// reachable from the path under test, and a nil call panics loudly if that ever
// stops being true.
type gateTestStore struct {
	pausedregistry.Store

	pool       *pgxpool.Pool
	migrations atomic.Int64
}

func (s *gateTestStore) Migrate(context.Context) error {
	s.migrations.Add(1)
	return nil
}

func (s *gateTestStore) Pool() *pgxpool.Pool { return s.pool }

type gateTestExtender struct{ calls atomic.Int64 }

func (e *gateTestExtender) ExtendLeases(context.Context, string, time.Duration) (time.Duration, int64, error) {
	e.calls.Add(1)
	return 0, 0, nil
}

// catalogGateTestPool gives this test its own schema on the shared test
// database, mirroring the helper the registry and catalog suites use — including
// the escape hatch that turns a missing database into a failure for a runner
// that is supposed to have one.
func catalogGateTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	dsn := os.Getenv("SCHEDULER_REGISTRY_TEST_DSN")
	if dsn == "" {
		if os.Getenv("SCHEDULER_REGISTRY_TEST_REQUIRED") != "" {
			t.Fatal("SCHEDULER_REGISTRY_TEST_REQUIRED is set but SCHEDULER_REGISTRY_TEST_DSN is not: this test would have been skipped")
		}
		t.Skip("SCHEDULER_REGISTRY_TEST_DSN is not set; skipping the real PostgreSQL test")
	}

	ctx := context.Background()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect to the test database: %v", err)
	}
	defer func() { _ = admin.Close(ctx) }()

	schema := "cmd_" + strings.ToLower(strings.NewReplacer("/", "_", " ", "_").Replace(t.Name()))
	if len(schema) > 40 {
		schema = schema[:40]
	}
	quoted := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, "DROP SCHEMA IF EXISTS "+quoted+" CASCADE"); err != nil {
		t.Fatalf("clear the private schema %s: %v", schema, err)
	}
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

	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse the test DSN: %v", err)
	}
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()

	pool, err := pgxpool.New(ctx, parsed.String())
	if err != nil {
		t.Fatalf("open a pool against %s: %v", schema, err)
	}
	t.Cleanup(pool.Close)
	return pool
}
