package registry

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// schemaDDL is the node's own schema bootstrap, copied verbatim from
// src/orchestrator/paused_registry/postgres.rs. It is duplicated here rather
// than referenced because the point of this test is to read a table shaped
// exactly the way the nodes create it — a paraphrase would pass while the real
// thing failed.
const schemaDDL = `
CREATE TABLE IF NOT EXISTS paused_sandboxes (
    sandbox_id     UUID        PRIMARY KEY,
    cluster_id     UUID        NOT NULL,
    state          TEXT        NOT NULL,
    generation     BIGINT      NOT NULL,
    origin_node_id TEXT        NOT NULL,
    snapshot_id    UUID,
    metadata       JSONB       NOT NULL,
    paused_at      TIMESTAMPTZ NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL
);
ALTER TABLE paused_sandboxes ADD COLUMN IF NOT EXISTS claimed_by_node_id TEXT;
ALTER TABLE paused_sandboxes DROP CONSTRAINT IF EXISTS paused_sandboxes_state_check;
ALTER TABLE paused_sandboxes ADD CONSTRAINT paused_sandboxes_state_check
    CHECK (state IN ('publishing', 'paused', 'resuming', 'local_only', 'running'));
ALTER TABLE paused_sandboxes ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ;
ALTER TABLE paused_sandboxes ADD COLUMN IF NOT EXISTS sandbox_expires_at TIMESTAMPTZ;
CREATE INDEX IF NOT EXISTS paused_sandboxes_origin_node_idx ON paused_sandboxes (origin_node_id);
CREATE INDEX IF NOT EXISTS paused_sandboxes_updated_at_idx ON paused_sandboxes (updated_at);
`

const (
	clusterA = "11111111-1111-1111-1111-111111111111"
	clusterB = "22222222-2222-2222-2222-222222222222"

	sandboxPublishing = "aaaaaaaa-0000-0000-0000-000000000001"
	sandboxPaused     = "aaaaaaaa-0000-0000-0000-000000000002"
	sandboxResuming   = "aaaaaaaa-0000-0000-0000-000000000003"
	sandboxLocalOnly  = "aaaaaaaa-0000-0000-0000-000000000004"
	sandboxRunning    = "aaaaaaaa-0000-0000-0000-000000000005"
	sandboxOtherClust = "bbbbbbbb-0000-0000-0000-000000000001"

	snapshotPaused    = "cccccccc-0000-0000-0000-000000000002"
	snapshotResuming  = "cccccccc-0000-0000-0000-000000000003"
	snapshotOtherClus = "cccccccc-0000-0000-0000-000000000009"
)

// requireTestDSN returns the test database DSN, or skips the calling test when
// none is configured.
//
// A skipped test reports as passing, so a runner that is *supposed* to have a
// database — CI, or a verification run — sets SCHEDULER_REGISTRY_TEST_REQUIRED
// and gets a loud failure instead of a file full of green no-ops. Without it
// these tests can be silently skipped by the one thing that is meant to be
// enforcing them.
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

// setupRegistryDatabase creates the node-shaped table in the database named by
// SCHEDULER_REGISTRY_TEST_DSN, seeds one row per state, and drops the table
// again when the test finishes.
//
// It refuses to run against a database that already has the table. That
// database would be somebody's real registry, and this test ends by dropping
// it.
func setupRegistryDatabase(t *testing.T) string {
	t.Helper()

	dsn := requireTestDSN(t)

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect to test database failed: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })

	// The DDL is the node's own, verbatim and idempotent, so sharing the table
	// with whatever else uses this database is fine. What is not fine is
	// dropping it: this test used to, and guarded that by refusing to run when
	// the table already existed — which made running the suite twice, or
	// alongside any other test that needs the table, a failure rather than a
	// pass. Each test owns its rows by cluster id and removes those instead.
	if _, err := conn.Exec(ctx, schemaDDL); err != nil {
		t.Fatalf("create schema failed: %v", err)
	}
	t.Cleanup(func() {
		if _, err := conn.Exec(context.Background(),
			"DELETE FROM paused_sandboxes WHERE cluster_id = ANY($1::uuid[])",
			[]string{clusterA, clusterB},
		); err != nil {
			t.Logf("clean up test rows failed: %v", err)
		}
	})

	seed := `
INSERT INTO paused_sandboxes (
    sandbox_id, cluster_id, state, generation, origin_node_id, claimed_by_node_id,
    snapshot_id, metadata, paused_at, updated_at, lease_expires_at, sandbox_expires_at
) VALUES
    -- publishing: no snapshot yet, lease live.
    ($1, $7, 'publishing', 1, 'node-a', NULL, NULL, '{}'::jsonb,
     now() - interval '2 minutes', now() - interval '2 minutes', now() + interval '90 seconds', NULL),
    -- paused: snapshot published, claimable by anybody.
    ($2, $7, 'paused', 4, 'node-a', NULL, $9, '{}'::jsonb,
     now() - interval '10 minutes', now() - interval '9 minutes', now() + interval '90 seconds', NULL),
    -- resuming: origin still holds the artifacts, claimer is bringing it up.
    ($3, $7, 'resuming', 5, 'node-a', 'node-b', $10, '{}'::jsonb,
     now() - interval '10 minutes', now() - interval '30 seconds', now() + interval '90 seconds', now() + interval '1 hour'),
    -- local_only: the upload failed, the only copy is on node-b, and both
    -- lease columns were never written.
    ($4, $7, 'local_only', 2, 'node-b', NULL, NULL, '{}'::jsonb,
     now() - interval '1 hour', now() - interval '1 hour', NULL, NULL),
    -- running: live on node-b, lease lapsed and deadline passed, which is the
    -- combination the node-side reclaim acts on.
    ($5, $7, 'running', 7, 'node-b', NULL, NULL, '{}'::jsonb,
     now() - interval '3 hours', now() - interval '3 hours', now() - interval '2 hours', now() - interval '1 hour'),
    -- another cluster's row, to prove the cluster filter.
    ($6, $8, 'paused', 1, 'node-z', NULL, $11, '{}'::jsonb,
     now(), now(), now() + interval '90 seconds', NULL)
`
	if _, err := conn.Exec(ctx, seed,
		sandboxPublishing, sandboxPaused, sandboxResuming, sandboxLocalOnly, sandboxRunning, sandboxOtherClust,
		clusterA, clusterB,
		snapshotPaused, snapshotResuming, snapshotOtherClus,
	); err != nil {
		t.Fatalf("seed rows failed: %v", err)
	}

	return dsn
}

func newTestReader(t *testing.T, dsn string, clusterID string) *PostgresReader {
	t.Helper()

	reader, err := New(context.Background(), Config{
		DSN:          dsn,
		ClusterID:    clusterID,
		QueryTimeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("build reader failed: %v", err)
	}
	t.Cleanup(reader.Close)
	return reader
}

func TestPostgresReaderListReadsEveryStateWithLeases(t *testing.T) {
	dsn := setupRegistryDatabase(t)
	reader := newTestReader(t, dsn, clusterA)

	if reader.Ready() {
		t.Fatal("expected a reader that has not read yet to report not ready")
	}

	listing, err := reader.List(context.Background())
	if err != nil {
		t.Fatalf("list failed: %v", err)
	}
	if !reader.Ready() {
		t.Fatal("expected a successful read to make the reader ready")
	}
	if listing.Now.IsZero() {
		t.Fatal("expected the listing to carry the database clock")
	}
	if drift := time.Since(listing.Now); drift > time.Minute || drift < -time.Minute {
		t.Fatalf("database clock is implausibly far from ours: %s", drift)
	}

	byID := make(map[string]Sandbox, len(listing.Sandboxes))
	for _, sandbox := range listing.Sandboxes {
		byID[sandbox.SandboxID] = sandbox
	}
	if len(byID) != 5 {
		t.Fatalf("expected 5 rows for cluster A, got %d", len(byID))
	}
	if _, ok := byID[sandboxOtherClust]; ok {
		t.Fatal("cluster filter let another cluster's row through")
	}

	publishing := byID[sandboxPublishing]
	if publishing.State != StatePublishing {
		t.Fatalf("unexpected state %q", publishing.State)
	}
	if publishing.ClusterID != clusterA {
		t.Fatalf("unexpected cluster id %q", publishing.ClusterID)
	}
	if publishing.SnapshotID != "" {
		t.Fatalf("expected no snapshot while publishing, got %q", publishing.SnapshotID)
	}
	if publishing.ClaimedByNodeID != "" {
		t.Fatalf("expected no claimer while publishing, got %q", publishing.ClaimedByNodeID)
	}
	if publishing.Generation != 1 {
		t.Fatalf("unexpected generation %d", publishing.Generation)
	}
	if publishing.LeaseExpiresAt == nil {
		t.Fatal("expected the lease column to be read back")
	}
	if publishing.LeaseExpired(listing.Now) {
		t.Fatal("expected a live lease not to read as expired")
	}
	if publishing.Holder() != "node-a" {
		t.Fatalf("unexpected holder %q", publishing.Holder())
	}

	paused := byID[sandboxPaused]
	if paused.SnapshotID != snapshotPaused {
		t.Fatalf("unexpected snapshot id %q", paused.SnapshotID)
	}
	if paused.Invalid() {
		t.Fatal("a paused row with a snapshot must not read as invalid")
	}

	resuming := byID[sandboxResuming]
	if resuming.ClaimedByNodeID != "node-b" {
		t.Fatalf("unexpected claimer %q", resuming.ClaimedByNodeID)
	}
	if resuming.OriginNodeID != "node-a" {
		t.Fatalf("expected the claim to leave origin alone, got %q", resuming.OriginNodeID)
	}
	if resuming.Holder() != "node-b" {
		t.Fatalf("expected the claimer to hold a resuming row, got %q", resuming.Holder())
	}
	if resuming.SandboxExpiresAt == nil {
		t.Fatal("expected the sandbox deadline column to be read back")
	}

	localOnly := byID[sandboxLocalOnly]
	if localOnly.LeaseExpiresAt != nil {
		t.Fatalf("expected a NULL lease to stay nil, got %v", localOnly.LeaseExpiresAt)
	}
	if localOnly.SandboxExpiresAt != nil {
		t.Fatalf("expected a NULL deadline to stay nil, got %v", localOnly.SandboxExpiresAt)
	}
	// COALESCE(lease_expires_at, updated_at): a NULL lease reads as expired.
	if !localOnly.LeaseExpired(listing.Now) {
		t.Fatal("expected a NULL lease to read as expired")
	}

	running := byID[sandboxRunning]
	if !running.LeaseExpired(listing.Now) {
		t.Fatal("expected the lapsed lease to read as expired")
	}
	if running.SandboxExpiresAt == nil || !running.SandboxExpiresAt.Before(listing.Now) {
		t.Fatal("expected the running row's deadline to be in the past")
	}
}

func TestPostgresReaderListWithoutClusterFilterSeesEveryCluster(t *testing.T) {
	dsn := setupRegistryDatabase(t)
	reader := newTestReader(t, dsn, "")

	listing, err := reader.List(context.Background())
	if err != nil {
		t.Fatalf("list failed: %v", err)
	}
	if len(listing.Sandboxes) != 6 {
		t.Fatalf("expected all 6 rows without a cluster filter, got %d", len(listing.Sandboxes))
	}
}

func TestPostgresReaderGet(t *testing.T) {
	dsn := setupRegistryDatabase(t)
	reader := newTestReader(t, dsn, clusterA)
	ctx := context.Background()

	sandbox, ok, err := reader.Get(ctx, sandboxResuming)
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if !ok {
		t.Fatal("expected a hit for a seeded sandbox")
	}
	if sandbox.Holder() != "node-b" {
		t.Fatalf("unexpected holder %q", sandbox.Holder())
	}

	// A row that exists, but in another cluster: a miss, not a leak.
	if _, ok, err := reader.Get(ctx, sandboxOtherClust); err != nil || ok {
		t.Fatalf("expected another cluster's row to miss, got ok=%v err=%v", ok, err)
	}

	if _, ok, err := reader.Get(ctx, "dddddddd-0000-0000-0000-00000000ffff"); err != nil || ok {
		t.Fatalf("expected an unknown sandbox to miss, got ok=%v err=%v", ok, err)
	}

	// Not a uuid, so no row can match it. That is a miss, not a backend error:
	// counting it as one would make the read-failure counter fire on bad input.
	if _, ok, err := reader.Get(ctx, "not-a-uuid"); err != nil || ok {
		t.Fatalf("expected a malformed id to miss cleanly, got ok=%v err=%v", ok, err)
	}
}

// The pool is read-only at the database, not by convention. A write issued
// through it has to fail even though the role owning the DSN is perfectly able
// to write.
func TestPostgresReaderPoolRefusesWrites(t *testing.T) {
	dsn := setupRegistryDatabase(t)
	reader := newTestReader(t, dsn, clusterA)
	ctx := context.Background()

	// Ordered, with the DDL last: were the guard ever to come off, a DROP
	// running first would take the table out from under the other cases and
	// they would fail for the wrong reason.
	writes := []struct {
		name      string
		statement string
	}{
		{"update", "UPDATE paused_sandboxes SET generation = generation + 1"},
		{"delete", "DELETE FROM paused_sandboxes"},
		{"insert", "INSERT INTO paused_sandboxes (sandbox_id, cluster_id, state, generation, origin_node_id, metadata, paused_at, updated_at) " +
			"VALUES ('dddddddd-0000-0000-0000-000000000001', '" + clusterA + "', 'paused', 1, 'node-a', '{}'::jsonb, now(), now())"},
		{"ddl", "DROP TABLE paused_sandboxes"},
	}
	for _, write := range writes {
		t.Run(write.name, func(t *testing.T) {
			if _, err := reader.pool.Exec(ctx, write.statement); err == nil {
				t.Fatalf("expected the read-only pool to refuse %s", write.name)
			}
		})
	}

	// And the table is still intact afterwards.
	listing, err := reader.List(ctx)
	if err != nil {
		t.Fatalf("list after the refused writes failed: %v", err)
	}
	if len(listing.Sandboxes) != 5 {
		t.Fatalf("expected the 5 seeded rows to be untouched, got %d", len(listing.Sandboxes))
	}
}

// 🔴 A malformed id must not make the reader ready.
//
// Ready is what tells a lookup it may treat a missing row as the table's
// answer rather than as its own ignorance, and downstream that answer is a 404
// on a resume — the end of that sandbox's life. A read that never ran because
// PostgreSQL refused to cast the id says nothing about the table, so one probe
// with a bad id must not unlock authoritative absence answers from a reader
// that has never seen a row.
func TestPostgresReaderMalformedIDDoesNotMarkTheReaderReady(t *testing.T) {
	dsn := setupRegistryDatabase(t)
	reader := newTestReader(t, dsn, clusterA)
	ctx := context.Background()

	if _, ok, err := reader.Get(ctx, "not-a-uuid"); err != nil || ok {
		t.Fatalf("expected a malformed id to miss cleanly, got ok=%v err=%v", ok, err)
	}
	if reader.Ready() {
		t.Fatal("expected a refused cast to leave the reader not ready")
	}

	// A miss on a well-formed id is a different thing entirely: that query ran,
	// and its empty answer is the table's.
	if _, ok, err := reader.Get(ctx, "dddddddd-0000-0000-0000-00000000ffff"); err != nil || ok {
		t.Fatalf("expected an unknown sandbox to miss, got ok=%v err=%v", ok, err)
	}
	if !reader.Ready() {
		t.Fatal("expected a query that ran to make the reader ready")
	}
}

// The scope the reader reports is the scope it queries with. Anything
// comparing this table against another view of the fleet narrows that view with
// this value, so a reader that misreported it would have the comparison cover
// two different sets of nodes.
func TestPostgresReaderReportsItsClusterScope(t *testing.T) {
	dsn := setupRegistryDatabase(t)

	if got := newTestReader(t, dsn, clusterA).ClusterID(); got != clusterA {
		t.Fatalf("expected the configured cluster to be reported, got %q", got)
	}
	if got := newTestReader(t, dsn, "").ClusterID(); got != "" {
		t.Fatalf("expected an unscoped reader to report no cluster, got %q", got)
	}
}
