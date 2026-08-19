package registry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

// entryColumns is the node's ENTRY_COLUMNS (postgres.rs) with the two lease
// columns added and the uuid columns cast to text.
//
// The casts keep the decoder independent of which uuid codec the driver happens
// to register, and every one carries an explicit alias so that
// `SELECT claimed.*` over a RETURNING list stays readable by name.
//
// The lease columns are the one deliberate widening. The node leaves them out
// because no Rust consumer can see them, but the Go Entry type carries them and
// Sandbox.LeaseExpired reads LeaseExpiresAt with a fallback to UpdatedAt — so
// selecting nine columns into a struct that declares eleven would leave every
// entry claiming a lease that expired at the zero time. Reading two more
// columns cannot change which rows a statement matches.
const entryColumns = `sandbox_id::text         AS sandbox_id,
       cluster_id::text         AS cluster_id,
       state                    AS state,
       generation               AS generation,
       origin_node_id           AS origin_node_id,
       claimed_by_node_id       AS claimed_by_node_id,
       snapshot_id::text        AS snapshot_id,
       metadata                 AS metadata,
       paused_at                AS paused_at,
       updated_at               AS updated_at,
       lease_expires_at         AS lease_expires_at,
       sandbox_expires_at       AS sandbox_expires_at`

// leaseExpired is the node's LEASE_EXPIRED predicate, verbatim.
//
// COALESCE(lease_expires_at, updated_at) treats a row written before the column
// existed as already expired, which is the safe direction: a live holder
// refreshes it within one interval, a dead one never does.
const leaseExpired = "COALESCE(lease_expires_at, updated_at) < now()"

// liveHoldingsOfNode is the node's LIVE_HOLDINGS_OF_NODE, verbatim, including
// its parameter numbering: $2 is the node id in every statement that uses it.
//
// `running` names the node in origin_node_id, `resuming` in claimed_by_node_id,
// because a claim leaves origin_node_id pointing at whoever holds the local
// artifacts.
const liveHoldingsOfNode = `((state = 'running'  AND origin_node_id     = $2)
              OR (state = 'resuming' AND claimed_by_node_id = $2))`

const (
	// getManyChunk is the node's GET_MANY_CHUNK: the largest number of ids
	// bound into one statement, well under any server limit.
	getManyChunk = 1000

	// defaultLeaseTTL matches the node's `lease_ttl_secs` default. A lease
	// shorter than the cadence that renews it expires on a healthy node, and
	// the node applies a floor of three renewal intervals for that reason; this
	// side cannot see that cadence, so the default is the only value known to
	// satisfy it.
	defaultLeaseTTL = 90 * time.Second

	// defaultStoreQueryTimeout bounds a statement that would otherwise run
	// until the caller's own deadline. It is a backstop, not a policy: the node
	// sets the deadline that matters, and context.WithTimeout keeps whichever
	// of the two comes first.
	defaultStoreQueryTimeout = 30 * time.Second

	// defaultStoreMaxConnections is the whole point of moving these writes
	// behind one process: the node-side pool was 8 connections per machine.
	defaultStoreMaxConnections = 8
)

// PostgresStore is the write side of the paused registry.
//
// Every mutating statement carries its own precondition in the WHERE clause, so
// two callers racing on the same sandbox resolve through the database rather
// than through application-level locking: exactly one UPDATE matches, the other
// reports zero rows and its caller re-reads.
type PostgresStore struct {
	pool         *pgxpool.Pool
	leaseTTL     time.Duration
	queryTimeout time.Duration
	log          *zap.Logger
	// ownsPool is false on the views WithLeaseTTL hands out. Closing one of
	// those would drain the pool underneath the store it was derived from, and
	// a view is exactly the thing a per-request handler holds.
	ownsPool bool

	// grace withholds the claim's lapsed-lease arm through start-up. Held here
	// rather than checked by the handler because it selects between two
	// statements, and a guard that lives next to the SQL it changes cannot be
	// left out of a call path somebody adds later. Nil means no gate, which is
	// what a caller constructing the store directly gets.
	grace *Grace
	// breaker vetoes a reclamation pass that would delete more than it can
	// account for. Nil means no limit.
	breaker *DiscardBreaker
}

var _ Store = (*PostgresStore)(nil)

// NewStore builds the write store over the given DSN.
//
// It does not connect and it does not migrate: both are the caller's to
// sequence, because this process does more than serve the registry — a database
// that is down must not stop it from routing traffic. The registry surface
// answers UNAVAILABLE until Migrate has succeeded, which is the difference
// between "not ready to say" and "the cluster knows of no such sandbox".
//
// Only a DSN that cannot be parsed is an error here.
func NewStore(ctx context.Context, cfg StoreConfig) (Store, error) {
	dsn := strings.TrimSpace(cfg.DSN)
	if dsn == "" {
		return nil, errors.New("registry store dsn is required")
	}

	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse registry store dsn: %w", err)
	}
	if cfg.MaxConnections > 0 {
		poolCfg.MaxConns = cfg.MaxConnections
	} else {
		poolCfg.MaxConns = defaultStoreMaxConnections
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("create registry store pool: %w", err)
	}

	return newStoreWithPool(pool, cfg), nil
}

func newStoreWithPool(pool *pgxpool.Pool, cfg StoreConfig) *PostgresStore {
	leaseTTL := cfg.LeaseTTL
	if leaseTTL <= 0 {
		leaseTTL = defaultLeaseTTL
	}
	queryTimeout := cfg.QueryTimeout
	if queryTimeout <= 0 {
		queryTimeout = defaultStoreQueryTimeout
	}
	log := cfg.Logger
	if log == nil {
		log = zap.NewNop()
	}
	return &PostgresStore{
		pool:         pool,
		leaseTTL:     leaseTTL,
		queryTimeout: queryTimeout,
		log:          log,
		ownsPool:     true,
	}
}

// WithGuards attaches the restart gate and the discard breaker.
//
// Separate from construction because the gate has to exist before the store
// does: Migrate runs through the store, and the gate is cold until it returns.
func (s *PostgresStore) WithGuards(grace *Grace, breaker *DiscardBreaker) *PostgresStore {
	s.grace = grace
	s.breaker = breaker
	return s
}

// WithLeaseTTL returns a view of this store that stamps leases with ttl.
//
// 🔴 The lease TTL belongs to the node. It renews on its own cadence, and its
// configuration is what enforces that the TTL leaves room for two missed
// renewals; a TTL chosen on this side instead would expire rows underneath a
// node that is renewing exactly as it was told to — and a parked row whose
// lease has expired is one another node may take, rewinding the sandbox to an
// older snapshot. So the caller's value wins, and this store's own is only the
// fallback for a caller that reports none.
//
// A non-positive ttl returns the store unchanged rather than a view stamping
// zero-length leases, which would expire on arrival.
func (s *PostgresStore) WithLeaseTTL(ttl time.Duration) Store {
	if ttl <= 0 || ttl == s.leaseTTL {
		return s
	}
	view := *s
	view.leaseTTL = ttl
	view.ownsPool = false
	return &view
}

// Pool exposes the underlying pool so the caller can migrate through it and
// run the grace pass before opening the service.
func (s *PostgresStore) Pool() *pgxpool.Pool { return s.pool }

// LeaseTTL is the lease length this store hands out.
func (s *PostgresStore) LeaseTTL() time.Duration { return s.leaseTTL }

// Close drains the pool, unless this is a view handed out by WithLeaseTTL —
// those borrow the pool and closing one would take the original down with it.
func (s *PostgresStore) Close() {
	if s.ownsPool && s.pool != nil {
		s.pool.Close()
	}
}

// Migrate applies the schema, taking the same advisory lock the nodes take.
func (s *PostgresStore) Migrate(ctx context.Context) error {
	return Migrate(ctx, s.pool)
}

func (s *PostgresStore) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, s.queryTimeout)
}

func (s *PostgresStore) ttlSeconds() float64 {
	return s.leaseTTL.Seconds()
}

// ─────────────────────────────────────────────────────────────────────────────
// Reads
// ─────────────────────────────────────────────────────────────────────────────

const getSQL = `SELECT ` + entryColumns + `
  FROM paused_sandboxes
 WHERE sandbox_id = $1::uuid AND cluster_id = $2::uuid`

// Get reads one row, scoped to the caller's cluster.
//
// 🔴 A malformed id is an error here, not an absence. The read-only Reader
// answers "no row" for one because nothing downstream of it deletes anything;
// on this path absence is what makes a caller throw away the only copy of a
// workspace, so an id that never reached a WHERE clause must never be reported
// as a WHERE clause that matched nothing.
func (s *PostgresStore) Get(ctx context.Context, clusterID, sandboxID string) (Entry, bool, error) {
	cluster, err := requireUUID("cluster_id", clusterID)
	if err != nil {
		return Entry{}, false, err
	}
	sandbox, err := requireUUID("sandbox_id", sandboxID)
	if err != nil {
		return Entry{}, false, err
	}

	ctx, cancel := s.withTimeout(ctx)
	defer cancel()

	return s.fetch(ctx, cluster, sandbox)
}

// fetch is Get without the argument checking, for the internal re-reads that
// already hold validated ids.
func (s *PostgresStore) fetch(ctx context.Context, clusterID, sandboxID string) (Entry, bool, error) {
	rows, err := s.pool.Query(ctx, getSQL, sandboxID, clusterID)
	if err != nil {
		return Entry{}, false, fmt.Errorf("registry get: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return Entry{}, false, fmt.Errorf("registry get: %w", err)
		}
		return Entry{}, false, nil
	}

	entry, err := scanEntry(rows)
	if err != nil {
		return Entry{}, false, err
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return Entry{}, false, fmt.Errorf("registry get: %w", err)
	}
	return entry, true, nil
}

const getManySQL = `SELECT ` + entryColumns + `
  FROM paused_sandboxes
 WHERE cluster_id = $1::uuid AND sandbox_id = ANY($2::uuid[])`

// GetMany reads the rows that exist among the requested ids.
//
// 🔴 All or nothing. One chunk that fails, one row this build cannot decode,
// one id that is not a uuid — each of them fails the whole call. A shorter map
// is indistinguishable from "those sandboxes have no rows", and the caller
// answers that by deleting local artifacts and tearing down running VMs.
func (s *PostgresStore) GetMany(ctx context.Context, clusterID string, sandboxIDs []string) (Rows, error) {
	cluster, err := requireUUID("cluster_id", clusterID)
	if err != nil {
		return Rows{}, err
	}

	// Validated up front rather than left to PostgreSQL, so one malformed id
	// fails on its own terms instead of taking a whole chunk down with a cast
	// error that names no id.
	ids := make([]string, 0, len(sandboxIDs))
	for _, raw := range sandboxIDs {
		id, err := requireUUID("sandbox_id", raw)
		if err != nil {
			return Rows{}, err
		}
		ids = append(ids, id)
	}

	entries := make(map[string]Entry, len(ids))
	if len(ids) == 0 {
		// No statement at all: an empty batch has nothing to ask about, and
		// asking anyway is a round trip that can only fail. Covered is the
		// empty set rather than nil for the same reason the rest of this
		// answers precisely — "I looked up nothing" is a fact, and the caller
		// checks it against a request that asked for nothing.
		return Rows{Entries: entries, Covered: []string{}, Now: time.Time{}}, nil
	}

	ctx, cancel := s.withTimeout(ctx)
	defer cancel()

	// Read the clock before the rows, not after.
	//
	// The chunks below are separate statements, so no single snapshot covers
	// them anyway and pretending otherwise would be the lie. Taking `now` first
	// makes every lease look *less* expired than it is by up to the duration of
	// this call, and every decision downstream — who may take a sandbox over,
	// what may be reclaimed — errs toward leaving somebody else's holding
	// alone.
	var now time.Time
	if err := s.pool.QueryRow(ctx, "SELECT now()").Scan(&now); err != nil {
		return Rows{}, fmt.Errorf("registry get_many clock: %w", err)
	}

	for start := 0; start < len(ids); start += getManyChunk {
		end := start + getManyChunk
		if end > len(ids) {
			end = len(ids)
		}

		rows, err := s.pool.Query(ctx, getManySQL, cluster, ids[start:end])
		if err != nil {
			return Rows{}, fmt.Errorf("registry get_many: %w", err)
		}
		chunk, err := collectEntries(rows)
		if err != nil {
			return Rows{}, err
		}
		for _, entry := range chunk {
			entries[entry.SandboxID] = entry
		}
	}

	// Every id reached a WHERE clause: the loop above returns on the first
	// failure, so arriving here means all of them were looked up. That is the
	// all-or-nothing guarantee, stated rather than assumed — it is the caller
	// one process away who cannot see it and who deletes things when a row is
	// absent.
	return Rows{Entries: entries, Covered: ids, Now: now}, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Pause
// ─────────────────────────────────────────────────────────────────────────────

// beginPauseSQL is the node's begin_pause with one deliberate change: paused_at
// and updated_at are the database's now() rather than a timestamp the caller
// computed.
//
// 🔴 Why the change. The node bound its own Utc::now() here and the database's
// now() everywhere else, so updated_at carried two different clocks depending
// on which write touched the row last. Nothing read it until the controller's
// restart grace period, which infers how long this process was gone from
// max(updated_at) — and a node whose clock runs fast makes that look like less
// downtime than there was, which shortens the grace period, which is exactly
// the case it exists to cover. The caller's paused_at was already being
// discarded by the node's own binding, so nothing depended on it.
const beginPauseSQL = `
WITH previous AS (
    SELECT snapshot_id FROM paused_sandboxes
     WHERE sandbox_id = $1::uuid AND cluster_id = $2::uuid
),
upserted AS (
    INSERT INTO paused_sandboxes (
        sandbox_id, cluster_id, state, generation, origin_node_id,
        claimed_by_node_id, snapshot_id, metadata, paused_at, updated_at,
        lease_expires_at
    )
    VALUES ($1::uuid, $2::uuid, 'publishing', 1, $3, NULL, NULL, $4::jsonb, now(), now(),
            now() + make_interval(secs => $5::double precision))
    ON CONFLICT (sandbox_id) DO UPDATE SET
        state              = 'publishing',
        generation         = paused_sandboxes.generation + 1,
        origin_node_id     = EXCLUDED.origin_node_id,
        claimed_by_node_id = NULL,
        metadata           = EXCLUDED.metadata,
        paused_at          = EXCLUDED.paused_at,
        updated_at         = EXCLUDED.updated_at,
        lease_expires_at   = EXCLUDED.lease_expires_at
    WHERE paused_sandboxes.cluster_id = EXCLUDED.cluster_id
    RETURNING generation
)
SELECT upserted.generation          AS generation,
       previous.snapshot_id::text   AS previous_snapshot_id
  FROM upserted
  LEFT JOIN previous ON TRUE`

// BeginPause records that a sandbox is being paused.
//
// snapshot_id is deliberately absent from the update list, so the row keeps
// pointing at the last snapshot that actually reached the repository until
// CompletePause replaces it. Clearing it here would leave a pause whose upload
// then failed with a row naming no snapshot at all — while the perfectly good
// previous snapshot sat in the repository with nothing referencing it.
//
// The `previous` CTE reads the row as it stood before the upsert (every CTE
// sees the same statement-start snapshot), which is the only way to learn which
// snapshot this pause supersedes; RETURNING would hand back the new row.
func (s *PostgresStore) BeginPause(ctx context.Context, in BeginPauseInput) (BeganPause, error) {
	cluster, err := requireUUID("cluster_id", in.ClusterID)
	if err != nil {
		return BeganPause{}, err
	}
	sandbox, err := requireUUID("sandbox_id", in.SandboxID)
	if err != nil {
		return BeganPause{}, err
	}
	if strings.TrimSpace(in.OriginNodeID) == "" {
		return BeganPause{}, fmt.Errorf("%w: origin_node_id is required", ErrInvalidArgument)
	}
	// Checked for shape, never decoded.
	//
	// 🔴 "Valid JSON" is not enough, and `null` is why. It is a perfectly legal
	// JSON document, so a validity check lets it through and JSONB stores it —
	// but the node's SandboxMetadata has ten required fields, so that row
	// afterwards fails to decode *on the node*. And the node's decoder is
	// shared by get, get_many and claim_for_resume, so one such row makes its
	// whole batch fail and freezes that machine's reconciliation entirely. One
	// poisoned row, one stalled node, and the cause is a write this side
	// accepted. Arrays and scalars are refused for the same reason.
	//
	// What this side does *not* do is look inside the object. Which fields
	// belong there is the node's business, and a Go type asserting an opinion
	// would drop every field this build has not heard of — silently, because
	// the Rust type does not reject unknown ones.
	if !isJSONObject(in.Metadata) {
		return BeganPause{}, fmt.Errorf("%w: sandbox %s has no metadata object", ErrInvalidRecord, sandbox)
	}

	ctx, cancel := s.withTimeout(ctx)
	defer cancel()

	var (
		generation       int64
		previousSnapshot *string
	)
	err = s.pool.QueryRow(ctx, beginPauseSQL,
		sandbox, cluster, in.OriginNodeID, []byte(in.Metadata), s.ttlSeconds(),
	).Scan(&generation, &previousSnapshot)
	if errors.Is(err, pgx.ErrNoRows) {
		// The upsert's WHERE kept one cluster from taking over another's row.
		// Told rather than silently rewritten: the row belongs to somebody else.
		return BeganPause{}, fmt.Errorf("%w: registry already holds sandbox %s for a different cluster", ErrInvalidRecord, sandbox)
	}
	if err != nil {
		return BeganPause{}, fmt.Errorf("registry begin_pause: %w", err)
	}

	began := BeganPause{Generation: generation}
	if previousSnapshot != nil {
		began.PreviousSnapshotID = *previousSnapshot
	}
	return began, nil
}

const completePauseSQL = `
UPDATE paused_sandboxes
   SET state = 'paused', snapshot_id = $3::uuid, updated_at = now(),
       lease_expires_at = now() + make_interval(secs => $4::double precision)
 WHERE sandbox_id = $1::uuid AND cluster_id = $5::uuid
   AND generation = $2 AND state = 'publishing'`

// CompletePause publishes the snapshot a pause produced.
func (s *PostgresStore) CompletePause(ctx context.Context, clusterID, sandboxID string, expectGeneration int64, snapshotID string) error {
	cluster, err := requireUUID("cluster_id", clusterID)
	if err != nil {
		return err
	}
	sandbox, err := requireUUID("sandbox_id", sandboxID)
	if err != nil {
		return err
	}
	snapshot, err := requireUUID("snapshot_id", snapshotID)
	if err != nil {
		return err
	}

	ctx, cancel := s.withTimeout(ctx)
	defer cancel()

	tag, err := s.pool.Exec(ctx, completePauseSQL, sandbox, expectGeneration, snapshot, s.ttlSeconds(), cluster)
	if err != nil {
		return fmt.Errorf("registry complete_pause: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: sandbox %s is not publishing at generation %d", ErrGenerationConflict, sandbox, expectGeneration)
	}
	return nil
}

const markLocalOnlySQL = `
UPDATE paused_sandboxes
   SET state = 'local_only', updated_at = now(),
       lease_expires_at = now() + make_interval(secs => $3::double precision)
 WHERE sandbox_id = $1::uuid AND cluster_id = $4::uuid
   AND generation = $2 AND state = 'publishing'`

// MarkLocalOnly records that the snapshot never reached the repository.
//
// The row is kept, not deleted: the sandbox really is paused, only nobody but
// its origin node can bring it back. Deleting it would make "still parked on
// its own node" indistinguishable from "resumed elsewhere, or destroyed", and
// reconciliation answers the second by throwing away what is now the only copy.
//
// A no-op update is reported rather than swallowed: a downgrade that quietly
// matches nothing leaves the row stuck in `publishing`, and every resume from
// another node then answers "still uploading" about an upload that gave up long
// ago. The caller cannot repair it, but it can say so.
func (s *PostgresStore) MarkLocalOnly(ctx context.Context, clusterID, sandboxID string, expectGeneration int64) error {
	cluster, err := requireUUID("cluster_id", clusterID)
	if err != nil {
		return err
	}
	sandbox, err := requireUUID("sandbox_id", sandboxID)
	if err != nil {
		return err
	}

	ctx, cancel := s.withTimeout(ctx)
	defer cancel()

	tag, err := s.pool.Exec(ctx, markLocalOnlySQL, sandbox, expectGeneration, s.ttlSeconds(), cluster)
	if err != nil {
		return fmt.Errorf("registry mark_local_only: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: sandbox %s is not publishing at generation %d", ErrGenerationConflict, sandbox, expectGeneration)
	}
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Resume
// ─────────────────────────────────────────────────────────────────────────────

// claimForResumeSQL is the three-way test, verbatim from the node.
//
// Two ways to qualify, and one state that never does.
//
// A `paused` row is free for the taking: nobody is holding the sandbox and its
// snapshot is durable, which is the ordinary cross-node resume. The lease is
// not consulted, because making every ordinary resume wait out a lease would
// cost latency for nothing.
//
// `publishing` and `local_only` name a node that paused the sandbox but never
// got its snapshot into the repository. The VM is already stopped, so
// rebuilding elsewhere cannot duplicate it — it only rewinds to the snapshot
// the *previous* pause left behind. That is a real loss, so it waits for a full
// lease to go unrenewed and is logged as the degradation it is.
//
// 🔴 `running` and `resuming` are never claimable here, however long the lease
// has been lapsed. Their VM may still be up: a lapsed lease says the holder
// cannot reach this database, which a partitioned node — still running every
// sandbox it has, still being routed traffic — satisfies exactly as well as a
// dead one. Live rows are released only by the successor process on the
// holder's own machine (ReleaseNodeHoldings).
//
// The `previous` CTE is what makes the outcome knowable at all. Both CTEs read
// the same statement-start snapshot, so it sees the row as the UPDATE found it,
// while RETURNING can only describe the row the UPDATE left behind — where
// state is unconditionally `resuming`.
const claimForResumeSQL = `
WITH previous AS (
    SELECT state AS previous_state
      FROM paused_sandboxes
     WHERE sandbox_id = $1::uuid AND cluster_id = $4::uuid
),
claimed AS (
    UPDATE paused_sandboxes
       SET state = 'resuming', claimed_by_node_id = $2,
           generation = generation + 1, updated_at = now(),
           lease_expires_at = now() + make_interval(secs => $3::double precision)
     WHERE sandbox_id = $1::uuid
       AND cluster_id = $4::uuid
       AND snapshot_id IS NOT NULL
       AND (state = 'paused'
         OR (state IN ('publishing', 'local_only') AND ` + leaseExpired + `))
    RETURNING ` + entryColumns + `
)
SELECT claimed.*, previous.previous_state
  FROM claimed JOIN previous ON TRUE`

// claimForResumeDurableOnlySQL is claimForResumeSQL without its lapsed-lease
// arm: only a `paused` row, whose snapshot is durable and whose holder is
// nobody, may be taken. See Grace.
const claimForResumeDurableOnlySQL = `
WITH previous AS (
    SELECT state AS previous_state
      FROM paused_sandboxes
     WHERE sandbox_id = $1::uuid AND cluster_id = $4::uuid
),
claimed AS (
    UPDATE paused_sandboxes
       SET state = 'resuming', claimed_by_node_id = $2,
           generation = generation + 1, updated_at = now(),
           lease_expires_at = now() + make_interval(secs => $3::double precision)
     WHERE sandbox_id = $1::uuid
       AND cluster_id = $4::uuid
       AND snapshot_id IS NOT NULL
       AND state = 'paused'
    RETURNING ` + entryColumns + `
)
SELECT claimed.*, previous.previous_state
  FROM claimed JOIN previous ON TRUE`

// ClaimForResume takes ownership of a sandbox so nodeID can bring it back.
//
// origin_node_id deliberately stays untouched: it still names the node holding
// the local fast-path artifacts, and only becomes right again once this resume
// succeeds and MarkRunning repoints it. claimed_by_node_id is what says who is
// doing the work meanwhile — without it the origin node cannot tell a resume
// happening elsewhere from its own row and would happily start a second copy.
func (s *PostgresStore) ClaimForResume(ctx context.Context, clusterID, sandboxID, nodeID string) (ResumeClaim, error) {
	cluster, err := requireUUID("cluster_id", clusterID)
	if err != nil {
		return ResumeClaim{}, err
	}
	sandbox, err := requireUUID("sandbox_id", sandboxID)
	if err != nil {
		return ResumeClaim{}, err
	}
	if strings.TrimSpace(nodeID) == "" {
		return ResumeClaim{}, fmt.Errorf("%w: node_id is required", ErrInvalidArgument)
	}

	ctx, cancel := s.withTimeout(ctx)
	defer cancel()

	// The lapsed-lease arm is dropped through the restart grace window: every
	// lease in the table looks expired then because this process is the reason
	// nobody renewed it, and that arm is the one that takes a sandbox off the
	// node still holding it. A row it would have taken falls through to the
	// re-read below and is answered NotReady — parked on its origin — which is
	// both true and the answer the node already knows how to act on.
	claimSQL := claimForResumeSQL
	if !s.grace.allowsLeaseTakeover() {
		claimSQL = claimForResumeDurableOnlySQL
	}

	rows, err := s.pool.Query(ctx, claimSQL, sandbox, nodeID, s.ttlSeconds(), cluster)
	if err != nil {
		return ResumeClaim{}, fmt.Errorf("registry claim_for_resume: %w", err)
	}

	claimed := false
	var (
		entry         Entry
		previousState State
	)
	if rows.Next() {
		claimed = true
		entry, previousState, err = scanClaim(rows)
		if err != nil {
			rows.Close()
			return ResumeClaim{}, err
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return ResumeClaim{}, fmt.Errorf("registry claim_for_resume: %w", err)
	}

	if claimed {
		s.logClaim(sandbox, nodeID, entry, previousState)
		claimedEntry := entry
		return ResumeClaim{
			Outcome:       ClaimOutcomeClaimed,
			Entry:         &claimedEntry,
			PreviousState: previousState,
		}, nil
	}

	// The claim did not match. Re-read to tell the reasons apart, so the caller
	// can redirect instead of reporting a bare "not found". A parked row that
	// gets here still has a live lease; a live row gets here whatever its lease
	// says, and stays with its holder either way.
	current, found, err := s.fetch(ctx, cluster, sandbox)
	if err != nil {
		return ResumeClaim{}, err
	}
	if !found {
		return ResumeClaim{Outcome: ClaimOutcomeNotFound}, nil
	}

	switch current.State {
	case StatePublishing, StateLocalOnly:
		// Both mean "parked on its origin node": publishing is still uploading,
		// local_only never will be.
		return ResumeClaim{Outcome: ClaimOutcomeNotReady, OriginNodeID: current.OriginNodeID}, nil
	case StateResuming, StateRunning:
		// Live somewhere else, or being brought up somewhere else. The lease is
		// not consulted: no timeout makes a live sandbox safe to rebuild here.
		origin := current.ClaimedByNodeID
		if origin == "" {
			origin = current.OriginNodeID
		}
		return ResumeClaim{
			Outcome:        ClaimOutcomeConflict,
			OriginNodeID:   origin,
			ConflictReason: ConflictReasonLiveElsewhere,
		}, nil
	case StatePaused:
		// Lost a race with another claimer that has since released it. The row
		// is claimable again, so this is not "the sandbox is somewhere else" —
		// it is "try again", and a caller that cannot tell the two apart either
		// retries something it must not or gives up on something it could have.
		return ResumeClaim{
			Outcome:        ClaimOutcomeConflict,
			OriginNodeID:   current.OriginNodeID,
			ConflictReason: ConflictReasonClaimLost,
		}, nil
	default:
		// scanEntry rejects any state outside the five, so this is unreachable
		// unless that decoder and this switch have drifted apart.
		return ResumeClaim{}, fmt.Errorf("%w: sandbox %s has unhandled state %q", ErrInvalidRecord, sandbox, current.State)
	}
}

// logClaim says which of the three things actually happened, decided here
// rather than left for a reader to infer from the row: an ordinary resume and a
// claim that cost somebody their last unpublished pause are not the same event,
// and only the second is worth waking anybody for.
func (s *PostgresStore) logClaim(sandboxID, nodeID string, entry Entry, previousState State) {
	switch previousState {
	case StatePaused:
		s.log.Debug("claimed a sandbox from its published snapshot",
			zap.String("sandbox_id", sandboxID),
			zap.String("node_id", nodeID),
			zap.Int64("generation", entry.Generation),
			zap.String("claim_outcome", "durable"),
		)
	case StatePublishing, StateLocalOnly:
		registryClaimRewound.Inc()
		s.log.Warn("took over a sandbox parked on a node that stopped renewing its lease; "+
			"restoring from the last snapshot that reached the repository, so any work since that snapshot is lost",
			zap.String("sandbox_id", sandboxID),
			zap.String("node_id", nodeID),
			zap.String("claim_outcome", "rewound"),
			zap.String("previous_state", string(previousState)),
			zap.String("previous_holder", entry.OriginNodeID),
		)
	default:
		// Unreachable through the WHERE above, which is precisely why it is
		// loud: reaching it means the predicate and this switch have drifted
		// apart, and the claim just duplicated a sandbox that was live
		// somewhere else.
		registryClaimInvariantViolation.Inc()
		s.log.Error("claimed a sandbox that was not parked; a live sandbox may now exist twice",
			zap.String("sandbox_id", sandboxID),
			zap.String("node_id", nodeID),
			zap.String("claim_outcome", "invariant_violation"),
			zap.String("previous_state", string(previousState)),
			zap.String("previous_holder", entry.OriginNodeID),
		)
	}
}

const releaseClaimSQL = `
UPDATE paused_sandboxes
   SET state = 'paused', claimed_by_node_id = NULL, updated_at = now(),
       lease_expires_at = now() + make_interval(secs => $3::double precision)
 WHERE sandbox_id = $1::uuid AND cluster_id = $4::uuid
   AND generation = $2 AND state = 'resuming'`

// ReleaseClaim hands a claimed sandbox back without resuming it.
//
// Back to `paused` even when the claim was taken over a `running` row: the
// claim is only ever handed out when no node holds the sandbox, so its snapshot
// is the whole truth and `paused` is what describes that.
//
// 🔴 Zero rows is success, unlike CompletePause and MarkLocalOnly. The node
// treats it that way and its caller only warns on a transport error. A release
// that matches nothing means somebody else already moved the row on, which is
// the outcome this was trying to produce.
//
// Success, but no longer silent. The node used to run this statement itself and
// could see the zero-row tag; behind an RPC that evidence stops at this process
// unless it is carried back, and it is the only thing that says a node is
// quoting a generation it lost.
func (s *PostgresStore) ReleaseClaim(ctx context.Context, clusterID, sandboxID string, expectGeneration int64) (bool, error) {
	cluster, err := requireUUID("cluster_id", clusterID)
	if err != nil {
		return false, err
	}
	sandbox, err := requireUUID("sandbox_id", sandboxID)
	if err != nil {
		return false, err
	}

	ctx, cancel := s.withTimeout(ctx)
	defer cancel()

	tag, err := s.pool.Exec(ctx, releaseClaimSQL, sandbox, expectGeneration, s.ttlSeconds(), cluster)
	if err != nil {
		return false, fmt.Errorf("registry release_claim: %w", err)
	}
	return tag.RowsAffected() > 0, nil
}

const markRunningSQL = `
UPDATE paused_sandboxes
   SET state = 'running', origin_node_id = $2, claimed_by_node_id = NULL,
       generation = generation + 1, updated_at = now(),
       lease_expires_at = now() + make_interval(secs => $3::double precision)
 WHERE sandbox_id = $1::uuid
   AND cluster_id = $4::uuid
   AND (claimed_by_node_id IS NULL OR claimed_by_node_id = $2)`

// MarkRunning records that a sandbox is live on nodeID.
//
// 🔴 No insert, and that is the important half: a sandbox the cluster was never
// told about must stay that way, otherwise every resume on a node with a
// node-local history would start publishing rows for sandboxes that have no
// snapshot behind them.
//
// The claim guard is what stops one node's resume from erasing another node's
// in-flight one. Without it a blind write here would clear claimed_by_node_id
// mid-claim, and both nodes would go on to bring the same sandbox up believing
// they held it.
func (s *PostgresStore) MarkRunning(ctx context.Context, clusterID, sandboxID, nodeID string) (MarkRunningOutcome, error) {
	cluster, err := requireUUID("cluster_id", clusterID)
	if err != nil {
		return MarkRunningUntracked, err
	}
	sandbox, err := requireUUID("sandbox_id", sandboxID)
	if err != nil {
		return MarkRunningUntracked, err
	}
	if strings.TrimSpace(nodeID) == "" {
		return MarkRunningUntracked, fmt.Errorf("%w: node_id is required", ErrInvalidArgument)
	}

	ctx, cancel := s.withTimeout(ctx)
	defer cancel()

	tag, err := s.pool.Exec(ctx, markRunningSQL, sandbox, nodeID, s.ttlSeconds(), cluster)
	if err != nil {
		return MarkRunningUntracked, fmt.Errorf("registry mark_running: %w", err)
	}
	if tag.RowsAffected() > 0 {
		return MarkRunningAdopted, nil
	}

	// Nothing matched: either the cluster does not track this sandbox (by far
	// the common case, and correct), or somebody else holds the claim — which
	// means two nodes believe they are resuming it and is worth saying out loud.
	//
	// The re-read's error is propagated rather than folded into an outcome:
	// "untracked" tells the caller the registry has no say over this sandbox,
	// and a row that exists but could not be decoded is not that.
	//
	// 🔴 The re-read is also the whole reason this returns three answers rather
	// than two. It happens here now, so a caller across the wire never sees it;
	// before phase 2 the node did it itself and could act on what it found.
	entry, found, err := s.fetch(ctx, cluster, sandbox)
	if err != nil {
		return MarkRunningUntracked, err
	}
	if found {
		registryMarkRunningRefused.Inc()
		s.log.Warn("refused to mark the sandbox running here: another node holds the resume claim",
			zap.String("sandbox_id", sandbox),
			zap.String("node_id", nodeID),
			zap.String("claimed_by", entry.ClaimedByNodeID),
		)
		return MarkRunningHeldElsewhere, nil
	}
	return MarkRunningUntracked, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Leases
// ─────────────────────────────────────────────────────────────────────────────

// renewLeaseSQL renews only rows this node is actually the holder of.
//
// Passing the whole local roster is deliberate — the predicate, not the caller,
// decides which of them the node has standing to renew, so a node cannot extend
// another node's lease by listing a sandbox it does not hold.
//
// `paused` is absent on purpose: that state means nobody holds the sandbox, so
// there is nothing to keep alive and renewing it would only be noise.
//
// The deadline rides along on the same statement rather than being written
// anywhere else, because these two facts are only useful together: "when did
// the holder last check in" and "how long was this sandbox supposed to live"
// are what reclamation compares. Writing them at different moments would let a
// row claim a deadline from one instant and a lease from another.
const renewLeaseSQL = `
UPDATE paused_sandboxes AS p
   SET lease_expires_at   = now() + make_interval(secs => $1::double precision),
       sandbox_expires_at = v.expires_at,
       updated_at         = now()
  FROM (SELECT unnest($3::uuid[])        AS sandbox_id,
               unnest($4::timestamptz[]) AS expires_at) AS v
 WHERE p.sandbox_id = v.sandbox_id
   AND p.cluster_id = $2::uuid
   AND ((p.state IN ('running', 'publishing', 'local_only') AND p.origin_node_id = $5)
     OR (p.state = 'resuming' AND p.claimed_by_node_id = $5))`

// RenewLease extends the lease on every sandbox nodeID reports holding, and
// records each one's current deadline.
//
// The lease length is this store's — see WithLeaseTTL for why a caller that
// reports its own gets that one instead.
func (s *PostgresStore) RenewLease(ctx context.Context, clusterID, nodeID string, held []HeldSandbox) (uint64, error) {
	cluster, err := requireUUID("cluster_id", clusterID)
	if err != nil {
		return 0, err
	}
	if strings.TrimSpace(nodeID) == "" {
		return 0, fmt.Errorf("%w: node_id is required", ErrInvalidArgument)
	}
	if len(held) == 0 {
		// Nothing to renew, so nothing is asked. Matches the node, and keeps an
		// idle node's timer from producing a statement per tick.
		return 0, nil
	}

	ids := make([]string, 0, len(held))
	deadlines := make([]*time.Time, 0, len(held))
	for _, h := range held {
		id, err := requireUUID("sandbox_id", h.SandboxID)
		if err != nil {
			return 0, err
		}
		ids = append(ids, id)
		deadlines = append(deadlines, h.ExpiresAt)
	}

	ctx, cancel := s.withTimeout(ctx)
	defer cancel()

	tag, err := s.pool.Exec(ctx, renewLeaseSQL, s.ttlSeconds(), cluster, ids, deadlines, nodeID)
	if err != nil {
		return 0, fmt.Errorf("registry renew_lease: %w", err)
	}
	return uint64(tag.RowsAffected()), nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Reclamation
// ─────────────────────────────────────────────────────────────────────────────

// reclaimReleasedSQL and reclaimDiscardedSQL are the two halves of the
// cluster's backstop, and neither condition alone would do.
//
// The lease alone is what ClaimForResume refuses to act on: it cannot tell a
// dead node from a partitioned one, and acting on it duplicates live sandboxes.
//
// sandbox_expires_at < now() alone would race the node's own eviction. A
// reachable node evicts its expired sandboxes itself — pausing them properly
// and publishing a fresh snapshot — and that is by far the better outcome, so
// the cluster only steps in once nobody has renewed for a full lease.
//
// Together they describe a sandbox that has outlived the deadline its own user
// set, on a node that has not been heard from since before it did. Reclaiming
// that is enforcing the timeout, not guessing at the node's health.
//
// A NULL sandbox_expires_at never matches, which covers both a sandbox asked
// never to expire and a row whose holder has not renewed since the column
// existed. Both are the safe answer: leave it alone.
const reclaimReleasedSQL = `
UPDATE paused_sandboxes
   SET state = 'paused', claimed_by_node_id = NULL,
       generation = generation + 1, updated_at = now(),
       lease_expires_at = now()
 WHERE cluster_id = $1::uuid
   AND snapshot_id IS NOT NULL
   AND state IN ('running', 'resuming')
   AND ` + leaseExpired + `
   AND sandbox_expires_at < now()`

const reclaimDiscardedSQL = `
DELETE FROM paused_sandboxes
 WHERE cluster_id = $1::uuid
   AND snapshot_id IS NULL
   AND state IN ('running', 'resuming')
   AND ` + leaseExpired + `
   AND sandbox_expires_at < now()`

// countReclaimDiscardableSQL counts what reclaimDiscardedSQL would delete,
// inside the same transaction and therefore against the same snapshot.
//
// It exists for the circuit breaker: a count is the only way to decide whether
// to run the DELETE at all, and deciding after the fact is not deciding.
const countReclaimDiscardableSQL = `
SELECT count(*) FROM paused_sandboxes
 WHERE cluster_id = $1::uuid
   AND snapshot_id IS NULL
   AND state IN ('running', 'resuming')
   AND ` + leaseExpired + `
   AND sandbox_expires_at < now()`

// countClusterRowsSQL is the denominator the discard ratio is measured against.
const countClusterRowsSQL = `SELECT count(*) FROM paused_sandboxes WHERE cluster_id = $1::uuid`

// ReclaimExpiredHoldings frees rows whose holder has stopped renewing *and*
// whose sandbox has outlived its own deadline.
//
// Driven by a timer here rather than exposed over the wire: it was always the
// cluster's backstop rather than any node's business, and running it from
// several places at once is only harmless because each statement is a single
// conditional write.
func (s *PostgresStore) ReclaimExpiredHoldings(ctx context.Context, clusterID string) (ReleasedHoldings, error) {
	return s.reclaimExpiredHoldings(ctx, clusterID, s.breaker)
}

// reclaimExpiredHoldings is ReclaimExpiredHoldings with an optional veto on the
// discarding half. See DiscardBreaker.
func (s *PostgresStore) reclaimExpiredHoldings(ctx context.Context, clusterID string, breaker *DiscardBreaker) (ReleasedHoldings, error) {
	cluster, err := requireUUID("cluster_id", clusterID)
	if err != nil {
		return ReleasedHoldings{}, err
	}

	ctx, cancel := s.withTimeout(ctx)
	defer cancel()

	// One transaction, so a resume arriving between the statements cannot see a
	// sandbox that is neither released nor discarded.
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ReleasedHoldings{}, fmt.Errorf("registry reclaim_expired_holdings: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	released, err := tx.Exec(ctx, reclaimReleasedSQL, cluster)
	if err != nil {
		return ReleasedHoldings{}, fmt.Errorf("registry reclaim_expired_holdings: %w", err)
	}

	var discarded int64
	if breaker != nil {
		var candidates, total int64
		if err := tx.QueryRow(ctx, countReclaimDiscardableSQL, cluster).Scan(&candidates); err != nil {
			return ReleasedHoldings{}, fmt.Errorf("registry reclaim_expired_holdings: %w", err)
		}
		if err := tx.QueryRow(ctx, countClusterRowsSQL, cluster).Scan(&total); err != nil {
			return ReleasedHoldings{}, fmt.Errorf("registry reclaim_expired_holdings: %w", err)
		}
		if err := breaker.Allow(candidates, total); err != nil {
			// 🔴 The whole transaction is abandoned, releases included. The
			// releases are safe on their own, but a discard count this far out
			// of band says the premise underneath both halves is wrong, and
			// half of a pass nobody trusts is worse than none of it.
			return ReleasedHoldings{}, err
		}
	}

	tag, err := tx.Exec(ctx, reclaimDiscardedSQL, cluster)
	if err != nil {
		return ReleasedHoldings{}, fmt.Errorf("registry reclaim_expired_holdings: %w", err)
	}
	discarded = tag.RowsAffected()

	if err := tx.Commit(ctx); err != nil {
		return ReleasedHoldings{}, fmt.Errorf("registry reclaim_expired_holdings: %w", err)
	}

	out := ReleasedHoldings{Released: uint64(released.RowsAffected()), Discarded: uint64(discarded)}
	if out.Released > 0 || out.Discarded > 0 {
		registryReclaimReleased.Add(float64(out.Released))
		registryReclaimDiscarded.Add(float64(out.Discarded))
		s.log.Warn("reclaimed sandboxes that outlived their deadline on a node that stopped reporting; "+
			"released ones resume from their last published snapshot",
			zap.String("cluster_id", cluster),
			zap.Uint64("released", out.Released),
			zap.Uint64("discarded", out.Discarded),
		)
	}
	return out, nil
}

const releaseHoldingsReleasedSQL = `
UPDATE paused_sandboxes
   SET state = 'paused', claimed_by_node_id = NULL,
       generation = generation + 1, updated_at = now(),
       lease_expires_at = now()
 WHERE cluster_id = $1::uuid
   AND snapshot_id IS NOT NULL
   AND ` + liveHoldingsOfNode

const releaseHoldingsDiscardedSQL = `
DELETE FROM paused_sandboxes
 WHERE cluster_id = $1::uuid
   AND snapshot_id IS NULL
   AND ` + liveHoldingsOfNode

// ReleaseNodeHoldings frees the rows a previous process on this same machine
// was holding when it died.
//
// Recoverable rows go back to `paused` and can be resumed anywhere, including
// by this node, which is usually the one that picks them up again.
// lease_expires_at = now() rather than a fresh lease: `paused` means nobody
// holds it, and leaving a live-looking lease behind would only confuse the next
// reader.
//
// Rows without a snapshot are deleted: the sandbox was live, its local
// artifacts were consumed by the resume that started it, and nothing was ever
// published — there is nothing left to bring back, and a row that can never be
// claimed would just accumulate.
func (s *PostgresStore) ReleaseNodeHoldings(ctx context.Context, clusterID, nodeID string) (ReleasedHoldings, error) {
	cluster, err := requireUUID("cluster_id", clusterID)
	if err != nil {
		return ReleasedHoldings{}, err
	}
	if strings.TrimSpace(nodeID) == "" {
		return ReleasedHoldings{}, fmt.Errorf("%w: node_id is required", ErrInvalidArgument)
	}

	ctx, cancel := s.withTimeout(ctx)
	defer cancel()

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ReleasedHoldings{}, fmt.Errorf("registry release_node_holdings: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	released, err := tx.Exec(ctx, releaseHoldingsReleasedSQL, cluster, nodeID)
	if err != nil {
		return ReleasedHoldings{}, fmt.Errorf("registry release_node_holdings: %w", err)
	}
	discarded, err := tx.Exec(ctx, releaseHoldingsDiscardedSQL, cluster, nodeID)
	if err != nil {
		return ReleasedHoldings{}, fmt.Errorf("registry release_node_holdings: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return ReleasedHoldings{}, fmt.Errorf("registry release_node_holdings: %w", err)
	}

	out := ReleasedHoldings{Released: uint64(released.RowsAffected()), Discarded: uint64(discarded.RowsAffected())}
	if out.Released > 0 || out.Discarded > 0 {
		s.log.Warn("released sandboxes the previous process on this node was holding; "+
			"released ones resume from their last published snapshot, discarded ones never had one",
			zap.String("cluster_id", cluster),
			zap.String("node_id", nodeID),
			zap.Uint64("released", out.Released),
			zap.Uint64("discarded", out.Discarded),
		)
	}
	return out, nil
}

const removeSQL = `DELETE FROM paused_sandboxes
 WHERE sandbox_id = $1::uuid AND cluster_id = $2::uuid AND generation = $3`

// Remove deletes a row whose generation is still the one the caller quoted.
//
// 🔴 Conditional, and the condition is the point. The caller's guard used to be
// a read of the row beforehand — decide it is not live elsewhere, then delete —
// which is two statements with a window between them and the caller deciding.
// A node coming back from a partition, holding a view from before it left,
// takes exactly that path against a sandbox somebody else has since resumed;
// the delete matches, and the snapshot the other node is running from goes with
// it. Neither node reports anything.
//
// Quoting the generation closes the window without needing the caller to be
// right about anything: if the row moved since it was read, this matches
// nothing. A non-match is not an error — the row this caller meant to delete is
// already gone, which is what it wanted. e2b's catalog delete answers the same
// situation the same way, returning nil when the stored execution id is not the
// caller's.
func (s *PostgresStore) Remove(ctx context.Context, clusterID, sandboxID string, expectGeneration int64) (bool, error) {
	cluster, err := requireUUID("cluster_id", clusterID)
	if err != nil {
		return false, err
	}
	sandbox, err := requireUUID("sandbox_id", sandboxID)
	if err != nil {
		return false, err
	}

	ctx, cancel := s.withTimeout(ctx)
	defer cancel()

	tag, err := s.pool.Exec(ctx, removeSQL, sandbox, cluster, expectGeneration)
	if err != nil {
		return false, fmt.Errorf("registry remove: %w", err)
	}
	return tag.RowsAffected() > 0, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Decoding
// ─────────────────────────────────────────────────────────────────────────────

// scanEntry decodes one row of entryColumns.
//
// 🔴 Two rejections, both of which must stay rejections rather than becoming
// skips. A skipped row is a missing row, and a missing row is a delete order.
//
//   - An unrecognised state: the CHECK constraint pins five, so a sixth means
//     the table has moved on from this build and nothing here can say what the
//     row means.
//   - `paused` with no snapshot: the entry promises a cross-node resume it
//     cannot deliver.
//
// The metadata document is not one of them. It is copied out as bytes and never
// parsed: the node's own decoder is the only thing that knows what belongs in
// it, and a Go struct here would drop every field this build has not heard of —
// silently, because the Rust type does not reject unknown fields.
func scanEntry(rows pgx.Rows) (Entry, error) {
	var (
		entry           Entry
		state           string
		claimedByNodeID *string
		snapshotID      *string
		metadata        []byte
	)
	if err := rows.Scan(
		&entry.SandboxID,
		&entry.ClusterID,
		&state,
		&entry.Generation,
		&entry.OriginNodeID,
		&claimedByNodeID,
		&snapshotID,
		&metadata,
		&entry.PausedAt,
		&entry.UpdatedAt,
		&entry.LeaseExpiresAt,
		&entry.SandboxExpiresAt,
	); err != nil {
		return Entry{}, fmt.Errorf("decode registry row: %w", err)
	}

	parsed, ok := knownState(state)
	if !ok {
		return Entry{}, fmt.Errorf("%w: sandbox %s has unknown state %q", ErrInvalidRecord, entry.SandboxID, state)
	}
	entry.State = parsed
	if claimedByNodeID != nil {
		entry.ClaimedByNodeID = *claimedByNodeID
	}
	if snapshotID != nil {
		entry.SnapshotID = *snapshotID
	}
	if entry.Invalid() {
		return Entry{}, fmt.Errorf("%w: sandbox %s is paused but names no snapshot", ErrInvalidRecord, entry.SandboxID)
	}
	entry.Metadata = json.RawMessage(metadata)
	return entry, nil
}

// scanClaim decodes a claim's row: entryColumns followed by previous_state.
func scanClaim(rows pgx.Rows) (Entry, State, error) {
	var (
		entry           Entry
		state           string
		previous        string
		claimedByNodeID *string
		snapshotID      *string
		metadata        []byte
	)
	if err := rows.Scan(
		&entry.SandboxID,
		&entry.ClusterID,
		&state,
		&entry.Generation,
		&entry.OriginNodeID,
		&claimedByNodeID,
		&snapshotID,
		&metadata,
		&entry.PausedAt,
		&entry.UpdatedAt,
		&entry.LeaseExpiresAt,
		&entry.SandboxExpiresAt,
		&previous,
	); err != nil {
		return Entry{}, "", fmt.Errorf("decode registry row: %w", err)
	}

	parsed, ok := knownState(state)
	if !ok {
		return Entry{}, "", fmt.Errorf("%w: sandbox %s has unknown state %q", ErrInvalidRecord, entry.SandboxID, state)
	}
	entry.State = parsed
	previousState, ok := knownState(previous)
	if !ok {
		return Entry{}, "", fmt.Errorf("%w: sandbox %s has unknown previous state %q", ErrInvalidRecord, entry.SandboxID, previous)
	}
	if claimedByNodeID != nil {
		entry.ClaimedByNodeID = *claimedByNodeID
	}
	if snapshotID != nil {
		entry.SnapshotID = *snapshotID
	}
	if entry.Invalid() {
		return Entry{}, "", fmt.Errorf("%w: sandbox %s is paused but names no snapshot", ErrInvalidRecord, entry.SandboxID)
	}
	entry.Metadata = json.RawMessage(metadata)
	return entry, previousState, nil
}

// collectEntries drains a result set, failing the whole read on the first row
// it cannot decode.
func collectEntries(rows pgx.Rows) ([]Entry, error) {
	defer rows.Close()

	entries := make([]Entry, 0, 16)
	for rows.Next() {
		entry, err := scanEntry(rows)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read registry rows: %w", err)
	}
	return entries, nil
}

// knownState matches a column value against the five states exactly.
//
// Exactly, not case-insensitively like ParseState: that one resolves operator
// input, where being forgiving is a kindness. This one reads a column pinned by
// a CHECK constraint, and a value that differs from the five by so much as its
// case did not come from a build that agrees with this one about what the
// column means.
func knownState(raw string) (State, bool) {
	for _, state := range KnownStates() {
		if string(state) == raw {
			return state, true
		}
	}
	return "", false
}

// requireUUID rejects anything that is not a canonical hyphenated uuid.
//
// The check is here rather than left to PostgreSQL's cast so that a malformed
// id fails naming itself, instead of failing a whole batch with a cast error
// that names no id at all. It is deliberately stricter than PostgreSQL's own
// parser, which also accepts braced and unhyphenated forms: the node's ids come
// from a Uuid type whose Display is always canonical, so anything else on this
// path is a caller this build does not recognise.
func requireUUID(field, raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", fmt.Errorf("%w: %s is required", ErrInvalidArgument, field)
	}
	if !isCanonicalUUID(trimmed) {
		return "", fmt.Errorf("%w: %s %q is not a uuid", ErrInvalidArgument, field, raw)
	}
	return trimmed, nil
}

func isCanonicalUUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i := 0; i < 36; i++ {
		c := s[i]
		switch i {
		case 8, 13, 18, 23:
			if c != '-' {
				return false
			}
		default:
			isHex := (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
			if !isHex {
				return false
			}
		}
	}
	return true
}

// isJSONObject reports whether raw is a JSON document whose top level is an
// object.
//
// Decoding into map[string]json.RawMessage rather than `any` keeps this from
// being a licence to look further: it establishes the shape and stops, and the
// values stay as the bytes they arrived as.
func isJSONObject(raw json.RawMessage) bool {
	if len(bytes.TrimSpace(raw)) == 0 {
		return false
	}
	var probe map[string]json.RawMessage
	if err := json.Unmarshal(raw, &probe); err != nil {
		return false
	}
	// `null` unmarshals into a map without error, leaving it nil. It is the one
	// document that passes every cheaper check and still poisons the row.
	return probe != nil
}

// ErrInvalidArgument means the caller's own request was malformed — an id that
// is not a uuid, a missing cluster scope — as opposed to a row that is.
//
// It is separate from ErrInvalidRecord because the two want different answers:
// one is fixed by the caller, the other by an operator with a psql prompt.
var ErrInvalidArgument = errors.New("registry argument is invalid")
