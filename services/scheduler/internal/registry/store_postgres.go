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
       sandbox_expires_at       AS sandbox_expires_at,
       execution_id::text       AS execution_id,
       execution_started_at     AS execution_started_at`

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

	// beginPauseSQL and markRunningSQL are chosen once, at construction, from
	// two constants each. 🔴 Deliberately not one statement with an `OR $flag`
	// arm in it: a single statement covering both settings can never be tested
	// in either, and the planner decides in what order it evaluates the arm.
	// Two constants means the "off" behaviour is the statement this table had
	// before the identity axis, unchanged, and the tests it already has still
	// cover it.
	beginPauseSQL   string
	markRunningSQL  string
	executionFenced bool
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
	store := &PostgresStore{
		pool:            pool,
		leaseTTL:        leaseTTL,
		queryTimeout:    queryTimeout,
		log:             log,
		ownsPool:        true,
		beginPauseSQL:   beginPauseUnfencedSQL,
		markRunningSQL:  markRunningUnfencedSQL,
		executionFenced: cfg.WriteFencing,
	}
	if cfg.WriteFencing {
		store.beginPauseSQL = beginPauseFencedSQL
		store.markRunningSQL = markRunningFencedSQL
	}
	return store
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
	return fetchWith(ctx, s.pool, clusterID, sandboxID)
}

// rowQuerier is the sliver of pgxpool.Pool and pgx.Tx a read needs. It exists
// so a classification re-read can be made to run inside the transaction that
// wrote — the difference between "the row my statement did not match" and
// "whatever the table holds by now".
type rowQuerier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func fetchWith(ctx context.Context, q rowQuerier, clusterID, sandboxID string) (Entry, bool, error) {
	rows, err := q.Query(ctx, getSQL, sandboxID, clusterID)
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
// beginPauseFencedSQL adds one predicate to the upsert: the row has to already
// name the incarnation this pause belongs to.
//
// 🔴 What each arm of the WHERE keeps out:
//
//   - cluster_id (older): one cluster taking over another's row.
//   - execution_id (this release): a pause sent by an incarnation the cluster
//     has moved past. Three shapes reach it — a row a reclaim parked and
//     blanked (NULL, so the comparison is NULL and never matches), a row a
//     second node has since claimed and marked running under its own
//     incarnation, and a stale in-flight pause landing on a row whose next
//     incarnation is already up on the same machine.
//
// 🔴 The three-valued logic is the point, not an accident: every parked state
// carries NULL here, so no begin_pause can ever match a row the cluster
// believes nobody holds. That is fail-closed with nothing to remember.
//
// state is deliberately absent from the predicate. running→publishing and
// publishing→publishing (a retried upload) are both legitimate and both carry
// the same incarnation, so the identity axis judges this more precisely than
// the state would.
//
// execution_id and execution_started_at are absent from the update list on
// purpose: the predicate has already established they are equal, and writing
// them anyway would read as though a pause could change incarnations.
const beginPauseFencedSQL = `
WITH previous AS (
    SELECT snapshot_id FROM paused_sandboxes
     WHERE sandbox_id = $1::uuid AND cluster_id = $2::uuid
),
upserted AS (
    INSERT INTO paused_sandboxes (
        sandbox_id, cluster_id, state, generation, origin_node_id,
        claimed_by_node_id, snapshot_id, metadata, paused_at, updated_at,
        lease_expires_at, execution_id, execution_started_at
    )
    VALUES ($1::uuid, $2::uuid, 'publishing', 1, $3, NULL, NULL, $4::jsonb, now(), now(),
            now() + make_interval(secs => $5::double precision), $6::uuid, now())
    ON CONFLICT (sandbox_id) DO UPDATE SET
        state              = 'publishing',
        generation         = paused_sandboxes.generation + 1,
        origin_node_id     = EXCLUDED.origin_node_id,
        claimed_by_node_id = NULL,
        metadata           = EXCLUDED.metadata,
        paused_at          = EXCLUDED.paused_at,
        updated_at         = EXCLUDED.updated_at,
        lease_expires_at   = EXCLUDED.lease_expires_at
    WHERE paused_sandboxes.cluster_id   = EXCLUDED.cluster_id
      AND paused_sandboxes.execution_id = EXCLUDED.execution_id
    RETURNING generation
)
SELECT upserted.generation          AS generation,
       previous.snapshot_id::text   AS previous_snapshot_id
  FROM upserted
  LEFT JOIN previous ON TRUE`

// beginPauseUnfencedSQL is the statement this table had before the identity
// axis, with one addition it cannot do without: it installs execution_id
// instead of comparing it.
//
// 🔴 The addition is not a half-measure. The CHECK constraint is DDL and does
// not follow the setting, so a `publishing` row still has to name an
// incarnation — an upsert that left the column alone would move a parked row to
// `publishing` with a NULL in it and fail with 23514 every time. Switching the
// fencing off switches off the *checking*, never the column.
const beginPauseUnfencedSQL = `
WITH previous AS (
    SELECT snapshot_id FROM paused_sandboxes
     WHERE sandbox_id = $1::uuid AND cluster_id = $2::uuid
),
upserted AS (
    INSERT INTO paused_sandboxes (
        sandbox_id, cluster_id, state, generation, origin_node_id,
        claimed_by_node_id, snapshot_id, metadata, paused_at, updated_at,
        lease_expires_at, execution_id, execution_started_at
    )
    VALUES ($1::uuid, $2::uuid, 'publishing', 1, $3, NULL, NULL, $4::jsonb, now(), now(),
            now() + make_interval(secs => $5::double precision), $6::uuid, now())
    ON CONFLICT (sandbox_id) DO UPDATE SET
        state                = 'publishing',
        generation           = paused_sandboxes.generation + 1,
        origin_node_id       = EXCLUDED.origin_node_id,
        claimed_by_node_id   = NULL,
        metadata             = EXCLUDED.metadata,
        paused_at            = EXCLUDED.paused_at,
        updated_at           = EXCLUDED.updated_at,
        lease_expires_at     = EXCLUDED.lease_expires_at,
        execution_id         = EXCLUDED.execution_id,
        execution_started_at = EXCLUDED.execution_started_at
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

	execution, err := requireExecutionUUID(in.ExecutionID)
	if err != nil {
		return BeganPause{}, err
	}

	ctx, cancel := s.withTimeout(ctx)
	defer cancel()

	// 🔴 One transaction, because a zero-row upsert has to be classified and
	// the classification is a second statement. Read on another connection it
	// would see whatever a third party committed in between and could report
	// "another cluster owns this" about a row that was fenced, or the reverse.
	// The caller's response to the two is not the same — one is a bug report,
	// the other is "stop, you are dead" — so the classification has to see the
	// same table the write did. READ COMMITTED is enough: what is needed is
	// "the version my write did not match", not serialisability.
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return BeganPause{}, fmt.Errorf("registry begin_pause: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	var (
		generation       int64
		previousSnapshot *string
	)
	err = tx.QueryRow(ctx, s.beginPauseSQL,
		sandbox, cluster, in.OriginNodeID, []byte(in.Metadata), s.ttlSeconds(), execution,
	).Scan(&generation, &previousSnapshot)
	if errors.Is(err, pgx.ErrNoRows) {
		return BeganPause{}, s.classifyRefusedPause(ctx, tx, cluster, sandbox, execution)
	}
	if err != nil {
		return BeganPause{}, fmt.Errorf("registry begin_pause: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return BeganPause{}, fmt.Errorf("registry begin_pause: %w", err)
	}

	began := BeganPause{Generation: generation}
	if previousSnapshot != nil {
		began.PreviousSnapshotID = *previousSnapshot
	}
	return began, nil
}

// classifyRefusedPause says which predicate turned the upsert away.
//
// 🔴 The two answers are opposites and the caller acts on them differently: a
// row belonging to another cluster is a configuration fault nobody on the node
// can fix, while a fenced pause is this VM being told the cluster has moved on
// — stop, publish nothing, delete nothing. Reporting either as the other sends
// a node down the wrong path with no way to notice.
//
// It reads outside the cluster filter on purpose: the row it is asking about
// may be one this caller has no business seeing, and that is the fact being
// established.
func (s *PostgresStore) classifyRefusedPause(ctx context.Context, tx pgx.Tx, clusterID, sandboxID, executionID string) error {
	var (
		owner    string
		observed *string
	)
	err := tx.QueryRow(ctx,
		`SELECT cluster_id::text, execution_id::text FROM paused_sandboxes WHERE sandbox_id = $1::uuid`,
		sandboxID).Scan(&owner, &observed)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		// The row was there when the upsert ran — that is why it matched
		// nothing — and is gone now. Fenced, not retryable: the row this pause
		// meant to continue no longer exists, and re-sending would insert a
		// fresh one, resurrecting a sandbox somebody deleted.
		return fmt.Errorf("%w: sandbox %s has no registry row any more; incarnation %s is not the cluster's",
			ErrExecutionFenced, sandboxID, executionID)
	case err != nil:
		return fmt.Errorf("registry begin_pause: %w", err)
	}
	if !strings.EqualFold(owner, clusterID) {
		// Told rather than silently rewritten: the row belongs to somebody else.
		return fmt.Errorf("%w: registry already holds sandbox %s for a different cluster", ErrInvalidRecord, sandboxID)
	}
	return fmt.Errorf("%w: sandbox %s belongs to incarnation %s, not %s",
		ErrExecutionFenced, sandboxID, describeExecution(observed), executionID)
}

// describeExecution renders the incarnation column for an error message.
// "none" rather than an empty string, because the difference between "a
// different VM holds this" and "the cluster believes nobody does" is the whole
// reason the column is nullable.
func describeExecution(observed *string) string {
	if observed == nil {
		return "none"
	}
	return *observed
}

// 🔴 The two execution columns are cleared, and that is not tidying: the
// target state is `paused`, which the table's CHECK pins to carrying no
// incarnation. Leaving them would fail the statement with 23514.
//
// No execution predicate here, deliberately. This transition is guarded by the
// generation CAS, which begin_pause has already moved past for any stale
// incarnation, and every field made mandatory is another one a caller can
// forget to send. Worth revisiting when `publishing` becomes a long-lived
// retry state — a generation can then sit still for a long time — but not
// before.
const completePauseSQL = `
UPDATE paused_sandboxes
   SET state = 'paused', snapshot_id = $3::uuid, updated_at = now(),
       lease_expires_at = now() + make_interval(secs => $4::double precision),
       execution_id = NULL, execution_started_at = NULL
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

// The execution columns are cleared for the same reason as in
// completePauseSQL: `local_only` is a parked state and the CHECK pins parked
// rows to carrying no incarnation.
const markLocalOnlySQL = `
UPDATE paused_sandboxes
   SET state = 'local_only', updated_at = now(),
       lease_expires_at = now() + make_interval(secs => $3::double precision),
       execution_id = NULL, execution_started_at = NULL
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
           execution_id = $5::uuid, execution_started_at = now(),
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
           execution_id = $5::uuid, execution_started_at = now(),
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
func (s *PostgresStore) ClaimForResume(ctx context.Context, clusterID, sandboxID, nodeID, executionID string) (ResumeClaim, error) {
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
	execution, err := requireExecutionUUID(executionID)
	if err != nil {
		return ResumeClaim{}, err
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

	rows, err := s.pool.Query(ctx, claimSQL, sandbox, nodeID, s.ttlSeconds(), cluster, execution)
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

// 🔴 One of the three paths that take a sandbox away from whoever held it,
// and every one of them has to blank the identity axis. A `paused` row still
// carrying an incarnation is a row the old node's next automatic pause matches
// — which is the first step of the sequence this release exists to break, and
// it would be back with the fencing predicates still in place and doing
// nothing.
//
// No execution predicate, for the same reason: this is a seizure, and a
// seizure that needs the consent of whoever is being seized is not one.
const releaseClaimSQL = `
UPDATE paused_sandboxes
   SET state = 'paused', claimed_by_node_id = NULL, updated_at = now(),
       lease_expires_at = now() + make_interval(secs => $3::double precision),
       execution_id = NULL, execution_started_at = NULL
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

// 🔴 sandbox_expires_at is written here, not left to the first renewal.
//
// Reclamation needs a lapsed lease *and* a passed deadline, and NULL is not a
// passed deadline — it never matches, at any point in the future. Until D11 the
// column was written only by renew_lease, so every row spent its first
// reconcile interval carrying none, and a node lost inside that window left a
// row that could not be reclaimed, claimed or removed by anything. e2b has no
// equivalent gap because its catalog carries the expiry on every write.
// markRunningFencedSQL replaces the old claim guard with three explicit ways a
// node may say a sandbox is live on it, and no fourth.
//
//	① a cross-node resume this node holds the claim on, running under the
//	   incarnation the claim allocated. The incarnation clause is what makes
//	   this a check rather than an installation, and it is the contract with the
//	   node: it must start the VM under the value AcquireSandbox handed back.
//	② a sandbox parked on this node's own disk, woken in place. No claim was
//	   ever taken, so this is where an incarnation gets installed. Guarded on
//	   origin and on the row not being claimed by anybody.
//	③ the same incarnation saying so twice, which is what a retried RPC looks
//	   like.
//
// 🔴 What the old guard let through: `running` rows carry a NULL
// claimed_by_node_id, so `claimed_by_node_id IS NULL OR = $2` was true for
// every live row in the cluster — one data-plane request against the wrong node
// could repoint a sandbox that was running perfectly well somewhere else.
// Branch ③ now requires the incarnation to match, and `running` is absent from
// branch ②, so neither reaches a row belonging to another VM.
//
// 🔴 Two identities, not one, as of this fix. $2 is the claimant — the identity
// claim_for_resume was called under, node-scoped mutual exclusion, compared in
// every branch below exactly as it always was. $7 is the holder — the real
// machine the sandbox is running on — and it is written, never compared,
// except in branch ③ (see the note on that branch for why it is the one
// exception).
//
// 🔴 execution_started_at is NOT re-stamped on branch ③, which is a deliberate
// departure from the design's literal statement (`_design-phase3-scheduler.md`
// §3.3, corrected there under this adjudication). The whole reason the column
// exists is that updated_at is refreshed by every lease renewal and therefore
// cannot be the origin of the grace period B3's KillOrphan measures (§2.5).
// Branch ③ is the retried RPC: the predicate has already established that this
// is the same incarnation, so moving its start would put back exactly the drift
// the column was added to escape — the grace would restart on every retry, and
// an orphan that keeps retrying would never age out of it. Today mark_running
// is not periodic, so the effect is invisible; B3 is where it would bite, and by
// then nothing points back to this line. Branches ① and ② still stamp: ② is the
// installation point, and ① is the moment the VM the claim pre-allocated
// actually starts, which is a later and more accurate origin than the claim.
//
// 🔴 The unfenced statement carries the same CASE, and deliberately: this is a
// property of the column, not a feature of fencing. See markRunningUnfencedSQL
// for the adjudication — an invariant that held only in one position of the
// switch would make what the column means depend on where the switch stood.
//
// The CASE reads the pre-update row (in an UPDATE, SET expressions see the old
// values), and `state = 'running'` identifies branch ③ uniquely — branch ②
// excludes `running` and branch ① requires `resuming`. The execution_id clause
// is redundant against the WHERE and kept anyway so the expression states its
// own precondition instead of borrowing one from thirty lines below.
//
// 🔴 Branch ③ compares against $7 (holder), not $2 (claimant) — the one place
// this statement reads the holder instead of only writing it. Two callers on
// the node side write the same running row twice on an ordinary successful
// resume (the orchestrator's own mark_running, and the resume surface's
// idempotent follow-up once a claim was taken — see
// `PausedSandboxCoordinator::mark_sandbox_running`'s callers), and by the time
// the second call lands, branch ① or ② has already repointed origin_node_id at
// the holder. A retry guarded on the claimant would then never match whenever
// claimant and holder differ — every cross-node resume on the api half — and
// mark_running would answer held_elsewhere about a row this call itself just
// wrote, on every single one of them. Comparing against the holder is safe
// specifically *because* branch ③ never installs an incarnation on its own —
// execution_id is still the fencing token that decides whether a row may be
// touched at all, exactly as it is for every other write on this interface;
// this clause only decides whether a call that has already cleared that fence
// recognises the row as the one it (or its own retry) just wrote.
const markRunningFencedSQL = `
UPDATE paused_sandboxes
   SET state = 'running', origin_node_id = $7, claimed_by_node_id = NULL,
       execution_id = $6::uuid,
       execution_started_at = CASE
           WHEN state = 'running' AND execution_id = $6::uuid
                THEN execution_started_at
           ELSE now()
       END,
       generation = generation + 1, updated_at = now(),
       lease_expires_at = now() + make_interval(secs => $3::double precision),
       sandbox_expires_at = $5
 WHERE sandbox_id = $1::uuid
   AND cluster_id = $4::uuid
   AND (
         (state = 'resuming' AND claimed_by_node_id = $2
                             AND execution_id = $6::uuid)
      OR (state IN ('paused', 'publishing', 'local_only')
                             AND origin_node_id = $2
                             AND claimed_by_node_id IS NULL)
      OR (state = 'running'  AND origin_node_id = $7
                             AND execution_id = $6::uuid)
       )`

// markRunningUnfencedSQL is the guard this statement had before the identity
// axis, plus the column write the CHECK constraint requires of any row moving
// to `running`. See beginPauseUnfencedSQL: the setting turns the checking off,
// never the column.
//
// 🔴 The CASE is the same one the fenced statement carries, and it is here on
// purpose (adjudicated 2026-08-20).
//
// The earlier reading was that this statement needed no counterpart to branch
// ③, because it has no branches to tell a retry from a takeover and switching
// fencing off is precisely giving up that distinction — so a row whose
// incarnation anybody may overwrite has no start time worth preserving. ✅ 已裁决
// against: what execution_started_at means is a property of the column, not a
// feature of fencing. If it moved whenever the setting was off, the origin of
// the grace period B3 measures would depend on which position the switch was in
// when the row was last written, and one flip of that switch would silently
// change what the data means for rows nobody touched afterwards. What
// `write_fencing` is allowed to turn off is the *predicate* — whether a write is
// refused — never what a column records.
//
// The condition is decidable without any predicate: `state = 'running' AND
// execution_id = $6::uuid` reads the pre-update row and says "this row already
// names the incarnation the caller is declaring", which is a retry under either
// setting. A takeover names a different incarnation and still stamps.
//
// 🔴 $7 (holder) only in the SET, same as the fenced statement — see that one's
// note on the two identities. This statement's guard has no branch ③ to speak
// of (fencing is off, so there is nothing here for a retry to fail against in
// the first place: `claimed_by_node_id IS NULL OR = $2` matches a running row
// unconditionally), so unlike the fenced statement there is no comparison to
// redirect onto the holder here at all.
const markRunningUnfencedSQL = `
UPDATE paused_sandboxes
   SET state = 'running', origin_node_id = $7, claimed_by_node_id = NULL,
       execution_id = $6::uuid,
       execution_started_at = CASE
           WHEN state = 'running' AND execution_id = $6::uuid
                THEN execution_started_at
           ELSE now()
       END,
       generation = generation + 1, updated_at = now(),
       lease_expires_at = now() + make_interval(secs => $3::double precision),
       sandbox_expires_at = $5
 WHERE sandbox_id = $1::uuid
   AND cluster_id = $4::uuid
   AND (claimed_by_node_id IS NULL OR claimed_by_node_id = $2)`

// MarkRunning records that a sandbox is live, claimed under nodeID and
// physically running on holderNodeID.
//
// 🔴 Two identities, two jobs — see markRunningFencedSQL for the long version.
// nodeID is the CAS guard, compared against every branch of the statement; it
// must be the exact identity ClaimForResume was called under for this resume
// (or, on a local reopen with no claim taken, the caller's own identity —
// nodeID and holderNodeID are then the same value, which is also what every
// pre-split caller sent and is why that shape stays correct unchanged).
// holderNodeID is a plain write into origin_node_id, participating in no
// comparison except the one noted on branch ③ above.
//
// holderNodeID empty means "same as nodeID" — an older node built before this
// axis existed, or a caller with nothing more precise to say. That is exactly
// nodeID's own pre-split job, so falling back to it here is not a degraded case,
// it is what every non-cross-node caller has always done.
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
func (s *PostgresStore) MarkRunning(ctx context.Context, clusterID, sandboxID, nodeID, holderNodeID, executionID string, expiresAt *time.Time) (MarkRunningOutcome, error) {
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
	holder := strings.TrimSpace(holderNodeID)
	if holder == "" {
		holder = nodeID
	}
	execution, err := requireExecutionUUID(executionID)
	if err != nil {
		return MarkRunningUntracked, err
	}

	ctx, cancel := s.withTimeout(ctx)
	defer cancel()

	// 🔴 One transaction around the write and the re-read that classifies it.
	// The classification used to run on another pooled connection, which was a
	// benign race while it only decided a log line; it now decides whether the
	// caller retries or stops for good, and a classification made against a
	// version of the row this statement never saw can send a node either way
	// for no reason.
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return MarkRunningUntracked, fmt.Errorf("registry mark_running: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	tag, err := tx.Exec(ctx, s.markRunningSQL, sandbox, nodeID, s.ttlSeconds(), cluster, expiresAt, execution, holder)
	if err != nil {
		return MarkRunningUntracked, fmt.Errorf("registry mark_running: %w", err)
	}
	if tag.RowsAffected() > 0 {
		if err := tx.Commit(ctx); err != nil {
			return MarkRunningUntracked, fmt.Errorf("registry mark_running: %w", err)
		}
		return MarkRunningAdopted, nil
	}

	// Nothing matched. Three possibilities now, not two: the cluster does not
	// track this sandbox (by far the common case, and correct), somebody else
	// holds it, or this node holds it under an incarnation that is no longer
	// the one on the row.
	//
	// The re-read's error is propagated rather than folded into an outcome:
	// "untracked" tells the caller the registry has no say over this sandbox,
	// and a row that exists but could not be decoded is not that.
	//
	// 🔴 The re-read is also the whole reason this returns more than one
	// answer. It happens here now, so a caller across the wire never sees it;
	// before phase 2 the node did it itself and could act on what it found.
	entry, found, err := fetchWith(ctx, tx, cluster, sandbox)
	if err != nil {
		return MarkRunningUntracked, err
	}
	if !found {
		return MarkRunningUntracked, nil
	}

	// Read straight off the statement above: branches ① and ③ are the only two
	// carrying an incarnation clause, so a row eligible for either can have
	// failed on nothing else. Branch ② has no such clause — if it were eligible
	// the UPDATE would have matched.
	//
	// 🔴 Branch ③'s half of this compares against holder, not nodeID, mirroring
	// which identity that branch's WHERE clause reads.
	staleIncarnation := (entry.State == StateResuming && entry.ClaimedByNodeID == nodeID) ||
		(entry.State == StateRunning && entry.OriginNodeID == holder)
	if staleIncarnation {
		registryMarkRunningRefused.Inc()
		s.log.Warn("refused to mark the sandbox running here: this node's incarnation is not the one on the row",
			zap.String("sandbox_id", sandbox),
			zap.String("node_id", nodeID),
			zap.String("holder_node_id", holder),
			zap.String("expected_execution_id", entry.ExecutionID),
			zap.String("observed_execution_id", execution),
			zap.String("fencing_stage", "registry_write"),
		)
		return MarkRunningUntracked, fmt.Errorf("%w: sandbox %s belongs to incarnation %s, not %s",
			ErrExecutionFenced, sandbox, entry.ExecutionID, execution)
	}

	registryMarkRunningRefused.Inc()
	s.log.Warn("refused to mark the sandbox running here: another node holds the resume claim",
		zap.String("sandbox_id", sandbox),
		zap.String("node_id", nodeID),
		zap.String("holder_node_id", holder),
		zap.String("claimed_by", entry.ClaimedByNodeID),
	)
	return MarkRunningHeldElsewhere, nil
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

// renewParkedLeaseSQL is renewLeaseSQL's narrower sibling for the
// scheduler's own heartbeat-driven reconciliation.
//
// Three differences from renewLeaseSQL, each load-bearing:
//
//  1. sandbox_expires_at is absent from the SET list entirely — not even
//     re-read and rewritten to its own value. That deadline is the API
//     half's authority; a heartbeat proves only that a node still has a
//     sandbox's bytes, and this statement must not be the place a stale or
//     absent opinion about the deadline leaks into the row.
//  2. `running` and `resuming` are absent from the state list. Those rows'
//     leases lapsing invites no takeover on a timer (see liveLeaseLapsed),
//     and this statement exists only for the risk on the other two states —
//     a takeover that rewinds a snapshot.
//  3. The (sandbox, node) pairs are a caller-supplied list, not one node's
//     own roster under its own identity — see ParkedLeaseHolder. The WHERE
//     clause is what turns that from a bare assertion into a check: a pair
//     whose node does not match the row's own origin_node_id renews nothing,
//     exactly as renewLeaseSQL's own comment describes for its single-node
//     form.
//
// `paused` stays absent for the reason renewLeaseSQL gives: nobody holds it,
// so there is nothing to keep alive.
const renewParkedLeaseSQL = `
UPDATE paused_sandboxes AS p
   SET lease_expires_at = now() + make_interval(secs => $1::double precision),
       updated_at       = now()
  FROM (SELECT unnest($3::uuid[]) AS sandbox_id,
               unnest($4::text[]) AS node_id) AS v
 WHERE p.sandbox_id = v.sandbox_id
   AND p.cluster_id = $2::uuid
   AND p.state IN ('publishing', 'local_only')
   AND p.origin_node_id = v.node_id`

// RenewParkedLeases implements Store.
//
// The lease length is this store's own — like RenewLease's, but here there is
// no caller-reported TTL to prefer, because the caller is not the node that
// set one: it is the scheduler's own reconciliation, working from a roster
// that carries no lease length at all.
func (s *PostgresStore) RenewParkedLeases(ctx context.Context, clusterID string, holders []ParkedLeaseHolder) (uint64, error) {
	cluster, err := requireUUID("cluster_id", clusterID)
	if err != nil {
		return 0, err
	}
	if len(holders) == 0 {
		// Nothing asserted, so nothing is asked — matches RenewLease's own
		// empty-input handling.
		return 0, nil
	}

	ids := make([]string, 0, len(holders))
	nodeIDs := make([]string, 0, len(holders))
	for _, h := range holders {
		id, err := requireUUID("sandbox_id", h.SandboxID)
		if err != nil {
			return 0, err
		}
		if strings.TrimSpace(h.NodeID) == "" {
			return 0, fmt.Errorf("%w: node_id is required", ErrInvalidArgument)
		}
		ids = append(ids, id)
		nodeIDs = append(nodeIDs, h.NodeID)
	}

	ctx, cancel := s.withTimeout(ctx)
	defer cancel()

	tag, err := s.pool.Exec(ctx, renewParkedLeaseSQL, s.ttlSeconds(), cluster, ids, nodeIDs)
	if err != nil {
		return 0, fmt.Errorf("registry renew_parked_leases: %w", err)
	}
	return uint64(tag.RowsAffected()), nil
}

// renewLiveLeaseSQL is renewParkedLeaseSQL's sibling for `running` rows.
//
// Same shape, same three load-bearing properties (see renewParkedLeaseSQL's
// own comment): sandbox_expires_at stays untouched — that deadline is the API
// half's to set, not a fact a heartbeat proves anything about — the
// (sandbox, node) pairs are caller-asserted and re-checked against the row's
// own origin_node_id rather than trusted, and `running` is the only state
// this statement will touch.
//
// updated_at *is* written, deliberately unlike leaving it alone: this is the
// one write on this row that only ever happens when the row's own holder has
// a fresh heartbeat roster that still lists the sandbox (see
// registryReconcileResult.liveLeaseRenewals), so bumping it here is a
// confirmation earned the same way MarkRunning's own updated_at write is —
// unlike a write sourced from the api half's cluster-wide metadata view,
// which proves nothing about whether the node behind origin_node_id is still
// reachable and must never be allowed to feed reconcile.go's ghost detection
// a false "recently confirmed".
const renewLiveLeaseSQL = `
UPDATE paused_sandboxes AS p
   SET lease_expires_at = now() + make_interval(secs => $1::double precision),
       updated_at       = now()
  FROM (SELECT unnest($3::uuid[]) AS sandbox_id,
               unnest($4::text[]) AS node_id) AS v
 WHERE p.sandbox_id = v.sandbox_id
   AND p.cluster_id = $2::uuid
   AND p.state = 'running'
   AND p.origin_node_id = v.node_id`

// RenewLiveLeases implements Store.
//
// The lease length is this store's own, the same reasoning as
// RenewParkedLeases: the caller here is the scheduler's own reconciliation,
// working from a roster that carries no lease length of its own.
func (s *PostgresStore) RenewLiveLeases(ctx context.Context, clusterID string, holders []ParkedLeaseHolder) (uint64, error) {
	cluster, err := requireUUID("cluster_id", clusterID)
	if err != nil {
		return 0, err
	}
	if len(holders) == 0 {
		// Nothing asserted, so nothing is asked — matches RenewParkedLeases'
		// own empty-input handling.
		return 0, nil
	}

	ids := make([]string, 0, len(holders))
	nodeIDs := make([]string, 0, len(holders))
	for _, h := range holders {
		id, err := requireUUID("sandbox_id", h.SandboxID)
		if err != nil {
			return 0, err
		}
		if strings.TrimSpace(h.NodeID) == "" {
			return 0, fmt.Errorf("%w: node_id is required", ErrInvalidArgument)
		}
		ids = append(ids, id)
		nodeIDs = append(nodeIDs, h.NodeID)
	}

	ctx, cancel := s.withTimeout(ctx)
	defer cancel()

	tag, err := s.pool.Exec(ctx, renewLiveLeaseSQL, s.ttlSeconds(), cluster, ids, nodeIDs)
	if err != nil {
		return 0, fmt.Errorf("registry renew_live_leases: %w", err)
	}
	return uint64(tag.RowsAffected()), nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Reclamation
// ─────────────────────────────────────────────────────────────────────────────

// reclaimReleasedRunningSQL, reclaimReleasedResumingSQL and
// reclaimDiscardedSQL are the cluster's backstop for the three ways a live row
// can be abandoned.
//
// The lease alone is what ClaimForResume refuses to act on: it cannot tell a
// dead node from a partitioned one, and acting on it duplicates live sandboxes.
// Every one of the three statements below requires it regardless.
//
// 🔴 `running` and `resuming` no longer share one condition, and that split is
// the fix for a first-resume claim that never completes (a node claims a
// sandbox, dies before its mark_running lands, and the api replica that made
// the claim is a Kubernetes Deployment pod that is never coming back under
// that name — see ClaimForResume's own doc on why release_stale_node_holdings
// cannot reach a `resuming` row by identity in that split). claim_for_resume
// never writes sandbox_expires_at, so such a row's column is NULL from the
// moment begin_pause first parked the sandbox, and `NULL < now()` never
// matches — a resuming row stuck this way used to sit in the table forever,
// unclaimable (claim_for_resume refuses live rows outright) and unreleased
// (nothing else releases them), fixable only by an operator's UPDATE.
//
// `running` keeps needing both conditions, and the reasoning is what it
// always was: sandbox_expires_at < now() alone would race the node's own
// eviction (a reachable node evicts its own expired sandboxes properly,
// which is the better outcome), so the cluster only steps in once nobody has
// renewed for a full lease *and* the sandbox has outlived the deadline its
// own user set. A NULL sandbox_expires_at never matches for `running`, which
// covers both a sandbox asked never to expire and a row whose holder has not
// renewed since the column existed — the safe answer for a row that is
// genuinely still wanted.
//
// `resuming` is a different kind of row and does not need the second
// condition at all. It has no owner-set deadline of its own to outlive — it
// is a claim in flight, not a sandbox somebody asked to keep running — and a
// claim whose lease has lapsed is, by the lease's own definition, one nobody
// is still working on. Requiring a second, structurally-often-NULL condition
// on top of that only ever makes the release *more* conservative than the
// state machine already is elsewhere: claim_for_resume itself asks nothing
// but a lapsed lease before taking a `publishing`/`local_only` row from its
// origin (see claimForResumeSQL). Releasing a stuck `resuming` row the same
// way — on a lapsed lease alone — is that same rule applied to the row this
// backstop exists to protect from becoming permanently unclaimable.
//
// 🔴 execution_id = NULL is the second of the three seizure paths, and the
// one the sequence in the design starts from: reclaim parks a live row, the
// node that was running it comes back, its one-second eviction timer fires,
// and its begin_pause lands on the row. Blanked, that pause compares against
// NULL and never matches. Left in place, it matches perfectly and the old
// incarnation publishes a stale snapshot over the new one's. Both statements
// below carry it for the same reason.
const reclaimReleasedRunningSQL = `
UPDATE paused_sandboxes
   SET state = 'paused', claimed_by_node_id = NULL,
       execution_id = NULL, execution_started_at = NULL,
       generation = generation + 1, updated_at = now(),
       lease_expires_at = now()
 WHERE cluster_id = $1::uuid
   AND snapshot_id IS NOT NULL
   AND state = 'running'
   AND ` + leaseExpired + `
   AND sandbox_expires_at < now()`

// reclaimReleasedResumingSQL is reclaimReleasedRunningSQL's sibling for
// `resuming`, deliberately without the sandbox_expires_at arm — see the
// shared doc above.
//
// snapshot_id IS NOT NULL is kept even though it is not load-bearing today:
// claim_for_resume's own WHERE requires a non-null snapshot_id before a row
// can ever become `resuming` in the first place, so no resuming row can fail
// this check. Stated anyway so this statement's own precondition does not
// depend on a reader having claimForResumeSQL open in another tab, and so a
// future change to the claim path that weakens that guarantee fails loudly
// here instead of silently discarding a resumable row through the wrong
// branch.
const reclaimReleasedResumingSQL = `
UPDATE paused_sandboxes
   SET state = 'paused', claimed_by_node_id = NULL,
       execution_id = NULL, execution_started_at = NULL,
       generation = generation + 1, updated_at = now(),
       lease_expires_at = now()
 WHERE cluster_id = $1::uuid
   AND snapshot_id IS NOT NULL
   AND state = 'resuming'
   AND ` + leaseExpired

// reclaimDiscardedSQL is untouched by the running/resuming split above: its
// `resuming` arm is already unreachable, for the same reason
// reclaimReleasedResumingSQL's snapshot_id check is redundant — claim_for_resume
// requires snapshot_id IS NOT NULL, so no resuming row can ever match this
// statement's own snapshot_id IS NULL. Splitting it would add a second
// statement with nothing left for either half to match.
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

	releasedRunning, err := tx.Exec(ctx, reclaimReleasedRunningSQL, cluster)
	if err != nil {
		return ReleasedHoldings{}, fmt.Errorf("registry reclaim_expired_holdings: %w", err)
	}
	releasedResuming, err := tx.Exec(ctx, reclaimReleasedResumingSQL, cluster)
	if err != nil {
		return ReleasedHoldings{}, fmt.Errorf("registry reclaim_expired_holdings: %w", err)
	}
	released := releasedRunning.RowsAffected() + releasedResuming.RowsAffected()

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

	out := ReleasedHoldings{Released: uint64(released), Discarded: uint64(discarded)}
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

// The third seizure path, and it blanks the identity axis for the same reason
// reclaimReleasedRunningSQL does — see the note there.
const releaseHoldingsReleasedSQL = `
UPDATE paused_sandboxes
   SET state = 'paused', claimed_by_node_id = NULL,
       execution_id = NULL, execution_started_at = NULL,
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
		executionID     *string
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
		&executionID,
		&entry.ExecutionStartedAt,
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
	if executionID != nil {
		entry.ExecutionID = *executionID
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
		executionID     *string
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
		&executionID,
		&entry.ExecutionStartedAt,
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
	if executionID != nil {
		entry.ExecutionID = *executionID
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

// requireExecutionUUID is requireUUID plus the lower-casing the arbitration
// downstream depends on.
//
// 🔴 isCanonicalUUID accepts upper-case hex, and the routing side orders these
// ids lexicographically to decide which of two incarnations is newer — and in
// ASCII '0'-'9' < 'A'-'F' < 'a'-'f', so one upper-case id reverses that order.
// The column is a uuid so PostgreSQL normalises whatever it stores; this
// normalises the copy that travels back out in an entry and in an error
// message, which is the copy the comparison actually sees.
func requireExecutionUUID(raw string) (string, error) {
	id, err := requireUUID("execution_id", raw)
	if err != nil {
		return "", err
	}
	return strings.ToLower(id), nil
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
