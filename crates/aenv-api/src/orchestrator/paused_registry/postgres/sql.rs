//! SQL statement text, ported verbatim (including load-bearing comments)
//! from `services/scheduler/internal/registry/store_postgres.go`.
//!
//! 🔴 Only the **fenced** variants are ported. Go keeps an `Unfenced` sibling
//! of `beginPauseSQL`/`markRunningSQL` for `WriteFencing = false` -- its own
//! rollback path for a live fleet that had never shipped the identity axis.
//! This backend has no such history: it is a new implementation, never
//! deployed without the identity axis, so there is nothing to roll back to
//! and no unfenced variant to carry. Per the task's own D2/D3: implement the
//! final, fixed form directly rather than the history that led to it.
//!
//! `$n` placeholders are PostgreSQL's native positional parameters -- pgx and
//! sqlx both bind them the same way, so every statement below is copied
//! character-for-character from its Go source, casts included, with only the
//! Unfenced branches and the read-only-reader (`postgres.go`) surface
//! dropped (this process is a legitimate writer; see the plan doc's own §7
//! point 1 on why `SET default_transaction_read_only` does not port).

/// `leaseExpired` (`store_postgres.go:45-51`), verbatim.
pub const LEASE_EXPIRED: &str = "COALESCE(lease_expires_at, updated_at) < now()";

/// `liveHoldingsOfNode` (`store_postgres.go:53-59`), verbatim. `$2` is the
/// node id in every statement that embeds this.
pub const LIVE_HOLDINGS_OF_NODE: &str = "((state = 'running'  AND origin_node_id     = $2)
              OR (state = 'resuming' AND claimed_by_node_id = $2))";

/// The 14 columns the write path's row type (`PausedSandboxEntry` +
/// the reconcile-only lease/execution-start fields) is built from --
/// `entryColumns` (`store_postgres.go:17-43`), verbatim.
pub const ENTRY_COLUMNS: &str = "sandbox_id::text         AS sandbox_id,
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
       execution_started_at     AS execution_started_at";

pub fn get_sql() -> String {
    format!(
        "SELECT {ENTRY_COLUMNS}
  FROM paused_sandboxes
 WHERE sandbox_id = $1::uuid AND cluster_id = $2::uuid"
    )
}

pub fn get_many_sql() -> String {
    format!(
        "SELECT {ENTRY_COLUMNS}
  FROM paused_sandboxes
 WHERE cluster_id = $1::uuid AND sandbox_id = ANY($2::uuid[])"
    )
}

/// [`super::reads::list_all`]'s row query -- the `ListRegistrySandboxes`
/// listing, ported from Go's `PostgresReader.List` (`postgres.go:135-183`,
/// the `selectColumns`/`WHERE cluster_id = $1` half; the `SELECT now()`
/// half of that same read-only transaction is issued separately, see that
/// function's own doc for why one transaction still covers both). No
/// `ORDER BY`: [`super::reads::list_all`]'s caller
/// (`src/node_registry/grpc_service.rs::list_registry_sandboxes`) sorts
/// after filtering, the same layering Go's own `listRegistrySandboxes`
/// uses (`service.go:915-918`) rather than this query's.
pub fn list_all_sql() -> String {
    format!(
        "SELECT {ENTRY_COLUMNS}
  FROM paused_sandboxes
 WHERE cluster_id = $1::uuid"
    )
}

/// `beginPauseFencedSQL` (`store_postgres.go:402-468`), verbatim modulo the
/// timestamp binding: Go relies on the driver's own array/uuid casts and
/// sqlx binds the same way, so no adaptation was needed beyond formatting.
///
/// 🔴 What each arm of the WHERE keeps out: `cluster_id` (one cluster taking
/// over another's row) and `execution_id` (a pause sent by an incarnation
/// the cluster has moved past -- a reclaimed and blanked row, a row a second
/// node has since claimed and marked running under its own incarnation, or a
/// stale in-flight pause landing on a row whose next incarnation is already
/// up on the same machine). Every parked state carries a NULL `execution_id`,
/// so no `begin_pause` can ever match a row the cluster believes nobody
/// holds -- fail-closed with nothing to remember. `state` is deliberately
/// absent from the predicate: `running`->`publishing` and
/// `publishing`->`publishing` (a retried upload) are both legitimate and
/// both carry the same incarnation, so the identity axis judges this more
/// precisely than the state would.
pub const BEGIN_PAUSE_SQL: &str = "
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
  LEFT JOIN previous ON TRUE";

/// A `begin_pause` whose upsert matched zero rows: re-read to classify why
/// -- `classifyRefusedPause`'s query (`store_postgres.go:615-617`), verbatim.
pub const CLASSIFY_REFUSED_PAUSE_SQL: &str =
    "SELECT cluster_id::text, execution_id::text FROM paused_sandboxes WHERE sandbox_id = $1::uuid";

/// `completePauseSQL` (`store_postgres.go:648-664`), verbatim. The two
/// execution columns are cleared: the target state is `paused`, which the
/// table's CHECK pins to carrying no incarnation.
pub const COMPLETE_PAUSE_SQL: &str = "
UPDATE paused_sandboxes
   SET state = 'paused', snapshot_id = $3::uuid, updated_at = now(),
       lease_expires_at = now() + make_interval(secs => $4::double precision),
       execution_id = NULL, execution_started_at = NULL
 WHERE sandbox_id = $1::uuid AND cluster_id = $5::uuid
   AND generation = $2 AND state = 'publishing'";

/// `markLocalOnlySQL` (`store_postgres.go:694-703`), verbatim.
pub const MARK_LOCAL_ONLY_SQL: &str = "
UPDATE paused_sandboxes
   SET state = 'local_only', updated_at = now(),
       lease_expires_at = now() + make_interval(secs => $3::double precision),
       execution_id = NULL, execution_started_at = NULL
 WHERE sandbox_id = $1::uuid AND cluster_id = $4::uuid
   AND generation = $2 AND state = 'publishing'";

/// `claimForResumeSQL` (`store_postgres.go:743-789`), verbatim: the
/// three-way test. A `paused` row is free for the taking (no lease check --
/// nobody holds it). `publishing`/`local_only` are claimable once their
/// lease has lapsed (a degraded takeover -- see [`super::grace`]). `running`
/// and `resuming` are never claimable here, however long the lease has
/// lapsed: a lapsed lease says only that the holder cannot reach this
/// database, which a partitioned-but-still-serving node satisfies exactly as
/// well as a dead one.
pub fn claim_for_resume_sql() -> String {
    format!(
        "
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
         OR (state IN ('publishing', 'local_only') AND {LEASE_EXPIRED}))
    RETURNING {ENTRY_COLUMNS}
)
SELECT claimed.*, previous.previous_state
  FROM claimed JOIN previous ON TRUE"
    )
}

/// `claimForResumeDurableOnlySQL` (`store_postgres.go:791-813`), verbatim:
/// `claim_for_resume_sql` without its lapsed-lease arm -- used while the
/// cluster's restart grace window is still open (see [`super::grace`]).
pub fn claim_for_resume_durable_only_sql() -> String {
    format!(
        "
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
    RETURNING {ENTRY_COLUMNS}
)
SELECT claimed.*, previous.previous_state
  FROM claimed JOIN previous ON TRUE"
    )
}

/// `releaseClaimSQL` (`store_postgres.go:971-986`), verbatim: a seizure, so
/// no execution predicate -- this is not asking the incumbent's consent.
pub const RELEASE_CLAIM_SQL: &str = "
UPDATE paused_sandboxes
   SET state = 'paused', claimed_by_node_id = NULL, updated_at = now(),
       lease_expires_at = now() + make_interval(secs => $3::double precision),
       execution_id = NULL, execution_started_at = NULL
 WHERE sandbox_id = $1::uuid AND cluster_id = $4::uuid
   AND generation = $2 AND state = 'resuming'";

/// `markRunningFencedSQL` (`store_postgres.go:1023-1196`), verbatim. See D3
/// in the task brief / this crate's own report for the claimant ($2) vs
/// holder ($7) split this statement embodies -- branch ③ is the one place
/// this statement reads the holder instead of only writing it, and that
/// asymmetry is deliberate (a retried write from the process that already
/// repointed `origin_node_id` at the holder must still recognise its own
/// row).
pub const MARK_RUNNING_SQL: &str = "
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
       )";

/// `renewSandboxDeadlineSQL` (`store_postgres.go:1296-1320`), verbatim: no
/// node identity anywhere -- fenced on `execution_id` alone, see the trait
/// doc on `renew_sandbox_deadline` for why no rival actor needs guarding
/// against here.
pub const RENEW_SANDBOX_DEADLINE_SQL: &str = "
UPDATE paused_sandboxes
   SET sandbox_expires_at = $3,
       updated_at         = now()
 WHERE sandbox_id = $1::uuid
   AND cluster_id = $4::uuid
   AND state = 'running'
   AND execution_id = $2::uuid";

/// `renewLeaseSQL` (`store_postgres.go:1388-1412`), verbatim: `$5` is the
/// caller's own asserted identity -- `running`/`publishing`/`local_only`
/// match by `origin_node_id`, `resuming` by `claimed_by_node_id`. See D2 Fix
/// A's own doc (`super::lease`) for why this never renews a `running` row
/// under the split node/api identity model, and why that is by design rather
/// than a residual bug -- [`RENEW_LIVE_LEASE_SQL`] covers that state instead.
pub const RENEW_LEASE_SQL: &str = "
UPDATE paused_sandboxes AS p
   SET lease_expires_at   = now() + make_interval(secs => $1::double precision),
       sandbox_expires_at = v.expires_at,
       updated_at         = now()
  FROM (SELECT unnest($3::uuid[])        AS sandbox_id,
               unnest($4::timestamptz[]) AS expires_at) AS v
 WHERE p.sandbox_id = v.sandbox_id
   AND p.cluster_id = $2::uuid
   AND ((p.state IN ('running', 'publishing', 'local_only') AND p.origin_node_id = $5)
     OR (p.state = 'resuming' AND p.claimed_by_node_id = $5))";

/// `renewParkedLeaseSQL` (`store_postgres.go:1454-1486`), verbatim:
/// heartbeat-driven, caller-asserted `(sandbox, node)` pairs re-checked
/// against the row's own `origin_node_id` -- never touches
/// `sandbox_expires_at`, which is the API half's authority alone.
pub const RENEW_PARKED_LEASE_SQL: &str = "
UPDATE paused_sandboxes AS p
   SET lease_expires_at = now() + make_interval(secs => $1::double precision),
       updated_at       = now()
  FROM (SELECT unnest($3::uuid[]) AS sandbox_id,
               unnest($4::text[]) AS node_id) AS v
 WHERE p.sandbox_id = v.sandbox_id
   AND p.cluster_id = $2::uuid
   AND p.state IN ('publishing', 'local_only')
   AND p.origin_node_id = v.node_id";

/// `renewLiveLeaseSQL` (`store_postgres.go:1529-1556`), verbatim -- Fix A
/// (`151d00b`): `renewParkedLeaseSQL`'s sibling for `running` rows, added
/// because `renewLeaseSQL`'s own `running` branch never matches an api
/// replica's pod identity.
pub const RENEW_LIVE_LEASE_SQL: &str = "
UPDATE paused_sandboxes AS p
   SET lease_expires_at = now() + make_interval(secs => $1::double precision),
       updated_at       = now()
  FROM (SELECT unnest($3::uuid[]) AS sandbox_id,
               unnest($4::text[]) AS node_id) AS v
 WHERE p.sandbox_id = v.sandbox_id
   AND p.cluster_id = $2::uuid
   AND p.state = 'running'
   AND p.origin_node_id = v.node_id";

/// `reclaimReleasedRunningSQL` (`store_postgres.go:1602-1698`), verbatim.
/// Both conditions required: a lapsed lease alone cannot distinguish a dead
/// node from a partitioned-but-alive one.
pub const RECLAIM_RELEASED_RUNNING_SQL: &str = "
UPDATE paused_sandboxes
   SET state = 'paused', claimed_by_node_id = NULL,
       execution_id = NULL, execution_started_at = NULL,
       generation = generation + 1, updated_at = now(),
       lease_expires_at = now()
 WHERE cluster_id = $1::uuid
   AND snapshot_id IS NOT NULL
   AND state = 'running'
   AND COALESCE(lease_expires_at, updated_at) < now()
   AND sandbox_expires_at < now()";

/// `reclaimReleasedResumingSQL` (`store_postgres.go:1602-1698`), verbatim --
/// Fix B (`7335219`): deliberately **without** the `sandbox_expires_at` arm.
/// `claim_for_resume` never writes that column, so a first-resume claim
/// whose claiming replica died before `mark_running` landed carries a NULL
/// there forever; requiring it here would leave such a row unclaimable
/// (`claim_for_resume` refuses live rows outright) and unreleasable
/// (nothing else releases a `resuming` row) permanently. A `resuming` row is
/// a claim in flight, not a sandbox with a user-set deadline to outlive, so
/// a lapsed lease alone is enough -- the same standard `claim_for_resume`
/// itself applies to `publishing`/`local_only`.
///
/// 🔴 **Do not re-merge this with `RECLAIM_RELEASED_RUNNING_SQL`** into one
/// `state IN ('running', 'resuming')` statement. That is the exact
/// pre-Fix-B shape, and folding them back together reopens the stuck-forever
/// deadlock -- worse under N `--role api` replicas than under Go's single
/// instance, because a Kubernetes Deployment pod name is never reused after
/// a reschedule, so `release_node_holdings`'s identity-based fallback can
/// never reach a row a dead replica orphaned mid-claim either.
pub const RECLAIM_RELEASED_RESUMING_SQL: &str = "
UPDATE paused_sandboxes
   SET state = 'paused', claimed_by_node_id = NULL,
       execution_id = NULL, execution_started_at = NULL,
       generation = generation + 1, updated_at = now(),
       lease_expires_at = now()
 WHERE cluster_id = $1::uuid
   AND snapshot_id IS NOT NULL
   AND state = 'resuming'
   AND COALESCE(lease_expires_at, updated_at) < now()";

/// `reclaimDiscardedSQL` (`store_postgres.go:1602-1698`), verbatim: a live
/// row with no durable snapshot behind it and no reachable holder is gone
/// for good, not merely parked.
pub const RECLAIM_DISCARDED_SQL: &str = "
DELETE FROM paused_sandboxes
 WHERE cluster_id = $1::uuid
   AND snapshot_id IS NULL
   AND state IN ('running', 'resuming')
   AND COALESCE(lease_expires_at, updated_at) < now()
   AND sandbox_expires_at < now()";

/// `countReclaimDiscardableSQL` (`store_postgres.go:1700-1711`), verbatim --
/// the DiscardBreaker's numerator, read inside the same transaction as
/// [`RECLAIM_DISCARDED_SQL`] so it sees the same snapshot.
pub const COUNT_RECLAIM_DISCARDABLE_SQL: &str = "
SELECT count(*) FROM paused_sandboxes
 WHERE cluster_id = $1::uuid
   AND snapshot_id IS NULL
   AND state IN ('running', 'resuming')
   AND COALESCE(lease_expires_at, updated_at) < now()
   AND sandbox_expires_at < now()";

/// `countClusterRowsSQL` (`store_postgres.go:1713-1714`), verbatim -- the
/// DiscardBreaker's denominator.
pub const COUNT_CLUSTER_ROWS_SQL: &str =
    "SELECT count(*) FROM paused_sandboxes WHERE cluster_id = $1::uuid";

/// `releaseHoldingsReleasedSQL` (`store_postgres.go:1798-1814`), verbatim.
pub fn release_holdings_released_sql() -> String {
    format!(
        "
UPDATE paused_sandboxes
   SET state = 'paused', claimed_by_node_id = NULL,
       execution_id = NULL, execution_started_at = NULL,
       generation = generation + 1, updated_at = now(),
       lease_expires_at = now()
 WHERE cluster_id = $1::uuid
   AND snapshot_id IS NOT NULL
   AND {LIVE_HOLDINGS_OF_NODE}"
    )
}

/// `releaseHoldingsDiscardedSQL` (`store_postgres.go:1798-1814`), verbatim.
pub fn release_holdings_discarded_sql() -> String {
    format!(
        "
DELETE FROM paused_sandboxes
 WHERE cluster_id = $1::uuid
   AND snapshot_id IS NULL
   AND {LIVE_HOLDINGS_OF_NODE}"
    )
}

/// `removeSQL` (`store_postgres.go:1872-1873`), verbatim.
pub const REMOVE_SQL: &str = "DELETE FROM paused_sandboxes
 WHERE sandbox_id = $1::uuid AND cluster_id = $2::uuid AND generation = $3";

/// `extendLeasesSQL` (`grace.go:356-374`), verbatim: `Grace.ExtendLeases`.
/// See [`super::grace`] for the N-replica leadership redesign around this
/// statement -- the statement itself is unchanged from Go.
pub const EXTEND_LEASES_SQL: &str = "
WITH observed AS (
    SELECT GREATEST(
               COALESCE(now() - max(updated_at), make_interval(secs => $2::double precision)),
               interval '0'
           ) AS downtime
      FROM paused_sandboxes
     WHERE cluster_id = $1::uuid
),
extended AS (
    UPDATE paused_sandboxes p
       SET lease_expires_at = COALESCE(p.lease_expires_at, p.updated_at)
                            + (SELECT downtime FROM observed)
                            + make_interval(secs => $2::double precision)
     WHERE p.cluster_id = $1::uuid
    RETURNING 1
)
SELECT EXTRACT(EPOCH FROM (SELECT downtime FROM observed))::double precision AS downtime_secs,
       (SELECT count(*) FROM extended)                                       AS extended";
