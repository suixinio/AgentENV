//! Fenced PostgreSQL statements for the paused registry.
//! `$n` placeholders are native positional parameters shared by pgx and sqlx.
//! No unfenced rollback variants exist in this backend.

/// Lease-expiry predicate shared by registry statements.
pub const LEASE_EXPIRED: &str = "COALESCE(lease_expires_at, updated_at) < now()";

/// Live rows held by node `$2`, whether running or resuming.
pub const LIVE_HOLDINGS_OF_NODE: &str = "((state = 'running'  AND origin_node_id     = $2)
              OR (state = 'resuming' AND claimed_by_node_id = $2))";

/// Canonical row columns shared by every reader.
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

/// Lists all rows in one cluster; the caller owns sorting.
pub fn list_all_sql() -> String {
    format!(
        "SELECT {ENTRY_COLUMNS}
  FROM paused_sandboxes
 WHERE cluster_id = $1::uuid"
    )
}

/// Begins or retries a pause only for the matching cluster and execution.
///
/// State is intentionally not fenced; execution identity is the authority.
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

/// Classifies a refused `begin_pause` by cluster and execution.
pub const CLASSIFY_REFUSED_PAUSE_SQL: &str =
    "SELECT cluster_id::text, execution_id::text FROM paused_sandboxes WHERE sandbox_id = $1::uuid";

/// Completes a publishing pause and clears its execution identity.
pub const COMPLETE_PAUSE_SQL: &str = "
UPDATE paused_sandboxes
   SET state = 'paused', snapshot_id = $3::uuid, updated_at = now(),
       lease_expires_at = now() + make_interval(secs => $4::double precision),
       execution_id = NULL, execution_started_at = NULL
 WHERE sandbox_id = $1::uuid AND cluster_id = $5::uuid
   AND generation = $2 AND state = 'publishing'";

/// Marks a publishing pause local-only.
pub const MARK_LOCAL_ONLY_SQL: &str = "
UPDATE paused_sandboxes
   SET state = 'local_only', updated_at = now(),
       lease_expires_at = now() + make_interval(secs => $3::double precision),
       execution_id = NULL, execution_started_at = NULL
 WHERE sandbox_id = $1::uuid AND cluster_id = $4::uuid
   AND generation = $2 AND state = 'publishing'";

/// Claims durable paused rows, plus expired parked rows after restart grace.
///
/// Running and resuming rows are never claimable by lease expiry alone.
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

/// Claims only durable paused rows while restart grace is active.
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

/// Releases a resuming claim without an execution predicate.
pub const RELEASE_CLAIM_SQL: &str = "
UPDATE paused_sandboxes
   SET state = 'paused', claimed_by_node_id = NULL, updated_at = now(),
       lease_expires_at = now() + make_interval(secs => $3::double precision),
       execution_id = NULL, execution_started_at = NULL
 WHERE sandbox_id = $1::uuid AND cluster_id = $4::uuid
   AND generation = $2 AND state = 'resuming'";

/// Marks a sandbox running while separating claimant `$2` from holder `$7`.
///
/// Idempotent retries recognize the holder already written to `origin_node_id`.
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

/// Renews a running sandbox deadline under its execution identity.
pub const RENEW_SANDBOX_DEADLINE_SQL: &str = "
UPDATE paused_sandboxes
   SET sandbox_expires_at = $3,
       updated_at         = now()
 WHERE sandbox_id = $1::uuid
   AND cluster_id = $4::uuid
   AND state = 'running'
   AND execution_id = $2::uuid";

/// Renews rows still held by caller identity `$5`.
///
/// Split-node running rows use [`RENEW_LIVE_LEASE_SQL`] instead.
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

/// Renews heartbeat-confirmed parked rows without changing sandbox deadlines.
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

/// Renews heartbeat-confirmed running rows by their real holder.
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

/// Releases running rows only after both lease and sandbox deadline expire.
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

/// Releases resuming rows on lease expiry alone; claims have no user deadline.
///
/// Keep this separate from [`RECLAIM_RELEASED_RUNNING_SQL`].
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

/// Deletes expired live rows with no durable snapshot.
pub const RECLAIM_DISCARDED_SQL: &str = "
DELETE FROM paused_sandboxes
 WHERE cluster_id = $1::uuid
   AND snapshot_id IS NULL
   AND state IN ('running', 'resuming')
   AND COALESCE(lease_expires_at, updated_at) < now()
   AND sandbox_expires_at < now()";

/// Counts discard candidates in the reclaim transaction.
pub const COUNT_RECLAIM_DISCARDABLE_SQL: &str = "
SELECT count(*) FROM paused_sandboxes
 WHERE cluster_id = $1::uuid
   AND snapshot_id IS NULL
   AND state IN ('running', 'resuming')
   AND COALESCE(lease_expires_at, updated_at) < now()
   AND sandbox_expires_at < now()";

/// Counts all cluster rows for the discard breaker.
pub const COUNT_CLUSTER_ROWS_SQL: &str =
    "SELECT count(*) FROM paused_sandboxes WHERE cluster_id = $1::uuid";

/// Releases durable live rows held by node `$2`.
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

/// Deletes non-durable live rows held by node `$2`.
pub fn release_holdings_discarded_sql() -> String {
    format!(
        "
DELETE FROM paused_sandboxes
 WHERE cluster_id = $1::uuid
   AND snapshot_id IS NULL
   AND {LIVE_HOLDINGS_OF_NODE}"
    )
}

/// Removes one exact sandbox generation.
pub const REMOVE_SQL: &str = "DELETE FROM paused_sandboxes
 WHERE sandbox_id = $1::uuid AND cluster_id = $2::uuid AND generation = $3";

/// Extends leases by the observed downtime plus restart-grace TTL.
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
