use std::collections::HashMap;

use anyhow::anyhow;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::postgres::{PgPoolOptions, PgRow};
use sqlx::{PgPool, Row};
use tracing::{debug, warn};
use uuid::Uuid;

use super::{
    log_claim_outcome, BeganPause, ConflictReason, HeldSandbox, MarkRunningOutcome,
    PausedRegistryError, PausedRegistryState, PausedSandboxEntry, PausedSandboxRegistry,
    ReclaimedHoldings, RegistryResult, ReleasedHoldings, ResumeClaim,
};
use crate::orchestrator::store::SandboxMetadata;
use crate::snapshot::SnapshotId;
use crate::types::SandboxId;

/// Columns every read path selects, in one place so the row decoder stays in
/// sync with the queries.
const ENTRY_COLUMNS: &str = "sandbox_id, cluster_id, state, generation, origin_node_id, \
                             claimed_by_node_id, snapshot_id, metadata, paused_at, updated_at";

/// Idempotent schema bootstrap.
///
/// One table, created on startup rather than through a migration tool: the
/// registry is a single-purpose index whose shape is owned by this module, and a
/// node must be able to come up against a fresh database without an out-of-band
/// migration step.
///
/// Everything after the `CREATE TABLE` brings an already-deployed table up to
/// the current shape, because `CREATE TABLE IF NOT EXISTS` silently does
/// nothing when the table exists — including when its columns and constraints
/// are a version behind. Each statement is written to be a no-op on a table
/// that is already current, so this runs unchanged on both a fresh database and
/// one seeded by an earlier build. The whole script is one implicit
/// transaction, so a node either sees the old shape or the new one.
const SCHEMA_DDL: &str = r#"
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
"#;

/// Largest number of sandbox IDs bound into a single `get_many` statement.
const GET_MANY_CHUNK: usize = 1_000;

/// How a row proves its holder is still attached to the cluster.
///
/// Every state except `paused` names a node that is doing something the row
/// describes — running the sandbox, uploading its snapshot, bringing it back
/// up — and that node refreshes `lease_expires_at` on a timer.
///
/// 🔴 What a lapsed lease proves, and what it does not. It proves the holder
/// cannot reach PostgreSQL. It does **not** prove the holder's process is dead,
/// and the two are only the same thing on a machine that has actually gone
/// away: a node partitioned from the database keeps running every sandbox it
/// has, keeps serving traffic through the gateway, and keeps writing to its
/// rootfs layers, all while its rows quietly expire. Rebuilding one of those
/// sandboxes elsewhere on that evidence produces two live copies of it, both
/// diverging from the same snapshot, with the gateway flapping between them.
///
/// So this predicate only ever gates states whose VM is already stopped —
/// `publishing` and `local_only` — where taking over costs the work since the
/// last durable snapshot and nothing more. Live states (`running`, `resuming`)
/// are never claimable on a timer; they are released by the only party that can
/// prove the previous process is gone, which is the next process on that same
/// machine (see `release_node_holdings`).
///
/// `COALESCE(lease_expires_at, updated_at)` treats a row written before this
/// column existed as already expired, which is the safe direction: a live
/// holder refreshes it within one interval, a dead one never does.
const LEASE_EXPIRED: &str = "COALESCE(lease_expires_at, updated_at) < now()";

/// Rows a node is the holder of *and* whose sandbox is live: the ones only that
/// node's own successor may release. `running` names the node in
/// `origin_node_id`, `resuming` in `claimed_by_node_id` — the claim leaves
/// `origin_node_id` pointing at whoever holds the local artifacts.
const LIVE_HOLDINGS_OF_NODE: &str = "((state = 'running'  AND origin_node_id     = $2)
              OR (state = 'resuming' AND claimed_by_node_id = $2))";

/// PostgreSQL-backed [`PausedSandboxRegistry`].
///
/// Every mutating statement carries its own precondition in the `WHERE` clause,
/// so two nodes racing on the same sandbox resolve through the database rather
/// than through application-level locking: exactly one `UPDATE` matches, the
/// other reports zero rows and its caller re-reads.
pub struct PostgresPausedSandboxRegistry {
    pool: PgPool,
    cluster_id: Uuid,
    /// Lease length handed out with every write, in seconds. Bound as a
    /// parameter rather than baked into the SQL so all nodes agree on it
    /// through configuration, and expiry is always judged against the
    /// database's clock rather than each node's own.
    lease_ttl_secs: f64,
}

impl PostgresPausedSandboxRegistry {
    /// Connects, verifies the schema, and returns a ready registry.
    pub async fn connect(
        dsn: &str,
        cluster_id: Uuid,
        max_connections: u32,
        lease_ttl_secs: f64,
    ) -> anyhow::Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(max_connections.max(1))
            .connect(dsn)
            .await
            .map_err(|e| anyhow!("connect to paused sandbox registry database: {e}"))?;

        Self::ensure_schema(&pool).await?;

        // The "registry ready" line lives in `build_paused_registry`, one
        // statement shared by all three backends, so that the backend a node
        // actually ended up on is reported the same way whichever one it is.

        Ok(Self {
            pool,
            cluster_id,
            lease_ttl_secs,
        })
    }

    /// Creates the table and indexes, serialized cluster-wide.
    ///
    /// `CREATE TABLE IF NOT EXISTS` is **not** atomic against a concurrent
    /// creator: two nodes booting together both pass the existence check and
    /// then collide inside the system catalogs
    /// (`duplicate key value violates unique constraint "pg_type_typname_nsp_index"`),
    /// which is fatal for whichever node loses. Observed on the very first
    /// two-node rollout. An advisory lock makes exactly one node run the
    /// bootstrap at a time; the others then see the finished table.
    ///
    /// The lock is taken on a single pinned connection because advisory locks
    /// are session-scoped — taking it from the pool and releasing it on another
    /// connection would leave it held until that session ends.
    async fn ensure_schema(pool: &PgPool) -> anyhow::Result<()> {
        // Arbitrary but stable: any constant works as long as every node agrees.
        const SCHEMA_LOCK_KEY: i64 = 0x0A6E_7653_4348_4D41;

        let mut conn = pool
            .acquire()
            .await
            .map_err(|e| anyhow!("acquire connection for registry schema bootstrap: {e}"))?;

        sqlx::query("SELECT pg_advisory_lock($1)")
            .bind(SCHEMA_LOCK_KEY)
            .execute(&mut *conn)
            .await
            .map_err(|e| anyhow!("lock paused sandbox registry schema: {e}"))?;

        let applied = sqlx::raw_sql(SCHEMA_DDL).execute(&mut *conn).await;

        // Release before reporting the DDL outcome: holding the lock through an
        // error path would block every other node until this session drops.
        if let Err(err) = sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(SCHEMA_LOCK_KEY)
            .execute(&mut *conn)
            .await
        {
            debug!(error = %err, "failed to release registry schema advisory lock");
        }

        applied.map_err(|e| anyhow!("ensure paused sandbox registry schema: {e}"))?;

        Ok(())
    }

    /// Decodes one state column by name.
    ///
    /// Taken as a parameter because `claim_for_resume` reads two of them out of
    /// the same row — the state the claim produced and the one it replaced —
    /// and both have to be rejected the same way when the text is not a state
    /// this build knows.
    fn decode_state(
        row: &PgRow,
        column: &str,
        sandbox_id: &SandboxId,
    ) -> RegistryResult<PausedRegistryState> {
        let text: String = row.try_get(column).map_err(|e| {
            PausedRegistryError::backend(
                "decode state",
                anyhow!(e).context(format!("column {column}")),
            )
        })?;

        PausedRegistryState::parse(&text).ok_or_else(|| PausedRegistryError::InvalidRecord {
            sandbox_id: sandbox_id.to_string(),
            reason: format!("unknown state '{text}' in column '{column}'"),
            source: None,
        })
    }

    fn decode(row: &PgRow) -> RegistryResult<PausedSandboxEntry> {
        let sandbox_uuid: Uuid = row
            .try_get("sandbox_id")
            .map_err(|e| PausedRegistryError::backend("decode sandbox_id", anyhow!(e)))?;
        let sandbox_id = SandboxId::from_uuid(sandbox_uuid);

        let invalid =
            |reason: String, source: Option<anyhow::Error>| PausedRegistryError::InvalidRecord {
                sandbox_id: sandbox_id.to_string(),
                reason,
                source,
            };

        let state = Self::decode_state(row, "state", &sandbox_id)?;

        let metadata_json: serde_json::Value = row
            .try_get("metadata")
            .map_err(|e| PausedRegistryError::backend("decode metadata", anyhow!(e)))?;
        let metadata: SandboxMetadata = serde_json::from_value(metadata_json).map_err(|e| {
            invalid(
                "metadata is not a sandbox record".to_string(),
                Some(e.into()),
            )
        })?;

        let snapshot_uuid: Option<Uuid> = row
            .try_get("snapshot_id")
            .map_err(|e| PausedRegistryError::backend("decode snapshot_id", anyhow!(e)))?;

        // A durable row must name the snapshot it can be rebuilt from; without
        // it the entry promises a cross-node resume it cannot deliver.
        if state == PausedRegistryState::Paused && snapshot_uuid.is_none() {
            return Err(invalid(
                "paused entry carries no snapshot reference".to_string(),
                None,
            ));
        }

        Ok(PausedSandboxEntry {
            sandbox_id,
            cluster_id: row
                .try_get("cluster_id")
                .map_err(|e| PausedRegistryError::backend("decode cluster_id", anyhow!(e)))?,
            state,
            generation: row
                .try_get("generation")
                .map_err(|e| PausedRegistryError::backend("decode generation", anyhow!(e)))?,
            origin_node_id: row
                .try_get("origin_node_id")
                .map_err(|e| PausedRegistryError::backend("decode origin_node_id", anyhow!(e)))?,
            claimed_by_node_id: row.try_get("claimed_by_node_id").map_err(|e| {
                PausedRegistryError::backend("decode claimed_by_node_id", anyhow!(e))
            })?,
            snapshot_id: snapshot_uuid.map(SnapshotId::from_uuid),
            metadata: Some(metadata),
            paused_at: row
                .try_get("paused_at")
                .map_err(|e| PausedRegistryError::backend("decode paused_at", anyhow!(e)))?,
            updated_at: row
                .try_get("updated_at")
                .map_err(|e| PausedRegistryError::backend("decode updated_at", anyhow!(e)))?,
        })
    }

    /// Reads a row, scoped to this node's cluster.
    ///
    /// The cluster scope is not decoration. Nothing stops two clusters from
    /// being pointed at one registry database — it is an ordinary external
    /// dependency — and without the scope a node would happily claim another
    /// cluster's sandbox, fail to find its snapshot in its own repository, and
    /// then delete the other cluster's row as dangling.
    async fn fetch(&self, sandbox_id: &SandboxId) -> RegistryResult<Option<PausedSandboxEntry>> {
        let sql = format!(
            "SELECT {ENTRY_COLUMNS} FROM paused_sandboxes \
             WHERE sandbox_id = $1 AND cluster_id = $2"
        );
        let row = sqlx::query(&sql)
            .bind(sandbox_id.into_inner())
            .bind(self.cluster_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| PausedRegistryError::backend("get", anyhow!(e)))?;

        row.as_ref().map(Self::decode).transpose()
    }
}

#[async_trait]
impl PausedSandboxRegistry for PostgresPausedSandboxRegistry {
    async fn begin_pause(&self, entry: &PausedSandboxEntry) -> RegistryResult<BeganPause> {
        // The write path always has the record; refusing here rather than
        // writing an empty one keeps a row from promising a rebuild it cannot
        // deliver.
        let Some(metadata) = entry.metadata.as_ref() else {
            return Err(PausedRegistryError::InvalidRecord {
                sandbox_id: entry.sandbox_id.to_string(),
                reason: "sandbox metadata is missing".to_string(),
                source: None,
            });
        };
        let metadata =
            serde_json::to_value(metadata).map_err(|e| PausedRegistryError::InvalidRecord {
                sandbox_id: entry.sandbox_id.to_string(),
                reason: "sandbox metadata is not serializable".to_string(),
                source: Some(e.into()),
            })?;
        let now: DateTime<Utc> = Utc::now();

        // A re-pause of a sandbox that already has a row (paused -> resumed ->
        // paused) reuses the row and bumps the generation, so any in-flight
        // writer holding the previous generation is fenced out.
        //
        // `snapshot_id` is deliberately absent from the update list, so the row
        // keeps pointing at the last snapshot that actually reached the
        // repository until `complete_pause` replaces it. Clearing it here would
        // leave a pause whose upload then failed with a row naming no snapshot
        // at all — while the perfectly good previous snapshot sat in the
        // repository with nothing referencing it. Losing the origin node at that
        // moment would lose the sandbox outright, which is precisely the case
        // this registry exists to survive.
        //
        // The `previous` CTE reads the row as it stood before the upsert (every
        // CTE sees the same statement-start snapshot), which is the only way to
        // learn which snapshot this pause supersedes — `RETURNING` would hand
        // back the new row. A sandbox is only ever paused from the one node
        // running it, so nothing else can be rewriting this row concurrently.
        //
        // The `WHERE` on the conflict path keeps one cluster from taking over
        // another's row when both are pointed at the same database; the upsert
        // then matches nothing and the caller is told, rather than silently
        // rewriting a row it does not own.
        let row = sqlx::query(
            r#"
            WITH previous AS (
                SELECT snapshot_id FROM paused_sandboxes
                 WHERE sandbox_id = $1 AND cluster_id = $2
            ),
            upserted AS (
                INSERT INTO paused_sandboxes (
                    sandbox_id, cluster_id, state, generation, origin_node_id,
                    claimed_by_node_id, snapshot_id, metadata, paused_at, updated_at,
                    lease_expires_at
                )
                VALUES ($1, $2, 'publishing', 1, $3, NULL, NULL, $4, $5, $5,
                        now() + make_interval(secs => $6::double precision))
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
            SELECT upserted.generation AS generation,
                   previous.snapshot_id AS previous_snapshot_id
              FROM upserted
              LEFT JOIN previous ON TRUE
            "#,
        )
        .bind(entry.sandbox_id.into_inner())
        .bind(self.cluster_id)
        .bind(&entry.origin_node_id)
        .bind(metadata)
        .bind(now)
        .bind(self.lease_ttl_secs)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| PausedRegistryError::backend("begin_pause", anyhow!(e)))?
        .ok_or_else(|| PausedRegistryError::InvalidRecord {
            sandbox_id: entry.sandbox_id.to_string(),
            reason: "registry already holds this sandbox for a different cluster".to_string(),
            source: None,
        })?;

        let generation: i64 = row
            .try_get("generation")
            .map_err(|e| PausedRegistryError::backend("begin_pause generation", anyhow!(e)))?;
        let previous_snapshot: Option<Uuid> = row.try_get("previous_snapshot_id").map_err(|e| {
            PausedRegistryError::backend("begin_pause previous_snapshot_id", anyhow!(e))
        })?;

        debug!(sandbox_id = %entry.sandbox_id, generation, "registered paused sandbox");

        Ok(BeganPause {
            generation,
            previous_snapshot_id: previous_snapshot.map(SnapshotId::from_uuid),
        })
    }

    async fn complete_pause(
        &self,
        sandbox_id: &SandboxId,
        generation: i64,
        snapshot_id: &SnapshotId,
    ) -> RegistryResult<()> {
        let result = sqlx::query(
            r#"
            UPDATE paused_sandboxes
               SET state = 'paused', snapshot_id = $3, updated_at = now(),
                   lease_expires_at = now() + make_interval(secs => $4::double precision)
             WHERE sandbox_id = $1 AND cluster_id = $5
               AND generation = $2 AND state = 'publishing'
            "#,
        )
        .bind(sandbox_id.into_inner())
        .bind(generation)
        .bind(snapshot_id.to_uuid())
        .bind(self.lease_ttl_secs)
        .bind(self.cluster_id)
        .execute(&self.pool)
        .await
        .map_err(|e| PausedRegistryError::backend("complete_pause", anyhow!(e)))?;

        if result.rows_affected() == 0 {
            return Err(PausedRegistryError::GenerationConflict {
                sandbox_id: sandbox_id.to_string(),
                expected: generation,
            });
        }

        debug!(%sandbox_id, %snapshot_id, "paused sandbox is durable");

        Ok(())
    }

    async fn mark_local_only(&self, sandbox_id: &SandboxId, generation: i64) -> RegistryResult<()> {
        let result = sqlx::query(
            r#"
            UPDATE paused_sandboxes
               SET state = 'local_only', updated_at = now(),
                   lease_expires_at = now() + make_interval(secs => $3::double precision)
             WHERE sandbox_id = $1 AND cluster_id = $4
               AND generation = $2 AND state = 'publishing'
            "#,
        )
        .bind(sandbox_id.into_inner())
        .bind(generation)
        .bind(self.lease_ttl_secs)
        .bind(self.cluster_id)
        .execute(&self.pool)
        .await
        .map_err(|e| PausedRegistryError::backend("mark_local_only", anyhow!(e)))?;

        // Reported rather than swallowed: a downgrade that quietly matches
        // nothing leaves the row stuck in `publishing`, and every resume from
        // another node then answers "still uploading" about an upload that gave
        // up long ago. The caller cannot repair it, but it can say so.
        if result.rows_affected() == 0 {
            return Err(PausedRegistryError::GenerationConflict {
                sandbox_id: sandbox_id.to_string(),
                expected: generation,
            });
        }

        Ok(())
    }

    async fn get(&self, sandbox_id: &SandboxId) -> RegistryResult<Option<PausedSandboxEntry>> {
        self.fetch(sandbox_id).await
    }

    async fn get_many(
        &self,
        sandbox_ids: &[SandboxId],
    ) -> RegistryResult<HashMap<SandboxId, PausedSandboxEntry>> {
        let mut rows = HashMap::with_capacity(sandbox_ids.len());
        // Chunked so a node with a very large roster cannot build a parameter
        // array big enough to be refused; the chunk size is well under any
        // server limit and keeps each statement's plan trivial.
        for chunk in sandbox_ids.chunks(GET_MANY_CHUNK) {
            let ids: Vec<Uuid> = chunk.iter().map(|id| id.into_inner()).collect();
            let sql = format!(
                "SELECT {ENTRY_COLUMNS} FROM paused_sandboxes \
                 WHERE cluster_id = $1 AND sandbox_id = ANY($2)"
            );
            let fetched = sqlx::query(&sql)
                .bind(self.cluster_id)
                .bind(&ids)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| PausedRegistryError::backend("get_many", anyhow!(e)))?;

            for row in &fetched {
                let entry = Self::decode(row)?;
                rows.insert(entry.sandbox_id, entry);
            }
        }

        Ok(rows)
    }

    async fn claim_for_resume(
        &self,
        sandbox_id: &SandboxId,
        node_id: &str,
    ) -> RegistryResult<ResumeClaim> {
        // `origin_node_id` deliberately stays untouched: it still names the node
        // holding the local fast-path artifacts, and it only becomes right again
        // once this resume succeeds and `mark_running` repoints it here.
        // `claimed_by_node_id` is what says who is doing the work meanwhile —
        // without it the origin node cannot tell a resume happening elsewhere
        // from its own row and would happily start a second copy.
        //
        // Two ways to qualify, and one state that never does.
        //
        // A `paused` row is free for the taking: nobody is holding the sandbox
        // and its snapshot is durable, which is the ordinary cross-node resume.
        //
        // `publishing` and `local_only` name a node that paused the sandbox but
        // never got its snapshot into the repository. The VM is already stopped,
        // so rebuilding elsewhere cannot duplicate it — it only rewinds to the
        // snapshot the *previous* pause left behind. That is a real loss, so it
        // waits for a full lease to go unrenewed and is logged as the
        // degradation it is, but it is strictly better than the alternative of
        // leaving the sandbox stranded on a node that may never return.
        //
        // 🔴 `running` and `resuming` are never claimable here, however long the
        // lease has been lapsed. Their VM may still be up: a lapsed lease says
        // the holder cannot reach this database, which a partitioned node —
        // still running every sandbox it has, still being routed traffic —
        // satisfies exactly as well as a dead one. e2b makes the same call from
        // the other side of the same fact: a resume that finds the sandbox in
        // its store is refused outright rather than placed somewhere else
        // (`e2b/packages/api/internal/handlers/sandbox_resume.go`, StateRunning
        // ⇒ 409), and its orphan sweep only ever kills sandboxes the store has
        // no record of at all. Live rows are released instead by the successor
        // process on the holder's own machine — see `release_node_holdings`,
        // the only place in this system that can prove a VM is gone.
        //
        // The `previous` CTE is what makes the outcome knowable at all. Both
        // CTEs read the same statement-start snapshot, so it sees the row as
        // the `UPDATE` found it, while `RETURNING` can only describe the row
        // the `UPDATE` left behind — where `state` is unconditionally
        // `Resuming`. Judging the outcome from the returned row therefore
        // reports every claim as the rarest one, which is exactly what this
        // used to do. Same reasoning, and the same shape, as `begin_pause`.
        let sql = format!(
            r#"
            WITH previous AS (
                SELECT state AS previous_state
                  FROM paused_sandboxes
                 WHERE sandbox_id = $1 AND cluster_id = $4
            ),
            claimed AS (
                UPDATE paused_sandboxes
                   SET state = 'resuming', claimed_by_node_id = $2,
                       generation = generation + 1, updated_at = now(),
                       lease_expires_at = now() + make_interval(secs => $3::double precision)
                 WHERE sandbox_id = $1
                   AND cluster_id = $4
                   AND snapshot_id IS NOT NULL
                   AND (state = 'paused'
                     OR (state IN ('publishing', 'local_only') AND {LEASE_EXPIRED}))
                RETURNING {ENTRY_COLUMNS}
            )
            SELECT claimed.*, previous.previous_state
              FROM claimed JOIN previous ON TRUE
            "#
        );
        let claimed = sqlx::query(&sql)
            .bind(sandbox_id.into_inner())
            .bind(node_id)
            .bind(self.lease_ttl_secs)
            .bind(self.cluster_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| PausedRegistryError::backend("claim_for_resume", anyhow!(e)))?;

        if let Some(row) = claimed {
            let entry = Self::decode(&row)?;
            let previous_state = Self::decode_state(&row, "previous_state", &entry.sandbox_id)?;

            log_claim_outcome(sandbox_id, node_id, &entry, previous_state);

            return Ok(ResumeClaim::Claimed {
                entry: Box::new(entry),
                previous_state,
            });
        }

        // The claim did not match. Re-read to tell the reasons apart, so the
        // caller can redirect instead of reporting a bare "not found". A parked
        // row that got here still has a live lease; a live row gets here
        // whatever its lease says, and stays with its holder either way.
        match self.fetch(sandbox_id).await? {
            None => Ok(ResumeClaim::NotFound),
            Some(entry) => match entry.state {
                // Both mean "parked on its origin node": Publishing is still
                // uploading, LocalOnly never will.
                PausedRegistryState::Publishing | PausedRegistryState::LocalOnly => {
                    Ok(ResumeClaim::NotReady {
                        origin_node_id: entry.origin_node_id,
                    })
                }
                // Live somewhere else, or being brought up somewhere else. The
                // lease is not consulted: no timeout makes a live sandbox safe
                // to rebuild here, so this stays a conflict until the holder's
                // own successor releases the row.
                PausedRegistryState::Resuming | PausedRegistryState::Running => {
                    Ok(ResumeClaim::Conflict {
                        origin_node_id: entry.claimed_by_node_id.unwrap_or(entry.origin_node_id),
                        reason: ConflictReason::LiveElsewhere,
                    })
                }
                // Lost a race with another claimer that has since released it.
                // The row is claimable again, which makes this a different
                // answer from the one above even though both are conflicts.
                PausedRegistryState::Paused => Ok(ResumeClaim::Conflict {
                    origin_node_id: entry.origin_node_id,
                    reason: ConflictReason::ClaimLost,
                }),
            },
        }
    }

    async fn renew_lease(&self, node_id: &str, held: &[HeldSandbox]) -> RegistryResult<u64> {
        if held.is_empty() {
            return Ok(0);
        }

        let ids: Vec<Uuid> = held.iter().map(|h| h.sandbox_id.into_inner()).collect();
        let deadlines: Vec<Option<DateTime<Utc>>> = held.iter().map(|h| h.expires_at).collect();

        // Only rows this node is actually the holder of. Passing the whole local
        // roster is deliberate — the predicate, not the caller, decides which of
        // them this node has standing to renew, so a node cannot extend another
        // node's lease by listing a sandbox it does not hold.
        //
        // `paused` is absent on purpose: that state means nobody holds the
        // sandbox, so there is nothing to keep alive and renewing it would only
        // be noise.
        //
        // The deadline rides along on the same statement rather than being
        // written anywhere else, because these two facts are only useful
        // together: "when did the holder last check in" and "how long was this
        // sandbox supposed to live" are what reclamation compares. Writing them
        // at different moments would let a row claim a deadline from one instant
        // and a lease from another.
        let renewed = sqlx::query(
            r#"
            UPDATE paused_sandboxes AS p
               SET lease_expires_at   = now() + make_interval(secs => $1::double precision),
                   sandbox_expires_at = v.expires_at,
                   updated_at         = now()
              FROM (SELECT unnest($3::uuid[])        AS sandbox_id,
                           unnest($4::timestamptz[]) AS expires_at) AS v
             WHERE p.sandbox_id = v.sandbox_id
               AND p.cluster_id = $2
               AND ((p.state IN ('running', 'publishing', 'local_only') AND p.origin_node_id = $5)
                 OR (p.state = 'resuming' AND p.claimed_by_node_id = $5))
            "#,
        )
        .bind(self.lease_ttl_secs)
        .bind(self.cluster_id)
        .bind(&ids)
        .bind(&deadlines)
        .bind(node_id)
        .execute(&self.pool)
        .await
        .map_err(|e| PausedRegistryError::backend("renew_lease", anyhow!(e)))?
        .rows_affected();

        debug!(node_id, renewed, "renewed paused registry leases");

        Ok(renewed)
    }

    async fn reclaim_expired_holdings(&self) -> RegistryResult<ReclaimedHoldings> {
        // Both conditions, and neither alone would do.
        //
        // `{LEASE_EXPIRED}` alone is what `claim_for_resume` refuses to act on:
        // it cannot tell a dead node from a partitioned one, and acting on it
        // duplicates live sandboxes.
        //
        // `sandbox_expires_at < now()` alone would race the node's own eviction.
        // A reachable node evicts its expired sandboxes itself — pausing them
        // properly and publishing a fresh snapshot — and that is by far the
        // better outcome, so the cluster only steps in once nobody has renewed
        // for a full lease.
        //
        // Together they describe a sandbox that has outlived the deadline its
        // own user set, on a node that has not been heard from since before it
        // did. Reclaiming that is enforcing the timeout, not guessing at the
        // node's health — and it is the one thing that keeps a decommissioned
        // machine's sandboxes from being stranded forever.
        //
        // A NULL `sandbox_expires_at` never matches, which covers both a
        // sandbox asked never to expire and a row whose holder has not renewed
        // since this column existed. Both are the safe answer: leave it alone.
        let mut tx =
            self.pool.begin().await.map_err(|e| {
                PausedRegistryError::backend("reclaim_expired_holdings", anyhow!(e))
            })?;

        let released = sqlx::query(&format!(
            r#"
            UPDATE paused_sandboxes
               SET state = 'paused', claimed_by_node_id = NULL,
                   generation = generation + 1, updated_at = now(),
                   lease_expires_at = now()
             WHERE cluster_id = $1
               AND snapshot_id IS NOT NULL
               AND state IN ('running', 'resuming')
               AND {LEASE_EXPIRED}
               AND sandbox_expires_at < now()
            "#
        ))
        .bind(self.cluster_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| PausedRegistryError::backend("reclaim_expired_holdings", anyhow!(e)))?
        .rows_affected();

        // Same reasoning as `release_node_holdings`: no snapshot means nothing
        // to rebuild from, so the row is only ever going to sit there.
        let discarded = sqlx::query(&format!(
            r#"
            DELETE FROM paused_sandboxes
             WHERE cluster_id = $1
               AND snapshot_id IS NULL
               AND state IN ('running', 'resuming')
               AND {LEASE_EXPIRED}
               AND sandbox_expires_at < now()
            "#
        ))
        .bind(self.cluster_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| PausedRegistryError::backend("reclaim_expired_holdings", anyhow!(e)))?
        .rows_affected();

        tx.commit()
            .await
            .map_err(|e| PausedRegistryError::backend("reclaim_expired_holdings", anyhow!(e)))?;

        if released > 0 || discarded > 0 {
            warn!(
                released,
                discarded,
                "reclaimed sandboxes that outlived their deadline on a node that stopped \
                 reporting; released ones resume from their last published snapshot"
            );
        }

        Ok(ReclaimedHoldings {
            released,
            discarded,
        })
    }

    async fn release_claim(&self, sandbox_id: &SandboxId, generation: i64) -> RegistryResult<bool> {
        // Back to `paused` even when the claim was taken over a `running` row:
        // the claim is only ever handed out when no node holds the sandbox, so
        // its snapshot is the whole truth and `paused` is what describes that.
        let updated = sqlx::query(
            r#"
            UPDATE paused_sandboxes
               SET state = 'paused', claimed_by_node_id = NULL, updated_at = now(),
                   lease_expires_at = now() + make_interval(secs => $3::double precision)
             WHERE sandbox_id = $1 AND cluster_id = $4
               AND generation = $2 AND state = 'resuming'
            "#,
        )
        .bind(sandbox_id.into_inner())
        .bind(generation)
        .bind(self.lease_ttl_secs)
        .bind(self.cluster_id)
        .execute(&self.pool)
        .await
        .map_err(|e| PausedRegistryError::backend("release_claim", anyhow!(e)))?
        .rows_affected();

        // Zero rows stays a success — somebody else already moved the row on,
        // which is the outcome this was trying to produce — but the caller is
        // told, because it is the only signal that this node is quoting a
        // generation it has already lost.
        Ok(updated > 0)
    }

    async fn mark_running(
        &self,
        sandbox_id: &SandboxId,
        node_id: &str,
    ) -> RegistryResult<MarkRunningOutcome> {
        // No insert, and that is the important half: a sandbox the cluster was
        // never told about must stay that way, otherwise every resume on a node
        // with a node-local history would start publishing rows for sandboxes
        // that have no snapshot behind them.
        //
        // The claim guard is what stops one node's resume from erasing another
        // node's in-flight one. Without it a blind write here would clear
        // `claimed_by_node_id` mid-claim, and both nodes would go on to bring
        // the same sandbox up believing they held it — a second live copy, and
        // a row naming only whichever wrote last.
        let updated = sqlx::query(
            r#"
            UPDATE paused_sandboxes
               SET state = 'running', origin_node_id = $2, claimed_by_node_id = NULL,
                   generation = generation + 1, updated_at = now(),
                   lease_expires_at = now() + make_interval(secs => $3::double precision)
             WHERE sandbox_id = $1
               AND cluster_id = $4
               AND (claimed_by_node_id IS NULL OR claimed_by_node_id = $2)
            "#,
        )
        .bind(sandbox_id.into_inner())
        .bind(node_id)
        .bind(self.lease_ttl_secs)
        .bind(self.cluster_id)
        .execute(&self.pool)
        .await
        .map_err(|e| PausedRegistryError::backend("mark_running", anyhow!(e)))?
        .rows_affected();

        if updated == 0 {
            // Nothing matched: either the cluster does not track this sandbox
            // (by far the common case, and correct), or someone else holds the
            // claim — which means two nodes believe they are resuming it and is
            // worth saying out loud. The caller is told which, because those
            // two call for entirely different things.
            if let Some(entry) = self.fetch(sandbox_id).await? {
                warn!(
                    %sandbox_id,
                    node_id,
                    claimed_by = ?entry.claimed_by_node_id,
                    "refused to mark the sandbox running here: another node holds the resume claim"
                );

                return Ok(MarkRunningOutcome::HeldElsewhere);
            }

            return Ok(MarkRunningOutcome::Untracked);
        }

        Ok(MarkRunningOutcome::Adopted)
    }

    async fn release_node_holdings(&self, node_id: &str) -> RegistryResult<ReleasedHoldings> {
        // One transaction, so a resume arriving between the two statements
        // cannot see a sandbox that is neither released nor discarded.
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| PausedRegistryError::backend("release_node_holdings", anyhow!(e)))?;

        // Recoverable: the snapshot outlives the process that was running the
        // sandbox, so the row goes back to being claimable by anyone —
        // including this node, which is usually the one that picks it up again.
        // `lease_expires_at = now()` rather than a fresh lease: `paused` means
        // nobody holds it, and leaving a live-looking lease behind would only
        // confuse the next reader.
        let released = sqlx::query(&format!(
            r#"
            UPDATE paused_sandboxes
               SET state = 'paused', claimed_by_node_id = NULL,
                   generation = generation + 1, updated_at = now(),
                   lease_expires_at = now()
             WHERE cluster_id = $1
               AND snapshot_id IS NOT NULL
               AND {LIVE_HOLDINGS_OF_NODE}
            "#
        ))
        .bind(self.cluster_id)
        .bind(node_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| PausedRegistryError::backend("release_node_holdings", anyhow!(e)))?
        .rows_affected();

        // Unrecoverable: a live sandbox with no published snapshot has its only
        // artifacts on the local disk of the process that just died, and the
        // resume that started it consumed the paused record they belonged to.
        // Keeping the row would leave something no node can ever claim
        // (`claim_for_resume` requires a snapshot) and no node can ever clear.
        let discarded = sqlx::query(&format!(
            r#"
            DELETE FROM paused_sandboxes
             WHERE cluster_id = $1
               AND snapshot_id IS NULL
               AND {LIVE_HOLDINGS_OF_NODE}
            "#
        ))
        .bind(self.cluster_id)
        .bind(node_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| PausedRegistryError::backend("release_node_holdings", anyhow!(e)))?
        .rows_affected();

        tx.commit()
            .await
            .map_err(|e| PausedRegistryError::backend("release_node_holdings", anyhow!(e)))?;

        if released > 0 || discarded > 0 {
            warn!(
                node_id,
                released,
                discarded,
                "released sandboxes the previous process on this node was holding; \
                 released ones resume from their last published snapshot, \
                 discarded ones never had one"
            );
        }

        Ok(ReleasedHoldings {
            released,
            discarded,
        })
    }

    async fn remove(&self, sandbox_id: &SandboxId, generation: i64) -> RegistryResult<bool> {
        // 🔴 Conditional on the generation the caller last read. Without it the
        // guard is the caller's own read-then-delete, and a node returning from
        // a partition walks straight through that window against a sandbox
        // somebody else has since resumed.
        let deleted = sqlx::query(
            "DELETE FROM paused_sandboxes
              WHERE sandbox_id = $1 AND cluster_id = $2 AND generation = $3",
        )
        .bind(sandbox_id.into_inner())
        .bind(self.cluster_id)
        .bind(generation)
        .execute(&self.pool)
        .await
        .map_err(|e| PausedRegistryError::backend("remove", anyhow!(e)))?
        .rows_affected();

        Ok(deleted > 0)
    }

    fn is_cluster_backed(&self) -> bool {
        true
    }
}
