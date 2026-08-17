use anyhow::anyhow;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::postgres::{PgPoolOptions, PgRow};
use sqlx::{PgPool, Row};
use tracing::{debug, info, warn};
use uuid::Uuid;

use super::{
    BeganPause, PausedRegistryError, PausedRegistryState, PausedSandboxEntry,
    PausedSandboxRegistry, RegistryResult, ResumeClaim,
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
CREATE INDEX IF NOT EXISTS paused_sandboxes_origin_node_idx ON paused_sandboxes (origin_node_id);
CREATE INDEX IF NOT EXISTS paused_sandboxes_updated_at_idx ON paused_sandboxes (updated_at);
"#;

/// How a row proves its holder is still alive.
///
/// Every state except `paused` names a node that is doing something the row
/// describes — running the sandbox, uploading its snapshot, bringing it back
/// up — and that node refreshes `lease_expires_at` on a timer. Another node may
/// only take the sandbox over once the lease has run out.
///
/// Without this the registry has no liveness signal at all, and the only thing
/// left to infer "the holder is gone" from is the scheduler having no binding
/// for the sandbox. That inference is wrong: bindings are held in memory with a
/// 30s TTL and are lost outright when the scheduler restarts, so a perfectly
/// healthy sandbox reads as unheld for as long as it takes the next heartbeat
/// to re-seed the binding — and a resume arriving in that window would start a
/// second live copy of it on another node.
///
/// `COALESCE(lease_expires_at, updated_at)` treats a row written before this
/// column existed as already expired, which is the safe direction: a live
/// holder refreshes it within one interval, a dead one never does.
const LEASE_EXPIRED: &str = "COALESCE(lease_expires_at, updated_at) < now()";

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

        info!(%cluster_id, lease_ttl_secs, "paused sandbox registry ready");

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

        let state_text: String = row
            .try_get("state")
            .map_err(|e| PausedRegistryError::backend("decode state", anyhow!(e)))?;
        let state = PausedRegistryState::parse(&state_text)
            .ok_or_else(|| invalid(format!("unknown state '{state_text}'"), None))?;

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
            metadata,
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
        let metadata = serde_json::to_value(&entry.metadata).map_err(|e| {
            PausedRegistryError::InvalidRecord {
                sandbox_id: entry.sandbox_id.to_string(),
                reason: "sandbox metadata is not serializable".to_string(),
                source: Some(e.into()),
            }
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
        // Two ways to qualify. A `paused` row is free for the taking: nobody is
        // holding the sandbox and its snapshot is durable, which is the ordinary
        // cross-node resume. Every other state names a node that is mid-flight,
        // so taking it over requires that node's lease to have run out — the
        // only evidence in this system that it is actually gone. Claiming a
        // `running` row on any weaker signal starts a second live copy of a
        // healthy sandbox.
        //
        // Taking over `publishing` or `local_only` restores from the snapshot
        // the *previous* pause left behind, since the newest one never reached
        // the repository. That loses the last pause's work, so it happens only
        // after a full lease has gone unrenewed, and it is logged as the
        // degradation it is.
        let sql = format!(
            r#"
            UPDATE paused_sandboxes
               SET state = 'resuming', claimed_by_node_id = $2,
                   generation = generation + 1, updated_at = now(),
                   lease_expires_at = now() + make_interval(secs => $3::double precision)
             WHERE sandbox_id = $1
               AND cluster_id = $4
               AND snapshot_id IS NOT NULL
               AND (state = 'paused' OR {LEASE_EXPIRED})
            RETURNING {ENTRY_COLUMNS}
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
            match entry.state {
                PausedRegistryState::Paused => {
                    debug!(%sandbox_id, node_id, generation = entry.generation, "claimed paused sandbox")
                }
                stale => warn!(
                    %sandbox_id,
                    node_id,
                    previous_state = ?stale,
                    previous_holder = %entry.origin_node_id,
                    "took over a sandbox whose holder stopped renewing its lease; \
                     restoring from the last snapshot that reached the repository"
                ),
            }

            return Ok(ResumeClaim::Claimed(Box::new(entry)));
        }

        // The claim did not match. Re-read to tell the reasons apart, so the
        // caller can redirect instead of reporting a bare "not found". Every
        // answer below describes a live holder, because a dead one's row would
        // have been claimed above.
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
                // Someone else is bringing it up, or it is live somewhere with
                // no snapshot behind it yet. Either way this node must not.
                PausedRegistryState::Resuming | PausedRegistryState::Running => {
                    Ok(ResumeClaim::Conflict {
                        origin_node_id: entry.claimed_by_node_id.unwrap_or(entry.origin_node_id),
                    })
                }
                // Lost a race with another claimer that has since released it.
                PausedRegistryState::Paused => Ok(ResumeClaim::Conflict {
                    origin_node_id: entry.origin_node_id,
                }),
            },
        }
    }

    async fn renew_lease(&self, node_id: &str, sandbox_ids: &[SandboxId]) -> RegistryResult<u64> {
        if sandbox_ids.is_empty() {
            return Ok(0);
        }

        let ids: Vec<Uuid> = sandbox_ids.iter().map(|id| id.into_inner()).collect();

        // Only rows this node is actually the holder of. Passing the whole local
        // roster is deliberate — the predicate, not the caller, decides which of
        // them this node has standing to renew, so a node cannot extend another
        // node's lease by listing a sandbox it does not hold.
        //
        // `paused` is absent on purpose: that state means nobody holds the
        // sandbox, so there is nothing to keep alive and renewing it would only
        // be noise.
        let renewed = sqlx::query(
            r#"
            UPDATE paused_sandboxes
               SET lease_expires_at = now() + make_interval(secs => $1::double precision),
                   updated_at = now()
             WHERE cluster_id = $2
               AND sandbox_id = ANY($3)
               AND ((state IN ('running', 'publishing', 'local_only') AND origin_node_id = $4)
                 OR (state = 'resuming' AND claimed_by_node_id = $4))
            "#,
        )
        .bind(self.lease_ttl_secs)
        .bind(self.cluster_id)
        .bind(&ids)
        .bind(node_id)
        .execute(&self.pool)
        .await
        .map_err(|e| PausedRegistryError::backend("renew_lease", anyhow!(e)))?
        .rows_affected();

        debug!(node_id, renewed, "renewed paused registry leases");

        Ok(renewed)
    }

    async fn release_claim(&self, sandbox_id: &SandboxId, generation: i64) -> RegistryResult<()> {
        // Back to `paused` even when the claim was taken over a `running` row:
        // the claim is only ever handed out when no node holds the sandbox, so
        // its snapshot is the whole truth and `paused` is what describes that.
        sqlx::query(
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
        .map_err(|e| PausedRegistryError::backend("release_claim", anyhow!(e)))?;

        Ok(())
    }

    async fn mark_running(&self, sandbox_id: &SandboxId, node_id: &str) -> RegistryResult<()> {
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
            // worth saying out loud.
            if let Some(entry) = self.fetch(sandbox_id).await? {
                warn!(
                    %sandbox_id,
                    node_id,
                    claimed_by = ?entry.claimed_by_node_id,
                    "refused to mark the sandbox running here: another node holds the resume claim"
                );
            }
        }

        Ok(())
    }

    async fn remove(&self, sandbox_id: &SandboxId) -> RegistryResult<()> {
        sqlx::query("DELETE FROM paused_sandboxes WHERE sandbox_id = $1 AND cluster_id = $2")
            .bind(sandbox_id.into_inner())
            .bind(self.cluster_id)
            .execute(&self.pool)
            .await
            .map_err(|e| PausedRegistryError::backend("remove", anyhow!(e)))?;

        Ok(())
    }

    fn is_cluster_backed(&self) -> bool {
        true
    }
}
