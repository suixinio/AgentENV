use anyhow::anyhow;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::postgres::{PgPoolOptions, PgRow};
use sqlx::{PgPool, Row};
use tracing::{debug, info};
use uuid::Uuid;

use super::{
    PausedRegistryError, PausedRegistryState, PausedSandboxEntry, PausedSandboxRegistry,
    RegistryResult, ResumeClaim,
};
use crate::orchestrator::store::SandboxMetadata;
use crate::snapshot::SnapshotId;
use crate::types::SandboxId;

/// Columns every read path selects, in one place so the row decoder stays in
/// sync with the queries.
const ENTRY_COLUMNS: &str = "sandbox_id, cluster_id, state, generation, origin_node_id, \
                             snapshot_id, metadata, paused_at, updated_at";

/// Idempotent schema bootstrap.
///
/// One table, created on startup rather than through a migration tool: the
/// registry is a single-purpose index whose shape is owned by this module, and a
/// node must be able to come up against a fresh database without an out-of-band
/// migration step.
const SCHEMA_DDL: &str = r#"
CREATE TABLE IF NOT EXISTS paused_sandboxes (
    sandbox_id     UUID        PRIMARY KEY,
    cluster_id     UUID        NOT NULL,
    state          TEXT        NOT NULL CHECK (state IN ('publishing', 'paused', 'resuming', 'local_only')),
    generation     BIGINT      NOT NULL,
    origin_node_id TEXT        NOT NULL,
    snapshot_id    UUID,
    metadata       JSONB       NOT NULL,
    paused_at      TIMESTAMPTZ NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS paused_sandboxes_origin_node_idx ON paused_sandboxes (origin_node_id);
CREATE INDEX IF NOT EXISTS paused_sandboxes_updated_at_idx ON paused_sandboxes (updated_at);
"#;

/// PostgreSQL-backed [`PausedSandboxRegistry`].
///
/// Every mutating statement carries its own precondition in the `WHERE` clause,
/// so two nodes racing on the same sandbox resolve through the database rather
/// than through application-level locking: exactly one `UPDATE` matches, the
/// other reports zero rows and its caller re-reads.
pub struct PostgresPausedSandboxRegistry {
    pool: PgPool,
    cluster_id: Uuid,
}

impl PostgresPausedSandboxRegistry {
    /// Connects, verifies the schema, and returns a ready registry.
    pub async fn connect(
        dsn: &str,
        cluster_id: Uuid,
        max_connections: u32,
    ) -> anyhow::Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(max_connections.max(1))
            .connect(dsn)
            .await
            .map_err(|e| anyhow!("connect to paused sandbox registry database: {e}"))?;

        Self::ensure_schema(&pool).await?;

        info!(%cluster_id, "paused sandbox registry ready");

        Ok(Self { pool, cluster_id })
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

    async fn fetch(&self, sandbox_id: &SandboxId) -> RegistryResult<Option<PausedSandboxEntry>> {
        let sql = format!("SELECT {ENTRY_COLUMNS} FROM paused_sandboxes WHERE sandbox_id = $1");
        let row = sqlx::query(&sql)
            .bind(sandbox_id.into_inner())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| PausedRegistryError::backend("get", anyhow!(e)))?;

        row.as_ref().map(Self::decode).transpose()
    }
}

#[async_trait]
impl PausedSandboxRegistry for PostgresPausedSandboxRegistry {
    async fn begin_pause(&self, entry: &PausedSandboxEntry) -> RegistryResult<i64> {
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
        let row = sqlx::query(
            r#"
            INSERT INTO paused_sandboxes (
                sandbox_id, cluster_id, state, generation, origin_node_id,
                snapshot_id, metadata, paused_at, updated_at
            )
            VALUES ($1, $2, 'publishing', 1, $3, NULL, $4, $5, $5)
            ON CONFLICT (sandbox_id) DO UPDATE SET
                state          = 'publishing',
                generation     = paused_sandboxes.generation + 1,
                origin_node_id = EXCLUDED.origin_node_id,
                snapshot_id    = NULL,
                metadata       = EXCLUDED.metadata,
                paused_at      = EXCLUDED.paused_at,
                updated_at     = EXCLUDED.updated_at
            RETURNING generation
            "#,
        )
        .bind(entry.sandbox_id.into_inner())
        .bind(self.cluster_id)
        .bind(&entry.origin_node_id)
        .bind(metadata)
        .bind(now)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| PausedRegistryError::backend("begin_pause", anyhow!(e)))?;

        let generation: i64 = row
            .try_get("generation")
            .map_err(|e| PausedRegistryError::backend("begin_pause generation", anyhow!(e)))?;

        debug!(sandbox_id = %entry.sandbox_id, generation, "registered paused sandbox");

        Ok(generation)
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
               SET state = 'paused', snapshot_id = $3, updated_at = $4
             WHERE sandbox_id = $1 AND generation = $2 AND state = 'publishing'
            "#,
        )
        .bind(sandbox_id.into_inner())
        .bind(generation)
        .bind(snapshot_id.to_uuid())
        .bind(Utc::now())
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
        sqlx::query(
            r#"
            UPDATE paused_sandboxes
               SET state = 'local_only', updated_at = $3
             WHERE sandbox_id = $1 AND generation = $2 AND state = 'publishing'
            "#,
        )
        .bind(sandbox_id.into_inner())
        .bind(generation)
        .bind(Utc::now())
        .execute(&self.pool)
        .await
        .map_err(|e| PausedRegistryError::backend("mark_local_only", anyhow!(e)))?;

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
        // holding the local fast-path artifacts, and it only becomes wrong once
        // this resume succeeds — at which point the row is removed entirely.
        let sql = format!(
            r#"
            UPDATE paused_sandboxes
               SET state = 'resuming', generation = generation + 1, updated_at = $2
             WHERE sandbox_id = $1 AND state = 'paused'
            RETURNING {ENTRY_COLUMNS}
            "#
        );
        let claimed = sqlx::query(&sql)
            .bind(sandbox_id.into_inner())
            .bind(Utc::now())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| PausedRegistryError::backend("claim_for_resume", anyhow!(e)))?;

        if let Some(row) = claimed {
            let entry = Self::decode(&row)?;
            debug!(%sandbox_id, node_id, generation = entry.generation, "claimed paused sandbox");

            return Ok(ResumeClaim::Claimed(Box::new(entry)));
        }

        // The claim did not match. Re-read to tell the three reasons apart, so
        // the caller can redirect instead of reporting a bare "not found".
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
                PausedRegistryState::Resuming => Ok(ResumeClaim::Conflict {
                    origin_node_id: entry.origin_node_id,
                }),
                // Lost a race with another claimer that has since released it.
                PausedRegistryState::Paused => Ok(ResumeClaim::Conflict {
                    origin_node_id: entry.origin_node_id,
                }),
            },
        }
    }

    async fn release_claim(&self, sandbox_id: &SandboxId, generation: i64) -> RegistryResult<()> {
        sqlx::query(
            r#"
            UPDATE paused_sandboxes
               SET state = 'paused', updated_at = $3
             WHERE sandbox_id = $1 AND generation = $2 AND state = 'resuming'
            "#,
        )
        .bind(sandbox_id.into_inner())
        .bind(generation)
        .bind(Utc::now())
        .execute(&self.pool)
        .await
        .map_err(|e| PausedRegistryError::backend("release_claim", anyhow!(e)))?;

        Ok(())
    }

    async fn remove(&self, sandbox_id: &SandboxId) -> RegistryResult<()> {
        sqlx::query("DELETE FROM paused_sandboxes WHERE sandbox_id = $1")
            .bind(sandbox_id.into_inner())
            .execute(&self.pool)
            .await
            .map_err(|e| PausedRegistryError::backend("remove", anyhow!(e)))?;

        Ok(())
    }

    fn is_cluster_backed(&self) -> bool {
        true
    }
}
