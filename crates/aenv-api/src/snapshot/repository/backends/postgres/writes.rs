//! Transactional PostgreSQL catalog writes and build admission.

use anyhow::anyhow;
use sqlx::PgPool;
use uuid::Uuid;

use crate::snapshot::repository::backends::catalog_write::{
    alias_conflict, commit_opening_record, CatalogRefusal, CatalogWrite,
};
use crate::snapshot::repository::interfaces::StartedBuild;
use crate::snapshot::repository::{RepositoryError, RepositoryResult};
use crate::snapshot::types::{
    SnapshotAlias, SnapshotId, SnapshotRecord, SnapshotSource, TemplateBuildErrorReason,
};
use crate::types::SandboxResources;

use super::convert::{
    encode_build_error, encode_committed, opening_status, source_kind_str, source_sandbox_id,
    STATUS_BUILDING,
};
use super::reads::backend_error;

use crate::pg::GO_BUILD_ADMISSION_LOCK_KEY;

fn refused(operation: &'static str, refusal: CatalogRefusal) -> RepositoryError {
    RepositoryError::backend(
        format!("snapshot catalog refused '{operation}'"),
        anyhow!("{refusal}"),
    )
}

fn build_refusal(id: &SnapshotId, refusal: CatalogRefusal) -> RepositoryError {
    match refusal {
        CatalogRefusal::NotFound => RepositoryError::SnapshotNotFound {
            lookup: id.to_string(),
        },
        other => RepositoryError::InvalidRequest {
            reason: format!("build '{id}' was not admitted: {other}"),
        },
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

/// Opens a pre-byte row and binds its alias atomically.
pub async fn begin_snapshot(
    pool: &PgPool,
    cluster_id: Uuid,
    node_id: &str,
    record: &SnapshotRecord,
    status: &str,
    published: bool,
) -> RepositoryResult<CatalogWrite<SnapshotRecord>> {
    let origin_node_id = if published {
        None
    } else {
        Some(node_id.to_string())
    };

    let mut tx = pool
        .begin()
        .await
        .map_err(backend_error("begin_snapshot"))?;

    let inserted: Option<(String,)> = sqlx::query_as(
        "INSERT INTO snapshots (
            id, cluster_id, source_kind, source_sandbox_id,
            cpu_count, memory_mib, disk_size_mib,
            status, status_group,
            published, origin_node_id,
            sandbox_started_at_ms, created_at_ms, updated_at_ms
         ) VALUES (
            $1, $2, $3, $4,
            $5, $6, $7,
            $8, 'pending',
            $9, $10,
            NULL, $11, $11
         )
         ON CONFLICT (id) DO NOTHING
         RETURNING id::text",
    )
    .bind(record.id.to_uuid())
    .bind(cluster_id)
    .bind(source_kind_str(record))
    .bind(source_sandbox_id(record))
    .bind(record.resources.cpu_count as i32)
    .bind(record.resources.memory_mib as i32)
    .bind(record.resources.disk_size_mib as i32)
    .bind(status)
    .bind(published)
    .bind(origin_node_id)
    .bind(record.created_at_unix_ms)
    .fetch_optional(&mut *tx)
    .await
    .map_err(backend_error("begin_snapshot"))?;

    if inserted.is_none() {
        let _ = tx.rollback().await;
        return Ok(CatalogWrite::Refused(CatalogRefusal::AlreadyExists));
    }

    if let Some(alias) = &record.alias {
        if let Some(refusal) = bind_alias(
            &mut tx,
            cluster_id,
            &record.id,
            alias,
            record.created_at_unix_ms,
        )
        .await?
        {
            let _ = tx.rollback().await;
            return Ok(CatalogWrite::Refused(refusal));
        }
    }

    tx.commit().await.map_err(backend_error("begin_snapshot"))?;

    Ok(CatalogWrite::Applied(SnapshotRecord {
        id: record.id.clone(),
        alias: record.alias.clone(),
        source: record.source.clone(),
        resources: record.resources,
        created_at_unix_ms: record.created_at_unix_ms,
        updated_at_unix_ms: record.created_at_unix_ms,
        committed: None,
        origin_node_id: None,
    }))
}

// Replaces this snapshot's old alias and claims the new one in the transaction.
async fn bind_alias(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    cluster_id: Uuid,
    snapshot_id: &SnapshotId,
    alias: &SnapshotAlias,
    created_at_unix_ms: i64,
) -> RepositoryResult<Option<CatalogRefusal>> {
    sqlx::query("DELETE FROM aliases WHERE cluster_id = $1 AND snapshot_id = $2 AND alias <> $3")
        .bind(cluster_id)
        .bind(snapshot_id.to_uuid())
        .bind(alias.to_string())
        .execute(&mut **tx)
        .await
        .map_err(backend_error("bind_alias"))?;

    let bound = sqlx::query(
        "INSERT INTO aliases (cluster_id, alias, snapshot_id, created_at_ms) VALUES ($1, $2, $3, $4) \
         ON CONFLICT (cluster_id, alias) DO NOTHING",
    )
    .bind(cluster_id)
    .bind(alias.to_string())
    .bind(snapshot_id.to_uuid())
    .bind(created_at_unix_ms)
    .execute(&mut **tx)
    .await
    .map_err(backend_error("bind_alias"))?;

    if bound.rows_affected() > 0 {
        return Ok(None);
    }

    let holder: Option<Uuid> =
        sqlx::query_scalar("SELECT snapshot_id FROM aliases WHERE cluster_id = $1 AND alias = $2")
            .bind(cluster_id)
            .bind(alias.to_string())
            .fetch_optional(&mut **tx)
            .await
            .map_err(backend_error("bind_alias"))?;

    // Rebinding an alias to the same snapshot is idempotent.
    if holder == Some(snapshot_id.to_uuid()) {
        return Ok(None);
    }

    Ok(Some(CatalogRefusal::AliasTaken {
        holder: holder.map(|id| id.to_string()).unwrap_or_default(),
    }))
}

// Arguments for atomically committing a ready snapshot.
struct CommitArgs<'a> {
    id: &'a SnapshotId,
    committed_payload: Vec<u8>,
    alias: Option<&'a SnapshotAlias>,
    resources: SandboxResources,
    // Preserve the commit's template-versus-sandbox source in the returned record.
    source: SnapshotSource,
    // The node that staged the bytes; recorded whether or not they are published.
    origin_node_id: Option<String>,
}

async fn commit_snapshot(
    pool: &PgPool,
    cluster_id: Uuid,
    args: CommitArgs<'_>,
    published: bool,
    node_id: &str,
    updated_at_ms: i64,
) -> RepositoryResult<CatalogWrite<SnapshotRecord>> {
    // A row always names where its bytes were staged: a resume prefers that
    // node. Unpublished bytes are additionally pinned to it by the schema.
    let origin_node_id = args
        .origin_node_id
        .clone()
        .or_else(|| (!published).then(|| node_id.to_string()));

    let mut tx = pool
        .begin()
        .await
        .map_err(backend_error("commit_snapshot"))?;

    let updated: Option<(String, i64, i64)> = sqlx::query_as(
        "UPDATE snapshots
            SET status                  = 'ready',
                committed_payload       = $3,
                committed_schema        = $4,
                published               = $5,
                origin_node_id          = COALESCE($6, origin_node_id),
                updated_at_ms           = $7,
                cpu_count               = COALESCE($8, cpu_count),
                memory_mib              = COALESCE($9, memory_mib),
                disk_size_mib           = COALESCE($10, disk_size_mib)
          WHERE id = $1
            AND cluster_id = $2
            AND deleted_at_ms IS NULL
            AND status = 'building'
        RETURNING id::text, created_at_ms, updated_at_ms",
    )
    .bind(args.id.to_uuid())
    .bind(cluster_id)
    .bind(&args.committed_payload)
    .bind(super::convert::COMMITTED_PAYLOAD_SCHEMA)
    .bind(published)
    .bind(origin_node_id.clone())
    .bind(updated_at_ms)
    .bind(args.resources.cpu_count as i32)
    .bind(args.resources.memory_mib as i32)
    .bind(args.resources.disk_size_mib as i32)
    .fetch_optional(&mut *tx)
    .await
    .map_err(backend_error("commit_snapshot"))?;

    let Some((_, created_at_ms, updated_at_ms)) = updated else {
        let refusal = observed_refusal(&mut tx, cluster_id, args.id).await?;
        let _ = tx.rollback().await;
        return Ok(CatalogWrite::Refused(refusal));
    };

    if let Some(alias) = args.alias {
        if let Some(refusal) =
            bind_alias(&mut tx, cluster_id, args.id, alias, updated_at_ms).await?
        {
            let _ = tx.rollback().await;
            return Ok(CatalogWrite::Refused(refusal));
        }
    }

    sqlx::query(
        "UPDATE builds
            SET status = 'ready', finished_at_ms = $3, error_reason = NULL
          WHERE template_id = $1 AND cluster_id = $2 AND status_group IN ('pending', 'in_progress')",
    )
    .bind(args.id.to_uuid())
    .bind(cluster_id)
    .bind(updated_at_ms)
    .execute(&mut *tx)
    .await
    .map_err(backend_error("commit_snapshot"))?;

    tx.commit()
        .await
        .map_err(backend_error("commit_snapshot"))?;

    let committed: crate::snapshot::types::CommittedSnapshot =
        serde_json::from_slice(&args.committed_payload).map_err(|error| {
            RepositoryError::backend("re-decode the payload this call just wrote", error)
        })?;

    // Only template snapshots carry build finish timestamps.
    let source = match args.source {
        SnapshotSource::Template { .. } => SnapshotSource::Template {
            build: crate::snapshot::types::TemplateBuildInfo {
                status: crate::snapshot::types::TemplateBuildStatus::Ready,
                started_at_unix_ms: None,
                finished_at_unix_ms: Some(updated_at_ms),
                error_reason: None,
            },
        },
        sandbox @ SnapshotSource::Sandbox { .. } => sandbox,
    };

    Ok(CatalogWrite::Applied(SnapshotRecord {
        id: args.id.clone(),
        alias: args.alias.cloned(),
        source,
        resources: args.resources,
        created_at_unix_ms: created_at_ms,
        updated_at_unix_ms: updated_at_ms,
        origin_node_id,
        committed: Some(committed),
    }))
}

// Reads current state to classify a fenced write.
async fn observed_refusal(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    cluster_id: Uuid,
    id: &SnapshotId,
) -> RepositoryResult<CatalogRefusal> {
    let row: Option<(String, Option<i64>)> = sqlx::query_as(
        "SELECT status, deleted_at_ms FROM snapshots WHERE id = $1 AND cluster_id = $2",
    )
    .bind(id.to_uuid())
    .bind(cluster_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(backend_error("observed_status"))?;
    Ok(match row {
        None => CatalogRefusal::NotFound,
        Some((_, Some(_))) => CatalogRefusal::NotFound,
        Some((status, None)) => CatalogRefusal::StatusMismatch { observed: status },
    })
}

// Moves a snapshot and its optional active build to error atomically.
async fn fail_snapshot(
    pool: &PgPool,
    cluster_id: Uuid,
    id: &SnapshotId,
    reason: &TemplateBuildErrorReason,
    updated_at_ms: i64,
    fail_active_build: bool,
) -> RepositoryResult<CatalogWrite<()>> {
    let error_json = encode_build_error(reason);

    let mut tx = pool.begin().await.map_err(backend_error("fail_snapshot"))?;

    let updated: Option<(String,)> = sqlx::query_as(
        "UPDATE snapshots
            SET status = 'error', build_error = $3, updated_at_ms = $4
          WHERE id = $1 AND cluster_id = $2 AND deleted_at_ms IS NULL AND status <> 'ready'
        RETURNING id::text",
    )
    .bind(id.to_uuid())
    .bind(cluster_id)
    .bind(&error_json)
    .bind(updated_at_ms)
    .fetch_optional(&mut *tx)
    .await
    .map_err(backend_error("fail_snapshot"))?;

    let Some(_) = updated else {
        let refusal = observed_refusal(&mut tx, cluster_id, id).await?;
        let _ = tx.rollback().await;
        return Ok(CatalogWrite::Refused(refusal));
    };

    if fail_active_build {
        sqlx::query(
            "UPDATE builds
                SET status = 'error', finished_at_ms = $3, error_reason = $4
              WHERE template_id = $1 AND cluster_id = $2 AND status_group IN ('pending', 'in_progress')",
        )
        .bind(id.to_uuid())
        .bind(cluster_id)
        .bind(updated_at_ms)
        .bind(&error_json)
        .execute(&mut *tx)
        .await
        .map_err(backend_error("fail_snapshot"))?;
    }

    tx.commit().await.map_err(backend_error("fail_snapshot"))?;
    Ok(CatalogWrite::Applied(()))
}

// Soft-deletes the row and alias idempotently.
async fn delete_snapshot(
    pool: &PgPool,
    cluster_id: Uuid,
    id: &SnapshotId,
    deleted_at_unix_ms: i64,
) -> RepositoryResult<bool> {
    let now = deleted_at_unix_ms;
    let mut tx = pool
        .begin()
        .await
        .map_err(backend_error("delete_snapshot"))?;

    let deleted: Option<(String,)> = sqlx::query_as(
        "UPDATE snapshots SET deleted_at_ms = $3, updated_at_ms = $3
          WHERE id = $1 AND cluster_id = $2 AND deleted_at_ms IS NULL
        RETURNING id::text",
    )
    .bind(id.to_uuid())
    .bind(cluster_id)
    .bind(now)
    .fetch_optional(&mut *tx)
    .await
    .map_err(backend_error("delete_snapshot"))?;

    if deleted.is_none() {
        tx.commit()
            .await
            .map_err(backend_error("delete_snapshot"))?;
        return Ok(false);
    }

    sqlx::query("DELETE FROM aliases WHERE cluster_id = $1 AND snapshot_id = $2")
        .bind(cluster_id)
        .bind(id.to_uuid())
        .execute(&mut *tx)
        .await
        .map_err(backend_error("delete_snapshot"))?;

    tx.commit()
        .await
        .map_err(backend_error("delete_snapshot"))?;
    Ok(true)
}

/// Admits a build atomically under the cluster ceiling and per-template exclusion.
///
/// Negative ceiling disables admission counting; zero is an enforced ceiling.
pub async fn start_build(
    pool: &PgPool,
    cluster_id: Uuid,
    node_id: &str,
    template_id: &SnapshotId,
    build_id: &SnapshotId,
    max_concurrent_builds: i32,
    started_at_ms: i64,
) -> RepositoryResult<CatalogWrite<StartedBuild>> {
    let mut tx = pool.begin().await.map_err(backend_error("start_build"))?;

    if max_concurrent_builds > 0 {
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(GO_BUILD_ADMISSION_LOCK_KEY)
            .execute(&mut *tx)
            .await
            .map_err(backend_error("start_build"))?;

        let active_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM builds WHERE cluster_id = $1 AND status_group IN ('pending', 'in_progress')",
        )
        .bind(cluster_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(backend_error("start_build"))?;
        if active_count >= max_concurrent_builds as i64 {
            let _ = tx.rollback().await;
            return Ok(CatalogWrite::Refused(CatalogRefusal::BuildQueueFull));
        }
    }

    let active_for_template: Option<(String,)> = sqlx::query_as(
        "SELECT id::text FROM builds
          WHERE cluster_id = $1 AND template_id = $2 AND status_group IN ('pending', 'in_progress')
          LIMIT 1",
    )
    .bind(cluster_id)
    .bind(template_id.to_uuid())
    .fetch_optional(&mut *tx)
    .await
    .map_err(backend_error("start_build"))?;
    if let Some((active_build_id,)) = active_for_template {
        let _ = tx.rollback().await;
        return Ok(CatalogWrite::Refused(CatalogRefusal::BuildInProgress {
            active_build_id,
        }));
    }

    let marked: Option<(String,)> = sqlx::query_as(
        "UPDATE snapshots
            SET status = 'building', updated_at_ms = $3, build_error = NULL
          WHERE id = $1 AND cluster_id = $2 AND deleted_at_ms IS NULL AND status IN ('waiting', 'error')
        RETURNING id::text",
    )
    .bind(template_id.to_uuid())
    .bind(cluster_id)
    .bind(started_at_ms)
    .fetch_optional(&mut *tx)
    .await
    .map_err(backend_error("start_build"))?;
    let Some(_) = marked else {
        let refusal = observed_refusal(&mut tx, cluster_id, template_id).await?;
        let _ = tx.rollback().await;
        return Ok(CatalogWrite::Refused(refusal));
    };

    sqlx::query(
        "INSERT INTO builds (
            id, template_id, cluster_id, status, status_group, node_id, heartbeat_at_ms,
            created_at_ms, started_at_ms
         ) VALUES (
            $1, $2, $3, 'building', 'in_progress', $4,
            (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT, $5, $5
         )",
    )
    .bind(build_id.to_uuid())
    .bind(template_id.to_uuid())
    .bind(cluster_id)
    .bind(node_id)
    .bind(started_at_ms)
    .execute(&mut *tx)
    .await
    .map_err(backend_error("start_build"))?;

    tx.commit().await.map_err(backend_error("start_build"))?;

    Ok(CatalogWrite::Applied(StartedBuild {
        record: SnapshotRecord {
            id: template_id.clone(),
            alias: None,
            source: SnapshotSource::Template {
                build: crate::snapshot::types::TemplateBuildInfo {
                    status: crate::snapshot::types::TemplateBuildStatus::Building,
                    started_at_unix_ms: Some(started_at_ms),
                    finished_at_unix_ms: None,
                    error_reason: None,
                },
            },
            // The status-only update does not read resources back.
            resources: SandboxResources::default(),
            created_at_unix_ms: started_at_ms,
            updated_at_unix_ms: started_at_ms,
            origin_node_id: None,
            committed: None,
        },
        build_id: build_id.clone(),
    }))
}

// Renews a live build; `false` tells the builder to stop.
async fn renew_build_lease(
    pool: &PgPool,
    cluster_id: Uuid,
    node_id: &str,
    build_id: &SnapshotId,
) -> RepositoryResult<bool> {
    let updated = sqlx::query(
        "UPDATE builds
            SET heartbeat_at_ms = (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT
          WHERE id = $3 AND cluster_id = $1 AND node_id = $2 AND status_group IN ('pending', 'in_progress')",
    )
    .bind(cluster_id)
    .bind(node_id)
    .bind(build_id.to_uuid())
    .execute(pool)
    .await
    .map_err(backend_error("renew_build_lease"))?;
    Ok(updated.rows_affected() > 0)
}

pub async fn create(
    pool: &PgPool,
    cluster_id: Uuid,
    node_id: &str,
    record: SnapshotRecord,
) -> RepositoryResult<SnapshotRecord> {
    match begin_snapshot(
        pool,
        cluster_id,
        node_id,
        &record,
        opening_status(&record),
        true,
    )
    .await?
    {
        CatalogWrite::Applied(row) => Ok(row),
        CatalogWrite::Refused(CatalogRefusal::AliasTaken { holder }) => {
            Err(alias_conflict(record.alias.as_ref(), &record.id, holder))
        }
        CatalogWrite::Refused(refusal) => Err(refused("create", refusal)),
    }
}

pub async fn publish_commit(
    pool: &PgPool,
    cluster_id: Uuid,
    node_id: &str,
    commit: crate::snapshot::repository::interfaces::SnapshotCommit,
) -> RepositoryResult<SnapshotRecord> {
    let opening = commit_opening_record(&commit);
    match begin_snapshot(pool, cluster_id, node_id, &opening, STATUS_BUILDING, false).await? {
        CatalogWrite::Applied(_) | CatalogWrite::Refused(CatalogRefusal::AlreadyExists) => {}
        CatalogWrite::Refused(CatalogRefusal::AliasTaken { holder }) => {
            return Err(alias_conflict(commit.alias.as_ref(), &commit.id, holder))
        }
        CatalogWrite::Refused(refusal) => return Err(refused("publish_commit", refusal)),
    }

    let committed_payload = encode_committed(&commit.committed)?;
    match commit_snapshot(
        pool,
        cluster_id,
        CommitArgs {
            id: &commit.id,
            committed_payload,
            alias: commit.alias.as_ref(),
            resources: commit.resources,
            source: opening.source,
            origin_node_id: commit.origin_node_id.clone(),
        },
        true,
        node_id,
        now_ms(),
    )
    .await?
    {
        CatalogWrite::Applied(row) => Ok(row),
        CatalogWrite::Refused(CatalogRefusal::AliasTaken { holder }) => {
            Err(alias_conflict(commit.alias.as_ref(), &commit.id, holder))
        }
        CatalogWrite::Refused(refusal) => Err(refused("publish_commit", refusal)),
    }
}

/// Repoints a row at the node a resume of its sandbox landed on.
pub async fn set_origin_node_id(
    pool: &PgPool,
    cluster_id: Uuid,
    id: &SnapshotId,
    origin_node_id: &str,
) -> RepositoryResult<()> {
    sqlx::query(
        "UPDATE snapshots SET origin_node_id = $3, updated_at_ms = $4
          WHERE id = $1 AND cluster_id = $2 AND deleted_at_ms IS NULL",
    )
    .bind(id.to_uuid())
    .bind(cluster_id)
    .bind(origin_node_id)
    .bind(now_ms())
    .execute(pool)
    .await
    .map_err(backend_error("set_origin_node_id"))?;
    Ok(())
}

pub async fn delete_record(
    pool: &PgPool,
    cluster_id: Uuid,
    record: &SnapshotRecord,
) -> RepositoryResult<()> {
    let deleted = delete_snapshot(pool, cluster_id, &record.id, now_ms()).await?;
    if !deleted {
        tracing::debug!(target: "agentenv", snapshot_id = %record.id, "snapshot catalog had nothing to delete");
    }
    Ok(())
}

pub async fn try_start_build(
    pool: &PgPool,
    cluster_id: Uuid,
    node_id: &str,
    id: &SnapshotId,
    max_concurrent_builds: i32,
) -> RepositoryResult<StartedBuild> {
    match start_build(
        pool,
        cluster_id,
        node_id,
        id,
        &SnapshotId::generate(),
        max_concurrent_builds,
        now_ms(),
    )
    .await?
    {
        CatalogWrite::Applied(started) => Ok(started),
        CatalogWrite::Refused(refusal) => Err(build_refusal(id, refusal)),
    }
}

pub async fn renew_lease(
    pool: &PgPool,
    cluster_id: Uuid,
    node_id: &str,
    build_id: &SnapshotId,
) -> RepositoryResult<bool> {
    renew_build_lease(pool, cluster_id, node_id, build_id).await
}

pub async fn mark_build_error(
    pool: &PgPool,
    cluster_id: Uuid,
    id: &SnapshotId,
    reason: TemplateBuildErrorReason,
) -> RepositoryResult<()> {
    match fail_snapshot(pool, cluster_id, id, &reason, now_ms(), true).await? {
        CatalogWrite::Applied(()) => Ok(()),
        CatalogWrite::Refused(refusal) => Err(refused("mark_build_error", refusal)),
    }
}
