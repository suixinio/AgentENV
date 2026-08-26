//! The writes — a Rust port of
//! `services/scheduler/internal/catalog/queries_admin.go`, minus the
//! `PausedHalf` transaction join (see `mod.rs`'s module doc on why that is
//! not ported) and minus build admission's own file split (Go keeps
//! `queries_admin.go` and `queries_resolved.go` apart so neither set of
//! statements is edited while looking at the other; this module and
//! `reads.rs` keep the same split).

use anyhow::anyhow;
use sqlx::PgPool;
use uuid::Uuid;

use crate::snapshot::repository::backends::central::{
    alias_conflict, commit_opening_record, CatalogRefusal, CatalogWrite,
};
use crate::snapshot::repository::interfaces::{CatalogReadScope, SnapshotCommit, StartedBuild};
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

/// `buildAdmissionKey` — reused literally, see `src/pg/lock_keys.rs`'s
/// `GO_BUILD_ADMISSION_LOCK_KEY`.
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

// ─────────────────────────────────────────────────────────────────────────
// Transaction A — the row before the bytes
// ─────────────────────────────────────────────────────────────────────────

/// Opens a row before any bytes exist, and binds its alias in the same
/// transaction if it has one — matches `insertSnapshotSQL` +
/// `releaseOtherAliasesSQL` + `bindAliasSQL`.
pub(crate) async fn begin_snapshot(
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
            sandbox_started_at_ms, created_at_ms, updated_at_ms,
            publishing_execution_id
         ) VALUES (
            $1, $2, $3, $4,
            $5, $6, $7,
            $8, 'pending',
            $9, $10,
            NULL, $11, $11,
            NULL
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
        // Rolling back is a formality (nothing was written), but explicit
        // beats relying on drop order.
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
    }))
}

/// Releases any other alias this snapshot held, then claims `alias`.
/// `Ok(None)` on success, `Ok(Some(refusal))` when another live snapshot
/// holds the name — matches `releaseOtherAliasesSQL` + `bindAliasSQL` +
/// `aliasHolderSQL`.
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

    // 🔴 The name is already this snapshot's own — a no-op success, not a
    // conflict. This branch is the one thing the port of `bindAlias` left
    // behind: Go's is `strings.EqualFold(holder, snapshot)` -> `return nil,
    // nil`, under its own doc comment "Binding a name this snapshot already
    // holds is a no-op success: the two callers that bind — opening a row and
    // committing it — are both allowed to name the same alias, and a retry of
    // either must not become a conflict with itself." The OSS backend states
    // the same rule (`backends/oss/catalog.rs::bind_alias`: "If it already
    // points to `id`, return success"), which left PostgreSQL as the only
    // backend that refused itself.
    //
    // Both of this module's callers reach that state on every v3 template
    // build: `POST /v3/templates` opens the row with the alias bound to the
    // new template's own id (`begin_snapshot`), and the build's own
    // `publish_commit` binds the same (alias, id) pair a second time
    // (`commit_snapshot`). Without this branch that second bind falls through
    // the `ON CONFLICT ... DO NOTHING` above and is reported as `AliasTaken`
    // naming the caller itself — "alias 'x' already points to '<id>', cannot
    // rebind to '<id>'", the same id printed twice, on every build.
    //
    // Compared as `Uuid` rather than as text: the holder is read as the
    // `uuid` column itself, so this is the canonical value comparison Go can
    // only approximate with a case-insensitive string match.
    if holder == Some(snapshot_id.to_uuid()) {
        return Ok(None);
    }

    Ok(Some(CatalogRefusal::AliasTaken {
        holder: holder.map(|id| id.to_string()).unwrap_or_default(),
    }))
}

// ─────────────────────────────────────────────────────────────────────────
// Transaction B — the flip
// ─────────────────────────────────────────────────────────────────────────

/// Flips a row to `ready` — the only statement that produces one. Binds the
/// alias (if any) and closes the template's active build (if any) in the
/// same transaction — matches `commitSnapshotSQL` + `finishActiveBuildSQL`.
struct CommitArgs<'a> {
    id: &'a SnapshotId,
    committed_payload: Vec<u8>,
    alias: Option<&'a SnapshotAlias>,
    resources: SandboxResources,
    /// The commit's own source axis (template vs sandbox) — this statement
    /// never changes `source_kind` in the database (the axis is fixed at
    /// `begin_snapshot`), but the in-memory record this call hands back must
    /// still report it correctly rather than assuming every commit is a
    /// template. A sandbox commit whose returned record silently claimed to
    /// be a template would lose `source_sandbox_id` from every caller that
    /// trusts this return value instead of re-reading the row.
    source: SnapshotSource,
}

async fn commit_snapshot(
    pool: &PgPool,
    cluster_id: Uuid,
    args: CommitArgs<'_>,
    published: bool,
    node_id: &str,
    updated_at_ms: i64,
) -> RepositoryResult<CatalogWrite<SnapshotRecord>> {
    let origin_node_id = if published {
        None
    } else {
        Some(node_id.to_string())
    };

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
    .bind(origin_node_id)
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

    // 🔴 Unconditional — every commit takes its template's active build off
    // the queue, which is harmless (one probe of a partial index matching
    // nothing) for a pause with no build in flight. See `finishActiveBuildSQL`.
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

    // 🔴 A `Ready` template carries its build's own finish time; a sandbox
    // snapshot has no build state at all (`SnapshotSource::Sandbox` carries
    // only the id it was captured from) -- see `decode_row`'s identical
    // branch for the read path this must agree with.
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
        committed: Some(committed),
    }))
}

/// Reads what a fenced write's row carries now, for the refusal — matches
/// `observedSnapshotStatusSQL`.
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

// ─────────────────────────────────────────────────────────────────────────
// Transaction C — the failure
// ─────────────────────────────────────────────────────────────────────────

/// Moves a row to `error`, and (when asked) ends its active build in the
/// same transaction — matches `failSnapshotSQL` + `failActiveBuildSQL`.
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

// ─────────────────────────────────────────────────────────────────────────
// Delete
// ─────────────────────────────────────────────────────────────────────────

/// Soft-deletes a row and drops the alias pointing at it — matches
/// `softDeleteSnapshotSQL` + `dropAliasesOfSnapshotSQL`. Idempotent: deleting
/// an already-deleted row succeeds and answers `false`.
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

// ─────────────────────────────────────────────────────────────────────────
// Build admission
// ─────────────────────────────────────────────────────────────────────────

/// Admits one build: the cluster-wide ceiling, the per-template exclusion,
/// and the template's `waiting|error -> building` transition, all in one
/// transaction — matches `queries_admin.go`'s "Build admission" section
/// exactly, down to the ceiling check running under a `pg_advisory_xact_lock`
/// and the reasoning in `buildAdmissionKey`'s comment for why: under READ
/// COMMITTED two concurrent admissions cannot see each other's uncommitted
/// rows, so counting after inserting lets both through. The per-template
/// exclusion needs no such lock — it is fenced by the row-level lock the
/// `UPDATE` below already takes on the template's own row, the same
/// guarantee `builds_one_active_per_template`'s unique index gives Go's
/// insert.
///
/// `max_concurrent_builds` is the *already-resolved* ceiling — 0 meaning
/// "enforce a ceiling of zero", not "unlimited" — matching
/// [`super::PostgresSnapshotCatalog::with_max_concurrent_builds`]'s own
/// resolution of a configured `0` up to the default before it ever reaches
/// here; only a negative value disables the check. That mirrors
/// `store_postgres.go:809`'s `if s.maxConcurrentBuilds > 0`, including
/// skipping the lock and the cluster-wide `count(*)` entirely once the
/// ceiling is off — `store_postgres.go`'s own version of the same skip.
pub(crate) async fn start_build(
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
            // 🔴 Resources are left at their prior values by this statement
            // (it only ever touches `status`/`updated_at_ms`/`build_error`),
            // so this synthetic record cannot report them without a second
            // read this call has no reason to pay for — `try_start_build`'s
            // trait-level caller only reads `StartedBuild::build_id`, per
            // `interfaces.rs`'s own doc on the type.
            resources: SandboxResources::default(),
            created_at_unix_ms: started_at_ms,
            updated_at_unix_ms: started_at_ms,
            committed: None,
        },
        build_id: build_id.clone(),
    }))
}

/// One heartbeat — matches `renewBuildLeaseSQL`. `false` means the build is
/// no longer the live one (the reaper freed it, or it never existed) and the
/// builder must stop.
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

// ─────────────────────────────────────────────────────────────────────────
// Trait-facing composition — matches `impl SnapshotCatalog for CentralSnapshotCatalog`
// ─────────────────────────────────────────────────────────────────────────

pub(crate) async fn create(
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

pub(crate) async fn publish_commit(
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
            // `opening.source` was derived from `commit.source` by
            // `commit_opening_record` above and carries the same axis.
            source: opening.source,
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

pub(crate) async fn delete_record(
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

pub(crate) async fn try_start_build(
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

pub(crate) async fn renew_lease(
    pool: &PgPool,
    cluster_id: Uuid,
    node_id: &str,
    build_id: &SnapshotId,
) -> RepositoryResult<bool> {
    renew_build_lease(pool, cluster_id, node_id, build_id).await
}

pub(crate) async fn mark_build_error(
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

// ─────────────────────────────────────────────────────────────────────────
// Trait-facing composition — matches `impl CentralCatalogWrites for CentralSnapshotCatalog`
// (`src/snapshot/repository/mirror/central.rs`). This is the surface that
// lets `PostgresSnapshotCatalog` stand in as the "central" side of
// `write = "both"` and `write = "postgres"` — see `postgres::mod`'s
// `impl CentralCatalogWrites for PostgresSnapshotCatalog`.
// ─────────────────────────────────────────────────────────────────────────

/// Flips a row to `ready` from an externally supplied [`SnapshotCommit`],
/// with no `begin` pre-step — matches `CentralSnapshotCatalog::commit_snapshot`
/// exactly (the two-step "begin, then commit" sequence a caller like
/// `publish_commit` above or the mirror's own `publish_commit` wants is
/// composed by the caller, not by this function).
pub(crate) async fn commit(
    pool: &PgPool,
    cluster_id: Uuid,
    node_id: &str,
    commit: &SnapshotCommit,
    published: bool,
    updated_at_unix_ms: i64,
) -> RepositoryResult<CatalogWrite<SnapshotRecord>> {
    let committed_payload = encode_committed(&commit.committed)?;
    commit_snapshot(
        pool,
        cluster_id,
        CommitArgs {
            id: &commit.id,
            committed_payload,
            alias: commit.alias.as_ref(),
            resources: commit.resources,
            source: commit_opening_record(commit).source,
        },
        published,
        node_id,
        updated_at_unix_ms,
    )
    .await
}

/// Moves a row to `error`, and reports the row as it now stands — matches
/// `CentralSnapshotCatalog::fail_snapshot`'s return shape.
///
/// 🔴 `fail_snapshot`'s own `UPDATE` only ever returns the row's id (see its
/// `RETURNING id::text`), because none of the trait-facing writes above ever
/// needed the full row back. This is the one caller that does — the mirror
/// discards it too today (`Ok(CatalogWrite::Applied(_))`), but the trait's
/// signature promises it, so a follow-up read fills it in rather than
/// fabricating one from the caller's inputs alone. Not part of the same
/// transaction as the `UPDATE`; nothing currently depends on the two being
/// atomic (see the callers cited above), and the alternative — genericizing
/// every read helper in `reads.rs` over `sqlx::Executor` so this could read
/// inside the same `Transaction` — is more machinery than the one caller
/// that needs it justifies today.
pub(crate) async fn fail(
    pool: &PgPool,
    cluster_id: Uuid,
    id: &SnapshotId,
    reason: &TemplateBuildErrorReason,
    updated_at_unix_ms: i64,
) -> RepositoryResult<CatalogWrite<SnapshotRecord>> {
    match fail_snapshot(pool, cluster_id, id, reason, updated_at_unix_ms, true).await? {
        CatalogWrite::Applied(()) => {
            let row = super::reads::get_scoped(
                pool,
                cluster_id,
                &id.to_string(),
                CatalogReadScope::AnyStatus,
            )
            .await?
            .ok_or_else(|| {
                RepositoryError::backend(
                    "re-read a row this call just failed",
                    anyhow!("the row was gone by the time it was read back"),
                )
            })?;
            Ok(CatalogWrite::Applied(row))
        }
        CatalogWrite::Refused(refusal) => Ok(CatalogWrite::Refused(refusal)),
    }
}

/// Soft-deletes one row, resolving `id_or_alias` the same way `get_scoped`
/// does: tries it as an id first, and falls back to an alias lookup — an
/// alias is allowed to look exactly like a uuid, so the shape of the string
/// is a hint rather than an answer. Idempotent: nothing to delete is
/// `Ok(false)`, not an error.
pub(crate) async fn delete(
    pool: &PgPool,
    cluster_id: Uuid,
    id_or_alias: &str,
    deleted_at_unix_ms: i64,
) -> RepositoryResult<bool> {
    if let Ok(id) = SnapshotId::parse(id_or_alias) {
        if delete_snapshot(pool, cluster_id, &id, deleted_at_unix_ms).await? {
            return Ok(true);
        }
    }

    let Some(id) = super::reads::resolve_alias_scoped(
        pool,
        cluster_id,
        id_or_alias,
        CatalogReadScope::AnyStatus,
    )
    .await?
    else {
        return Ok(false);
    };
    delete_snapshot(pool, cluster_id, &id, deleted_at_unix_ms).await
}
