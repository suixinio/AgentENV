//! Fenced paused-registry writes.
//! Each operation relies on PostgreSQL row locking and generation/execution
//! predicates, so replicas need no caller-side lock.

use chrono::{DateTime, Utc};
use uuid::Uuid;

use super::row::{decode_claim, ClaimRow};
use super::sql;
use super::PostgresPausedSandboxRegistry;
use crate::orchestrator::paused_registry::{
    log_claim_outcome, BeganPause, ConflictReason, DeadlineRenewalOutcome, MarkRunningOutcome,
    PausedRegistryError, PausedRegistryState, PausedSandboxEntry, RegistryResult, ResumeClaim,
};
use crate::snapshot::SnapshotId;
use crate::types::{ExecutionId, SandboxId};

fn backend_err(operation: &'static str, err: sqlx::Error) -> PausedRegistryError {
    PausedRegistryError::backend(operation, err)
}

fn invalid(sandbox_id: &str, reason: impl Into<String>) -> PausedRegistryError {
    PausedRegistryError::InvalidRecord {
        sandbox_id: sandbox_id.to_string(),
        reason: reason.into(),
        source: None,
    }
}

/// Begins a pause and classifies refusals in the same transaction.
///
/// The shared transaction prevents classification against a newer row version.
pub async fn begin_pause(
    registry: &PostgresPausedSandboxRegistry,
    entry: &PausedSandboxEntry,
) -> RegistryResult<BeganPause> {
    let sandbox_id_str = entry.sandbox_id.to_string();
    let Some(metadata) = entry.metadata.as_ref() else {
        return Err(invalid(&sandbox_id_str, "sandbox metadata is missing"));
    };
    let metadata_json =
        serde_json::to_value(metadata).map_err(|e| PausedRegistryError::InvalidRecord {
            sandbox_id: sandbox_id_str.clone(),
            reason: "sandbox metadata is not serializable".to_string(),
            source: Some(e.into()),
        })?;
    // Fence on the execution ID inside metadata, the record being paused.
    let execution_id = metadata.execution_id;

    let mut tx = registry
        .pool
        .begin()
        .await
        .map_err(|e| backend_err("begin_pause", e))?;

    let row: Option<(i64, Option<String>)> = sqlx::query_as(sql::BEGIN_PAUSE_SQL)
        .bind(entry.sandbox_id.into_inner())
        .bind(registry.cluster_id)
        .bind(&entry.origin_node_id)
        .bind(&metadata_json)
        .bind(registry.lease_ttl_secs())
        .bind(execution_id.into_inner())
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| backend_err("begin_pause", e))?;

    let Some((generation, previous_snapshot_id)) = row else {
        // Classify on the same still-open transaction.
        return Err(classify_refused_pause(
            &mut *tx,
            registry.cluster_id,
            &sandbox_id_str,
            execution_id,
        )
        .await);
    };

    // Commit before parsing the unrelated previous snapshot ID.
    tx.commit()
        .await
        .map_err(|e| backend_err("begin_pause", e))?;

    let previous_snapshot_id = match previous_snapshot_id {
        None => None,
        Some(raw) => {
            Some(SnapshotId::parse(&raw).map_err(|e| invalid(&sandbox_id_str, e.to_string()))?)
        }
    };

    Ok(BeganPause {
        generation,
        previous_snapshot_id,
    })
}

// Reads without a cluster filter to distinguish cross-cluster ownership from
// execution fencing, using the caller's transaction.
async fn classify_refused_pause<'e, E>(
    conn: E,
    cluster_id: Uuid,
    sandbox_id: &str,
    execution_id: ExecutionId,
) -> PausedRegistryError
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    let parsed_sandbox_id = match Uuid::parse_str(sandbox_id) {
        Ok(id) => id,
        Err(_) => {
            return invalid(sandbox_id, "sandbox id is not a uuid");
        }
    };

    let row: Result<Option<(String, Option<String>)>, sqlx::Error> =
        sqlx::query_as(sql::CLASSIFY_REFUSED_PAUSE_SQL)
            .bind(parsed_sandbox_id)
            .fetch_optional(conn)
            .await;

    match row {
        Err(e) => backend_err("begin_pause", e),
        Ok(None) => {
            // A disappeared refused row is fenced; retrying could resurrect it.
            tracing::debug!(
                sandbox_id,
                %execution_id,
                "begin_pause refused: no registry row any more"
            );
            PausedRegistryError::ExecutionFenced {
                sandbox_id: sandbox_id.to_string(),
            }
        }
        Ok(Some((owner, observed))) if owner.eq_ignore_ascii_case(&cluster_id.to_string()) => {
            tracing::debug!(
                sandbox_id,
                %execution_id,
                observed = observed.as_deref().unwrap_or("none"),
                "begin_pause refused: sandbox belongs to a different incarnation"
            );
            PausedRegistryError::ExecutionFenced {
                sandbox_id: sandbox_id.to_string(),
            }
        }
        // Cross-cluster rows are reported instead of rewritten.
        Ok(Some(_)) => invalid(
            sandbox_id,
            "registry already holds this sandbox for a different cluster",
        ),
    }
}

/// Completes a publishing pause.
pub async fn complete_pause(
    registry: &PostgresPausedSandboxRegistry,
    sandbox_id: &SandboxId,
    generation: i64,
    snapshot_id: &SnapshotId,
) -> RegistryResult<()> {
    let result = sqlx::query(sql::COMPLETE_PAUSE_SQL)
        .bind(sandbox_id.into_inner())
        .bind(generation)
        .bind(snapshot_id.to_uuid())
        .bind(registry.lease_ttl_secs())
        .bind(registry.cluster_id)
        .execute(&registry.pool)
        .await
        .map_err(|e| backend_err("complete_pause", e))?;

    if result.rows_affected() == 0 {
        return Err(PausedRegistryError::GenerationConflict {
            sandbox_id: sandbox_id.to_string(),
            expected: generation,
        });
    }
    Ok(())
}

/// Marks a publishing pause local-only.
pub async fn mark_local_only(
    registry: &PostgresPausedSandboxRegistry,
    sandbox_id: &SandboxId,
    generation: i64,
) -> RegistryResult<()> {
    let result = sqlx::query(sql::MARK_LOCAL_ONLY_SQL)
        .bind(sandbox_id.into_inner())
        .bind(generation)
        .bind(registry.lease_ttl_secs())
        .bind(registry.cluster_id)
        .execute(&registry.pool)
        .await
        .map_err(|e| backend_err("mark_local_only", e))?;

    if result.rows_affected() == 0 {
        return Err(PausedRegistryError::GenerationConflict {
            sandbox_id: sandbox_id.to_string(),
            expected: generation,
        });
    }
    Ok(())
}

/// Claims a sandbox, optionally restricting takeover to durable paused rows.
pub async fn claim_for_resume(
    registry: &PostgresPausedSandboxRegistry,
    durable_only: bool,
    sandbox_id: &SandboxId,
    node_id: &str,
    execution_id: ExecutionId,
) -> RegistryResult<ResumeClaim> {
    let claim_sql = if durable_only {
        sql::claim_for_resume_durable_only_sql()
    } else {
        sql::claim_for_resume_sql()
    };

    let row: Option<ClaimRow> = sqlx::query_as(&claim_sql)
        .bind(sandbox_id.into_inner())
        .bind(node_id)
        .bind(registry.lease_ttl_secs())
        .bind(registry.cluster_id)
        .bind(execution_id.into_inner())
        .fetch_optional(&registry.pool)
        .await
        .map_err(|e| backend_err("claim_for_resume", e))?;

    match row {
        Some(row) => {
            let (entry, previous_state) = decode_claim(row)?;
            log_claim_outcome(sandbox_id, node_id, &entry, previous_state);
            Ok(ResumeClaim::Claimed {
                entry: Box::new(entry),
                previous_state,
            })
        }
        None => claim_for_resume_not_claimed(registry, sandbox_id).await,
    }
}

async fn claim_for_resume_not_claimed(
    registry: &PostgresPausedSandboxRegistry,
    sandbox_id: &SandboxId,
) -> RegistryResult<ResumeClaim> {
    let current = super::reads::get(registry, sandbox_id).await?;
    let Some(current) = current else {
        return Ok(ResumeClaim::NotFound);
    };
    match current.state {
        PausedRegistryState::Publishing | PausedRegistryState::LocalOnly => {
            Ok(ResumeClaim::NotReady {
                origin_node_id: current.origin_node_id,
            })
        }
        PausedRegistryState::Resuming | PausedRegistryState::Running => {
            let origin_node_id = current.claimed_by_node_id.unwrap_or(current.origin_node_id);
            Ok(ResumeClaim::Conflict {
                origin_node_id,
                reason: ConflictReason::LiveElsewhere,
            })
        }
        PausedRegistryState::Paused => Ok(ResumeClaim::Conflict {
            origin_node_id: current.origin_node_id,
            reason: ConflictReason::ClaimLost,
        }),
    }
}

/// Releases a resuming claim; a non-match is successful `false`.
pub async fn release_claim(
    registry: &PostgresPausedSandboxRegistry,
    sandbox_id: &SandboxId,
    generation: i64,
) -> RegistryResult<bool> {
    let result = sqlx::query(sql::RELEASE_CLAIM_SQL)
        .bind(sandbox_id.into_inner())
        .bind(generation)
        .bind(registry.lease_ttl_secs())
        .bind(registry.cluster_id)
        .execute(&registry.pool)
        .await
        .map_err(|e| backend_err("release_claim", e))?;

    Ok(result.rows_affected() > 0)
}

/// Marks a sandbox running with distinct claimant and holder identities.
///
/// Write and refusal classification share one transaction.
pub async fn mark_running(
    registry: &PostgresPausedSandboxRegistry,
    sandbox_id: &SandboxId,
    node_id: &str,
    holder_node_id: &str,
    execution_id: ExecutionId,
    expires_at: Option<DateTime<Utc>>,
) -> RegistryResult<MarkRunningOutcome> {
    // An empty holder means the claimant is also the physical holder.
    let holder = if holder_node_id.trim().is_empty() {
        node_id
    } else {
        holder_node_id
    };

    let mut tx = registry
        .pool
        .begin()
        .await
        .map_err(|e| backend_err("mark_running", e))?;

    let result = sqlx::query(sql::MARK_RUNNING_SQL)
        .bind(sandbox_id.into_inner())
        .bind(node_id)
        .bind(registry.lease_ttl_secs())
        .bind(registry.cluster_id)
        .bind(expires_at)
        .bind(execution_id.into_inner())
        .bind(holder)
        .execute(&mut *tx)
        .await
        .map_err(|e| backend_err("mark_running", e))?;

    if result.rows_affected() > 0 {
        tx.commit()
            .await
            .map_err(|e| backend_err("mark_running", e))?;
        return Ok(MarkRunningOutcome::Adopted);
    }

    // Classify a non-match on the same transaction as the write.
    let Some(entry) = super::reads::get_via_conn(&mut *tx, registry.cluster_id, sandbox_id).await?
    else {
        tx.commit()
            .await
            .map_err(|e| backend_err("mark_running", e))?;
        return Ok(MarkRunningOutcome::Untracked);
    };

    // Resuming and idempotent-running branches fence on different identities.
    let stale_incarnation = (entry.state == PausedRegistryState::Resuming
        && entry.claimed_by_node_id.as_deref() == Some(node_id))
        || (entry.state == PausedRegistryState::Running && entry.origin_node_id == holder);

    tx.commit()
        .await
        .map_err(|e| backend_err("mark_running", e))?;

    if stale_incarnation {
        return Err(PausedRegistryError::ExecutionFenced {
            sandbox_id: sandbox_id.to_string(),
        });
    }

    Ok(MarkRunningOutcome::HeldElsewhere)
}

/// Renews a deadline under execution fencing.
///
/// Write and refusal classification share one transaction.
pub async fn renew_sandbox_deadline(
    registry: &PostgresPausedSandboxRegistry,
    sandbox_id: &SandboxId,
    execution_id: ExecutionId,
    expires_at: Option<DateTime<Utc>>,
) -> RegistryResult<DeadlineRenewalOutcome> {
    let mut tx = registry
        .pool
        .begin()
        .await
        .map_err(|e| backend_err("renew_sandbox_deadline", e))?;

    let result = sqlx::query(sql::RENEW_SANDBOX_DEADLINE_SQL)
        .bind(sandbox_id.into_inner())
        .bind(execution_id.into_inner())
        .bind(expires_at)
        .bind(registry.cluster_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| backend_err("renew_sandbox_deadline", e))?;

    if result.rows_affected() > 0 {
        tx.commit()
            .await
            .map_err(|e| backend_err("renew_sandbox_deadline", e))?;
        return Ok(DeadlineRenewalOutcome::Renewed);
    }

    let outcome =
        match super::reads::get_via_conn(&mut *tx, registry.cluster_id, sandbox_id).await? {
            None => DeadlineRenewalOutcome::NotTracked,
            Some(_) => DeadlineRenewalOutcome::Superseded,
        };
    tx.commit()
        .await
        .map_err(|e| backend_err("renew_sandbox_deadline", e))?;
    Ok(outcome)
}

/// Removes one generation; a non-match is successful `false`.
pub async fn remove(
    registry: &PostgresPausedSandboxRegistry,
    sandbox_id: &SandboxId,
    generation: i64,
) -> RegistryResult<bool> {
    let result = sqlx::query(sql::REMOVE_SQL)
        .bind(sandbox_id.into_inner())
        .bind(registry.cluster_id)
        .bind(generation)
        .execute(&registry.pool)
        .await
        .map_err(|e| backend_err("remove", e))?;

    Ok(result.rows_affected() > 0)
}
