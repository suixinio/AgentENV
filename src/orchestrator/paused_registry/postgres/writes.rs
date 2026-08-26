//! The fencing write path: `begin_pause`, `complete_pause`, `mark_local_only`,
//! `claim_for_resume`, `release_claim`, `mark_running`,
//! `renew_sandbox_deadline`, `remove`.
//!
//! Every function here is a direct, single-connection-pool port of its
//! `services/scheduler/internal/registry/store_postgres.go` namesake; see
//! `sql.rs` for the SQL text itself. **D1's concurrency claim for every
//! statement in this file**: each is a single CAS'd `UPDATE`/`INSERT ...
//! RETURNING`, so PostgreSQL's own row-level locking serialises two
//! `--role api` replicas racing the same sandbox -- neither statement here
//! needs the caller to hold any lock of its own. See the Stage C report's
//! "D1" section for the exhaustive per-statement review.

use chrono::{DateTime, Utc};
use sqlx::PgPool;
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

/// `BeginPause` (`store.go`) + `classifyRefusedPause` (`store_postgres.go:
/// 598-634`), ported verbatim.
pub(super) async fn begin_pause(
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
    // 🔴 The execution id fenced against is the record's own
    // (`metadata.execution_id`), not `entry.execution_id` -- mirrors
    // `central.rs::begin_pause`'s identical choice: the run being paused,
    // read off the metadata rather than a separate argument a caller could
    // quote a different one through.
    let execution_id = metadata.execution_id;

    let row: Option<(i64, Option<String>)> = sqlx::query_as(&sql::begin_pause_sql())
        .bind(entry.sandbox_id.into_inner())
        .bind(registry.cluster_id)
        .bind(&entry.origin_node_id)
        .bind(&metadata_json)
        .bind(registry.lease_ttl_secs())
        .bind(execution_id.into_inner())
        .fetch_optional(&registry.pool)
        .await
        .map_err(|e| backend_err("begin_pause", e))?;

    let Some((generation, previous_snapshot_id)) = row else {
        return Err(classify_refused_pause(
            &registry.pool,
            registry.cluster_id,
            &sandbox_id_str,
            execution_id,
        )
        .await);
    };

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

/// `classifyRefusedPause` (`store_postgres.go:598-634`), ported: **reads
/// outside the cluster filter on purpose** -- the row `begin_pause` was
/// refused against may belong to a different cluster entirely, and which of
/// those two things happened is exactly what this distinguishes. (Go's
/// version also renders the observed incarnation into its error text; the
/// Rust `PausedRegistryError::ExecutionFenced`/`InvalidRecord` variants carry
/// only `sandbox_id` by design -- the detail is logged instead, at `debug`,
/// rather than smuggled into the error's `Display`.)
async fn classify_refused_pause(
    pool: &PgPool,
    cluster_id: Uuid,
    sandbox_id: &str,
    execution_id: ExecutionId,
) -> PausedRegistryError {
    let parsed_sandbox_id = match Uuid::parse_str(sandbox_id) {
        Ok(id) => id,
        Err(_) => {
            return invalid(sandbox_id, "sandbox id is not a uuid");
        }
    };

    let row: Result<Option<(String, Option<String>)>, sqlx::Error> =
        sqlx::query_as(sql::CLASSIFY_REFUSED_PAUSE_SQL)
            .bind(parsed_sandbox_id)
            .fetch_optional(pool)
            .await;

    match row {
        Err(e) => backend_err("begin_pause", e),
        Ok(None) => {
            // The row was there when the upsert ran -- that is why it
            // matched nothing -- and is gone now. Fenced, not retryable: the
            // row this pause meant to continue no longer exists, and
            // re-sending would insert a fresh one, resurrecting a sandbox
            // somebody deleted.
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
        // Told rather than silently rewritten: the row belongs to somebody
        // else's cluster.
        Ok(Some(_)) => invalid(
            sandbox_id,
            "registry already holds this sandbox for a different cluster",
        ),
    }
}

/// `CompletePause` (`completePauseSQL`, `store_postgres.go:648-664`).
pub(super) async fn complete_pause(
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

/// `MarkLocalOnly` (`markLocalOnlySQL`, `store_postgres.go:694-703`).
pub(super) async fn mark_local_only(
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

/// `ClaimForResume` (`store_postgres.go:815-930`), ported: `durable_only`
/// selects `claim_for_resume_durable_only_sql` in place of the full
/// three-way test -- the Rust equivalent of Go's
/// `!s.grace.allowsLeaseTakeover()` gate (see [`super::grace`]).
pub(super) async fn claim_for_resume(
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

/// `ReleaseClaim` (`releaseClaimSQL`, `store_postgres.go:971-986`): a
/// seizure, so zero rows affected is a plain, non-error `false` -- somebody
/// else already moved the row on, which is what this call wanted.
pub(super) async fn release_claim(
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

/// `MarkRunning` (`store_postgres.go:1196-1294`), ported: **D3's split**
/// stays split all the way through the bind list -- `node_id` (claimant,
/// `$2`, compared in every WHERE branch) and `holder_node_id` (`$7`, written
/// unconditionally into `origin_node_id`, compared only inside branch ③'s
/// retry check) are two distinct parameters end to end, never merged into
/// one before reaching SQL.
pub(super) async fn mark_running(
    registry: &PostgresPausedSandboxRegistry,
    sandbox_id: &SandboxId,
    node_id: &str,
    holder_node_id: &str,
    execution_id: ExecutionId,
    expires_at: Option<DateTime<Utc>>,
) -> RegistryResult<MarkRunningOutcome> {
    // `holderNodeID empty means "same as nodeID"` -- `MarkRunning`'s own doc
    // (`store.go`), ported verbatim: an older caller with nothing more
    // precise to say, or any backend that runs its own sandboxes (where
    // `node_id` already names the real machine).
    let holder = if holder_node_id.trim().is_empty() {
        node_id
    } else {
        holder_node_id
    };

    let result = sqlx::query(sql::MARK_RUNNING_SQL)
        .bind(sandbox_id.into_inner())
        .bind(node_id)
        .bind(registry.lease_ttl_secs())
        .bind(registry.cluster_id)
        .bind(expires_at)
        .bind(execution_id.into_inner())
        .bind(holder)
        .execute(&registry.pool)
        .await
        .map_err(|e| backend_err("mark_running", e))?;

    if result.rows_affected() > 0 {
        return Ok(MarkRunningOutcome::Adopted);
    }

    // Nothing matched. Re-read to tell "untracked", "someone else holds it"
    // and "this node's incarnation is stale" apart -- `MarkRunning`'s own
    // re-read, `store_postgres.go:1258-1282`.
    let Some(entry) = super::reads::get(registry, sandbox_id).await? else {
        return Ok(MarkRunningOutcome::Untracked);
    };

    // Read straight off the statement above: branches ① and ③ are the only
    // two carrying an incarnation clause, so a row eligible for either can
    // have failed on nothing else. Branch ③'s half compares against
    // `holder`, mirroring which identity that branch's WHERE clause reads.
    let stale_incarnation = (entry.state == PausedRegistryState::Resuming
        && entry.claimed_by_node_id.as_deref() == Some(node_id))
        || (entry.state == PausedRegistryState::Running && entry.origin_node_id == holder);

    if stale_incarnation {
        return Err(PausedRegistryError::ExecutionFenced {
            sandbox_id: sandbox_id.to_string(),
        });
    }

    Ok(MarkRunningOutcome::HeldElsewhere)
}

/// `RenewSandboxDeadline` (`renewSandboxDeadlineSQL`,
/// `store_postgres.go:1296-1382`): no node identity in the WHERE at all --
/// fenced on `execution_id` alone.
pub(super) async fn renew_sandbox_deadline(
    registry: &PostgresPausedSandboxRegistry,
    sandbox_id: &SandboxId,
    execution_id: ExecutionId,
    expires_at: Option<DateTime<Utc>>,
) -> RegistryResult<DeadlineRenewalOutcome> {
    let result = sqlx::query(sql::RENEW_SANDBOX_DEADLINE_SQL)
        .bind(sandbox_id.into_inner())
        .bind(execution_id.into_inner())
        .bind(expires_at)
        .bind(registry.cluster_id)
        .execute(&registry.pool)
        .await
        .map_err(|e| backend_err("renew_sandbox_deadline", e))?;

    if result.rows_affected() > 0 {
        return Ok(DeadlineRenewalOutcome::Renewed);
    }

    match super::reads::get(registry, sandbox_id).await? {
        None => Ok(DeadlineRenewalOutcome::NotTracked),
        Some(_) => Ok(DeadlineRenewalOutcome::Superseded),
    }
}

/// `Remove` (`removeSQL`, `store_postgres.go:1872-1909`): a non-match is a
/// plain, non-error `false` -- the row this caller meant to delete is
/// already gone, which is what it wanted.
pub(super) async fn remove(
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
