//! Identity-checked renewal for parked and running paused-registry leases.

use chrono::{DateTime, Utc};
use uuid::Uuid;

use super::sql;
use super::PostgresPausedSandboxRegistry;
use crate::orchestrator::paused_registry::{HeldSandbox, PausedRegistryError, RegistryResult};
use crate::types::SandboxId;

fn backend_err(operation: &'static str, err: sqlx::Error) -> PausedRegistryError {
    PausedRegistryError::backend(operation, err)
}

/// Renews held rows that still belong to `node_id`.
pub async fn renew_lease(
    registry: &PostgresPausedSandboxRegistry,
    node_id: &str,
    held: &[HeldSandbox],
) -> RegistryResult<u64> {
    if held.is_empty() {
        return Ok(0);
    }

    let ids: Vec<Uuid> = held.iter().map(|h| h.sandbox_id.into_inner()).collect();
    let expires: Vec<Option<DateTime<Utc>>> = held.iter().map(|h| h.expires_at).collect();

    let result = sqlx::query(sql::RENEW_LEASE_SQL)
        .bind(registry.lease_ttl_secs())
        .bind(registry.cluster_id)
        .bind(&ids)
        .bind(&expires)
        .bind(node_id)
        .execute(&registry.pool)
        .await
        .map_err(|e| backend_err("renew_lease", e))?;

    Ok(result.rows_affected())
}

/// Caller-asserted `(sandbox, node)` pair; SQL rechecks the row's holder.
#[derive(Debug, Clone)]
pub struct LeaseHolder {
    pub sandbox_id: SandboxId,
    pub node_id: String,
}

/// Renews `publishing` and `local_only` leases without changing deadlines.
pub async fn renew_parked_leases(
    registry: &PostgresPausedSandboxRegistry,
    holders: &[LeaseHolder],
) -> RegistryResult<u64> {
    renew_heartbeat_leases(
        registry,
        sql::RENEW_PARKED_LEASE_SQL,
        "renew_parked_leases",
        holders,
    )
    .await
}

/// Renews `running` leases without changing deadlines.
pub async fn renew_live_leases(
    registry: &PostgresPausedSandboxRegistry,
    holders: &[LeaseHolder],
) -> RegistryResult<u64> {
    renew_heartbeat_leases(
        registry,
        sql::RENEW_LIVE_LEASE_SQL,
        "renew_live_leases",
        holders,
    )
    .await
}

async fn renew_heartbeat_leases(
    registry: &PostgresPausedSandboxRegistry,
    statement: &str,
    operation: &'static str,
    holders: &[LeaseHolder],
) -> RegistryResult<u64> {
    if holders.is_empty() {
        return Ok(0);
    }

    let ids: Vec<Uuid> = holders.iter().map(|h| h.sandbox_id.into_inner()).collect();
    let node_ids: Vec<&str> = holders.iter().map(|h| h.node_id.as_str()).collect();

    let result = sqlx::query(statement)
        .bind(registry.lease_ttl_secs())
        .bind(registry.cluster_id)
        .bind(&ids)
        .bind(&node_ids)
        .execute(&registry.pool)
        .await
        .map_err(|e| backend_err(operation, e))?;

    Ok(result.rows_affected())
}
