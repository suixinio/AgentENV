//! Lease renewal: `renew_lease` (the trait method, one node/replica
//! reporting its own roster under its own identity) and the two
//! heartbeat-driven, internal-only siblings Fix A/Fix B's reconcile loop
//! uses -- `renew_parked_leases`/`renew_live_leases`, Go's
//! `RenewParkedLeases`/`RenewLiveLeases` (`store.go`'s
//! `ParkedLeaseRenewer`/`HeartbeatLeaseRenewer` sub-interfaces). Neither of
//! the latter two is part of [`crate::orchestrator::paused_registry::
//! PausedSandboxRegistry`] -- Go never exposed them over the `PausedRegistry`
//! gRPC surface either, since the only caller is the reconcile loop itself.

use chrono::{DateTime, Utc};
use uuid::Uuid;

use super::sql;
use super::PostgresPausedSandboxRegistry;
use crate::orchestrator::paused_registry::{HeldSandbox, PausedRegistryError, RegistryResult};
use crate::types::SandboxId;

fn backend_err(operation: &'static str, err: sqlx::Error) -> PausedRegistryError {
    PausedRegistryError::backend(operation, err)
}

/// `RenewLease` (`renewLeaseSQL`, `store_postgres.go:1388-1412`): `$5` is
/// the caller's own asserted identity. See D2 Fix A's doc in `sql.rs` for
/// why this never renews a `Running` row's lease under the split node/api
/// identity model -- that state has [`renew_live_leases`] instead, driven by
/// the reconcile loop rather than by this per-replica call.
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

/// One `(sandbox, node)` pair a caller asserts -- `ParkedLeaseHolder`
/// (`store.go`). Caller-asserted, not caller-identity: the SQL's own WHERE
/// re-checks `node_id` against the row's `origin_node_id` before renewing
/// anything, so a wrong assertion here renews nothing rather than corrupting
/// a row -- see [`super::sql::RENEW_PARKED_LEASE_SQL`]/
/// [`super::sql::RENEW_LIVE_LEASE_SQL`]'s own doc.
#[derive(Debug, Clone)]
pub struct LeaseHolder {
    pub sandbox_id: SandboxId,
    pub node_id: String,
}

/// `RenewParkedLeases` (`renewParkedLeaseSQL`, `store_postgres.go:
/// 1454-1486`): `publishing`/`local_only` rows only, `sandbox_expires_at`
/// untouched.
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

/// `RenewLiveLeases` (`renewLiveLeaseSQL`, `store_postgres.go:1529-1556`) --
/// Fix A (`151d00b`): `running` rows only, `sandbox_expires_at` untouched.
/// This is the statement that makes a healthy node's lease survive under
/// the split node/api identity model; see the Stage C report's D2 section.
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
