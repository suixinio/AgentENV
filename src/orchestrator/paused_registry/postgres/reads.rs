//! `Get`/`GetMany` (`store_postgres.go:251-267`, `:317-330`), ported.
//!
//! 🔴 §7 point 1 of the plan doc ("两套并行的读模型"): Go keeps two
//! independent column lists (`entryColumns` for the write path, `selectColumns`
//! for the read-only `postgres.go`/`registry.go` surface) that can drift
//! apart -- `postgres.go`'s own comment names the failure mode ("adding a
//! column to one and not the other does not fail: it makes that column read
//! as empty"). This port has exactly one column list
//! ([`super::sql::ENTRY_COLUMNS`]) and one row type
//! ([`super::row::EntryRow`]/[`super::row::RegistryRow`]): every reader in
//! this backend, trait-facing or internal, is built from the same query
//! shape, so there is nothing left to drift.

use std::collections::HashMap;

use uuid::Uuid;

use super::row::{decode_entry, decode_registry_row, EntryRow, RegistryRow};
use super::sql;
use super::PostgresPausedSandboxRegistry;
use crate::orchestrator::paused_registry::{
    PausedRegistryError, PausedSandboxEntry, RegistryResult,
};
use crate::types::SandboxId;

fn backend_err(operation: &'static str, err: sqlx::Error) -> PausedRegistryError {
    PausedRegistryError::backend(operation, err)
}

pub(super) async fn get(
    registry: &PostgresPausedSandboxRegistry,
    sandbox_id: &SandboxId,
) -> RegistryResult<Option<PausedSandboxEntry>> {
    let row: Option<EntryRow> = sqlx::query_as(&sql::get_sql())
        .bind(sandbox_id.into_inner())
        .bind(registry.cluster_id)
        .fetch_optional(&registry.pool)
        .await
        .map_err(|e| backend_err("get", e))?;

    row.map(decode_entry).transpose()
}

pub(super) async fn get_many(
    registry: &PostgresPausedSandboxRegistry,
    sandbox_ids: &[SandboxId],
) -> RegistryResult<HashMap<SandboxId, PausedSandboxEntry>> {
    if sandbox_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let ids: Vec<Uuid> = sandbox_ids.iter().map(|id| id.into_inner()).collect();
    let rows: Vec<EntryRow> = sqlx::query_as(&sql::get_many_sql())
        .bind(registry.cluster_id)
        .bind(&ids)
        .fetch_all(&registry.pool)
        .await
        .map_err(|e| backend_err("get_many", e))?;

    let mut out = HashMap::with_capacity(rows.len());
    for row in rows {
        let entry = decode_entry(row)?;
        out.insert(entry.sandbox_id, entry);
    }
    Ok(out)
}

/// Every row in `cluster_id` -- the internal listing
/// [`super::reconcile::compute_reconcile`] compares against the heartbeat
/// roster. No pagination: `RunRegistryReconcile`'s own Go equivalent
/// (`PostgresReader::List`) has none either, and a cluster large enough for
/// that to matter is a scaling question for a later stage, not a
/// correctness one for this one.
pub(super) async fn list_registry_rows(
    registry: &PostgresPausedSandboxRegistry,
) -> RegistryResult<Vec<RegistryRow>> {
    let query = format!(
        "SELECT {} FROM paused_sandboxes WHERE cluster_id = $1::uuid",
        sql::ENTRY_COLUMNS
    );
    let rows: Vec<EntryRow> = sqlx::query_as(&query)
        .bind(registry.cluster_id)
        .fetch_all(&registry.pool)
        .await
        .map_err(|e| backend_err("list", e))?;

    rows.into_iter().map(decode_registry_row).collect()
}
