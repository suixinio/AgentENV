//! Paused-registry reads share [`super::sql::ENTRY_COLUMNS`] and one row
//! decoder so trait and internal query shapes cannot drift.

use std::collections::{HashMap, HashSet};

use tracing::warn;
use uuid::Uuid;

use super::row::{decode_entry, decode_list_entry, decode_registry_row, EntryRow, RegistryRow};
use super::sql;
use super::PostgresPausedSandboxRegistry;
use crate::orchestrator::paused_registry::{
    PausedRegistryError, PausedRegistryListEntry, PausedRegistryListing, PausedRegistryRows,
    PausedSandboxEntry, RegistryResult,
};
use crate::types::SandboxId;

fn backend_err(operation: &'static str, err: sqlx::Error) -> PausedRegistryError {
    PausedRegistryError::backend(operation, err)
}

pub async fn get(
    registry: &PostgresPausedSandboxRegistry,
    sandbox_id: &SandboxId,
) -> RegistryResult<Option<PausedSandboxEntry>> {
    get_via_conn(&registry.pool, registry.cluster_id, sandbox_id).await
}

/// Reads on the caller's executor so zero-row write outcomes can be classified
/// against the same connection or transaction.
pub async fn get_via_conn<'e, E>(
    executor: E,
    cluster_id: Uuid,
    sandbox_id: &SandboxId,
) -> RegistryResult<Option<PausedSandboxEntry>>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    let row: Option<EntryRow> = sqlx::query_as(&sql::get_sql())
        .bind(sandbox_id.into_inner())
        .bind(cluster_id)
        .fetch_optional(executor)
        .await
        .map_err(|e| backend_err("get", e))?;

    row.map(decode_entry).transpose()
}

pub async fn get_many(
    registry: &PostgresPausedSandboxRegistry,
    sandbox_ids: &[SandboxId],
) -> RegistryResult<PausedRegistryRows> {
    if sandbox_ids.is_empty() {
        // An empty request covers no IDs.
        return Ok(PausedRegistryRows::default());
    }

    let ids: Vec<Uuid> = sandbox_ids.iter().map(|id| id.into_inner()).collect();
    let rows: Vec<EntryRow> = sqlx::query_as(&sql::get_many_sql())
        .bind(registry.cluster_id)
        .bind(&ids)
        .fetch_all(&registry.pool)
        .await
        .map_err(|e| backend_err("get_many", e))?;

    // Undecodable rows are omitted from entries and coverage so destructive
    // callers cannot mistake them for absent sandboxes.
    let (rows, skipped) = collect_entries(sandbox_ids, rows);
    if skipped > 0 {
        warn!(
            target: "agentenv",
            skipped,
            requested = sandbox_ids.len(),
            covered = rows.covered.len(),
            "paused registry get_many: undecodable rows were withheld from the batch's coverage"
        );
    }
    Ok(rows)
}

// Coverage includes real absences but excludes undecodable rows. If a bad row
// cannot be attributed to an ID, cover nothing.
fn collect_entries(requested: &[SandboxId], rows: Vec<EntryRow>) -> (PausedRegistryRows, u32) {
    let mut entries = HashMap::with_capacity(rows.len());
    let mut undecodable: HashSet<SandboxId> = HashSet::new();
    let mut unattributable = 0u32;
    let mut skipped = 0u32;

    for row in rows {
        let raw_id = row.sandbox_id.clone();
        match decode_entry(row) {
            Ok(entry) => {
                entries.insert(entry.sandbox_id, entry);
            }
            Err(err) => {
                skipped += 1;
                match SandboxId::parse_str(&raw_id) {
                    Ok(sandbox_id) => {
                        undecodable.insert(sandbox_id);
                    }
                    Err(_) => unattributable += 1,
                }
                warn!(
                    target: "agentenv",
                    sandbox_id = raw_id,
                    error = %err,
                    "paused registry get_many: withholding a row this build cannot decode"
                );
            }
        }
    }

    let covered = if unattributable > 0 {
        Vec::new()
    } else {
        requested
            .iter()
            .copied()
            .filter(|id| !undecodable.contains(id))
            .collect()
    };

    (PausedRegistryRows { entries, covered }, skipped)
}

/// Lists decodable registry rows for reconciliation, warning on skipped rows.
pub async fn list_registry_rows(
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

    let (out, skipped) = skip_bad_registry_rows(rows);
    if skipped > 0 {
        warn!(
            target: "agentenv",
            skipped,
            total = out.len() + skipped as usize,
            "paused registry list: some rows were skipped rather than failing the whole reconcile pass"
        );
    }
    Ok(out)
}

// Reconciliation only aggregates returned rows, so skipped rows need no
// destructive-read coverage accounting.
fn skip_bad_registry_rows(rows: Vec<EntryRow>) -> (Vec<RegistryRow>, u32) {
    let mut out = Vec::with_capacity(rows.len());
    let mut skipped = 0u32;
    for row in rows {
        let sandbox_id = row.sandbox_id.clone();
        match decode_registry_row(row) {
            Ok(row) => out.push(row),
            Err(err) => {
                skipped += 1;
                warn!(
                    target: "agentenv",
                    sandbox_id,
                    error = %err,
                    "paused registry list: skipping a row this build cannot decode"
                );
            }
        }
    }
    (out, skipped)
}

/// Lists rows and the database time from one read-only transaction.
///
/// The shared transaction keeps lease comparisons on a single clock instant.
pub async fn list_all(
    registry: &PostgresPausedSandboxRegistry,
) -> RegistryResult<PausedRegistryListing> {
    let mut tx = registry
        .pool
        .begin()
        .await
        .map_err(|e| backend_err("list_all", e))?;

    let now: chrono::DateTime<chrono::Utc> = sqlx::query_scalar("SELECT now()")
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| backend_err("list_all", e))?;

    let rows: Vec<EntryRow> = sqlx::query_as(&sql::list_all_sql())
        .bind(registry.cluster_id)
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| backend_err("list_all", e))?;

    tx.rollback()
        .await
        .map_err(|e| backend_err("list_all", e))?;

    let (out, skipped) = skip_bad_list_entries(rows);
    if skipped > 0 {
        warn!(
            target: "agentenv",
            skipped,
            total = out.len() + skipped as usize,
            "paused registry list_all: some rows were skipped rather than failing the whole listing"
        );
    }
    Ok(PausedRegistryListing {
        sandboxes: out,
        now,
    })
}

fn skip_bad_list_entries(rows: Vec<EntryRow>) -> (Vec<PausedRegistryListEntry>, u32) {
    let mut out = Vec::with_capacity(rows.len());
    let mut skipped = 0u32;
    for row in rows {
        let sandbox_id = row.sandbox_id.clone();
        match decode_list_entry(row) {
            Ok(entry) => out.push(entry),
            Err(err) => {
                skipped += 1;
                warn!(
                    target: "agentenv",
                    sandbox_id,
                    error = %err,
                    "paused registry list_all: skipping a row this build cannot decode"
                );
            }
        }
    }
    (out, skipped)
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::*;
    use crate::orchestrator::store::SandboxMetadata;
    use crate::types::ExecutionId;

    fn good_row(sandbox_id: Uuid, cluster_id: Uuid) -> EntryRow {
        let id = SandboxId::parse_str(&sandbox_id.to_string()).unwrap();
        let metadata = SandboxMetadata {
            id,
            execution_id: ExecutionId::new(),
            ..SandboxMetadata::default()
        };
        EntryRow {
            sandbox_id: sandbox_id.to_string(),
            cluster_id: cluster_id.to_string(),
            state: "paused".to_string(),
            generation: 1,
            origin_node_id: "node-a".to_string(),
            claimed_by_node_id: None,
            snapshot_id: Some(Uuid::new_v4().to_string()),
            metadata: serde_json::to_value(&metadata).expect("metadata should serialize"),
            paused_at: Utc::now(),
            updated_at: Utc::now(),
            lease_expires_at: None,
            sandbox_expires_at: None,
            execution_id: None,
            execution_started_at: None,
        }
    }

    fn unreadable_row(sandbox_id: Uuid, cluster_id: Uuid) -> EntryRow {
        let mut row = good_row(sandbox_id, cluster_id);
        row.state = "not_a_real_state".to_string();
        row
    }

    fn sid(raw: Uuid) -> SandboxId {
        SandboxId::parse_str(&raw.to_string()).unwrap()
    }

    #[test]
    fn collect_entries_keeps_the_good_row_and_counts_the_bad_one() {
        let cluster_id = Uuid::new_v4();
        let good_id = Uuid::new_v4();
        let bad_id = Uuid::new_v4();
        let requested = vec![sid(good_id), sid(bad_id)];
        let rows = vec![
            good_row(good_id, cluster_id),
            unreadable_row(bad_id, cluster_id),
        ];

        let (rows, skipped) = collect_entries(&requested, rows);
        assert_eq!(skipped, 1, "exactly the one unreadable row must be skipped");
        assert_eq!(rows.entries.len(), 1, "the good row must still be present");
        assert!(
            rows.entries.contains_key(&sid(good_id)),
            "the good row's own sandbox id must be the one that survived"
        );
    }

    #[test]
    fn collect_entries_withholds_coverage_for_a_row_it_cannot_decode() {
        let cluster_id = Uuid::new_v4();
        let good_id = Uuid::new_v4();
        let bad_id = Uuid::new_v4();
        let requested = vec![sid(good_id), sid(bad_id)];
        let rows = vec![
            good_row(good_id, cluster_id),
            unreadable_row(bad_id, cluster_id),
        ];

        let (rows, _) = collect_entries(&requested, rows);
        assert!(
            !rows.covers(&requested),
            "a batch holding an undecodable row must not claim to cover it"
        );
        assert_eq!(rows.covered, vec![sid(good_id)]);
        assert!(
            !rows.covered.contains(&sid(bad_id)),
            "the undecodable row's id is exactly the one absence must not be trusted for"
        );
    }

    #[test]
    fn collect_entries_covers_an_id_with_no_row_at_all() {
        let cluster_id = Uuid::new_v4();
        let present = Uuid::new_v4();
        let absent = Uuid::new_v4();
        let requested = vec![sid(present), sid(absent)];

        let (rows, skipped) = collect_entries(&requested, vec![good_row(present, cluster_id)]);
        assert_eq!(skipped, 0);
        assert!(rows.entries.contains_key(&sid(present)));
        assert!(!rows.entries.contains_key(&sid(absent)));
        assert!(
            rows.covers(&requested),
            "an absent row is an answer, not a gap in coverage"
        );
    }

    #[test]
    fn collect_entries_covers_nothing_when_a_bad_row_cannot_be_attributed() {
        let cluster_id = Uuid::new_v4();
        let good_id = Uuid::new_v4();
        let requested = vec![sid(good_id), sid(Uuid::new_v4())];
        let mut orphan = good_row(Uuid::new_v4(), cluster_id);
        orphan.sandbox_id = "not-a-uuid".to_string();

        let (rows, skipped) =
            collect_entries(&requested, vec![good_row(good_id, cluster_id), orphan]);
        assert_eq!(skipped, 1);
        assert!(
            rows.covered.is_empty(),
            "an unattributable bad row makes every absence in the batch untrustworthy"
        );
        assert!(
            rows.entries.contains_key(&sid(good_id)),
            "the rows that did decode are still returned; only coverage is withheld"
        );
    }

    #[test]
    fn collect_entries_reports_nothing_skipped_when_every_row_decodes() {
        let cluster_id = Uuid::new_v4();
        let first = Uuid::new_v4();
        let second = Uuid::new_v4();
        let requested = vec![sid(first), sid(second)];
        let rows = vec![good_row(first, cluster_id), good_row(second, cluster_id)];

        let (rows, skipped) = collect_entries(&requested, rows);
        assert_eq!(skipped, 0);
        assert_eq!(rows.entries.len(), 2);
        assert!(rows.covers(&requested));
    }

    #[test]
    fn skip_bad_registry_rows_keeps_the_good_row_and_counts_the_bad_one() {
        let cluster_id = Uuid::new_v4();
        let good_id = Uuid::new_v4();
        let bad_id = Uuid::new_v4();
        let rows = vec![
            good_row(good_id, cluster_id),
            unreadable_row(bad_id, cluster_id),
        ];

        let (out, skipped) = skip_bad_registry_rows(rows);
        assert_eq!(skipped, 1);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].sandbox_id.to_string(), good_id.to_string());
    }

    #[test]
    fn skip_bad_list_entries_keeps_the_good_row_and_counts_the_bad_one() {
        let cluster_id = Uuid::new_v4();
        let good_id = Uuid::new_v4();
        let bad_id = Uuid::new_v4();
        let rows = vec![
            good_row(good_id, cluster_id),
            unreadable_row(bad_id, cluster_id),
        ];

        let (out, skipped) = skip_bad_list_entries(rows);
        assert_eq!(skipped, 1, "exactly the one unreadable row must be skipped");
        assert_eq!(out.len(), 1, "the good row must still be present");
        assert_eq!(out[0].sandbox_id.to_string(), good_id.to_string());
    }

    #[test]
    fn skip_bad_list_entries_reports_nothing_skipped_when_every_row_decodes() {
        let cluster_id = Uuid::new_v4();
        let rows = vec![
            good_row(Uuid::new_v4(), cluster_id),
            good_row(Uuid::new_v4(), cluster_id),
        ];

        let (out, skipped) = skip_bad_list_entries(rows);
        assert_eq!(skipped, 0);
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn decode_list_entry_carries_the_lease_and_execution_columns() {
        let cluster_id = Uuid::new_v4();
        let sandbox_id = Uuid::new_v4();
        let mut row = good_row(sandbox_id, cluster_id);
        let lease_expires_at = Utc::now();
        let sandbox_expires_at = Utc::now();
        let execution_id = ExecutionId::new();
        row.lease_expires_at = Some(lease_expires_at);
        row.sandbox_expires_at = Some(sandbox_expires_at);
        row.execution_id = Some(execution_id.to_string());

        let entry = decode_list_entry(row).expect("a well-formed row must decode");
        assert_eq!(entry.lease_expires_at, Some(lease_expires_at));
        assert_eq!(entry.sandbox_expires_at, Some(sandbox_expires_at));
        assert_eq!(entry.execution_id, Some(execution_id));
        assert_eq!(entry.holder(), "node-a", "Holder() must be origin_node_id");
    }
}
