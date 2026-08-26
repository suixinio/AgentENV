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

use tracing::warn;
use uuid::Uuid;

use super::row::{decode_entry, decode_list_entry, decode_registry_row, EntryRow, RegistryRow};
use super::sql;
use super::PostgresPausedSandboxRegistry;
use crate::orchestrator::paused_registry::{
    PausedRegistryError, PausedRegistryListEntry, PausedRegistryListing, PausedSandboxEntry,
    RegistryResult,
};
use crate::types::SandboxId;

fn backend_err(operation: &'static str, err: sqlx::Error) -> PausedRegistryError {
    PausedRegistryError::backend(operation, err)
}

pub(super) async fn get(
    registry: &PostgresPausedSandboxRegistry,
    sandbox_id: &SandboxId,
) -> RegistryResult<Option<PausedSandboxEntry>> {
    get_via_conn(&registry.pool, registry.cluster_id, sandbox_id).await
}

/// [`get`]'s own query, over an arbitrary executor rather than
/// `registry.pool` specifically -- **B5**'s own reason to exist: a caller
/// that just wrote a conditional `UPDATE` and needs to classify why it
/// matched zero rows must read the row it is classifying on the *same*
/// connection/transaction as the write, or the read can land on a
/// different physical connection and see a version of the row the write
/// never observed (see `writes.rs`'s `mark_running`/`renew_sandbox_deadline`
/// for the two internal callers this exists for). Takes anything
/// implementing [`sqlx::Executor`] for `Postgres` -- `&PgPool` (this
/// function's own use above), `&mut PgConnection`, or `&mut Transaction<'_,
/// Postgres>` via `&mut *tx` all satisfy it.
pub(super) async fn get_via_conn<'e, E>(
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

    // 🔴 B6 (stop-gap): one row this crate cannot decode must not fail the
    // whole batch. `get_many` exists precisely so a reconciliation pass can
    // compare a node's roster against every row it holds in one round trip
    // (this function's own doc); a `?` here turns one unreadable row into
    // "every sandbox on this node looks untracked", which is a strictly
    // worse outcome than the one bad row alone. Skipped rows are counted and
    // named at `warn` so an operator can find and fix the record instead of
    // this silently losing sandboxes from every caller's view.
    //
    // Not the full fix -- see this crate's own module doc on `get_many`'s
    // trait contract for why the correct shape is a `Covered`/`Now`-style
    // result (mirroring Go's `GetManyResult`) rather than a plain map, which
    // is deferred to a later stage; this only stops one bad row from taking
    // down every other row in the same batch.
    let (out, skipped) = skip_bad_entries(rows);
    if skipped > 0 {
        warn!(
            target: "agentenv",
            skipped,
            requested = sandbox_ids.len(),
            "paused registry get_many: some rows were skipped rather than failing the whole batch"
        );
    }
    Ok(out)
}

/// The pure skip-and-count loop [`get_many`] runs -- pulled out so it is
/// testable without a database (see `#[cfg(test)] mod tests` below).
fn skip_bad_entries(rows: Vec<EntryRow>) -> (HashMap<SandboxId, PausedSandboxEntry>, u32) {
    let mut out = HashMap::with_capacity(rows.len());
    let mut skipped = 0u32;
    for row in rows {
        let sandbox_id = row.sandbox_id.clone();
        match decode_entry(row) {
            Ok(entry) => {
                out.insert(entry.sandbox_id, entry);
            }
            Err(err) => {
                skipped += 1;
                warn!(
                    target: "agentenv",
                    sandbox_id,
                    error = %err,
                    "paused registry get_many: skipping a row this build cannot decode"
                );
            }
        }
    }
    (out, skipped)
}

/// Every row in `cluster_id` -- the internal listing
/// [`super::reconcile::compute_reconcile`] compares against the heartbeat
/// roster. No pagination: `RunRegistryReconcile`'s own Go equivalent
/// (`PostgresReader::List`) has none either, and a cluster large enough for
/// that to matter is a scaling question for a later stage, not a
/// correctness one for this one.
///
/// 🔴 B6 (stop-gap): same reasoning as [`get_many`] above, and higher
/// stakes -- this is the reconcile leader's own per-tick read. Before this
/// fix, one row this build could not decode failed the whole `Vec::collect`,
/// which failed every tick's reconcile pass forever (the bad row never goes
/// away on its own), which stops Fix A's lease renewal for the *entire*
/// cluster -- precisely the "running row lease freeze" failure mode this
/// backend already exists to avoid (see B1's own doc). A skipped row is
/// counted and named at `warn` instead.
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

/// The pure skip-and-count loop [`list_registry_rows`] runs -- see
/// [`skip_bad_entries`]'s identical shape and reasoning.
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

/// `ListRegistrySandboxes`' own read -- every row in `cluster_id`, plus the
/// database clock they were read against, in one read-only transaction.
/// Ports Go's `PostgresReader.List` (`postgres.go:135-183`).
///
/// The two come from the same transaction deliberately, mirroring Go's own
/// comment on that method: `now()` is fixed at the transaction's start
/// regardless of isolation level, so reading it over a second round trip
/// would risk pairing a row set against a clock reading taken at a
/// different instant -- and every lease judgement downstream
/// (`LeaseExpiresAtUnixMs`/`SandboxExpiresAtUnixMs` against
/// `DatabaseNowUnixMs` on the wire) depends on that not happening. Rolled
/// back rather than committed when it succeeds, same as Go's `defer
/// tx.Rollback(ctx)`: read-only, so there is nothing to keep.
///
/// No pagination, matching [`list_registry_rows`]'s own reasoning (and
/// Go's `PostgresReader.List` having none either): filtering and paging are
/// `list_registry_sandboxes`'s job (`src/node_registry/grpc_service.rs`),
/// not this read's.
pub(super) async fn list_all(
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

    // Read-only: nothing to keep, so a rollback (matching Go's own
    // `defer tx.Rollback(ctx)`) is exactly as correct as a commit here and
    // costs nothing to prefer -- there is no write on this path to lose.
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

/// The pure skip-and-count loop [`list_all`] runs -- see [`skip_bad_entries`]'s
/// identical shape and reasoning: one row this build cannot decode must not
/// take an admin/debug listing of every other row down with it.
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

    /// A minimal, decodable [`EntryRow`] -- every field a real row would
    /// carry for a `paused` sandbox, so `decode_entry`/`decode_registry_row`
    /// both succeed on it. Metadata comes from `SandboxMetadata::default()`
    /// (the same technique `postgres::contract`'s own `entry()` helper
    /// uses) rather than a hand-written JSON literal, so this stays correct
    /// across `SandboxMetadata`'s own field changes instead of drifting into
    /// a shape the real decoder no longer accepts.
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

    /// A row that fails to decode, however far its own state gets it: an
    /// unrecognised `state` string, which no CHECK constraint short of a
    /// direct hand-corruption (or a build mismatch reading a differently
    /// migrated table) should ever actually produce -- but exactly the
    /// shape [`skip_bad_entries`]/[`skip_bad_registry_rows`] exist to
    /// survive rather than propagate.
    fn unreadable_row(sandbox_id: Uuid, cluster_id: Uuid) -> EntryRow {
        let mut row = good_row(sandbox_id, cluster_id);
        row.state = "not_a_real_state".to_string();
        row
    }

    /// The core B6 claim for `get_many`: one bad row must not take the good
    /// one down with it.
    #[test]
    fn skip_bad_entries_keeps_the_good_row_and_counts_the_bad_one() {
        let cluster_id = Uuid::new_v4();
        let good_id = Uuid::new_v4();
        let bad_id = Uuid::new_v4();
        let rows = vec![
            good_row(good_id, cluster_id),
            unreadable_row(bad_id, cluster_id),
        ];

        let (out, skipped) = skip_bad_entries(rows);
        assert_eq!(skipped, 1, "exactly the one unreadable row must be skipped");
        assert_eq!(out.len(), 1, "the good row must still be present");
        assert!(
            out.contains_key(&SandboxId::parse_str(&good_id.to_string()).unwrap()),
            "the good row's own sandbox id must be the one that survived"
        );
    }

    /// A batch with nothing wrong in it must report zero skipped -- this
    /// fix must not turn a clean batch into a partially-reported one.
    #[test]
    fn skip_bad_entries_reports_nothing_skipped_when_every_row_decodes() {
        let cluster_id = Uuid::new_v4();
        let rows = vec![
            good_row(Uuid::new_v4(), cluster_id),
            good_row(Uuid::new_v4(), cluster_id),
        ];

        let (out, skipped) = skip_bad_entries(rows);
        assert_eq!(skipped, 0);
        assert_eq!(out.len(), 2);
    }

    /// The identical claim for [`list_registry_rows`]'s own loop -- the one
    /// that, before this fix, could stop Fix A's cluster-wide lease renewal
    /// forever on a single bad row (see this module's own doc on
    /// `list_registry_rows`).
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

    /// The identical claim for [`list_all`]'s own loop -- `ListRegistrySandboxes`
    /// is an admin/debug listing of the whole table, and one row this build
    /// cannot decode must not blank out every other sandbox in the answer.
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

    /// A clean batch must report zero skipped -- mirrors
    /// `skip_bad_entries_reports_nothing_skipped_when_every_row_decodes`.
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

    /// [`decode_list_entry`] must carry the lease/execution-id columns
    /// [`decode_entry`] leaves out -- the whole reason this listing exists
    /// as its own decode rather than reusing that one. A row with every one
    /// of those columns populated must come back with all of them, not
    /// silently dropped the way the trait-facing `PausedSandboxEntry` drops
    /// them.
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
