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
        // 🔴 Asked nothing, covers nothing. The caller compares `covered`
        // against the ids it passed, so an empty request is satisfied by an
        // empty coverage list -- not by a claim to have answered for ids that
        // were never in the batch.
        return Ok(PausedRegistryRows::default());
    }

    let ids: Vec<Uuid> = sandbox_ids.iter().map(|id| id.into_inner()).collect();
    let rows: Vec<EntryRow> = sqlx::query_as(&sql::get_many_sql())
        .bind(registry.cluster_id)
        .bind(&ids)
        .fetch_all(&registry.pool)
        .await
        .map_err(|e| backend_err("get_many", e))?;

    // 🔴 One row this crate cannot decode must not fail the whole batch,
    // and must not be reported as an absence either. `get_many` exists so a
    // reconciliation pass can compare a node's whole roster against the
    // registry in one round trip (the trait's own doc); a `?` here turns one
    // unreadable row into "every sandbox on this node looks untracked", while
    // silently dropping it turns that one row into "this sandbox is gone" --
    // and the caller destroys a paused record or a live VM on exactly that
    // answer. Both are wrong in the destructive direction, so the row is
    // dropped from `entries` *and* withheld from `covered`: the batch keeps
    // answering for every other id and declines to answer for this one.
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

/// The pure decode-and-account loop [`get_many`] runs -- pulled out so it is
/// testable without a database (see `#[cfg(test)] mod tests` below).
///
/// `covered` is built from `requested` rather than from the rows that came
/// back, because absence is a real answer here: an id the cluster holds no row
/// for is answered for, and the caller is entitled to act on it. What removes
/// an id from `covered` is a row that exists and could not be read.
///
/// 🔴 A row whose own `sandbox_id` column will not parse cannot be
/// attributed to any requested id, so there is no single id to withhold. The
/// batch then covers nothing: with an unattributable row in the result set,
/// *any* of the requested ids could be the one whose row was unreadable, and
/// "some absence in here is a lie" is not a state a destructive caller can
/// safely act on for the rest.
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

/// Every row in `cluster_id` -- the internal listing
/// [`super::reconcile::compute_reconcile`] compares against the heartbeat
/// roster. No pagination: `RunRegistryReconcile`'s own Go equivalent
/// (`PostgresReader::List`) has none either, and a cluster large enough for
/// that to matter is a scaling question for a later stage, not a
/// correctness one for this one.
///
/// 🔴 Skip-and-count, and unlike [`get_many`] that is the whole fix here rather
/// than half of one. One row this build could not decode used to fail the whole
/// `Vec::collect`, which failed every tick's reconcile pass forever (the bad row
/// never goes away on its own), which stops Fix A's lease renewal for the
/// *entire* cluster -- precisely the "running row lease freeze" failure mode
/// this backend already exists to avoid (see B1's own doc). A skipped row is
/// counted and named at `warn` instead, and needs no coverage list on top
/// because this listing's only consumer counts rows rather than acting on their
/// absence -- see [`skip_bad_registry_rows`].
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

/// The pure skip-and-count loop [`list_registry_rows`] runs.
///
/// Still a plain skip-and-count, unlike [`collect_entries`]: this listing's
/// consumer is [`super::reconcile::compute_reconcile`], which counts metrics
/// over the rows it is given and never acts on a row's *absence*. A skipped row
/// costs this pass its contribution to those counters and nothing else, so
/// there is no absence for a coverage list to protect here.
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

/// The pure skip-and-count loop [`list_all`] runs: one row this build cannot
/// decode must not take an admin/debug listing of every other row down with it.
///
/// No coverage list, for [`skip_bad_registry_rows`]'s reason -- this feeds
/// `ListRegistrySandboxes`, a read-only listing whose callers report what they
/// were shown rather than destroying what they were not.
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
    /// shape [`collect_entries`]/[`skip_bad_registry_rows`] exist to
    /// survive rather than propagate.
    fn unreadable_row(sandbox_id: Uuid, cluster_id: Uuid) -> EntryRow {
        let mut row = good_row(sandbox_id, cluster_id);
        row.state = "not_a_real_state".to_string();
        row
    }

    fn sid(raw: Uuid) -> SandboxId {
        SandboxId::parse_str(&raw.to_string()).unwrap()
    }

    /// One bad row must not take the good one down with it.
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

    /// 🔴 The A1 claim itself: an unreadable row is withheld from `covered`, so
    /// a caller that destroys on absence cannot read it as "this sandbox is
    /// gone". Without this the row simply vanishes from the map and
    /// `reap_superseded_running_sandboxes` tears down a live VM.
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

    /// An id the cluster holds no row for is still *answered for*: absence is a
    /// real result, and reconciliation is entitled to act on it. Only a row
    /// that exists and cannot be read costs coverage.
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

    /// 🔴 A row whose own id column will not parse cannot be attributed to a
    /// requested id, so no single id can be withheld -- and any of them could
    /// be the one. The batch then covers nothing rather than covering the rest
    /// on a guess.
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

    /// A batch with nothing wrong in it must report zero skipped and full
    /// coverage -- this fix must not turn a clean batch into a partially
    /// reported one.
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
    /// `collect_entries_reports_nothing_skipped_when_every_row_decodes`.
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
