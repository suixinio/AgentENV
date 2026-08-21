//! What each catalog actually holds, compared directly.
//!
//! 🔴 This exists because every *other* number that guards the read-side switch
//! describes the **queue**, not the two catalogs.
//!
//! - `mirror_lag{central}` counts writes waiting to be replayed.
//! - `mirror_diverged{central}` counts writes replay could never settle.
//! - The history marker records that the object store's pre-existing rows were
//!   enumerated once.
//!
//! Each of those has been observed reading zero on a cluster where PostgreSQL
//! held **0 rows** and object storage held **32** — twice, by two different
//! mechanisms. The second time was worse than the first: clearing PostgreSQL
//! without clearing the node-local mirror store leaves the marker on disk, so
//! the backfill returns immediately, nothing is queued, nothing is compared,
//! and all three numbers agree that the two catalogs agree.
//!
//! A count of one store against a count of the other is the one check in this
//! family that depends on no marker, no gauge and no queue: it asks both stores
//! and believes neither.
//!
//! 🔴 What it still cannot do — and this must survive into whatever reads it:
//!
//! 1. **It compares identity, not content.** Two rows sharing an id but
//!    disagreeing about `created_at_ms`, alias or payload pass. The mirror's
//!    field-by-field comparison covers only entries that *left the queue*, so
//!    rows both stores took on the live path are compared by neither.
//! 2. **It is one node's view at one instant.** Nothing aggregates it across a
//!    cluster, and a node that never restarts never runs it.
//! 3. **It cannot attribute a difference.** A row PostgreSQL has and object
//!    storage does not may be another node publishing during the comparison, or
//!    object storage having lost it. It refuses either way, which is the safe
//!    reading and sometimes the annoying one.

use std::collections::BTreeSet;

use async_trait::async_trait;

use super::backlog::{CatalogReadSide, MirrorBacklog, MirrorDirection};
use crate::snapshot::repository::backends::central::{CatalogReadScope, CentralSnapshotCatalog};
use crate::snapshot::repository::interfaces::{SnapshotCatalog, SnapshotListFilter};
use crate::snapshot::repository::RepositoryResult;
use crate::snapshot::types::SnapshotId;

/// How many ids a refusal names before it stops listing them.
const IDS_IN_REFUSAL: usize = 8;

/// The result of asking both catalogs what they hold.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CatalogPopulations {
    /// How many rows object storage answered with, before de-duplication.
    pub object_store_rows: usize,
    /// How many rows the central catalog answered with, before de-duplication.
    pub central_rows: usize,
    /// Ids object storage holds and the central catalog does not.
    pub missing_from_central: Vec<SnapshotId>,
    /// Ids the central catalog holds and object storage does not.
    pub missing_from_object_store: Vec<SnapshotId>,
}

impl CatalogPopulations {
    /// Compares two listings.
    ///
    /// 🔴 Counts *and* sets, because they catch different things. The counts
    /// catch a store answering with the same id twice — which set comparison
    /// silently forgives — and the sets turn "31 against 32" into the id an
    /// operator can go and look at.
    pub fn compare(object_store: &[SnapshotId], central: &[SnapshotId]) -> Self {
        let in_object_store: BTreeSet<&SnapshotId> = object_store.iter().collect();
        let in_central: BTreeSet<&SnapshotId> = central.iter().collect();

        Self {
            object_store_rows: object_store.len(),
            central_rows: central.len(),
            missing_from_central: in_object_store
                .difference(&in_central)
                .map(|id| (*id).clone())
                .collect(),
            missing_from_object_store: in_central
                .difference(&in_object_store)
                .map(|id| (*id).clone())
                .collect(),
        }
    }

    /// Whether the two catalogs hold the same snapshots.
    pub fn agree(&self) -> bool {
        self.object_store_rows == self.central_rows
            && self.missing_from_central.is_empty()
            && self.missing_from_object_store.is_empty()
    }

    /// Why the switch is refused, in terms an operator can act on.
    ///
    /// `repaired` is the compensator's replay count for the central direction,
    /// reported rather than judged: it is what distinguishes "the backfill ran"
    /// from "the backfill never had anything to do", and at startup it is zero
    /// in both cases.
    pub fn refusal(&self, repaired: u64) -> String {
        format!(
            "snapshot.catalog.read = \"postgres\" is refused: the two catalogs do not hold the \
             same snapshots. Object storage answered with {object_store} row(s), the central \
             catalog with {central}.{missing_central}{missing_object} Reads from PostgreSQL \
             would report the snapshots it does not have as absent, and callers delete artifacts \
             and refuse resumes on absence. This comparison asked both stores directly, so it is \
             not the gauges being stale: \
             agentenv_snapshot_catalog_mirror_lag{{direction=\"central\"}} and \
             agentenv_snapshot_catalog_mirror_diverged{{direction=\"central\"}} can both read 0 \
             over exactly this state. Leave the read side on object storage, let the compensator \
             replay (this process has repaired {repaired} write(s) toward the central catalog so \
             far — a value of 0 after a pass means it had nothing queued, which on a cluster with \
             snapshots means the history backfill never ran; clear the node's \
             snapshot.catalog.mirror_backlog_path so it does), and switch once the counts match.",
            object_store = self.object_store_rows,
            central = self.central_rows,
            missing_central = Self::name_ids(
                " Object storage holds and the central catalog does not:",
                &self.missing_from_central,
            ),
            missing_object = Self::name_ids(
                " The central catalog holds and object storage does not:",
                &self.missing_from_object_store,
            ),
        )
    }

    fn name_ids(lead: &str, ids: &[SnapshotId]) -> String {
        if ids.is_empty() {
            return String::new();
        }
        let named: Vec<String> = ids
            .iter()
            .take(IDS_IN_REFUSAL)
            .map(ToString::to_string)
            .collect();
        let rest = ids.len().saturating_sub(named.len());
        let tail = if rest > 0 {
            format!(" and {rest} more")
        } else {
            String::new()
        };
        format!("{lead} {}{tail}.", named.join(", "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids(count: usize) -> Vec<SnapshotId> {
        (0..count).map(|_| SnapshotId::generate()).collect()
    }

    #[test]
    fn two_catalogs_holding_the_same_snapshots_agree() {
        let rows = ids(32);
        let populations = CatalogPopulations::compare(&rows, &rows);

        assert!(populations.agree());
        assert_eq!(populations.object_store_rows, 32);
        assert_eq!(populations.central_rows, 32);
    }

    /// Order is not identity. The two listings come from different stores with
    /// different orderings, and a comparison that depended on the order would
    /// refuse every healthy cluster.
    #[test]
    fn the_same_snapshots_in_a_different_order_agree() {
        let rows = ids(8);
        let mut reversed = rows.clone();
        reversed.reverse();

        assert!(CatalogPopulations::compare(&rows, &reversed).agree());
    }

    /// 🔴 The failure this whole gate exists for, in miniature: object storage
    /// holds a history PostgreSQL was never told about.
    #[test]
    fn a_central_catalog_that_never_heard_of_the_history_is_refused_by_name() {
        let rows = ids(32);
        let populations = CatalogPopulations::compare(&rows, &[]);

        assert!(!populations.agree());
        assert_eq!(populations.missing_from_central.len(), 32);
        assert!(populations.missing_from_object_store.is_empty());

        let refusal = populations.refusal(0);
        assert!(refusal.contains("32 row(s)"), "{refusal}");
        assert!(
            refusal.contains(&populations.missing_from_central[0].to_string()),
            "the refusal must name a snapshot an operator can go and look at: {refusal}"
        );
        assert!(
            refusal.contains("and 24 more"),
            "a long list is truncated, and says so: {refusal}"
        );
    }

    /// The other direction refuses too. PostgreSQL holding rows object storage
    /// does not is not "ahead" — the rollback to object storage would lose them.
    #[test]
    fn a_central_catalog_holding_more_than_object_storage_is_refused() {
        let shared = ids(3);
        let mut central = shared.clone();
        central.extend(ids(2));

        let populations = CatalogPopulations::compare(&shared, &central);

        assert!(!populations.agree());
        assert!(populations.missing_from_central.is_empty());
        assert_eq!(populations.missing_from_object_store.len(), 2);
    }

    /// 🔴 A set comparison alone forgives a store that answered with the same id
    /// twice; the counts are what catch it. Without them "32 rows" and "32 rows,
    /// one of them twice" are the same answer.
    #[test]
    fn a_duplicated_row_is_a_disagreement_even_though_the_sets_match() {
        let rows = ids(4);
        let mut doubled = rows.clone();
        doubled.push(rows[0].clone());

        let populations = CatalogPopulations::compare(&doubled, &rows);

        assert!(populations.missing_from_central.is_empty());
        assert!(populations.missing_from_object_store.is_empty());
        assert!(
            !populations.agree(),
            "5 rows against 4 is a disagreement whatever the sets say"
        );
    }

    /// 🔴 Each half of the set comparison, isolated.
    ///
    /// The fixtures look odd on purpose. In any *realistic* disagreement at
    /// least two of the three conditions fire together — 32 rows against 0
    /// differs in the count as well as the set — so a test built from one
    /// cannot tell whether either set check is doing anything. These two put a
    /// duplicate on one side to hold the counts equal, which leaves exactly one
    /// condition able to notice.
    #[test]
    fn rows_only_object_storage_holds_are_a_disagreement_on_their_own() {
        let shared = ids(1);
        let extra = ids(1);
        let object_store = vec![shared[0].clone(), extra[0].clone()];
        let central = vec![shared[0].clone(), shared[0].clone()];

        let populations = CatalogPopulations::compare(&object_store, &central);

        assert_eq!(populations.object_store_rows, populations.central_rows);
        assert!(
            populations.missing_from_object_store.is_empty(),
            "the other direction must have nothing to say, or this proves nothing"
        );
        assert_eq!(populations.missing_from_central, extra);
        assert!(!populations.agree());
    }

    #[test]
    fn rows_only_the_central_catalog_holds_are_a_disagreement_on_their_own() {
        let shared = ids(1);
        let extra = ids(1);
        let object_store = vec![shared[0].clone(), shared[0].clone()];
        let central = vec![shared[0].clone(), extra[0].clone()];

        let populations = CatalogPopulations::compare(&object_store, &central);

        assert_eq!(populations.object_store_rows, populations.central_rows);
        assert!(
            populations.missing_from_central.is_empty(),
            "the other direction must have nothing to say, or this proves nothing"
        );
        assert_eq!(populations.missing_from_object_store, extra);
        assert!(!populations.agree());
    }

    #[test]
    fn two_empty_catalogs_agree() {
        assert!(CatalogPopulations::compare(&[], &[]).agree());
    }

    /// The number is reported, not judged — and the refusal has to say what a
    /// zero means, because zero is what a startup check always reads.
    #[test]
    fn the_refusal_reports_the_repair_count_it_was_given() {
        let refusal = CatalogPopulations::compare(&ids(1), &[]).refusal(7);
        assert!(refusal.contains("repaired 7 write(s)"), "{refusal}");
    }
}

/// Everything one catalog holds, whatever status the rows are in.
///
/// 🔴 Whatever their status, and on both sides. The resolvable reading —
/// `status_group = 'ready'` — hides a `waiting` template in one catalog and
/// counts it in the other, and two counts of different things is a gate that
/// refuses every cluster that has ever built a template.
#[async_trait]
pub trait CatalogCensus: Send + Sync {
    async fn every_snapshot_id(&self) -> RepositoryResult<Vec<SnapshotId>>;
}

/// 🔴 The central catalog's census asks at the *any status* scope, unlike every
/// other read this node makes of it.
///
/// The resolvable scope is right everywhere else — it is what stops a snapshot
/// whose bytes are still uploading from starting a VM. Here it would be wrong
/// in a way that fails safe-looking: a template sitting at `waiting` would be
/// counted in object storage and hidden in PostgreSQL, so the gate would refuse
/// every cluster that has ever built one.
#[async_trait]
impl CatalogCensus for CentralSnapshotCatalog {
    async fn every_snapshot_id(&self) -> RepositoryResult<Vec<SnapshotId>> {
        Ok(self
            .list_scoped(
                SnapshotListFilter::matches_all(),
                CatalogReadScope::AnyStatus,
            )
            .await?
            .into_iter()
            .map(|record| record.id)
            .collect())
    }
}

/// The object store's census: its ordinary unbounded listing, which has never
/// had a status predicate on it.
pub struct ObjectStoreCensus<'a>(pub &'a dyn SnapshotCatalog);

#[async_trait]
impl CatalogCensus for ObjectStoreCensus<'_> {
    async fn every_snapshot_id(&self) -> RepositoryResult<Vec<SnapshotId>> {
        Ok(self
            .0
            .list(SnapshotListFilter::matches_all())
            .await?
            .into_iter()
            .map(|record| record.id)
            .collect())
    }
}

/// Decides whether this node may serve reads from the side it was configured
/// with, and records the answer.
///
/// 🔴 Three steps, in this order, and each ordering is load-bearing:
///
/// 1. **The queue's own refusals first.** They are local and free — owed
///    writes, recorded divergences, and whether the object store's history was
///    ever enumerated. A node whose mirror is simply behind learns so
///    immediately rather than after both catalogs have been listed.
/// 2. **Then the direct comparison, and only when reads are *moving* onto
///    PostgreSQL.** Every check in step 1 describes the queue, and all three
///    have been observed reading zero over a PostgreSQL holding no rows at all.
///    This one asks both stores.
/// 3. **The recording last.** It is what tells a later start that this was not
///    a switch, and that is only true if nothing that could still refuse runs
///    after it.
pub async fn admit_read_side(
    configured: CatalogReadSide,
    backlog: &MirrorBacklog,
    object_store: &dyn CatalogCensus,
    central: &dyn CatalogCensus,
) -> anyhow::Result<CatalogPopulations> {
    backlog.guard_read_side(configured).await?;

    let mut populations = CatalogPopulations::default();
    if configured == CatalogReadSide::Postgres
        && backlog.recorded_read_side().await? != Some(CatalogReadSide::Postgres)
    {
        populations = CatalogPopulations::compare(
            &object_store.every_snapshot_id().await?,
            &central.every_snapshot_id().await?,
        );
        if !populations.agree() {
            anyhow::bail!(populations.refusal(backlog.repaired_toward(MirrorDirection::Central)));
        }
    }

    backlog.record_read_side(configured).await?;
    Ok(populations)
}

#[cfg(test)]
mod admission_tests {
    use super::*;
    use crate::snapshot::repository::mirror::test_doubles::{record_for, ScriptedCatalog};
    use crate::snapshot::repository::mirror::MirrorOp;
    use crate::snapshot::types::SnapshotRecord;

    /// A backlog on disk. `with_history` says whether the object store's
    /// history has been queued — the condition that is not about a switch.
    async fn backlog_at(
        dir: &std::path::Path,
        with_history: bool,
    ) -> std::sync::Arc<MirrorBacklog> {
        let backlog = MirrorBacklog::open(dir.join("mirror"))
            .await
            .expect("the backlog should open");
        if with_history {
            backlog
                .queue_history_toward_central(&ScriptedCatalog::default())
                .await
                .expect("an empty object store has no history to queue");
        }
        backlog
    }

    async fn owe_central(backlog: &MirrorBacklog) {
        let record: SnapshotRecord = record_for(&SnapshotId::generate());
        backlog
            .record(MirrorDirection::Central, MirrorOp::Create { record })
            .await;
    }

    struct Census(Vec<SnapshotId>);

    #[async_trait]
    impl CatalogCensus for Census {
        async fn every_snapshot_id(&self) -> RepositoryResult<Vec<SnapshotId>> {
            Ok(self.0.clone())
        }
    }

    /// A census nobody may call. 🔴 The comparison costs a full listing of both
    /// catalogs; the cases that must not reach it are cases where a node would
    /// pay that for nothing, and "must not reach it" is only a claim if
    /// something fails when it does.
    struct Unreachable;

    #[async_trait]
    impl CatalogCensus for Unreachable {
        async fn every_snapshot_id(&self) -> RepositoryResult<Vec<SnapshotId>> {
            panic!("the population comparison must not run here");
        }
    }

    fn ids(count: usize) -> Vec<SnapshotId> {
        (0..count).map(|_| SnapshotId::generate()).collect()
    }

    /// 🔴 Refusal branch one, reached through the door the config layer used to
    /// hold shut: the object store's history has never been queued.
    ///
    /// It is not about a switch — a first start counts — so it fires before
    /// anything else, and before either catalog is listed.
    #[tokio::test]
    async fn a_history_that_was_never_queued_refuses_the_switch() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let backlog = backlog_at(dir.path(), false).await;

        let error = admit_read_side(
            CatalogReadSide::Postgres,
            &backlog,
            &Unreachable,
            &Unreachable,
        )
        .await
        .expect_err("PostgreSQL has never been given the history to answer from");

        assert!(error.to_string().contains("never been queued"), "{error}");
        assert_eq!(
            backlog
                .recorded_read_side()
                .await
                .expect("reading the side should work"),
            None,
            "a refused switch must not record the side it refused"
        );
    }

    /// 🔴 Refusal branch two: writes the central catalog is still owed.
    #[tokio::test]
    async fn a_central_catalog_that_is_behind_refuses_the_switch() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let backlog = backlog_at(dir.path(), true).await;
        backlog
            .record_read_side(CatalogReadSide::ObjectStore)
            .await
            .expect("recording the starting side should work");
        owe_central(&backlog).await;

        let error = admit_read_side(
            CatalogReadSide::Postgres,
            &backlog,
            &Unreachable,
            &Unreachable,
        )
        .await
        .expect_err("reads must not move onto a catalog that is behind");

        assert!(
            error.to_string().contains("1 write(s) still owed"),
            "{error}"
        );
    }

    /// 🔴 Refusal branch three, and the one no gauge can express: the two
    /// catalogs simply do not hold the same snapshots.
    ///
    /// The queue is empty, the history marker is on disk, both gauges read
    /// zero — the exact state the dev cluster reached twice, once because the
    /// double write only looked forward and once because clearing PostgreSQL
    /// left the marker behind.
    #[tokio::test]
    async fn catalogs_holding_different_snapshots_refuse_the_switch() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let backlog = backlog_at(dir.path(), true).await;
        backlog
            .record_read_side(CatalogReadSide::ObjectStore)
            .await
            .expect("recording the starting side should work");
        let held = ids(32);

        assert_eq!(backlog.lag_toward(MirrorDirection::Central), 0);
        assert_eq!(backlog.diverged_toward(MirrorDirection::Central), 0);

        let error = admit_read_side(
            CatalogReadSide::Postgres,
            &backlog,
            &Census(held.clone()),
            &Census(Vec::new()),
        )
        .await
        .expect_err("object storage holds 32 snapshots PostgreSQL has never heard of");

        assert!(error.to_string().contains("32 row(s)"), "{error}");
        assert_eq!(
            backlog
                .recorded_read_side()
                .await
                .expect("reading the side should work"),
            Some(CatalogReadSide::ObjectStore),
            "a refused switch leaves the recorded side where it was, so the next start still \
             sees a switch and still runs the comparison"
        );

        // The control: the same call, with a central catalog that holds the
        // same snapshots, is allowed.
        admit_read_side(
            CatalogReadSide::Postgres,
            &backlog,
            &Census(held.clone()),
            &Census(held),
        )
        .await
        .expect("two catalogs holding the same snapshots may switch");
        assert_eq!(
            backlog
                .recorded_read_side()
                .await
                .expect("reading the side should work"),
            Some(CatalogReadSide::Postgres)
        );
    }

    /// 🔴 The comparison runs on the switch and not on every start. Listing
    /// both catalogs on every restart would make an ordinary rollout cost two
    /// full enumerations per node, and would turn an object store that is
    /// briefly unreachable into a node that will not come up.
    #[tokio::test]
    async fn a_restart_already_reading_postgres_does_not_compare_again() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let backlog = backlog_at(dir.path(), true).await;
        backlog
            .record_read_side(CatalogReadSide::Postgres)
            .await
            .expect("recording the starting side should work");

        admit_read_side(
            CatalogReadSide::Postgres,
            &backlog,
            &Unreachable,
            &Unreachable,
        )
        .await
        .expect("a restart on the side already being read is not a switch");
    }

    /// 🔴 The admission is what records the side, and nothing else may be.
    ///
    /// Every other test here records it by hand as setup, which means all of
    /// them pass over an admission that never records anything — and a node
    /// that never records a side reads every start as a switch, so it lists
    /// both catalogs on every restart and can be stopped by an object store
    /// that is briefly unreachable.
    #[tokio::test]
    async fn admitting_a_side_is_what_records_it() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let backlog = backlog_at(dir.path(), true).await;
        assert_eq!(
            backlog
                .recorded_read_side()
                .await
                .expect("reading the side should work"),
            None
        );

        admit_read_side(
            CatalogReadSide::ObjectStore,
            &backlog,
            &Unreachable,
            &Unreachable,
        )
        .await
        .expect("a first start on object storage is allowed");

        assert_eq!(
            backlog
                .recorded_read_side()
                .await
                .expect("reading the side should work"),
            Some(CatalogReadSide::ObjectStore)
        );
    }

    /// 🔴 The rollback, which is the other half of the switch being real. Going
    /// back to object storage is guarded by object storage's own debt, and by
    /// nothing else — the central catalog being behind says nothing about
    /// whether the store being read has everything.
    #[tokio::test]
    async fn the_rollback_to_object_storage_is_guarded_by_object_storages_own_debt() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let backlog = backlog_at(dir.path(), true).await;
        backlog
            .record_read_side(CatalogReadSide::Postgres)
            .await
            .expect("recording the starting side should work");
        owe_central(&backlog).await;

        admit_read_side(
            CatalogReadSide::ObjectStore,
            &backlog,
            &Unreachable,
            &Unreachable,
        )
        .await
        .expect("a debt owed to the catalog being switched away from does not block the way back");
    }
}
