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

use super::backlog::{CatalogReadSide, MirrorBacklog, MirrorDirection, MirrorTargets};
use crate::snapshot::repository::backends::central::CentralSnapshotCatalog;
use crate::snapshot::repository::interfaces::CatalogReadScope;
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
    /// `repaired` is this process's replay count for the central direction and
    /// `still_owed` what the queue has left, both reported rather than judged.
    /// 🔴 They are what separates the two things a difference can mean, and
    /// the separation is only possible because the replay has already run by
    /// the time this is written: `repaired > 0` says the queue was drained and
    /// this difference survived it, so no amount of waiting closes it. A
    /// `repaired` of 0 on a cluster that has snapshots says nothing was queued
    /// to replay in the first place — the history backfill has been marked
    /// done over an enumeration that never happened.
    pub fn refusal(&self, repaired: u64, still_owed: u64) -> String {
        format!(
            "snapshot.catalog.read = \"postgres\" is refused: the two catalogs do not hold the \
             same snapshots. Object storage answered with {object_store} row(s), the central \
             catalog with {central}.{missing_central}{missing_object} Reads from PostgreSQL \
             would report the snapshots it does not have as absent, and callers delete artifacts \
             and refuse resumes on absence. This comparison asked both stores directly, so it is \
             not the gauges being stale: \
             agentenv_snapshot_catalog_mirror_lag{{direction=\"central\"}} and \
             agentenv_snapshot_catalog_mirror_diverged{{direction=\"central\"}} can both read 0 \
             over exactly this state. This start already replayed what the mirror owed the \
             central catalog and asked both stores again before refusing: it has repaired \
             {repaired} write(s) toward the central catalog and {still_owed} are still owed, so \
             the difference above is what a replay could not close. Rows the central catalog \
             holds and object storage does not are closed by no replay in this direction at all \
             — they take deleting the row or restoring the object, and an operator. A repaired \
             count of 0 on a cluster that has snapshots means nothing was queued to replay at \
             all: the history backfill is marked done over an enumeration that never happened, \
             and clearing the node's snapshot.catalog.mirror_backlog_path makes the next start \
             redo it. Leave the read side on object storage until the counts match.",
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

        let refusal = populations.refusal(0, 0);
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

    /// Both numbers are reported, not judged — and the refusal has to say what
    /// a zero repair count means, because that is the one that says the queue
    /// had nothing in it to replay.
    ///
    /// 🔴 Two assertions over two different values, and they must not be able
    /// to pass over each other: a refusal that printed the debt where the
    /// repair count goes would still contain both digits somewhere.
    #[test]
    fn the_refusal_reports_the_repair_count_and_the_debt_it_was_given() {
        let refusal = CatalogPopulations::compare(&ids(1), &[]).refusal(7, 3);
        assert!(refusal.contains("repaired 7 write(s)"), "{refusal}");
        assert!(refusal.contains("3 are still owed"), "{refusal}");
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
/// 🔴 Four steps, in this order, and each ordering is load-bearing:
///
/// 1. **The queue's own refusals first.** They are local and free — owed
///    writes, recorded divergences, and whether the object store's history was
///    ever enumerated. A node whose mirror is simply behind learns so
///    immediately rather than after both catalogs have been listed.
/// 2. **Then the direct comparison, and only when reads are *moving* onto
///    PostgreSQL.** Every check in step 1 describes the queue, and all three
///    have been observed reading zero over a PostgreSQL holding no rows at all.
///    This one asks both stores.
/// 3. **A difference is replayed against before it is refused, and the two
///    stores are then asked again.** See below.
/// 4. **The recording last.** It is what tells a later start that this was not
///    a switch, and that is only true if nothing that could still refuse runs
///    after it.
///
/// 🔴 Step 3 is what keeps this from being a gate a process cannot get through
/// on its own. "Moving onto PostgreSQL" includes a mirror store that has never
/// recorded a side at all — a first start, or a node whose mirror directory has
/// the lifetime of its container — so a replica whose `$AENV_HOME` is scratch
/// reads *every* start as the switch and meets this comparison every time. The
/// thing that would close the difference is the queue: the object store's
/// history is enumerated into it just before this runs, and replaying it is
/// what gives the central catalog the rows it is missing. But the compensator
/// that replays it is spawned *after* this admission, so on such a replica the
/// repair was unreachable from the state that needs it — the same shape as the
/// rollback [`MirrorBacklog::settle_before_reading_from`] exists for, reached
/// through the door marked "first start". A cluster whose api replicas all run
/// on scratch stops entirely: every new Pod refuses, and only Pods older than
/// the difference keep serving.
///
/// So a difference is not the end of the check. What the queue owes the central
/// catalog is replayed here, and if that replayed anything the two stores are
/// asked again. The refusal is what survives a replay, which is the only kind
/// of difference an operator can actually do something about — and it is
/// narrower than what refused before rather than looser: the drain repairs, it
/// never forgives. Rows the central catalog holds and object storage does not
/// are closed by no replay in this direction at all, and are refused exactly as
/// they were.
///
/// 🔴 And it is asked for *after* the comparison, not before it. A start whose
/// two catalogs already agree pays nothing — no replay, no second listing —
/// which matters because on a scratch store the queue holds the whole history
/// and draining it eagerly would move every api replica's history replay in
/// front of its readiness probe.
pub async fn admit_read_side(
    configured: CatalogReadSide,
    backlog: &MirrorBacklog,
    targets: &MirrorTargets,
    object_store: &dyn CatalogCensus,
    central: &dyn CatalogCensus,
) -> anyhow::Result<CatalogPopulations> {
    admit_read_side_with_confirmation(configured, backlog, targets, object_store, central, None)
        .await
}

/// Whether "the read side has already been confirmed onto PostgreSQL" is a
/// fact this process can ask something other than its own node-local mirror
/// store.
///
/// 🔴 This is the structural fix for the CrashLoopBackOff
/// [`admit_read_side`] used to cause on `--role api`. That role's replicas
/// each hold their own `MirrorBacklog` on `$AENV_HOME`, which is an
/// `emptyDir` there — so every fresh replica's [`MirrorBacklog::recorded_read_side`]
/// answers `None`, [`admit_read_side_with_confirmation`] reads that as "this
/// is the switch", and the full population comparison runs on every single
/// replica start rather than once for the cluster. An implementation of this
/// trait backed by a table every replica shares (Stage B's
/// `catalog_migration_state`, see
/// `crate::snapshot::repository::backends::postgres::migration_state`) turns
/// that node-local question into the cluster fact it actually is: once any
/// one replica's comparison has agreed, every replica after it reads that
/// confirmation instead of repeating the comparison.
///
/// Deliberately narrow — two methods, not a general key/value store — so a
/// fake for this module's own tests costs nothing to write. `is_confirmed`
/// and `confirm` are asked in that order and never out of it: see
/// [`admit_read_side_with_confirmation`]'s doc comment on why `confirm` may
/// only be called once the population comparison this admission itself just
/// ran has actually agreed.
#[async_trait]
pub trait ReadSideConfirmationStore: Send + Sync {
    /// Whether the shared record already says the read side has been
    /// confirmed onto PostgreSQL for this cluster.
    async fn is_confirmed(&self) -> anyhow::Result<bool>;
    /// Records that it now has been.
    async fn confirm(&self) -> anyhow::Result<()>;
}

/// The sole thing standing between `snapshot.catalog.write = "postgres"` and
/// silently orphaning every snapshot object storage still holds: refuses
/// unless `store` already says this cluster's read side has been confirmed
/// onto PostgreSQL.
///
/// 🔴 Extracted out of `backends/mod.rs::build_snapshot_backend`'s own
/// `write = "postgres"` branch for the same reason
/// `assemble_postgres_only_backend` was pulled out of that function: that
/// caller reads `ConfigManager::global_config()` and a live `sqlx::PgPool`
/// directly, so nothing in it can be driven by a unit test with an arbitrary
/// confirmation state. This function depends on nothing but the trait, so a
/// [`ReadSideConfirmationStore`] test double — this module's own
/// `FakeSharedStore` among them — exercises the refusal without a database.
pub(crate) async fn require_read_side_confirmed(
    store: &dyn ReadSideConfirmationStore,
) -> anyhow::Result<()> {
    if !store.is_confirmed().await? {
        anyhow::bail!(
            "snapshot.catalog.write = \"postgres\" requires this cluster's read side to have \
             already been confirmed onto PostgreSQL first — run write = \"both\", read = \
             \"postgres\" until admit_read_side_with_confirmation's population comparison has \
             passed, then switch write to \"postgres\". Nothing has confirmed that for this \
             cluster yet."
        );
    }
    Ok(())
}

/// [`admit_read_side`], but able to ask a cluster-shared store — rather than
/// only this node's local [`MirrorBacklog`] — whether the switch has already
/// been confirmed.
///
/// `shared_confirmation` is `None` for every caller that predates Stage B
/// (including every test in this module, which keeps calling
/// [`admit_read_side`] unchanged) and for `--role api`/`--role all` replicas
/// with no `[pg]` pool configured: behaviour is then byte-for-byte identical
/// to before this function existed. When `Some`, the "is this process moving
/// reads onto PostgreSQL for the first time" question this function's own doc
/// comment (see [`admit_read_side`]) describes is answered by
/// `shared_confirmation.is_confirmed()` instead of
/// `backlog.recorded_read_side()`.
///
/// 🔴 `backlog.record_read_side(configured)` still runs unconditionally at
/// the end either way. It is not only what this function itself reads next
/// time — [`MirrorBacklog::guard_read_side`]'s own debt/divergence checks
/// read it too, to tell a genuine switch from a restart on the side already
/// being read, and that is a **node-local** question about what *this
/// node's own queue* still owes: it does not become a cluster fact just
/// because the population comparison's answer now is.
///
/// 🔴 `shared_confirmation.confirm()` is called only after the population
/// comparison in this same call has itself agreed — never speculatively and
/// never by a caller that skipped the comparison. Writing the confirmation
/// ahead of a comparison that might still refuse would let a later replica
/// read "confirmed" over two catalogs that do not actually agree.
pub async fn admit_read_side_with_confirmation(
    configured: CatalogReadSide,
    backlog: &MirrorBacklog,
    targets: &MirrorTargets,
    object_store: &dyn CatalogCensus,
    central: &dyn CatalogCensus,
    shared_confirmation: Option<&dyn ReadSideConfirmationStore>,
) -> anyhow::Result<CatalogPopulations> {
    backlog.guard_read_side(configured).await?;

    let mut populations = CatalogPopulations::default();
    if configured == CatalogReadSide::Postgres {
        let already_confirmed = match shared_confirmation {
            Some(store) => store.is_confirmed().await?,
            None => backlog.recorded_read_side().await? == Some(CatalogReadSide::Postgres),
        };
        if !already_confirmed {
            populations = CatalogPopulations::compare(
                &object_store.every_snapshot_id().await?,
                &central.every_snapshot_id().await?,
            );
            if !populations.agree() {
                let repaired = repair_toward_central(backlog, targets).await;
                // 🔴 Only when the replay actually moved something. A second full
                // listing of both catalogs would otherwise be the standing cost of
                // every refusal, paid for an answer that cannot have changed.
                if repaired > 0 {
                    populations = CatalogPopulations::compare(
                        &object_store.every_snapshot_id().await?,
                        &central.every_snapshot_id().await?,
                    );
                }
                if !populations.agree() {
                    anyhow::bail!(populations.refusal(
                        backlog.repaired_toward(MirrorDirection::Central),
                        backlog.lag_toward(MirrorDirection::Central),
                    ));
                }
            }
            if let Some(store) = shared_confirmation {
                store.confirm().await?;
            }
        }
    }

    backlog.record_read_side(configured).await?;
    Ok(populations)
}

/// Replays what the central catalog is owed, and says how much of it landed.
///
/// The count is taken from the same counter the refusal reports, so "the
/// refusal says 0 repaired" and "this replayed nothing" are one fact rather
/// than two that can drift apart.
async fn repair_toward_central(backlog: &MirrorBacklog, targets: &MirrorTargets) -> u64 {
    let before = backlog.repaired_toward(MirrorDirection::Central);
    backlog
        .drain_debt_toward(MirrorDirection::Central, targets)
        .await;
    backlog
        .repaired_toward(MirrorDirection::Central)
        .saturating_sub(before)
}

#[cfg(test)]
mod admission_tests {
    use super::*;
    use crate::snapshot::repository::mirror::test_doubles::{record_for, ScriptedCatalog};
    use crate::snapshot::repository::mirror::{MirrorOp, MirrorTargets};
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

    /// Targets a replay can land nothing in: the central half is absent, so
    /// every entry the drain looks at is skipped.
    ///
    /// 🔴 Used by every test here whose subject is a refusal that must stand.
    /// The admission now replays before it refuses, and a refusal proven with
    /// a target that *could* have repaired proves less than it looks: the
    /// replay landing nothing has to be a property of the fixture rather than
    /// a hope about the queue being empty.
    pub(super) fn no_repair_possible() -> MirrorTargets {
        MirrorTargets::object_store(std::sync::Arc::new(ScriptedCatalog::default())
            as std::sync::Arc<dyn crate::snapshot::repository::interfaces::SnapshotCatalog>)
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

    /// 🔴 The rollback that could not be reached from the state that needs it.
    ///
    /// A node serving reads from PostgreSQL is put back on object storage.
    /// Object storage owes writes, so the guard refuses — correctly; those are
    /// snapshots it would answer as absent. But the compensator that pays the
    /// debt off is started *after* this point, so a node that cannot start
    /// cannot drain, and a node that cannot drain cannot start: the rollback
    /// was only reachable by putting the node back on the side it had just
    /// been taken off, waiting, and switching again.
    ///
    /// The drain runs first now. The control is the second half: a debt the
    /// drain cannot settle still refuses, because the refusal itself was never
    /// the defect.
    #[tokio::test]
    async fn a_rollback_drains_what_it_owes_before_it_is_refused() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let backlog = backlog_at(dir.path(), true).await;

        // The node was serving PostgreSQL reads.
        backlog
            .record_read_side(CatalogReadSide::Postgres)
            .await
            .expect("recording the side should work");

        // And object storage is behind, which is what the rollback is refused
        // for.
        let record = record_for(&SnapshotId::generate());
        backlog
            .record(
                MirrorDirection::ObjectStore,
                MirrorOp::Create {
                    record: record.clone(),
                },
            )
            .await;
        assert_eq!(backlog.lag_toward(MirrorDirection::ObjectStore), 1);

        let object_store = std::sync::Arc::new(ScriptedCatalog::default());
        let targets = MirrorTargets::object_store(std::sync::Arc::clone(&object_store)
            as std::sync::Arc<dyn crate::snapshot::repository::interfaces::SnapshotCatalog>);

        // 🔴 The store is unreachable on this attempt, so the drain settles
        // nothing and the guard has to refuse exactly as it did before. This
        // is the control: without it, a test could pass by the drain having
        // quietly become "give up and allow it".
        object_store.break_it();
        assert_eq!(
            backlog
                .settle_before_reading_from(CatalogReadSide::ObjectStore, &targets)
                .await,
            1,
            "a drain against a store nobody can reach settles nothing"
        );
        let error = admit_read_side(
            CatalogReadSide::ObjectStore,
            &backlog,
            &targets,
            &Unreachable,
            &Unreachable,
        )
        .await
        .expect_err("a rollback onto a store that is genuinely behind is still refused");
        assert!(
            error.to_string().contains("THE WAY OUT"),
            "the refusal has to say how to get out of it, because the obvious move — start the \
             node so the compensator runs — is the one that does not work: {error}"
        );

        // Now the store is reachable, which is the ordinary case: the debt is
        // real, it is settleable, and nothing but the ordering stopped it.
        object_store.fix_it();
        assert_eq!(
            backlog
                .settle_before_reading_from(CatalogReadSide::ObjectStore, &targets)
                .await,
            0,
            "the drain has to pay off what the guard would refuse on"
        );
        admit_read_side(
            CatalogReadSide::ObjectStore,
            &backlog,
            &targets,
            &Unreachable,
            &Unreachable,
        )
        .await
        .expect("the rollback is allowed once what object storage owed is written");
        assert_eq!(
            object_store.holds(&record.id).map(|held| held.id),
            Some(record.id),
            "and the write it owed really landed, rather than being dropped to clear the number"
        );
    }

    /// A restart on the side the node was already reading does not wait on a
    /// drain: the guard does not refuse it, so there is nothing to unblock,
    /// and every start would otherwise pay for a queue nobody is switching
    /// away from.
    #[tokio::test]
    async fn a_restart_on_the_same_side_does_not_drain() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let backlog = backlog_at(dir.path(), true).await;
        backlog
            .record_read_side(CatalogReadSide::ObjectStore)
            .await
            .expect("recording the side should work");
        backlog
            .record(
                MirrorDirection::ObjectStore,
                MirrorOp::Create {
                    record: record_for(&SnapshotId::generate()),
                },
            )
            .await;

        let object_store = std::sync::Arc::new(ScriptedCatalog::default());
        let targets = MirrorTargets::object_store(std::sync::Arc::clone(&object_store)
            as std::sync::Arc<dyn crate::snapshot::repository::interfaces::SnapshotCatalog>);

        assert_eq!(
            backlog
                .settle_before_reading_from(CatalogReadSide::ObjectStore, &targets)
                .await,
            1,
            "it reports the debt"
        );
        assert!(
            object_store.calls().is_empty(),
            "but it must not have touched the store: {:?}",
            object_store.calls()
        );
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
            &no_repair_possible(),
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
            &no_repair_possible(),
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
            &no_repair_possible(),
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
            &no_repair_possible(),
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
            &no_repair_possible(),
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
            &no_repair_possible(),
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
            &no_repair_possible(),
            &Unreachable,
            &Unreachable,
        )
        .await
        .expect("a debt owed to the catalog being switched away from does not block the way back");
    }

    // ─────────────────────────────────────────────────────────────────────
    // The replay the comparison runs before it refuses
    // ─────────────────────────────────────────────────────────────────────

    use crate::snapshot::repository::mirror::test_doubles::{committed_record, ScriptedCentral};
    use crate::snapshot::repository::mirror::CentralCatalogWrites;
    use std::sync::Arc;

    /// Everything `build_snapshot_backend` assembles before the admission, in
    /// its order, over a mirror store the caller supplies.
    ///
    /// `recorded` is the side already on disk — `None` is a store with the
    /// lifetime of a container, which is what an api replica's `$AENV_HOME`
    /// is. `central_also_holds` are rows the central catalog has and object
    /// storage does not: the direction of difference that no replay toward the
    /// central catalog can close.
    pub(super) struct FreshStart {
        pub(super) backlog: Arc<MirrorBacklog>,
        pub(super) object_store: Arc<ScriptedCatalog>,
        pub(super) central: Arc<ScriptedCentral>,
        pub(super) targets: MirrorTargets,
        /// What `settle_before_reading_from` left the central catalog owing.
        left_by_the_pre_guard_drain: u64,
    }

    pub(super) async fn start_with(
        dir: &std::path::Path,
        recorded: Option<CatalogReadSide>,
        history: &[SnapshotId],
        central_also_holds: &[SnapshotId],
    ) -> FreshStart {
        let backlog = MirrorBacklog::open(dir.join("mirror"))
            .await
            .expect("the backlog should open");
        if let Some(side) = recorded {
            backlog
                .record_read_side(side)
                .await
                .expect("recording the starting side should work");
        }

        let object_store = Arc::new(
            ScriptedCatalog::default().with_history(history.iter().map(committed_record).collect()),
        );
        let central = Arc::new(ScriptedCentral::default());
        for id in central_also_holds {
            central.seed(committed_record(id));
        }
        let targets = MirrorTargets::object_store(Arc::clone(&object_store)
            as Arc<dyn crate::snapshot::repository::interfaces::SnapshotCatalog>)
        .with_central(Arc::clone(&central) as Arc<dyn CentralCatalogWrites>);

        // What the assembly does before the guard: enumerate the object
        // store's history into the queue, then offer the pre-guard drain.
        backlog
            .queue_history_toward_central(object_store.as_ref())
            .await
            .expect("the history should queue");
        let left_by_the_pre_guard_drain = backlog
            .settle_before_reading_from(CatalogReadSide::Postgres, &targets)
            .await;

        FreshStart {
            backlog,
            object_store,
            central,
            targets,
            left_by_the_pre_guard_drain,
        }
    }

    impl FreshStart {
        async fn admit(&self) -> anyhow::Result<CatalogPopulations> {
            admit_read_side(
                CatalogReadSide::Postgres,
                &self.backlog,
                &self.targets,
                &ObjectStoreCensus(self.object_store.as_ref()),
                self.central.as_ref(),
            )
            .await
        }
    }

    /// 🔴 The defect a scratch `$AENV_HOME` turns into a cluster-wide stop.
    ///
    /// An api replica's mirror directory lives and dies with its Pod, so every
    /// replica opens a store that has never recorded a read side — and
    /// `read = "postgres"` reads *that* as the switch, so the comparison runs
    /// on every start rather than once. The rows the central catalog is
    /// missing are queued into the mirror a moment earlier by the history
    /// backfill; the compensator that would replay them is spawned after the
    /// admission. So a replica that refuses here never drains, and the next
    /// replica repeats it: measured on the cluster as every new api Pod in
    /// CrashLoopBackOff over a difference of five snapshots, with only the
    /// Pods older than the difference still serving.
    ///
    /// The control is the second half, and it differs in one value: rows the
    /// *central* catalog holds and object storage does not are closed by no
    /// replay in this direction, and are still refused. Without it this test
    /// would pass just as well over an admission that had stopped comparing.
    #[tokio::test]
    async fn a_store_with_no_recorded_side_replays_what_it_queued_before_it_refuses() {
        let held: Vec<SnapshotId> = ids(5);

        let repairable = tempfile::TempDir::new().expect("tempdir");
        let start = start_with(repairable.path(), None, &held, &[]).await;
        assert_eq!(
            start.backlog.lag_toward(MirrorDirection::Central),
            5,
            "the five rows the central catalog is missing are sitting in the queue, and the \
             pre-guard drain declined to touch them"
        );

        let populations = start
            .admit()
            .await
            .expect("a difference the queue owes is repaired, not refused");
        assert_eq!(
            populations.central_rows, 5,
            "and the comparison that admitted is the one taken after the replay"
        );
        for id in &held {
            assert!(
                start.central.holds(id).is_some(),
                "the rows really landed, rather than the gate having been dropped"
            );
        }
        assert_eq!(start.backlog.lag_toward(MirrorDirection::Central), 0);
        assert_eq!(
            start
                .backlog
                .recorded_read_side()
                .await
                .expect("reading the side should work"),
            Some(CatalogReadSide::Postgres)
        );

        // The control: one row in the direction a replay cannot reach.
        let unrepairable = tempfile::TempDir::new().expect("tempdir");
        let stray = SnapshotId::generate();
        let start = start_with(
            unrepairable.path(),
            None,
            &held,
            std::slice::from_ref(&stray),
        )
        .await;

        let error = start.admit().await.expect_err(
            "a row the central catalog holds and object storage does not still refuses",
        );
        assert!(
            error.to_string().contains(&stray.to_string()),
            "and it names the row a replay could not account for: {error}"
        );
        assert!(
            error.to_string().contains("repaired 5 write(s)"),
            "🔴 the refusal has to say the replay ran, or an operator reads it as the queue \
             never having been drained and waits for a compensator that will never start: \
             {error}"
        );
        assert_eq!(
            start
                .backlog
                .recorded_read_side()
                .await
                .expect("reading the side should work"),
            None,
            "a refused admission records nothing, so the next start still compares"
        );
    }

    /// 🔴 Why the repair had to move to the comparison, in one value.
    ///
    /// `settle_before_reading_from` drains before the guard, and it is the
    /// reason a deliberate switch already worked: a node moving from object
    /// storage to PostgreSQL pays its queue off and then compares a catalog
    /// that has the rows. Its early return on a store with *no* recorded side
    /// is a correct reading of the guard — [`MirrorBacklog::guard_read_side`]
    /// refuses on debt only when a previous side is on record, so on a fresh
    /// store there is genuinely nothing there to unblock — and it is exactly
    /// wrong about the comparison, which does refuse a fresh store and is the
    /// check the queue could have satisfied.
    ///
    /// So the two halves differ in one value and give opposite answers, and
    /// the half that leaves the debt standing is the one the whole change is
    /// about. Without this, nothing states which of the two drains is doing
    /// the work in the test above.
    #[tokio::test]
    async fn the_pre_guard_drain_declines_the_store_that_needs_it_most() {
        let held: Vec<SnapshotId> = ids(5);

        let recorded = tempfile::TempDir::new().expect("tempdir");
        let switch = start_with(
            recorded.path(),
            Some(CatalogReadSide::ObjectStore),
            &held,
            &[],
        )
        .await;
        assert_eq!(
            switch.left_by_the_pre_guard_drain, 0,
            "a node with a side on record is switching, and the pre-guard drain pays the queue off"
        );
        assert_eq!(
            switch.backlog.repaired_toward(MirrorDirection::Central),
            5,
            "and it really replayed them"
        );

        let unrecorded = tempfile::TempDir::new().expect("tempdir");
        let fresh = start_with(unrecorded.path(), None, &held, &[]).await;
        assert_eq!(
            fresh.left_by_the_pre_guard_drain, 5,
            "🔴 a store with no side on record is told it is not switching, so the same five              writes are left owed — and the comparison then refuses over them"
        );
        assert_eq!(
            fresh.backlog.repaired_toward(MirrorDirection::Central),
            0,
            "nothing was replayed at all"
        );
    }

    /// 🔴 The deliberate switch is unchanged, in both directions.
    ///
    /// Its repair comes from the pre-guard drain and always did; what this
    /// pins is that adding a second repair did not turn its refusal into an
    /// admission. A difference in the direction no replay toward the central
    /// catalog reaches refuses exactly as before, and leaves the node reading
    /// the side it was already reading.
    #[tokio::test]
    async fn a_recorded_switch_still_refuses_a_difference_no_replay_reaches() {
        let held: Vec<SnapshotId> = ids(5);

        let repairable = tempfile::TempDir::new().expect("tempdir");
        let start = start_with(
            repairable.path(),
            Some(CatalogReadSide::ObjectStore),
            &held,
            &[],
        )
        .await;
        start
            .admit()
            .await
            .expect("the switch is allowed once the queue it owed has been replayed");
        assert_eq!(
            start
                .backlog
                .recorded_read_side()
                .await
                .expect("reading the side should work"),
            Some(CatalogReadSide::Postgres)
        );

        // The control, differing in one value: a row only the central catalog
        // holds.
        let unrepairable = tempfile::TempDir::new().expect("tempdir");
        let start = start_with(
            unrepairable.path(),
            Some(CatalogReadSide::ObjectStore),
            &held,
            &ids(1),
        )
        .await;
        start
            .admit()
            .await
            .expect_err("a difference no replay closes refuses the switch exactly as before");
        assert_eq!(
            start
                .backlog
                .recorded_read_side()
                .await
                .expect("reading the side should work"),
            Some(CatalogReadSide::ObjectStore),
            "a refused switch leaves the node reading the side it was already reading"
        );
    }

    /// 🔴 The replay is asked for by the difference, not by the start.
    ///
    /// On a store with the lifetime of a Pod the queue holds the *whole*
    /// history on every start, so a repair run before the comparison rather
    /// than after it would move every replica's full history replay in front
    /// of its readiness probe — a fault of its own, on every start, on a
    /// cluster that has nothing wrong with it.
    ///
    /// Both halves are admitted; what differs is what it cost, and they differ
    /// in one value — whether the central catalog already holds the history.
    #[tokio::test]
    async fn an_admission_whose_catalogs_agree_replays_nothing() {
        let held: Vec<SnapshotId> = ids(5);

        let agreeing = tempfile::TempDir::new().expect("tempdir");
        let start = start_with(agreeing.path(), None, &held, &held).await;
        start
            .admit()
            .await
            .expect("two catalogs holding the same snapshots may switch");
        assert_eq!(
            start.backlog.repaired_toward(MirrorDirection::Central),
            0,
            "nothing was replayed: the comparison agreed and never asked"
        );
        assert_eq!(
            start.backlog.lag_toward(MirrorDirection::Central),
            5,
            "and the queue is left where it was, for the compensator to settle once the node is up"
        );

        // The same start, differing only in the central catalog being empty.
        let differing = tempfile::TempDir::new().expect("tempdir");
        let start = start_with(differing.path(), None, &held, &[]).await;
        start
            .admit()
            .await
            .expect("a difference the queue owes is repaired");
        assert_eq!(
            start.backlog.repaired_toward(MirrorDirection::Central),
            5,
            "here the difference did ask, and the replay ran"
        );
    }
}

/// Tests for [`admit_read_side_with_confirmation`]'s `shared_confirmation`
/// argument specifically — the Stage B addition. Every scenario the plain
/// `None` path already covers is in `admission_tests` above and is left
/// untouched by this module; these tests are only about the branch that did
/// not exist before.
#[cfg(test)]
mod shared_confirmation_tests {
    #[allow(unused_imports)]
    use super::admission_tests::{no_repair_possible, start_with, FreshStart};
    use super::*;
    use crate::snapshot::repository::mirror::backlog::MirrorBacklog;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    /// A fake cluster-shared store: one `AtomicBool` standing in for the
    /// `catalog_migration_state` row, shared (via `Arc`) across as many
    /// "replicas" as a test constructs — each replica in these tests gets its
    /// own, independent, empty `MirrorBacklog` (the node-local half), the same
    /// way `--role api` replicas each get their own empty `$AENV_HOME`.
    #[derive(Default)]
    struct FakeSharedStore {
        confirmed: AtomicBool,
        confirm_calls: AtomicUsize,
    }

    #[async_trait]
    impl ReadSideConfirmationStore for FakeSharedStore {
        async fn is_confirmed(&self) -> anyhow::Result<bool> {
            Ok(self.confirmed.load(Ordering::SeqCst))
        }

        async fn confirm(&self) -> anyhow::Result<()> {
            self.confirm_calls.fetch_add(1, Ordering::SeqCst);
            self.confirmed.store(true, Ordering::SeqCst);
            Ok(())
        }
    }

    struct Unreachable;

    #[async_trait]
    impl CatalogCensus for Unreachable {
        async fn every_snapshot_id(&self) -> RepositoryResult<Vec<SnapshotId>> {
            panic!("the population comparison must not run once the shared store says confirmed");
        }
    }

    /// The whole point of D4: a replica whose local `MirrorBacklog` has never
    /// recorded anything (a fresh `$AENV_HOME`) does not run the comparison
    /// at all when the shared store already says "confirmed" — unlike the
    /// `None` path, where exactly this local state means "this is the
    /// switch" and pays for a full listing of both catalogs.
    #[tokio::test]
    async fn a_confirmed_shared_store_skips_the_comparison_even_on_a_fresh_local_backlog() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let backlog = MirrorBacklog::open(dir.path().join("mirror"))
            .await
            .expect("the backlog should open");
        backlog
            .queue_history_toward_central(
                &crate::snapshot::repository::mirror::test_doubles::ScriptedCatalog::default(),
            )
            .await
            .expect("queuing empty history should succeed");
        assert_eq!(
            backlog.recorded_read_side().await.unwrap(),
            None,
            "this backlog has never recorded a side -- the fresh-replica case"
        );

        let shared = FakeSharedStore {
            confirmed: AtomicBool::new(true),
            confirm_calls: AtomicUsize::new(0),
        };

        admit_read_side_with_confirmation(
            CatalogReadSide::Postgres,
            &backlog,
            &no_repair_possible(),
            &Unreachable,
            &Unreachable,
            Some(&shared),
        )
        .await
        .expect("a shared store that already says confirmed must not run the comparison at all");

        assert_eq!(
            shared.confirm_calls.load(Ordering::SeqCst),
            0,
            "already confirmed, so confirming again is pure waste"
        );
    }

    /// The other half: a shared store that says "not yet confirmed" still
    /// runs the real comparison, and calls `confirm()` exactly once it
    /// agrees -- simulating the first replica to ever admit this cluster.
    #[tokio::test]
    async fn an_unconfirmed_shared_store_runs_the_comparison_and_confirms_on_agreement() {
        let held: Vec<SnapshotId> = (0..5).map(|_| SnapshotId::generate()).collect();
        let dir = tempfile::TempDir::new().expect("tempdir");
        let start = start_with(dir.path(), None, &held, &held).await;

        let shared = FakeSharedStore::default();
        let populations = admit_read_side_with_confirmation(
            CatalogReadSide::Postgres,
            &start.backlog,
            &start.targets,
            &ObjectStoreCensus(start.object_store.as_ref()),
            start.central.as_ref(),
            Some(&shared),
        )
        .await
        .expect("two catalogs holding the same snapshots may switch");

        assert!(populations.agree());
        assert_eq!(
            shared.confirm_calls.load(Ordering::SeqCst),
            1,
            "the comparison agreed, so the shared store must be told exactly once"
        );
        assert!(shared.is_confirmed().await.unwrap());
    }

    /// A comparison that still disagrees after the replay must not confirm
    /// the shared store -- a later replica reading "confirmed" over catalogs
    /// that do not actually agree would skip the check that exists to catch
    /// exactly that.
    #[tokio::test]
    async fn a_disagreement_that_survives_the_replay_never_confirms_the_shared_store() {
        let held: Vec<SnapshotId> = (0..5).map(|_| SnapshotId::generate()).collect();
        let stray = SnapshotId::generate();
        let dir = tempfile::TempDir::new().expect("tempdir");
        // `central_also_holds` carries a row no replay toward the central
        // catalog can ever produce out of thin air on the object-store side.
        let start = start_with(dir.path(), None, &held, std::slice::from_ref(&stray)).await;

        let shared = FakeSharedStore::default();
        let error = admit_read_side_with_confirmation(
            CatalogReadSide::Postgres,
            &start.backlog,
            &start.targets,
            &ObjectStoreCensus(start.object_store.as_ref()),
            start.central.as_ref(),
            Some(&shared),
        )
        .await
        .expect_err("a row the central catalog holds and object storage does not still refuses");

        assert!(error.to_string().contains(&stray.to_string()));
        assert_eq!(
            shared.confirm_calls.load(Ordering::SeqCst),
            0,
            "a refusal must never confirm the shared store"
        );
        assert!(!shared.is_confirmed().await.unwrap());
    }

    /// The end-to-end story D4 exists for: two "replicas", each with its own
    /// empty local backlog, sharing one `FakeSharedStore`. The first pays for
    /// the comparison and confirms; the second, arriving after it with an
    /// equally empty local backlog, reads the shared confirmation and never
    /// touches the catalogs at all.
    #[tokio::test]
    async fn a_second_replica_with_its_own_empty_backlog_reads_the_first_replicas_confirmation() {
        let held: Vec<SnapshotId> = (0..5).map(|_| SnapshotId::generate()).collect();
        let shared = FakeSharedStore::default();

        let first_dir = tempfile::TempDir::new().expect("tempdir");
        let first = start_with(first_dir.path(), None, &held, &held).await;
        admit_read_side_with_confirmation(
            CatalogReadSide::Postgres,
            &first.backlog,
            &first.targets,
            &ObjectStoreCensus(first.object_store.as_ref()),
            first.central.as_ref(),
            Some(&shared),
        )
        .await
        .expect("the first replica's comparison agrees");
        assert_eq!(shared.confirm_calls.load(Ordering::SeqCst), 1);

        // A second, independent replica -- its own backlog, never told
        // anything about the first replica's local state (an emptyDir mirror
        // directory has no way to be), reaching the *same* shared store.
        let second_dir = tempfile::TempDir::new().expect("tempdir");
        let second_backlog = MirrorBacklog::open(second_dir.path().join("mirror"))
            .await
            .expect("the second backlog should open");
        second_backlog
            .queue_history_toward_central(
                &crate::snapshot::repository::mirror::test_doubles::ScriptedCatalog::default(),
            )
            .await
            .expect("queuing empty history should succeed");

        admit_read_side_with_confirmation(
            CatalogReadSide::Postgres,
            &second_backlog,
            &no_repair_possible(),
            &Unreachable,
            &Unreachable,
            Some(&shared),
        )
        .await
        .expect("the second replica must not need to run the comparison itself");
        assert_eq!(
            shared.confirm_calls.load(Ordering::SeqCst),
            1,
            "the second replica read the confirmation rather than re-confirming"
        );
    }

    // ── require_read_side_confirmed ─────────────────────────────────────
    //
    // 🔴 P2: the sole gate `snapshot.catalog.write = "postgres"` passes
    // through before it will start (`backends/mod.rs::build_snapshot_backend`'s
    // `write == Postgres` branch) — reusing `FakeSharedStore` rather than a
    // new fixture, since it already implements `ReadSideConfirmationStore`
    // and already lives in this test module.

    #[tokio::test]
    async fn an_unconfirmed_store_refuses_write_postgres() {
        let shared = FakeSharedStore::default();
        assert!(
            !shared
                .is_confirmed()
                .await
                .expect("is_confirmed should not error"),
            "a fresh FakeSharedStore must start unconfirmed for this test's proof to hold"
        );

        let error = require_read_side_confirmed(&shared)
            .await
            .expect_err("an unconfirmed store must refuse write = \"postgres\"");
        assert!(
            error.to_string().contains("already been confirmed"),
            "got: {error}"
        );
    }

    #[tokio::test]
    async fn a_confirmed_store_admits_write_postgres() {
        let shared = FakeSharedStore::default();
        shared.confirm().await.expect("confirm should not error");

        require_read_side_confirmed(&shared)
            .await
            .expect("a confirmed store must admit write = \"postgres\"");
    }
}
