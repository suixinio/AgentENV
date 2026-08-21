//! The writes each catalog still owes, and the pass that pays them.
//!
//! 🔴 Durable, and that is the whole reason it is here rather than a `Vec` in a
//! field. The number this holds is what decides whether the read side may be
//! moved — "no data is lost by switching" is only true while nothing is
//! outstanding — and a backlog that emptied itself on restart would make that
//! check pass by forgetting rather than by being right.
//!
//! 🔴 **Both directions, tagged.** It used to be one: central catalog to object
//! store, on the argument that an object-store row can be rebuilt from a
//! catalog row and not the other way round. That argument is about
//! *reconstructing a row by diffing state*, and this queue does not do that. It
//! records the **operation** — the whole [`MirrorOp`], carrying the whole
//! `SnapshotRecord` or `SnapshotCommit` — and replays it, which works equally
//! well against either store. What the one-way rule actually bought was a
//! central write that had to fail the user's publish when the scheduler was
//! unreachable, and that trade is not worth making: see [`super`].
//!
//! The direction is not cosmetic. [`MirrorBacklog::lag_toward`] is read by two
//! different switches that care about two different debts, so a lag that meant
//! both at once would answer neither.
//!
//! 🔴 **Lag is not agreement.** A write nothing can replay is not owed, but the
//! two catalogs still disagree about it, and the batch has a known permanent
//! source of those: `try_start_build` writes object storage alone, so every
//! template's central row stays `waiting` while the object store's moves on.
//! Those are recorded here too, durably, as divergences rather than as debt —
//! separately counted, because the compensator cannot pay them, and consulted
//! by the same guard, because a read side moved over the top of one loses rows.
//!
//! 🔴 **And lag is not history.** Both numbers describe writes the double write
//! *saw*, so a snapshot published before it was turned on is counted by
//! neither: at the moment of the flip, this cluster read `lag = 0,
//! diverged = 0`, PostgreSQL 0 rows, object storage 32. Every one of those
//! thirty-two would have vanished from the API the moment reads moved.
//! [`MirrorBacklog::queue_history_toward_central`] puts them in here as
//! ordinary debt, so that `lag == 0 && diverged == 0` means "the two catalogs
//! agree" rather than "nothing is queued".
//!
//! 🔴 **Nothing is retried forever.** An entry that fails
//! [`MAX_REPLAY_ATTEMPTS`] passes without either settling or learning anything
//! stops being debt and becomes a recorded divergence. The permanent rejection
//! that motivated it is classified at the wire now, but a cap has to hold for
//! the failure nobody has classified yet.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tracing::{error, warn};

use crate::local_store::{LocalKvStore, LocalStoreDurability};
use crate::snapshot::repository::backends::central::{
    commit_opening_record, opening_status, CatalogRefusal, CatalogWrite, STATUS_BUILDING,
};
use crate::snapshot::repository::interfaces::{
    SnapshotCatalog, SnapshotCommit, SnapshotListFilter,
};
use crate::snapshot::repository::{RepositoryError, RepositoryResult};
use crate::snapshot::types::{
    SnapshotId, SnapshotPublishSource, SnapshotRecord, SnapshotSource, TemplateBuildErrorReason,
    TemplateBuildStatus,
};

use super::central::CentralCatalogWrites;
use super::metrics::{
    record_mirror_content_mismatch, record_mirror_divergence_retired, record_mirror_repair_failed,
    record_mirror_repaired, record_mirror_unrecorded, set_mirror_diverged, set_mirror_lag,
};

/// Which store is behind.
///
/// 🔴 Names the store that still owes the write, not the store the write came
/// from. Every counter and every guard below reads it that way.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum MirrorDirection {
    /// The object-store catalog has not taken it.
    #[default]
    ObjectStore,
    /// The central catalog has not taken it.
    Central,
}

impl MirrorDirection {
    pub(super) const ALL: [Self; 2] = [Self::ObjectStore, Self::Central];

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::ObjectStore => "object_store",
            Self::Central => "central",
        }
    }

    /// What the store on this side is called in a message to an operator.
    fn store_name(self) -> &'static str {
        match self {
            Self::ObjectStore => "the object-store catalog",
            Self::Central => "the central catalog",
        }
    }

    fn key_byte(self) -> u8 {
        match self {
            Self::ObjectStore => b'o',
            Self::Central => b'c',
        }
    }
}

/// One catalog write a store has not taken yet.
///
/// The *operation*, not the row it would produce. Recording the operation is
/// what lets a replay be judged against the same contract the original call
/// was: a `delete_record` whose target is already gone succeeded, and a row
/// diff could not tell that from a delete that never ran. It is also what makes
/// the queue work in both directions — an operation is replayable against
/// whichever store missed it, while a row diff would only be replayable against
/// the store whose columns are a subset of the other's.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) enum MirrorOp {
    Create {
        record: SnapshotRecord,
    },
    PublishCommit {
        commit: SnapshotCommit,
    },
    TryStartBuild {
        id: SnapshotId,
    },
    MarkBuildError {
        id: SnapshotId,
        reason: TemplateBuildErrorReason,
    },
    DeleteRecord {
        record: SnapshotRecord,
    },
}

impl MirrorOp {
    pub(super) fn name(&self) -> &'static str {
        match self {
            Self::Create { .. } => "create",
            Self::PublishCommit { .. } => "publish_commit",
            Self::TryStartBuild { .. } => "try_start_build",
            Self::MarkBuildError { .. } => "mark_build_error",
            Self::DeleteRecord { .. } => "delete_record",
        }
    }

    /// Which snapshot this write is about.
    ///
    /// The replay's ordering key: two writes to one snapshot must be replayed
    /// in the order they were made, and writes to different snapshots are
    /// independent of each other.
    pub(super) fn snapshot_id(&self) -> &SnapshotId {
        match self {
            Self::Create { record } | Self::DeleteRecord { record } => &record.id,
            Self::PublishCommit { commit } => &commit.id,
            Self::TryStartBuild { id } | Self::MarkBuildError { id, .. } => id,
        }
    }
}

/// One owed write, the store that owes it, and how many times a pass has tried.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct OwedWrite {
    #[serde(default)]
    direction: MirrorDirection,
    op: MirrorOp,
    /// Replays that neither settled this write nor learned anything from it.
    ///
    /// 🔴 Persisted, and it is the persistence that makes the cap mean
    /// anything: a counter held in memory would reset on every restart, and a
    /// node that restarts more often than the cap expires would retry the same
    /// unwinnable write for as long as the cluster lived. Defaulted, because
    /// entries written before this field existed are ordinary entries that have
    /// simply not been counted yet.
    #[serde(default)]
    attempts: u32,
}

/// How many passes may fail to settle one entry before it stops being debt.
///
/// 🔴 An unbounded retry is a defect whatever it is retrying. The measured one
/// was a permanent rejection misread as an outage — that is classified at the
/// source now — but a cap has to hold for the failure nobody classified, which
/// is every failure that has not happened yet.
///
/// At [`super::DEFAULT_COMPENSATOR_INTERVAL`] this is about an hour, and the
/// hour is the trade. Shorter, and a controller rollout that runs long turns
/// real debt into a permanent disagreement. Longer, and a queue nothing can
/// drain keeps a switch shut without saying why. An hour is longer than any
/// rollout this cluster has taken and short enough that an operator looking at
/// a stuck lag the next morning finds a divergence record naming the reason
/// rather than a counter still climbing.
///
/// 🔴 What it costs when it fires: the entry leaves the lag and becomes a
/// recorded divergence, which no replay clears. That is the honest state — the
/// central catalog really is missing the write — and it keeps the read-side
/// switch shut, which is the behaviour that matters. Clearing it takes deleting
/// the snapshot, or an operator.
pub(super) const MAX_REPLAY_ATTEMPTS: u32 = 120;

impl OwedWrite {
    /// Decodes an entry, including one written before the queue had directions.
    ///
    /// A backlog on disk outlives the build that wrote it — `write = "both"` is
    /// rolled back by turning the double write off and letting the compensator
    /// finish, so the entries a *previous* build recorded are exactly the ones
    /// the rollback depends on. An entry from before this field existed is an
    /// object-store debt, which is all the queue held then.
    fn decode(value: &[u8]) -> Result<Self, serde_json::Error> {
        match serde_json::from_slice::<Self>(value) {
            Ok(owed) => Ok(owed),
            Err(newer) => match serde_json::from_slice::<MirrorOp>(value) {
                Ok(op) => Ok(Self {
                    direction: MirrorDirection::ObjectStore,
                    op,
                    attempts: 0,
                }),
                Err(_) => Err(newer),
            },
        }
    }
}

/// Whether `existing` already reflects `op`.
///
/// 🔴 A read, not an error-message match. A replay can fail because the first
/// attempt actually landed and only its acknowledgement was lost, and the two
/// are indistinguishable from the error alone — every backend spells "already
/// exists" differently and one of them spells it `InvalidRequest`. Asking the
/// target what it has settles it.
fn reflects(op: &MirrorOp, existing: Option<&SnapshotRecord>) -> bool {
    match op {
        MirrorOp::Create { .. } => existing.is_some(),
        MirrorOp::PublishCommit { .. } => existing
            .map(|record| record.committed.is_some())
            .unwrap_or(false),
        MirrorOp::DeleteRecord { .. } => existing.is_none(),
        MirrorOp::TryStartBuild { .. } => {
            existing.map(build_status_at_least_started).unwrap_or(false)
        }
        MirrorOp::MarkBuildError { .. } => existing
            .map(|record| {
                matches!(
                    build_status(record),
                    Some(crate::snapshot::types::TemplateBuildStatus::Error)
                )
            })
            .unwrap_or(false),
    }
}

fn build_status(record: &SnapshotRecord) -> Option<crate::snapshot::types::TemplateBuildStatus> {
    match &record.source {
        crate::snapshot::types::SnapshotSource::Template { build } => Some(build.status),
        crate::snapshot::types::SnapshotSource::Sandbox { .. } => None,
    }
}

/// True once a template has left `waiting`, in either direction.
///
/// A `try_start_build` whose replay finds the row already `ready` or `error`
/// has been overtaken, not lost: something later in this snapshot's own queue
/// already moved it, and re-running the transition would be refused for the
/// right reason.
fn build_status_at_least_started(record: &SnapshotRecord) -> bool {
    !matches!(
        build_status(record),
        Some(crate::snapshot::types::TemplateBuildStatus::Waiting) | None
    )
}

// ─────────────────────────────────────────────────────────────────────────────
// Replay targets
// ─────────────────────────────────────────────────────────────────────────────

/// A store an owed write can be replayed against.
///
/// Two implementations, one per direction. They are not interchangeable: the
/// probe a replay is judged against has to see rows at the widest scope the
/// store has, and for the central catalog that is a different call from the one
/// an ordinary read makes.
#[async_trait]
trait MirrorTarget: Send + Sync {
    async fn apply(&self, op: &MirrorOp) -> RepositoryResult<()>;
    async fn probe(&self, id: &SnapshotId) -> RepositoryResult<Option<SnapshotRecord>>;
}

struct ObjectStoreTarget(Arc<dyn SnapshotCatalog>);

#[async_trait]
impl MirrorTarget for ObjectStoreTarget {
    async fn apply(&self, op: &MirrorOp) -> RepositoryResult<()> {
        match op {
            MirrorOp::Create { record } => self.0.create(record.clone()).await.map(|_| ()),
            MirrorOp::PublishCommit { commit } => {
                self.0.publish_commit(commit.clone()).await.map(|_| ())
            }
            MirrorOp::TryStartBuild { id } => self.0.try_start_build(id).await.map(|_| ()),
            MirrorOp::MarkBuildError { id, reason } => {
                self.0.mark_build_error(id, reason.clone()).await
            }
            MirrorOp::DeleteRecord { record } => self.0.delete_record(record).await,
        }
    }

    async fn probe(&self, id: &SnapshotId) -> RepositoryResult<Option<SnapshotRecord>> {
        self.0.get(&id.to_string()).await
    }
}

struct CentralTarget(Arc<dyn CentralCatalogWrites>);

impl CentralTarget {
    /// Turns a refusal into a replay outcome.
    ///
    /// 🔴 `AlreadyExists` is a success here, and it is the ordinary answer
    /// rather than an edge case: every replayed `publish_commit` opens the row
    /// before flipping it, and a replay that got as far as opening it once
    /// already finds it open. That is exactly what makes central writes
    /// re-runnable, which is what the queue's second direction rests on.
    fn settled<T>(op: &'static str, write: CatalogWrite<T>) -> RepositoryResult<()> {
        match write {
            CatalogWrite::Applied(_) | CatalogWrite::Refused(CatalogRefusal::AlreadyExists) => {
                Ok(())
            }
            CatalogWrite::Refused(refusal) => Err(RepositoryError::InvalidRequest {
                reason: format!("the central catalog refused a replayed '{op}': {refusal}"),
            }),
        }
    }
}

#[async_trait]
impl MirrorTarget for CentralTarget {
    async fn apply(&self, op: &MirrorOp) -> RepositoryResult<()> {
        match op {
            MirrorOp::Create { record } => Self::settled(
                "create",
                self.0.begin(record, opening_status(record), true).await?,
            ),
            MirrorOp::PublishCommit { commit } => {
                let opening = commit_opening_record(commit);
                Self::settled(
                    "publish_commit.begin",
                    self.0.begin(&opening, STATUS_BUILDING, false).await?,
                )?;
                Self::settled(
                    "publish_commit.commit",
                    self.0.commit(commit, true, now_unix_ms()).await?,
                )
            }
            // Never recorded toward the central catalog: the transition is
            // build admission, which this batch does not wire, so there is no
            // write to owe. The arm exists because the enum is shared.
            MirrorOp::TryStartBuild { id } => Err(RepositoryError::Unsupported {
                feature: format!(
                    "replaying a build start for '{id}' into the central catalog: the transition \
                     is build admission, which is not wired"
                ),
            }),
            MirrorOp::MarkBuildError { id, reason } => Self::settled(
                "mark_build_error",
                self.0.fail(id, reason, now_unix_ms()).await?,
            ),
            MirrorOp::DeleteRecord { record } => self
                .0
                .delete(&record.id.to_string(), now_unix_ms())
                .await
                .map(|_| ()),
        }
    }

    async fn probe(&self, id: &SnapshotId) -> RepositoryResult<Option<SnapshotRecord>> {
        self.0.get_any_status(&id.to_string()).await
    }
}

/// The object store and the central catalog, in that order.
type TargetPair<'a> = (&'a Arc<dyn MirrorTarget>, &'a Arc<dyn MirrorTarget>);

/// The stores a repair pass may replay into.
///
/// 🔴 Either may be absent, and absent is not the same as broken. Rolling the
/// double write back to `write = "object_store"` takes the central catalog away
/// on purpose; the pass still has to pay off what object storage is owed,
/// because those publishes reported success and object storage is about to be
/// the only catalog there is. Central-direction entries are then left alone
/// rather than attempted or dropped.
#[derive(Clone, Default)]
pub struct MirrorTargets {
    object_store: Option<Arc<dyn MirrorTarget>>,
    central: Option<Arc<dyn MirrorTarget>>,
}

impl MirrorTargets {
    pub fn object_store(catalog: Arc<dyn SnapshotCatalog>) -> Self {
        Self {
            object_store: Some(Arc::new(ObjectStoreTarget(catalog))),
            central: None,
        }
    }

    pub fn with_central(mut self, central: Arc<dyn CentralCatalogWrites>) -> Self {
        self.central = Some(Arc::new(CentralTarget(central)));
        self
    }

    fn for_direction(&self, direction: MirrorDirection) -> Option<&Arc<dyn MirrorTarget>> {
        match direction {
            MirrorDirection::ObjectStore => self.object_store.as_ref(),
            MirrorDirection::Central => self.central.as_ref(),
        }
    }

    /// Both stores, or nothing.
    ///
    /// A comparison needs two sides. With the double write rolled back to one
    /// direction there is no second row to compare against, and inventing one
    /// would be the same failure as the number this comparison exists to fix.
    fn pair(&self) -> Option<TargetPair<'_>> {
        Some((self.object_store.as_ref()?, self.central.as_ref()?))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Do the two catalogs agree about this snapshot?
// ─────────────────────────────────────────────────────────────────────────────

/// What comparing the two catalogs' rows for one snapshot found.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum CatalogComparison {
    /// Both stores answered and their rows say the same thing.
    Agree,
    /// There is only one store, so there is nothing to compare against.
    NotComparable,
    /// One of the stores could not be asked, so nothing was learned.
    Unreadable(String),
    /// Both answered and they say different things.
    Disagree { field: &'static str, detail: String },
}

/// The fields the two catalogs are required to agree on, in the order a
/// disagreement is reported.
///
/// 🔴 What is *not* here is as deliberate as what is, because every exclusion
/// is a way the two rows may legitimately differ:
///
///   - `updated_at_unix_ms`. Each store stamps its own clock on the paths that
///     do not carry one — the central table's trigger fills it in when the
///     caller says nothing — so two rows written by one call differ by the
///     latency of an RPC. It records when a row was last touched, and a replay
///     really is a touch.
///   - `build.started_at_unix_ms` / `build.finished_at_unix_ms`. This build
///     never sends them to the central catalog; comparing them would report a
///     disagreement about a value one side was never given.
///   - `build.error_reason`. The history backfill synthesises a reason for a
///     build that had already failed before the mirror existed and whose object
///     store row records no reason at all. That is a deliberate asymmetry, and
///     flagging it would make every such template a permanent divergence.
///
/// `created_at_unix_ms` leads the list because it is the one this comparison
/// was written for: it is the column the listing orders by, the value
/// `createdAt` is served from, and the only field no later write ever changes —
/// so a difference in it is never a race and always a defect.
fn catalog_disagreement(
    object_store: Option<&SnapshotRecord>,
    central: Option<&SnapshotRecord>,
) -> Option<(&'static str, String)> {
    let (left, right) = match (object_store, central) {
        (None, None) => return None,
        (Some(left), None) => {
            return Some((
                "presence",
                format!(
                    "object storage holds '{}' and the central catalog does not",
                    left.id
                ),
            ))
        }
        (None, Some(right)) => {
            return Some((
                "presence",
                format!(
                    "the central catalog holds '{}' and object storage does not",
                    right.id
                ),
            ))
        }
        (Some(left), Some(right)) => (left, right),
    };

    let compared: [(&'static str, String, String); 5] = [
        (
            "created_at",
            left.created_at_unix_ms.to_string(),
            right.created_at_unix_ms.to_string(),
        ),
        ("alias", alias_of(left), alias_of(right)),
        ("resources", resources_of(left), resources_of(right)),
        ("source", source_of(left), source_of(right)),
        (
            "build_status",
            build_status_of(left),
            build_status_of(right),
        ),
    ];
    for (field, ours, theirs) in compared {
        if ours != theirs {
            return Some((
                field,
                format!(
                    "the two catalogs disagree about '{}': object storage says {field} is \
                     {ours}, the central catalog says {theirs}",
                    left.id
                ),
            ));
        }
    }

    // 🔴 Structurally, not by serialised bytes. `CommandContext` carries two
    // `HashMap`s, whose JSON key order is whatever the hasher chose this
    // process — so a byte comparison would call two identical payloads
    // different, at random, on about every other run.
    let payload = |record: &SnapshotRecord| {
        record
            .committed
            .as_ref()
            .map(serde_json::to_value)
            .transpose()
    };
    match (payload(left), payload(right)) {
        (Ok(ours), Ok(theirs)) if ours != theirs => Some((
            "committed",
            format!(
                "the two catalogs hold different committed payloads for '{}'",
                left.id
            ),
        )),
        // Neither payload can be re-encoded, which is this process's defect and
        // not a disagreement between two stores. Left to the ordinary
        // serialisation error paths rather than reported as a divergence.
        _ => None,
    }
}

fn alias_of(record: &SnapshotRecord) -> String {
    record
        .alias
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_else(|| "none".to_string())
}

fn resources_of(record: &SnapshotRecord) -> String {
    format!(
        "{}cpu/{}MiB/{}MiB",
        record.resources.cpu_count, record.resources.memory_mib, record.resources.disk_size_mib
    )
}

fn source_of(record: &SnapshotRecord) -> String {
    match &record.source {
        SnapshotSource::Template { .. } => "template".to_string(),
        SnapshotSource::Sandbox { source_sandbox_id } => format!("sandbox/{source_sandbox_id}"),
    }
}

fn build_status_of(record: &SnapshotRecord) -> String {
    match build_status(record) {
        Some(status) => format!("{status:?}"),
        None => "n/a".to_string(),
    }
}

/// Reads one snapshot out of both stores and compares the two rows.
async fn compare_catalogs(targets: &MirrorTargets, id: &SnapshotId) -> CatalogComparison {
    let Some((object_store, central)) = targets.pair() else {
        return CatalogComparison::NotComparable;
    };
    let left = match object_store.probe(id).await {
        Ok(row) => row,
        Err(error) => {
            return CatalogComparison::Unreadable(format!(
                "object storage could not be asked what it holds for '{id}': {error}"
            ))
        }
    };
    let right = match central.probe(id).await {
        Ok(row) => row,
        Err(error) => {
            return CatalogComparison::Unreadable(format!(
                "the central catalog could not be asked what it holds for '{id}': {error}"
            ))
        }
    };
    match catalog_disagreement(left.as_ref(), right.as_ref()) {
        None => CatalogComparison::Agree,
        Some((field, detail)) => CatalogComparison::Disagree { field, detail },
    }
}

/// What one replay attempt settled.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum RepairVerdict {
    /// The target now reflects the write. Drop the entry.
    Repaired,
    /// Nothing was learned; the same attempt may work later. Keep the entry and
    /// stop touching this snapshot for the rest of the pass.
    Retry,
    /// The target refused in a way a retry cannot change.
    ///
    /// 🔴 The write stops being *debt* and becomes a recorded *divergence*: the
    /// two stores disagree about this snapshot and nothing here can settle it,
    /// so replaying it every thirty seconds forever buys nothing and costs a
    /// request against the store the acceptance number is measured on. It stays
    /// counted — by [`MirrorBacklog::diverged_toward`] rather than by the lag —
    /// and the read-side guard consults both.
    Diverged,
}

impl RepairVerdict {
    fn as_str(self) -> &'static str {
        match self {
            Self::Repaired => "repaired",
            Self::Retry => "retry",
            Self::Diverged => "diverged",
        }
    }
}

/// Decides what a replay attempt means.
///
/// Split out from the pass so it can be exercised against every outcome
/// without a store on either side.
pub(super) fn verdict_for(
    replay: &RepositoryResult<()>,
    already_applied: Option<bool>,
) -> RepairVerdict {
    if replay.is_ok() {
        return RepairVerdict::Repaired;
    }
    match already_applied {
        // The first attempt landed after all, or a later write already carried
        // this snapshot past the state this one wanted.
        Some(true) => RepairVerdict::Repaired,
        // The probe itself failed, so the store is not answering and the
        // replay's own failure says nothing either way.
        None => RepairVerdict::Retry,
        Some(false) => match replay {
            // 🔴 Only `Backend` is retried. Everything else is the store
            // stating a rule — a name already taken, a request it will not
            // accept, an operation it does not have — and asking again in
            // thirty seconds gets the same answer while the queue behind it
            // stops moving.
            Err(RepositoryError::Backend { .. }) => RepairVerdict::Retry,
            _ => RepairVerdict::Diverged,
        },
    }
}

/// What one pass over the backlog did.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RepairPass {
    pub repaired: usize,
    pub retry: usize,
    pub diverged: usize,
    /// Entries the pass did not attempt: an earlier entry for the same snapshot
    /// and direction had not cleared, or this direction has no target.
    pub skipped: usize,
    pub remaining: u64,
}

/// What one sweep over the recorded divergences did.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DivergenceSweep {
    /// Divergences whose subject was looked up in both catalogs.
    pub examined: usize,
    /// Divergences retired: neither catalog holds the snapshot any more.
    pub retired: usize,
    /// Divergences kept: a catalog still holds the snapshot, or would not say.
    pub kept: usize,
    /// Divergences this sweep did not reach — the per-sweep cap, an
    /// outstanding queue entry about the same snapshot, or only one store
    /// configured.
    pub skipped: usize,
    /// Divergences still recorded, in both directions, after the sweep.
    pub remaining: u64,
}

/// How many divergences one sweep re-checks.
///
/// 🔴 Capped, and the cap is the point of the cursor beside it. Each one costs
/// a read against *both* stores, and one of those is the store the acceptance
/// number is measured on. A cluster holding a divergence per template would
/// otherwise turn a background tidy-up into a steady load nobody asked for.
/// Sweeps resume where the last one stopped, so a large set is worked through
/// over several rather than truncated at the same place forever.
pub(super) const MAX_DIVERGENCES_PER_SWEEP: usize = 64;

/// Which store answers catalog reads.
///
/// Recorded across restarts so the guard below can tell an ordinary restart
/// from somebody moving the read side.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CatalogReadSide {
    ObjectStore,
    Postgres,
}

impl CatalogReadSide {
    fn as_bytes(self) -> &'static [u8] {
        match self {
            Self::ObjectStore => b"object_store",
            Self::Postgres => b"postgres",
        }
    }

    fn parse(raw: &[u8]) -> Option<Self> {
        match raw {
            b"object_store" => Some(Self::ObjectStore),
            b"postgres" => Some(Self::Postgres),
            _ => None,
        }
    }

    /// The direction whose debt would be lost by reading from this side.
    ///
    /// Reads answered from object storage cannot see what object storage does
    /// not have, and reads answered from PostgreSQL cannot see what the central
    /// catalog does not have. The switch onto a side is guarded by that side's
    /// own debt, and by nothing else — which is what keeps a mirror that is
    /// behind in the direction nobody is about to read from being treated as an
    /// outage.
    fn debt_that_would_be_invisible(self) -> MirrorDirection {
        match self {
            Self::ObjectStore => MirrorDirection::ObjectStore,
            Self::Postgres => MirrorDirection::Central,
        }
    }
}

/// Key holding the read side the last process ran with.
const READ_SIDE_KEY: &[u8] = b"meta:read_side";

/// Key recording that everything object storage already held has been queued
/// for the central catalog.
///
/// 🔴 Set only once the whole listing is on disk. A marker written over a
/// partial enumeration would be the exact failure this whole mechanism exists
/// to prevent: a number that says the two catalogs agree because nobody
/// counted.
const HISTORY_KEY: &[u8] = b"meta:history_queued";

/// First byte of every queued entry's key.
const OWED_PREFIX: u8 = b'o';

/// First byte of every recorded divergence's key.
const DIVERGED_PREFIX: u8 = b'd';

/// One snapshot the two catalogs disagree about, which no replay can settle.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Divergence {
    snapshot_id: SnapshotId,
    /// The write that discovered it.
    op: String,
    reason: String,
    noted_at_unix_ms: i64,
}

/// The counters for one direction.
#[derive(Default)]
struct DirectionCounters {
    /// Entries sitting in the durable queue.
    owed: AtomicU64,
    /// 🔴 Writes this process could not even write down.
    ///
    /// Counted into the lag rather than dropped from it. The write is owed
    /// whether or not the note survived, and a lag that only counted what it
    /// managed to record would report agreement precisely when it had lost the
    /// evidence of disagreement. What it cannot do is survive a restart —
    /// nothing wrote it anywhere — so it is also a counter an operator can see.
    unrecorded: AtomicU64,
    /// Snapshots recorded as permanently disagreeing.
    diverged: AtomicU64,
}

/// The durable queue of owed catalog writes, and the disagreements nothing can
/// replay.
pub struct MirrorBacklog {
    store: LocalKvStore,
    next_seq: AtomicU64,
    object_store: DirectionCounters,
    central: DirectionCounters,
    /// Where the last divergence sweep stopped, so the next resumes rather than
    /// re-reading the same first page every time.
    sweep_cursor: std::sync::Mutex<Option<Vec<u8>>>,
    /// Test-only fault injection.
    ///
    /// The local store refusing a write is the one branch below that nothing
    /// else can reach — RocksDB takes a write into its memtable whatever the
    /// filesystem is doing — and it is the branch where the lag has to keep
    /// counting a write it could not write down. Injecting it is the only way
    /// to hold that behaviour still.
    #[cfg(test)]
    refuse_local_writes: std::sync::atomic::AtomicBool,
}

impl MirrorBacklog {
    /// Opens the backlog at `path`, recovering whatever the last process left.
    ///
    /// 🔴 `Wal`, not `Memory`. What is in here is the difference between "the
    /// two stores agree" and "they do not", and a process that crashed holding
    /// the answer must not come back believing they agreed.
    pub async fn open(path: impl Into<std::path::PathBuf>) -> anyhow::Result<Arc<Self>> {
        let store = LocalKvStore::open(path, LocalStoreDurability::Wal)
            .await
            .context("open the snapshot catalog mirror backlog")?;

        let backlog = Arc::new(Self {
            store,
            next_seq: AtomicU64::new(1),
            object_store: DirectionCounters::default(),
            central: DirectionCounters::default(),
            sweep_cursor: std::sync::Mutex::new(None),
            #[cfg(test)]
            refuse_local_writes: std::sync::atomic::AtomicBool::new(false),
        });

        let owed = backlog
            .store
            .scan_prefix(vec![OWED_PREFIX])
            .await
            .context("read the snapshot catalog mirror backlog")?;
        let mut highest = 0u64;
        for (key, value) in &owed {
            let Some(seq) = decode_seq(key) else { continue };
            highest = highest.max(seq);
            // An entry nobody can decode is still an entry. Counted against the
            // object store, which is the conservative reading: it is the side
            // whose debt refuses the rollback the promise of zero data loss
            // rests on.
            let direction = OwedWrite::decode(value)
                .map(|owed| owed.direction)
                .unwrap_or(MirrorDirection::ObjectStore);
            backlog
                .counters(direction)
                .owed
                .fetch_add(1, Ordering::AcqRel);
        }
        backlog
            .next_seq
            .store(highest.saturating_add(1), Ordering::Release);

        let diverged = backlog
            .store
            .scan_prefix(vec![DIVERGED_PREFIX])
            .await
            .context("read the snapshot catalog mirror divergences")?;
        for (key, _) in &diverged {
            if let Some(direction) = decode_diverged_direction(key) {
                backlog
                    .counters(direction)
                    .diverged
                    .fetch_add(1, Ordering::AcqRel);
            }
        }

        backlog.publish_gauges();
        for direction in MirrorDirection::ALL {
            let lag = backlog.lag_toward(direction);
            let diverged = backlog.diverged_toward(direction);
            if lag > 0 || diverged > 0 {
                warn!(
                    direction = direction.as_str(),
                    mirror_lag = lag,
                    mirror_diverged = diverged,
                    "the snapshot catalog mirror starts with writes {} still owes",
                    direction.store_name()
                );
            }
        }

        Ok(backlog)
    }

    #[cfg(test)]
    fn local_writes_refused(&self) -> bool {
        self.refuse_local_writes.load(Ordering::SeqCst)
    }

    #[cfg(not(test))]
    fn local_writes_refused(&self) -> bool {
        false
    }

    async fn put_locally(&self, key: Vec<u8>, value: Vec<u8>) -> anyhow::Result<()> {
        if self.local_writes_refused() {
            anyhow::bail!("the local store refused the write");
        }
        self.store.put(key, value).await
    }

    fn counters(&self, direction: MirrorDirection) -> &DirectionCounters {
        match direction {
            MirrorDirection::ObjectStore => &self.object_store,
            MirrorDirection::Central => &self.central,
        }
    }

    fn publish_gauges(&self) {
        for direction in MirrorDirection::ALL {
            set_mirror_lag(direction, self.lag_toward(direction));
            set_mirror_diverged(direction, self.diverged_toward(direction));
        }
    }

    /// How many writes the store on `direction` owes right now.
    pub fn lag_toward(&self, direction: MirrorDirection) -> u64 {
        let counters = self.counters(direction);
        counters.owed.load(Ordering::Acquire) + counters.unrecorded.load(Ordering::Acquire)
    }

    /// How many snapshots the store on `direction` is known to disagree about
    /// in a way no replay can settle.
    pub fn diverged_toward(&self, direction: MirrorDirection) -> u64 {
        self.counters(direction).diverged.load(Ordering::Acquire)
    }

    /// Everything owed, in either direction.
    pub fn lag(&self) -> u64 {
        MirrorDirection::ALL
            .into_iter()
            .map(|direction| self.lag_toward(direction))
            .sum()
    }

    /// Refuses to move reads onto a store that is behind.
    ///
    /// 🔴 This is what makes "switching the read side loses nothing" a fact
    /// rather than a hope. Every outstanding entry is a snapshot one catalog
    /// knows about and the other does not, so a node serving reads from the
    /// side that is behind would answer "no such snapshot" for each of them —
    /// and callers delete artifacts and refuse resumes on that answer.
    ///
    /// 🔴 Debt is not the only way the two can disagree. `try_start_build`
    /// writes object storage alone in this batch, so every template's central
    /// row stays `waiting` while the object store's moves on — permanently, and
    /// with a lag of zero, because nothing the compensator could replay would
    /// close it. Moving reads to PostgreSQL on a cluster holding those
    /// templates would make every one of them vanish from the API while every
    /// number said the mirror was clean. So the guard reads the recorded
    /// divergences as well, and the switch is refused while either is non-zero.
    ///
    /// 🔴 It fires on the *switch*, not on every start. A node that crashed
    /// holding a backlog while already reading a store has to come back —
    /// refusing there would turn a mirror that is behind into a node that is
    /// down, which is exactly the trade the double write exists to avoid. So
    /// the side the last process ran with is recorded, and only a move is
    /// refused.
    ///
    /// 🔴 The one condition that is *not* about a switch: reads may not be
    /// answered from PostgreSQL while the object store's history has never been
    /// queued for it. Debt and divergence both describe writes the double write
    /// saw; a snapshot published before it was turned on was seen by neither,
    /// so both numbers read zero about it — which is how a cluster arrived at
    /// `lag = 0, diverged = 0, PostgreSQL 0 rows, object storage 32`. Unlike
    /// debt, this cannot come back: once
    /// [`Self::queue_history_toward_central`] has run, the marker is on disk
    /// for good, so refusing here cannot strand a node that was serving
    /// PostgreSQL reads a moment ago.
    pub async fn guard_read_side(&self, configured: CatalogReadSide) -> anyhow::Result<()> {
        let previous = self
            .store
            .get(READ_SIDE_KEY.to_vec())
            .await
            .context("read the catalog mirror's recorded read side")?
            .and_then(|raw| CatalogReadSide::parse(&raw));

        if configured == CatalogReadSide::Postgres && !self.history_is_queued().await? {
            anyhow::bail!(
                "snapshot.catalog.read = \"postgres\" is refused: the snapshots object storage \
                 held before the double write was turned on have never been queued for the \
                 central catalog, so neither \
                 agentenv_snapshot_catalog_mirror_lag{{direction=\"central\"}} nor \
                 agentenv_snapshot_catalog_mirror_diverged{{direction=\"central\"}} says \
                 anything about them — they read zero because nothing counted them, not because \
                 the two catalogs agree. Reads from PostgreSQL would report every one of those \
                 snapshots as absent, and callers delete artifacts and refuse resumes on that \
                 answer. Start this node once with snapshot.catalog.write = \"both\" and a \
                 reachable object store so the history is queued, wait for both gauges to reach \
                 0, then switch."
            );
        }

        if previous.is_some() && previous != Some(configured) {
            let direction = configured.debt_that_would_be_invisible();
            let lag = self.lag_toward(direction);
            let diverged = self.diverged_toward(direction);
            if lag > 0 || diverged > 0 {
                anyhow::bail!(
                    "snapshot.catalog.read is being moved to \"{configured}\" while {store} is \
                     behind: {lag} write(s) still owed and {diverged} snapshot(s) recorded as \
                     diverged. Those snapshots exist in the other catalog and not in this one, so \
                     reads from it would report them as absent and callers would treat them as \
                     deleted. Leave the read side where it is until \
                     agentenv_snapshot_catalog_mirror_lag{{direction=\"{direction}\"}} and \
                     agentenv_snapshot_catalog_mirror_diverged{{direction=\"{direction}\"}} both \
                     reach 0, then switch.",
                    configured = match configured {
                        CatalogReadSide::ObjectStore => "object_store",
                        CatalogReadSide::Postgres => "postgres",
                    },
                    store = direction.store_name(),
                    direction = direction.as_str(),
                );
            }
        }

        self.store
            .put(READ_SIDE_KEY.to_vec(), configured.as_bytes().to_vec())
            .await
            .context("record the catalog mirror's read side")
    }

    /// Whether the object store's history has been queued for the central
    /// catalog.
    pub async fn history_is_queued(&self) -> anyhow::Result<bool> {
        Ok(self
            .store
            .get(HISTORY_KEY.to_vec())
            .await
            .context("read whether the catalog mirror has queued the object store's history")?
            .is_some())
    }

    /// Queues everything object storage already holds, for the catalog that
    /// was not there when it was written.
    ///
    /// 🔴 The gap this closes was measured, on both nodes, at the moment
    /// `write = "both"` was switched on: `mirror_lag{central} = 0`,
    /// `mirror_diverged{central} = 0`, PostgreSQL **0 rows**, object storage
    /// **32**. Both gauges said the two catalogs agreed about thirty-two
    /// snapshots neither of them had ever compared. The double write only ever
    /// looks forward — it mirrors writes made *after* it was turned on — so
    /// every snapshot older than the switch was invisible to it, and a
    /// read-side switch authorised on those two numbers would have made all
    /// thirty-two vanish from the API.
    ///
    /// 🔴 The invariant this restores is the whole point of the numbers:
    /// **`lag == 0 && diverged == 0` must mean "the two catalogs agree", not
    /// "nothing is queued".** History is queued as ordinary debt rather than
    /// copied by a bespoke path, so it inherits everything the queue already
    /// does — durability across restarts, per-snapshot ordering, the
    /// probe-before-verdict that makes a replay idempotent, the attempt cap,
    /// and a permanent refusal becoming a recorded divergence instead of a
    /// silent loss. A separate copier would have needed all of that written
    /// again, and would have been the one path nothing else tests.
    ///
    /// Returns how many entries were queued. Idempotent by the marker, and
    /// harmless without it: a replay is judged against what the target already
    /// has, so queueing a snapshot the central catalog already holds settles as
    /// repaired on the first pass.
    ///
    /// 🔴 Per node, and that is a real cost rather than an oversight. A second
    /// node joining a cluster whose history another node has already mirrored
    /// enumerates it again and replays it into a catalog that already agrees —
    /// N entries that all settle on the first pass. The alternative is a
    /// cluster-wide "this has been done" fact, which nothing in this phase has
    /// a place to keep.
    pub async fn queue_history_toward_central(
        &self,
        object_store: &dyn SnapshotCatalog,
    ) -> anyhow::Result<u64> {
        if self.history_is_queued().await? {
            return Ok(0);
        }

        let records = object_store
            .list(SnapshotListFilter::matches_all())
            .await
            .context(
                "list the object-store catalog in order to queue its history for the central \
                 catalog",
            )?;

        let mut queued = 0u64;
        let mut all_recorded = true;
        for record in records {
            for op in history_ops(record) {
                all_recorded &= self.record(MirrorDirection::Central, op).await;
                queued += 1;
            }
        }

        if !all_recorded {
            anyhow::bail!(
                "queued {queued} historical catalog write(s) for the central catalog, but at \
                 least one could not be written down; leaving the backfill unmarked so the next \
                 start does it again"
            );
        }

        self.store
            .put(HISTORY_KEY.to_vec(), b"queued".to_vec())
            .await
            .context("record that the object store's history has been queued")?;

        if queued > 0 {
            warn!(
                queued,
                "queued the object-store catalog's history for the central catalog; the read side \
                 cannot move to PostgreSQL until the compensator has replayed all of it"
            );
        }
        Ok(queued)
    }

    /// Records one owed write.
    ///
    /// 🔴 A failure here is loud but not fatal to the caller: the other store
    /// already took the write and the operation already did what it was asked.
    /// What is lost is the durable record that this store is behind — so the
    /// lag counts it anyway, from memory, and says separately that it did.
    ///
    /// Returns whether the note is on disk. Only the history backfill reads the
    /// answer: it must not mark itself done over a write nothing wrote down.
    pub(super) async fn record(&self, direction: MirrorDirection, op: MirrorOp) -> bool {
        let seq = self.next_seq.fetch_add(1, Ordering::AcqRel);
        let owed = OwedWrite {
            direction,
            op,
            attempts: 0,
        };
        let stored = match serde_json::to_vec(&owed) {
            Ok(encoded) => self.put_locally(encode_seq(seq), encoded).await,
            Err(error) => Err(error.into()),
        };

        let persisted = stored.is_ok();
        match stored {
            Ok(()) => {
                self.counters(direction).owed.fetch_add(1, Ordering::AcqRel);
            }
            Err(error) => {
                // 🔴 Counted all the same. The write is owed whether or not the
                // note survived, and a lag that quietly dropped it would report
                // the two catalogs as agreeing at the one moment it had proof
                // they did not. What it cannot do is survive a restart, so it
                // is counted where an operator can see that too.
                self.counters(direction)
                    .unrecorded
                    .fetch_add(1, Ordering::AcqRel);
                record_mirror_unrecorded(direction, owed.op.name());
                error!(
                    direction = direction.as_str(),
                    op = owed.op.name(),
                    snapshot_id = %owed.op.snapshot_id(),
                    %error,
                    "could not record an owed catalog mirror write; {} is now behind by a write \
                     nothing will replay, and a restart will forget it",
                    direction.store_name()
                );
            }
        }
        self.publish_gauges();
        persisted
    }

    /// Records that the two catalogs disagree about one snapshot, permanently.
    ///
    /// Keyed by snapshot and direction, so the same disagreement discovered
    /// twice — a template's build start and then its commit — is one
    /// divergence, not two. Returns whether it was written down durably.
    pub(super) async fn note_divergence(
        &self,
        direction: MirrorDirection,
        id: &SnapshotId,
        op: &'static str,
        reason: String,
    ) -> bool {
        let key = encode_diverged(direction, id);
        let already = matches!(self.store.get(key.clone()).await, Ok(Some(_)));

        let mark = Divergence {
            snapshot_id: id.clone(),
            op: op.to_string(),
            reason,
            noted_at_unix_ms: now_unix_ms(),
        };
        let stored = match serde_json::to_vec(&mark) {
            Ok(encoded) => self.put_locally(key, encoded).await,
            Err(error) => Err(error.into()),
        };

        if let Err(error) = &stored {
            error!(
                direction = direction.as_str(),
                catalog_op = op,
                snapshot_id = %id,
                %error,
                "could not record a catalog mirror divergence; it is counted for as long as this \
                 process lives and forgotten after that"
            );
        }
        if !already {
            self.counters(direction)
                .diverged
                .fetch_add(1, Ordering::AcqRel);
            self.publish_gauges();
        }
        stored.is_ok()
    }

    /// Forgets every recorded disagreement about one snapshot.
    ///
    /// 🔴 Called when the snapshot is deleted, and only then. A delete goes to
    /// both catalogs — one of them now, the other now or from the queue — so
    /// whatever they used to disagree about is superseded by both of them not
    /// having it at all. Clearing it anywhere else would be forgetting a
    /// disagreement rather than resolving one.
    pub(super) async fn clear_divergences(&self, id: &SnapshotId) {
        for direction in MirrorDirection::ALL {
            let key = encode_diverged(direction, id);
            if !matches!(self.store.get(key.clone()).await, Ok(Some(_))) {
                continue;
            }
            if let Err(error) = self.store.delete(key).await {
                warn!(%error, snapshot_id = %id, "could not clear a catalog mirror divergence");
                continue;
            }
            self.drop_diverged(direction);
        }
        self.publish_gauges();
    }

    /// Retires recorded divergences whose subject no longer exists anywhere.
    ///
    /// 🔴 B-2. `clear_divergences` runs on delete, and the delete is routed —
    /// by the gateway, to one node. The divergence record is node-local. So a
    /// snapshot whose disagreement was recorded on one node and whose delete
    /// went through another leaves that node's
    /// `mirror_diverged{direction="central"}` pinned at one, forever, over a
    /// snapshot that exists in neither catalog. Measured: thirteen deletes
    /// split six/eight across two nodes, and one gauge stuck with no API call
    /// able to clear it. That gauge is what the read-side switch is gated on,
    /// so one stranded record blocks the switch permanently and the only remedy
    /// is wiping the node's mirror store.
    ///
    /// 🔴 Chosen over broadcasting deletes or moving the record into the
    /// cluster because it needs nothing that does not already exist, and
    /// because it is the *same* test the delete path makes: a divergence is
    /// superseded when both catalogs have stopped holding the snapshot. This
    /// asks that question directly instead of inferring it from having seen the
    /// delete go by.
    ///
    /// 🔴 What keeps it from retiring a disagreement that is still real:
    ///
    ///   - **Both** stores must answer, and both must answer *absent*. A store
    ///     that still holds the row keeps the record; a store that could not be
    ///     reached keeps it too, because "I could not look" is not "it is gone".
    ///   - A snapshot the queue still owes a write about is skipped entirely.
    ///     Otherwise a create waiting in the queue could be replayed into one
    ///     catalog just after the sweep retired the record describing exactly
    ///     that disagreement — the sweep resurrecting what it had just retired.
    ///
    /// Both targets are required: with the double write rolled back to one
    /// direction there is no second store to ask, and a sweep that retired on
    /// one store's word would be forgetting rather than settling.
    pub async fn retire_settled_divergences(
        &self,
        targets: &MirrorTargets,
    ) -> anyhow::Result<DivergenceSweep> {
        let mut sweep = DivergenceSweep::default();
        let recorded = self
            .store
            .scan_prefix(vec![DIVERGED_PREFIX])
            .await
            .context("read the snapshot catalog mirror divergences")?;
        sweep.remaining = recorded.len() as u64;
        if recorded.is_empty() {
            return Ok(sweep);
        }

        let Some((object_store, central)) = targets.pair() else {
            sweep.skipped = recorded.len();
            return Ok(sweep);
        };

        // Snapshots the queue is still going to write about. Their rows are
        // mid-flight and the sweep has no business deciding they have settled.
        let owed = self
            .store
            .scan_prefix(vec![OWED_PREFIX])
            .await
            .context("read the snapshot catalog mirror backlog")?;
        let in_flight: HashSet<SnapshotId> = owed
            .iter()
            .filter_map(|(_, value)| OwedWrite::decode(value).ok())
            .map(|owed| owed.op.snapshot_id().clone())
            .collect();

        let resume_after = self.sweep_cursor.lock().expect("sweep cursor").clone();
        let start = resume_after
            .as_ref()
            .map(|cursor| {
                recorded
                    .iter()
                    .position(|(key, _)| key > cursor)
                    .unwrap_or(0)
            })
            .unwrap_or(0);

        let mut last_examined = None;
        for offset in 0..recorded.len() {
            let (key, _) = &recorded[(start + offset) % recorded.len()];
            if sweep.examined >= MAX_DIVERGENCES_PER_SWEEP {
                sweep.skipped += 1;
                continue;
            }
            let (Some(direction), Some(id)) =
                (decode_diverged_direction(key), decode_diverged_id(key))
            else {
                // Not a key this build wrote. Left where it is: it still counts
                // toward the gauge, and removing a record nothing understands
                // is exactly the forgetting this sweep must not do.
                sweep.skipped += 1;
                continue;
            };
            if in_flight.contains(&id) {
                sweep.skipped += 1;
                continue;
            }

            last_examined = Some(key.clone());
            sweep.examined += 1;
            match (object_store.probe(&id).await, central.probe(&id).await) {
                (Ok(None), Ok(None)) => {}
                _ => {
                    sweep.kept += 1;
                    continue;
                }
            }

            if let Err(error) = self.store.delete(key.clone()).await {
                warn!(%error, snapshot_id = %id, "could not retire a settled catalog mirror divergence");
                sweep.kept += 1;
                continue;
            }
            self.drop_diverged(direction);
            record_mirror_divergence_retired(direction);
            warn!(
                direction = direction.as_str(),
                snapshot_id = %id,
                "retired a catalog mirror divergence: neither catalog holds this snapshot any \
                 more, so there is nothing left for them to disagree about"
            );
            sweep.retired += 1;
        }

        *self.sweep_cursor.lock().expect("sweep cursor") = last_examined;
        self.publish_gauges();
        sweep.remaining = MirrorDirection::ALL
            .into_iter()
            .map(|direction| self.diverged_toward(direction))
            .sum();
        Ok(sweep)
    }

    /// Replays everything owed, in order, once.
    ///
    /// 🔴 Per-snapshot ordering, not global ordering. Two writes to one
    /// snapshot have to land in the order they were made — a commit replayed
    /// before the create it depends on is a different outcome — but a snapshot
    /// whose queue is stuck must not hold up every other snapshot's, which is
    /// what a single global stop-on-first-failure would do. The two directions
    /// are independent queues for the same reason: they are different stores.
    ///
    /// 🔴 `scan_prefix`, not `entries`. The read side's recorded value and
    /// every divergence live in this store too, and a pass that loaded the
    /// whole database to find the queue would grow with the number of
    /// snapshots the two catalogs have ever disagreed about.
    pub async fn drain_once(&self, targets: &MirrorTargets) -> anyhow::Result<RepairPass> {
        let entries = self
            .store
            .scan_prefix(vec![OWED_PREFIX])
            .await
            .context("read the snapshot catalog mirror backlog")?;

        let mut pass = RepairPass::default();
        let mut blocked: HashSet<(MirrorDirection, SnapshotId)> = HashSet::new();

        // 🔴 Decoded up front, because the pass needs to know which entry is
        // the *last* one queued about each snapshot before it starts settling
        // any of them. See where `last_word` is consulted below.
        let mut queued: Vec<(Vec<u8>, u64, OwedWrite)> = Vec::with_capacity(entries.len());
        let mut last_word: HashMap<SnapshotId, u64> = HashMap::new();
        for (key, value) in entries {
            let Some(seq) = decode_seq(&key) else {
                continue;
            };
            let owed = match OwedWrite::decode(&value) {
                Ok(owed) => owed,
                Err(error) => {
                    // Unreadable and never going to become readable. Left in
                    // place so the lag still counts it and the read side stays
                    // pinned; an operator has to look.
                    error!(
                        %error,
                        "an owed catalog mirror write cannot be decoded; a catalog is behind by a \
                         write nothing can replay"
                    );
                    pass.diverged += 1;
                    continue;
                }
            };
            let latest = last_word
                .entry(owed.op.snapshot_id().clone())
                .or_insert(seq);
            *latest = (*latest).max(seq);
            queued.push((key, seq, owed));
        }

        for (key, seq, owed) in queued {
            let direction = owed.direction;
            let attempts = owed.attempts;
            let op = owed.op;

            let Some(target) = targets.for_direction(direction) else {
                // Nothing to replay into. Not an error and not a divergence:
                // the double write was turned off in this direction on purpose,
                // and the entry keeps counting until somebody turns it back on.
                pass.skipped += 1;
                continue;
            };

            if blocked.contains(&(direction, op.snapshot_id().clone())) {
                pass.skipped += 1;
                continue;
            }

            let replay = target.apply(&op).await;
            let already_applied = if replay.is_ok() {
                None
            } else {
                target
                    .probe(op.snapshot_id())
                    .await
                    .ok()
                    .map(|existing| reflects(&op, existing.as_ref()))
            };
            let mut verdict = verdict_for(&replay, already_applied);

            // 🔴 Repaired used to mean "the target took the write", and the
            // read-side guard reported that as the two catalogs *agreeing*.
            // Those are not the same claim: the backfill replayed thirty-two
            // publishes that every number called repaired, into rows whose
            // creation times were all wrong. So an entry that is about to leave
            // the queue is checked — both stores are read and their rows
            // compared — and a difference keeps it as debt rather than letting
            // the lag reach zero over it.
            //
            // 🔴 Only for the last entry queued about this snapshot. An earlier
            // one is *meant* to be overtaken: a create followed by the failure
            // that ended that build leaves the row in a state the create never
            // described, and comparing there would record a divergence the very
            // next entry was about to settle — permanently, since nothing but a
            // delete clears one.
            let mut mismatch: Option<(&'static str, String)> = None;
            if verdict == RepairVerdict::Repaired && last_word.get(op.snapshot_id()) == Some(&seq) {
                match compare_catalogs(targets, op.snapshot_id()).await {
                    CatalogComparison::Agree | CatalogComparison::NotComparable => {}
                    CatalogComparison::Unreadable(reason) => {
                        // Unverified is not verified. The write landed, but the
                        // claim the lag makes is about *agreement*, and nothing
                        // here established any — so the entry stays debt until
                        // something can.
                        mismatch = Some(("unreadable", reason));
                        verdict = RepairVerdict::Retry;
                    }
                    CatalogComparison::Disagree { field, detail } => {
                        record_mirror_content_mismatch(direction, field);
                        mismatch = Some((field, detail));
                        verdict = RepairVerdict::Retry;
                    }
                }
            }

            // 🔴 The cap is applied here rather than inside `verdict_for`,
            // which is a decision about *this* attempt and has no business
            // knowing how many came before it. What the cap changes is whether
            // an attempt that learned nothing is still worth keeping as debt.
            let attempts = attempts.saturating_add(1);
            let exhausted = verdict == RepairVerdict::Retry && attempts >= MAX_REPLAY_ATTEMPTS;
            if exhausted {
                verdict = RepairVerdict::Diverged;
            }

            match verdict {
                RepairVerdict::Repaired => {
                    if let Err(error) = self.store.delete(key).await {
                        // The write landed; only the note saying it was owed
                        // did not go away. Retrying it is safe — every replay
                        // is checked against the target first — so leave it.
                        warn!(%error, "could not clear a repaired catalog mirror entry");
                        pass.retry += 1;
                        blocked.insert((direction, op.snapshot_id().clone()));
                        continue;
                    }
                    self.drop_owed(direction);
                    record_mirror_repaired(direction, op.name());
                    pass.repaired += 1;
                }
                RepairVerdict::Diverged => {
                    error!(
                        direction = direction.as_str(),
                        op = op.name(),
                        snapshot_id = %op.snapshot_id(),
                        error = ?replay.as_ref().err(),
                        "{} refused an owed catalog mirror write in a way a retry cannot change; \
                         the two catalogs disagree about this snapshot",
                        direction.store_name()
                    );
                    record_mirror_repair_failed(direction, op.name(), verdict.as_str());

                    // 🔴 Moved out of the queue rather than left in it. Nothing
                    // a replay can do will change the answer, so retrying it
                    // every interval buys nothing and costs one request against
                    // the store the acceptance number is measured on. It is
                    // still counted, as a divergence — but only once the record
                    // of it is durable, because a queue entry that was dropped
                    // and not written down anywhere would be a disagreement
                    // that disappeared.
                    let refusal = mismatch
                        .as_ref()
                        .map(|(_, detail)| detail.clone())
                        .or_else(|| replay.as_ref().err().map(|error| error.to_string()))
                        .unwrap_or_else(|| "the store refused the replay".to_string());
                    let reason = if exhausted {
                        format!(
                            "{MAX_REPLAY_ATTEMPTS} replays did not settle this write, so it is \
                             no longer counted as debt: {refusal}"
                        )
                    } else {
                        refusal
                    };
                    let recorded = self
                        .note_divergence(direction, op.snapshot_id(), op.name(), reason)
                        .await;
                    if recorded && self.store.delete(key).await.is_ok() {
                        self.drop_owed(direction);
                    }
                    pass.diverged += 1;
                    blocked.insert((direction, op.snapshot_id().clone()));
                }
                RepairVerdict::Retry => {
                    if let Some((field, detail)) = &mismatch {
                        warn!(
                            direction = direction.as_str(),
                            op = op.name(),
                            snapshot_id = %op.snapshot_id(),
                            field,
                            detail,
                            "an owed catalog mirror write replayed, but the two catalogs still \
                             do not agree about this snapshot; it stays counted as debt"
                        );
                    }
                    // The count goes back to disk before the entry is left
                    // alone, so that the cap survives the restart the entry
                    // itself is designed to survive.
                    let counted = OwedWrite {
                        direction,
                        op: op.clone(),
                        attempts,
                    };
                    match serde_json::to_vec(&counted) {
                        Ok(encoded) => {
                            if let Err(error) = self.put_locally(key, encoded).await {
                                warn!(
                                    %error,
                                    "could not count a failed catalog mirror replay; this entry \
                                     will be retried without its attempts being capped"
                                );
                            }
                        }
                        Err(error) => warn!(%error, "could not encode a catalog mirror entry"),
                    }
                    pass.retry += 1;
                    record_mirror_repair_failed(direction, op.name(), verdict.as_str());
                    blocked.insert((direction, op.snapshot_id().clone()));
                }
            }
        }

        self.publish_gauges();
        pass.remaining = self.lag();
        Ok(pass)
    }

    fn drop_owed(&self, direction: MirrorDirection) {
        let counters = self.counters(direction);
        if counters.owed.fetch_sub(1, Ordering::AcqRel) == 0 {
            counters.owed.store(0, Ordering::Release);
        }
    }

    fn drop_diverged(&self, direction: MirrorDirection) {
        let counters = self.counters(direction);
        if counters.diverged.fetch_sub(1, Ordering::AcqRel) == 0 {
            counters.diverged.store(0, Ordering::Release);
        }
    }
}

/// Big-endian so the store's key order is the order the writes were made.
///
/// Prefixed so the other things this store holds — which side reads came from
/// last, and every recorded divergence — cannot land inside the queue's key
/// range and be replayed as if they were writes.
fn encode_seq(seq: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + 8);
    key.push(OWED_PREFIX);
    key.extend_from_slice(&seq.to_be_bytes());
    key
}

fn decode_seq(key: &[u8]) -> Option<u64> {
    let (prefix, seq) = key.split_first()?;
    if *prefix != OWED_PREFIX {
        return None;
    }
    seq.try_into().ok().map(u64::from_be_bytes)
}

/// Keyed by direction then snapshot, so one disagreement is one entry however
/// many times it is discovered.
fn encode_diverged(direction: MirrorDirection, id: &SnapshotId) -> Vec<u8> {
    let mut key = Vec::new();
    key.push(DIVERGED_PREFIX);
    key.push(direction.key_byte());
    key.extend_from_slice(id.to_string().as_bytes());
    key
}

fn decode_diverged_direction(key: &[u8]) -> Option<MirrorDirection> {
    match key {
        [DIVERGED_PREFIX, byte, ..] => MirrorDirection::ALL
            .into_iter()
            .find(|direction| direction.key_byte() == *byte),
        _ => None,
    }
}

/// The snapshot a divergence key is about.
///
/// Read off the key rather than out of the value: the key is the identity the
/// record is filed under, and a value written by an older build might not carry
/// one at all.
fn decode_diverged_id(key: &[u8]) -> Option<SnapshotId> {
    match key {
        [DIVERGED_PREFIX, _, id @ ..] => {
            SnapshotId::parse(std::str::from_utf8(id).ok()?.trim()).ok()
        }
        _ => None,
    }
}

/// The writes that would have been mirrored had the double write been on when
/// this record was made.
///
/// 🔴 The *operations*, reconstructed — not a row copy. A committed record
/// replays as the publish it was, which is the one write that carries the
/// payload and binds the alias; an uncommitted one replays as the create it
/// was, and a build that failed replays as the create plus the failure, because
/// no single central statement opens a row already in `error`.
fn history_ops(record: SnapshotRecord) -> Vec<MirrorOp> {
    if record.committed.is_some() {
        return match commit_from_record(record) {
            Some(commit) => vec![MirrorOp::PublishCommit { commit }],
            None => Vec::new(),
        };
    }

    let failed = match &record.source {
        SnapshotSource::Template { build } if build.status == TemplateBuildStatus::Error => {
            Some(build.error_reason.clone().unwrap_or_else(|| {
                TemplateBuildErrorReason::new(
                    "this build had already failed when the catalog mirror was turned on, and \
                     object storage did not record why",
                )
            }))
        }
        _ => None,
    };

    match failed {
        Some(reason) => {
            let id = record.id.clone();
            vec![
                MirrorOp::Create { record },
                MirrorOp::MarkBuildError { id, reason },
            ]
        }
        None => vec![MirrorOp::Create { record }],
    }
}

/// The commit a committed record describes.
///
/// The inverse of `commit_opening_record`, and it exists only for the backfill:
/// every other publish still has the real `SnapshotCommit` in hand.
///
/// 🔴 `created_at_unix_ms` is copied, not left for the replay to stamp. This
/// is the one place in the whole queue where the write being replayed is older
/// than the queue itself, and a replay that stamped its own clock rewrote every
/// backfilled row's creation time to the moment the backfill ran. Measured:
/// thirty-two rows in PostgreSQL all reading one instant, against object
/// storage's real spread — a different listing order and a wrong `createdAt`
/// for every snapshot that existed before the double write was turned on.
fn commit_from_record(record: SnapshotRecord) -> Option<SnapshotCommit> {
    let committed = record.committed?;
    Some(SnapshotCommit {
        id: record.id,
        alias: record.alias,
        source: match record.source {
            SnapshotSource::Template { .. } => SnapshotPublishSource::Template,
            SnapshotSource::Sandbox { source_sandbox_id } => {
                SnapshotPublishSource::Sandbox { source_sandbox_id }
            }
        },
        resources: record.resources,
        created_at_unix_ms: Some(record.created_at_unix_ms),
        committed,
    })
}

fn now_unix_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::repository::mirror::test_doubles::{
        commit_for, committed_record, record_for, ScriptedCatalog, ScriptedCentral,
    };
    use crate::snapshot::types::SnapshotAlias;

    /// A backlog on a node that has already queued its object store's history.
    ///
    /// 🔴 The default, deliberately. Every test below is about *debt*, and a
    /// node that has not run the backfill is refused from PostgreSQL for a
    /// reason that has nothing to do with what those tests hold still — so
    /// leaving it unqueued would make them all fail for one shared, unrelated
    /// reason, which is the same as not testing them. The unqueued state is
    /// covered on its own, further down.
    async fn backlog(dir: &tempfile::TempDir) -> Arc<MirrorBacklog> {
        let backlog = unmirrored_backlog(dir).await;
        backlog
            .queue_history_toward_central(&ScriptedCatalog::default())
            .await
            .expect("an empty object store has no history to queue");
        backlog
    }

    /// A backlog exactly as it comes off disk, history not yet queued.
    async fn unmirrored_backlog(dir: &tempfile::TempDir) -> Arc<MirrorBacklog> {
        MirrorBacklog::open(dir.path().join("mirror"))
            .await
            .expect("the backlog should open")
    }

    fn targets(catalog: &Arc<ScriptedCatalog>) -> MirrorTargets {
        MirrorTargets::object_store(Arc::clone(catalog) as Arc<dyn SnapshotCatalog>)
    }

    // ── the repair decision ─────────────────────────────────────────────

    #[test]
    fn a_successful_replay_is_repaired() {
        assert_eq!(verdict_for(&Ok(()), None), RepairVerdict::Repaired);
    }

    /// 🔴 The lost-acknowledgement case. The first attempt landed and only its
    /// answer went missing, so the replay is refused — and the target, asked
    /// directly, already has it. Reading this as anything but repaired leaves
    /// an entry nothing can ever clear.
    #[test]
    fn a_replay_the_target_already_reflects_is_repaired() {
        let refused: RepositoryResult<()> = Err(RepositoryError::InvalidRequest {
            reason: "already exists".to_string(),
        });
        assert_eq!(verdict_for(&refused, Some(true)), RepairVerdict::Repaired);
    }

    /// 🔴 A probe that failed says nothing either way, so the failure of the
    /// replay says nothing either. Calling this diverged would abandon a write
    /// over a store that was merely unreachable for a moment.
    #[test]
    fn an_unanswerable_probe_is_retried() {
        let refused: RepositoryResult<()> = Err(RepositoryError::InvalidRequest {
            reason: "already exists".to_string(),
        });
        assert_eq!(verdict_for(&refused, None), RepairVerdict::Retry);
    }

    #[test]
    fn a_transport_failure_the_target_does_not_reflect_is_retried() {
        let unreachable: RepositoryResult<()> = Err(RepositoryError::Backend {
            message: "connection refused".to_string(),
            source: None,
        });
        assert_eq!(verdict_for(&unreachable, Some(false)), RepairVerdict::Retry);
    }

    /// 🔴 The one that must not be retried forever. The store stated a rule —
    /// the name is taken — and asking again in thirty seconds gets the same
    /// answer while everything behind it stops.
    #[test]
    fn a_refusal_the_target_does_not_reflect_is_diverged() {
        for refusal in [
            RepositoryError::AliasConflict {
                alias: "taken".to_string(),
                existing: SnapshotId::generate(),
                new_id: SnapshotId::generate(),
            },
            RepositoryError::InvalidRequest {
                reason: "no".to_string(),
            },
            RepositoryError::Unsupported {
                feature: "no".to_string(),
            },
        ] {
            let refused: RepositoryResult<()> = Err(refusal);
            assert_eq!(
                verdict_for(&refused, Some(false)),
                RepairVerdict::Diverged,
                "a rule the store stated must not be retried forever"
            );
        }
    }

    /// The probe's reading, per operation. A delete is applied when the row is
    /// *gone*, which is the one that inverts.
    #[test]
    fn what_counts_as_already_applied_depends_on_the_operation() {
        let id = SnapshotId::generate();
        let record = record_for(&id);
        let mut committed = record_for(&id);
        committed.mark_committed(
            None,
            crate::types::SandboxResources::default(),
            crate::snapshot::types::CommittedSnapshot::mock(),
            crate::snapshot::types::SnapshotPublishSource::Template,
            0,
        );

        let create = MirrorOp::Create {
            record: record.clone(),
        };
        assert!(!reflects(&create, None));
        assert!(reflects(&create, Some(&record)));

        let publish = MirrorOp::PublishCommit {
            commit: commit_for(&id, None),
        };
        assert!(
            !reflects(&publish, Some(&record)),
            "a waiting row is not a commit"
        );
        assert!(reflects(&publish, Some(&committed)));

        let delete = MirrorOp::DeleteRecord {
            record: record.clone(),
        };
        assert!(reflects(&delete, None), "a row that is gone was deleted");
        assert!(!reflects(&delete, Some(&record)));
    }

    // ── the pass ────────────────────────────────────────────────────────

    #[tokio::test]
    async fn a_recorded_write_raises_the_lag_and_a_repaired_one_lowers_it() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let backlog = backlog(&dir).await;
        let id = SnapshotId::generate();

        assert_eq!(backlog.lag(), 0);
        backlog
            .record(
                MirrorDirection::ObjectStore,
                MirrorOp::PublishCommit {
                    commit: commit_for(&id, None),
                },
            )
            .await;
        assert_eq!(backlog.lag(), 1);
        assert_eq!(backlog.lag_toward(MirrorDirection::ObjectStore), 1);
        assert_eq!(backlog.lag_toward(MirrorDirection::Central), 0);

        let target = Arc::new(ScriptedCatalog::default());
        let pass = backlog
            .drain_once(&targets(&target))
            .await
            .expect("the pass should run");

        assert_eq!(pass.repaired, 1);
        assert_eq!(pass.remaining, 0);
        assert_eq!(backlog.lag(), 0);
        assert_eq!(target.calls(), vec![format!("publish_commit:{id}")]);
    }

    /// 🔴 The two directions are two queues. A snapshot the object store owes
    /// and the central catalog does not must not have its central lag counted,
    /// because the two numbers guard two different switches.
    #[tokio::test]
    async fn the_two_directions_are_counted_apart() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let backlog = backlog(&dir).await;

        backlog
            .record(
                MirrorDirection::ObjectStore,
                MirrorOp::Create {
                    record: record_for(&SnapshotId::generate()),
                },
            )
            .await;
        backlog
            .record(
                MirrorDirection::Central,
                MirrorOp::Create {
                    record: record_for(&SnapshotId::generate()),
                },
            )
            .await;
        backlog
            .record(
                MirrorDirection::Central,
                MirrorOp::Create {
                    record: record_for(&SnapshotId::generate()),
                },
            )
            .await;

        assert_eq!(backlog.lag_toward(MirrorDirection::ObjectStore), 1);
        assert_eq!(backlog.lag_toward(MirrorDirection::Central), 2);
        assert_eq!(backlog.lag(), 3);
    }

    /// 🔴 A direction with no target is left alone rather than attempted or
    /// dropped. Rolling the double write back takes the central catalog away on
    /// purpose, and the pass still has to pay off what object storage is owed.
    #[tokio::test]
    async fn a_direction_with_no_target_is_skipped_and_the_other_still_drains() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let backlog = backlog(&dir).await;
        let owed_here = SnapshotId::generate();

        backlog
            .record(
                MirrorDirection::Central,
                MirrorOp::Create {
                    record: record_for(&SnapshotId::generate()),
                },
            )
            .await;
        backlog
            .record(
                MirrorDirection::ObjectStore,
                MirrorOp::Create {
                    record: record_for(&owed_here),
                },
            )
            .await;

        let target = Arc::new(ScriptedCatalog::default());
        let pass = backlog
            .drain_once(&targets(&target))
            .await
            .expect("the pass should run");

        assert_eq!(pass.repaired, 1);
        assert_eq!(pass.skipped, 1);
        assert_eq!(backlog.lag_toward(MirrorDirection::ObjectStore), 0);
        assert_eq!(
            backlog.lag_toward(MirrorDirection::Central),
            1,
            "the central debt is still owed, and still counted"
        );
        assert_eq!(target.calls(), vec![format!("create:{owed_here}")]);
    }

    /// A central-direction entry replays into the central catalog, through the
    /// probe that can see rows an ordinary read cannot.
    #[tokio::test]
    async fn a_central_entry_replays_into_the_central_catalog() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let backlog = backlog(&dir).await;
        let id = SnapshotId::generate();
        backlog
            .record(
                MirrorDirection::Central,
                MirrorOp::Create {
                    record: record_for(&id),
                },
            )
            .await;

        // The object store already took this write — that is *why* it is owed
        // to the central catalog and to nothing else.
        let object_store = Arc::new(ScriptedCatalog::default());
        object_store.seed(record_for(&id));
        let central = Arc::new(ScriptedCentral::default());
        let pass = backlog
            .drain_once(
                &targets(&object_store)
                    .with_central(Arc::clone(&central) as Arc<dyn CentralCatalogWrites>),
            )
            .await
            .expect("the pass should run");

        assert_eq!(pass.repaired, 1);
        assert!(central.holds(&id).is_some());
        assert!(
            !object_store
                .calls()
                .iter()
                .any(|call| !call.starts_with("get:")),
            "a central debt must not be *written* into the object store; only read back to \
             compare: {:?}",
            object_store.calls()
        );
    }

    /// 🔴 A central replay is judged against rows an ordinary read cannot see.
    ///
    /// A template the catalog holds is `waiting` and a build it failed is
    /// `error`; both are invisible at the resolvable scope. A probe made there
    /// would report a row that is plainly present as absent — and every replay
    /// it judged would be retried or abandoned forever.
    #[tokio::test]
    async fn a_central_replay_is_judged_against_rows_an_ordinary_read_cannot_see() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let backlog = backlog(&dir).await;
        let id = SnapshotId::generate();
        backlog
            .record(
                MirrorDirection::Central,
                MirrorOp::Create {
                    record: record_for(&id),
                },
            )
            .await;

        let central = Arc::new(ScriptedCentral::default());
        // The row is there, `waiting`, which no resolvable read would return —
        // and the write itself is refused, so the probe is what decides.
        central.seed(record_for(&id));
        central.refuse(
            super::super::test_doubles::CentralCall::Begin,
            CatalogRefusal::AliasTaken {
                holder: SnapshotId::generate().to_string(),
            },
        );
        let object_store = Arc::new(ScriptedCatalog::default());
        object_store.seed(record_for(&id));

        let pass = backlog
            .drain_once(
                &targets(&object_store)
                    .with_central(Arc::clone(&central) as Arc<dyn CentralCatalogWrites>),
            )
            .await
            .expect("the pass should run");

        assert_eq!(
            pass.repaired, 1,
            "the catalog plainly holds the row; the entry must clear"
        );
        assert_eq!(backlog.lag_toward(MirrorDirection::Central), 0);
    }

    /// 🔴 Per-snapshot ordering. A commit replayed before the create it depends
    /// on is a different outcome, so one snapshot's stuck entry holds the rest
    /// of *its own* queue — and nothing else's.
    #[tokio::test]
    async fn a_stuck_snapshot_holds_only_its_own_queue() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let backlog = backlog(&dir).await;
        let stuck = SnapshotId::generate();
        let healthy = SnapshotId::generate();

        backlog
            .record(
                MirrorDirection::ObjectStore,
                MirrorOp::Create {
                    record: record_for(&stuck),
                },
            )
            .await;
        backlog
            .record(
                MirrorDirection::ObjectStore,
                MirrorOp::PublishCommit {
                    commit: commit_for(&stuck, None),
                },
            )
            .await;
        backlog
            .record(
                MirrorDirection::ObjectStore,
                MirrorOp::Create {
                    record: record_for(&healthy),
                },
            )
            .await;

        let target = Arc::new(ScriptedCatalog::default());
        target.fail(&stuck, true);

        let pass = backlog
            .drain_once(&targets(&target))
            .await
            .expect("the pass should run");

        assert_eq!(pass.retry, 1, "the stuck snapshot's first entry retries");
        assert_eq!(pass.skipped, 1, "its second entry is not attempted");
        assert_eq!(pass.repaired, 1, "the other snapshot's entry is repaired");
        assert_eq!(backlog.lag(), 2);
        assert!(
            !target.calls().contains(&format!("publish_commit:{stuck}")),
            "the second write for a snapshot whose first is stuck must not be attempted: {:?}",
            target.calls()
        );
        assert!(target.calls().contains(&format!("create:{healthy}")));
    }

    /// 🔴 A divergence stops being debt and becomes a recorded disagreement.
    ///
    /// Left in the queue it would be replayed, and probed for, every interval
    /// for as long as the node lived — buying an answer that cannot change and
    /// costing a request against the store the acceptance number is measured
    /// on. It still has to be *counted*, because the two stores really do
    /// disagree, so it moves rather than disappearing.
    #[tokio::test]
    async fn a_diverged_entry_becomes_a_recorded_divergence_and_stops_being_replayed() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let backlog = backlog(&dir).await;
        let id = SnapshotId::generate();
        backlog
            .record(
                MirrorDirection::ObjectStore,
                MirrorOp::Create {
                    record: record_for(&id),
                },
            )
            .await;

        let target = Arc::new(ScriptedCatalog::default());
        target.fail(&id, false);

        let pass = backlog
            .drain_once(&targets(&target))
            .await
            .expect("the pass should run");

        assert_eq!(pass.diverged, 1);
        assert_eq!(pass.repaired, 0);
        assert_eq!(
            backlog.lag(),
            0,
            "it is not debt any more; nothing can replay it"
        );
        assert_eq!(
            backlog.diverged_toward(MirrorDirection::ObjectStore),
            1,
            "but the two stores still disagree, and the guard reads this"
        );

        // The control: a second pass does not touch the store again.
        let before = target.calls().len();
        backlog
            .drain_once(&targets(&target))
            .await
            .expect("the pass should run");
        assert_eq!(
            target.calls().len(),
            before,
            "a recorded divergence must not be replayed again: {:?}",
            target.calls()
        );
    }

    /// 🔴 And it stays counted across a restart. A divergence that only lived
    /// in memory would let the next start authorise the switch it had just
    /// refused.
    #[tokio::test]
    async fn a_recorded_divergence_survives_the_process_that_found_it() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let id = SnapshotId::generate();
        {
            let backlog = backlog(&dir).await;
            backlog
                .note_divergence(
                    MirrorDirection::Central,
                    &id,
                    "try_start_build",
                    "build admission is not wired".to_string(),
                )
                .await;
            assert_eq!(backlog.diverged_toward(MirrorDirection::Central), 1);
        }

        let reopened = backlog(&dir).await;
        assert_eq!(reopened.diverged_toward(MirrorDirection::Central), 1);
        assert_eq!(reopened.diverged_toward(MirrorDirection::ObjectStore), 0);
    }

    // ── divergences that stopped being true ─────────────────────────────

    /// A backlog with both stores wired, so a comparison and a sweep have two
    /// sides to work with.
    fn both_targets(
        object_store: &Arc<ScriptedCatalog>,
        central: &Arc<ScriptedCentral>,
    ) -> MirrorTargets {
        targets(object_store).with_central(Arc::clone(central) as Arc<dyn CentralCatalogWrites>)
    }

    /// 🔴 B-2. A divergence must not outlive the snapshot it is about.
    ///
    /// `clear_divergences` runs on the node that handles the delete, and the
    /// gateway chooses that node. Measured: thirteen deletes split six/eight
    /// across two nodes, leaving one node's `mirror_diverged{central}` pinned
    /// at one over a snapshot that no longer existed in either catalog, with no
    /// API call able to clear it — and that gauge is what gates the read-side
    /// switch.
    #[tokio::test]
    async fn a_divergence_about_a_snapshot_nothing_holds_any_more_is_retired() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let backlog = backlog(&dir).await;
        let id = SnapshotId::generate();
        backlog
            .note_divergence(
                MirrorDirection::Central,
                &id,
                "try_start_build",
                "build admission is not wired".to_string(),
            )
            .await;
        assert_eq!(backlog.diverged_toward(MirrorDirection::Central), 1);

        // Deleted through some other node: neither store has it any more.
        let object_store = Arc::new(ScriptedCatalog::default());
        let central = Arc::new(ScriptedCentral::default());
        let sweep = backlog
            .retire_settled_divergences(&both_targets(&object_store, &central))
            .await
            .expect("the sweep should run");

        assert_eq!(sweep.retired, 1);
        assert_eq!(backlog.diverged_toward(MirrorDirection::Central), 0);
        assert_eq!(sweep.remaining, 0);
    }

    /// And it stays retired across a restart — the record is gone from disk,
    /// not just from this process's counter.
    #[tokio::test]
    async fn a_retired_divergence_does_not_come_back() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let id = SnapshotId::generate();
        {
            let backlog = backlog(&dir).await;
            backlog
                .note_divergence(
                    MirrorDirection::Central,
                    &id,
                    "try_start_build",
                    "why".into(),
                )
                .await;
            backlog
                .retire_settled_divergences(&both_targets(
                    &Arc::new(ScriptedCatalog::default()),
                    &Arc::new(ScriptedCentral::default()),
                ))
                .await
                .expect("the sweep should run");
        }

        let reopened = backlog(&dir).await;
        assert_eq!(reopened.diverged_toward(MirrorDirection::Central), 0);
    }

    /// 🔴 The disagreement is still real while either catalog still holds the
    /// snapshot. Retiring there would be forgetting one, which is the failure
    /// the whole divergence record exists to prevent.
    #[tokio::test]
    async fn a_divergence_about_a_snapshot_that_still_exists_is_kept() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let backlog = backlog(&dir).await;
        let id = SnapshotId::generate();
        backlog
            .note_divergence(
                MirrorDirection::Central,
                &id,
                "try_start_build",
                "why".into(),
            )
            .await;

        let object_store = Arc::new(ScriptedCatalog::default());
        object_store.seed(record_for(&id));
        let central = Arc::new(ScriptedCentral::default());
        let sweep = backlog
            .retire_settled_divergences(&both_targets(&object_store, &central))
            .await
            .expect("the sweep should run");

        assert_eq!(sweep.retired, 0);
        assert_eq!(sweep.kept, 1);
        assert_eq!(backlog.diverged_toward(MirrorDirection::Central), 1);
    }

    /// 🔴 "I could not look" is not "it is gone". A store that would not answer
    /// keeps the record.
    #[tokio::test]
    async fn a_divergence_is_kept_when_a_catalog_will_not_answer() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let backlog = backlog(&dir).await;
        let id = SnapshotId::generate();
        backlog
            .note_divergence(
                MirrorDirection::Central,
                &id,
                "try_start_build",
                "why".into(),
            )
            .await;

        let object_store = Arc::new(ScriptedCatalog::default());
        object_store.break_reads();
        let central = Arc::new(ScriptedCentral::default());
        let sweep = backlog
            .retire_settled_divergences(&both_targets(&object_store, &central))
            .await
            .expect("the sweep should run");

        assert_eq!(sweep.retired, 0);
        assert_eq!(backlog.diverged_toward(MirrorDirection::Central), 1);
    }

    /// 🔴 The one way this sweep could resurrect a disagreement it had just
    /// retired: a write still sitting in the queue is about to put the snapshot
    /// back into one catalog and not the other. A snapshot the queue still owes
    /// a write about is left alone until it does not.
    #[tokio::test]
    async fn a_divergence_is_not_retired_while_the_queue_still_owes_a_write_about_it() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let backlog = backlog(&dir).await;
        let id = SnapshotId::generate();
        backlog
            .note_divergence(
                MirrorDirection::Central,
                &id,
                "try_start_build",
                "why".into(),
            )
            .await;
        backlog
            .record(
                MirrorDirection::Central,
                MirrorOp::Create {
                    record: record_for(&id),
                },
            )
            .await;

        let object_store = Arc::new(ScriptedCatalog::default());
        let central = Arc::new(ScriptedCentral::default());
        let sweep = backlog
            .retire_settled_divergences(&both_targets(&object_store, &central))
            .await
            .expect("the sweep should run");

        assert_eq!(sweep.retired, 0);
        assert_eq!(sweep.skipped, 1);
        assert_eq!(backlog.diverged_toward(MirrorDirection::Central), 1);
    }

    /// With the double write rolled back there is only one store, and one
    /// store's word is not enough to say two of them have stopped disagreeing.
    #[tokio::test]
    async fn a_sweep_with_only_one_store_retires_nothing() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let backlog = backlog(&dir).await;
        let id = SnapshotId::generate();
        backlog
            .note_divergence(
                MirrorDirection::Central,
                &id,
                "try_start_build",
                "why".into(),
            )
            .await;

        let object_store = Arc::new(ScriptedCatalog::default());
        let sweep = backlog
            .retire_settled_divergences(&targets(&object_store))
            .await
            .expect("the sweep should run");

        assert_eq!(sweep.retired, 0);
        assert_eq!(sweep.skipped, 1);
        assert_eq!(backlog.diverged_toward(MirrorDirection::Central), 1);
    }

    /// 🔴 One sweep does bounded work. Each divergence costs a read against
    /// both stores, and one of them is the store the migration's acceptance
    /// number is measured on.
    #[tokio::test]
    async fn one_sweep_examines_at_most_its_cap_and_the_next_carries_on() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let backlog = backlog(&dir).await;
        for _ in 0..(MAX_DIVERGENCES_PER_SWEEP + 5) {
            backlog
                .note_divergence(
                    MirrorDirection::Central,
                    &SnapshotId::generate(),
                    "try_start_build",
                    "why".into(),
                )
                .await;
        }

        let object_store = Arc::new(ScriptedCatalog::default());
        let central = Arc::new(ScriptedCentral::default());
        let targets = both_targets(&object_store, &central);

        let first = backlog
            .retire_settled_divergences(&targets)
            .await
            .expect("the sweep should run");
        assert_eq!(first.examined, MAX_DIVERGENCES_PER_SWEEP);
        assert_eq!(first.retired, MAX_DIVERGENCES_PER_SWEEP);
        assert_eq!(first.skipped, 5);

        let second = backlog
            .retire_settled_divergences(&targets)
            .await
            .expect("the sweep should run");
        assert_eq!(second.retired, 5, "the next sweep finishes the rest");
        assert_eq!(backlog.diverged_toward(MirrorDirection::Central), 0);
    }

    /// One snapshot's disagreement, discovered twice, is one divergence.
    #[tokio::test]
    async fn the_same_disagreement_discovered_twice_is_counted_once() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let backlog = backlog(&dir).await;
        let id = SnapshotId::generate();

        backlog
            .note_divergence(MirrorDirection::Central, &id, "try_start_build", "a".into())
            .await;
        backlog
            .note_divergence(MirrorDirection::Central, &id, "publish_commit", "b".into())
            .await;

        assert_eq!(backlog.diverged_toward(MirrorDirection::Central), 1);
    }

    /// The lost-acknowledgement case end to end: the replay is refused, the
    /// target turns out to have it, and the entry clears.
    #[tokio::test]
    async fn an_entry_the_target_already_holds_clears_without_being_rewritten() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let backlog = backlog(&dir).await;
        let id = SnapshotId::generate();
        backlog
            .record(
                MirrorDirection::ObjectStore,
                MirrorOp::PublishCommit {
                    commit: commit_for(&id, None),
                },
            )
            .await;

        let target = Arc::new(ScriptedCatalog::default());
        target.fail(&id, false);
        target.hold(&id);

        let pass = backlog
            .drain_once(&targets(&target))
            .await
            .expect("the pass should run");

        assert_eq!(pass.repaired, 1);
        assert_eq!(backlog.lag(), 0);
        assert_eq!(backlog.diverged_toward(MirrorDirection::ObjectStore), 0);
    }

    /// 🔴 The whole reason this is on disk. A process that crashed holding
    /// owed writes must come back still owing them, in the direction it owed
    /// them.
    #[tokio::test]
    async fn owed_writes_survive_the_process_that_recorded_them() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let id = SnapshotId::generate();
        {
            let backlog = backlog(&dir).await;
            backlog
                .record(
                    MirrorDirection::Central,
                    MirrorOp::PublishCommit {
                        commit: commit_for(&id, None),
                    },
                )
                .await;
            assert_eq!(backlog.lag(), 1);
        }

        let reopened = backlog(&dir).await;
        assert_eq!(reopened.lag(), 1, "a restart must not forget what is owed");
        assert_eq!(
            reopened.lag_toward(MirrorDirection::Central),
            1,
            "nor which store owed them"
        );

        let central = Arc::new(ScriptedCentral::default());
        let object_store = Arc::new(ScriptedCatalog::default());
        object_store.hold(&id);
        let pass = reopened
            .drain_once(
                &targets(&object_store)
                    .with_central(Arc::clone(&central) as Arc<dyn CentralCatalogWrites>),
            )
            .await
            .expect("the pass should run");
        assert_eq!(pass.repaired, 1);
    }

    /// 🔴 An entry a previous build wrote is still an entry. The rollback from
    /// `write = "both"` depends on paying off what an older process recorded,
    /// and that process wrote the operation with no direction on it.
    #[tokio::test]
    async fn an_entry_from_before_directions_existed_is_an_object_store_debt() {
        let id = SnapshotId::generate();
        let legacy = serde_json::to_vec(&MirrorOp::Create {
            record: record_for(&id),
        })
        .expect("the old shape should encode");

        let decoded = OwedWrite::decode(&legacy).expect("an old entry must still decode");
        assert_eq!(decoded.direction, MirrorDirection::ObjectStore);
        assert_eq!(decoded.op.snapshot_id(), &id);

        assert!(OwedWrite::decode(b"not json at all").is_err());
    }

    // ── the lag it cannot write down ────────────────────────────────────

    /// 🔴 F-7. A write the local store refused is still owed. Dropping it from
    /// the lag would report the two catalogs as agreeing at the one moment
    /// there is proof they do not — and the lag is what the read-side switch
    /// is authorised on.
    #[tokio::test]
    async fn a_write_that_could_not_be_written_down_still_counts_against_the_lag() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let backlog = backlog(&dir).await;
        backlog.refuse_local_writes.store(true, Ordering::SeqCst);

        backlog
            .record(
                MirrorDirection::ObjectStore,
                MirrorOp::Create {
                    record: record_for(&SnapshotId::generate()),
                },
            )
            .await;

        assert_eq!(
            backlog.lag_toward(MirrorDirection::ObjectStore),
            1,
            "the write is owed whether or not the note survived"
        );

        // And it pins the switch, which is the point of counting it.
        backlog
            .guard_read_side(CatalogReadSide::Postgres)
            .await
            .expect("recording the side should work");
        backlog
            .guard_read_side(CatalogReadSide::ObjectStore)
            .await
            .expect_err("a lag that cannot be replayed still refuses the switch");
    }

    /// 🔴 A divergence the queue could not record keeps its queue entry. The
    /// entry is dropped only once something durable has taken its place;
    /// dropping it either way would be a disagreement that disappeared.
    #[tokio::test]
    async fn a_divergence_that_could_not_be_recorded_keeps_its_queue_entry() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let backlog = backlog(&dir).await;
        let id = SnapshotId::generate();
        backlog
            .record(
                MirrorDirection::ObjectStore,
                MirrorOp::Create {
                    record: record_for(&id),
                },
            )
            .await;

        let target = Arc::new(ScriptedCatalog::default());
        target.fail(&id, false);
        backlog.refuse_local_writes.store(true, Ordering::SeqCst);

        let pass = backlog
            .drain_once(&targets(&target))
            .await
            .expect("the pass should run");

        assert_eq!(pass.diverged, 1);
        assert_eq!(
            backlog.lag_toward(MirrorDirection::ObjectStore),
            1,
            "the entry stays until something durable replaces it"
        );
    }

    // ── the read-side guard ─────────────────────────────────────────────

    /// 🔴 The guard fires on the switch, not on the start. A node that crashed
    /// while already reading the object store has to come back; refusing there
    /// would turn a mirror that is behind into a node that is down.
    #[tokio::test]
    async fn an_ordinary_restart_on_the_object_store_is_allowed_with_a_backlog() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let backlog = backlog(&dir).await;
        backlog
            .guard_read_side(CatalogReadSide::ObjectStore)
            .await
            .expect("the first start records the side");
        backlog
            .record(
                MirrorDirection::ObjectStore,
                MirrorOp::Create {
                    record: record_for(&SnapshotId::generate()),
                },
            )
            .await;

        backlog
            .guard_read_side(CatalogReadSide::ObjectStore)
            .await
            .expect("a restart on the same side must be allowed");
    }

    /// 🔴 Moving reads back onto a store that is behind would report every
    /// owed snapshot as absent, and absence is an instruction downstream.
    #[tokio::test]
    async fn moving_reads_back_to_the_object_store_is_refused_while_it_is_behind() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let backlog = backlog(&dir).await;
        backlog
            .guard_read_side(CatalogReadSide::Postgres)
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

        let error = backlog
            .guard_read_side(CatalogReadSide::ObjectStore)
            .await
            .expect_err("the switch must be refused while writes are owed");
        assert!(
            error.to_string().contains("1 write(s) still owed"),
            "the refusal must say how far behind: {error}"
        );

        // The control: with nothing owed, the same switch is allowed.
        let target = Arc::new(ScriptedCatalog::default());
        backlog
            .drain_once(&targets(&target))
            .await
            .expect("the pass should run");
        assert_eq!(backlog.lag(), 0);
        backlog
            .guard_read_side(CatalogReadSide::ObjectStore)
            .await
            .expect("a drained mirror must let the switch through");
    }

    /// 🔴 The switch is guarded by the debt of the side being switched *to*,
    /// and by nothing else. An object-store debt says nothing about whether
    /// PostgreSQL is complete, and refusing over it would make a mirror that is
    /// behind in the direction nobody is about to read stop the node.
    #[tokio::test]
    async fn only_the_debt_of_the_side_being_read_refuses_the_switch() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let backlog = backlog(&dir).await;
        backlog
            .guard_read_side(CatalogReadSide::ObjectStore)
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

        backlog
            .guard_read_side(CatalogReadSide::Postgres)
            .await
            .expect("an object-store debt must not refuse a move onto PostgreSQL");
    }

    /// The symmetric refusal: PostgreSQL is behind, so reads must not move onto
    /// it.
    #[tokio::test]
    async fn moving_reads_to_postgres_is_refused_while_the_central_catalog_is_behind() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let backlog = backlog(&dir).await;
        backlog
            .guard_read_side(CatalogReadSide::ObjectStore)
            .await
            .expect("recording the side should work");
        backlog
            .record(
                MirrorDirection::Central,
                MirrorOp::Create {
                    record: record_for(&SnapshotId::generate()),
                },
            )
            .await;

        backlog
            .guard_read_side(CatalogReadSide::Postgres)
            .await
            .expect_err("reads must not move onto a catalog that is behind");
    }

    /// A first start records the side without judging it: there is nothing to
    /// switch from.
    #[tokio::test]
    async fn a_first_start_is_never_a_switch() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let backlog = backlog(&dir).await;
        backlog
            .record(
                MirrorDirection::Central,
                MirrorOp::Create {
                    record: record_for(&SnapshotId::generate()),
                },
            )
            .await;

        backlog
            .guard_read_side(CatalogReadSide::Postgres)
            .await
            .expect("a node that has never recorded a side is not moving one");
    }

    // ── the attempt cap ─────────────────────────────────────────────────

    /// 🔴 An unbounded retry is a defect whatever it is retrying. A store that
    /// never comes back would otherwise be asked forever, one request per
    /// entry per interval, against the store the acceptance number is measured
    /// on — and the lag it holds up is what the next batch's gate reads.
    #[tokio::test]
    async fn an_entry_that_never_settles_stops_being_debt() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let backlog = backlog(&dir).await;
        let target = Arc::new(ScriptedCatalog::default());
        target.break_it();

        backlog
            .record(
                MirrorDirection::ObjectStore,
                MirrorOp::Create {
                    record: record_for(&SnapshotId::generate()),
                },
            )
            .await;

        for attempt in 1..MAX_REPLAY_ATTEMPTS {
            let pass = backlog
                .drain_once(&targets(&target))
                .await
                .expect("the pass should run");
            assert_eq!(
                pass.retry, 1,
                "attempt {attempt} must still be counted as debt worth keeping"
            );
            assert_eq!(backlog.lag_toward(MirrorDirection::ObjectStore), 1);
        }

        let pass = backlog
            .drain_once(&targets(&target))
            .await
            .expect("the pass should run");
        assert_eq!(pass.diverged, 1, "the cap has to actually fire");
        assert_eq!(
            backlog.lag_toward(MirrorDirection::ObjectStore),
            0,
            "an entry nothing can settle stops being debt"
        );
        assert_eq!(
            backlog.diverged_toward(MirrorDirection::ObjectStore),
            1,
            "and becomes a disagreement an operator can see instead of disappearing"
        );
    }

    /// 🔴 The count is on disk, not in memory. A node that restarts more often
    /// than the cap expires would otherwise retry the same unwinnable write for
    /// as long as the cluster lived, which is the cap not existing.
    #[tokio::test]
    async fn the_attempt_count_survives_a_restart() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let target = Arc::new(ScriptedCatalog::default());
        target.break_it();

        {
            let backlog = backlog(&dir).await;
            backlog
                .record(
                    MirrorDirection::ObjectStore,
                    MirrorOp::Create {
                        record: record_for(&SnapshotId::generate()),
                    },
                )
                .await;
            for _ in 1..MAX_REPLAY_ATTEMPTS {
                backlog
                    .drain_once(&targets(&target))
                    .await
                    .expect("the pass should run");
            }
            assert_eq!(backlog.lag_toward(MirrorDirection::ObjectStore), 1);
        }

        let restarted = unmirrored_backlog(&dir).await;
        assert_eq!(
            restarted.lag_toward(MirrorDirection::ObjectStore),
            1,
            "the entry itself survives, which is what the queue is for"
        );
        let pass = restarted
            .drain_once(&targets(&target))
            .await
            .expect("the pass should run");
        assert_eq!(
            pass.diverged, 1,
            "and so does what it has already cost: one more attempt is the last one"
        );
    }

    /// The control face: an entry that settles clears without ever reaching the
    /// cap, and a store that comes back on the last attempt is repaired rather
    /// than abandoned.
    #[tokio::test]
    async fn a_store_that_comes_back_before_the_cap_is_repaired() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let backlog = backlog(&dir).await;
        let target = Arc::new(ScriptedCatalog::default());
        target.break_it();

        backlog
            .record(
                MirrorDirection::ObjectStore,
                MirrorOp::Create {
                    record: record_for(&SnapshotId::generate()),
                },
            )
            .await;
        for _ in 1..MAX_REPLAY_ATTEMPTS {
            backlog
                .drain_once(&targets(&target))
                .await
                .expect("the pass should run");
        }

        target.fix_it();
        let pass = backlog
            .drain_once(&targets(&target))
            .await
            .expect("the pass should run");
        assert_eq!(pass.repaired, 1);
        assert_eq!(backlog.lag(), 0);
        assert_eq!(backlog.diverged_toward(MirrorDirection::ObjectStore), 0);
    }

    // ── the history the double write never saw ──────────────────────────

    fn errored_template(id: &SnapshotId) -> SnapshotRecord {
        let mut record = record_for(id);
        if let SnapshotSource::Template { build } = &mut record.source {
            build.status = TemplateBuildStatus::Error;
            build.error_reason = Some(TemplateBuildErrorReason::new("it did not build"));
        }
        record
    }

    /// 🔴 The state every cluster is in at the moment `write = "both"` is
    /// switched on: object storage holds snapshots, PostgreSQL holds none, and
    /// both gauges read zero because the double write only ever mirrors writes
    /// made after it was turned on. Measured on the cluster as `lag = 0,
    /// diverged = 0, PG 0 rows, object storage 32` — and a read-side switch
    /// authorised on those numbers makes all thirty-two vanish from the API.
    #[tokio::test]
    async fn the_history_the_double_write_never_saw_is_queued_as_debt() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let backlog = unmirrored_backlog(&dir).await;
        let published = SnapshotId::generate();
        let waiting = SnapshotId::generate();
        let failed = SnapshotId::generate();
        let object_store = ScriptedCatalog::default().with_history(vec![
            committed_record(&published),
            record_for(&waiting),
            errored_template(&failed),
        ]);

        assert_eq!(
            backlog.lag_toward(MirrorDirection::Central),
            0,
            "nothing is queued before the backfill runs, which is the whole problem"
        );

        let queued = backlog
            .queue_history_toward_central(&object_store)
            .await
            .expect("the history should queue");

        // Four: the publish, the create, and the create-then-fail pair that a
        // build which had already failed takes two statements to describe.
        assert_eq!(queued, 4);
        assert_eq!(backlog.lag_toward(MirrorDirection::Central), 4);
        assert_eq!(
            backlog.lag_toward(MirrorDirection::ObjectStore),
            0,
            "object storage is the side that already has all of this"
        );
    }

    /// It runs once. A node that restarts would otherwise re-queue its whole
    /// history every start, which is harmless per entry and unbounded in
    /// aggregate.
    #[tokio::test]
    async fn the_history_is_queued_only_once() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let backlog = unmirrored_backlog(&dir).await;
        let object_store =
            ScriptedCatalog::default().with_history(vec![record_for(&SnapshotId::generate())]);

        assert_eq!(
            backlog
                .queue_history_toward_central(&object_store)
                .await
                .expect("the history should queue"),
            1
        );
        assert_eq!(
            backlog
                .queue_history_toward_central(&object_store)
                .await
                .expect("a second call should do nothing"),
            0
        );
        assert_eq!(backlog.lag_toward(MirrorDirection::Central), 1);
    }

    /// 🔴 What is queued is replayable, which is the reason it goes through the
    /// queue at all rather than through a bespoke copier. A backfill that
    /// produced entries the compensator could not land would be a lag that
    /// never drains — the same defect it was written to remove.
    #[tokio::test]
    async fn the_queued_history_replays_into_the_central_catalog() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let backlog = unmirrored_backlog(&dir).await;
        let published = SnapshotId::generate();
        let waiting = SnapshotId::generate();
        let object_store = Arc::new(
            ScriptedCatalog::default()
                .with_history(vec![committed_record(&published), record_for(&waiting)]),
        );
        backlog
            .queue_history_toward_central(object_store.as_ref())
            .await
            .expect("the history should queue");

        let central = Arc::new(ScriptedCentral::default());
        let pass = backlog
            .drain_once(
                &MirrorTargets::object_store(Arc::clone(&object_store) as Arc<dyn SnapshotCatalog>)
                    .with_central(Arc::clone(&central) as Arc<dyn CentralCatalogWrites>),
            )
            .await
            .expect("the pass should run");

        assert_eq!(pass.repaired, 2);
        assert_eq!(backlog.lag_toward(MirrorDirection::Central), 0);
        assert!(
            central
                .holds(&published)
                .is_some_and(|row| row.committed.is_some()),
            "a snapshot that was published before the mirror existed must arrive published"
        );
        assert!(central.holds(&waiting).is_some());
    }

    /// 🔴 B-1. The replay carries the snapshot's own creation time, not the
    /// replay's.
    ///
    /// Measured on the cluster: all thirty-two backfilled rows in PostgreSQL
    /// read one instant — the moment the backfill ran — against object
    /// storage's real spread across two minutes. `created_at_ms` is what the
    /// listing orders by and what `createdAt` is served from, so PostgreSQL
    /// returned those thirty-two in a completely different order and would have
    /// told every caller the wrong creation date. And `lag == 0` called it
    /// agreement.
    #[tokio::test]
    async fn a_backfilled_publish_keeps_the_creation_time_it_already_had() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let backlog = unmirrored_backlog(&dir).await;
        let published = SnapshotId::generate();
        let object_store =
            Arc::new(ScriptedCatalog::default().with_history(vec![committed_record(&published)]));
        backlog
            .queue_history_toward_central(object_store.as_ref())
            .await
            .expect("the history should queue");

        let central = Arc::new(ScriptedCentral::default());
        let pass = backlog
            .drain_once(
                &targets(&object_store)
                    .with_central(Arc::clone(&central) as Arc<dyn CentralCatalogWrites>),
            )
            .await
            .expect("the pass should run");

        assert_eq!(pass.repaired, 1);
        let row = central.holds(&published).expect("the row should be there");
        assert_eq!(
            row.created_at_unix_ms,
            committed_record(&published).created_at_unix_ms,
            "a replayed publish must record the snapshot's creation time, not the replay's"
        );
    }

    /// 🔴 B-1, the other half. A store that *took* the write is not the same
    /// thing as two stores that agree, and the lag is read as the second.
    ///
    /// Here the central catalog accepts a create whose creation time is not the
    /// one object storage holds. Every number before this said repaired.
    #[tokio::test]
    async fn a_write_the_target_took_over_a_row_that_disagrees_is_still_debt() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let backlog = backlog(&dir).await;
        let id = SnapshotId::generate();

        let object_store = Arc::new(ScriptedCatalog::default());
        object_store.seed(record_for(&id));

        let mut drifted = record_for(&id);
        drifted.created_at_unix_ms += 5_000;
        backlog
            .record(
                MirrorDirection::Central,
                MirrorOp::Create { record: drifted },
            )
            .await;

        let central = Arc::new(ScriptedCentral::default());
        let pass = backlog
            .drain_once(
                &targets(&object_store)
                    .with_central(Arc::clone(&central) as Arc<dyn CentralCatalogWrites>),
            )
            .await
            .expect("the pass should run");

        assert_eq!(pass.repaired, 0, "the two catalogs do not agree");
        assert_eq!(pass.retry, 1);
        assert_eq!(
            backlog.lag_toward(MirrorDirection::Central),
            1,
            "the lag must not reach zero over rows that say different things"
        );
    }

    /// A disagreement no pass can close stops being debt and becomes a
    /// divergence that names the field, rather than the refusal of a replay
    /// that in fact succeeded.
    #[tokio::test]
    async fn a_disagreement_nothing_settles_becomes_a_divergence() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let backlog = backlog(&dir).await;
        let id = SnapshotId::generate();

        let object_store = Arc::new(ScriptedCatalog::default());
        object_store.seed(record_for(&id));
        let mut drifted = record_for(&id);
        drifted.created_at_unix_ms += 5_000;
        backlog
            .record(
                MirrorDirection::Central,
                MirrorOp::Create { record: drifted },
            )
            .await;

        let central = Arc::new(ScriptedCentral::default());
        let targets = targets(&object_store)
            .with_central(Arc::clone(&central) as Arc<dyn CentralCatalogWrites>);
        for _ in 1..MAX_REPLAY_ATTEMPTS {
            backlog
                .drain_once(&targets)
                .await
                .expect("the pass should run");
        }
        let pass = backlog
            .drain_once(&targets)
            .await
            .expect("the pass should run");

        assert_eq!(pass.diverged, 1);
        assert_eq!(backlog.lag_toward(MirrorDirection::Central), 0);
        assert_eq!(backlog.diverged_toward(MirrorDirection::Central), 1);
    }

    /// 🔴 A comparison nobody could make is not a comparison that passed.
    ///
    /// The write landed; what the lag claims is that the two catalogs agree,
    /// and a store that would not answer established nothing of the sort.
    #[tokio::test]
    async fn a_comparison_that_could_not_be_made_leaves_the_write_owed() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let backlog = backlog(&dir).await;
        let id = SnapshotId::generate();

        let object_store = Arc::new(ScriptedCatalog::default());
        object_store.seed(record_for(&id));
        object_store.break_reads();
        backlog
            .record(
                MirrorDirection::Central,
                MirrorOp::Create {
                    record: record_for(&id),
                },
            )
            .await;

        let central = Arc::new(ScriptedCentral::default());
        let pass = backlog
            .drain_once(
                &targets(&object_store)
                    .with_central(Arc::clone(&central) as Arc<dyn CentralCatalogWrites>),
            )
            .await
            .expect("the pass should run");

        assert!(
            central.holds(&id).is_some(),
            "the write itself still has to land"
        );
        assert_eq!(pass.repaired, 0);
        assert_eq!(backlog.lag_toward(MirrorDirection::Central), 1);
    }

    /// 🔴 Only the *last* entry queued about a snapshot is compared, and this
    /// is why.
    ///
    /// A build that had already failed replays as a create plus the failure —
    /// no single central statement opens a row already in `error`. Between the
    /// two the central row is `waiting` while object storage says `error`, and
    /// a comparison made there would record a divergence the very next entry
    /// was about to settle. Nothing but deleting the snapshot clears one.
    #[tokio::test]
    async fn a_snapshot_still_holding_a_later_entry_is_not_compared_yet() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let backlog = unmirrored_backlog(&dir).await;
        let failed = SnapshotId::generate();
        let object_store =
            Arc::new(ScriptedCatalog::default().with_history(vec![errored_template(&failed)]));
        backlog
            .queue_history_toward_central(object_store.as_ref())
            .await
            .expect("the history should queue");

        let central = Arc::new(ScriptedCentral::default());
        let pass = backlog
            .drain_once(
                &targets(&object_store)
                    .with_central(Arc::clone(&central) as Arc<dyn CentralCatalogWrites>),
            )
            .await
            .expect("the pass should run");

        assert_eq!(pass.repaired, 2, "both halves of the replay have to land");
        assert_eq!(backlog.lag_toward(MirrorDirection::Central), 0);
        assert_eq!(
            backlog.diverged_toward(MirrorDirection::Central),
            0,
            "the state between a create and the failure that follows it is not a divergence"
        );
    }

    // ── what "the two catalogs agree" compares ──────────────────────────

    #[test]
    fn two_rows_that_say_the_same_thing_agree() {
        let id = SnapshotId::generate();
        assert_eq!(
            catalog_disagreement(Some(&committed_record(&id)), Some(&committed_record(&id))),
            None
        );
    }

    #[test]
    fn a_row_only_one_catalog_holds_is_a_disagreement() {
        let id = SnapshotId::generate();
        let row = record_for(&id);
        assert_eq!(
            catalog_disagreement(Some(&row), None).map(|(field, _)| field),
            Some("presence")
        );
        assert_eq!(
            catalog_disagreement(None, Some(&row)).map(|(field, _)| field),
            Some("presence")
        );
        assert_eq!(catalog_disagreement(None, None), None);
    }

    #[test]
    fn each_compared_field_is_actually_compared() {
        let id = SnapshotId::generate();
        let base = committed_record(&id);

        let mut moved = base.clone();
        moved.created_at_unix_ms += 1;
        let mut renamed = base.clone();
        renamed.alias = Some(SnapshotAlias::parse("named").expect("alias parses"));
        let mut resized = base.clone();
        resized.resources.memory_mib += 1;
        let mut resourced = base.clone();
        resourced.source = SnapshotSource::Sandbox {
            source_sandbox_id: "sbx".to_string(),
        };
        let mut failed = base.clone();
        if let SnapshotSource::Template { build } = &mut failed.source {
            build.status = TemplateBuildStatus::Error;
        }
        let mut repayloaded = base.clone();
        repayloaded
            .committed
            .as_mut()
            .expect("a committed row")
            .runtime_versions
            .kernel_version = "kernel-other".to_string();
        let mut uncommitted = base.clone();
        uncommitted.committed = None;

        for (expected, other) in [
            ("created_at", moved),
            ("alias", renamed),
            ("resources", resized),
            ("source", resourced),
            ("build_status", failed),
            ("committed", repayloaded),
            ("committed", uncommitted),
        ] {
            assert_eq!(
                catalog_disagreement(Some(&base), Some(&other)).map(|(field, _)| field),
                Some(expected),
                "a difference in {expected} has to be reported as one"
            );
        }
    }

    /// 🔴 The exclusions, held still. Each of these differs between the two
    /// catalogs on paths that are working correctly, and reporting one would
    /// make a healthy cluster look permanently divergent.
    #[test]
    fn the_bookkeeping_a_store_keeps_to_itself_is_not_a_disagreement() {
        let id = SnapshotId::generate();
        let base = committed_record(&id);

        let mut touched = base.clone();
        touched.updated_at_unix_ms += 60_000;

        let mut timed = base.clone();
        if let SnapshotSource::Template { build } = &mut timed.source {
            build.started_at_unix_ms = Some(1);
            build.finished_at_unix_ms = Some(2);
        }

        let mut explained = base.clone();
        if let SnapshotSource::Template { build } = &mut explained.source {
            build.error_reason = Some(TemplateBuildErrorReason::new("object storage never said"));
        }

        for (why, other) in [
            ("updated_at is each store's own clock", touched),
            (
                "build timestamps are never sent to the central catalog",
                timed,
            ),
            (
                "the backfill synthesises a reason object storage does not hold",
                explained,
            ),
        ] {
            assert_eq!(
                catalog_disagreement(Some(&base), Some(&other)),
                None,
                "{why}"
            );
        }
    }

    /// 🔴 A backfill that could not read the object store must not mark itself
    /// done. Marking it would make the gauges say the two catalogs agree about
    /// snapshots nobody managed to enumerate — which is the exact shape of the
    /// bug, one level up.
    #[tokio::test]
    async fn a_backfill_that_cannot_list_leaves_itself_undone() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let backlog = unmirrored_backlog(&dir).await;
        let object_store = ScriptedCatalog::default();
        object_store.break_it();

        backlog
            .queue_history_toward_central(&object_store)
            .await
            .expect_err("an object store that cannot be listed has not been queued");
        assert!(!backlog
            .history_is_queued()
            .await
            .expect("reading the marker should work"));
    }

    /// 🔴 And until it has run, reads may not be answered from PostgreSQL. Debt
    /// and divergence both describe writes the double write *saw*; a snapshot
    /// published before it was turned on was seen by neither, so both numbers
    /// read zero about it. This is the condition that is not about a switch —
    /// a first start counts too, because there is no state a fresh node could
    /// be coming back from.
    #[tokio::test]
    async fn reads_may_not_move_to_postgres_before_the_history_is_queued() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let backlog = unmirrored_backlog(&dir).await;

        let error = backlog
            .guard_read_side(CatalogReadSide::Postgres)
            .await
            .expect_err("PostgreSQL has never been given the history to answer from");
        assert!(
            error.to_string().contains("never been queued"),
            "the refusal must say what is missing: {error}"
        );

        // Reading from object storage is unaffected: it is the side that has
        // the history.
        backlog
            .guard_read_side(CatalogReadSide::ObjectStore)
            .await
            .expect("object storage is the side that already holds all of it");

        // The control: once the history is queued and drained, the same switch
        // is allowed.
        backlog
            .queue_history_toward_central(&ScriptedCatalog::default())
            .await
            .expect("an empty object store has no history to queue");
        backlog
            .guard_read_side(CatalogReadSide::Postgres)
            .await
            .expect("a queued and empty history must let the switch through");
    }
}
