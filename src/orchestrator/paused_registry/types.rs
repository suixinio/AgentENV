use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::orchestrator::store::SandboxMetadata;
use crate::snapshot::SnapshotId;
use crate::types::{ExecutionId, SandboxId};

/// Lifecycle of a registry row.
///
/// The row is created by the node that pauses the sandbox and lives until the
/// sandbox is deleted. It deliberately outlives the paused period: once a
/// sandbox is resumed the row stays behind as `Running`, still naming the last
/// durable snapshot, so losing the node that resumed it does not lose the
/// sandbox — the next resume rebuilds it from that snapshot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PausedRegistryState {
    /// The sandbox is paused locally but its snapshot has not landed in the
    /// repository yet. Only the origin node can resume it in this state.
    Publishing,
    /// The snapshot is durable in the repository. Any node can resume it.
    Paused,
    /// A node has claimed this sandbox and is bringing it back up.
    Resuming,
    /// The sandbox is paused but its snapshot never reached the repository, so
    /// only `origin_node_id` can bring it back. The row exists purely so the
    /// cluster can tell "this sandbox is still parked on its node" apart from
    /// "this sandbox is gone" — a distinction reconciliation depends on.
    LocalOnly,
    /// The sandbox is live on `origin_node_id`. `snapshot_id` still names the
    /// snapshot it was last resumed from, which is what makes it recoverable if
    /// that node is lost before the sandbox is paused again.
    Running,
}

impl PausedRegistryState {
    /// Decodes the textual form stored in the registry. The encoded values are
    /// written literally by the backend's SQL and pinned there by a CHECK
    /// constraint, so this is the only place that has to know them.
    pub(super) fn parse(value: &str) -> Option<Self> {
        match value {
            "publishing" => Some(Self::Publishing),
            "paused" => Some(Self::Paused),
            "resuming" => Some(Self::Resuming),
            "local_only" => Some(Self::LocalOnly),
            "running" => Some(Self::Running),
            _ => None,
        }
    }
}

/// A paused sandbox as seen by the whole cluster.
///
/// This never carries artifacts. `snapshot_id` is the reference the resuming
/// node hands to the snapshot repository to rebuild the sandbox; `metadata` is
/// the same [`SandboxMetadata`] the node-local persister stores, so a resume on
/// a different node reconstructs identical sandbox identity and configuration.
#[derive(Clone, Debug)]
pub struct PausedSandboxEntry {
    pub sandbox_id: SandboxId,
    pub cluster_id: Uuid,
    pub state: PausedRegistryState,
    /// Bumped on every state transition. Callers pass the generation they
    /// observed back into the registry so a stale writer cannot overwrite a
    /// newer decision (for example a pause completing after another node has
    /// already claimed the sandbox for resume).
    pub generation: i64,
    /// The node that currently holds the sandbox: the one that paused it, or
    /// the one it was last resumed on. A scheduling hint, never a binding —
    /// but also the answer to "whose local copy is authoritative", which is
    /// what lets every other node recognise its own copy as superseded.
    pub origin_node_id: String,
    /// The node that took the sandbox for a resume. Only set while
    /// `state == Resuming`; distinct from `origin_node_id`, which still names
    /// the node whose disk holds the local artifacts.
    pub claimed_by_node_id: Option<String>,
    /// `None` while `state == Publishing`.
    pub snapshot_id: Option<SnapshotId>,
    /// The sandbox's identity and configuration, as it looked when it was
    /// paused. Present on the write path and on a granted claim; `None` on a
    /// bulk read.
    ///
    /// 🔴 `None` says *this answer did not carry the record*, never *this
    /// sandbox has no record*. Every row in the table has one — the write that
    /// creates the row is refused without it — so there is no such thing as a
    /// sandbox whose record is absent, and code that reads `None` as an empty
    /// or default record would rebuild a sandbox that is not the one that was
    /// asked for.
    ///
    /// 🔴 Optional because one backend genuinely cannot supply it, not because
    /// it is unimportant. Exactly one caller reads it — the cross-node rebuild,
    /// whose entry comes from [`ResumeClaim::Claimed`] — while the two bulk
    /// consumers look only at `state`, `origin_node_id`, `claimed_by_node_id`
    /// and `generation`. A backend that fetches rows over the network therefore
    /// leaves it out of the batch read, which keeps a node's whole roster well
    /// under any message size limit and confines the byte-for-byte round trip
    /// of this record to the two calls that actually carry it.
    ///
    /// Absent, not defaulted: a default `SandboxMetadata` carries a freshly
    /// generated id that matches no sandbox, so it would rebuild something that
    /// is not the sandbox that was asked for, and nothing would report an
    /// error.
    pub metadata: Option<SandboxMetadata>,
    /// The incarnation this row is fenced against, or `None` when the row's
    /// state has none — a parked row names no run.
    ///
    /// 🔴 On a granted claim this is the incarnation the claim allocated, and
    /// it is the one the claimant must run under and quote at `mark_running`.
    /// A claimant that mints its own instead matches no predicate on the
    /// registry side, and every cross-node resume fails.
    pub execution_id: Option<ExecutionId>,
    pub paused_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// What [`PausedSandboxRegistry::begin_pause`](super::PausedSandboxRegistry::begin_pause) hands back.
#[derive(Debug, Clone)]
pub struct BeganPause {
    /// The generation the caller must quote when completing or aborting.
    pub generation: i64,
    /// The snapshot the row pointed at before this pause replaced it, if any.
    ///
    /// Nothing references it once the new pause completes, so the caller is
    /// responsible for deleting it — that deletion is deliberately deferred to
    /// here rather than done at resume time, so a sandbox always has one
    /// durable snapshot behind it while it runs.
    pub previous_snapshot_id: Option<SnapshotId>,
}

/// One sandbox a node is reporting itself the holder of, and when that sandbox
/// is currently due to end.
///
/// The deadline travels with the renewal rather than being derived from the
/// stored `metadata` because the two disagree in exactly the case that matters.
/// `metadata` is whatever the sandbox looked like when it was paused; a resume
/// may set a different timeout, and callers extend timeouts on live sandboxes
/// all the time. Reading the deadline out of the row would therefore reclaim
/// sandboxes that still had hours to run.
///
/// `None` means the sandbox has no deadline at all, which is not the same as
/// "unknown": it is a sandbox that was asked never to expire, and reclamation
/// leaves it alone forever.
#[derive(Clone, Copy, Debug)]
pub struct HeldSandbox {
    pub sandbox_id: SandboxId,
    pub expires_at: Option<DateTime<Utc>>,
}

/// What a node's successor process found waiting for it in the registry.
///
/// Both numbers describe sandboxes the previous process on this machine was
/// holding when it died. They are reported separately because they mean
/// different things to an operator: `released` sandboxes are recoverable and
/// will come back on the next resume, `discarded` ones are gone.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReleasedHoldings {
    /// Rows handed back to the cluster as `paused`, recoverable from the
    /// snapshot they name.
    pub released: u64,
    /// Rows deleted because no snapshot was ever published for them, so nothing
    /// remains to rebuild the sandbox from.
    pub discarded: u64,
}

/// What a reclamation pass did to the rows of nodes that stopped reporting.
///
/// Same two outcomes as [`ReleasedHoldings`], for the same reason — the
/// difference is only in what established that the holder is gone: there, being
/// its successor; here, the sandbox outliving its own deadline while nobody
/// renewed for it.
pub type ReclaimedHoldings = ReleasedHoldings;

impl ReleasedHoldings {
    /// Whether anything at all was found, i.e. whether the previous process
    /// died holding sandboxes rather than shutting down cleanly.
    pub fn is_empty(&self) -> bool {
        self.released == 0 && self.discarded == 0
    }
}

/// Outcome of trying to take ownership of a paused sandbox for a resume.
#[derive(Debug)]
pub enum ResumeClaim {
    /// The caller owns the sandbox and must either resume it or release the claim.
    Claimed {
        entry: Box<PausedSandboxEntry>,
        /// What the row said *before* the claim moved it to `Resuming`.
        ///
        /// 🔴 It cannot be read off `entry`: the claim is a single conditional
        /// `UPDATE`, and `RETURNING` hands back the row as the statement left
        /// it — `state` is therefore always `Resuming` there, whatever it was
        /// a moment earlier. Reading it from `entry` is how this claim came to
        /// report every ordinary resume as a lease takeover for months.
        ///
        /// The distinction it carries is not cosmetic. `Paused` means the
        /// snapshot was durable and nothing was lost. `Publishing` /
        /// `LocalOnly` mean the claim overrode a node that never finished
        /// uploading, so the sandbox comes back one snapshot behind and the
        /// work since that snapshot is gone — the one event on this path an
        /// operator has to be able to find.
        previous_state: PausedRegistryState,
    },
    /// No registry row: the sandbox is unknown to the cluster.
    NotFound,
    /// The snapshot is still uploading, so only the origin node can serve this
    /// resume. Carries the origin node so the caller can redirect.
    NotReady { origin_node_id: String },
    /// Another node claimed it first.
    Conflict {
        origin_node_id: String,
        reason: ConflictReason,
    },
}

/// Which of the two situations a [`ResumeClaim::Conflict`] describes.
///
/// They call for opposite responses. `LiveElsewhere` means the sandbox is
/// running on another node and this one must not touch it — retrying is how a
/// second live copy happens. `ClaimLost` means the row is claimable again and
/// this caller merely lost a race, so retrying is exactly right. Flattened into
/// one variant, a caller either retries something it must not or gives up on
/// something it could have had.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConflictReason {
    /// The sandbox is live on the node named alongside this. Nobody may take it.
    LiveElsewhere,
    /// This caller held the claim and no longer does; the row is claimable.
    ClaimLost,
    /// The backend did not say. Treated as `LiveElsewhere` wherever the two
    /// differ, because that is the answer whose mistake is recoverable.
    Unspecified,
}

/// Which of the three answers [`mark_running`] gave.
///
/// [`mark_running`]: super::PausedSandboxRegistry::mark_running
///
/// 🔴 `Untracked` and `HeldElsewhere` were one `false` until D11, and they mean
/// opposite things. Untracked is the common, healthy case: a sandbox that has
/// never been paused has no row, and the node carries on. HeldElsewhere means a
/// row exists and another node holds the claim on it — two nodes believe they
/// are bringing the same sandbox up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarkRunningOutcome {
    /// No row. What a sandbox that has never been paused looks like.
    Untracked,
    /// The row now names this node as its holder.
    Adopted,
    /// A row exists and another node holds the claim on it.
    HeldElsewhere,
}

impl MarkRunningOutcome {
    /// Whether the cluster tracks this sandbox and now names this node.
    ///
    /// Kept as a helper rather than left to callers comparing variants, so the
    /// two non-adopted cases cannot quietly collapse back into one at a call
    /// site that only wanted the boolean.
    pub fn adopted(self) -> bool {
        matches!(self, Self::Adopted)
    }
}

/// Which of the three answers [`renew_sandbox_deadline`] gave.
///
/// [`renew_sandbox_deadline`]: super::PausedSandboxRegistry::renew_sandbox_deadline
///
/// A bare "did it write" would run together the same two situations
/// [`MarkRunningOutcome`] exists to split apart: a sandbox this cluster never
/// tracked (the common, healthy case — a disabled registry, or a resume whose
/// own `mark_running` has not landed yet) and a row that exists but has moved
/// on to a different incarnation since the caller last observed it `Running`
/// locally. The second case is the one a caller must never retry the same
/// deadline against: the row it would be retrying is not the sandbox that
/// asked for the extension any more.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeadlineRenewalOutcome {
    /// The row now carries the new deadline.
    Renewed,
    /// No row. What a sandbox this cluster does not track looks like.
    NotTracked,
    /// A row exists but is not `Running` under the incarnation this call
    /// named — it moved on since. The deadline was not written.
    Superseded,
}
