use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::orchestrator::store::SandboxMetadata;
use crate::snapshot::SnapshotId;
use crate::types::SandboxId;

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
    pub metadata: SandboxMetadata,
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

/// Outcome of trying to take ownership of a paused sandbox for a resume.
#[derive(Debug)]
pub enum ResumeClaim {
    /// The caller owns the sandbox and must either resume it or release the claim.
    Claimed(Box<PausedSandboxEntry>),
    /// No registry row: the sandbox is unknown to the cluster.
    NotFound,
    /// The snapshot is still uploading, so only the origin node can serve this
    /// resume. Carries the origin node so the caller can redirect.
    NotReady { origin_node_id: String },
    /// Another node claimed it first.
    Conflict { origin_node_id: String },
}
