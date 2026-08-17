use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::orchestrator::store::SandboxMetadata;
use crate::snapshot::SnapshotId;
use crate::types::SandboxId;

/// Lifecycle of a registry row.
///
/// The row exists only while the sandbox is not running anywhere. It is created
/// by the node that pauses the sandbox and removed by whichever node resumes it.
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
    /// The node that produced the snapshot. A scheduling hint, never a binding:
    /// resume prefers it and falls back to any node when it cannot serve.
    pub origin_node_id: String,
    /// `None` while `state == Publishing`.
    pub snapshot_id: Option<SnapshotId>,
    pub metadata: SandboxMetadata,
    pub paused_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
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
