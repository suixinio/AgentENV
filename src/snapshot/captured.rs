//! Snapshot captures handed to the repository.
//!
//! Captures are either already-staged values or local artifacts exposed through
//! the one manifest method publication needs.

use std::fmt;

use super::repository::interfaces::StagedSnapshot;
use super::SnapshotRecord;
use crate::types::FirecrackerSnapshotManifest;

/// Local artifacts whose manifest may be staged by the repository.
pub trait LocalCapturedArtifacts: Send + 'static {
    /// Returns a publishable manifest, or `None` for backend-owned ephemeral artifacts.
    fn publishable_manifest(&self) -> Option<&FirecrackerSnapshotManifest>;
}

/// Manifest stored in a caller-owned artifact directory.
#[derive(Debug)]
pub struct CallerOwnedArtifacts(FirecrackerSnapshotManifest);

impl CallerOwnedArtifacts {
    pub fn new(manifest: FirecrackerSnapshotManifest) -> Self {
        Self(manifest)
    }
}

impl LocalCapturedArtifacts for CallerOwnedArtifacts {
    fn publishable_manifest(&self) -> Option<&FirecrackerSnapshotManifest> {
        Some(&self.0)
    }
}

/// Local capture with no publishable manifest.
#[derive(Debug, Default)]
pub struct UnpublishableCapture;

impl LocalCapturedArtifacts for UnpublishableCapture {
    fn publishable_manifest(&self) -> Option<&FirecrackerSnapshotManifest> {
        None
    }
}

/// One-shot local or already-staged snapshot capture.
pub enum CapturedSandboxSnapshot {
    /// Pure staged value produced by the node holding the bytes.
    Staged(Box<StagedSnapshot>),
    /// Artifacts readable by this process.
    Local(Box<dyn LocalCapturedArtifacts>),
}

impl CapturedSandboxSnapshot {
    /// A capture backed by artifacts on this machine.
    pub fn local<T>(artifacts: T) -> Self
    where
        T: LocalCapturedArtifacts,
    {
        Self::Local(Box::new(artifacts))
    }

    /// A capture the node holding the bytes already staged.
    pub fn staged(staged: StagedSnapshot) -> Self {
        Self::Staged(Box::new(staged))
    }

    /// A local capture with nothing publishable behind it.
    pub fn unpublishable() -> Self {
        Self::local(UnpublishableCapture)
    }
}

impl fmt::Debug for CapturedSandboxSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Staged(staged) => f
                .debug_struct("CapturedSandboxSnapshot")
                .field("staged_by", &staged.origin_node_id)
                .finish(),
            Self::Local(_) => f
                .debug_struct("CapturedSandboxSnapshot")
                .field("local", &true)
                .finish(),
        }
    }
}

/// Best-effort advertisement of newly committed local artifacts.
#[async_trait::async_trait]
pub trait SnapshotArtifactAdvertiser: Send + Sync {
    /// Best effort: a snapshot is committed and reachable whether or not this
    /// succeeds, so failures are logged rather than returned.
    async fn advertise(&self, record: &SnapshotRecord, manifest: &FirecrackerSnapshotManifest);
}
