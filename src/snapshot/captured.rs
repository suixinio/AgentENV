//! What a capture hands the snapshot repository.
//!
//! # 🔴 Two shapes, named rather than type-erased
//!
//! [`CapturedSandboxSnapshot`] used to be a `Box<dyn Any + Send>`, and
//! [`SnapshotManager::stage_captured`][crate::snapshot::SnapshotManager::stage_captured]
//! recovered the shape by downcasting: first to [`StagedSnapshot`], then to the
//! Firecracker backend's own capture type. That second downcast is what made
//! the snapshot layer — which decides *where bytes go* — name a type belonging
//! to the sandbox runtime, which is the one thing a process that runs no
//! microVMs must never have to link.
//!
//! The two shapes were never open-ended. One is a pure value that arrived over
//! the wire from the node that already wrote the bytes; the other is artifacts
//! sitting in a directory on *this* machine, and the only thing the repository
//! ever asked it for was the manifest naming them. So both are stated here:
//! the wire half by its own strong type, the local half behind a trait whose
//! single method is the question that was actually being asked. The concrete
//! capture — and the guard keeping its temporary directory alive — stays with
//! the backend that produced it.

use std::fmt;

use super::repository::interfaces::StagedSnapshot;
use crate::types::FirecrackerSnapshotManifest;

/// Artifacts a capture wrote on the machine this process is running on.
///
/// Implemented by sandbox backends. The value is held — never inspected beyond
/// this one method — for exactly as long as the staging still needs the files,
/// which is how a backend-managed temporary directory survives publication.
pub trait LocalCapturedArtifacts: Send + 'static {
    /// The manifest naming the artifacts a repository would stage.
    ///
    /// 🔴 `None` is a real answer and not a failure: a backend whose capture
    /// lives in storage it reclaims as soon as the paused state drops has
    /// nothing a repository could commit. The caller turns that into
    /// [`RepositoryError::Unsupported`][crate::snapshot::RepositoryError],
    /// which is the same refusal the downcast used to produce — stated by the
    /// backend instead of inferred from its type.
    fn publishable_manifest(&self) -> Option<&FirecrackerSnapshotManifest>;
}

/// Artifacts in a directory whose lifetime somebody else owns.
///
/// 🔴 The manifest and nothing else. A backend whose capture needs a guard to
/// keep a temporary directory alive implements
/// [`LocalCapturedArtifacts`] on its own type instead; this is for the case
/// where the directory outlives the capture on its own, which is what makes it
/// safe for a process that never ran the VM to construct one.
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

/// A local capture that names nothing a repository can stage.
///
/// For callers that need a capture value where the publication path is not
/// under test, and for backends whose artifacts are reclaimed with the paused
/// state.
#[derive(Debug, Default)]
pub struct UnpublishableCapture;

impl LocalCapturedArtifacts for UnpublishableCapture {
    fn publishable_manifest(&self) -> Option<&FirecrackerSnapshotManifest> {
        None
    }
}

/// Captured snapshot artifacts produced from a running sandbox.
///
/// Unlike [`PausedSandboxState`][crate::sandbox::PausedSandboxState], this
/// value is intended for one-shot consumption by snapshot publication code.
pub enum CapturedSandboxSnapshot {
    /// Already staged by the node that holds the bytes.
    ///
    /// A pure value: it decoded out of a gRPC response and points at nothing on
    /// this machine.
    Staged(Box<StagedSnapshot>),
    /// Artifacts in a directory this process can read.
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
