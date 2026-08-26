//! What a POSIX-filesystem snapshot store does *without* overlaybd.
//!
//! # 🔴 Half a store, on purpose
//!
//! Importing a snapshot's bytes means reading overlaybd layer files, dense-
//! exporting sparse ones and hard-linking managed layers into the shared store.
//! Only the machine that captured the snapshot can do any of it, and it needs
//! the overlaybd layer format to do it. Deleting them is `remove_dir_all` on a
//! directory named by an id.
//!
//! So the store is split where the dependency is. This half — rows already
//! written, bytes already durable, and a snapshot going away — is what a
//! process that never ran a microVM builds. The other half lives in
//! [`super::import`] and wraps this one.

use std::fs;
use std::path::PathBuf;

use async_trait::async_trait;
use tracing::warn;

use super::layout::PosixFsSnapshotArtifactLayout;
use crate::snapshot::repository::interfaces::{ImportedSnapshotArtifacts, SnapshotArtifactStore};
use crate::snapshot::{
    PersistedDiskImagePublication, RepositoryError, RepositoryResult, SnapshotId,
    SnapshotPublishMetadata,
};
use crate::types::FirecrackerSnapshotManifest;

/// Artifact store backed by files in a POSIX-compatible shared filesystem.
///
/// Never publishes to an external registry, so the publications it reports and
/// the ones it is asked to roll back are always empty.
#[derive(Clone)]
pub struct PosixFsArtifactStore {
    root: PathBuf,
}

impl PosixFsArtifactStore {
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    pub(crate) fn committed_layout(
        &self,
        snapshot_id: &SnapshotId,
    ) -> PosixFsSnapshotArtifactLayout {
        PosixFsSnapshotArtifactLayout::new(&self.root, snapshot_id)
    }

    /// Removes one snapshot's artifact directory. Idempotent; the commit
    /// marker inside it goes with it.
    fn remove_snapshot_dir(&self, snapshot_id: &SnapshotId) -> RepositoryResult<()> {
        let snapshot_dir = self.committed_layout(snapshot_id).snapshot_dir();
        match fs::remove_dir_all(&snapshot_dir) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(RepositoryError::backend(
                format!("remove snapshot dir '{}'", snapshot_dir.display()),
                error,
            )),
        }
    }
}

#[async_trait]
impl SnapshotArtifactStore for PosixFsArtifactStore {
    /// 🔴 Refuses, and the refusal is the point. See this module's own doc: a
    /// process holding only this half has no overlaybd layers to import and no
    /// code that could read them.
    async fn import_built_artifacts(
        &self,
        _metadata: &SnapshotPublishMetadata,
        _manifest: &FirecrackerSnapshotManifest,
        _publications: &mut Vec<PersistedDiskImagePublication>,
    ) -> RepositoryResult<ImportedSnapshotArtifacts> {
        Err(RepositoryError::Unsupported {
            feature: "importing snapshot artifacts on a process that runs no sandbox runtime"
                .to_string(),
        })
    }

    async fn delete_artifacts(
        &self,
        id: &SnapshotId,
        _publications: &[PersistedDiskImagePublication],
    ) {
        let store = self.clone();
        let owned_id = id.clone();
        let removed = run_artifact_blocking("delete snapshot artifacts", move || {
            store.remove_snapshot_dir(&owned_id)
        })
        .await;
        if let Err(error) = removed {
            warn!(snapshot_id = %id, error = %error, "failed to delete posixfs snapshot artifacts");
        }
    }
}

pub(crate) async fn run_artifact_blocking<T, F>(
    operation: &'static str,
    work: F,
) -> RepositoryResult<T>
where
    T: Send + 'static,
    F: FnOnce() -> RepositoryResult<T> + Send + 'static,
{
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|error| RepositoryError::backend(format!("join {operation} task"), error))?
}
