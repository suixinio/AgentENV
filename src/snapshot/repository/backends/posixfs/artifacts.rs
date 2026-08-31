//! Delete-only POSIX artifact access for processes without local OverlayBD layers.
//!
//! Importing remains on the node that captured the snapshot.

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

    pub fn committed_layout(&self, snapshot_id: &SnapshotId) -> PosixFsSnapshotArtifactLayout {
        PosixFsSnapshotArtifactLayout::new(&self.root, snapshot_id)
    }

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

pub async fn run_artifact_blocking<T, F>(operation: &'static str, work: F) -> RepositoryResult<T>
where
    T: Send + 'static,
    F: FnOnce() -> RepositoryResult<T> + Send + 'static,
{
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|error| RepositoryError::backend(format!("join {operation} task"), error))?
}
