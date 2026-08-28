use std::path::{Path, PathBuf};

use crate::snapshot::SnapshotId;

pub fn managed_layer_file_name(digest: &str) -> String {
    format!("{}.overlaybd.commit", digest.replace([':', '/'], "_"))
}

/// Committed artifact layout for the POSIX-backed snapshot repository.
///
/// 🔴 Artifacts only. This carried the `catalog/records/` and
/// `catalog/aliases/` paths too, plus their lock files and the per-snapshot
/// `commit` marker, until the object-storage catalog was removed; the rows are
/// PostgreSQL's now and this backend stores bytes alone. Existing repositories
/// still have a `catalog/` directory on disk — frozen at the cutover, and read
/// by nothing.
pub struct PosixFsSnapshotArtifactLayout {
    root: PathBuf,
    snapshot_id: SnapshotId,
}

impl PosixFsSnapshotArtifactLayout {
    pub fn new(root: impl Into<PathBuf>, snapshot_id: &SnapshotId) -> Self {
        Self {
            root: root.into(),
            snapshot_id: snapshot_id.clone(),
        }
    }

    pub fn snapshots_dir(root: &Path) -> PathBuf {
        root.join("snapshots")
    }

    pub fn managed_layers_dir(root: &Path) -> PathBuf {
        root.join("managed-layers")
    }

    pub fn managed_layer_path(root: &Path, digest: &str) -> PathBuf {
        Self::managed_layers_dir(root).join(managed_layer_file_name(digest))
    }

    pub fn snapshot_dir(&self) -> PathBuf {
        Self::snapshots_dir(&self.root).join(self.snapshot_id.to_string())
    }

    pub fn path(&self, relative_path: &str) -> PathBuf {
        self.snapshot_dir().join(relative_path)
    }
}
