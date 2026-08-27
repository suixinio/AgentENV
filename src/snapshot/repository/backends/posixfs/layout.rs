use std::path::{Path, PathBuf};

use crate::snapshot::{SnapshotAlias, SnapshotId};

pub const POSIXFS_SNAPSHOT_COMMIT_MARKER: &str = "commit";
const LOCK_SUFFIX: &str = ".lock";

pub fn managed_layer_file_name(digest: &str) -> String {
    format!("{}.overlaybd.commit", digest.replace([':', '/'], "_"))
}

/// Committed artifact layout for the POSIX-backed snapshot repository.
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

    pub fn catalog_dir(root: &Path) -> PathBuf {
        root.join("catalog")
    }

    pub fn aliases_dir(root: &Path) -> PathBuf {
        Self::catalog_dir(root).join("aliases")
    }

    pub fn records_dir(root: &Path) -> PathBuf {
        Self::catalog_dir(root).join("records")
    }

    pub fn alias_path(root: &Path, alias: &SnapshotAlias) -> PathBuf {
        Self::aliases_dir(root).join(alias.to_string())
    }

    pub fn alias_lock_path(root: &Path, alias: &SnapshotAlias) -> PathBuf {
        Self::aliases_dir(root).join(format!("{alias}{LOCK_SUFFIX}"))
    }

    pub fn record_path(root: &Path, id: &SnapshotId) -> PathBuf {
        Self::records_dir(root).join(format!("{id}.json"))
    }

    pub fn record_lock_path(root: &Path, id: &SnapshotId) -> PathBuf {
        Self::records_dir(root).join(format!("{id}{LOCK_SUFFIX}"))
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
