//! POSIX artifact deletion for processes that materialize no local bytes.
//!
//! Imports stay on nodes; PostgreSQL is composed over this byte-only store.

use std::path::Path;
use std::sync::Arc;

use super::artifacts::PosixFsArtifactStore;
use crate::snapshot::repository::interfaces::SnapshotArtifactStore;

pub fn posixfs_artifacts_only_store(root: &Path) -> Arc<dyn SnapshotArtifactStore> {
    Arc::new(PosixFsArtifactStore::new(root.to_path_buf()))
}
