//! POSIX artifact deletion for processes that materialize no local bytes.
//!
//! Imports stay on nodes; PostgreSQL is composed over this byte-only repository.

use std::path::Path;
use std::sync::Arc;

use super::artifacts::PosixFsArtifactStore;
use crate::snapshot::repository::no_catalog::NoSnapshotCatalog;
use crate::snapshot::repository::SnapshotRepository;

pub fn posixfs_artifacts_only_repository(root: &Path) -> SnapshotRepository {
    SnapshotRepository::new(
        Arc::new(NoSnapshotCatalog),
        Arc::new(PosixFsArtifactStore::new(root.to_path_buf())),
    )
}
