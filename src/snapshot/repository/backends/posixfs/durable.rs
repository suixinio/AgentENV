//! The POSIX repository a process that materializes no bytes builds.
//!
//! 🔴 Not [`posixfs_repository`][super::posixfs_repository], and the difference
//! is the artifact half. That one wires in
//! [`PosixFsArtifactImporter`][super::import::PosixFsArtifactImporter], which
//! reads overlaybd layers; this one wires in
//! [`PosixFsArtifactStore`][super::artifacts::PosixFsArtifactStore], which only
//! removes what a commit already wrote.
//!
//! 🔴 Neither carries a catalog. The rows are PostgreSQL's, and
//! `build_snapshot_backend` is what puts that catalog in front of this byte
//! half — see [`NoSnapshotCatalog`][crate::snapshot::repository::no_catalog::NoSnapshotCatalog].

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
