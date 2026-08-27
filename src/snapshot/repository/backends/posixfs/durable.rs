//! The POSIX repository a process that materializes no bytes builds.
//!
//! 🔴 Not [`posixfs_repository`][super::posixfs_repository], and the difference
//! is the artifact half. That one wires in
//! [`PosixFsArtifactImporter`][super::import::PosixFsArtifactImporter], which
//! reads overlaybd layers; this one wires in
//! [`PosixFsArtifactStore`][super::artifacts::PosixFsArtifactStore], which only
//! removes what a commit already wrote.

use std::path::Path;
use std::sync::Arc;

use super::artifacts::PosixFsArtifactStore;
use super::catalog::PosixFsCatalogStore;
use crate::snapshot::repository::SnapshotRepository;

pub fn posixfs_catalog_only_repository(root: &Path) -> SnapshotRepository {
    SnapshotRepository::new(
        Arc::new(PosixFsCatalogStore::new(root.to_path_buf())),
        Arc::new(PosixFsArtifactStore::new(root.to_path_buf())),
    )
}
