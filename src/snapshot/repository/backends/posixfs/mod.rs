//! The POSIX snapshot backend's durable halves.

pub mod artifacts;
pub mod durable;
pub mod layout;

/// The durable byte half without local materialization.
pub use durable::posixfs_artifacts_only_repository;
pub use layout::PosixFsSnapshotArtifactLayout;
