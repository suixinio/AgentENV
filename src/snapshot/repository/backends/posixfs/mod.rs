//! The POSIX snapshot backend's durable halves.
//!
//! 🔴 The importing half and the runtime resolver are `aenv-node`'s `posixfs`
//! module — see [`durable`]'s own doc for the seam.

pub mod artifacts;
pub mod durable;
pub mod layout;

/// The durable byte half on its own — see
/// `backends::build_artifacts_only_repository` for why a process that
/// materializes no bytes still needs it.
pub use durable::posixfs_artifacts_only_repository;
pub use layout::PosixFsSnapshotArtifactLayout;
