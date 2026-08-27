//! The POSIX snapshot backend's durable halves.
//!
//! 🔴 The importing half and the runtime resolver are `aenv-node`'s `posixfs`
//! module — see [`durable`]'s own doc for the seam.

pub mod artifacts;
pub mod catalog;
pub mod durable;
pub mod layout;

pub use catalog::PosixFsCatalogStore;
/// The durable halves on their own — see `backends::build_catalog_only_repository`
/// for why a process that materializes no bytes still needs them.
pub use durable::posixfs_catalog_only_repository;
pub use layout::PosixFsSnapshotArtifactLayout;
