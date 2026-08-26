mod artifacts;
mod backend;
mod catalog;
mod layout;
mod runtime;

/// The durable halves on their own — see `backends::build_catalog_only_repository`
/// for why a process that materializes no bytes still needs them.
pub(crate) use backend::posixfs_repository;
pub use backend::{PosixFsBackend, PosixFsBackendConfig};
pub(crate) use catalog::PosixFsCatalogStore;
pub(crate) use layout::PosixFsSnapshotArtifactLayout;
