mod artifacts;
mod backend;
mod catalog;
mod durable;
mod import;
mod layout;
mod runtime;

pub use backend::{PosixFsBackend, PosixFsBackendConfig};
pub(crate) use catalog::PosixFsCatalogStore;
/// The durable halves on their own — see `backends::build_catalog_only_repository`
/// for why a process that materializes no bytes still needs them.
pub(crate) use durable::posixfs_catalog_only_repository;
pub(crate) use layout::PosixFsSnapshotArtifactLayout;
