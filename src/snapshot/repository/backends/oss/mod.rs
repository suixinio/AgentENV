//! The OSS snapshot backend's durable halves: the catalog, the delete-only
//! artifact store, and the client both use.
//!
//! 🔴 The importing half and the runtime resolver are `aenv-node`'s `oss`
//! module — see [`durable`]'s own doc for the seam.

pub mod artifacts;
pub mod catalog;
pub mod client;
pub mod config;
pub mod durable;
pub mod layout;
#[doc(hidden)]
pub mod test_support;

pub use durable::{oss_durable_parts, OssDurableParts};

pub use self::artifacts::OssSnapshotArtifactStore;
pub use self::catalog::OssSnapshotCatalog;
pub use self::client::OssClient;
pub use self::config::NormalizedOssConfig;
pub use self::layout::OssSnapshotArtifactLayout;
