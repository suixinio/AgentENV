//! The OSS snapshot backend's durable halves: the delete-only artifact store
//! and the client it uses.
//!
//! 🔴 No catalog. The rows are PostgreSQL's — object storage held a catalog
//! until the Stage B cutover and holds byte artifacts alone now.
//!
//! 🔴 The importing half and the runtime resolver are `aenv-node`'s `oss`
//! module — see [`durable`]'s own doc for the seam.

pub mod artifacts;
pub mod client;
pub mod config;
pub mod durable;
pub mod layout;

pub use durable::{oss_durable_parts, OssDurableParts};

pub use self::artifacts::OssSnapshotArtifactStore;
pub use self::client::OssClient;
pub use self::config::NormalizedOssConfig;
pub use self::layout::OssSnapshotArtifactLayout;
