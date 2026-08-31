//! The OSS snapshot backend's durable halves: the delete-only artifact store
//! and the client it uses.

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
