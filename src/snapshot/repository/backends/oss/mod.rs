mod artifacts;
mod backend;
mod catalog;
mod client;
mod config;
mod durable;
mod import;
mod layout;
mod resolver;
#[cfg(test)]
mod test_support;

pub use backend::OssBackend;
pub(crate) use durable::oss_durable_parts;

pub(crate) use self::catalog::OssSnapshotCatalog;
pub(crate) use self::client::OssClient;
pub(crate) use self::config::NormalizedOssConfig;
pub(crate) use self::layout::OssSnapshotArtifactLayout;
