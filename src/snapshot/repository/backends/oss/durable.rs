//! OSS artifact deletion for processes that materialize no local bytes.
//!
//! Imports stay on nodes; deletion remains available after the originating node is gone.

use std::sync::Arc;

use anyhow::Result;

use super::artifacts::OssSnapshotArtifactStore;
use super::client::OssClient;
use super::config::NormalizedOssConfig;
use crate::cfg::{OssBackendConfig, SnapshotImageStoragePolicy};
use crate::snapshot::repository::no_catalog::NoSnapshotCatalog;
use crate::snapshot::repository::SnapshotRepository;

/// The durable repository and values required by its optional runtime resolver.
pub struct OssDurableParts {
    pub repository: Arc<SnapshotRepository>,
    pub client: Arc<OssClient>,
    pub managed_layers_repo_blob_url: String,
}

impl OssDurableParts {
    /// The repository on its own, dropping what only the resolver would use.
    pub fn into_repository(self) -> Arc<SnapshotRepository> {
        self.repository
    }
}

/// Builds the delete-only artifact repository before a catalog is composed over it.
pub fn oss_durable_parts(
    config: &OssBackendConfig,
    snapshot_image_storage: SnapshotImageStoragePolicy,
) -> Result<OssDurableParts> {
    let config = NormalizedOssConfig::new(config, snapshot_image_storage)?;
    let managed_layers_repo_blob_url = config.managed_layers_repo_blob_url();
    let client = Arc::new(OssClient::new(
        config.bucket().to_string(),
        config.endpoint().to_string(),
        config.region().to_string(),
        config.prefix().to_string(),
        config.credential_source(),
    )?);

    let repository = Arc::new(SnapshotRepository::new(
        Arc::new(NoSnapshotCatalog),
        Arc::new(OssSnapshotArtifactStore::new(Arc::clone(&client))),
    ));

    Ok(OssDurableParts {
        repository,
        client,
        managed_layers_repo_blob_url,
    })
}
