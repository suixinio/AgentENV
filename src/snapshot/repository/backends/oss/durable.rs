//! OSS artifact deletion for processes that materialize no local bytes.
//!
//! Imports stay on nodes; deletion remains available after the originating node is gone.

use std::sync::Arc;

use anyhow::Result;

use super::artifacts::OssSnapshotArtifactStore;
use super::client::OssClient;
use super::config::NormalizedOssConfig;
use crate::cfg::{OssBackendConfig, SnapshotImageStoragePolicy};
use crate::snapshot::repository::interfaces::SnapshotArtifactStore;

/// The durable byte store and the values required by its optional runtime resolver.
pub struct OssDurableParts {
    pub artifacts: Arc<dyn SnapshotArtifactStore>,
    pub client: Arc<OssClient>,
    pub managed_layers_repo_blob_url: String,
}

impl OssDurableParts {
    /// The byte store on its own, dropping what only the resolver would use.
    pub fn into_artifacts(self) -> Arc<dyn SnapshotArtifactStore> {
        self.artifacts
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

    let artifacts: Arc<dyn SnapshotArtifactStore> =
        Arc::new(OssSnapshotArtifactStore::new(Arc::clone(&client)));

    Ok(OssDurableParts {
        artifacts,
        client,
        managed_layers_repo_blob_url,
    })
}
