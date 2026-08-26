//! The OSS repository a process that materializes no bytes builds.
//!
//! 🔴 Not [`OssBackend`][super::backend::OssBackend], and the difference is the
//! artifact half. That one wires in
//! [`OssSnapshotArtifactImporter`][super::import::OssSnapshotArtifactImporter],
//! which reads overlaybd layers off local disk; this one wires in
//! [`OssSnapshotArtifactStore`][super::artifacts::OssSnapshotArtifactStore],
//! whose deletion is a pure network operation — an object-store `DELETE` under
//! the snapshot's prefix, plus `rollback_publication` against the source
//! registry.
//!
//! The delete has to stay on this side rather than being shipped to the node
//! that owns the bytes: that node may be gone (hard death, or rolled), and a
//! delete with nowhere to go leaves the row removed, the bytes orphaned, and
//! nobody holding a record of either. The exact counterpart of
//! `posixfs::durable`.

use std::sync::Arc;

use anyhow::Result;

use super::artifacts::OssSnapshotArtifactStore;
use super::catalog::OssSnapshotCatalog;
use super::client::OssClient;
use super::config::NormalizedOssConfig;
use crate::cfg::{OssBackendConfig, SnapshotImageStoragePolicy};
use crate::snapshot::repository::SnapshotRepository;

/// What [`oss_durable_parts`] hands back: the durable repository, plus the two
/// values the runtime resolver needs on top of it. Only
/// [`OssBackend::from_parts`][super::backend::OssBackend::from_parts] consumes
/// the latter two — every other caller wants [`Self::into_repository`].
pub struct OssDurableParts {
    pub(super) repository: Arc<SnapshotRepository>,
    pub(super) client: Arc<OssClient>,
    pub(super) managed_layers_repo_blob_url: String,
}

impl OssDurableParts {
    /// The repository on its own, dropping what only the resolver would use.
    pub fn into_repository(self) -> Arc<SnapshotRepository> {
        self.repository
    }
}

/// The durable halves on their own: the catalog and the delete-only artifact
/// store, already composed into a [`SnapshotRepository`].
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
        Arc::new(OssSnapshotCatalog::new(Arc::clone(&client))),
        Arc::new(OssSnapshotArtifactStore::new(Arc::clone(&client))),
    ));

    Ok(OssDurableParts {
        repository,
        client,
        managed_layers_repo_blob_url,
    })
}
