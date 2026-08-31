//! Delete-only OSS artifact access for processes without local OverlayBD layers.
//!
//! Importing remains on the node that captured the snapshot.

use std::sync::Arc;

use async_trait::async_trait;
use tracing::warn;

use super::client::OssClient;
use super::layout::OssSnapshotArtifactLayout;
use crate::snapshot::repository::backends::common::acr::AcrPublicationRollback;
use crate::snapshot::repository::interfaces::{ImportedSnapshotArtifacts, SnapshotArtifactStore};
use crate::snapshot::repository::{RepositoryError, RepositoryResult};
use crate::snapshot::{PersistedDiskImagePublication, SnapshotId, SnapshotPublishMetadata};
use crate::types::FirecrackerSnapshotManifest;

/// Snapshot bytes stored in OSS: what removing them takes.
pub struct OssSnapshotArtifactStore {
    client: Arc<OssClient>,
    rollback: AcrPublicationRollback,
}

impl OssSnapshotArtifactStore {
    pub fn new(client: Arc<OssClient>) -> Self {
        Self {
            client,
            rollback: AcrPublicationRollback::new(),
        }
    }

    fn layout<'a>(&self, id: &'a SnapshotId) -> OssSnapshotArtifactLayout<'a> {
        OssSnapshotArtifactLayout::new(id)
    }
}

#[async_trait]
impl SnapshotArtifactStore for OssSnapshotArtifactStore {
    async fn import_built_artifacts(
        &self,
        _metadata: &SnapshotPublishMetadata,
        _manifest: &FirecrackerSnapshotManifest,
        _publications: &mut Vec<PersistedDiskImagePublication>,
    ) -> RepositoryResult<ImportedSnapshotArtifacts> {
        Err(RepositoryError::Unsupported {
            feature: "importing snapshot artifacts on a process that runs no sandbox runtime"
                .to_string(),
        })
    }

    async fn delete_artifacts(
        &self,
        id: &SnapshotId,
        publications: &[PersistedDiskImagePublication],
    ) {
        // Content-addressed managed layers are intentionally left in place;
        // they are shared across snapshots and require separate GC.
        for publication in publications.iter().rev() {
            if let Err(error) = self.rollback.rollback_publication(publication).await {
                warn!(
                    snapshot_id = %id,
                    image_ref = %publication.image_ref,
                    manifest_digest = %publication.manifest_digest,
                    error = %error,
                    "failed to remove ACR snapshot publication; leaving cleanup to registry GC"
                );
            }
        }
        if let Err(error) = self
            .client
            .delete_prefix(&self.layout(id).artifact_prefix())
            .await
        {
            warn!(snapshot_id = %id, error = %error, "failed to delete oss snapshot artifacts");
        }
    }
}
