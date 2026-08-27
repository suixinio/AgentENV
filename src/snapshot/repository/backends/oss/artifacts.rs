//! What an OSS snapshot store does *without* overlaybd.
//!
//! # 🔴 Half a store, on purpose
//!
//! Importing a snapshot's bytes means reading overlaybd layer files off local
//! disk, dense-exporting the sparse ones and uploading them. Only the machine
//! that captured the snapshot has those files. Deleting them is a prefix delete
//! against object storage plus a `DELETE` per external registry publication,
//! both of which need nothing but what the catalog row already says.
//!
//! So the store is split where the dependency is. This half is what a process
//! that never ran a microVM builds; the other half lives in [`super::import`]
//! and wraps this one.

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
    /// 🔴 Refuses, and the refusal is the point. See this module's own doc.
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
