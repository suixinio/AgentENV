//! The OSS artifact store: snapshot bytes, and nothing else.
//!
//! Object layout under the configured prefix:
//!
//! ```text
//! artifacts/{id}/vm_state.bin
//! artifacts/{id}/firecracker-manifest.json → FirecrackerSnapshotManifest (paths omitted)
//! managed-layers/{digest}                  → content-addressed, shared, never per-snapshot
//! ```
//!
//! Nothing here reads or writes a catalog row. What it hands back —
//! [`ImportedSnapshotArtifacts`] — is entirely logical references, so the row it
//! feeds can be written by a process that never saw these files.

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use overlaybd::config::{load_image_config as load_overlaybd_image_config, LayerConfig};
use overlaybd::dense_export;
use overlaybd::layer_metadata::read_overlaybd_layer_uuid;
use tracing::{debug, info, warn};

use super::client::{OssClient, OssUploadArtifact};
use super::layout::OssSnapshotArtifactLayout;
use crate::cfg::SnapshotImageStoragePolicy;
use crate::snapshot::repository::backends::common::acr::{
    AcrDiskImageExporter, DiskImageExportOutcome, DiskImageSubject, SnapshotOciConfigInput,
};
use crate::snapshot::repository::backends::common::write_dense_overlaybd_layer_to_file;
use crate::snapshot::repository::interfaces::{ImportedSnapshotArtifacts, SnapshotArtifactStore};
use crate::snapshot::repository::{RepositoryError, RepositoryResult};
use crate::snapshot::{
    CommittedAttachedDrive, ExternalLayer, ManagedLayer, OverlaybdLayerRef,
    PersistedDiskImagePublication, SnapshotId, SnapshotPublishMetadata, SNAPSHOT_ARTIFACT_LAYOUT,
};
use crate::types::FirecrackerSnapshotManifest;

/// Snapshot bytes stored in OSS, plus the source-registry export path.
pub(crate) struct OssSnapshotArtifactStore {
    client: Arc<OssClient>,
    snapshot_image_storage: SnapshotImageStoragePolicy,
    acr_exporter: AcrDiskImageExporter,
}

impl OssSnapshotArtifactStore {
    pub(crate) fn new(
        client: Arc<OssClient>,
        snapshot_image_storage: SnapshotImageStoragePolicy,
    ) -> Self {
        Self {
            client,
            snapshot_image_storage,
            acr_exporter: AcrDiskImageExporter::new(),
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
        metadata: &SnapshotPublishMetadata,
        manifest: &FirecrackerSnapshotManifest,
        publications: &mut Vec<PersistedDiskImagePublication>,
    ) -> RepositoryResult<ImportedSnapshotArtifacts> {
        let id = &metadata.id;
        let layout = self.layout(id);

        validate_publish_manifest_image_configs(manifest)?;

        // 1. Export rootfs disk image with the effective runtime config.
        let rootfs_config =
            SnapshotOciConfigInput::new(&metadata.context, metadata.image_configs.rootfs_config());
        let rootfs_outcome = self
            .export_disk_image(
                id,
                DiskImageSubject::Rootfs,
                &manifest.rootfs.image_config_path,
                Some(rootfs_config),
            )
            .await?;
        if let Some(publication) = rootfs_outcome.publication.clone() {
            publications.push(publication);
        }
        let rootfs_layers = rootfs_outcome.layers;

        let memory_layers = self
            .derive_and_upload_memory_layers(&manifest.memory.image_config_path)
            .await?;

        // 2. Upload per-snapshot fixed artifacts.
        let vm_state_local_path = manifest.vm_state.path.as_path();
        self.client
            .put_file(
                &layout.artifact_key(SNAPSHOT_ARTIFACT_LAYOUT.vm_state),
                vm_state_local_path,
                OssUploadArtifact::VmState,
            )
            .await
            .map_err(|e| {
                RepositoryError::backend(
                    format!(
                        "upload artifact '{}' from '{}' for snapshot '{}'",
                        SNAPSHOT_ARTIFACT_LAYOUT.vm_state,
                        vm_state_local_path.display(),
                        id
                    ),
                    e,
                )
            })?;

        let persisted_manifest_bytes = serde_json::to_vec_pretty(manifest)
            .map_err(|e| RepositoryError::backend("serialize firecracker manifest", e))?;
        self.client
            .put_bytes(
                &layout.artifact_key(SNAPSHOT_ARTIFACT_LAYOUT.firecracker_manifest),
                persisted_manifest_bytes,
                OssUploadArtifact::FirecrackerManifest,
            )
            .await
            .map_err(|e| RepositoryError::backend("write firecracker manifest to oss", e))?;

        // 3. Export attached-drive disk images and derive their committed metadata.
        let attached_drives = self
            .export_attached_drives(id, manifest, publications)
            .await?;

        debug!(snapshot_id = %id, "imported snapshot artifacts into oss");
        Ok(ImportedSnapshotArtifacts {
            rootfs_layers,
            memory_layers,
            attached_drives,
            disk_publications: publications.clone(),
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
            if let Err(error) = self.acr_exporter.rollback_publication(publication).await {
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

// ── private helpers ────────────────────────────────────────────────────

impl OssSnapshotArtifactStore {
    async fn export_disk_image(
        &self,
        snapshot_id: &SnapshotId,
        subject: DiskImageSubject,
        image_config_path: &Path,
        config: Option<SnapshotOciConfigInput<'_>>,
    ) -> RepositoryResult<DiskImageExportOutcome> {
        let artifact = match &subject {
            DiskImageSubject::Rootfs => OssUploadArtifact::RootfsLayer,
            DiskImageSubject::AttachedDrive { .. } => OssUploadArtifact::AttachedDriveLayer,
        };
        if !matches!(
            self.snapshot_image_storage,
            SnapshotImageStoragePolicy::SourceRegistry
        ) {
            return self
                .export_managed_disk_image(image_config_path, artifact)
                .await;
        }
        match self
            .acr_exporter
            .export(snapshot_id, subject.clone(), image_config_path, config)
            .await
        {
            Err(RepositoryError::Unsupported { feature }) => {
                if fallback_to_object_storage_would_mix_sources(
                    image_config_path,
                    &self.client.managed_layers_repo_blob_url(),
                )? {
                    return Err(RepositoryError::Unsupported {
                        feature: format!(
                            "source-registry export is unsupported for this remote-backed disk image: {feature}"
                        ),
                    });
                }
                info!(
                    snapshot_id = %snapshot_id,
                    subject = subject.log_label(),
                    reason = %feature,
                    "falling back to managed disk image layers"
                );
                self.export_managed_disk_image(image_config_path, artifact)
                    .await
            }
            result => result,
        }
    }

    async fn export_managed_disk_image(
        &self,
        image_config_path: &Path,
        artifact: OssUploadArtifact,
    ) -> RepositoryResult<DiskImageExportOutcome> {
        Ok(DiskImageExportOutcome {
            layers: self
                .derive_and_upload_disk_image_layers(image_config_path, artifact)
                .await?,
            publication: None,
        })
    }

    pub(super) async fn derive_and_upload_disk_image_layers(
        &self,
        image_config_path: &Path,
        artifact: OssUploadArtifact,
    ) -> RepositoryResult<Vec<OverlaybdLayerRef>> {
        let image_config = load_overlaybd_image_config(image_config_path).map_err(|e| {
            RepositoryError::backend(
                format!(
                    "load overlaybd image config '{}'",
                    image_config_path.display()
                ),
                e,
            )
        })?;

        let mut layers = Vec::with_capacity(image_config.lowers.len());

        for (index, layer) in image_config.lowers.into_iter().enumerate() {
            if !layer.file.is_empty() {
                let layer_path = Path::new(&layer.file);
                if !layer.digest.is_empty() && layer.size > 0 {
                    let managed = self
                        .import_managed_layer_with_descriptor(
                            layer_path,
                            &layer.digest,
                            layer.size,
                            artifact,
                        )
                        .await?;
                    layers.push(OverlaybdLayerRef::Managed(managed));
                    continue;
                }
                if crate::image::local_layer::rootfs_layer_is_runtime_generated_delta(layer_path) {
                    let managed = self
                        .import_descriptorless_rootfs_layer(layer_path, artifact)
                        .await?;
                    layers.push(OverlaybdLayerRef::Managed(managed));
                    continue;
                }
                return Err(RepositoryError::Unsupported {
                    feature: format!(
                        "local overlaybd lower layer {index} '{}' missing digest/size",
                        layer_path.display()
                    ),
                });
            }
            let repo_blob_url = layer
                .effective_repo_blob_url(&image_config.repo_blob_url)
                .to_string();
            if !repo_blob_url.is_empty() {
                let digest = if !layer.digest.is_empty() {
                    layer.digest
                } else if !layer.target_digest.is_empty() {
                    layer.target_digest
                } else {
                    format!("external:{index}")
                };
                layers.push(OverlaybdLayerRef::External(ExternalLayer {
                    digest,
                    repo_blob_url: repo_blob_url.clone(),
                    size: layer.size,
                }));
                continue;
            }
            return Err(RepositoryError::Unsupported {
                feature: format!("overlaybd lower layer {index} without local file or repoBlobUrl"),
            });
        }

        Ok(layers)
    }

    async fn derive_and_upload_memory_layers(
        &self,
        mem_image_config_path: &Path,
    ) -> RepositoryResult<Vec<ManagedLayer>> {
        let image_config = load_overlaybd_image_config(mem_image_config_path).map_err(|e| {
            RepositoryError::backend(
                format!(
                    "load mem image config '{}'",
                    mem_image_config_path.display()
                ),
                e,
            )
        })?;

        let mut layers = Vec::with_capacity(image_config.lowers.len());
        for (index, layer) in image_config.lowers.into_iter().enumerate() {
            if layer.file.is_empty() {
                let repo_blob_url = layer
                    .effective_repo_blob_url(&image_config.repo_blob_url)
                    .to_string();
                if !repo_blob_url.is_empty() {
                    layers.push(managed_memory_layer_from_remote_lower(
                        index,
                        layer,
                        &repo_blob_url,
                        &self.client.managed_layers_repo_blob_url(),
                    )?);
                    continue;
                }
                return Err(RepositoryError::Unsupported {
                    feature: format!("memory layer {index} without local file path"),
                });
            }
            let layer_path = Path::new(&layer.file);
            if !layer.digest.is_empty() && layer.size > 0 {
                layers.push(
                    self.import_managed_layer_with_descriptor(
                        layer_path,
                        &layer.digest,
                        layer.size,
                        OssUploadArtifact::MemoryLayer,
                    )
                    .await?,
                );
                continue;
            }
            layers.push(
                self.import_managed_layer_by_hash(layer_path, OssUploadArtifact::MemoryLayer)
                    .await?,
            );
        }

        Ok(layers)
    }

    /// Export attached-drive disk images and derive committed metadata.
    async fn export_attached_drives(
        &self,
        snapshot_id: &SnapshotId,
        manifest: &FirecrackerSnapshotManifest,
        publications: &mut Vec<PersistedDiskImagePublication>,
    ) -> RepositoryResult<Vec<CommittedAttachedDrive>> {
        let mut drives = Vec::new();

        for drive in &manifest.attached_drives {
            let outcome = self
                .export_disk_image(
                    snapshot_id,
                    DiskImageSubject::AttachedDrive {
                        drive_id: drive.drive_id.clone(),
                    },
                    &drive.image_config_path,
                    None,
                )
                .await?;
            if let Some(publication) = outcome.publication.clone() {
                publications.push(publication);
            }
            drives.push(CommittedAttachedDrive::Overlaybd {
                drive_id: drive.drive_id.clone(),
                layers: outcome.layers,
                read_only: drive.read_only,
                virtual_size: drive.virtual_size,
                mount_path: crate::types::normalize_mount_path_for_drive(
                    &drive.drive_id,
                    drive.mount_path.clone(),
                )
                .unwrap_or_else(|_| crate::types::ExtraDrive::default_mount_path(&drive.drive_id)),
                sub_path: drive.sub_path.clone(),
            });
        }

        Ok(drives)
    }

    async fn import_descriptorless_rootfs_layer(
        &self,
        source: &Path,
        artifact: OssUploadArtifact,
    ) -> RepositoryResult<ManagedLayer> {
        let canonical = std::fs::canonicalize(source).map_err(|e| {
            RepositoryError::backend(
                format!("canonicalize managed layer '{}'", source.display()),
                e,
            )
        })?;
        if dense_export::should_dense_export_layer(&canonical) {
            return self
                .import_sparse_overlaybd_layer_dense(&canonical, artifact)
                .await;
        }
        self.import_managed_layer_by_hash(&canonical, artifact)
            .await
    }

    async fn import_sparse_overlaybd_layer_dense(
        &self,
        canonical: &Path,
        artifact: OssUploadArtifact,
    ) -> RepositoryResult<ManagedLayer> {
        let dense_temp = tempfile::NamedTempFile::new().map_err(|e| {
            RepositoryError::backend(
                format!(
                    "create temp dense overlaybd layer for '{}'",
                    canonical.display()
                ),
                e,
            )
        })?;
        let dense_path = dense_temp.path().to_path_buf();
        let descriptor = write_dense_overlaybd_layer_to_file(canonical, &dense_path)
            .await
            .map_err(|e| {
                RepositoryError::backend(
                    format!(
                        "dense-export sparse overlaybd layer '{}'",
                        canonical.display()
                    ),
                    e,
                )
            })?;
        let oss_key = OssSnapshotArtifactLayout::managed_layer_key(&descriptor.digest);
        upload_managed_layer_if_missing(&self.client, &oss_key, &dense_path, artifact).await?;

        Ok(ManagedLayer {
            digest: descriptor.digest,
            size: descriptor.size,
            uuid: None,
        })
    }

    async fn import_managed_layer_by_hash(
        &self,
        source: &Path,
        artifact: OssUploadArtifact,
    ) -> RepositoryResult<ManagedLayer> {
        let canonical = std::fs::canonicalize(source).map_err(|e| {
            RepositoryError::backend(
                format!("canonicalize managed layer '{}'", source.display()),
                e,
            )
        })?;
        let descriptor = crate::digest::FileDigest::describe(&canonical)
            .await
            .map_err(|e| {
                RepositoryError::backend(
                    format!("describe managed layer '{}'", canonical.display()),
                    e,
                )
            })?;
        let oss_key = OssSnapshotArtifactLayout::managed_layer_key(&descriptor.sha256);
        upload_managed_layer_if_missing(&self.client, &oss_key, &canonical, artifact).await?;

        Ok(ManagedLayer {
            digest: descriptor.sha256,
            size: descriptor.size,
            uuid: overlaybd_layer_uuid(&canonical),
        })
    }

    async fn import_managed_layer_with_descriptor(
        &self,
        source: &Path,
        digest: &str,
        size: u64,
        artifact: OssUploadArtifact,
    ) -> RepositoryResult<ManagedLayer> {
        let canonical = std::fs::canonicalize(source).map_err(|e| {
            RepositoryError::backend(
                format!("canonicalize managed layer '{}'", source.display()),
                e,
            )
        })?;
        let source_size = std::fs::metadata(&canonical)
            .map_err(|e| {
                RepositoryError::backend(
                    format!("read managed layer metadata '{}'", canonical.display()),
                    e,
                )
            })?
            .len();
        if source_size != size {
            return Err(RepositoryError::Backend {
                message: format!(
                    "managed layer descriptor size mismatch for '{}': descriptor says {}, file has {}",
                    canonical.display(),
                    size,
                    source_size
                ),
                source: None,
            });
        }

        // Descriptor-backed imports intentionally trust internally generated
        // content digests and only validate the cheap size invariant here.
        let oss_key = OssSnapshotArtifactLayout::managed_layer_key(digest);
        upload_managed_layer_if_missing(&self.client, &oss_key, &canonical, artifact).await?;

        Ok(ManagedLayer {
            digest: digest.to_string(),
            size,
            uuid: overlaybd_layer_uuid(&canonical),
        })
    }
}

fn same_repo_blob_url(left: &str, right: &str) -> bool {
    !left.is_empty() && left.trim_end_matches('/') == right.trim_end_matches('/')
}

fn managed_memory_layer_from_remote_lower(
    index: usize,
    layer: LayerConfig,
    repo_blob_url: &str,
    managed_layers_repo_blob_url: &str,
) -> RepositoryResult<ManagedLayer> {
    if !same_repo_blob_url(repo_blob_url, managed_layers_repo_blob_url) {
        return Err(RepositoryError::Unsupported {
            feature: format!("memory layer {index} uses non-OSS managed repoBlobUrl"),
        });
    }
    let digest = if !layer.digest.is_empty() {
        layer.digest
    } else if !layer.target_digest.is_empty() {
        layer.target_digest
    } else {
        return Err(RepositoryError::Unsupported {
            feature: format!("memory layer {index} without digest"),
        });
    };
    Ok(ManagedLayer {
        digest,
        size: layer.size,
        uuid: None,
    })
}

fn overlaybd_layer_uuid(source: &Path) -> Option<String> {
    read_overlaybd_layer_uuid(source)
        .ok()
        .filter(|uuid| !uuid.is_nil())
        .map(|uuid| uuid.to_string())
}

pub(super) fn fallback_to_object_storage_would_mix_sources(
    image_config_path: &Path,
    managed_layers_repo_blob_url: &str,
) -> RepositoryResult<bool> {
    let image_config = load_overlaybd_image_config(image_config_path).map_err(|e| {
        RepositoryError::backend(
            format!(
                "load overlaybd image config '{}'",
                image_config_path.display()
            ),
            e,
        )
    })?;
    Ok(image_config.lowers.iter().any(|layer| {
        layer.file.is_empty()
            && !same_repo_blob_url(
                layer.effective_repo_blob_url(&image_config.repo_blob_url),
                managed_layers_repo_blob_url,
            )
    }))
}

/// Upload a content-addressed managed layer if it is not already present.
///
/// Managed layers are keyed by `sha256:{digest}`, so concurrent writers
/// uploading the same digest always produce identical content.  This makes
/// the `exists() → put()` TOCTOU benign: the worst case is a redundant
/// upload of identical bytes, never data corruption.
///
/// We intentionally use an unconditional `put_file` instead of the
/// conditional `put_file_if_not_exists` here because Alibaba Cloud OSS
/// does not support conditional headers (`x-oss-forbid-overwrite`) on
/// multipart/streaming uploads — only on single-PUT operations.  OpenDAL's
/// `writer_with().if_not_exists(true)` triggers the multipart path for
/// large files, which causes a `NotImplemented` error on OSS.
async fn upload_managed_layer_if_missing(
    client: &OssClient,
    key: &str,
    canonical: &Path,
    artifact: OssUploadArtifact,
) -> RepositoryResult<()> {
    let already_exists = client
        .exists(key)
        .await
        .map_err(|e| RepositoryError::backend("check managed layer existence", e))?;

    if !already_exists {
        client
            .put_file(key, canonical, artifact)
            .await
            .map_err(|e| {
                RepositoryError::backend(
                    format!("upload managed layer '{}'", canonical.display()),
                    e,
                )
            })?;
    }

    Ok(())
}

pub(super) fn validate_publish_manifest_image_configs(
    manifest: &FirecrackerSnapshotManifest,
) -> RepositoryResult<()> {
    load_overlaybd_image_config(&manifest.rootfs.image_config_path).map_err(|e| {
        RepositoryError::backend(
            format!(
                "validate rootfs image config '{}'",
                manifest.rootfs.image_config_path.display()
            ),
            e,
        )
    })?;
    load_overlaybd_image_config(&manifest.memory.image_config_path).map_err(|e| {
        RepositoryError::backend(
            format!(
                "validate memory image config '{}'",
                manifest.memory.image_config_path.display()
            ),
            e,
        )
    })?;
    for drive in &manifest.attached_drives {
        load_overlaybd_image_config(&drive.image_config_path).map_err(|e| {
            RepositoryError::backend(
                format!(
                    "validate drive image config '{}' for drive '{}'",
                    drive.image_config_path.display(),
                    drive.drive_id
                ),
                e,
            )
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use serde_json::json;

    use super::*;

    fn write_test_image(path: &Path, value: serde_json::Value) {
        std::fs::write(
            path,
            serde_json::to_vec_pretty(&value).expect("serialize image config"),
        )
        .expect("write image config");
    }

    fn test_store() -> OssSnapshotArtifactStore {
        let client = OssClient::new(
            "bucket".to_string(),
            "https://oss.example.com".to_string(),
            "region".to_string(),
            "prefix".to_string(),
            object_store_operator::CredentialSource::Anonymous,
        )
        .expect("oss client");
        OssSnapshotArtifactStore::new(Arc::new(client), SnapshotImageStoragePolicy::ObjectStorage)
    }

    #[test]
    fn publish_manifest_preflight_rejects_invalid_memory_image_config() {
        let temp = tempfile::tempdir().expect("tempdir");
        let rootfs_image_config = temp.path().join("rootfs-image.json");
        let memory_image_config = temp.path().join("mem-image.json");

        write_test_image(
            &rootfs_image_config,
            json!({
                "lowers": [
                    { "file": "rootfs.commit" }
                ],
                "upper": {},
                "resultFile": "",
                "download": {}
            }),
        );
        write_test_image(
            &memory_image_config,
            json!({
                "lowers": [
                    { "digest": "sha256:parent", "size": 4096 }
                ],
                "upper": {},
                "resultFile": "",
                "download": {}
            }),
        );

        let mut manifest = FirecrackerSnapshotManifest::for_test(1024, &[]);
        manifest.rootfs.image_config_path = rootfs_image_config;
        manifest.memory.image_config_path = memory_image_config;

        let err = validate_publish_manifest_image_configs(&manifest)
            .expect_err("missing memory repoBlobUrl should fail preflight");
        assert!(err.to_string().contains("validate memory image config"));
    }

    #[test]
    fn detects_source_registry_fallback_source_mixing() {
        let temp = tempfile::tempdir().expect("tempdir");
        let source_registry_image = temp.path().join("source-image.json");
        write_test_image(
            &source_registry_image,
            json!({
                "repoBlobUrl": "https://registry.example/v2/ns/image/blobs",
                "lowers": [
                    { "digest": "sha256:base", "size": 4096 },
                    { "file": "snapshot.commit", "digest": "sha256:delta", "size": 5 }
                ],
                "upper": {},
                "resultFile": "",
                "download": {}
            }),
        );
        assert!(fallback_to_object_storage_would_mix_sources(
            &source_registry_image,
            "s3://bucket/prefix/managed-layers"
        )
        .unwrap());

        let oss_image = temp.path().join("oss-image.json");
        write_test_image(
            &oss_image,
            json!({
                "repoBlobUrl": "s3://bucket/prefix/managed-layers",
                "lowers": [
                    { "digest": "sha256:base", "size": 4096 },
                    { "file": "snapshot.commit", "digest": "sha256:delta", "size": 5 }
                ],
                "upper": {},
                "resultFile": "",
                "download": {}
            }),
        );

        assert!(!fallback_to_object_storage_would_mix_sources(
            &oss_image,
            "s3://bucket/prefix/managed-layers"
        )
        .unwrap());

        let layer_level_image = temp.path().join("layer-level-image.json");
        write_test_image(
            &layer_level_image,
            json!({
                "repoBlobUrl": "",
                "lowers": [
                    {
                        "digest": "sha256:base",
                        "size": 4096,
                        "repoBlobUrl": "https://registry.example/v2/ns/image/blobs"
                    },
                    { "file": "snapshot.commit", "digest": "sha256:delta", "size": 5 }
                ],
                "upper": {},
                "resultFile": "",
                "download": {}
            }),
        );
        assert!(fallback_to_object_storage_would_mix_sources(
            &layer_level_image,
            "s3://bucket/prefix/managed-layers"
        )
        .unwrap());
    }

    #[tokio::test]
    async fn derives_external_layers_from_layer_repo_blob_urls() {
        let temp = tempfile::tempdir().expect("tempdir");
        let image = temp.path().join("image.json");
        write_test_image(
            &image,
            json!({
                "repoBlobUrl": "",
                "lowers": [
                    {
                        "digest": "sha256:base",
                        "size": 4096,
                        "repoBlobUrl": "https://registry.example/v2/ns/image/blobs"
                    },
                    {
                        "digest": "sha256:delta",
                        "size": 8192,
                        "repoBlobUrl": "s3://bucket/prefix/managed-layers"
                    }
                ],
                "upper": {},
                "resultFile": "",
                "download": {}
            }),
        );

        let layers = test_store()
            .derive_and_upload_disk_image_layers(&image, OssUploadArtifact::RootfsLayer)
            .await
            .expect("derive layers");

        assert_eq!(
            layers,
            vec![
                OverlaybdLayerRef::External(ExternalLayer {
                    digest: "sha256:base".to_string(),
                    repo_blob_url: "https://registry.example/v2/ns/image/blobs".to_string(),
                    size: 4096,
                }),
                OverlaybdLayerRef::External(ExternalLayer {
                    digest: "sha256:delta".to_string(),
                    repo_blob_url: "s3://bucket/prefix/managed-layers".to_string(),
                    size: 8192,
                }),
            ]
        );
    }
}
