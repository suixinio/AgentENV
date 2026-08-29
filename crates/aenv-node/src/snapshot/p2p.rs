use std::collections::HashSet;

use futures::{stream, StreamExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use overlaybd::config::load_image_config as load_overlaybd_image_config;
use overlaybd::layer_metadata::read_overlaybd_layer_uuid;
use tracing::{debug, warn};
use uuid::Uuid;

use bytes::Bytes;

use crate::overlaybd::{layer_key_from_digest, layer_key_from_uuid, LayerMetadata};
use crate::p2p::{
    P2pArtifactKey, P2pPublishMode, P2pPublishRequest, P2pPublishSource, P2pTransport,
};
use crate::snapshot::{
    CommittedAttachedDrive, ManagedLayer, OverlaybdLayerRef, SnapshotArtifactAdvertiser,
    SnapshotId, SnapshotRecord, SNAPSHOT_ARTIFACT_LAYOUT,
};
use crate::types::FirecrackerSnapshotManifest;

const SNAPSHOT_P2P_KEY_PREFIX: &str = "snapshot/v1";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotP2pArtifact {
    pub key: P2pArtifactKey,
    pub source: P2pPublishSource,
    publish_mode: P2pPublishMode,
    metadata: serde_json::Value,
}

impl SnapshotP2pArtifact {
    pub fn fixed(
        snapshot_id: &SnapshotId,
        name: impl AsRef<str>,
        source: impl Into<PathBuf>,
    ) -> Self {
        let source = source.into();
        Self {
            key: fixed_artifact_key(snapshot_id, name),
            source: P2pPublishSource::Path(source),
            publish_mode: P2pPublishMode::Copy,
            metadata: serde_json::Value::Null,
        }
    }

    pub fn bytes(
        snapshot_id: &SnapshotId,
        name: impl AsRef<str>,
        source: impl Into<Bytes>,
    ) -> Self {
        Self {
            key: fixed_artifact_key(snapshot_id, name),
            source: P2pPublishSource::Bytes(source.into()),
            publish_mode: P2pPublishMode::Copy,
            metadata: serde_json::Value::Null,
        }
    }

    pub fn content_addressed_overlaybd_layer(
        source: impl Into<PathBuf>,
        sha256: impl Into<String>,
        size: u64,
    ) -> Self {
        let sha256 = sha256.into();
        let key = layer_key_from_digest(&sha256);
        let metadata = LayerMetadata::from_digest(sha256, Some(size), None).to_value();
        Self {
            key,
            source: P2pPublishSource::Path(source.into()),
            publish_mode: P2pPublishMode::Copy,
            metadata,
        }
    }

    pub fn uuid_overlaybd_layer(source: impl Into<PathBuf>, uuid: Uuid, size: u64) -> Self {
        let key = layer_key_from_uuid(&uuid);
        let metadata = LayerMetadata::from_uuid(uuid, Some(size)).to_value();
        Self {
            key,
            source: P2pPublishSource::Path(source.into()),
            publish_mode: P2pPublishMode::Copy,
            metadata,
        }
    }

    pub fn local_overlaybd_layers(
        image_config_path: &Path,
        committed_uuids: &HashSet<String>,
    ) -> Vec<Self> {
        let image_config = match load_overlaybd_image_config(image_config_path) {
            Ok(image_config) => image_config,
            Err(error) => {
                warn!(
                    path = %image_config_path.display(),
                    error = %error,
                    "skipping snapshot P2P layer publication because image config could not be loaded"
                );
                return Vec::new();
            }
        };

        image_config
            .lowers
            .into_iter()
            .flat_map(|layer| {
                if layer.file.is_empty() {
                    return Vec::new();
                }

                let mut artifacts = Vec::new();
                if !layer.digest.is_empty() && layer.size > 0 {
                    artifacts.push(Self::content_addressed_overlaybd_layer(
                        layer.file.clone(),
                        layer.digest,
                        layer.size,
                    ));
                }
                if committed_uuids.is_empty() {
                    return artifacts;
                }
                let path = PathBuf::from(&layer.file);
                let uuid = match read_overlaybd_layer_uuid(&path) {
                    Ok(uuid) if !uuid.is_nil() => uuid,
                    Ok(_) => {
                        warn!(
                            path = %path.display(),
                            "skipping snapshot P2P layer publication because overlaybd layer uuid is nil"
                        );
                        return artifacts;
                    }
                    Err(error) => {
                        warn!(
                            path = %path.display(),
                            error = %error,
                            "skipping snapshot P2P layer publication because overlaybd layer uuid could not be read"
                        );
                        return artifacts;
                    }
                };
                if !committed_uuids.contains(&uuid.to_string()) {
                    return artifacts;
                }
                let size = match std::fs::metadata(&path) {
                    Ok(metadata) if metadata.is_file() => metadata.len(),
                    Ok(_) => {
                        warn!(
                            path = %path.display(),
                            "skipping snapshot P2P layer publication because path is not a regular file"
                        );
                        return artifacts;
                    }
                    Err(error) => {
                        warn!(
                            path = %path.display(),
                            error = %error,
                            "skipping snapshot P2P layer publication because layer size could not be read"
                        );
                        return artifacts;
                    }
                };
                artifacts.push(Self::uuid_overlaybd_layer(path, uuid, size));
                artifacts
            })
            .collect()
    }

    pub async fn publish(&self, transport: &Arc<dyn P2pTransport>) -> Result<()> {
        let request = match &self.source {
            P2pPublishSource::Path(source) => {
                P2pPublishRequest::file(self.key.clone(), source.clone())
                    .with_publish_mode(self.publish_mode)
            }
            P2pPublishSource::Bytes(bytes) => {
                P2pPublishRequest::bytes(self.key.clone(), bytes.clone())
            }
        }
        .with_metadata(self.metadata.clone());

        transport
            .publish(&request)
            .await
            .with_context(|| format!("publish snapshot artifact '{}' to P2P", self.key))
    }
}

pub fn fixed_artifact_key(snapshot_id: &SnapshotId, name: impl AsRef<str>) -> P2pArtifactKey {
    format!(
        "{SNAPSHOT_P2P_KEY_PREFIX}/artifacts/{snapshot_id}/{}",
        name.as_ref()
    )
}

pub async fn fetch_artifact(
    transport: &Arc<dyn P2pTransport>,
    key: &P2pArtifactKey,
    destination: &Path,
) -> Result<u64> {
    let Some(descriptor) = transport.lookup(key).await? else {
        anyhow::bail!("snapshot P2P artifact '{key}' was not found");
    };
    let size = transport
        .fetch(&descriptor, destination)
        .await
        .with_context(|| format!("fetch snapshot P2P artifact '{key}'"))?;
    debug!(key, destination = %destination.display(), size, "fetched snapshot artifact from P2P");
    Ok(size)
}

pub async fn fetch_artifact_bytes(
    transport: &Arc<dyn P2pTransport>,
    key: &P2pArtifactKey,
) -> Result<Bytes> {
    let Some(descriptor) = transport.lookup(key).await? else {
        anyhow::bail!("snapshot P2P artifact '{key}' was not found");
    };
    let bytes = transport
        .fetch_bytes(&descriptor)
        .await
        .with_context(|| format!("fetch snapshot P2P artifact '{key}'"))?;
    debug!(
        key,
        size = bytes.len(),
        "fetched snapshot artifact from P2P"
    );
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use overlaybd::backend::local::LocalFile;
    use overlaybd::config::{ImageConfig, LayerConfig};
    use overlaybd::index_file::{create_file_rw, LayerInfo};
    use overlaybd::virtual_file::VirtualFile;
    use std::sync::Arc;
    use storage_util::io_ring::spawn_io_ring_worker;

    async fn write_sealed_layer(path: &Path, uuid: Uuid) {
        let index_path = path.with_extension("index");
        let (io_ring, _join_handle) = spawn_io_ring_worker::<io_uring::squeue::Entry>(0);
        let data_file: Arc<dyn VirtualFile> = Arc::new(
            LocalFile::new(path, io_ring.clone())
                .await
                .expect("data file"),
        );
        let index_file: Arc<dyn VirtualFile> = Arc::new(
            LocalFile::new(index_path, io_ring)
                .await
                .expect("index file"),
        );
        let mut info = LayerInfo::new(data_file, Some(index_file), 8192);
        info.uuid = uuid;
        let file = create_file_rw(info).await.expect("create rw layer");
        file.write_at(0, &[0x5a; 4096]).await.expect("write layer");
        file.close_seal().await.expect("seal layer");
    }

    #[tokio::test]
    async fn local_overlaybd_layers_publish_only_digest_layers_without_committed_uuid() {
        let temp = tempfile::TempDir::new().expect("tempdir");
        let descriptorless = temp.path().join("snapshot.commit");
        let described = temp.path().join("described.commit");
        let uuid = Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap();
        write_sealed_layer(&descriptorless, uuid).await;
        std::fs::write(&described, b"described").expect("write described layer");

        let image_config = ImageConfig {
            lowers: vec![
                LayerConfig {
                    file: descriptorless.display().to_string(),
                    ..Default::default()
                },
                LayerConfig {
                    file: described.display().to_string(),
                    digest:
                        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                            .to_string(),
                    size: 9,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let image_config_path = temp.path().join("image.json");
        std::fs::write(
            &image_config_path,
            serde_json::to_vec(&image_config).expect("serialize image config"),
        )
        .expect("write image config");

        let artifacts =
            SnapshotP2pArtifact::local_overlaybd_layers(&image_config_path, &HashSet::new());

        assert_eq!(artifacts.len(), 1);
        assert_eq!(artifacts[0].publish_mode, P2pPublishMode::Copy);
        assert_eq!(
            artifacts[0].key,
            "overlaybd-layer/v1/sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
    }

    #[tokio::test]
    async fn local_overlaybd_layers_publishes_uuid_alongside_digest_layers() {
        let temp = tempfile::TempDir::new().expect("tempdir");
        let committed_layer_path = temp.path().join("snapshot.commit");
        let skipped_layer_path = temp.path().join("skipped.commit");
        let committed_uuid = Uuid::parse_str("22222222-3333-4444-5555-666666666666").unwrap();
        let skipped_uuid = Uuid::parse_str("33333333-4444-5555-6666-777777777777").unwrap();
        write_sealed_layer(&committed_layer_path, committed_uuid).await;
        write_sealed_layer(&skipped_layer_path, skipped_uuid).await;
        let committed_descriptor = crate::digest::FileDigest::describe(&committed_layer_path)
            .await
            .expect("describe committed layer");
        let image_config = ImageConfig {
            lowers: vec![
                LayerConfig {
                    file: committed_layer_path.display().to_string(),
                    digest: committed_descriptor.sha256.clone(),
                    size: committed_descriptor.size,
                    ..Default::default()
                },
                LayerConfig {
                    file: skipped_layer_path.display().to_string(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let image_config_path = temp.path().join("image.json");
        std::fs::write(
            &image_config_path,
            serde_json::to_vec(&image_config).expect("serialize image config"),
        )
        .expect("write image config");
        let committed_uuids = HashSet::from([committed_uuid.to_string()]);

        let artifacts =
            SnapshotP2pArtifact::local_overlaybd_layers(&image_config_path, &committed_uuids);

        assert_eq!(artifacts.len(), 2);
        assert!(artifacts
            .iter()
            .any(|artifact| artifact.key == layer_key_from_digest(&committed_descriptor.sha256)));
        assert!(artifacts.iter().any(|artifact| artifact.key
            == "overlaybd-layer/v1/uuid/22222222-3333-4444-5555-666666666666"));
    }
}

/// Concurrency limit for publishing snapshot artifacts to P2P after commit.
const SNAPSHOT_P2P_PUBLISH_CONCURRENCY: usize = 8;

fn managed_layer_uuids(layers: &[OverlaybdLayerRef]) -> HashSet<String> {
    layers
        .iter()
        .filter_map(|layer| match layer {
            OverlaybdLayerRef::Managed(managed) => managed.uuid.clone(),
            OverlaybdLayerRef::External(_) => None,
        })
        .collect()
}

fn managed_layer_uuids_from_managed(layers: &[ManagedLayer]) -> HashSet<String> {
    layers
        .iter()
        .filter_map(|layer| layer.uuid.clone())
        .collect()
}

/// Offers a freshly committed snapshot's local artifacts over P2P.
///
/// 🔴 Only ever constructed by the half that holds the bytes. See
/// [`SnapshotArtifactAdvertiser`]'s own doc.
pub struct P2pSnapshotAdvertiser {
    transport: Arc<dyn P2pTransport>,
}

impl P2pSnapshotAdvertiser {
    pub fn new(transport: Arc<dyn P2pTransport>) -> Self {
        Self { transport }
    }
}

#[async_trait::async_trait]
impl SnapshotArtifactAdvertiser for P2pSnapshotAdvertiser {
    #[tracing::instrument(skip(self, record, manifest), fields(snapshot_id = %record.id))]
    async fn advertise(&self, record: &SnapshotRecord, manifest: &FirecrackerSnapshotManifest) {
        let transport = &self.transport;
        let snapshot_id = &record.id;
        let Some(committed) = record.committed.as_ref() else {
            return;
        };

        // Prepare the manifest and VM state.
        let manifest_bytes = serde_json::to_vec(manifest).expect("manifest should serialize");
        let mut artifacts = vec![
            SnapshotP2pArtifact::fixed(
                snapshot_id,
                SNAPSHOT_ARTIFACT_LAYOUT.vm_state,
                manifest.vm_state.path.clone(),
            ),
            SnapshotP2pArtifact::bytes(
                snapshot_id,
                SNAPSHOT_ARTIFACT_LAYOUT.firecracker_manifest,
                manifest_bytes,
            ),
        ];

        // Collect any overlaybd layers referenced by this snapshot's runtime images.
        let rootfs_uuids = managed_layer_uuids(&committed.rootfs_layers);
        artifacts.extend(SnapshotP2pArtifact::local_overlaybd_layers(
            &manifest.rootfs.image_config_path,
            &rootfs_uuids,
        ));
        let memory_uuids = managed_layer_uuids_from_managed(&committed.memory_layers);
        artifacts.extend(SnapshotP2pArtifact::local_overlaybd_layers(
            &manifest.memory.image_config_path,
            &memory_uuids,
        ));
        for drive in &manifest.attached_drives {
            let drive_uuids = committed
                .attached_drives
                .iter()
                .find_map(|committed_drive| match committed_drive {
                    CommittedAttachedDrive::Overlaybd {
                        drive_id, layers, ..
                    } if drive_id == &drive.drive_id => Some(managed_layer_uuids(layers)),
                    _ => None,
                })
                .unwrap_or_default();
            artifacts.extend(SnapshotP2pArtifact::local_overlaybd_layers(
                &drive.image_config_path,
                &drive_uuids,
            ));
        }

        // Publish all artifacts concurrently, but don't fail if any individual artifact fails to publish.
        stream::iter(artifacts)
            .for_each_concurrent(SNAPSHOT_P2P_PUBLISH_CONCURRENCY, |artifact| async move {
                if let Err(error) = artifact.publish(transport).await {
                    warn!(
                        key = %artifact.key,
                        source = %artifact.source,
                        error = %error,
                        "failed to publish snapshot artifact to P2P"
                    );
                }
            })
            .await;
    }
}

#[cfg(test)]
mod advertisement_tests {
    //! 🔴 These live here and not beside `SnapshotManager` because what they
    //! assert is the *advertisement*, and the advertisement is this module's:
    //! it reads overlaybd layer files off this machine's disk and offers them
    //! to peers. The manager only decides whether there is anything to offer.
    use std::sync::Arc;

    use tempfile::TempDir;

    use crate::overlaybd::layer_key_from_digest;
    use crate::p2p::mock::MockTransport;
    use crate::p2p::P2pTransport as _;
    use crate::snapshot::mock::{write_mock_built_artifacts, InMemorySnapshotCatalog};
    use crate::snapshot::p2p::fixed_artifact_key;
    use crate::snapshot::repository::backends::storage::{PosixFsBackend, PosixFsBackendConfig};
    use crate::snapshot::{
        CallerOwnedArtifacts, CapturedSandboxSnapshot, SnapshotId, SnapshotManager,
        SnapshotPublishMetadata, SNAPSHOT_ARTIFACT_LAYOUT,
    };
    use crate::tests::snapshot_manager::{capture_of, staged_elsewhere};

    /// An adopted staging holds no local bytes and offers nothing to P2P; a local
    /// one holds them and does.
    ///
    /// 🔴 Two assertions per half, and the pair is deliberate. The P2P lookups
    /// alone are not enough: "nothing was advertised" is the same observation
    /// whether the staging had nothing to offer or had a manifest and failed to
    /// read the files it names, so a build that handed an adopted staging some
    /// other snapshot's manifest would still look right from there. The handle
    /// is asked directly for the fact itself.
    ///
    /// 🔴 And the local publish in the same round is what stops the negative
    /// halves passing on a build where staging or advertising stopped working
    /// altogether — a different and much worse bug than the one this pins.
    #[tokio::test]
    async fn only_bytes_this_process_holds_are_advertised() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let backend = PosixFsBackend::new(PosixFsBackendConfig {
            root: tempdir.path().join("repository"),
            cache_root: Some(tempdir.path().join("runtime-cache")),
            runtime_cache_root: Some(tempdir.path().join("runtime-cache").join("runtime")),
        })
        .expect("posix backend");
        let (repository, runtime_resolver) = backend.into_parts();
        // The POSIX byte half under a catalog these tests can publish through:
        // a node's own repository refuses every catalog call by construction.
        let repository = InMemorySnapshotCatalog::in_front_of(&repository);
        let p2p = Arc::new(MockTransport::default());
        let manager = SnapshotManager::from_parts(
            repository,
            Some(runtime_resolver),
            Some(Arc::new(crate::snapshot::P2pSnapshotAdvertiser::new(
                p2p.clone(),
            ))),
        );
        let sandbox = "one-sandbox";

        let staged_id = SnapshotId::generate();
        let adopted = manager
            .stage_captured(
                capture_of(sandbox, SnapshotId::generate()),
                CapturedSandboxSnapshot::staged(staged_elsewhere(staged_id.clone(), sandbox)),
            )
            .await
            .expect("an adopted staging");
        assert!(
            !adopted.holds_local_bytes(),
            "a staging performed on another machine claimed bytes this process holds"
        );
        manager
            .commit_and_advertise(adopted)
            .await
            .expect("an adopted staging should commit");

        let workspace = TempDir::new().expect("tempdir should exist");
        let (_, _, manifest) =
            write_mock_built_artifacts(workspace.path()).expect("mock artifacts should write");
        let local_id = SnapshotId::generate();
        let local = manager
            .stage_captured(
                capture_of(sandbox, local_id.clone()),
                CapturedSandboxSnapshot::local(CallerOwnedArtifacts::new(manifest)),
            )
            .await
            .expect("a local staging");
        assert!(
            local.holds_local_bytes(),
            "a staging this process performed disclaimed its own bytes, so the assertion above \
             proves nothing"
        );
        manager
            .commit_and_advertise(local)
            .await
            .expect("a local capture should publish");

        assert!(
            p2p.lookup(&fixed_artifact_key(
                &staged_id,
                SNAPSHOT_ARTIFACT_LAYOUT.vm_state
            ))
            .await
            .expect("lookup")
            .is_none(),
            "this process advertised bytes it has never held"
        );
        assert!(
            p2p.lookup(&fixed_artifact_key(
                &local_id,
                SNAPSHOT_ARTIFACT_LAYOUT.vm_state
            ))
            .await
            .expect("lookup")
            .is_some(),
            "the local publish advertised nothing, so the assertion above proves nothing"
        );
    }

    #[tokio::test]
    async fn publish_advertises_snapshot_artifacts_to_p2p_after_commit() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let backend = PosixFsBackend::new(PosixFsBackendConfig {
            root: tempdir.path().join("repository"),
            cache_root: Some(tempdir.path().join("runtime-cache")),
            runtime_cache_root: Some(tempdir.path().join("runtime-cache").join("runtime")),
        })
        .expect("posix backend");
        let (repository, runtime_resolver) = backend.into_parts();
        // The POSIX byte half under a catalog these tests can publish through:
        // a node's own repository refuses every catalog call by construction.
        let repository = InMemorySnapshotCatalog::in_front_of(&repository);
        let p2p = Arc::new(MockTransport::default());
        let manager = SnapshotManager::from_parts(
            repository,
            Some(runtime_resolver),
            Some(Arc::new(crate::snapshot::P2pSnapshotAdvertiser::new(
                p2p.clone(),
            ))),
        );

        let workspace = TempDir::new().expect("tempdir should exist");
        let (rootfs_lower, _, manifest) =
            write_mock_built_artifacts(workspace.path()).expect("mock artifacts should write");
        let snapshot_id = SnapshotId::generate();
        let metadata = SnapshotPublishMetadata {
            id: snapshot_id.clone(),
            ..SnapshotPublishMetadata::mock()
        };

        manager
            .publish(metadata, manifest)
            .await
            .expect("publish should commit");

        let vm_state_key = fixed_artifact_key(&snapshot_id, SNAPSHOT_ARTIFACT_LAYOUT.vm_state);
        let manifest_key =
            fixed_artifact_key(&snapshot_id, SNAPSHOT_ARTIFACT_LAYOUT.firecracker_manifest);
        let rootfs_layer_digest = crate::digest::FileDigest::describe(&rootfs_lower)
            .await
            .expect("describe rootfs lower");
        let rootfs_layer_key = layer_key_from_digest(&rootfs_layer_digest.sha256);

        assert!(p2p
            .lookup(&vm_state_key)
            .await
            .expect("lookup vm state")
            .is_some());
        assert!(p2p
            .lookup(&manifest_key)
            .await
            .expect("lookup manifest")
            .is_some());
        assert!(p2p
            .lookup(&rootfs_layer_key)
            .await
            .expect("lookup rootfs layer")
            .is_some());
    }
}
