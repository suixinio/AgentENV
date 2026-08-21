use std::collections::HashSet;
use std::sync::Arc;

use anyhow::Context;
use futures::{stream, StreamExt};
use tracing::warn;

use super::p2p::SnapshotP2pArtifact;
use super::types::SNAPSHOT_ARTIFACT_LAYOUT;
use crate::p2p::P2pTransport;
use crate::sandbox::{
    CapturedSandboxSnapshot, FirecrackerCapturedSnapshot, FirecrackerSnapshotManifest,
};
use crate::snapshot::repository::backends::build_snapshot_backend;
use crate::snapshot::repository::interfaces::{SnapshotRuntimeResolver, StagedSnapshot};
use crate::snapshot::repository::SnapshotRepository;
use crate::snapshot::repository::{RepositoryError, SnapshotListFilter};
use crate::snapshot::{
    ManagedLayer, OverlaybdLayerRef, RunnableSnapshot, SnapshotId, SnapshotPublishMetadata,
    SnapshotRecord,
};
use crate::types::ExecutionId;

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

/// What a `stage` on this node produced: the value that travels, and the part
/// that cannot.
///
/// 🔴 The split is the whole point. [`StagedSnapshot`] is pure and goes
/// anywhere; [`LocalSnapshotStaging`] is files on this disk and goes nowhere.
/// Keeping them in one struct with two accessors — rather than one struct with
/// a `#[serde(skip)]` field — means a caller that only has the travelling half
/// cannot accidentally be handed an empty local half that looks valid.
pub struct StagedSnapshotHandle {
    staged: StagedSnapshot,
    local: LocalSnapshotStaging,
}

impl StagedSnapshotHandle {
    /// The half that crosses a process boundary.
    pub fn staged(&self) -> &StagedSnapshot {
        &self.staged
    }

    pub fn into_parts(self) -> (StagedSnapshot, LocalSnapshotStaging) {
        (self.staged, self.local)
    }

    /// Drops the local half, keeping only what travels.
    ///
    /// Releases the capture's temporary directory, so anything still needing
    /// the files — the P2P advertisement — must already have run.
    pub fn into_staged(self) -> StagedSnapshot {
        self.staged
    }
}

/// The residue a `stage` leaves on the node it ran on.
///
/// Only one thing consumes it: the post-commit P2P advertisement, which reads
/// the manifest's local paths. It is not serialisable and must not become so —
/// the manifest's paths are all `#[serde(skip)]`, so a serialised copy would
/// arrive somewhere else looking complete and pointing at nothing.
pub struct LocalSnapshotStaging {
    manifest: FirecrackerSnapshotManifest,
    /// Holds the capture's temporary artifact directory open. Never read;
    /// dropping this is what reclaims it.
    _capture: Option<CapturedSandboxSnapshot>,
}

#[derive(Clone)]
/// Coordinates committed snapshot lifecycle operations over repository-backed state.
///
/// Durable reachability of committed snapshots is owned entirely by the
/// [`SnapshotRepository`] (PosixFS `managed-layers/`, OSS object storage, or the
/// source registry). The node-local overlaybd layer cache (`image-cache/commits/`)
/// is reclaimable - committed snapshots never pin it - so this manager records no
/// local image ref pins.
pub struct SnapshotManager {
    repository: Arc<SnapshotRepository>,
    runtime_resolver: Arc<dyn SnapshotRuntimeResolver>,
    p2p_transport: Option<Arc<dyn P2pTransport>>,
    /// Held, not used. The replay of owed object-store writes stops when the
    /// last handle is dropped, so it lives as long as the manager does.
    _mirror_compensator: Option<Arc<crate::snapshot::repository::mirror::MirrorCompensator>>,
}

impl SnapshotManager {
    /// Builds a manager using the configured repository backend.
    ///
    /// Async because assembling the catalog may have to open the double
    /// write's durable backlog and check it before anything is served.
    pub async fn new(p2p_transport: Option<Arc<dyn P2pTransport>>) -> anyhow::Result<Self> {
        let assembled = build_snapshot_backend(p2p_transport.clone()).await?;
        Ok(Self {
            repository: assembled.repository,
            runtime_resolver: assembled.runtime_resolver,
            p2p_transport,
            _mirror_compensator: assembled.mirror_compensator,
        })
    }

    /// Builds a manager from the given components.
    pub fn from_parts(
        repository: Arc<SnapshotRepository>,
        runtime_resolver: Arc<dyn SnapshotRuntimeResolver>,
        p2p_transport: Option<Arc<dyn P2pTransport>>,
    ) -> Self {
        Self {
            repository,
            runtime_resolver,
            p2p_transport,
            _mirror_compensator: None,
        }
    }

    pub async fn create(
        &self,
        record: SnapshotRecord,
    ) -> crate::snapshot::RepositoryResult<SnapshotRecord> {
        self.repository.create(record).await
    }

    #[tracing::instrument(skip(self, metadata, manifest), fields(snapshot_id = %metadata.id))]
    pub async fn publish(
        &self,
        metadata: SnapshotPublishMetadata,
        manifest: FirecrackerSnapshotManifest,
    ) -> crate::snapshot::RepositoryResult<SnapshotRecord> {
        let handle = self.stage(metadata, manifest, None).await?;
        self.commit_and_advertise(handle).await
    }

    #[tracing::instrument(skip(self, metadata), fields(snapshot_id = %metadata.id))]
    pub async fn publish_captured(
        &self,
        metadata: SnapshotPublishMetadata,
        captured_snapshot: CapturedSandboxSnapshot,
    ) -> crate::snapshot::RepositoryResult<SnapshotRecord> {
        let handle = self
            .stage_captured(metadata, captured_snapshot, None)
            .await?;
        self.commit_and_advertise(handle).await
    }

    /// Writes one capture's bytes into durable storage without announcing them.
    ///
    /// 🔴 Consumes the capture. The handle it returns owns it from here, which
    /// is what keeps the temporary artifact directory alive for exactly as long
    /// as this node still has something to do with it — and no longer.
    #[tracing::instrument(skip(self, metadata, captured_snapshot), fields(snapshot_id = %metadata.id))]
    pub async fn stage_captured(
        &self,
        metadata: SnapshotPublishMetadata,
        captured_snapshot: CapturedSandboxSnapshot,
        execution_id: Option<ExecutionId>,
    ) -> crate::snapshot::RepositoryResult<StagedSnapshotHandle> {
        let manifest = captured_snapshot
            .downcast_ref::<FirecrackerCapturedSnapshot>()
            .map(|snapshot| snapshot.manifest().clone())
            .ok_or_else(|| RepositoryError::Unsupported {
                feature: "publishing captured snapshots for this sandbox backend".to_string(),
            })?;

        let staged = self
            .repository
            .stage(metadata, manifest.clone(), execution_id)
            .await?;

        Ok(StagedSnapshotHandle {
            staged,
            local: LocalSnapshotStaging {
                manifest,
                _capture: Some(captured_snapshot),
            },
        })
    }

    /// [`Self::stage_captured`] for artifacts that were built rather than
    /// captured, and so are not held alive by a capture guard.
    #[tracing::instrument(skip(self, metadata, manifest), fields(snapshot_id = %metadata.id))]
    pub async fn stage(
        &self,
        metadata: SnapshotPublishMetadata,
        manifest: FirecrackerSnapshotManifest,
        execution_id: Option<ExecutionId>,
    ) -> crate::snapshot::RepositoryResult<StagedSnapshotHandle> {
        let staged = self
            .repository
            .stage(metadata, manifest.clone(), execution_id)
            .await?;

        Ok(StagedSnapshotHandle {
            staged,
            local: LocalSnapshotStaging {
                manifest,
                _capture: None,
            },
        })
    }

    /// Announces a staged snapshot. The flip, and nothing else.
    ///
    /// 🔴 Takes the pure value, not the handle. A caller that has one of these
    /// and nothing else — which is every caller once `--role api` exists — can
    /// still commit, and that is the property the seam is for. Serialising a
    /// [`StagedSnapshot`], sending it, and committing it on the far side has to
    /// work, so nothing here may consult the local half.
    #[tracing::instrument(skip(self, staged), fields(snapshot_id = %staged.commit.id))]
    pub async fn commit_staged(
        &self,
        staged: StagedSnapshot,
    ) -> crate::snapshot::RepositoryResult<SnapshotRecord> {
        self.repository.commit_staged(staged).await
    }

    /// Offers a committed snapshot's local bytes to the P2P transport.
    ///
    /// 🔴 After the commit, never before: publishing artifacts for a snapshot
    /// whose row was never written would advertise something no reader can
    /// resolve, and the convention that P2P only carries committed snapshots is
    /// what lets a peer treat a hit as authoritative.
    ///
    /// 🔴 Node-local, and that is the piece the next phase has to move. It
    /// reads files, so it can only run where the bytes are — while the commit
    /// that must precede it will be running somewhere else. A commit performed
    /// by `--role api` therefore needs a way to tell this node it happened;
    /// until that exists, the two are in the same process and this ordering is
    /// simply a statement order.
    /// 🔴 Takes the residue by value, and the capture inside it goes out of
    /// scope when this returns. A borrow would also have to be `Sync` to be
    /// held across the awaits below, and the capture is deliberately not — it
    /// is a `Box<dyn Any + Send>` owned by exactly one place at a time.
    pub async fn advertise_committed(&self, record: &SnapshotRecord, local: LocalSnapshotStaging) {
        self.publish_p2p_artifacts(record, &local.manifest).await;
    }

    /// stage -> commit -> advertise, in the one process that can do all three.
    async fn commit_and_advertise(
        &self,
        handle: StagedSnapshotHandle,
    ) -> crate::snapshot::RepositoryResult<SnapshotRecord> {
        let (staged, local) = handle.into_parts();
        let record = self.commit_staged(staged).await?;
        self.advertise_committed(&record, local).await;
        Ok(record)
    }

    /// Best effort attempt to publish snapshot artifacts to P2P.
    #[tracing::instrument(skip(self, record, manifest), fields(snapshot_id = %record.id))]
    async fn publish_p2p_artifacts(
        &self,
        record: &SnapshotRecord,
        manifest: &FirecrackerSnapshotManifest,
    ) {
        let Some(transport) = self.p2p_transport.as_ref() else {
            return;
        };
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
                    crate::snapshot::CommittedAttachedDrive::Overlaybd {
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

    /// Loads a snapshot record by id or alias.
    pub async fn get(
        &self,
        id_or_alias: impl AsRef<str>,
    ) -> anyhow::Result<Option<SnapshotRecord>> {
        self.repository
            .get(id_or_alias.as_ref())
            .await
            .with_context(|| {
                format!(
                    "load committed snapshot '{}' through repository",
                    id_or_alias.as_ref()
                )
            })
    }

    /// Lists snapshot records that match the given filter.
    pub async fn list(&self, filter: SnapshotListFilter) -> anyhow::Result<Vec<SnapshotRecord>> {
        self.repository
            .list(filter)
            .await
            .context("list committed snapshots through repository")
    }

    /// Deletes a snapshot by id or alias.
    ///
    /// Returns `Ok(())` on success. The operation is idempotent:
    /// if the snapshot does not exist, it is still considered success.
    pub async fn delete(&self, id_or_alias: impl AsRef<str>) -> anyhow::Result<()> {
        self.repository
            .delete(id_or_alias.as_ref())
            .await
            .with_context(|| {
                format!(
                    "delete snapshot '{}' through repository",
                    id_or_alias.as_ref()
                )
            })
    }

    /// Resolves an alias to its committed snapshot id.
    pub async fn resolve_committed_alias(&self, alias: &str) -> anyhow::Result<Option<SnapshotId>> {
        self.repository.resolve_alias(alias).await.with_context(|| {
            format!("resolve committed snapshot alias '{alias}' through repository")
        })
    }

    /// Resolves a committed snapshot into node-local runnable artifact paths.
    pub async fn resolve_runnable(
        &self,
        snapshot: SnapshotRecord,
    ) -> anyhow::Result<RunnableSnapshot> {
        self.runtime_resolver
            .resolve(Arc::new(snapshot))
            .await
            .context("resolve committed snapshot into runnable runtime paths")
    }

    /// Loads a committed snapshot and immediately resolves it into runnable state.
    #[tracing::instrument(
        skip(self, id_or_alias),
        fields(snapshot_ref = %id_or_alias.as_ref())
    )]
    pub async fn load_runnable(
        &self,
        id_or_alias: impl AsRef<str>,
    ) -> anyhow::Result<Option<RunnableSnapshot>> {
        let Some(snapshot) = self.get(id_or_alias.as_ref()).await? else {
            return Ok(None);
        };
        self.resolve_runnable(snapshot).await.map(Some)
    }

    /// Atomically transitions one template build from waiting to building.
    pub async fn try_start_build(
        &self,
        id: &SnapshotId,
    ) -> crate::snapshot::RepositoryResult<SnapshotRecord> {
        self.repository.try_start_build(id).await
    }

    /// Marks one template build as failed.
    pub async fn mark_build_error(
        &self,
        id: &SnapshotId,
        reason: crate::snapshot::TemplateBuildErrorReason,
    ) -> crate::snapshot::RepositoryResult<()> {
        self.repository.mark_build_error(id, reason).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::overlaybd::layer_key_from_digest;
    use crate::p2p::mock::MockTransport;
    use crate::snapshot::mock::write_mock_built_artifacts;
    use crate::snapshot::p2p::fixed_artifact_key;
    use crate::snapshot::repository::backends::{PosixFsBackend, PosixFsBackendConfig};
    use crate::snapshot::{SnapshotAlias, SnapshotId, SnapshotPublishMetadata};
    use std::path::Path;
    use tempfile::TempDir;

    fn test_manager(root: &Path) -> SnapshotManager {
        let backend = PosixFsBackend::new(PosixFsBackendConfig {
            root: root.join("repository"),
            cache_root: Some(root.join("runtime-cache")),
            runtime_cache_root: Some(root.join("runtime-cache").join("runtime")),
        })
        .expect("posix backend");
        let (repository, runtime_resolver) = backend.into_parts();
        SnapshotManager::from_parts(repository, runtime_resolver, None)
    }

    async fn seed_built_snapshot(manager: &SnapshotManager, snapshot_id: SnapshotId, alias: &str) {
        let workspace = TempDir::new().expect("tempdir should exist");
        let (_, _, manifest) =
            write_mock_built_artifacts(workspace.path()).expect("mock artifacts should write");
        let metadata = SnapshotPublishMetadata {
            id: snapshot_id,
            alias: Some(SnapshotAlias::parse(alias).expect("alias should parse")),
            ..SnapshotPublishMetadata::mock()
        };
        manager
            .publish(metadata, manifest)
            .await
            .expect("seed publish should work");
    }

    #[tokio::test]
    async fn repository_management_methods_delegate_to_committed_store() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let manager = test_manager(tempdir.path());
        let snapshot_id = SnapshotId::generate();
        seed_built_snapshot(&manager, snapshot_id.clone(), "managed").await;

        let resolved = manager
            .resolve_committed_alias("managed")
            .await
            .expect("resolve alias should work");
        assert_eq!(resolved, Some(snapshot_id.clone()));

        let loaded = manager
            .get("managed")
            .await
            .expect("load should work")
            .expect("snapshot should exist");
        assert_eq!(loaded.id, snapshot_id);

        let listed = manager
            .list(crate::snapshot::repository::SnapshotListFilter::matches_all())
            .await
            .expect("list should work");
        assert_eq!(listed.len(), 1);

        manager.delete("managed").await.expect("delete should work");
        assert!(manager
            .get("managed")
            .await
            .expect("load after delete should work")
            .is_none());
    }

    #[tokio::test]
    async fn load_runnable_uses_committed_snapshot_and_runtime_resolution() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let manager = test_manager(tempdir.path());
        let snapshot_id = SnapshotId::generate();
        seed_built_snapshot(&manager, snapshot_id.clone(), "runnable").await;

        let runnable = manager
            .load_runnable("runnable")
            .await
            .expect("load runnable should work")
            .expect("runnable snapshot should exist");

        assert_eq!(runnable.record().id, snapshot_id);
        assert!(runnable.manifest().rootfs.image_config_path.exists());
        assert!(runnable.manifest().vm_state.path.exists());
    }

    /// 🔴 P12, second half, against a real backend rather than a fake catalog.
    /// After `stage` and before `commit_staged` the artifacts are on disk and
    /// neither read path can see the snapshot. The control is the commit: the
    /// same two reads, run again after it, must both find it.
    #[tokio::test]
    async fn a_staged_snapshot_has_bytes_on_disk_and_no_row_anywhere() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let manager = test_manager(tempdir.path());
        let snapshot_id = SnapshotId::generate();
        let workspace = TempDir::new().expect("tempdir should exist");
        let (_, _, manifest) =
            write_mock_built_artifacts(workspace.path()).expect("mock artifacts should write");
        let metadata = SnapshotPublishMetadata {
            id: snapshot_id.clone(),
            alias: Some(SnapshotAlias::parse("staged-only").expect("alias should parse")),
            ..SnapshotPublishMetadata::mock()
        };

        let handle = manager
            .stage(metadata, manifest, None)
            .await
            .expect("staging should work");

        let artifacts = tempdir
            .path()
            .join("repository")
            .join("snapshots")
            .join(snapshot_id.to_string());
        assert!(
            artifacts.join("vm_state.bin").exists(),
            "the bytes must be durable before the row exists"
        );
        assert!(
            manager
                .get(snapshot_id.to_string())
                .await
                .expect("get should work")
                .is_none(),
            "a staged snapshot must not be resolvable by id"
        );
        assert!(
            manager
                .resolve_committed_alias("staged-only")
                .await
                .expect("resolve should work")
                .is_none(),
            "a staged snapshot must not be resolvable by alias"
        );
        assert!(manager
            .list(crate::snapshot::repository::SnapshotListFilter::matches_all())
            .await
            .expect("list should work")
            .is_empty());

        // The control: one more call flips all three answers.
        let (staged, local) = handle.into_parts();
        let record = manager
            .commit_staged(staged)
            .await
            .expect("commit should work");
        manager.advertise_committed(&record, local).await;

        assert!(manager
            .get(snapshot_id.to_string())
            .await
            .expect("get should work")
            .is_some());
        assert_eq!(
            manager
                .resolve_committed_alias("staged-only")
                .await
                .expect("resolve should work"),
            Some(snapshot_id)
        );
        assert_eq!(
            manager
                .list(crate::snapshot::repository::SnapshotListFilter::matches_all())
                .await
                .expect("list should work")
                .len(),
            1
        );
    }

    /// The staged value that reached the commit has to be the one that could
    /// have travelled — so run the round trip through the manager's own API,
    /// not just the repository's.
    #[tokio::test]
    async fn a_manager_staged_snapshot_commits_after_a_serde_round_trip() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let manager = test_manager(tempdir.path());
        let snapshot_id = SnapshotId::generate();
        let workspace = TempDir::new().expect("tempdir should exist");
        let (_, _, manifest) =
            write_mock_built_artifacts(workspace.path()).expect("mock artifacts should write");
        let metadata = SnapshotPublishMetadata {
            id: snapshot_id.clone(),
            alias: Some(SnapshotAlias::parse("round-tripped").expect("alias should parse")),
            ..SnapshotPublishMetadata::mock()
        };

        let handle = manager
            .stage(metadata, manifest, None)
            .await
            .expect("staging should work");
        let encoded = serde_json::to_vec(handle.staged()).expect("staged value should serialize");
        // 🔴 Dropped before the commit: the local half is gone, and the commit
        // still has to work. That is the property `--role api` depends on.
        drop(handle);

        let decoded: StagedSnapshot =
            serde_json::from_slice(&encoded).expect("staged value should deserialize");
        let record = manager
            .commit_staged(decoded)
            .await
            .expect("a round-tripped staged snapshot must commit");

        assert_eq!(record.id, snapshot_id);
        assert!(manager
            .get("round-tripped")
            .await
            .expect("get should work")
            .is_some());
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
        let p2p = Arc::new(MockTransport::default());
        let manager = SnapshotManager::from_parts(repository, runtime_resolver, Some(p2p.clone()));

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
