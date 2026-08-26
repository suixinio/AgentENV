use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use super::repository::{
    ImportedSnapshotArtifacts, RepositoryError, RepositoryResult, SnapshotArtifactStore,
    SnapshotCatalog, SnapshotCommit, SnapshotListFilter, SnapshotRepository,
    SnapshotRuntimeResolver, StartedBuild,
};
use super::{
    PersistedDiskImagePublication, RunnableSnapshot, SnapshotId, SnapshotManager,
    SnapshotPublishMetadata, SnapshotRecord, SNAPSHOT_ARTIFACT_LAYOUT,
};
use crate::sandbox::FirecrackerSnapshotManifest;

/// Test double for catalog interactions that should stay unreachable.
///
/// `get_calls` — see [`Self::get_calls`] — is what
/// `node_server::tests`'s fallback/skip pairs assert on rather than matching
/// on this catalog's and [`MockSnapshotRuntimeResolver`]'s differently-worded
/// refusals: a string match cannot tell "the catalog was consulted and
/// refused" from "the catalog was never asked" once the wording changes (for
/// instance, when `--role node` stops holding a catalog at all and the
/// refusal becomes something like "no catalog access on `--role node`" —
/// still containing the substring "catalog"), while a call count goes to
/// zero the moment nothing calls [`Self::get`] any more, whatever the
/// message says.
#[derive(Debug, Default)]
pub struct MockSnapshotCatalog {
    get_calls: std::sync::atomic::AtomicUsize,
}

impl MockSnapshotCatalog {
    fn unsupported() -> RepositoryError {
        RepositoryError::Unsupported {
            feature: "mock snapshot catalog should not be called in this test".to_string(),
        }
    }

    /// How many times [`SnapshotCatalog::get`] has actually been called
    /// against this instance.
    pub fn get_calls(&self) -> usize {
        self.get_calls.load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[async_trait]
impl SnapshotCatalog for MockSnapshotCatalog {
    async fn create(&self, _record: SnapshotRecord) -> RepositoryResult<SnapshotRecord> {
        Err(Self::unsupported())
    }

    async fn publish_commit(&self, _commit: SnapshotCommit) -> RepositoryResult<SnapshotRecord> {
        Err(Self::unsupported())
    }

    async fn get(&self, _id_or_alias: &str) -> RepositoryResult<Option<SnapshotRecord>> {
        self.get_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Err(Self::unsupported())
    }

    async fn list(&self, _filter: SnapshotListFilter) -> RepositoryResult<Vec<SnapshotRecord>> {
        Err(Self::unsupported())
    }

    async fn delete_record(&self, _record: &SnapshotRecord) -> RepositoryResult<()> {
        Err(Self::unsupported())
    }

    async fn resolve_alias(&self, _alias: &str) -> RepositoryResult<Option<SnapshotId>> {
        Err(Self::unsupported())
    }

    async fn try_start_build(&self, _id: &SnapshotId) -> RepositoryResult<StartedBuild> {
        Err(Self::unsupported())
    }

    async fn mark_build_error(
        &self,
        _id: &SnapshotId,
        _reason: crate::snapshot::TemplateBuildErrorReason,
    ) -> RepositoryResult<()> {
        Err(Self::unsupported())
    }
}

/// Test double for artifact-store interactions that should stay unreachable.
#[derive(Clone, Debug, Default)]
pub struct MockSnapshotArtifactStore;

#[async_trait]
impl SnapshotArtifactStore for MockSnapshotArtifactStore {
    async fn import_built_artifacts(
        &self,
        _metadata: &SnapshotPublishMetadata,
        _manifest: &FirecrackerSnapshotManifest,
        _publications: &mut Vec<PersistedDiskImagePublication>,
    ) -> RepositoryResult<ImportedSnapshotArtifacts> {
        Err(RepositoryError::Unsupported {
            feature: "mock snapshot artifact store should not be called in this test".to_string(),
        })
    }

    async fn delete_artifacts(
        &self,
        _id: &SnapshotId,
        _publications: &[PersistedDiskImagePublication],
    ) {
    }
}

/// Test double for runtime resolution that should stay unreachable.
#[derive(Clone, Debug, Default)]
pub struct MockSnapshotRuntimeResolver;

impl MockSnapshotRuntimeResolver {
    fn unsupported() -> RepositoryError {
        RepositoryError::Unsupported {
            feature: "mock snapshot runtime resolver should not be called in this test".to_string(),
        }
    }
}

#[async_trait]
impl SnapshotRuntimeResolver for MockSnapshotRuntimeResolver {
    async fn resolve(&self, _snapshot: Arc<SnapshotRecord>) -> RepositoryResult<RunnableSnapshot> {
        Err(Self::unsupported())
    }
}

/// A catalog and artifact store that accept what they are given, and remember
/// it.
///
/// 🔴 Separate from the refusing doubles above rather than a mode on them. The
/// refusing pair exists so a test that reaches the repository fails loudly; a
/// pair that could be switched into accepting would make "the repository was
/// never called" and "the repository was called and said yes" the same default.
#[derive(Debug, Default)]
pub struct RecordingSnapshotRepository {
    /// Snapshot ids `stage` was asked to write bytes for, in order.
    staged: std::sync::Mutex<Vec<SnapshotId>>,
    /// Commits announced, in order: id and the alias each was bound under.
    committed: std::sync::Mutex<Vec<(SnapshotId, Option<String>)>>,
    /// When set, staging refuses. Committing is unaffected.
    staging_fails: std::sync::atomic::AtomicBool,
}

impl RecordingSnapshotRepository {
    pub fn staged(&self) -> Vec<SnapshotId> {
        self.staged.lock().expect("staged mutex poisoned").clone()
    }

    pub fn committed(&self) -> Vec<(SnapshotId, Option<String>)> {
        self.committed
            .lock()
            .expect("committed mutex poisoned")
            .clone()
    }

    pub fn fail_staging(&self) {
        self.staging_fails
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

#[async_trait]
impl SnapshotArtifactStore for RecordingSnapshotRepository {
    async fn import_built_artifacts(
        &self,
        metadata: &SnapshotPublishMetadata,
        _manifest: &FirecrackerSnapshotManifest,
        _publications: &mut Vec<PersistedDiskImagePublication>,
    ) -> RepositoryResult<ImportedSnapshotArtifacts> {
        if self.staging_fails.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(RepositoryError::Backend {
                message: "no room on the device".to_string(),
                source: None,
            });
        }
        self.staged
            .lock()
            .expect("staged mutex poisoned")
            .push(metadata.id.clone());
        Ok(ImportedSnapshotArtifacts::default())
    }

    async fn delete_artifacts(
        &self,
        _id: &SnapshotId,
        _publications: &[PersistedDiskImagePublication],
    ) {
    }
}

#[async_trait]
impl SnapshotCatalog for RecordingSnapshotRepository {
    async fn create(&self, record: SnapshotRecord) -> RepositoryResult<SnapshotRecord> {
        Ok(record)
    }

    async fn publish_commit(&self, commit: SnapshotCommit) -> RepositoryResult<SnapshotRecord> {
        self.committed
            .lock()
            .expect("committed mutex poisoned")
            .push((
                commit.id.clone(),
                commit.alias.as_ref().map(ToString::to_string),
            ));
        Ok(SnapshotRecord {
            id: commit.id,
            alias: commit.alias,
            source: match commit.source {
                crate::snapshot::SnapshotPublishSource::Template => {
                    crate::snapshot::SnapshotSource::Template {
                        build: crate::snapshot::TemplateBuildInfo {
                            status: crate::snapshot::TemplateBuildStatus::Ready,
                            started_at_unix_ms: None,
                            finished_at_unix_ms: commit.created_at_unix_ms,
                            error_reason: None,
                        },
                    }
                }
                crate::snapshot::SnapshotPublishSource::Sandbox { source_sandbox_id } => {
                    crate::snapshot::SnapshotSource::Sandbox { source_sandbox_id }
                }
            },
            resources: commit.resources,
            created_at_unix_ms: commit.created_at_unix_ms.unwrap_or_default(),
            updated_at_unix_ms: commit.created_at_unix_ms.unwrap_or_default(),
            committed: Some(commit.committed),
        })
    }

    async fn get(&self, _id_or_alias: &str) -> RepositoryResult<Option<SnapshotRecord>> {
        Ok(None)
    }

    async fn list(&self, _filter: SnapshotListFilter) -> RepositoryResult<Vec<SnapshotRecord>> {
        Ok(Vec::new())
    }

    async fn delete_record(&self, _record: &SnapshotRecord) -> RepositoryResult<()> {
        Ok(())
    }

    async fn resolve_alias(&self, _alias: &str) -> RepositoryResult<Option<SnapshotId>> {
        Ok(None)
    }

    async fn try_start_build(&self, _id: &SnapshotId) -> RepositoryResult<StartedBuild> {
        Err(MockSnapshotCatalog::unsupported())
    }

    async fn mark_build_error(
        &self,
        _id: &SnapshotId,
        _reason: crate::snapshot::TemplateBuildErrorReason,
    ) -> RepositoryResult<()> {
        Err(MockSnapshotCatalog::unsupported())
    }
}

/// A snapshot manager whose repository accepts stages and commits, and the
/// record of what it was asked to do.
pub fn recording_snapshot_manager() -> (SnapshotManager, Arc<RecordingSnapshotRepository>) {
    let repository = Arc::new(RecordingSnapshotRepository::default());
    let manager = SnapshotManager::from_parts(
        Arc::new(SnapshotRepository::new(
            Arc::clone(&repository) as Arc<dyn SnapshotCatalog>,
            Arc::clone(&repository) as Arc<dyn SnapshotArtifactStore>,
        )),
        Arc::new(MockSnapshotRuntimeResolver),
        None,
    );
    (manager, repository)
}

/// Builds a snapshot manager backed by snapshot test doubles.
pub fn mock_snapshot_manager() -> SnapshotManager {
    mock_snapshot_manager_with_catalog().0
}

/// [`mock_snapshot_manager`], but also hands back the concrete
/// [`MockSnapshotCatalog`] instance it wired in — for tests that need to
/// read [`MockSnapshotCatalog::get_calls`] after driving a request, rather
/// than inferring whether the catalog was consulted from the wording of
/// whatever it refused with.
pub fn mock_snapshot_manager_with_catalog() -> (SnapshotManager, Arc<MockSnapshotCatalog>) {
    let catalog = Arc::new(MockSnapshotCatalog::default());
    let manager = SnapshotManager::from_parts(
        Arc::new(SnapshotRepository::new(
            Arc::clone(&catalog) as Arc<dyn SnapshotCatalog>,
            Arc::new(MockSnapshotArtifactStore),
        )),
        Arc::new(MockSnapshotRuntimeResolver),
        None,
    );
    (manager, catalog)
}

/// Writes a minimal exported snapshot artifact set for snapshot publish/resolve tests.
pub fn write_mock_built_artifacts(
    root: &Path,
) -> Result<(PathBuf, PathBuf, FirecrackerSnapshotManifest)> {
    std::fs::create_dir_all(root.join(SNAPSHOT_ARTIFACT_LAYOUT.rootfs_dir))?;

    let rootfs_lower = root.join("base.overlaybd.commit");
    let memory_lower = root.join("mem.overlaybd.commit");

    std::fs::write(&rootfs_lower, b"base-layer")?;
    std::fs::write(&memory_lower, b"mem-layer")?;
    let rootfs_descriptor = crate::digest::FileDigest::describe_blocking(&rootfs_lower)?;
    let memory_descriptor = crate::digest::FileDigest::describe_blocking(&memory_lower)?;
    std::fs::write(root.join(SNAPSHOT_ARTIFACT_LAYOUT.vm_state), b"vm state")?;
    std::fs::write(
        root.join(SNAPSHOT_ARTIFACT_LAYOUT.rootfs_image_config),
        format!(
            r#"{{
  "repoBlobUrl": "",
  "lowers": [{{ "file": "{}", "digest": "{}", "size": {} }}],
  "upper": {{}},
  "resultFile": ""
}}"#,
            rootfs_lower.display(),
            rootfs_descriptor.sha256,
            rootfs_descriptor.size
        ),
    )?;
    std::fs::write(
        root.join(SNAPSHOT_ARTIFACT_LAYOUT.memory_image_config),
        format!(
            r#"{{"lowers":[{{"file":"{}","digest":"{}","size":{}}}]}}"#,
            memory_lower.display(),
            memory_descriptor.sha256,
            memory_descriptor.size
        ),
    )?;

    let manifest = FirecrackerSnapshotManifest::new(
        root.join(SNAPSHOT_ARTIFACT_LAYOUT.vm_state),
        root.join(SNAPSHOT_ARTIFACT_LAYOUT.memory_image_config),
        0,
        root.join(SNAPSHOT_ARTIFACT_LAYOUT.rootfs_image_config),
        32768,
        &Vec::new(),
    )?;

    Ok((rootfs_lower, memory_lower, manifest))
}
