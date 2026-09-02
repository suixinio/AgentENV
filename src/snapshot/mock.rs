use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use super::repository::{
    ImportedSnapshotArtifacts, RepositoryError, RepositoryResult, SnapshotArtifactStore,
    SnapshotCatalog, SnapshotCommit, SnapshotCursor, SnapshotListFilter, SnapshotListPage,
    SnapshotRepository, SnapshotRuntimeResolver, StartedBuild,
};
use super::{
    CommittedSnapshot, PausedSandboxConfig, PersistedDiskImagePublication, SnapshotId,
    SnapshotManager, SnapshotPublishMetadata, SnapshotRecord, SnapshotSource,
    SNAPSHOT_ARTIFACT_LAYOUT,
};
use crate::orchestrator::SandboxTimeoutAction;
use crate::runtime_snapshot::RunnableSnapshot;
use crate::sandbox::SandboxNetworkPolicy;
use crate::types::{FirecrackerSnapshotManifest, SandboxId};

/// Refusing catalog test double that records whether reads were attempted.
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

    async fn list_page(&self, _filter: SnapshotListFilter) -> RepositoryResult<SnapshotListPage> {
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

/// Recording repository test double that accepts stages and commits.
#[derive(Debug, Default)]
pub struct RecordingSnapshotRepository {
    staged: std::sync::Mutex<Vec<SnapshotId>>,
    committed: std::sync::Mutex<Vec<(SnapshotId, Option<String>)>>,
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
            origin_node_id: None,
            committed: Some(commit.committed),
        })
    }

    async fn get(&self, _id_or_alias: &str) -> RepositoryResult<Option<SnapshotRecord>> {
        Ok(None)
    }

    async fn list_page(&self, _filter: SnapshotListFilter) -> RepositoryResult<SnapshotListPage> {
        Ok(SnapshotListPage::single(Vec::new()))
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
        Some(Arc::new(MockSnapshotRuntimeResolver)),
        None,
    );
    (manager, repository)
}

/// Builds a snapshot manager backed by snapshot test doubles.
pub fn mock_snapshot_manager() -> SnapshotManager {
    mock_snapshot_manager_with_catalog().0
}

/// Builds a refusing snapshot manager and returns its concrete catalog.
pub fn mock_snapshot_manager_with_catalog() -> (SnapshotManager, Arc<MockSnapshotCatalog>) {
    let catalog = Arc::new(MockSnapshotCatalog::default());
    let manager = SnapshotManager::from_parts(
        Arc::new(SnapshotRepository::new(
            Arc::clone(&catalog) as Arc<dyn SnapshotCatalog>,
            Arc::new(MockSnapshotArtifactStore),
        )),
        Some(Arc::new(MockSnapshotRuntimeResolver)),
        None,
    );
    (manager, catalog)
}

/// Read-only one-row catalog paired with a refusing runtime resolver.
pub struct OneRowSnapshotCatalog {
    row: SnapshotRecord,
}

#[async_trait]
impl SnapshotCatalog for OneRowSnapshotCatalog {
    async fn create(&self, _record: SnapshotRecord) -> RepositoryResult<SnapshotRecord> {
        Err(MockSnapshotCatalog::unsupported())
    }

    async fn publish_commit(&self, _commit: SnapshotCommit) -> RepositoryResult<SnapshotRecord> {
        Err(MockSnapshotCatalog::unsupported())
    }

    async fn get(&self, _id_or_alias: &str) -> RepositoryResult<Option<SnapshotRecord>> {
        Ok(Some(self.row.clone()))
    }

    async fn list_page(&self, filter: SnapshotListFilter) -> RepositoryResult<SnapshotListPage> {
        Ok(paginate_records(
            vec![self.row.clone()],
            filter.effective_limit(),
            filter.cursor.as_ref(),
        ))
    }

    async fn delete_record(&self, _record: &SnapshotRecord) -> RepositoryResult<()> {
        Err(MockSnapshotCatalog::unsupported())
    }

    async fn resolve_alias(&self, _alias: &str) -> RepositoryResult<Option<SnapshotId>> {
        Ok(Some(self.row.id.clone()))
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

/// Builds a manager over an in-memory catalog a test can seed and read back.
/// Artifacts are discarded and the runtime resolver refuses.
pub fn in_memory_snapshot_manager() -> (SnapshotManager, Arc<InMemorySnapshotCatalog>) {
    let catalog = Arc::new(InMemorySnapshotCatalog::default());
    let manager = SnapshotManager::from_parts(
        Arc::new(SnapshotRepository::new(
            Arc::clone(&catalog) as Arc<dyn SnapshotCatalog>,
            Arc::new(MockSnapshotArtifactStore),
        )),
        Some(Arc::new(MockSnapshotRuntimeResolver)),
        None,
    );
    (manager, catalog)
}

/// The pause configuration of a test row: a resumable, insecure sandbox from
/// template `tpl-paused` with no timeout of its own.
pub fn mock_paused_sandbox_config() -> PausedSandboxConfig {
    PausedSandboxConfig {
        template_id: "tpl-paused".to_string(),
        template_alias: None,
        created_at_unix_ms: 1_700_000_000_000,
        timeout_secs: None,
        timeout_action: SandboxTimeoutAction::Pause,
        auto_resume: true,
        user_metadata: None,
        network_policy: SandboxNetworkPolicy::default(),
        secure: false,
        control_plane_config: None,
        max_lifetime_secs: None,
        running_elapsed_secs: 0,
    }
}

/// A committed row paused from `sandbox_id` at `paused_at_unix_ms`, staged on
/// `origin_node_id` when one is named.
pub fn paused_sandbox_record(
    sandbox_id: SandboxId,
    origin_node_id: Option<&str>,
    paused: PausedSandboxConfig,
    paused_at_unix_ms: i64,
) -> SnapshotRecord {
    SnapshotRecord {
        id: SnapshotId::generate(),
        alias: None,
        source: SnapshotSource::Sandbox {
            source_sandbox_id: sandbox_id.to_string(),
        },
        resources: Default::default(),
        created_at_unix_ms: paused_at_unix_ms,
        updated_at_unix_ms: paused_at_unix_ms,
        committed: Some(CommittedSnapshot {
            paused_sandbox: Some(paused),
            ..CommittedSnapshot::mock()
        }),
        origin_node_id: origin_node_id.map(str::to_string),
    }
}

/// Builds a manager whose catalog has `row` and whose runtime resolver refuses.
pub fn unresolvable_snapshot_manager(row: SnapshotRecord) -> SnapshotManager {
    SnapshotManager::from_parts(
        Arc::new(SnapshotRepository::new(
            Arc::new(OneRowSnapshotCatalog { row }) as Arc<dyn SnapshotCatalog>,
            Arc::new(MockSnapshotArtifactStore),
        )),
        Some(Arc::new(MockSnapshotRuntimeResolver)),
        None,
    )
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

/// In-memory snapshot catalog for byte-store and manager tests.
///
/// It mirrors production alias, paging, build-state, and artifact-retention semantics.
#[derive(Debug, Default)]
pub struct InMemorySnapshotCatalog {
    rows: std::sync::Mutex<std::collections::HashMap<String, SnapshotRecord>>,
    aliases: std::sync::Mutex<std::collections::HashMap<String, SnapshotId>>,
}

/// Sorts an in-memory listing and returns one production-compatible page.
fn paginate_records(
    mut records: Vec<SnapshotRecord>,
    limit: u32,
    cursor: Option<&SnapshotCursor>,
) -> SnapshotListPage {
    if limit == 0 {
        return SnapshotListPage {
            items: Vec::new(),
            next: None,
        };
    }

    records.sort_by(SnapshotCursor::order);
    if let Some(cursor) = cursor {
        records.retain(|record| cursor.is_before(record));
    }

    let limit = limit as usize;
    let next = if records.len() > limit {
        records.get(limit - 1).map(SnapshotCursor::of)
    } else {
        None
    };
    records.truncate(limit);

    SnapshotListPage {
        items: records,
        next,
    }
}

impl InMemorySnapshotCatalog {
    /// Seeds an exact row, including its paging timestamp, bypassing write rules.
    pub fn seed(&self, record: SnapshotRecord) {
        self.rows
            .lock()
            .expect("rows")
            .insert(record.id.to_string(), record);
    }

    /// The byte half the caller built, under a catalog a test can publish
    /// through. `repository.artifacts()` is kept; its catalog is replaced.
    pub fn in_front_of(repository: &SnapshotRepository) -> Arc<SnapshotRepository> {
        Arc::new(SnapshotRepository::new(
            Arc::new(Self::default()),
            repository.artifacts(),
        ))
    }

    fn now_ms() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_millis() as i64)
            .unwrap_or(0)
    }

    fn claim_alias(
        &self,
        alias: &super::SnapshotAlias,
        new_id: &SnapshotId,
    ) -> RepositoryResult<()> {
        let rows = self.rows.lock().expect("rows");
        let mut aliases = self.aliases.lock().expect("aliases");
        if let Some(existing) = aliases.get(&alias.to_string()).cloned() {
            if &existing == new_id {
                return Ok(());
            }
            if rows.contains_key(&existing.to_string()) {
                return Err(RepositoryError::AliasConflict {
                    alias: alias.to_string(),
                    existing,
                    new_id: new_id.clone(),
                });
            }
            aliases.remove(&alias.to_string());
        }
        aliases.insert(alias.to_string(), new_id.clone());
        Ok(())
    }

    fn matches(record: &SnapshotRecord, filter: &SnapshotListFilter) -> bool {
        use super::{SnapshotSource, SnapshotSourceKind};

        if let Some(prefix) = filter.alias_prefix.as_deref() {
            match record.alias.as_ref() {
                Some(alias) if alias.to_string().starts_with(prefix) => {}
                _ => return false,
            }
        }
        if let Some(ids) = filter.snapshot_ids.as_ref() {
            if !ids.iter().any(|id| id == &record.id) {
                return false;
            }
        }
        if let Some(id_or_alias) = filter.snapshot_id_or_alias.as_deref() {
            if record.id.to_string() != id_or_alias
                && record
                    .alias
                    .as_ref()
                    .is_none_or(|alias| alias.as_ref() != id_or_alias)
            {
                return false;
            }
        }
        if let Some(sandbox) = filter.source_sandbox_id.as_deref() {
            match &record.source {
                SnapshotSource::Sandbox { source_sandbox_id } if source_sandbox_id == sandbox => {}
                _ => return false,
            }
        }
        if let Some(sources) = filter.sources.as_ref() {
            let kind = match &record.source {
                SnapshotSource::Template { .. } => SnapshotSourceKind::Template,
                SnapshotSource::Sandbox { .. } => SnapshotSourceKind::Sandbox,
            };
            if !sources.contains(&kind) {
                return false;
            }
        }
        if let Some(statuses) = filter.template_statuses.as_ref() {
            let SnapshotSource::Template { build } = &record.source else {
                return false;
            };
            if !statuses.contains(&build.status) {
                return false;
            }
        }
        true
    }
}

#[async_trait]
impl SnapshotCatalog for InMemorySnapshotCatalog {
    async fn create(&self, record: SnapshotRecord) -> RepositoryResult<SnapshotRecord> {
        use super::SnapshotSource;

        if !matches!(record.source, SnapshotSource::Template { .. }) {
            return Err(RepositoryError::InvalidRequest {
                reason: "only template snapshots can be pre-created".to_string(),
            });
        }
        if record.committed.is_some() {
            return Err(RepositoryError::InvalidRequest {
                reason: "pre-created template snapshots must not already be committed".to_string(),
            });
        }
        if self
            .rows
            .lock()
            .expect("rows")
            .contains_key(&record.id.to_string())
        {
            return Err(RepositoryError::InvalidRequest {
                reason: format!("snapshot '{}' already exists", record.id),
            });
        }
        if let Some(alias) = record.alias.as_ref() {
            self.claim_alias(alias, &record.id)?;
        }
        self.rows
            .lock()
            .expect("rows")
            .insert(record.id.to_string(), record.clone());
        Ok(record)
    }

    async fn publish_commit(&self, commit: SnapshotCommit) -> RepositoryResult<SnapshotRecord> {
        use super::{
            SnapshotPublishSource, SnapshotSource, TemplateBuildInfo, TemplateBuildStatus,
        };

        let now = Self::now_ms();
        if let Some(alias) = commit.alias.as_ref() {
            self.claim_alias(alias, &commit.id)?;
        }

        let existing = self
            .rows
            .lock()
            .expect("rows")
            .get(&commit.id.to_string())
            .cloned();
        let record = match existing {
            Some(mut record) => {
                record.mark_committed(
                    commit.alias.clone(),
                    commit.resources,
                    commit.committed.clone(),
                    commit.source.clone(),
                    now,
                );
                record
            }
            None => {
                let source = match commit.source.clone() {
                    SnapshotPublishSource::Template => SnapshotSource::Template {
                        build: TemplateBuildInfo {
                            status: TemplateBuildStatus::Ready,
                            started_at_unix_ms: None,
                            finished_at_unix_ms: Some(now),
                            error_reason: None,
                        },
                    },
                    SnapshotPublishSource::Sandbox { source_sandbox_id } => {
                        SnapshotSource::Sandbox { source_sandbox_id }
                    }
                };
                SnapshotRecord {
                    id: commit.id.clone(),
                    alias: commit.alias.clone(),
                    source,
                    resources: commit.resources,
                    created_at_unix_ms: commit.created_at_unix_ms.unwrap_or(now),
                    updated_at_unix_ms: now,
                    origin_node_id: commit.origin_node_id.clone(),
                    committed: Some(commit.committed.clone()),
                }
            }
        };
        self.rows
            .lock()
            .expect("rows")
            .insert(record.id.to_string(), record.clone());
        Ok(record)
    }

    async fn get(&self, id_or_alias: &str) -> RepositoryResult<Option<SnapshotRecord>> {
        if let Some(record) = self.rows.lock().expect("rows").get(id_or_alias).cloned() {
            return Ok(Some(record));
        }
        let Some(id) = self
            .aliases
            .lock()
            .expect("aliases")
            .get(id_or_alias)
            .cloned()
        else {
            return Ok(None);
        };
        Ok(self
            .rows
            .lock()
            .expect("rows")
            .get(&id.to_string())
            .cloned())
    }

    async fn list_page(&self, filter: SnapshotListFilter) -> RepositoryResult<SnapshotListPage> {
        let records: Vec<SnapshotRecord> = self
            .rows
            .lock()
            .expect("rows")
            .values()
            .filter(|record| Self::matches(record, &filter))
            .cloned()
            .collect();
        Ok(paginate_records(
            records,
            filter.effective_limit(),
            filter.cursor.as_ref(),
        ))
    }

    async fn delete_record(&self, record: &SnapshotRecord) -> RepositoryResult<()> {
        self.rows
            .lock()
            .expect("rows")
            .remove(&record.id.to_string());
        if let Some(alias) = record.alias.as_ref() {
            let mut aliases = self.aliases.lock().expect("aliases");
            if aliases.get(&alias.to_string()) == Some(&record.id) {
                aliases.remove(&alias.to_string());
            }
        }
        Ok(())
    }

    async fn resolve_alias(&self, alias: &str) -> RepositoryResult<Option<SnapshotId>> {
        let Some(id) = self.aliases.lock().expect("aliases").get(alias).cloned() else {
            return Ok(None);
        };
        if self
            .rows
            .lock()
            .expect("rows")
            .contains_key(&id.to_string())
        {
            return Ok(Some(id));
        }
        self.aliases.lock().expect("aliases").remove(alias);
        Ok(None)
    }

    async fn try_start_build(&self, id: &SnapshotId) -> RepositoryResult<StartedBuild> {
        use super::repository::interfaces::build_may_start_from;
        use super::{SnapshotSource, TemplateBuildStatus};

        let mut rows = self.rows.lock().expect("rows");
        let record =
            rows.get_mut(&id.to_string())
                .ok_or_else(|| RepositoryError::SnapshotNotFound {
                    lookup: id.to_string(),
                })?;
        let now = Self::now_ms();
        let SnapshotSource::Template { build } = &mut record.source else {
            return Err(RepositoryError::InvalidRequest {
                reason: format!("snapshot '{id}' is not a template build"),
            });
        };
        if !build_may_start_from(build.status) {
            return Err(RepositoryError::InvalidRequest {
                reason: format!(
                    "template build '{id}' cannot start from {:?}: only a template that has \
                     never been built or whose last build failed may be built",
                    build.status
                ),
            });
        }
        build.status = TemplateBuildStatus::Building;
        build.started_at_unix_ms = Some(now);
        build.error_reason = None;
        record.updated_at_unix_ms = now;
        Ok(StartedBuild::untracked(record.clone()))
    }

    async fn mark_build_error(
        &self,
        id: &SnapshotId,
        reason: super::TemplateBuildErrorReason,
    ) -> RepositoryResult<()> {
        use super::{SnapshotSource, TemplateBuildStatus};

        let mut rows = self.rows.lock().expect("rows");
        let record =
            rows.get_mut(&id.to_string())
                .ok_or_else(|| RepositoryError::SnapshotNotFound {
                    lookup: id.to_string(),
                })?;
        let now = Self::now_ms();
        let SnapshotSource::Template { build } = &mut record.source else {
            return Err(RepositoryError::InvalidRequest {
                reason: format!("snapshot '{id}' is not a template build"),
            });
        };
        build.status = TemplateBuildStatus::Error;
        build.finished_at_unix_ms = Some(now);
        build.error_reason = Some(reason);
        record.updated_at_unix_ms = now;
        Ok(())
    }

    async fn set_origin_node_id(
        &self,
        id: &SnapshotId,
        origin_node_id: &str,
    ) -> RepositoryResult<()> {
        let mut rows = self.rows.lock().expect("rows");
        let record =
            rows.get_mut(&id.to_string())
                .ok_or_else(|| RepositoryError::SnapshotNotFound {
                    lookup: id.to_string(),
                })?;
        record.origin_node_id = Some(origin_node_id.to_string());
        Ok(())
    }

    async fn retains_artifacts_on_publish_failure(
        &self,
        id: &SnapshotId,
    ) -> RepositoryResult<bool> {
        Ok(self
            .rows
            .lock()
            .expect("rows")
            .get(&id.to_string())
            .is_some_and(|record| record.committed.is_some()))
    }
}
