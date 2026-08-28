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
    PersistedDiskImagePublication, SnapshotId, SnapshotManager, SnapshotPublishMetadata,
    SnapshotRecord, SNAPSHOT_ARTIFACT_LAYOUT,
};
use crate::runtime_snapshot::RunnableSnapshot;
use crate::types::FirecrackerSnapshotManifest;

/// Test double for catalog interactions that should stay unreachable.
///
/// `get_calls` — see [`Self::get_calls`] — is what
/// `node_server::tests`'s fallback/skip pairs assert on rather than matching
/// on this catalog's and [`MockSnapshotRuntimeResolver`]'s differently-worded
/// refusals: a string match cannot tell "the catalog was consulted and
/// refused" from "the catalog was never asked" once the wording changes (for
/// instance, when `aenv-node` stops holding a catalog at all and the
/// refusal becomes something like "no catalog access on `aenv-node`" —
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
        Some(Arc::new(MockSnapshotRuntimeResolver)),
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
        Some(Arc::new(MockSnapshotRuntimeResolver)),
        None,
    );
    (manager, catalog)
}

/// A catalog that answers every read with one committed row, and refuses
/// every write.
///
/// 🔴 Exists to be paired with [`MockSnapshotRuntimeResolver`] by
/// [`unresolvable_snapshot_manager`], and the pairing is the whole point: the
/// catalog says yes, the resolver says no. Over that manager, a flow that
/// completes is a flow that never resolved anything — not because a string in
/// an error message says so, but because resolving would have returned `Err`
/// and the flow would have failed. See
/// `MockSnapshotRuntimeResolver::resolve`.
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

    async fn list(&self, _filter: SnapshotListFilter) -> RepositoryResult<Vec<SnapshotRecord>> {
        Ok(vec![self.row.clone()])
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

/// A snapshot manager whose catalog holds `row` and whose runtime resolver
/// refuses every call.
///
/// This is the shape of `aenv-api`'s world once it stops resolving: it can
/// read the catalog, and it has no business turning a row into local bytes.
///
/// 🔴 A resolver that refuses, deliberately, rather than the `None` a real
/// `aenv-api` is now assembled with (see `build_storage_for_role`). This
/// fixture is handed to `aenv-node` in the same tests, and
/// for those two the refusal is the *positive* control: they must fail exactly
/// here, which is what proves they resolved rather than shipped the row. A
/// `None` would make both roles fail with the same message and the fork would
/// stop being observable.
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

/// A whole snapshot catalog, in memory.
///
/// # 🔴 Why this exists
///
/// PostgreSQL is the only snapshot catalog there is, and it lives in
/// `aenv-api`. A test that wants to exercise the *byte* half — the POSIX and
/// OSS artifact stores, the runtime resolver, `SnapshotManager`'s
/// stage/commit/delete flow — still needs a catalog behind it to publish
/// through, and it cannot use the real one: `aenv-node` links no `sqlx`, and
/// half of these tests live in that crate.
///
/// Until the object-storage catalog was deleted those tests used
/// `PosixFsCatalogStore` as their catalog, incidentally, because it happened to
/// sit in the same backend. This is the deliberate replacement, and its
/// semantics are that store's: the alias rules, the `AliasConflict` a rebind
/// over a live row produces, the build-status transitions, and
/// `retains_artifacts_on_publish_failure` returning true for an id that is
/// already committed — which is what stops a failed re-publish from deleting a
/// live snapshot's bytes.
///
/// Not a production type and not a substitute for one: no durability, no
/// locking beyond one `Mutex`, no cluster.
#[derive(Debug, Default)]
pub struct InMemorySnapshotCatalog {
    rows: std::sync::Mutex<std::collections::HashMap<String, SnapshotRecord>>,
    aliases: std::sync::Mutex<std::collections::HashMap<String, SnapshotId>>,
}

impl InMemorySnapshotCatalog {
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

    /// The alias rule both writes share: rebinding a name a *live* row holds is
    /// a conflict; rebinding one whose row is gone is a stale entry to clear.
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

    async fn list(&self, filter: SnapshotListFilter) -> RepositoryResult<Vec<SnapshotRecord>> {
        let mut records: Vec<SnapshotRecord> = self
            .rows
            .lock()
            .expect("rows")
            .values()
            .filter(|record| Self::matches(record, &filter))
            .cloned()
            .collect();
        records.sort_by(|left, right| {
            right
                .created_at_unix_ms
                .cmp(&left.created_at_unix_ms)
                .then_with(|| left.id.to_string().cmp(&right.id.to_string()))
        });
        Ok(records)
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

    /// 🔴 True for an id that is already committed, matching the POSIX catalog
    /// this replaced: a failed re-publish over a live snapshot must not take
    /// that snapshot's bytes with it.
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
