use std::sync::Arc;

use anyhow::Context;

use crate::runtime_snapshot::RunnableSnapshot;
use crate::snapshot::captured::{CapturedSandboxSnapshot, SnapshotArtifactAdvertiser};
use crate::snapshot::repository::backends::AssembledSnapshotBackend;
use crate::snapshot::repository::interfaces::{SnapshotRuntimeResolver, StagedSnapshot};
use crate::snapshot::repository::{CatalogReadScope, SnapshotAbsence, SnapshotRepository};
use crate::snapshot::repository::{RepositoryError, SnapshotListFilter, SnapshotListPage};
use crate::snapshot::{SnapshotId, SnapshotPublishMetadata, SnapshotPublishSource, SnapshotRecord};
use crate::types::FirecrackerSnapshotManifest;

/// Returns the source sandbox id, or `None` for template publication.
fn source_sandbox_id(source: &SnapshotPublishSource) -> Option<&str> {
    match source {
        SnapshotPublishSource::Template => None,
        SnapshotPublishSource::Sandbox { source_sandbox_id } => Some(source_sandbox_id.as_str()),
    }
}

fn describe_source(source_sandbox_id: Option<&str>) -> String {
    match source_sandbox_id {
        Some(id) => format!("sandbox {id}"),
        None => "a template build".to_string(),
    }
}

/// Pure staged value paired with node-local residue that never crosses processes.
pub struct StagedSnapshotHandle {
    staged: StagedSnapshot,
    local: LocalSnapshotStaging,
}

impl StagedSnapshotHandle {
    /// Adopts staging performed on another machine with no local artifact residue.
    fn adopted(staged: StagedSnapshot) -> Self {
        Self {
            staged,
            local: LocalSnapshotStaging {
                manifest: None,
                _capture: None,
            },
        }
    }

    /// The half that crosses a process boundary.
    pub fn staged(&self) -> &StagedSnapshot {
        &self.staged
    }

    pub fn into_parts(self) -> (StagedSnapshot, LocalSnapshotStaging) {
        (self.staged, self.local)
    }

    /// Whether this process holds the staged bytes.
    pub fn holds_local_bytes(&self) -> bool {
        self.local.manifest.is_some()
    }

    /// Drops the local half, keeping only what travels.
    ///
    /// Releases the capture's temporary directory, so anything still needing
    /// the files — the P2P advertisement — must already have run.
    pub fn into_staged(self) -> StagedSnapshot {
        self.staged
    }
}

/// Node-local residue retained only for post-commit artifact advertisement.
pub struct LocalSnapshotStaging {
    /// Manifest readable by this process, absent for remote staging.
    manifest: Option<FirecrackerSnapshotManifest>,
    /// Keeps a temporary artifact directory alive until dropped.
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
    /// Optional resolver; absent on processes that run no sandbox runtime.
    runtime_resolver: Option<Arc<dyn SnapshotRuntimeResolver>>,
    advertiser: Option<Arc<dyn SnapshotArtifactAdvertiser>>,
}

impl SnapshotManager {
    /// Builds from a backend assembled by the owning binary.
    pub fn from_assembled(
        assembled: AssembledSnapshotBackend,
        advertiser: Option<Arc<dyn SnapshotArtifactAdvertiser>>,
    ) -> Self {
        Self {
            repository: assembled.repository,
            runtime_resolver: assembled.runtime_resolver,
            advertiser,
        }
    }

    /// Builds a manager from the given components.
    ///
    /// `runtime_resolver` is `None` for a process that never turns a snapshot
    /// into local bytes; [`Self::resolve_runnable`] then refuses instead of
    /// resolving. See
    /// [`build_storage_for_role`][crate::snapshot::repository::backends]'s own
    /// doc for which roles that is.
    pub fn from_parts(
        repository: Arc<SnapshotRepository>,
        runtime_resolver: Option<Arc<dyn SnapshotRuntimeResolver>>,
        advertiser: Option<Arc<dyn SnapshotArtifactAdvertiser>>,
    ) -> Self {
        Self {
            repository,
            runtime_resolver,
            advertiser,
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
        let handle = self.stage(metadata, manifest).await?;
        self.commit_and_advertise(handle).await
    }

    #[tracing::instrument(skip(self, metadata), fields(snapshot_id = %metadata.id))]
    pub async fn publish_captured(
        &self,
        metadata: SnapshotPublishMetadata,
        captured_snapshot: CapturedSandboxSnapshot,
    ) -> crate::snapshot::RepositoryResult<SnapshotRecord> {
        let handle = self.stage_captured(metadata, captured_snapshot).await?;
        self.commit_and_advertise(handle).await
    }

    /// Stages a local capture or adopts a value already staged on another node.
    ///
    /// The returned handle owns any temporary local capture until later commit
    /// and advertisement complete.
    #[tracing::instrument(skip(self, metadata, captured_snapshot), fields(snapshot_id = %metadata.id))]
    pub async fn stage_captured(
        &self,
        metadata: SnapshotPublishMetadata,
        captured_snapshot: CapturedSandboxSnapshot,
    ) -> crate::snapshot::RepositoryResult<StagedSnapshotHandle> {
        let local_capture = match captured_snapshot {
            CapturedSandboxSnapshot::Staged(staged) => return self.adopt_staged(metadata, *staged),
            CapturedSandboxSnapshot::Local(local) => local,
        };

        let manifest = local_capture
            .publishable_manifest()
            .cloned()
            .ok_or_else(|| RepositoryError::Unsupported {
                feature: "publishing captured snapshots for this sandbox backend".to_string(),
            })?;

        let staged = self.repository.stage(metadata, manifest.clone()).await?;

        Ok(StagedSnapshotHandle {
            staged,
            local: LocalSnapshotStaging {
                manifest: Some(manifest),
                _capture: Some(CapturedSandboxSnapshot::Local(local_capture)),
            },
        })
    }

    /// Adopts remote staging after verifying it answers for the requested source sandbox.
    fn adopt_staged(
        &self,
        metadata: SnapshotPublishMetadata,
        mut staged: StagedSnapshot,
    ) -> crate::snapshot::RepositoryResult<StagedSnapshotHandle> {
        let asked = source_sandbox_id(&metadata.source);
        let answered = source_sandbox_id(&staged.commit.source);
        if asked != answered {
            return Err(RepositoryError::InvalidRequest {
                reason: format!(
                    "node {} staged a snapshot of {} for a capture of {}",
                    staged.origin_node_id,
                    describe_source(answered),
                    describe_source(asked),
                ),
            });
        }

        staged.commit.alias = metadata.alias;
        // The stager's record of the sandbox carries none of its user-facing
        // configuration; the committing caller's does, so its answer wins.
        if let Some(paused) = metadata.paused_sandbox {
            staged.commit.committed.paused_sandbox = Some(paused);
        }
        Ok(StagedSnapshotHandle::adopted(staged))
    }

    /// Stages built artifacts that need no capture guard.
    #[tracing::instrument(skip(self, metadata, manifest), fields(snapshot_id = %metadata.id))]
    pub async fn stage(
        &self,
        metadata: SnapshotPublishMetadata,
        manifest: FirecrackerSnapshotManifest,
    ) -> crate::snapshot::RepositoryResult<StagedSnapshotHandle> {
        let staged = self.repository.stage(metadata, manifest.clone()).await?;

        Ok(StagedSnapshotHandle {
            staged,
            local: LocalSnapshotStaging {
                manifest: Some(manifest),
                _capture: None,
            },
        })
    }

    /// Records where a resume of a paused sandbox landed on its row.
    pub async fn set_origin_node_id(
        &self,
        id: &crate::snapshot::SnapshotId,
        origin_node_id: &str,
    ) -> crate::snapshot::RepositoryResult<()> {
        self.repository
            .catalog()
            .set_origin_node_id(id, origin_node_id)
            .await
    }

    /// Commits a pure staged value without consulting node-local residue.
    #[tracing::instrument(skip(self, staged), fields(snapshot_id = %staged.commit.id))]
    pub async fn commit_staged(
        &self,
        staged: StagedSnapshot,
    ) -> crate::snapshot::RepositoryResult<SnapshotRecord> {
        self.repository.commit_staged(staged).await
    }

    /// Advertises committed bytes only when this process holds their local manifest.
    pub async fn advertise_committed(&self, record: &SnapshotRecord, local: LocalSnapshotStaging) {
        let Some(manifest) = local.manifest.as_ref() else {
            // Remote staging has no local files this process can advertise.
            return;
        };
        let Some(advertiser) = self.advertiser.as_ref() else {
            return;
        };
        advertiser.advertise(record, manifest).await;
    }

    /// Stages, commits, then advertises in the process holding local bytes.
    pub async fn commit_and_advertise(
        &self,
        handle: StagedSnapshotHandle,
    ) -> crate::snapshot::RepositoryResult<SnapshotRecord> {
        let (staged, local) = handle.into_parts();
        let record = self.commit_staged(staged).await?;
        self.advertise_committed(&record, local).await;
        Ok(record)
    }

    /// Loads a snapshot record by id or alias.
    ///
    /// Resolvable rows only: this is the read a launch reaches. A caller that
    /// is asking *about* a snapshot rather than in order to run one — every
    /// endpoint under `/templates` — wants [`Self::get_scoped`].
    pub async fn get(
        &self,
        id_or_alias: impl AsRef<str>,
    ) -> anyhow::Result<Option<SnapshotRecord>> {
        self.get_scoped(id_or_alias, CatalogReadScope::Resolvable)
            .await
    }

    /// [`Self::get`] at an explicitly chosen scope.
    pub async fn get_scoped(
        &self,
        id_or_alias: impl AsRef<str>,
        scope: CatalogReadScope,
    ) -> anyhow::Result<Option<SnapshotRecord>> {
        self.repository
            .get_scoped(id_or_alias.as_ref(), scope)
            .await
            .with_context(|| {
                format!(
                    "load committed snapshot '{}' through repository",
                    id_or_alias.as_ref()
                )
            })
    }

    /// Determines whether absence is settled enough for destructive action.
    pub async fn absence_of(&self, id: &SnapshotId) -> anyhow::Result<SnapshotAbsence> {
        self.repository
            .absence_of(id)
            .await
            .with_context(|| format!("settle whether snapshot '{id}' is really gone"))
    }

    /// Lists one bounded page of resolvable snapshots, newest first.
    pub async fn list_page(&self, filter: SnapshotListFilter) -> anyhow::Result<SnapshotListPage> {
        self.list_page_scoped(filter, CatalogReadScope::Resolvable)
            .await
    }

    /// [`Self::list_page`] at an explicitly chosen scope.
    pub async fn list_page_scoped(
        &self,
        filter: SnapshotListFilter,
        scope: CatalogReadScope,
    ) -> anyhow::Result<SnapshotListPage> {
        self.repository
            .list_page_scoped(filter, scope)
            .await
            .context("list one page of committed snapshots through repository")
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
        self.resolve_alias_scoped(alias, CatalogReadScope::Resolvable)
            .await
    }

    /// [`Self::resolve_committed_alias`] at an explicitly chosen scope.
    pub async fn resolve_alias_scoped(
        &self,
        alias: &str,
        scope: CatalogReadScope,
    ) -> anyhow::Result<Option<SnapshotId>> {
        self.repository
            .resolve_alias_scoped(alias, scope)
            .await
            .with_context(|| {
                format!("resolve committed snapshot alias '{alias}' through repository")
            })
    }

    /// Resolves a committed snapshot into node-local runnable artifact paths.
    /// Returns [`RepositoryError::Unsupported`] when no runtime resolver is configured;
    /// `aenv-api` is intentionally assembled without one.
    pub async fn resolve_runnable(
        &self,
        snapshot: SnapshotRecord,
    ) -> anyhow::Result<RunnableSnapshot> {
        let Some(runtime_resolver) = self.runtime_resolver.as_ref() else {
            return Err(anyhow::Error::new(RepositoryError::Unsupported {
                feature: format!(
                    "resolve snapshot '{}' into runnable runtime paths: this process was \
                     assembled without a snapshot runtime resolver, because it runs no sandbox \
                     runtime",
                    snapshot.id
                ),
            }));
        };
        runtime_resolver
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
    ) -> crate::snapshot::RepositoryResult<crate::snapshot::repository::StartedBuild> {
        self.repository.try_start_build(id).await
    }

    /// Renews build ownership; `false` means the builder must stop.
    pub async fn renew_build_lease(
        &self,
        build_id: &SnapshotId,
    ) -> crate::snapshot::RepositoryResult<bool> {
        self.repository.renew_build_lease(build_id).await
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
