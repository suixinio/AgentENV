use std::sync::Arc;

use anyhow::Context;

use crate::snapshot::captured::{CapturedSandboxSnapshot, SnapshotArtifactAdvertiser};
use crate::snapshot::repository::backends::AssembledSnapshotBackend;
use crate::snapshot::repository::interfaces::{SnapshotRuntimeResolver, StagedSnapshot};
use crate::snapshot::repository::{CatalogReadScope, SnapshotAbsence, SnapshotRepository};
use crate::snapshot::repository::{RepositoryError, SnapshotListFilter, SnapshotListPage};
use crate::snapshot::{
    RunnableSnapshot, SnapshotId, SnapshotPublishMetadata, SnapshotPublishSource, SnapshotRecord,
};
use crate::types::{ExecutionId, FirecrackerSnapshotManifest};

/// The sandbox a publication says it came from, or `None` for a template
/// build.
///
/// 🔴 A borrow of the id and not the whole enum, because the comparison is
/// between two values built by two different processes from two different
/// records, and `SnapshotPublishSource` has no `PartialEq` for exactly that
/// kind of reason — adding one would invite comparisons of variants whose
/// equality means nothing here.
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
    /// A staging that ran on another machine.
    ///
    /// 🔴 The local half is deliberately empty rather than reconstructed. The
    /// bytes are on the node that staged them and this process has never seen
    /// them, so there is no manifest to hold and no temporary directory to keep
    /// alive — and anything that later reaches for one has to find nothing,
    /// not a plausible-looking value pointing at paths that do not exist here.
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

    /// Whether the bytes behind this staging are on this machine.
    ///
    /// 🔴 Asked of the handle rather than inferred from what the advertisement
    /// did. "Nothing was advertised" is the same observation whether the
    /// advertisement had nothing to offer or had something and failed to read
    /// it, and only the first is a property of the staging.
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

/// The residue a `stage` leaves on the node it ran on.
///
/// Only one thing consumes it: the post-commit P2P advertisement, which reads
/// the manifest's local paths. It is not serialisable and must not become so —
/// the manifest's paths are all `#[serde(skip)]`, so a serialised copy would
/// arrive somewhere else looking complete and pointing at nothing.
pub struct LocalSnapshotStaging {
    /// `None` when the staging ran on another machine.
    ///
    /// 🔴 Not "no artifacts": every staged snapshot has a manifest, and the one
    /// for a remote staging is on the node that wrote it. `None` means *this
    /// process cannot read the files the manifest names*, which is the only
    /// thing the one consumer — the P2P advertisement — actually needs to know.
    manifest: Option<FirecrackerSnapshotManifest>,
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
    /// 🔴 `None` on a role that runs no sandbox runtime — see
    /// [`AssembledSnapshotBackend::runtime_resolver`][crate::snapshot::repository::backends::AssembledSnapshotBackend].
    /// [`Self::resolve_runnable`] is the only reader, and it refuses rather
    /// than unwrapping.
    runtime_resolver: Option<Arc<dyn SnapshotRuntimeResolver>>,
    advertiser: Option<Arc<dyn SnapshotArtifactAdvertiser>>,
    /// Held, not used. The replay of owed object-store writes stops when the
    /// last handle is dropped, so it lives as long as the manager does.
    _mirror_compensator: Option<Arc<crate::snapshot::repository::mirror::MirrorCompensator>>,
}

impl SnapshotManager {
    /// Builds a manager over an already-assembled backend.
    ///
    /// # 🔴 Assembly happens in the process, not here
    ///
    /// This used to be `SnapshotManager::new`, which called
    /// [`build_snapshot_backend`] itself and so needed everything that
    /// function needs — including an `Option<sqlx::PgPool>` threaded through
    /// a type that has no business holding one. Each binary now assembles the
    /// backend its own half is allowed to build and hands the result here:
    /// `aenv-node` supplies the byte half and no central catalog, `aenv-api`
    /// supplies the catalog-only repository and, when `[pg]` is configured,
    /// the PostgreSQL parts.
    pub fn from_assembled(
        assembled: AssembledSnapshotBackend,
        advertiser: Option<Arc<dyn SnapshotArtifactAdvertiser>>,
    ) -> Self {
        Self {
            repository: assembled.repository,
            runtime_resolver: assembled.runtime_resolver,
            advertiser,
            _mirror_compensator: assembled.mirror_compensator,
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
            _mirror_compensator: None,
        }
    }

    pub async fn create(
        &self,
        record: SnapshotRecord,
    ) -> crate::snapshot::RepositoryResult<SnapshotRecord> {
        self.repository.create(record).await
    }

    /// Boundedly closes any durable local store this manager's repository
    /// owns, ahead of process shutdown.
    ///
    /// 🔴 A no-op for every repository configuration except the dual-write
    /// catalog's durable mirror backlog (`SnapshotCatalog::close`'s default is
    /// a no-op; only `DualWriteCatalog` overrides it) — most deployments have
    /// nothing here to close, and this call is safe and cheap regardless. See
    /// `crate::local_store::LocalKvStore::close` for the mechanism this
    /// exists to bound.
    pub async fn close_stores(&self, timeout: std::time::Duration) {
        self.repository.catalog().close(timeout).await;
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
    ///
    /// # 🔴 Two shapes of capture arrive here, and only one of them still needs
    /// staging
    ///
    /// A capture produced by a sandbox running in *this* process is artifacts in
    /// a temporary directory: nothing about it is durable yet, and `metadata`
    /// decides where the bytes go — `metadata.id` most of all, because it names
    /// the directory they are written into.
    ///
    /// A capture that arrived from the node holding the sandbox has already been
    /// staged **there**. Its bytes are durable, under an id that node chose,
    /// because the id *is* the directory they are already sitting in. There is
    /// nothing left for `metadata.id` to decide and re-staging is not available
    /// to this process anyway: it has no files to read.
    ///
    /// What the two have in common is the *name*. Staging never reads
    /// `metadata.alias` — `SnapshotArtifactStore::import_built_artifacts` takes
    /// the whole of `metadata` and touches only `id` — so the alias is settled
    /// by the commit either way, and this half imposing it on an adopted row is
    /// not overriding a decision the staging node made. It is supplying one the
    /// staging node was never asked for.
    ///
    /// # 🔴 Which id won is never hidden
    ///
    /// For an adopted staging, `metadata.id` is *not* the id of the snapshot
    /// this returns. Callers report `record.id` from the commit rather than the
    /// id they proposed, which is the only reading that is true on both arms.
    #[tracing::instrument(skip(self, metadata, captured_snapshot), fields(snapshot_id = %metadata.id))]
    pub async fn stage_captured(
        &self,
        metadata: SnapshotPublishMetadata,
        captured_snapshot: CapturedSandboxSnapshot,
        execution_id: Option<ExecutionId>,
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

        let staged = self
            .repository
            .stage(metadata, manifest.clone(), execution_id)
            .await?;

        Ok(StagedSnapshotHandle {
            staged,
            local: LocalSnapshotStaging {
                manifest: Some(manifest),
                _capture: Some(CapturedSandboxSnapshot::Local(local_capture)),
            },
        })
    }

    /// Takes over a staging performed by the node that holds the bytes.
    ///
    /// 🔴 One field is checked and the rest are not, and the asymmetry is the
    /// point. Everything in `metadata` except `source` describes the machine
    /// that ran the VM — its kernel, its Firecracker, the image configs it
    /// resolved — and this half has no second opinion about any of it worth
    /// preferring; the staging node's own record is the one that saw the
    /// capture happen. `source` is different: it is the sandbox this half asked
    /// about *by name*, and a staged row naming a different one is an answer to
    /// a question nobody asked. Committing it would file one sandbox's snapshot
    /// under another's provenance, which no later read can tell from the truth.
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
        Ok(StagedSnapshotHandle::adopted(staged))
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
                manifest: Some(manifest),
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
        let Some(manifest) = local.manifest.as_ref() else {
            // 🔴 Staged on another machine, so there is nothing here to offer.
            // Advertising it anyway would announce this node as a source for
            // bytes it has never held, and a peer that took the hint would get
            // a miss it had been told was a hit. The node that *does* hold them
            // advertises its own overlaybd layers through the facade in
            // `src/overlaybd/p2p/`, which is where a remote staging's bytes are
            // reachable from.
            return;
        };
        let Some(advertiser) = self.advertiser.as_ref() else {
            return;
        };
        advertiser.advertise(record, manifest).await;
    }

    /// stage -> commit -> advertise, in the one process that can do all three.
    pub(crate) async fn commit_and_advertise(
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

    /// Whether a snapshot's absence is the last word on it.
    ///
    /// 🔴 Not [`Self::get_scoped`] answering `None`. That is one store's answer
    /// at one scope; this is whether anything the node can consult still holds
    /// the snapshot or still owes a write that would produce it. Ask it only
    /// where absence is about to destroy something —
    /// [`SnapshotCatalog::absence_of`](crate::snapshot::repository::SnapshotCatalog::absence_of)
    /// says why the two questions are not the same one.
    pub async fn absence_of(&self, id: &SnapshotId) -> anyhow::Result<SnapshotAbsence> {
        self.repository
            .absence_of(id)
            .await
            .with_context(|| format!("settle whether snapshot '{id}' is really gone"))
    }

    /// Lists every snapshot record that matches the given filter.
    pub async fn list(&self, filter: SnapshotListFilter) -> anyhow::Result<Vec<SnapshotRecord>> {
        self.repository
            .list(filter)
            .await
            .context("list committed snapshots through repository")
    }

    /// Lists one page of snapshot records, newest first.
    ///
    /// What every listing endpoint calls: the page bounds ride on the filter so
    /// that a catalog able to push them into its storage does, and one that
    /// cannot still answers the same page.
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
    ///
    /// 🔴 Refuses, rather than panicking, when this process was assembled
    /// without a runtime resolver. That is not a defensive `unwrap` dressed up:
    /// `--role api` is assembled that way on purpose (see
    /// [`build_storage_for_role`][crate::snapshot::repository::backends]) and
    /// every one of its callers already forks on
    /// `ServerRole::runs_sandbox_runtime` and ships the catalog row to a node
    /// instead. A typed [`RepositoryError::Unsupported`] is what a future
    /// caller that forgets the fork gets back — a 5xx with a legible reason,
    /// on one request, rather than the whole api process aborting.
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

    /// Says this node is still running `build_id`.
    ///
    /// 🔴 `false` means the template has been handed to somebody else and this
    /// build must stop. See [`SnapshotCatalog::renew_build_lease`].
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

#[cfg(test)]
pub(crate) mod tests {
    use super::super::captured::CallerOwnedArtifacts;
    use super::*;
    use crate::snapshot::mock::write_mock_built_artifacts;
    use crate::snapshot::repository::backends::{PosixFsBackend, PosixFsBackendConfig};
    use crate::snapshot::repository::StagedSnapshot;
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
        SnapshotManager::from_parts(repository, Some(runtime_resolver), None)
    }

    /// 🔴 A manager assembled without a runtime resolver — which is what
    /// `--role api` gets — must *refuse* a resolve, not abort the process.
    ///
    /// The refusal is typed: `RepositoryError::Unsupported`, downcastable, so
    /// a caller that forgot to fork on `ServerRole::runs_sandbox_runtime` gets
    /// a legible 5xx on one request instead of taking every in-flight request
    /// down with it.
    ///
    /// The control is the second half: the *same* record through a manager
    /// that does hold a resolver reaches the resolver and fails on the missing
    /// artifacts instead. Without it, "returns an error" would be satisfied by
    /// a manager that can never resolve anything at all.
    #[tokio::test]
    async fn a_manager_with_no_runtime_resolver_refuses_rather_than_panicking() {
        let record = SnapshotRecord::mock_ready(crate::snapshot::CommittedSnapshot::mock());

        let tempdir = TempDir::new().expect("tempdir should exist");
        let repository = {
            let backend = PosixFsBackend::new(PosixFsBackendConfig {
                root: tempdir.path().join("repository"),
                cache_root: Some(tempdir.path().join("runtime-cache")),
                runtime_cache_root: Some(tempdir.path().join("runtime-cache").join("runtime")),
            })
            .expect("posix backend");
            let (repository, _) = backend.into_parts();
            repository
        };

        let without = SnapshotManager::from_parts(Arc::clone(&repository), None, None);
        let err = without
            .resolve_runnable(record.clone())
            .await
            .expect_err("a manager with no runtime resolver must refuse to resolve");
        let typed = err
            .downcast_ref::<crate::snapshot::RepositoryError>()
            .unwrap_or_else(|| panic!("the refusal must be a typed RepositoryError: {err:?}"));
        assert!(
            matches!(typed, crate::snapshot::RepositoryError::Unsupported { .. }),
            "the refusal must say the operation is unsupported here, not that something \
             went wrong reaching it: {typed:?}"
        );

        // The control: the same record, through a manager that *does* hold a
        // resolver, gets past the gate and fails on the artifacts instead.
        let with = test_manager(tempdir.path());
        let err = with
            .resolve_runnable(record)
            .await
            .expect_err("nothing was ever published, so resolving must fail");
        let typed = err
            .downcast_ref::<crate::snapshot::RepositoryError>()
            .unwrap_or_else(|| panic!("the resolver's own failure is typed too: {err:?}"));
        assert!(
            !matches!(typed, crate::snapshot::RepositoryError::Unsupported { .. }),
            "a manager that holds a resolver reported the no-resolver refusal, so the \
             assertion above proves nothing: {typed:?}"
        );
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

    /// Builds a row staged somewhere this process cannot read.
    pub(crate) fn staged_elsewhere(id: SnapshotId, source_sandbox_id: &str) -> StagedSnapshot {
        StagedSnapshot {
            commit: crate::snapshot::repository::SnapshotCommit {
                id,
                // 🔴 Unnamed, always. A staging node is never told the alias —
                // staging does not read one — so a test that seeded one here
                // would be proving the committer *kept* a name rather than
                // that it supplied one.
                alias: None,
                source: SnapshotPublishSource::Sandbox {
                    source_sandbox_id: source_sandbox_id.to_string(),
                },
                resources: Default::default(),
                created_at_unix_ms: Some(1_700_000_000_000),
                committed: crate::snapshot::CommittedSnapshot::mock(),
            },
            staged_at_unix_ms: 1_700_000_000_000,
            origin_node_id: "the-node-holding-the-bytes".to_string(),
            execution_id: None,
        }
    }

    pub(crate) fn capture_of(source_sandbox_id: &str, id: SnapshotId) -> SnapshotPublishMetadata {
        SnapshotPublishMetadata {
            id,
            source: SnapshotPublishSource::Sandbox {
                source_sandbox_id: source_sandbox_id.to_string(),
            },
            ..SnapshotPublishMetadata::mock()
        }
    }

    /// A capture that arrived already staged is committed where it lies, and
    /// the row it produces is the staging node's identity wearing this half's
    /// name.
    ///
    /// 🔴 Both halves run in this one test, against the same repository, and
    /// each is the other's control. The local publish is what makes "no
    /// directory was written for the adopted id" mean something: the same
    /// assertion against the same directory finds the locally staged snapshot's
    /// bytes sitting there. Without it, a `stage_captured` that had silently
    /// stopped writing anything at all would pass.
    #[tokio::test]
    async fn a_capture_staged_elsewhere_is_committed_rather_than_staged_again() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let manager = test_manager(tempdir.path());
        let snapshots = tempdir.path().join("repository").join("snapshots");
        let sandbox = "the-sandbox-both-halves-are-talking-about";

        // The half that stages here: real artifacts, and the bytes land under
        // the id this process chose.
        let workspace = TempDir::new().expect("tempdir should exist");
        let (_, _, manifest) =
            write_mock_built_artifacts(workspace.path()).expect("mock artifacts should write");
        let local_id = SnapshotId::generate();
        let local = manager
            .publish_captured(
                capture_of(sandbox, local_id.clone()),
                CapturedSandboxSnapshot::local(CallerOwnedArtifacts::new(manifest)),
            )
            .await
            .expect("a local capture should publish");
        assert_eq!(local.id, local_id, "a local capture must keep its own id");

        // The other half: a row somebody else staged, offered under an id this
        // process minted and an alias only this process knows about.
        let staged_id = SnapshotId::generate();
        let proposed_id = SnapshotId::generate();
        let mut proposal = capture_of(sandbox, proposed_id.clone());
        proposal.alias = Some(SnapshotAlias::parse("the-name-the-user-asked-for").expect("alias"));
        let adopted = manager
            .publish_captured(
                proposal,
                CapturedSandboxSnapshot::staged(staged_elsewhere(staged_id.clone(), sandbox)),
            )
            .await
            .expect("an adopted staging should commit");

        assert_eq!(
            adopted.id, staged_id,
            "the row must carry the id the bytes were written under"
        );
        assert_ne!(
            adopted.id, proposed_id,
            "this half's proposed id must not win: no bytes were ever written under it"
        );
        assert_eq!(
            adopted.alias.as_ref().map(ToString::to_string).as_deref(),
            Some("the-name-the-user-asked-for"),
            "the alias is the committer's and staging never had it"
        );

        // 🔴 The pair that carries the whole claim: no *artifact* was written
        // here for the adopted snapshot, and the identical look at the locally
        // staged one finds its bytes. The directory itself is not the
        // assertion — committing a row creates one either way, to hold the
        // record — so `vm_state.bin` is what separates "the row was announced"
        // from "the bytes were written here".
        assert!(
            !snapshots
                .join(staged_id.to_string())
                .join("vm_state.bin")
                .exists(),
            "adopting a staging must not write bytes this process does not have"
        );
        assert!(
            snapshots
                .join(local_id.to_string())
                .join("vm_state.bin")
                .exists(),
            "the locally staged capture's bytes are missing, so the assertion above proves nothing"
        );

        // Both rows are announced, which is what makes the commit a commit.
        assert!(manager
            .get(staged_id.to_string())
            .await
            .expect("get should work")
            .is_some());
        assert!(manager
            .get(local_id.to_string())
            .await
            .expect("get should work")
            .is_some());
        assert!(manager
            .resolve_committed_alias("the-name-the-user-asked-for")
            .await
            .expect("resolve should work")
            .is_some());
    }

    /// A staging that names a different sandbox is refused, and refused before
    /// anything is announced.
    ///
    /// 🔴 The control is the same call with the sandbox ids agreeing. Without
    /// it "the row was not committed" is satisfied by an `adopt_staged` that
    /// refuses everything, which is the exact failure that would make the
    /// published arm silently useless.
    #[tokio::test]
    async fn a_staging_of_a_different_sandbox_is_refused_and_nothing_is_announced() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let manager = test_manager(tempdir.path());

        let mismatched_id = SnapshotId::generate();
        let err = manager
            .publish_captured(
                capture_of("the-sandbox-this-half-asked-about", SnapshotId::generate()),
                CapturedSandboxSnapshot::staged(staged_elsewhere(
                    mismatched_id.clone(),
                    "some-other-sandbox-entirely",
                )),
            )
            .await
            .expect_err("a staging of another sandbox must be refused");
        assert!(
            matches!(err, RepositoryError::InvalidRequest { .. }),
            "expected an invalid-request refusal, got {err:?}"
        );
        assert!(
            manager
                .get(mismatched_id.to_string())
                .await
                .expect("get should work")
                .is_none(),
            "the refused staging was announced anyway"
        );

        // The control face: the same call, sandbox ids agreeing, commits.
        let matching_id = SnapshotId::generate();
        let record = manager
            .publish_captured(
                capture_of("the-sandbox-this-half-asked-about", SnapshotId::generate()),
                CapturedSandboxSnapshot::staged(staged_elsewhere(
                    matching_id.clone(),
                    "the-sandbox-this-half-asked-about",
                )),
            )
            .await
            .expect("a staging of the sandbox that was asked about should commit");
        assert_eq!(record.id, matching_id);
        assert!(manager
            .get(matching_id.to_string())
            .await
            .expect("get should work")
            .is_some());
    }
}
