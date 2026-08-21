//! The one place snapshot rows and snapshot bytes are sequenced.
//!
//! [`SnapshotRepository`] used to be a trait each backend implemented end to
//! end, which is why the OSS backend's `publish()` ran from exporting a disk
//! image all the way to writing a catalog row without a seam anywhere in the
//! middle. It is now a composition over [`SnapshotCatalog`] and
//! [`SnapshotArtifactStore`]: it owns no storage, and the only thing it knows
//! how to do is put the two halves in the right order.
//!
//! That order is the point. Bytes first, row second, always:
//!
//! ```text
//! stage(..)          artifacts.import_built_artifacts(..)  // bytes
//!                    StagedSnapshot { .. }                 // pure, travels
//! commit_staged(..)  catalog.publish_commit(..)            // the flip
//! ```
//!
//! `publish` is the two of them run back to back, and it is only a convenience:
//! the seam between them is real. A `StagedSnapshot` can be serialised, sent to
//! another process, and committed there, because `commit_staged` is handed a
//! value and never asks the artifact store or the filesystem anything.
//!
//! Because the catalog is reached through `Arc<dyn SnapshotCatalog>`, the row
//! half can be replaced — by a remote client, or by a wrapper that writes to
//! two catalogs at once — without the byte half being rebuilt or even
//! recompiled against a different type.

use std::collections::HashSet;
use std::sync::Arc;

use crate::sandbox::FirecrackerSnapshotManifest;
use crate::snapshot::repository::interfaces::{
    SnapshotArtifactStore, SnapshotCatalog, SnapshotCommit, SnapshotListFilter, SnapshotListPage,
    StagedSnapshot, StartedBuild,
};
use crate::snapshot::repository::{RepositoryError, RepositoryResult};
use crate::snapshot::types::{
    PersistedDiskImagePublication, SnapshotId, SnapshotPublishMetadata, SnapshotRecord,
    TemplateBuildErrorReason,
};
use crate::types::ExecutionId;

/// Durable snapshot repository: a [`SnapshotCatalog`] and a
/// [`SnapshotArtifactStore`], sequenced.
///
/// Callers may assume returned records describe durable repository state rather
/// than process-local working directories or node-local runtime cache files.
/// The repository boundary intentionally excludes node-local derived state:
///
/// - local build-artifact allocation is owned by the manager
/// - runtime-ready `image.json` files are materialized by
///   [`crate::snapshot::repository::SnapshotRuntimeResolver`]
/// - node-local cache directories do not leak back into snapshot records
pub struct SnapshotRepository {
    catalog: Arc<dyn SnapshotCatalog>,
    artifacts: Arc<dyn SnapshotArtifactStore>,
    /// The machine `stage` writes bytes onto, stamped into every
    /// [`StagedSnapshot`] so a remote committer knows where they landed.
    origin_node_id: String,
}

impl SnapshotRepository {
    pub fn new(
        catalog: Arc<dyn SnapshotCatalog>,
        artifacts: Arc<dyn SnapshotArtifactStore>,
    ) -> Self {
        Self::on_node(catalog, artifacts, crate::identity::local_node_id())
    }

    /// [`Self::new`] with the staging node named explicitly.
    pub fn on_node(
        catalog: Arc<dyn SnapshotCatalog>,
        artifacts: Arc<dyn SnapshotArtifactStore>,
        origin_node_id: String,
    ) -> Self {
        Self {
            catalog,
            artifacts,
            origin_node_id,
        }
    }

    /// The row half on its own, for callers that only read the catalog.
    pub fn catalog(&self) -> Arc<dyn SnapshotCatalog> {
        Arc::clone(&self.catalog)
    }

    /// The byte half on its own.
    pub fn artifacts(&self) -> Arc<dyn SnapshotArtifactStore> {
        Arc::clone(&self.artifacts)
    }

    /// Creates a durable template snapshot record before build artifacts exist.
    pub async fn create(&self, record: SnapshotRecord) -> RepositoryResult<SnapshotRecord> {
        self.catalog.create(record).await
    }

    /// Publishes manager-owned local artifacts into committed repository state:
    /// bytes first, then the row that makes them findable.
    ///
    /// On failure the artifacts are rolled back unless the catalog says an
    /// earlier commit still owns them, and any external registry publications
    /// made along the way are rolled back with them. Content-addressed managed
    /// layers are deliberately left in place — they are shared across snapshots
    /// and need separate GC.
    pub async fn publish(
        &self,
        metadata: SnapshotPublishMetadata,
        manifest: FirecrackerSnapshotManifest,
    ) -> RepositoryResult<SnapshotRecord> {
        let staged = self.stage(metadata, manifest, None).await?;
        self.commit_staged(staged).await
    }

    /// Writes one snapshot's bytes and stops.
    ///
    /// 🔴 The half of `publish` that has to run where the sandbox is. It
    /// establishes everything the row needs — every artifact reference, the
    /// resources, the alias to bind — and announces none of it: until
    /// [`Self::commit_staged`] runs, no reader can resolve this snapshot, and
    /// nothing outside this process knows the bytes exist.
    ///
    /// On failure the artifacts are rolled back exactly as they were before the
    /// split, including the registry publications a partial import had already
    /// made.
    ///
    /// `execution_id` is recorded, never checked. See [`StagedSnapshot`].
    pub async fn stage(
        &self,
        metadata: SnapshotPublishMetadata,
        manifest: FirecrackerSnapshotManifest,
        execution_id: Option<ExecutionId>,
    ) -> RepositoryResult<StagedSnapshot> {
        validate_attached_drives(&manifest)?;

        // Held outside the fallible block so a rollback can still see the
        // publications a partial import already made.
        let mut publications: Vec<PersistedDiskImagePublication> = Vec::new();
        let imported = self
            .artifacts
            .import_built_artifacts(&metadata, &manifest, &mut publications)
            .await;

        match imported {
            // 🔴 One clock read, used twice. The instant the bytes were staged
            // *is* the instant the snapshot came into being, and it is the
            // value both catalogs must record — so it is decided once, here,
            // rather than by whichever store's `now` runs first.
            Ok(imported) => {
                let staged_at_unix_ms = now_unix_ms();
                Ok(StagedSnapshot {
                    commit: SnapshotCommit::new(&metadata, imported, staged_at_unix_ms),
                    staged_at_unix_ms,
                    origin_node_id: self.origin_node_id.clone(),
                    execution_id,
                })
            }
            Err(error) => {
                self.roll_back_publish(&metadata.id, &publications).await;
                Err(error)
            }
        }
    }

    /// Announces a staged snapshot: the flip, and the only thing that performs
    /// it.
    ///
    /// 🔴 Takes a value and nothing else. It cannot open a local file, cannot
    /// consult the artifact store about what it wrote, and cannot tell whether
    /// the bytes are on this machine — which is what makes it answerable by a
    /// process that never saw them. A `commit_staged` that reached back for
    /// anything local would compile today and fail the day the two halves are
    /// separated, which is the failure the whole seam exists to make
    /// impossible.
    ///
    /// On failure the artifacts are rolled back, unless the catalog says an
    /// earlier commit still owns them.
    pub async fn commit_staged(&self, staged: StagedSnapshot) -> RepositoryResult<SnapshotRecord> {
        let id = staged.commit.id.clone();
        // The publications are inside the payload by now: a commit that fails
        // has to roll back the same registry references the import made, and
        // the staged value is the only description of them it has.
        let publications = staged.commit.committed.disk_publications.clone();

        match self.catalog.publish_commit(staged.commit).await {
            Ok(record) => Ok(record),
            Err(error) => {
                self.roll_back_publish(&id, &publications).await;
                Err(error)
            }
        }
    }

    /// Loads one snapshot record by repository id or alias.
    pub async fn get(&self, id_or_alias: &str) -> RepositoryResult<Option<SnapshotRecord>> {
        self.catalog.get(id_or_alias).await
    }

    /// Lists every snapshot record matching the provided filter.
    pub async fn list(&self, filter: SnapshotListFilter) -> RepositoryResult<Vec<SnapshotRecord>> {
        self.catalog.list(filter).await
    }

    /// Lists one page of snapshot records, newest first.
    pub async fn list_page(
        &self,
        filter: SnapshotListFilter,
    ) -> RepositoryResult<SnapshotListPage> {
        self.catalog.list_page(filter).await
    }

    /// Deletes one snapshot by id or alias. Idempotent.
    ///
    /// The row goes first so no reader can resolve a snapshot whose bytes are
    /// already being removed; the artifacts follow on a best-effort basis.
    pub async fn delete(&self, id_or_alias: &str) -> RepositoryResult<()> {
        let Some(record) = self.catalog.get(id_or_alias).await? else {
            return Ok(());
        };
        self.catalog.delete_record(&record).await?;
        let publications = record
            .committed
            .as_ref()
            .map(|committed| committed.disk_publications.as_slice())
            .unwrap_or_default();
        self.artifacts
            .delete_artifacts(&record.id, publications)
            .await;
        Ok(())
    }

    /// Resolves a human-readable alias to the current snapshot id.
    pub async fn resolve_alias(&self, alias: &str) -> RepositoryResult<Option<SnapshotId>> {
        self.catalog.resolve_alias(alias).await
    }

    /// Says this node is still running `build_id`. `false` means stop.
    pub async fn renew_build_lease(&self, build_id: &SnapshotId) -> RepositoryResult<bool> {
        self.catalog.renew_build_lease(build_id).await
    }

    /// Atomically transitions one template build from waiting to building.
    pub async fn try_start_build(&self, id: &SnapshotId) -> RepositoryResult<StartedBuild> {
        self.catalog.try_start_build(id).await
    }

    /// Marks one template build as failed.
    pub async fn mark_build_error(
        &self,
        id: &SnapshotId,
        reason: TemplateBuildErrorReason,
    ) -> RepositoryResult<()> {
        self.catalog.mark_build_error(id, reason).await
    }

    async fn roll_back_publish(
        &self,
        id: &SnapshotId,
        publications: &[PersistedDiskImagePublication],
    ) {
        // A read failure here is treated as "nothing to protect", matching the
        // pre-split POSIX guard, which derived the same answer from a record
        // read whose errors it swallowed.
        let retained = self
            .catalog
            .retains_artifacts_on_publish_failure(id)
            .await
            .unwrap_or(false);
        if retained {
            // 🔴 Said out loud, because nothing else will. These bytes are
            // staying and no collector exists for them; the only way anyone
            // learns the prefix is here is by being told now.
            crate::snapshot::repository::metrics::record_artifacts_retained();
            tracing::warn!(
                snapshot_id = %id,
                artifact_count = publications.len(),
                "a failed publish left this snapshot's artifacts in place because a catalog \
                 still holds a committed row for it; nothing collects them"
            );
            return;
        }
        self.artifacts.delete_artifacts(id, publications).await;
    }
}

fn now_unix_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

/// Rejects publish manifests whose attached drives cannot be committed.
///
/// Both backends checked this identically before the split; it is a property of
/// the request, not of any store, so it belongs above both of them.
fn validate_attached_drives(manifest: &FirecrackerSnapshotManifest) -> RepositoryResult<()> {
    let mut drive_ids = HashSet::new();
    for drive in &manifest.attached_drives {
        if !drive_ids.insert(drive.drive_id.clone()) {
            return Err(RepositoryError::InvalidRequest {
                reason: format!(
                    "duplicate attached drive id in publish request: {}",
                    drive.drive_id
                ),
            });
        }
        if drive.virtual_size == 0 {
            return Err(RepositoryError::InvalidRequest {
                reason: format!(
                    "attached drive '{}' virtual_size must be non-zero",
                    drive.drive_id
                ),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_trait::async_trait;

    use super::*;
    use crate::sandbox::ExtraDrive;
    use crate::snapshot::repository::interfaces::{ImportedSnapshotArtifacts, SnapshotCatalog};
    use crate::snapshot::types::{
        CommittedSnapshot, PersistedDiskImagePublication, SnapshotAlias, SnapshotPublishSource,
        TemplateBuildStatus,
    };
    use crate::types::ExecutionId;

    /// Every call either half receives, in order, so a test can assert that the
    /// bytes were written before the row and not merely that both happened.
    #[derive(Debug, Default)]
    struct Journal(Mutex<Vec<String>>);

    impl Journal {
        fn record(&self, entry: impl Into<String>) {
            self.0.lock().expect("journal").push(entry.into());
        }

        fn entries(&self) -> Vec<String> {
            self.0.lock().expect("journal").clone()
        }
    }

    struct FakeCatalog {
        journal: Arc<Journal>,
        commit_fails: bool,
        retains_artifacts: bool,
    }

    impl FakeCatalog {
        fn new(journal: Arc<Journal>) -> Self {
            Self {
                journal,
                commit_fails: false,
                retains_artifacts: false,
            }
        }
    }

    #[async_trait]
    impl SnapshotCatalog for FakeCatalog {
        async fn create(&self, record: SnapshotRecord) -> RepositoryResult<SnapshotRecord> {
            self.journal.record("catalog.create");
            Ok(record)
        }

        async fn publish_commit(&self, commit: SnapshotCommit) -> RepositoryResult<SnapshotRecord> {
            self.journal.record("catalog.publish_commit");
            if self.commit_fails {
                return Err(RepositoryError::Backend {
                    message: "commit refused".to_string(),
                    source: None,
                });
            }
            let mut record =
                SnapshotRecord::template_waiting(commit.id, commit.alias.clone(), commit.resources);
            record.mark_committed(
                commit.alias,
                commit.resources,
                commit.committed,
                commit.source,
                0,
            );
            Ok(record)
        }

        async fn get(&self, _id_or_alias: &str) -> RepositoryResult<Option<SnapshotRecord>> {
            self.journal.record("catalog.get");
            let mut record =
                SnapshotRecord::template_waiting(SnapshotId::generate(), None, Default::default());
            record.mark_committed(
                None,
                Default::default(),
                CommittedSnapshot::mock(),
                SnapshotPublishSource::Template,
                0,
            );
            Ok(Some(record))
        }

        async fn list(&self, _filter: SnapshotListFilter) -> RepositoryResult<Vec<SnapshotRecord>> {
            self.journal.record("catalog.list");
            Ok(Vec::new())
        }

        async fn delete_record(&self, _record: &SnapshotRecord) -> RepositoryResult<()> {
            self.journal.record("catalog.delete_record");
            Ok(())
        }

        async fn resolve_alias(&self, _alias: &str) -> RepositoryResult<Option<SnapshotId>> {
            self.journal.record("catalog.resolve_alias");
            Ok(None)
        }

        async fn try_start_build(&self, id: &SnapshotId) -> RepositoryResult<StartedBuild> {
            self.journal.record("catalog.try_start_build");
            Ok(StartedBuild::untracked(SnapshotRecord::template_waiting(
                id.clone(),
                None,
                Default::default(),
            )))
        }

        async fn mark_build_error(
            &self,
            _id: &SnapshotId,
            _reason: TemplateBuildErrorReason,
        ) -> RepositoryResult<()> {
            self.journal.record("catalog.mark_build_error");
            Ok(())
        }

        async fn retains_artifacts_on_publish_failure(
            &self,
            _id: &SnapshotId,
        ) -> RepositoryResult<bool> {
            self.journal.record("catalog.retains_artifacts");
            Ok(self.retains_artifacts)
        }
    }

    struct FakeArtifactStore {
        journal: Arc<Journal>,
        import_fails_after_publishing: bool,
    }

    impl FakeArtifactStore {
        fn new(journal: Arc<Journal>) -> Self {
            Self {
                journal,
                import_fails_after_publishing: false,
            }
        }
    }

    fn publication(tag: &str) -> PersistedDiskImagePublication {
        PersistedDiskImagePublication {
            image_ref: format!("reg.example/app:{tag}"),
            tag: tag.to_string(),
            manifest_digest: "sha256:manifest".to_string(),
            repo_blob_url: "https://reg.example/v2/app/blobs".to_string(),
        }
    }

    #[async_trait]
    impl SnapshotArtifactStore for FakeArtifactStore {
        async fn import_built_artifacts(
            &self,
            _metadata: &SnapshotPublishMetadata,
            _manifest: &FirecrackerSnapshotManifest,
            publications: &mut Vec<PersistedDiskImagePublication>,
        ) -> RepositoryResult<ImportedSnapshotArtifacts> {
            self.journal.record("artifacts.import");
            publications.push(publication("rootfs"));
            if self.import_fails_after_publishing {
                return Err(RepositoryError::Backend {
                    message: "import gave up after publishing the rootfs".to_string(),
                    source: None,
                });
            }
            Ok(ImportedSnapshotArtifacts {
                disk_publications: publications.clone(),
                ..ImportedSnapshotArtifacts::default()
            })
        }

        async fn delete_artifacts(
            &self,
            _id: &SnapshotId,
            publications: &[PersistedDiskImagePublication],
        ) {
            self.journal.record(format!(
                "artifacts.delete[{}]",
                publications
                    .iter()
                    .map(|publication| publication.tag.as_str())
                    .collect::<Vec<_>>()
                    .join(",")
            ));
        }
    }

    fn manifest() -> FirecrackerSnapshotManifest {
        FirecrackerSnapshotManifest::for_test(1024, &[])
    }

    fn metadata() -> SnapshotPublishMetadata {
        SnapshotPublishMetadata {
            alias: Some(SnapshotAlias::parse("composed").expect("alias parses")),
            ..SnapshotPublishMetadata::mock()
        }
    }

    /// The whole reason the trait was split: bytes are durable *before* the row
    /// that makes them findable is written. A commit that ran first would put a
    /// resolvable snapshot in front of artifacts that may never arrive.
    #[tokio::test]
    async fn publish_writes_bytes_before_it_commits_the_row() {
        let journal = Arc::new(Journal::default());
        let repository = SnapshotRepository::new(
            Arc::new(FakeCatalog::new(Arc::clone(&journal))),
            Arc::new(FakeArtifactStore::new(Arc::clone(&journal))),
        );

        let record = repository
            .publish(metadata(), manifest())
            .await
            .expect("publish should work");

        assert_eq!(
            journal.entries(),
            vec!["artifacts.import", "catalog.publish_commit"],
        );
        assert!(record.committed.is_some());
        assert!(matches!(
            record.source,
            crate::snapshot::SnapshotSource::Template {
                build: crate::snapshot::TemplateBuildInfo {
                    status: TemplateBuildStatus::Ready,
                    ..
                }
            }
        ));
    }

    /// What the artifact store already published has to reach the rollback,
    /// including from an import that failed partway: the `publications`
    /// out-parameter exists for exactly this.
    #[tokio::test]
    async fn a_failed_import_rolls_back_what_it_had_already_published() {
        let journal = Arc::new(Journal::default());
        let repository = SnapshotRepository::new(
            Arc::new(FakeCatalog::new(Arc::clone(&journal))),
            Arc::new(FakeArtifactStore {
                journal: Arc::clone(&journal),
                import_fails_after_publishing: true,
            }),
        );

        repository
            .publish(metadata(), manifest())
            .await
            .expect_err("import failure should fail the publish");

        assert_eq!(
            journal.entries(),
            vec![
                "artifacts.import",
                "catalog.retains_artifacts",
                // The publication the failed import had already made.
                "artifacts.delete[rootfs]",
            ],
            "a partial import must not leave its registry publication behind"
        );
    }

    #[tokio::test]
    async fn a_failed_commit_deletes_the_bytes_it_just_wrote() {
        let journal = Arc::new(Journal::default());
        let repository = SnapshotRepository::new(
            Arc::new(FakeCatalog {
                journal: Arc::clone(&journal),
                commit_fails: true,
                retains_artifacts: false,
            }),
            Arc::new(FakeArtifactStore::new(Arc::clone(&journal))),
        );

        repository
            .publish(metadata(), manifest())
            .await
            .expect_err("commit failure should fail the publish");

        assert_eq!(
            journal.entries(),
            vec![
                "artifacts.import",
                "catalog.publish_commit",
                "catalog.retains_artifacts",
                "artifacts.delete[rootfs]",
            ],
        );
    }

    /// The guard the POSIX backend needs: re-publishing over an id that an
    /// earlier publish already committed must not take that snapshot's bytes
    /// down with it when the new commit fails.
    #[tokio::test]
    async fn a_failed_commit_keeps_bytes_an_earlier_commit_still_owns() {
        let journal = Arc::new(Journal::default());
        let repository = SnapshotRepository::new(
            Arc::new(FakeCatalog {
                journal: Arc::clone(&journal),
                commit_fails: true,
                retains_artifacts: true,
            }),
            Arc::new(FakeArtifactStore::new(Arc::clone(&journal))),
        );

        repository
            .publish(metadata(), manifest())
            .await
            .expect_err("commit failure should fail the publish");

        assert_eq!(
            journal.entries(),
            vec![
                "artifacts.import",
                "catalog.publish_commit",
                "catalog.retains_artifacts",
            ],
            "no artifact deletion may follow a catalog that claims the bytes"
        );
    }

    /// 🔴 And it says so. The bytes are staying, nothing collects them, and
    /// until this counter existed the only evidence was an orphan prefix in the
    /// store — measured on the cluster as two objects and 22,194 bytes that
    /// nothing outside the store could see. The trade is deliberate; being
    /// silent about it was not.
    #[test]
    fn keeping_the_bytes_is_counted_where_somebody_can_see_it() {
        use metrics_util::debugging::DebuggingRecorder;

        use crate::snapshot::repository::metrics::test_support::counter_total;
        use crate::snapshot::repository::metrics::ARTIFACTS_RETAINED_TOTAL;

        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("a runtime should build");
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        let journal = Arc::new(Journal::default());
        let repository = SnapshotRepository::new(
            Arc::new(FakeCatalog {
                journal: Arc::clone(&journal),
                commit_fails: true,
                retains_artifacts: true,
            }),
            Arc::new(FakeArtifactStore::new(Arc::clone(&journal))),
        );

        metrics::with_local_recorder(&recorder, || {
            runtime.block_on(async {
                repository
                    .publish(metadata(), manifest())
                    .await
                    .expect_err("commit failure should fail the publish");
            });
        });

        assert_eq!(
            counter_total(&snapshotter, ARTIFACTS_RETAINED_TOTAL),
            1,
            "a leak nothing collects has to be a number somebody can look at"
        );
    }

    /// 🔴 The same counter, driven by the code that actually decides — the
    /// double write over a real central catalog and a real object store — and
    /// through the arrangement that produced the leak measured on the cluster:
    /// one orphan prefix, two objects, 22,194 bytes.
    ///
    /// The two tests either side of this one use a fake catalog that is *told*
    /// to retain, so they hold the counter still and prove nothing about which
    /// states reach it. Here the template row already exists, so the publish's
    /// opening statement is answered `AlreadyExists` and the commit flips
    /// somebody else's row — which means the undo may not take it back. Object
    /// storage then refuses the alias, the publish fails, and the rollback
    /// finds a `ready` row in PostgreSQL still pointing at the bytes. Keeping
    /// them is right. Saying nothing about it was not.
    #[test]
    fn the_double_write_reaches_the_counter_over_a_row_it_may_not_take_back() {
        use metrics_util::debugging::DebuggingRecorder;

        use crate::snapshot::repository::metrics::test_support::counter_total;
        use crate::snapshot::repository::metrics::ARTIFACTS_RETAINED_TOTAL;
        use crate::snapshot::repository::mirror::test_doubles::{
            record_for, ScriptedCatalog, ScriptedCentral,
        };
        use crate::snapshot::repository::mirror::{
            CentralCatalogWrites, DualWriteCatalog, MirrorBacklog,
        };

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a runtime should build");
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        let workspace = tempfile::TempDir::new().expect("tempdir should exist");
        let metadata = metadata();
        let id = metadata.id.clone();

        let central = Arc::new(ScriptedCentral::default());
        // A template row somebody else opened: the publish's `begin` will be
        // answered `AlreadyExists`, so the undo is not allowed to delete it.
        central.seed(record_for(&id));
        let object_store = Arc::new(ScriptedCatalog::default());
        object_store.refuse_alias_to(SnapshotId::generate());

        let journal = Arc::new(Journal::default());
        let repository = runtime.block_on(async {
            let backlog = MirrorBacklog::open(workspace.path().join("mirror"))
                .await
                .expect("the backlog should open");
            SnapshotRepository::new(
                Arc::new(DualWriteCatalog::new(
                    Arc::clone(&central) as Arc<dyn CentralCatalogWrites>,
                    Arc::clone(&object_store) as Arc<dyn SnapshotCatalog>,
                    backlog,
                )),
                Arc::new(FakeArtifactStore::new(Arc::clone(&journal))),
            )
        });

        metrics::with_local_recorder(&recorder, || {
            runtime.block_on(async {
                repository
                    .publish(metadata, manifest())
                    .await
                    .expect_err("an alias the object store will not bind fails the publish");
            });
        });

        assert!(
            central
                .holds(&id)
                .is_some_and(|row| row.committed.is_some()),
            "the arrangement only means anything while the central catalog still holds the \
             committed row"
        );
        assert!(
            !journal
                .entries()
                .iter()
                .any(|entry| entry.starts_with("artifacts.delete")),
            "the bytes must stay: {:?}",
            journal.entries()
        );
        assert_eq!(
            counter_total(&snapshotter, ARTIFACTS_RETAINED_TOTAL),
            1,
            "a leak nothing collects has to be a number somebody can look at"
        );
    }

    /// The control face: a rollback that actually deletes is not a leak and
    /// must not be counted as one.
    #[test]
    fn deleting_the_bytes_is_not_counted_as_keeping_them() {
        use metrics_util::debugging::DebuggingRecorder;

        use crate::snapshot::repository::metrics::test_support::counter_total;
        use crate::snapshot::repository::metrics::ARTIFACTS_RETAINED_TOTAL;

        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("a runtime should build");
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        let journal = Arc::new(Journal::default());
        let repository = SnapshotRepository::new(
            Arc::new(FakeCatalog {
                journal: Arc::clone(&journal),
                commit_fails: true,
                retains_artifacts: false,
            }),
            Arc::new(FakeArtifactStore::new(Arc::clone(&journal))),
        );

        metrics::with_local_recorder(&recorder, || {
            runtime.block_on(async {
                repository
                    .publish(metadata(), manifest())
                    .await
                    .expect_err("commit failure should fail the publish");
            });
        });

        assert_eq!(counter_total(&snapshotter, ARTIFACTS_RETAINED_TOTAL), 0);
    }

    /// Deleting in the other order would let a reader resolve a snapshot whose
    /// bytes are already going away.
    #[tokio::test]
    async fn delete_removes_the_row_before_the_bytes() {
        let journal = Arc::new(Journal::default());
        let repository = SnapshotRepository::new(
            Arc::new(FakeCatalog::new(Arc::clone(&journal))),
            Arc::new(FakeArtifactStore::new(Arc::clone(&journal))),
        );

        repository.delete("anything").await.expect("delete works");

        assert_eq!(
            journal.entries(),
            vec!["catalog.get", "catalog.delete_record", "artifacts.delete[]"],
        );
    }

    /// P12, first half. `stage` establishes the bytes and says nothing to the
    /// catalog — the snapshot exists and is unfindable, which is the state the
    /// whole seam is built to produce.
    #[tokio::test]
    async fn stage_writes_the_bytes_and_tells_the_catalog_nothing() {
        let journal = Arc::new(Journal::default());
        let repository = SnapshotRepository::on_node(
            Arc::new(FakeCatalog::new(Arc::clone(&journal))),
            Arc::new(FakeArtifactStore::new(Arc::clone(&journal))),
            "node-a".to_string(),
        );

        let staged = repository
            .stage(metadata(), manifest(), None)
            .await
            .expect("staging should work");

        assert_eq!(journal.entries(), vec!["artifacts.import"]);
        assert_eq!(staged.origin_node_id, "node-a");
        assert_eq!(
            staged.alias().map(SnapshotAlias::to_string),
            Some("composed".to_string())
        );
    }

    /// 🔴 P12. The staged value has to survive being written down and read back
    /// somewhere else, because that is exactly what the phase after this one
    /// does with it. Committing the *round-tripped* value — not the original —
    /// is what makes this a test of the wire form rather than of a struct.
    #[tokio::test]
    async fn a_staged_snapshot_commits_after_a_serde_round_trip() {
        let journal = Arc::new(Journal::default());
        let repository = SnapshotRepository::on_node(
            Arc::new(FakeCatalog::new(Arc::clone(&journal))),
            Arc::new(FakeArtifactStore::new(Arc::clone(&journal))),
            "node-a".to_string(),
        );

        let staged = repository
            .stage(metadata(), manifest(), Some(ExecutionId::new()))
            .await
            .expect("staging should work");
        let encoded = serde_json::to_vec(&staged).expect("a staged snapshot must serialize");
        let decoded: StagedSnapshot =
            serde_json::from_slice(&encoded).expect("a staged snapshot must deserialize");

        assert_eq!(decoded.commit.id, staged.commit.id);
        assert_eq!(decoded.origin_node_id, staged.origin_node_id);
        assert_eq!(decoded.execution_id, staged.execution_id);
        assert_eq!(
            decoded.commit.committed.disk_publications.len(),
            staged.commit.committed.disk_publications.len(),
            "the payload the row will carry must survive the round trip"
        );

        let record = repository
            .commit_staged(decoded)
            .await
            .expect("a round-tripped staged snapshot must still commit");

        assert!(record.committed.is_some());
        assert_eq!(
            journal.entries(),
            vec!["artifacts.import", "catalog.publish_commit"],
        );
    }

    /// The rollback still reaches the registry publications after the split,
    /// even though the commit no longer has the import's out-parameter — it
    /// reads them back out of the payload instead.
    #[tokio::test]
    async fn a_refused_commit_rolls_back_the_publications_inside_the_payload() {
        let journal = Arc::new(Journal::default());
        let repository = SnapshotRepository::on_node(
            Arc::new(FakeCatalog {
                journal: Arc::clone(&journal),
                commit_fails: true,
                retains_artifacts: false,
            }),
            Arc::new(FakeArtifactStore::new(Arc::clone(&journal))),
            "node-a".to_string(),
        );

        let staged = repository
            .stage(metadata(), manifest(), None)
            .await
            .expect("staging should work");
        repository
            .commit_staged(staged)
            .await
            .expect_err("the commit was refused");

        assert_eq!(
            journal.entries(),
            vec![
                "artifacts.import",
                "catalog.publish_commit",
                "catalog.retains_artifacts",
                "artifacts.delete[rootfs]",
            ],
        );
    }

    /// 🔴 Why [`StagedSnapshot`] carries a `CommittedSnapshot` and not the
    /// manifest it was derived from. Every path in the manifest is
    /// `#[serde(skip)]`, so a round trip yields a manifest that still *looks*
    /// like one and points at nothing. A commit handed that would fail in a way
    /// nobody can read; a commit that cannot be handed one at all cannot.
    #[test]
    fn a_round_tripped_manifest_loses_every_path_it_had() {
        let original = manifest();
        assert!(!original.vm_state.path.as_os_str().is_empty());

        let decoded: FirecrackerSnapshotManifest = serde_json::from_slice(
            &serde_json::to_vec(&original).expect("manifest should serialize"),
        )
        .expect("manifest should deserialize");

        assert!(
            decoded.vm_state.path.as_os_str().is_empty(),
            "if this ever survives the round trip, StagedSnapshot's reason for \
             excluding the manifest has changed and the exclusion must be re-argued"
        );
        assert!(decoded.rootfs.image_config_path.as_os_str().is_empty());
        assert!(decoded.memory.image_config_path.as_os_str().is_empty());
    }

    #[tokio::test]
    async fn a_rejected_manifest_never_reaches_either_half() {
        let journal = Arc::new(Journal::default());
        let repository = SnapshotRepository::new(
            Arc::new(FakeCatalog::new(Arc::clone(&journal))),
            Arc::new(FakeArtifactStore::new(Arc::clone(&journal))),
        );
        let drive =
            ExtraDrive::try_new_overlaybd("duplicate", "drives/duplicate/image.json", false)
                .expect("drive should build")
                .try_with_virtual_size(4096)
                .expect("drive size should be accepted");
        let duplicated = FirecrackerSnapshotManifest::for_test(1024, &[drive.clone(), drive]);

        let error = repository
            .publish(metadata(), duplicated)
            .await
            .expect_err("duplicate drive ids should be rejected");

        assert!(matches!(error, RepositoryError::InvalidRequest { .. }));
        assert!(
            journal.entries().is_empty(),
            "validation must run before any storage is touched, got {:?}",
            journal.entries()
        );
    }
}
