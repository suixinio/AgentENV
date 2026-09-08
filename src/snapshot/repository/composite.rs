//! Sequences durable snapshot bytes and catalog rows.
//!
//! Bytes are staged before the row makes them visible. A serializable
//! [`StagedSnapshot`] may cross processes before commit.

use std::collections::HashSet;
use std::sync::Arc;

use crate::snapshot::repository::interfaces::{
    CatalogReadScope, SnapshotAbsence, SnapshotArtifactStore, SnapshotCatalog, SnapshotCommit,
    SnapshotListFilter, SnapshotListPage, StagedSnapshot, StartedBuild,
};
use crate::snapshot::repository::{RepositoryError, RepositoryResult};
use crate::snapshot::types::{
    PersistedDiskImagePublication, SnapshotId, SnapshotPublishMetadata, SnapshotRecord,
    TemplateBuildErrorReason,
};
use crate::types::FirecrackerSnapshotManifest;

/// What a commit does with the staged bytes when the catalog definitely
/// refused it.
///
/// `Retain` belongs to every capture whose sandbox cannot be brought back
/// without it: the bytes are the only copy, and a later commit of the same
/// staged value is the recovery path.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OnCommitFailure {
    RollBack,
    Retain,
}

/// Durable catalog and artifact store, sequenced so returned records never
/// expose process-local paths.
pub struct SnapshotRepository {
    catalog: Arc<dyn SnapshotCatalog>,
    artifacts: Arc<dyn SnapshotArtifactStore>,
    /// Node where staged bytes landed.
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

    /// Publishes local artifacts, then the row that exposes them.
    ///
    /// Failed publications roll back artifacts unless an earlier commit retains them.
    pub async fn publish(
        &self,
        metadata: SnapshotPublishMetadata,
        manifest: FirecrackerSnapshotManifest,
    ) -> RepositoryResult<SnapshotRecord> {
        let staged = self.stage(metadata, manifest).await?;
        self.commit_staged(staged).await
    }

    /// Writes snapshot bytes without exposing a catalog row.
    ///
    /// Failed imports roll back partial external publications.
    pub async fn stage(
        &self,
        metadata: SnapshotPublishMetadata,
        manifest: FirecrackerSnapshotManifest,
    ) -> RepositoryResult<StagedSnapshot> {
        validate_attached_drives(&manifest)?;

        // Keep partial publications available to rollback.
        let mut publications: Vec<PersistedDiskImagePublication> = Vec::new();
        let imported = self
            .artifacts
            .import_built_artifacts(&metadata, &manifest, &mut publications)
            .await;

        match imported {
            // One timestamp identifies both staging and snapshot creation.
            Ok(imported) => {
                let staged_at_unix_ms = now_unix_ms();
                Ok(StagedSnapshot {
                    commit: SnapshotCommit::new(
                        &metadata,
                        imported,
                        staged_at_unix_ms,
                        Some(self.origin_node_id.clone()),
                    ),
                    staged_at_unix_ms,
                    origin_node_id: self.origin_node_id.clone(),
                })
            }
            Err(error) => {
                self.roll_back_publish(&metadata.id, &publications).await;
                Err(error)
            }
        }
    }

    /// [`Self::commit_staged_with`] under [`OnCommitFailure::RollBack`].
    pub async fn commit_staged(&self, staged: StagedSnapshot) -> RepositoryResult<SnapshotRecord> {
        self.commit_staged_with(staged, OnCommitFailure::RollBack)
            .await
    }

    /// Commits a staged value without consulting local files or the artifact store.
    ///
    /// A refused commit is re-read before anything is deleted: an error that
    /// crossed a lost acknowledgement leaves a committed row, and its bytes are
    /// the only thing that row points at. `on_failure` decides what a definite
    /// non-commit does with the bytes; the caller chooses it, because only the
    /// caller knows whether it can produce them again.
    pub async fn commit_staged_with(
        &self,
        staged: StagedSnapshot,
        on_failure: OnCommitFailure,
    ) -> RepositoryResult<SnapshotRecord> {
        let id = staged.commit.id.clone();
        // The staged payload is the commit side's rollback description.
        let publications = staged.commit.committed.disk_publications.clone();

        match self.catalog.publish_commit(staged.commit).await {
            Ok(record) => Ok(record),
            Err(error) => match self.probe_commit(&id).await {
                CommitProbe::Landed(record) => {
                    tracing::warn!(
                        snapshot_id = %id,
                        error = %error,
                        "a commit reported failure and the catalog holds its committed row; \
                         reporting the commit that happened"
                    );
                    Ok(*record)
                }
                CommitProbe::NotLanded => {
                    match on_failure {
                        OnCommitFailure::RollBack => {
                            self.roll_back_publish(&id, &publications).await
                        }
                        OnCommitFailure::Retain => self.retain_artifacts(
                            &id,
                            publications.len(),
                            "the caller cannot produce these bytes again, so a later commit \
                             of the same staged snapshot is the only way back",
                        ),
                    }
                    Err(error)
                }
                CommitProbe::Unknown(because) => {
                    self.retain_artifacts(&id, publications.len(), &because);
                    Err(error)
                }
            },
        }
    }

    /// Whether a refused commit nevertheless left a committed row behind.
    async fn probe_commit(&self, id: &SnapshotId) -> CommitProbe {
        match self
            .catalog
            .get_scoped(&id.to_string(), CatalogReadScope::AnyStatus)
            .await
        {
            Ok(Some(record)) if record.committed.is_some() => CommitProbe::Landed(Box::new(record)),
            Ok(_) => CommitProbe::NotLanded,
            Err(error) => CommitProbe::Unknown(format!(
                "the catalog could not say whether the commit landed: {error}"
            )),
        }
    }

    /// Loads a resolvable snapshot record by id or alias.
    pub async fn get(&self, id_or_alias: &str) -> RepositoryResult<Option<SnapshotRecord>> {
        self.catalog.get(id_or_alias).await
    }

    /// [`Self::get`] at an explicitly chosen scope.
    pub async fn get_scoped(
        &self,
        id_or_alias: &str,
        scope: CatalogReadScope,
    ) -> RepositoryResult<Option<SnapshotRecord>> {
        self.catalog.get_scoped(id_or_alias, scope).await
    }

    /// Returns whether catalog absence is settled enough for destructive action.
    pub async fn absence_of(&self, id: &SnapshotId) -> RepositoryResult<SnapshotAbsence> {
        self.catalog.absence_of(id).await
    }

    /// Lists one page of snapshot records, newest first.
    pub async fn list_page(
        &self,
        filter: SnapshotListFilter,
    ) -> RepositoryResult<SnapshotListPage> {
        self.catalog.list_page(filter).await
    }

    /// [`Self::list_page`] at an explicitly chosen scope.
    pub async fn list_page_scoped(
        &self,
        filter: SnapshotListFilter,
        scope: CatalogReadScope,
    ) -> RepositoryResult<SnapshotListPage> {
        self.catalog.list_page_scoped(filter, scope).await
    }

    /// Deletes a snapshot idempotently, removing its row before best-effort artifact cleanup.
    pub async fn delete(&self, id_or_alias: &str) -> RepositoryResult<()> {
        // Deletion must see waiting and failed rows as well as resolvable ones.
        let Some(record) = self
            .catalog
            .get_scoped(id_or_alias, CatalogReadScope::AnyStatus)
            .await?
        else {
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

    /// Deletes every pause of one sandbox in one catalog write, then cleans the
    /// artifacts of each row it removed. Returns how many rows went away.
    pub async fn delete_sandbox_pauses(&self, source_sandbox_id: &str) -> RepositoryResult<usize> {
        let removed = self
            .catalog
            .delete_sandbox_pauses(source_sandbox_id)
            .await?;
        for record in &removed {
            let publications = record
                .committed
                .as_ref()
                .map(|committed| committed.disk_publications.as_slice())
                .unwrap_or_default();
            self.artifacts
                .delete_artifacts(&record.id, publications)
                .await;
        }
        Ok(removed.len())
    }

    /// Resolves a human-readable alias to the current snapshot id.
    pub async fn resolve_alias(&self, alias: &str) -> RepositoryResult<Option<SnapshotId>> {
        self.catalog.resolve_alias(alias).await
    }

    /// [`Self::resolve_alias`] at an explicitly chosen scope.
    pub async fn resolve_alias_scoped(
        &self,
        alias: &str,
        scope: CatalogReadScope,
    ) -> RepositoryResult<Option<SnapshotId>> {
        self.catalog.resolve_alias_scoped(alias, scope).await
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
        match self.catalog.retains_artifacts_on_publish_failure(id).await {
            Ok(false) => self.artifacts.delete_artifacts(id, publications).await,
            Ok(true) => self.retain_artifacts(
                id,
                publications.len(),
                "a catalog still holds a committed row for it",
            ),
            // An unanswerable retention question is not permission to delete.
            Err(error) => self.retain_artifacts(
                id,
                publications.len(),
                &format!("the catalog could not say whether it still owns them: {error}"),
            ),
        }
    }

    fn retain_artifacts(&self, id: &SnapshotId, artifact_count: usize, because: &str) {
        crate::snapshot::repository::metrics::record_artifacts_retained();
        tracing::warn!(
            snapshot_id = %id,
            artifact_count,
            because,
            "a failed publish left this snapshot's artifacts in place; nothing collects them"
        );
    }
}

/// What a re-read of a refused commit's row says about it.
enum CommitProbe {
    /// The row is committed: the commit happened and its error was the answer.
    Landed(Box<SnapshotRecord>),
    /// No committed row exists, and the read that says so was complete.
    NotLanded,
    /// The catalog could not answer; `String` says why.
    Unknown(String),
}

fn now_unix_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

/// Rejects publish manifests whose attached drives cannot be committed.
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
    use crate::snapshot::repository::interfaces::{ImportedSnapshotArtifacts, SnapshotCatalog};
    use crate::snapshot::types::{
        CommittedSnapshot, PersistedDiskImagePublication, SnapshotAlias, SnapshotPublishSource,
        TemplateBuildStatus,
    };
    use crate::types::ExtraDrive;

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

    /// What the fake catalog's row read answers after a refused commit.
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum ProbeAnswer {
        AsGet,
        NoRow,
        Committed,
        Unreadable,
    }

    struct FakeCatalog {
        journal: Arc<Journal>,
        /// Refusals `publish_commit` answers with before it starts committing.
        commit_refusals: Mutex<usize>,
        retains_artifacts: bool,
        retention_unreadable: bool,
        probe: ProbeAnswer,
    }

    impl FakeCatalog {
        fn new(journal: Arc<Journal>) -> Self {
            Self {
                journal,
                commit_refusals: Mutex::new(0),
                retains_artifacts: false,
                retention_unreadable: false,
                probe: ProbeAnswer::AsGet,
            }
        }

        fn refusing(journal: Arc<Journal>, probe: ProbeAnswer) -> Self {
            Self {
                journal,
                commit_refusals: Mutex::new(usize::MAX),
                retains_artifacts: false,
                retention_unreadable: false,
                probe,
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
            {
                let mut refusals = self.commit_refusals.lock().expect("commit refusals");
                if *refusals > 0 {
                    *refusals = refusals.saturating_sub(1);
                    return Err(RepositoryError::Backend {
                        message: "commit refused".to_string(),
                        source: None,
                    });
                }
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

        async fn get_scoped(
            &self,
            id_or_alias: &str,
            _scope: CatalogReadScope,
        ) -> RepositoryResult<Option<SnapshotRecord>> {
            match self.probe {
                ProbeAnswer::AsGet => return self.get(id_or_alias).await,
                _ => self.journal.record("catalog.get_scoped"),
            }
            match self.probe {
                ProbeAnswer::AsGet => unreachable!("handled above"),
                ProbeAnswer::NoRow => Ok(None),
                ProbeAnswer::Committed => {
                    let id = SnapshotId::parse(id_or_alias).expect("a probe asks by id");
                    let mut record = SnapshotRecord::template_waiting(id, None, Default::default());
                    record.mark_committed(
                        None,
                        Default::default(),
                        CommittedSnapshot::mock(),
                        SnapshotPublishSource::Template,
                        0,
                    );
                    Ok(Some(record))
                }
                ProbeAnswer::Unreadable => Err(RepositoryError::Backend {
                    message: "the catalog is unreachable".to_string(),
                    source: None,
                }),
            }
        }

        async fn list_page(
            &self,
            _filter: SnapshotListFilter,
        ) -> RepositoryResult<SnapshotListPage> {
            self.journal.record("catalog.list_page");
            Ok(SnapshotListPage::single(Vec::new()))
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
            if self.retention_unreadable {
                return Err(RepositoryError::Backend {
                    message: "the catalog is unreachable".to_string(),
                    source: None,
                });
            }
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
                commit_refusals: Mutex::new(usize::MAX),
                retains_artifacts: false,
                retention_unreadable: false,
                probe: ProbeAnswer::NoRow,
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
                "catalog.get_scoped",
                "catalog.retains_artifacts",
                "artifacts.delete[rootfs]",
            ],
        );
    }

    #[tokio::test]
    async fn a_failed_commit_keeps_bytes_an_earlier_commit_still_owns() {
        let journal = Arc::new(Journal::default());
        let repository = SnapshotRepository::new(
            Arc::new(FakeCatalog {
                journal: Arc::clone(&journal),
                commit_refusals: Mutex::new(usize::MAX),
                retains_artifacts: true,
                retention_unreadable: false,
                probe: ProbeAnswer::NoRow,
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
                "catalog.get_scoped",
                "catalog.retains_artifacts",
            ],
            "no artifact deletion may follow a catalog that claims the bytes"
        );
    }

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
                commit_refusals: Mutex::new(usize::MAX),
                retains_artifacts: true,
                retention_unreadable: false,
                probe: ProbeAnswer::NoRow,
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
                commit_refusals: Mutex::new(usize::MAX),
                retains_artifacts: false,
                retention_unreadable: false,
                probe: ProbeAnswer::NoRow,
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

    #[tokio::test]
    async fn a_commit_whose_answer_was_lost_reports_the_row_it_actually_wrote() {
        let journal = Arc::new(Journal::default());
        let repository = SnapshotRepository::new(
            Arc::new(FakeCatalog::refusing(
                Arc::clone(&journal),
                ProbeAnswer::Committed,
            )),
            Arc::new(FakeArtifactStore::new(Arc::clone(&journal))),
        );

        let record = repository
            .publish(metadata(), manifest())
            .await
            .expect("a committed row makes the commit a success whatever the call said");

        assert!(record.committed.is_some());
        assert_eq!(
            journal.entries(),
            vec![
                "artifacts.import",
                "catalog.publish_commit",
                "catalog.get_scoped",
            ],
            "nothing may be deleted once the row that points at it exists"
        );
    }

    #[tokio::test]
    async fn a_commit_nobody_can_read_back_keeps_its_bytes() {
        let journal = Arc::new(Journal::default());
        let repository = SnapshotRepository::new(
            Arc::new(FakeCatalog::refusing(
                Arc::clone(&journal),
                ProbeAnswer::Unreadable,
            )),
            Arc::new(FakeArtifactStore::new(Arc::clone(&journal))),
        );

        repository
            .publish(metadata(), manifest())
            .await
            .expect_err("an unreadable catalog cannot turn into a successful publish");

        assert_eq!(
            journal.entries(),
            vec![
                "artifacts.import",
                "catalog.publish_commit",
                "catalog.get_scoped",
            ],
            "uncertainty is not permission to delete"
        );
    }

    #[tokio::test]
    async fn an_unanswerable_retention_check_keeps_the_bytes() {
        let journal = Arc::new(Journal::default());
        let repository = SnapshotRepository::new(
            Arc::new(FakeCatalog {
                journal: Arc::clone(&journal),
                commit_refusals: Mutex::new(usize::MAX),
                retains_artifacts: false,
                retention_unreadable: true,
                probe: ProbeAnswer::NoRow,
            }),
            Arc::new(FakeArtifactStore::new(Arc::clone(&journal))),
        );

        repository
            .publish(metadata(), manifest())
            .await
            .expect_err("the commit was refused");

        assert_eq!(
            journal.entries(),
            vec![
                "artifacts.import",
                "catalog.publish_commit",
                "catalog.get_scoped",
                "catalog.retains_artifacts",
            ],
            "a retention check that errored must not be read as permission to delete"
        );
    }

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

    #[tokio::test]
    async fn stage_writes_the_bytes_and_tells_the_catalog_nothing() {
        let journal = Arc::new(Journal::default());
        let repository = SnapshotRepository::on_node(
            Arc::new(FakeCatalog::new(Arc::clone(&journal))),
            Arc::new(FakeArtifactStore::new(Arc::clone(&journal))),
            "node-a".to_string(),
        );

        let staged = repository
            .stage(metadata(), manifest())
            .await
            .expect("staging should work");

        assert_eq!(journal.entries(), vec!["artifacts.import"]);
        assert_eq!(staged.origin_node_id, "node-a");
        assert_eq!(
            staged.alias().map(SnapshotAlias::to_string),
            Some("composed".to_string())
        );
    }

    #[tokio::test]
    async fn a_staged_snapshot_commits_after_a_serde_round_trip() {
        let journal = Arc::new(Journal::default());
        let repository = SnapshotRepository::on_node(
            Arc::new(FakeCatalog::new(Arc::clone(&journal))),
            Arc::new(FakeArtifactStore::new(Arc::clone(&journal))),
            "node-a".to_string(),
        );

        let staged = repository
            .stage(metadata(), manifest())
            .await
            .expect("staging should work");
        let encoded = serde_json::to_vec(&staged).expect("a staged snapshot must serialize");
        let decoded: StagedSnapshot =
            serde_json::from_slice(&encoded).expect("a staged snapshot must deserialize");

        assert_eq!(decoded.commit.id, staged.commit.id);
        assert_eq!(decoded.origin_node_id, staged.origin_node_id);
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

    #[tokio::test]
    async fn a_refused_commit_rolls_back_the_publications_inside_the_payload() {
        let journal = Arc::new(Journal::default());
        let repository = SnapshotRepository::on_node(
            Arc::new(FakeCatalog {
                journal: Arc::clone(&journal),
                commit_refusals: Mutex::new(usize::MAX),
                retains_artifacts: false,
                retention_unreadable: false,
                probe: ProbeAnswer::NoRow,
            }),
            Arc::new(FakeArtifactStore::new(Arc::clone(&journal))),
            "node-a".to_string(),
        );

        let staged = repository
            .stage(metadata(), manifest())
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
                "catalog.get_scoped",
                "catalog.retains_artifacts",
                "artifacts.delete[rootfs]",
            ],
        );
    }

    #[tokio::test]
    async fn a_retained_commit_keeps_the_bytes_for_the_commit_that_lands() {
        let journal = Arc::new(Journal::default());
        let repository = SnapshotRepository::on_node(
            Arc::new(FakeCatalog {
                journal: Arc::clone(&journal),
                commit_refusals: Mutex::new(1),
                retains_artifacts: false,
                retention_unreadable: false,
                probe: ProbeAnswer::NoRow,
            }),
            Arc::new(FakeArtifactStore::new(Arc::clone(&journal))),
            "node-a".to_string(),
        );

        let staged = repository
            .stage(metadata(), manifest())
            .await
            .expect("staging should work");
        repository
            .commit_staged_with(staged.clone(), OnCommitFailure::Retain)
            .await
            .expect_err("the first commit was refused");

        assert!(
            !journal
                .entries()
                .iter()
                .any(|entry| entry.starts_with("artifacts.delete")),
            "a retained commit must not delete the only copy of the sandbox: {:?}",
            journal.entries()
        );

        let record = repository
            .commit_staged_with(staged, OnCommitFailure::Retain)
            .await
            .expect("the second commit of the same staged snapshot lands");
        assert!(record.committed.is_some());
    }

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
