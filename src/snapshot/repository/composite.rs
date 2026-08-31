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
                    commit: SnapshotCommit::new(&metadata, imported, staged_at_unix_ms),
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

    /// Commits a staged value without consulting local files or the artifact store.
    ///
    /// Failure rolls back artifacts unless an earlier commit owns them.
    pub async fn commit_staged(&self, staged: StagedSnapshot) -> RepositoryResult<SnapshotRecord> {
        let id = staged.commit.id.clone();
        // The staged payload is the commit side's rollback description.
        let publications = staged.commit.committed.disk_publications.clone();

        match self.catalog.publish_commit(staged.commit).await {
            Ok(record) => Ok(record),
            Err(error) => {
                self.roll_back_publish(&id, &publications).await;
                Err(error)
            }
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
        // A failed retention check defaults to rollback, matching prior behavior.
        let retained = self
            .catalog
            .retains_artifacts_on_publish_failure(id)
            .await
            .unwrap_or(false);
        if retained {
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
                commit_fails: true,
                retains_artifacts: false,
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
                "catalog.retains_artifacts",
                "artifacts.delete[rootfs]",
            ],
        );
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
