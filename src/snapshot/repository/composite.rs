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
//! artifacts.import_built_artifacts(..)  ->  ImportedSnapshotArtifacts   // bytes
//! CommittedSnapshot { .. }                                              // pure
//! catalog.publish_commit(..)            ->  SnapshotRecord              // the flip
//! ```
//!
//! Because the catalog is reached through `Arc<dyn SnapshotCatalog>`, the row
//! half can be replaced — by a remote client, or by a wrapper that writes to
//! two catalogs at once — without the byte half being rebuilt or even
//! recompiled against a different type.

use std::collections::HashSet;
use std::sync::Arc;

use crate::sandbox::FirecrackerSnapshotManifest;
use crate::snapshot::repository::interfaces::{
    SnapshotArtifactStore, SnapshotCatalog, SnapshotCommit, SnapshotListFilter,
};
use crate::snapshot::repository::{RepositoryError, RepositoryResult};
use crate::snapshot::types::{
    PersistedDiskImagePublication, SnapshotId, SnapshotPublishMetadata, SnapshotRecord,
    TemplateBuildErrorReason,
};

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
}

impl SnapshotRepository {
    pub fn new(
        catalog: Arc<dyn SnapshotCatalog>,
        artifacts: Arc<dyn SnapshotArtifactStore>,
    ) -> Self {
        Self { catalog, artifacts }
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
        validate_attached_drives(&manifest)?;

        // Held outside the fallible block so a rollback can still see the
        // publications a partial import already made.
        let mut publications: Vec<PersistedDiskImagePublication> = Vec::new();
        let published = async {
            let imported = self
                .artifacts
                .import_built_artifacts(&metadata, &manifest, &mut publications)
                .await?;
            self.catalog
                .publish_commit(SnapshotCommit::new(&metadata, imported))
                .await
        }
        .await;

        match published {
            Ok(record) => Ok(record),
            Err(error) => {
                self.roll_back_publish(&metadata.id, &publications).await;
                Err(error)
            }
        }
    }

    /// Loads one snapshot record by repository id or alias.
    pub async fn get(&self, id_or_alias: &str) -> RepositoryResult<Option<SnapshotRecord>> {
        self.catalog.get(id_or_alias).await
    }

    /// Lists snapshot records matching the provided filter.
    pub async fn list(&self, filter: SnapshotListFilter) -> RepositoryResult<Vec<SnapshotRecord>> {
        self.catalog.list(filter).await
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

    /// Atomically transitions one template build from waiting to building.
    pub async fn try_start_build(&self, id: &SnapshotId) -> RepositoryResult<SnapshotRecord> {
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
            return;
        }
        self.artifacts.delete_artifacts(id, publications).await;
    }
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

        async fn try_start_build(&self, id: &SnapshotId) -> RepositoryResult<SnapshotRecord> {
            self.journal.record("catalog.try_start_build");
            Ok(SnapshotRecord::template_waiting(
                id.clone(),
                None,
                Default::default(),
            ))
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
