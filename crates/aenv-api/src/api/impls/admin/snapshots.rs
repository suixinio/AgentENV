use tracing::info;

use crate::snapshot::CatalogReadScope;
use agentenv_http_server::{apis::admin::*, models};

use super::ApiImpl;

impl ApiImpl {
    /// Deletes a snapshot's catalog records, alias, and artifacts.
    ///
    /// Missing snapshots return 404, and all snapshot statuses are addressable.
    pub(super) async fn delete_snapshot(
        &self,
        path_params: &models::SnapshotsSnapshotIdDeletePathParams,
    ) -> Result<SnapshotsSnapshotIdDeleteResponse, ()> {
        let snapshot_id = path_params.snapshot_id.as_str();
        let record = match self
            .snapshot_manager
            .get_scoped(snapshot_id, CatalogReadScope::AnyStatus)
            .await
        {
            Ok(Some(record)) => record,
            Ok(None) => {
                return Ok(SnapshotsSnapshotIdDeleteResponse::Status404_NotFound(
                    Self::error(404, format!("snapshot {snapshot_id} not found")),
                ));
            }
            Err(err) => {
                return Ok(SnapshotsSnapshotIdDeleteResponse::Status500_ServerError(
                    Self::snapshot_manager_error(&err),
                ));
            }
        };

        // Capture the record in the operator-visible log before deletion.
        info!(
            snapshot_id = %record.id,
            alias = ?record.alias.as_ref().map(ToString::to_string),
            committed = record.committed.is_some(),
            "an operator asked for this snapshot to be deleted; removing its row from every \
             catalog, its alias, and its artifacts"
        );

        match self.snapshot_manager.delete(&record.id.to_string()).await {
            Ok(()) => {
                Ok(SnapshotsSnapshotIdDeleteResponse::Status204_TheSnapshotWasDeletedSuccessfully)
            }
            Err(err) => Ok(SnapshotsSnapshotIdDeleteResponse::Status500_ServerError(
                Self::snapshot_manager_error(&err),
            )),
        }
    }
}

#[cfg(test)]
mod operator_snapshot_delete_tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use axum_extra::extract::CookieJar;
    use headers::Host;
    use http::Method;

    use agentenv_http_server::apis::admin::*;
    use agentenv_http_server::models;

    use super::ApiImpl;
    use crate::orchestrator::Orchestrator;
    use crate::sandbox::mock::MockBackendFactory;
    use crate::snapshot::repository::interfaces::{
        ImportedSnapshotArtifacts, SnapshotArtifactStore, SnapshotCatalog, SnapshotCommit,
        StartedBuild,
    };
    use crate::snapshot::repository::{
        RepositoryError, RepositoryResult, SnapshotListFilter, SnapshotListPage, SnapshotRepository,
    };
    use crate::snapshot::{
        CatalogReadScope, PersistedDiskImagePublication, SnapshotAlias, SnapshotId,
        SnapshotManager, SnapshotPublishMetadata, SnapshotRecord, SnapshotSource,
        TemplateBuildErrorReason, TemplateBuildInfo,
    };

    /// Catalog fixture holding one row and recording complete records passed to delete.
    struct OneRowCatalog {
        record: SnapshotRecord,
        deleted: Arc<Mutex<Vec<SnapshotRecord>>>,
    }

    #[async_trait]
    impl SnapshotCatalog for OneRowCatalog {
        async fn create(&self, _record: SnapshotRecord) -> RepositoryResult<SnapshotRecord> {
            unreachable!("these tests never create")
        }

        async fn publish_commit(
            &self,
            _commit: SnapshotCommit,
        ) -> RepositoryResult<SnapshotRecord> {
            unreachable!("these tests never commit")
        }

        async fn get(&self, id_or_alias: &str) -> RepositoryResult<Option<SnapshotRecord>> {
            // Resolvable reads exclude unfinished template records.
            Ok(self
                .names_it(id_or_alias)
                .then(|| self.record.clone())
                .filter(|record| record.committed.is_some()))
        }

        async fn get_scoped(
            &self,
            id_or_alias: &str,
            scope: CatalogReadScope,
        ) -> RepositoryResult<Option<SnapshotRecord>> {
            if matches!(scope, CatalogReadScope::AnyStatus) {
                return Ok(self.names_it(id_or_alias).then(|| self.record.clone()));
            }
            self.get(id_or_alias).await
        }

        /// Listing is unused by this fixture and intentionally empty.
        async fn list_page(
            &self,
            _filter: SnapshotListFilter,
        ) -> RepositoryResult<SnapshotListPage> {
            Ok(SnapshotListPage::single(Vec::new()))
        }

        async fn delete_record(&self, record: &SnapshotRecord) -> RepositoryResult<()> {
            self.deleted.lock().expect("deleted").push(record.clone());
            Ok(())
        }

        async fn resolve_alias(&self, _alias: &str) -> RepositoryResult<Option<SnapshotId>> {
            Ok(None)
        }

        async fn try_start_build(&self, _id: &SnapshotId) -> RepositoryResult<StartedBuild> {
            Err(RepositoryError::Unsupported {
                feature: "not part of this test".to_string(),
            })
        }

        async fn mark_build_error(
            &self,
            _id: &SnapshotId,
            _reason: TemplateBuildErrorReason,
        ) -> RepositoryResult<()> {
            Ok(())
        }
    }

    impl OneRowCatalog {
        fn names_it(&self, id_or_alias: &str) -> bool {
            id_or_alias == self.record.id.to_string()
                || self
                    .record
                    .alias
                    .as_ref()
                    .is_some_and(|alias| alias.as_ref() == id_or_alias)
        }
    }

    #[derive(Default)]
    struct CountingArtifacts {
        deletes: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl SnapshotArtifactStore for CountingArtifacts {
        async fn import_built_artifacts(
            &self,
            _metadata: &SnapshotPublishMetadata,
            _manifest: &crate::types::FirecrackerSnapshotManifest,
            _publications: &mut Vec<PersistedDiskImagePublication>,
        ) -> RepositoryResult<ImportedSnapshotArtifacts> {
            unreachable!("these tests never import")
        }

        async fn delete_artifacts(
            &self,
            _id: &SnapshotId,
            _publications: &[PersistedDiskImagePublication],
        ) {
            self.deletes.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct Surface {
        api: Arc<ApiImpl>,
        id: SnapshotId,
        deleted: Arc<Mutex<Vec<SnapshotRecord>>>,
        artifact_deletes: Arc<AtomicUsize>,
    }

    /// Builds a surface holding one committed or waiting snapshot.
    async fn surface(committed: bool) -> Surface {
        let id = SnapshotId::generate();
        let alias = SnapshotAlias::parse("an-orphan").expect("alias parses");
        let now = 1_700_000_000_000;
        let mut record = SnapshotRecord {
            id: id.clone(),
            alias: Some(alias),
            source: if committed {
                SnapshotSource::Sandbox {
                    source_sandbox_id: "sbx-long-gone".to_string(),
                }
            } else {
                SnapshotSource::Template {
                    build: TemplateBuildInfo::waiting(),
                }
            },
            resources: Default::default(),
            created_at_unix_ms: now,
            updated_at_unix_ms: now,
            committed: None,
            origin_node_id: None,
        };
        if committed {
            record.committed = Some(crate::snapshot::CommittedSnapshot::mock());
        }

        let deleted = Arc::new(Mutex::new(Vec::new()));
        let artifact_deletes = Arc::new(AtomicUsize::new(0));
        let catalog = Arc::new(OneRowCatalog {
            record,
            deleted: Arc::clone(&deleted),
        });

        let orchestrator = Orchestrator::with_in_memory_store(MockBackendFactory::new()).await;

        let snapshot_manager = Arc::new(SnapshotManager::from_parts(
            Arc::new(SnapshotRepository::new(
                catalog,
                Arc::new(CountingArtifacts {
                    deletes: Arc::clone(&artifact_deletes),
                }),
            )),
            Some(Arc::new(crate::snapshot::mock::MockSnapshotRuntimeResolver)),
            None,
        ));

        let api = Arc::new(ApiImpl::new(
            orchestrator,
            snapshot_manager,
            None,
            Vec::new(),
            crate::api::ResumeWiring::api_half_for_test(),
        ));

        Surface {
            api,
            id,
            deleted,
            artifact_deletes,
        }
    }

    async fn delete(api: &ApiImpl, snapshot_id: String) -> SnapshotsSnapshotIdDeleteResponse {
        api.snapshots_snapshot_id_delete(
            &Method::DELETE,
            &Host::from(http::uri::Authority::from_static("localhost")),
            &CookieJar::new(),
            &crate::api::impls::Claims,
            &models::SnapshotsSnapshotIdDeletePathParams { snapshot_id },
        )
        .await
        .expect("the handler answers")
    }

    #[tokio::test]
    async fn an_orphaned_snapshot_can_be_deleted_through_the_api() {
        let s = surface(true).await;

        let response = delete(&s.api, s.id.to_string()).await;

        assert!(
            matches!(
                response,
                SnapshotsSnapshotIdDeleteResponse::Status204_TheSnapshotWasDeletedSuccessfully
            ),
            "got {response:?}"
        );
        let deleted = s.deleted.lock().expect("deleted");
        assert_eq!(
            deleted.len(),
            1,
            "🔴 the assertion: it goes through the catalog, which is what reaches both stores"
        );
        assert_eq!(
            deleted[0].id, s.id,
            "and it deletes the snapshot that was asked for"
        );
        assert!(
            deleted[0].alias.is_some(),
            "the whole record travels, because unbinding the name needs it"
        );
        assert_eq!(
            s.artifact_deletes.load(Ordering::SeqCst),
            1,
            "and the bytes go with the row"
        );
    }

    #[tokio::test]
    async fn deleting_a_snapshot_nothing_holds_is_a_404() {
        let s = surface(true).await;

        let response = delete(&s.api, SnapshotId::generate().to_string()).await;

        assert!(
            matches!(
                response,
                SnapshotsSnapshotIdDeleteResponse::Status404_NotFound(_)
            ),
            "an operator who mistypes an id must not be told a snapshot was removed, got \
             {response:?}"
        );
        assert!(
            s.deleted.lock().expect("deleted").is_empty(),
            "and nothing may be deleted over a name that matched nothing"
        );
        assert_eq!(
            s.artifact_deletes.load(Ordering::SeqCst),
            0,
            "least of all somebody else's bytes"
        );
    }

    #[tokio::test]
    async fn a_template_that_has_never_been_built_can_still_be_deleted() {
        let s = surface(false).await;

        let response = delete(&s.api, s.id.to_string()).await;

        assert!(
            matches!(
                response,
                SnapshotsSnapshotIdDeleteResponse::Status204_TheSnapshotWasDeletedSuccessfully
            ),
            "got {response:?}"
        );
        assert_eq!(
            s.deleted.lock().expect("deleted").len(),
            1,
            "a row that exists and is not `ready` is still a row an operator can remove"
        );
    }
}
