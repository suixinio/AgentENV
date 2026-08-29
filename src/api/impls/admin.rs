use async_trait::async_trait;
use axum_extra::extract::CookieJar;
use headers::Host;
use http::Method;

use tracing::info;

use crate::observability::{DiskMetric, MachineInfo, NodeMetricsSnapshot, NodeSnapshot};
use crate::snapshot::CatalogReadScope;
use agentenv_http_server::{apis::admin::*, models};

use super::ApiImpl;

impl From<MachineInfo> for models::MachineInfo {
    fn from(machine_info: MachineInfo) -> Self {
        models::MachineInfo::new(
            machine_info.cpu_family,
            machine_info.cpu_model,
            machine_info.cpu_model_name,
            machine_info.cpu_architecture,
        )
    }
}

impl From<DiskMetric> for models::DiskMetrics {
    fn from(disk: DiskMetric) -> Self {
        models::DiskMetrics::new(
            disk.mount_point,
            disk.device,
            disk.filesystem_type,
            disk.used_bytes,
            disk.total_bytes,
        )
    }
}

impl From<NodeMetricsSnapshot> for models::NodeMetrics {
    fn from(metrics: NodeMetricsSnapshot) -> Self {
        models::NodeMetrics::new(
            metrics.allocated_cpu,
            metrics.cpu_percent,
            metrics.cpu_count,
            metrics.allocated_memory_bytes,
            metrics.memory_used_bytes,
            metrics.memory_total_bytes,
            metrics
                .disks
                .into_iter()
                .map(models::DiskMetrics::from)
                .collect(),
            metrics.paused_allocated_cpu,
            metrics.paused_allocated_memory_bytes,
        )
    }
}

/// What a node reports about itself.
///
/// Only two of the statuses are the node's to claim: it knows whether it has
/// been taken out of rotation, and otherwise it is serving. CONNECTING and
/// UNHEALTHY describe how the *scheduler* is getting on with this node, and a
/// node claiming either would be describing something it cannot observe.
fn node_status(node: &NodeSnapshot) -> models::NodeStatus {
    if node.draining {
        models::NodeStatus::NodeStatusDraining
    } else {
        models::NodeStatus::NodeStatusReady
    }
}

impl From<NodeSnapshot> for models::Node {
    fn from(node: NodeSnapshot) -> Self {
        // Read the status before the fields below move out of `node`.
        let status = node_status(&node);
        models::Node::new(
            node.version,
            node.commit,
            node.node_id,
            node.service_instance_id,
            node.cluster_id.to_string(),
            node.machine_info.into(),
            status,
            node.sandbox_count,
            node.metrics.into(),
            node.create_successes,
            node.create_fails,
            node.sandbox_starting_count,
            node.paused_sandbox_count,
        )
    }
}

#[async_trait]
impl Admin<()> for ApiImpl {
    type Claims = super::Claims;

    async fn nodes_get(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        query_params: &models::NodesGetQueryParams,
    ) -> Result<NodesGetResponse, ()> {
        let Some(observability) = self.observability() else {
            // When observability is disabled, the collection endpoint exposes
            // no nodes rather than returning a partial or synthetic record.
            return Ok(NodesGetResponse::Status200_SuccessfullyReturnedAllNodes(
                vec![],
            ));
        };
        if query_params
            .cluster_id
            .is_some_and(|cluster_id| cluster_id != observability.cluster_id())
        {
            return Ok(NodesGetResponse::Status200_SuccessfullyReturnedAllNodes(
                vec![],
            ));
        }
        let node = match observability.node_snapshot().await {
            Ok(node) => node,
            Err(err) => {
                return Ok(NodesGetResponse::Status500_ServerError(Self::error(
                    500,
                    err.to_string(),
                )));
            }
        };
        Ok(NodesGetResponse::Status200_SuccessfullyReturnedAllNodes(
            vec![models::Node::from(node)],
        ))
    }

    async fn nodes_node_id_get(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::NodesNodeIdGetPathParams,
        query_params: &models::NodesNodeIdGetQueryParams,
    ) -> Result<NodesNodeIdGetResponse, ()> {
        let Some(observability) = self.observability() else {
            // A disabled observability service behaves like node details are
            // unavailable on this process.
            return Ok(NodesNodeIdGetResponse::Status404_NotFound(Self::error(
                404,
                "observability is disabled on this node",
            )));
        };
        let cluster_mismatch = query_params
            .cluster_id
            .map(|cluster_id| cluster_id != observability.cluster_id())
            .unwrap_or(false);
        if path_params.node_id != observability.node_id() || cluster_mismatch {
            return Ok(NodesNodeIdGetResponse::Status404_NotFound(Self::error(
                404,
                format!("node {} not found", path_params.node_id),
            )));
        }

        let node = match observability.node_snapshot().await {
            Ok(node) => node,
            Err(err) => {
                return Ok(NodesNodeIdGetResponse::Status500_ServerError(Self::error(
                    500,
                    err.to_string(),
                )));
            }
        };

        let status = node_status(&node);
        let detail = models::NodeDetail::new(
            node.cluster_id.to_string(),
            node.version,
            node.commit,
            node.node_id,
            node.service_instance_id,
            node.machine_info.into(),
            status,
            node.sandbox_count,
            node.metrics.into(),
            vec![],
            node.create_successes,
            node.create_fails,
            node.paused_sandbox_count,
        );
        Ok(NodesNodeIdGetResponse::Status200_SuccessfullyReturnedTheNode(detail))
    }

    /// Takes this node out of rotation, or puts it back.
    ///
    /// Only `ready` and `draining` are settable — see `node_status`. Asking for
    /// one of the derived statuses is answered with 409 rather than quietly
    /// ignored, so a caller that believes it parked a node never gets that
    /// belief for free.
    async fn nodes_node_id_post(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::NodesNodeIdPostPathParams,
        query_params: &models::NodesNodeIdPostQueryParams,
        body: &models::NodeStatusChange,
    ) -> Result<NodesNodeIdPostResponse, ()> {
        let Some(observability) = self.observability() else {
            return Ok(NodesNodeIdPostResponse::Status404_NotFound(Self::error(
                404,
                "observability is disabled on this node",
            )));
        };

        // Same check as the GET above, and it matters more here: this request
        // reached us through a gateway that resolved the node id to an
        // endpoint, and a routing mistake must not be allowed to park a node
        // nobody asked about. The cluster may be named in either place; both
        // have to agree with us.
        let cluster_mismatch = query_params
            .cluster_id
            .or(body.cluster_id)
            .map(|cluster_id| cluster_id != observability.cluster_id())
            .unwrap_or(false);
        if path_params.node_id != observability.node_id() || cluster_mismatch {
            return Ok(NodesNodeIdPostResponse::Status404_NotFound(Self::error(
                404,
                format!("node {} not found", path_params.node_id),
            )));
        }

        let disabled = match body.status {
            models::NodeStatus::NodeStatusDraining => true,
            models::NodeStatus::NodeStatusReady => false,
            status => {
                return Ok(NodesNodeIdPostResponse::Status409_Conflict(Self::error(
                    409,
                    format!("node status {status} is derived by the scheduler and cannot be set",),
                )));
            }
        };

        self.orchestrator().set_scheduling_disabled(disabled);

        Ok(NodesNodeIdPostResponse::Status204_TheNodeStatusWasChangedSuccessfully)
    }

    /// Removes one snapshot: its row in every catalog, its alias, its bytes.
    ///
    /// 🔴 An operator lever, and it is here rather than under `snapshots`
    /// because of what it is for. Every other delete in the tree is a
    /// consequence — a sandbox being deleted, a pause superseding the snapshot
    /// it replaces — and nothing reaches a snapshot whose sandbox is already
    /// gone. Those accumulate, and until now the only way to remove one was a
    /// `psql` prompt: a row deleted straight out of PostgreSQL leaves the
    /// artifacts sitting in object storage with nothing naming them, and no
    /// route left to find them. This goes through the catalog and then removes
    /// the alias and the bytes.
    ///
    /// 🔴 Not wired into anything automatic, and it must not be. The one
    /// caller in the tree that deletes on its own judgement is a cross-node
    /// resume dropping a registry row, and giving that path a way to delete
    /// snapshots as well would turn a mirror that is briefly behind into a
    /// mirror that destroys the thing it is behind on.
    ///
    /// 404 rather than a courteous 204 for a snapshot no catalog holds: the
    /// repository's delete is idempotent, but an operator who mistypes an id
    /// should hear that nothing matched instead of being told a snapshot was
    /// removed. Asked at every status, because a template that never built is
    /// `waiting` and a failed one is `error`, and a delete that cannot see
    /// those rows cannot remove them either.
    async fn snapshots_snapshot_id_delete(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
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

        // Named before the delete, and at `info`: this is the one place a
        // snapshot goes away because a person asked, and the record is the only
        // description of what went with it.
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

/// The operator lever, and the two ways it can be wrong.
///
/// 🔴 What it removes is the whole point of it: a row deleted straight out of
/// PostgreSQL leaves object storage holding a snapshot the database does not,
/// which is the population divergence the read-side guard refuses a switch
/// over. So these tests hold it to going through the catalog — where the double
/// write reaches both stores — and to taking the bytes with it.
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
    use crate::identity::NodeIdentity;
    use crate::orchestrator::{
        DisabledPausedSandboxRegistry, FileBackedSandboxPersister, InMemoryMetadataStore,
        Orchestrator,
    };
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

    /// One row, and a note of what was asked to be deleted.
    struct OneRowCatalog {
        record: SnapshotRecord,
        /// The records `delete_record` was handed, whole — the alias travels on
        /// the record, and unbinding it is what frees the name.
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
            // Resolvable: a `waiting` template is not one of these, which is
            // exactly what the scope below has to see past.
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

        /// 🔴 Empty on purpose, and not because the catalog is: this double
        /// holds one row and answers `get`/`get_scoped` with it. The admin
        /// surfaces under test read snapshots by id and never list, so a
        /// listing that started returning the row would be asserting nothing.
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

    /// Counts the one call that removes bytes.
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

    /// A surface holding one snapshot. `committed` picks whether it is a
    /// finished sandbox snapshot or a template that has never been built.
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

        let root = tempfile::tempdir().expect("a temp dir");
        let orchestrator = Orchestrator::new(
            crate::sandbox::AccessTokenSeedPolicy::MayGenerate,
            InMemoryMetadataStore::new(),
            MockBackendFactory::new(),
            FileBackedSandboxPersister::new_for_test(root.path().to_path_buf()),
            crate::image::DisabledRuntimeImageRefs::shared(),
        )
        .await
        .expect("an orchestrator");
        std::mem::forget(root);

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
            Arc::clone(&snapshot_manager),
            None,
            crate::api::PausedSandboxWiring::new(
                Arc::new(DisabledPausedSandboxRegistry),
                Arc::clone(&snapshot_manager),
                &NodeIdentity::from_config(&Default::default()),
            ),
            Vec::new(),
            // 🔴 `node_local`, i.e. the `aenv-node` half: these fixtures
            // predate the split and assert the behaviour of a process that
            // runs the sandboxes it answers for.
            crate::api::ResumeWiring::node_local(NodeIdentity::from_config(&Default::default()).id),
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
            &super::super::Claims,
            &models::SnapshotsSnapshotIdDeletePathParams { snapshot_id },
        )
        .await
        .expect("the handler answers")
    }

    /// 🔴 The gap this closes. `DELETE /snapshots/{id}` answered 405, and the
    /// only remaining way to remove an orphaned snapshot was a `psql` prompt —
    /// which removes it from one catalog and manufactures the divergence the
    /// read-side guard exists to detect.
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

    /// A snapshot no catalog holds is not something that was just deleted.
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

    /// 🔴 Every status, not the resolvable ones. A template that has never
    /// built is `waiting` and a failed one is `error`; read at the resolvable
    /// scope both are absent, and the lever would answer 404 over rows that are
    /// sitting right there — which is the same defect `/templates/{id}` had.
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
