use std::time::SystemTime;

use async_trait::async_trait;
use axum_extra::extract::CookieJar;
use chrono::{DateTime, Utc};
use headers::Host;
use http::Method;

use tracing::info;

use crate::node_registry::fleet::{FleetNode, FleetNodes};
use crate::observability::{DiskMetric, MachineInfo, NodeMetricsSnapshot, NodeSnapshot};
use crate::orchestrator::{PausedRegistryListEntry, PausedRegistryState};
use crate::proto::scheduler as scheduler_proto;
use crate::snapshot::CatalogReadScope;
use agentenv_http_server::types::Nullable;
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

/// Maps node-observable state to its public status.
fn node_status(node: &NodeSnapshot) -> models::NodeStatus {
    if node.draining {
        models::NodeStatus::NodeStatusDraining
    } else {
        models::NodeStatus::NodeStatusReady
    }
}

impl From<NodeSnapshot> for models::Node {
    fn from(node: NodeSnapshot) -> Self {
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

/// Renders the optional `clusterID` filter the way the registry reads it.
///
/// An empty filter spans every cluster, which is what an absent parameter means.
fn cluster_filter(cluster_id: Option<uuid::Uuid>) -> String {
    cluster_id.map(|id| id.to_string()).unwrap_or_default()
}

/// Maps a requested node status to the scheduling override it names.
///
/// Only `ready` and `draining` are operator-settable; every other status is
/// derived by the scheduler and refused.
fn scheduling_override(status: models::NodeStatus) -> Result<bool, NodesNodeIdPostResponse> {
    match status {
        models::NodeStatus::NodeStatusDraining => Ok(true),
        models::NodeStatus::NodeStatusReady => Ok(false),
        status => Err(NodesNodeIdPostResponse::Status409_Conflict(ApiImpl::error(
            409,
            format!("node status {status} is derived by the scheduler and cannot be set"),
        ))),
    }
}

/// Maps a scheduler-observed status to its REST spelling.
///
/// The gateway spells the same statuses out of the same RPC
/// (`services/gateway/internal/node_list.go`); one node must not read
/// differently by which address asked.
fn observed_status(status: scheduler_proto::NodeStatus) -> models::NodeStatus {
    match status {
        scheduler_proto::NodeStatus::Ready => models::NodeStatus::NodeStatusReady,
        scheduler_proto::NodeStatus::Draining => models::NodeStatus::NodeStatusDraining,
        scheduler_proto::NodeStatus::Unhealthy => models::NodeStatus::NodeStatusUnhealthy,
        scheduler_proto::NodeStatus::Lingering => models::NodeStatus::NodeStatusLingering,
        // A derived observed view never leaves the status unspecified.
        scheduler_proto::NodeStatus::Connecting | scheduler_proto::NodeStatus::Unspecified => {
            models::NodeStatus::NodeStatusConnecting
        }
    }
}

fn observed_machine_info(
    machine_info: Option<scheduler_proto::MachineInfo>,
) -> models::MachineInfo {
    let machine_info = machine_info.unwrap_or_default();
    models::MachineInfo::new(
        machine_info.cpu_family,
        machine_info.cpu_model,
        machine_info.cpu_model_name,
        machine_info.cpu_architecture,
    )
}

fn observed_metrics(snapshot: &scheduler_proto::NodeSnapshot) -> models::NodeMetrics {
    models::NodeMetrics::new(
        snapshot.allocated_cpu,
        snapshot.cpu_percent,
        snapshot.cpu_count,
        snapshot.allocated_memory_bytes,
        snapshot.memory_used_bytes,
        snapshot.memory_total_bytes,
        snapshot
            .disks
            .iter()
            .map(|disk| {
                models::DiskMetrics::new(
                    disk.mount_point.clone(),
                    disk.device.clone(),
                    disk.filesystem_type.clone(),
                    disk.used_bytes,
                    disk.total_bytes,
                )
            })
            .collect(),
        snapshot.paused_allocated_cpu,
        snapshot.paused_allocated_memory_bytes,
    )
}

/// Renders one cluster-observed node as a collection entry.
fn observed_node_model(observed: scheduler_proto::ObservedNode) -> models::Node {
    let snapshot = observed.snapshot.unwrap_or_default();
    models::Node::new(
        observed.version,
        observed.commit,
        observed.node_id,
        observed.service_instance_id,
        observed.cluster_id,
        observed_machine_info(observed.machine_info),
        observed_status(snapshot.status()),
        snapshot.sandbox_count,
        observed_metrics(&snapshot),
        snapshot.create_successes,
        snapshot.create_fails,
        snapshot.sandbox_starting_count,
        snapshot.paused_sandbox_count,
    )
}

/// Renders one cluster-observed node as its detail view.
///
/// Cached builds are a node-local inventory the heartbeat does not carry.
fn observed_node_detail(observed: scheduler_proto::ObservedNode) -> models::NodeDetail {
    let snapshot = observed.snapshot.unwrap_or_default();
    models::NodeDetail::new(
        observed.cluster_id,
        observed.version,
        observed.commit,
        observed.node_id,
        observed.service_instance_id,
        observed_machine_info(observed.machine_info),
        observed_status(snapshot.status()),
        snapshot.sandbox_count,
        observed_metrics(&snapshot),
        vec![],
        snapshot.create_successes,
        snapshot.create_fails,
        snapshot.paused_sandbox_count,
    )
}

/// Renders an absent deadline as JSON null.
///
/// An absent lease means "already expired" and an absent sandbox deadline means
/// "never expires"; zero would be the same number for both.
fn nullable_unix_ms(at: Option<DateTime<Utc>>) -> Nullable<i64> {
    match at {
        Some(at) => Nullable::Present(at.timestamp_millis()),
        None => Nullable::Null,
    }
}

impl From<&PausedRegistryListEntry> for models::RegistrySandbox {
    fn from(entry: &PausedRegistryListEntry) -> Self {
        models::RegistrySandbox::new(
            entry.sandbox_id.to_string(),
            entry.cluster_id.to_string(),
            entry.state.as_str().to_string(),
            entry.generation,
            entry.origin_node_id.clone(),
            entry.claimed_by_node_id.clone().unwrap_or_default(),
            entry
                .snapshot_id
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_default(),
            entry.holder().to_string(),
            entry.paused_at.timestamp_millis(),
            entry.updated_at.timestamp_millis(),
            nullable_unix_ms(entry.lease_expires_at),
            nullable_unix_ms(entry.sandbox_expires_at),
            entry
                .execution_id
                .map(|id| id.to_string())
                .unwrap_or_default(),
        )
    }
}

/// Resolves the `state` filter against the registry's own vocabulary.
fn registry_state_filter(raw: Option<&str>) -> Result<Option<PausedRegistryState>, String> {
    let trimmed = raw.unwrap_or_default().trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    if let Some(state) = PausedRegistryState::ALL
        .into_iter()
        .find(|state| state.as_str().eq_ignore_ascii_case(trimmed))
    {
        return Ok(Some(state));
    }
    let known: Vec<&str> = PausedRegistryState::ALL
        .iter()
        .copied()
        .map(PausedRegistryState::as_str)
        .collect();
    Err(format!(
        "unknown state '{trimmed}', must be one of {}",
        known.join(", ")
    ))
}

#[async_trait]
impl Admin<()> for ApiImpl {
    type Claims = super::Claims;

    /// Lists the cluster's nodes, or this process's own report when it keeps no
    /// cluster view — a node's `/nodes` is its self-report surface.
    async fn nodes_get(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        query_params: &models::NodesGetQueryParams,
    ) -> Result<NodesGetResponse, ()> {
        let cluster_id = cluster_filter(query_params.cluster_id);
        if let FleetNodes::Cluster(nodes) = self.node_fleet().list(&cluster_id, SystemTime::now()) {
            return Ok(NodesGetResponse::Status200_SuccessfullyReturnedAllNodes(
                nodes.into_iter().map(observed_node_model).collect(),
            ));
        }

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

    /// Reads one cluster node, or this process's own report when it keeps no
    /// cluster view.
    async fn nodes_node_id_get(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::NodesNodeIdGetPathParams,
        query_params: &models::NodesNodeIdGetQueryParams,
    ) -> Result<NodesNodeIdGetResponse, ()> {
        let cluster_id = cluster_filter(query_params.cluster_id);
        match self
            .node_fleet()
            .get(&path_params.node_id, &cluster_id, SystemTime::now())
        {
            FleetNode::Observed(observed) => {
                return Ok(
                    NodesNodeIdGetResponse::Status200_SuccessfullyReturnedTheNode(
                        observed_node_detail(*observed),
                    ),
                );
            }
            FleetNode::Absent => {
                return Ok(NodesNodeIdGetResponse::Status404_NotFound(Self::error(
                    404,
                    format!("node {} not found", path_params.node_id),
                )));
            }
            FleetNode::SelfReport => {}
        }

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

    /// Sets a node to `ready` or `draining`; scheduler-derived statuses return 409.
    ///
    /// A cluster-observed node is set over its node service. The reply carries
    /// no status: the observed one catches up on that node's next heartbeat,
    /// so a read immediately after this call may still see the old value.
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
        let cluster_id = cluster_filter(query_params.cluster_id.or(body.cluster_id));
        match self
            .node_fleet()
            .get(&path_params.node_id, &cluster_id, SystemTime::now())
        {
            FleetNode::Observed(observed) => {
                let disabled = match scheduling_override(body.status) {
                    Ok(disabled) => disabled,
                    Err(refused) => return Ok(refused),
                };
                if let Err(err) =
                    crate::node_client::override_node_status(&observed.endpoint, disabled).await
                {
                    return Ok(NodesNodeIdPostResponse::Status500_ServerError(Self::error(
                        500,
                        format!("{err:#}"),
                    )));
                }
                return Ok(NodesNodeIdPostResponse::Status204_TheNodeStatusWasChangedSuccessfully);
            }
            FleetNode::Absent => {
                return Ok(NodesNodeIdPostResponse::Status404_NotFound(Self::error(
                    404,
                    format!("node {} not found", path_params.node_id),
                )));
            }
            FleetNode::SelfReport => {}
        }

        let Some(observability) = self.observability() else {
            return Ok(NodesNodeIdPostResponse::Status404_NotFound(Self::error(
                404,
                "observability is disabled on this node",
            )));
        };

        // Both route and optional cluster identities must address this node.
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

        let disabled = match scheduling_override(body.status) {
            Ok(disabled) => disabled,
            Err(refused) => return Ok(refused),
        };

        self.orchestrator().set_scheduling_disabled(disabled);

        Ok(NodesNodeIdPostResponse::Status204_TheNodeStatusWasChangedSuccessfully)
    }

    /// Lists the cluster paused-sandbox registry, newest database clock included.
    ///
    /// A deployment without a cluster-backed registry answers 501: that is a
    /// property of the configuration, not something a retry changes.
    async fn registry_sandboxes_get(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        query_params: &models::RegistrySandboxesGetQueryParams,
    ) -> Result<RegistrySandboxesGetResponse, ()> {
        let registry = self.paused.registry();
        if !registry.is_cluster_backed() {
            return Ok(
                RegistrySandboxesGetResponse::Status501_ThisDeploymentIsNotConfiguredWithAClusterRegistry(
                    Self::error(501, "paused registry is not configured"),
                ),
            );
        }

        let state_filter = match registry_state_filter(query_params.state.as_deref()) {
            Ok(state_filter) => state_filter,
            Err(message) => {
                return Ok(RegistrySandboxesGetResponse::Status400_BadRequest(
                    Self::error(400, message),
                ));
            }
        };

        let listing = match registry.list_all().await {
            Ok(listing) => listing,
            Err(err) => {
                return Ok(
                    RegistrySandboxesGetResponse::Status503_TheRegistryCouldNotBeRead(Self::error(
                        503,
                        format!("paused registry unavailable: {err}"),
                    )),
                );
            }
        };

        // The node filter compares against the holder as the registry stores it;
        // node aliases are resolved by the node registry, which is not on this path.
        let node_filter = query_params.node_id.as_deref().unwrap_or_default().trim();
        let page_token = query_params
            .next_token
            .as_deref()
            .unwrap_or_default()
            .trim();

        let mut matched: Vec<PausedRegistryListEntry> = listing
            .sandboxes
            .into_iter()
            .filter(|entry| state_filter.is_none_or(|state| entry.state == state))
            .filter(|entry| node_filter.is_empty() || entry.holder() == node_filter)
            .filter(|entry| {
                page_token.is_empty() || entry.sandbox_id.to_string().as_str() > page_token
            })
            .collect();

        // Keyset paging needs a total order the backend does not promise.
        matched.sort_by_key(|entry| entry.sandbox_id);

        let mut page =
            models::RegistrySandboxListing::new(Vec::new(), listing.now.timestamp_millis());
        let page_size = query_params.limit.unwrap_or(0) as usize;
        if page_size > 0 && page_size < matched.len() {
            matched.truncate(page_size);
            page.next_token = Some(
                matched
                    .last()
                    .expect("truncate to a positive page_size leaves at least one row")
                    .sandbox_id
                    .to_string(),
            );
        }
        page.sandboxes = matched.iter().map(models::RegistrySandbox::from).collect();

        Ok(RegistrySandboxesGetResponse::Status200_SuccessfullyReturnedTheRegistryPage(page))
    }

    /// Deletes a snapshot's catalog records, alias, and artifacts.
    ///
    /// Missing snapshots return 404, and all snapshot statuses are addressable.
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

#[cfg(test)]
mod registry_listing_tests {
    use std::sync::Arc;
    use std::time::SystemTime;

    use async_trait::async_trait;
    use axum_extra::extract::CookieJar;
    use chrono::{DateTime, TimeZone, Utc};
    use headers::Host;
    use http::Method;
    use serde_json::{json, Value};
    use uuid::Uuid;

    use agentenv_http_server::apis::admin::*;
    use agentenv_http_server::models;

    use super::ApiImpl;
    use crate::identity::NodeIdentity;
    use crate::orchestrator::{
        BeganPause, DeadlineRenewalOutcome, HeldSandbox, InMemoryMetadataStore, MarkRunningOutcome,
        Orchestrator, PausedRegistryError, PausedRegistryListEntry, PausedRegistryListing,
        PausedRegistryRows, PausedRegistryState, PausedSandboxEntry, PausedSandboxRegistry,
        ReclaimedHoldings, RegistryResult, ReleasedHoldings, ResumeClaim,
    };
    use crate::orchestrator::{DisabledPausedSandboxRegistry, FileBackedSandboxPersister};
    use crate::sandbox::mock::MockBackendFactory;
    use crate::snapshot::mock::mock_snapshot_manager;
    use crate::snapshot::SnapshotId;
    use crate::types::{ExecutionId, SandboxId};

    /// Registry that only answers the two calls this endpoint is allowed to make.
    struct ListingRegistry {
        listing: PausedRegistryListing,
        unreadable: bool,
    }

    impl ListingRegistry {
        fn holding(sandboxes: Vec<PausedRegistryListEntry>) -> Arc<Self> {
            Arc::new(Self {
                listing: PausedRegistryListing {
                    sandboxes,
                    now: at(1_700_000_009_000),
                },
                unreadable: false,
            })
        }

        fn unreadable() -> Arc<Self> {
            Arc::new(Self {
                listing: PausedRegistryListing {
                    sandboxes: Vec::new(),
                    now: at(0),
                },
                unreadable: true,
            })
        }
    }

    #[async_trait]
    impl PausedSandboxRegistry for ListingRegistry {
        async fn list_all(&self) -> RegistryResult<PausedRegistryListing> {
            if self.unreadable {
                return Err(PausedRegistryError::Backend {
                    operation: "list_all",
                    source: anyhow::anyhow!("the database is down"),
                });
            }
            Ok(self.listing.clone())
        }

        fn is_cluster_backed(&self) -> bool {
            true
        }

        async fn begin_pause(&self, _entry: &PausedSandboxEntry) -> RegistryResult<BeganPause> {
            unimplemented!("the listing endpoint only reads")
        }
        async fn complete_pause(
            &self,
            _sandbox_id: &SandboxId,
            _generation: i64,
            _snapshot_id: &SnapshotId,
        ) -> RegistryResult<()> {
            unimplemented!("the listing endpoint only reads")
        }
        async fn mark_local_only(
            &self,
            _sandbox_id: &SandboxId,
            _generation: i64,
        ) -> RegistryResult<()> {
            unimplemented!("the listing endpoint only reads")
        }
        async fn get(&self, _sandbox_id: &SandboxId) -> RegistryResult<Option<PausedSandboxEntry>> {
            unimplemented!("the listing endpoint only reads")
        }
        async fn get_many(&self, _sandbox_ids: &[SandboxId]) -> RegistryResult<PausedRegistryRows> {
            unimplemented!("the listing endpoint only reads")
        }
        async fn claim_for_resume(
            &self,
            _sandbox_id: &SandboxId,
            _node_id: &str,
            _execution_id: ExecutionId,
        ) -> RegistryResult<ResumeClaim> {
            unimplemented!("the listing endpoint only reads")
        }
        async fn release_claim(
            &self,
            _sandbox_id: &SandboxId,
            _generation: i64,
        ) -> RegistryResult<bool> {
            unimplemented!("the listing endpoint only reads")
        }
        async fn renew_lease(&self, _node_id: &str, _held: &[HeldSandbox]) -> RegistryResult<u64> {
            unimplemented!("the listing endpoint only reads")
        }
        async fn reclaim_expired_holdings(&self) -> RegistryResult<ReclaimedHoldings> {
            unimplemented!("the listing endpoint only reads")
        }
        async fn mark_running(
            &self,
            _sandbox_id: &SandboxId,
            _node_id: &str,
            _holder_node_id: &str,
            _execution_id: ExecutionId,
            _expires_at: Option<SystemTime>,
        ) -> RegistryResult<MarkRunningOutcome> {
            unimplemented!("the listing endpoint only reads")
        }
        async fn renew_sandbox_deadline(
            &self,
            _sandbox_id: &SandboxId,
            _execution_id: ExecutionId,
            _expires_at: Option<SystemTime>,
        ) -> RegistryResult<DeadlineRenewalOutcome> {
            unimplemented!("the listing endpoint only reads")
        }
        async fn release_node_holdings(&self, _node_id: &str) -> RegistryResult<ReleasedHoldings> {
            unimplemented!("the listing endpoint only reads")
        }
        async fn remove(&self, _sandbox_id: &SandboxId, _generation: i64) -> RegistryResult<bool> {
            unimplemented!("the listing endpoint only reads")
        }
    }

    fn at(unix_ms: i64) -> DateTime<Utc> {
        Utc.timestamp_millis_opt(unix_ms).single().expect("a time")
    }

    fn sandbox_id(nth: u8) -> SandboxId {
        SandboxId::from_uuid(
            Uuid::parse_str(&format!("0192a0{nth:02}-0000-7000-8000-000000000000"))
                .expect("a uuid"),
        )
    }

    /// A row with every optional column populated.
    fn full_row() -> PausedRegistryListEntry {
        PausedRegistryListEntry {
            sandbox_id: sandbox_id(1),
            cluster_id: Uuid::parse_str("11111111-2222-3333-4444-555555555555").expect("a uuid"),
            state: PausedRegistryState::Running,
            generation: 7,
            origin_node_id: "node-a".to_string(),
            claimed_by_node_id: Some("node-b".to_string()),
            snapshot_id: Some(SnapshotId::generate()),
            paused_at: at(1_700_000_001_000),
            updated_at: at(1_700_000_002_000),
            lease_expires_at: Some(at(1_700_000_003_000)),
            sandbox_expires_at: Some(at(1_700_000_004_000)),
            execution_id: Some(ExecutionId::from_uuid(
                Uuid::parse_str("0192b000-0000-7000-8000-000000000000").expect("a uuid"),
            )),
        }
    }

    /// The same row with every nullable column absent.
    fn empty_row(nth: u8) -> PausedRegistryListEntry {
        PausedRegistryListEntry {
            sandbox_id: sandbox_id(nth),
            claimed_by_node_id: None,
            snapshot_id: None,
            lease_expires_at: None,
            sandbox_expires_at: None,
            execution_id: None,
            state: PausedRegistryState::Paused,
            ..full_row()
        }
    }

    async fn api_over(registry: Arc<dyn PausedSandboxRegistry>) -> Arc<ApiImpl> {
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
        let snapshot_manager = Arc::new(mock_snapshot_manager());

        Arc::new(ApiImpl::new(
            orchestrator,
            Arc::clone(&snapshot_manager),
            None,
            crate::api::PausedSandboxWiring::new(
                registry,
                snapshot_manager,
                &NodeIdentity::from_config(&Default::default()),
            ),
            Vec::new(),
            crate::api::ResumeWiring::node_local(NodeIdentity::from_config(&Default::default()).id),
        ))
    }

    fn params() -> models::RegistrySandboxesGetQueryParams {
        models::RegistrySandboxesGetQueryParams {
            state: None,
            node_id: None,
            limit: None,
            next_token: None,
        }
    }

    async fn list(
        api: &ApiImpl,
        query_params: models::RegistrySandboxesGetQueryParams,
    ) -> RegistrySandboxesGetResponse {
        api.registry_sandboxes_get(
            &Method::GET,
            &Host::from(http::uri::Authority::from_static("localhost")),
            &CookieJar::new(),
            &super::super::Claims,
            &query_params,
        )
        .await
        .expect("the handler answers")
    }

    fn body(response: RegistrySandboxesGetResponse) -> Value {
        match response {
            RegistrySandboxesGetResponse::Status200_SuccessfullyReturnedTheRegistryPage(page) => {
                serde_json::to_value(page).expect("the page serializes")
            }
            other => panic!("expected a page, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn the_page_carries_every_field_the_gateway_renders() {
        let row = full_row();
        let api = api_over(ListingRegistry::holding(vec![row.clone()])).await;

        let page = body(list(&api, params()).await);

        assert_eq!(
            page,
            json!({
                "sandboxes": [{
                    "sandboxID": row.sandbox_id.to_string(),
                    "clusterID": row.cluster_id.to_string(),
                    "state": "running",
                    "generation": 7,
                    "originNodeID": "node-a",
                    "claimedByNodeID": "node-b",
                    "snapshotID": row.snapshot_id.expect("a snapshot").to_string(),
                    "holderNodeID": "node-a",
                    "pausedAtUnixMs": 1_700_000_001_000i64,
                    "updatedAtUnixMs": 1_700_000_002_000i64,
                    "leaseExpiresAtUnixMs": 1_700_000_003_000i64,
                    "sandboxExpiresAtUnixMs": 1_700_000_004_000i64,
                    "executionID": "0192b000-0000-7000-8000-000000000000",
                }],
                "databaseTimeUnixMs": 1_700_000_009_000i64,
            }),
            "the REST page is the gateway's registrySandboxItem field for field"
        );
    }

    #[tokio::test]
    async fn an_absent_lease_is_json_null_and_never_zero() {
        let api = api_over(ListingRegistry::holding(vec![empty_row(1)])).await;

        let page = body(list(&api, params()).await);

        let row = &page["sandboxes"][0];
        assert_eq!(
            row["leaseExpiresAtUnixMs"],
            Value::Null,
            "🔴 a null lease means already expired; zero would read as a deadline"
        );
        assert_eq!(
            row["sandboxExpiresAtUnixMs"],
            Value::Null,
            "🔴 a null sandbox deadline means never expires, the opposite meaning"
        );
        assert!(
            row.as_object()
                .expect("an object")
                .contains_key("leaseExpiresAtUnixMs"),
            "and the key stays present, so absent cannot be confused with unset"
        );
        assert_eq!(row["claimedByNodeID"], json!(""));
        assert_eq!(row["snapshotID"], json!(""));
        assert_eq!(row["executionID"], json!(""));
    }

    #[tokio::test]
    async fn a_lease_at_the_unix_epoch_stays_a_number() {
        let mut row = empty_row(1);
        row.lease_expires_at = Some(at(0));
        let api = api_over(ListingRegistry::holding(vec![row])).await;

        let page = body(list(&api, params()).await);

        assert_eq!(
            page["sandboxes"][0]["leaseExpiresAtUnixMs"],
            json!(0),
            "the option is carried, not encoded into a sentinel value"
        );
    }

    #[tokio::test]
    async fn the_last_page_omits_the_next_token() {
        let api = api_over(ListingRegistry::holding(vec![empty_row(1)])).await;

        let page = body(list(&api, params()).await);

        assert!(
            !page
                .as_object()
                .expect("an object")
                .contains_key("nextToken"),
            "got {page}"
        );
    }

    #[tokio::test]
    async fn a_limit_pages_by_sandbox_id() {
        let api = api_over(ListingRegistry::holding(vec![
            empty_row(3),
            empty_row(1),
            empty_row(2),
        ]))
        .await;

        let first = body(
            list(
                &api,
                models::RegistrySandboxesGetQueryParams {
                    limit: Some(2),
                    ..params()
                },
            )
            .await,
        );
        assert_eq!(
            first["sandboxes"][0]["sandboxID"],
            json!(sandbox_id(1).to_string())
        );
        assert_eq!(
            first["sandboxes"][1]["sandboxID"],
            json!(sandbox_id(2).to_string())
        );
        assert_eq!(first["nextToken"], json!(sandbox_id(2).to_string()));

        let second = body(
            list(
                &api,
                models::RegistrySandboxesGetQueryParams {
                    limit: Some(2),
                    next_token: first["nextToken"].as_str().map(str::to_string),
                    ..params()
                },
            )
            .await,
        );
        assert_eq!(second["sandboxes"].as_array().expect("rows").len(), 1);
        assert_eq!(
            second["sandboxes"][0]["sandboxID"],
            json!(sandbox_id(3).to_string())
        );
    }

    #[tokio::test]
    async fn the_filters_select_by_state_and_holder() {
        let mut other_node = empty_row(2);
        other_node.origin_node_id = "node-z".to_string();
        let api = api_over(ListingRegistry::holding(vec![full_row(), other_node])).await;

        let by_state = body(
            list(
                &api,
                models::RegistrySandboxesGetQueryParams {
                    state: Some("running".to_string()),
                    ..params()
                },
            )
            .await,
        );
        assert_eq!(by_state["sandboxes"].as_array().expect("rows").len(), 1);
        assert_eq!(by_state["sandboxes"][0]["state"], json!("running"));

        let by_node = body(
            list(
                &api,
                models::RegistrySandboxesGetQueryParams {
                    node_id: Some("node-z".to_string()),
                    ..params()
                },
            )
            .await,
        );
        assert_eq!(by_node["sandboxes"].as_array().expect("rows").len(), 1);
        assert_eq!(by_node["sandboxes"][0]["holderNodeID"], json!("node-z"));
    }

    #[tokio::test]
    async fn an_unknown_state_is_refused_rather_than_ignored() {
        let api = api_over(ListingRegistry::holding(vec![empty_row(1)])).await;

        let response = list(
            &api,
            models::RegistrySandboxesGetQueryParams {
                state: Some("asleep".to_string()),
                ..params()
            },
        )
        .await;

        assert!(
            matches!(
                response,
                RegistrySandboxesGetResponse::Status400_BadRequest(_)
            ),
            "🔴 a filter that silently does not apply returns every row with a 200, got \
             {response:?}"
        );
    }

    #[tokio::test]
    async fn a_deployment_without_a_cluster_registry_answers_501() {
        let api = api_over(Arc::new(DisabledPausedSandboxRegistry)).await;

        let response = list(&api, params()).await;

        assert!(
            matches!(
                response,
                RegistrySandboxesGetResponse::Status501_ThisDeploymentIsNotConfiguredWithAClusterRegistry(_)
            ),
            "a permanent property of the configuration is not a 503 a retry can fix, got \
             {response:?}"
        );
    }

    #[tokio::test]
    async fn an_unreadable_registry_answers_503() {
        let api = api_over(ListingRegistry::unreadable()).await;

        let response = list(&api, params()).await;

        assert!(
            matches!(
                response,
                RegistrySandboxesGetResponse::Status503_TheRegistryCouldNotBeRead(_)
            ),
            "got {response:?}"
        );
    }

    #[test]
    fn the_query_parameters_stay_a_closed_set() {
        // 🔴 Exhaustive destructuring: a fifth parameter — an executionID filter
        // above all — stops compiling here before it can reach the spec.
        let models::RegistrySandboxesGetQueryParams {
            state,
            node_id,
            limit,
            next_token,
        } = params();
        assert!(
            state.is_none() && node_id.is_none() && limit.is_none() && next_token.is_none(),
            "the default page asks for nothing"
        );
    }
}

#[cfg(test)]
mod fleet_node_tests {
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};

    use axum_extra::extract::CookieJar;
    use headers::Host;
    use http::Method;
    use serde_json::{json, Value};

    use agentenv_http_server::apis::admin::*;
    use agentenv_http_server::models;

    use super::{observed_node_detail, observed_node_model, observed_status, ApiImpl};
    use crate::identity::NodeIdentity;
    use crate::node_registry::registry::{AtomicNodeRegistry, NodeRegistry};
    use crate::node_registry::types::Node as DiscoveredNode;
    use crate::orchestrator::{
        DisabledPausedSandboxRegistry, FileBackedSandboxPersister, InMemoryMetadataStore,
        Orchestrator,
    };
    use crate::proto::scheduler as scheduler_proto;
    use crate::sandbox::mock::MockBackendFactory;
    use crate::snapshot::mock::mock_snapshot_manager;

    /// An observed node whose every field carries a value distinct from the rest.
    fn sentinel_observed() -> scheduler_proto::ObservedNode {
        scheduler_proto::ObservedNode {
            node_id: "node-a".to_string(),
            endpoint: "http://10.0.0.1:8080".to_string(),
            cluster_id: "cluster-1".to_string(),
            service_instance_id: "svc-a".to_string(),
            version: "0.1.2".to_string(),
            commit: "abcdef0".to_string(),
            machine_info: Some(scheduler_proto::MachineInfo {
                cpu_family: "6".to_string(),
                cpu_model: "143".to_string(),
                cpu_model_name: "Xeon Platinum".to_string(),
                cpu_architecture: "x86_64".to_string(),
                cpu_config_json: "{}".to_string(),
            }),
            snapshot: Some(scheduler_proto::NodeSnapshot {
                status: scheduler_proto::NodeStatus::Ready as i32,
                allocated_cpu: 11,
                allocated_memory_bytes: 12,
                cpu_percent: 13,
                cpu_count: 14,
                memory_used_bytes: 15,
                memory_total_bytes: 16,
                disks: vec![scheduler_proto::DiskMetric {
                    mount_point: "/".to_string(),
                    device: "/dev/sda1".to_string(),
                    filesystem_type: "ext4".to_string(),
                    used_bytes: 17,
                    total_bytes: 18,
                }],
                sandbox_count: 19,
                sandbox_starting_count: 20,
                create_successes: 21,
                create_fails: 22,
                reported_at_unix_ms: 1_700_000_000_000,
                paused_sandbox_count: 23,
                paused_allocated_cpu: 24,
                paused_allocated_memory_bytes: 25,
            }),
            last_seen_unix_ms: 1_700_000_000_001,
        }
    }

    fn value(model: impl serde::Serialize) -> Value {
        serde_json::to_value(model).expect("the model serializes")
    }

    #[test]
    fn a_listed_node_carries_every_field_the_gateway_renders() {
        // 🔴 Mirrors `nodeListItem`'s JSON tags in
        // services/gateway/internal/node_list.go. Two addresses, one shape.
        assert_eq!(
            value(observed_node_model(sentinel_observed())),
            json!({
                "version": "0.1.2",
                "commit": "abcdef0",
                "id": "node-a",
                "serviceInstanceID": "svc-a",
                "clusterID": "cluster-1",
                "machineInfo": {
                    "cpuFamily": "6",
                    "cpuModel": "143",
                    "cpuModelName": "Xeon Platinum",
                    "cpuArchitecture": "x86_64",
                },
                "status": "ready",
                "sandboxCount": 19,
                "metrics": {
                    "allocatedCPU": 11,
                    "cpuPercent": 13,
                    "cpuCount": 14,
                    "allocatedMemoryBytes": 12,
                    "memoryUsedBytes": 15,
                    "memoryTotalBytes": 16,
                    "disks": [{
                        "mountPoint": "/",
                        "device": "/dev/sda1",
                        "filesystemType": "ext4",
                        "usedBytes": 17,
                        "totalBytes": 18,
                    }],
                    "pausedAllocatedCPU": 24,
                    "pausedAllocatedMemoryBytes": 25,
                },
                "createSuccesses": 21,
                "createFails": 22,
                "sandboxStartingCount": 20,
                "sandboxPausedCount": 23,
            })
        );
    }

    #[test]
    fn a_node_detail_carries_the_same_reading_without_the_starting_count() {
        let detail = value(observed_node_detail(sentinel_observed()));
        let listed = value(observed_node_model(sentinel_observed()));

        let models::NodeDetail {
            cluster_id: _,
            version: _,
            commit: _,
            id: _,
            service_instance_id: _,
            machine_info: _,
            status: _,
            sandbox_count: _,
            metrics: _,
            cached_builds,
            create_successes: _,
            create_fails: _,
            sandbox_paused_count: _,
        } = observed_node_detail(sentinel_observed());
        assert!(
            cached_builds.is_empty(),
            "the heartbeat carries no cached-build inventory"
        );

        for (key, value) in listed.as_object().expect("an object") {
            if key == "sandboxStartingCount" {
                assert!(
                    !detail.as_object().expect("an object").contains_key(key),
                    "the detail schema has no starting count"
                );
                continue;
            }
            assert_eq!(detail.get(key), Some(value), "{key} disagrees");
        }
    }

    #[test]
    fn every_observed_status_keeps_the_gateway_spelling() {
        // 🔴 The right column is `nodeStatusToString` in node_list.go.
        for (observed, spelling) in [
            (scheduler_proto::NodeStatus::Ready, "ready"),
            (scheduler_proto::NodeStatus::Connecting, "connecting"),
            (scheduler_proto::NodeStatus::Unhealthy, "unhealthy"),
            (scheduler_proto::NodeStatus::Lingering, "lingering"),
            (scheduler_proto::NodeStatus::Draining, "draining"),
        ] {
            assert_eq!(
                observed_status(observed).to_string(),
                spelling,
                "{observed:?} must read the same through either address"
            );
        }
    }

    fn observing(nodes: &[&str]) -> Arc<AtomicNodeRegistry> {
        let registry = Arc::new(AtomicNodeRegistry::new(
            nodes
                .iter()
                .map(|id| DiscoveredNode {
                    id: (*id).to_string(),
                    endpoint: format!("http://{id}:8080"),
                    pod_name: (*id).to_string(),
                })
                .collect(),
            Duration::from_secs(30),
        ));
        for id in nodes {
            registry
                .heartbeat(
                    &scheduler_proto::HeartbeatRequest {
                        node_id: (*id).to_string(),
                        cluster_id: String::new(),
                        service_instance_id: format!("svc-{id}"),
                        snapshot: Some(scheduler_proto::NodeSnapshot {
                            status: scheduler_proto::NodeStatus::Ready as i32,
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                    SystemTime::now(),
                )
                .expect("the heartbeat lands");
        }
        registry
    }

    async fn api(fleet: Option<Arc<AtomicNodeRegistry>>) -> ApiImpl {
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
        let snapshot_manager = Arc::new(mock_snapshot_manager());
        let identity = NodeIdentity::from_config(&Default::default());

        let api = ApiImpl::new(
            orchestrator,
            Arc::clone(&snapshot_manager),
            None,
            crate::api::PausedSandboxWiring::new(
                Arc::new(DisabledPausedSandboxRegistry),
                snapshot_manager,
                &identity,
            ),
            Vec::new(),
            crate::api::ResumeWiring::node_local(identity.id.clone()),
        );
        match fleet {
            Some(registry) => api.with_node_fleet(registry as Arc<dyn NodeRegistry>),
            None => api,
        }
    }

    async fn list(api: &ApiImpl) -> Vec<models::Node> {
        let response = api
            .nodes_get(
                &Method::GET,
                &Host::from(http::uri::Authority::from_static("localhost")),
                &CookieJar::new(),
                &super::super::Claims,
                &models::NodesGetQueryParams { cluster_id: None },
            )
            .await
            .expect("the handler answers");
        match response {
            NodesGetResponse::Status200_SuccessfullyReturnedAllNodes(nodes) => nodes,
            other => panic!("expected a listing, got {other:?}"),
        }
    }

    async fn detail(api: &ApiImpl, node_id: &str) -> NodesNodeIdGetResponse {
        api.nodes_node_id_get(
            &Method::GET,
            &Host::from(http::uri::Authority::from_static("localhost")),
            &CookieJar::new(),
            &super::super::Claims,
            &models::NodesNodeIdGetPathParams {
                node_id: node_id.to_string(),
            },
            &models::NodesNodeIdGetQueryParams { cluster_id: None },
        )
        .await
        .expect("the handler answers")
    }

    #[tokio::test]
    async fn a_fleet_view_lists_every_observed_node() {
        let api = api(Some(observing(&["node-a", "node-b"]))).await;

        let mut ids: Vec<String> = list(&api).await.into_iter().map(|node| node.id).collect();
        ids.sort();

        assert_eq!(ids, vec!["node-a".to_string(), "node-b".to_string()]);
    }

    #[tokio::test]
    async fn without_a_fleet_view_the_local_report_still_answers() {
        // Observability is off here, so the self-report surface has nothing to
        // show — but it is still the surface being asked.
        let api = api(None).await;

        assert!(list(&api).await.is_empty());
    }

    #[tokio::test]
    async fn a_fleet_view_reads_one_node_from_the_cluster() {
        let api = api(Some(observing(&["node-a", "node-b"]))).await;

        match detail(&api, "node-b").await {
            NodesNodeIdGetResponse::Status200_SuccessfullyReturnedTheNode(node) => {
                assert_eq!(node.id, "node-b");
                assert_eq!(node.status, models::NodeStatus::NodeStatusReady);
            }
            other => panic!("expected the node, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_fleet_view_that_does_not_observe_a_node_is_a_404() {
        let api = api(Some(observing(&["node-a"]))).await;

        assert!(matches!(
            detail(&api, "node-z").await,
            NodesNodeIdGetResponse::Status404_NotFound(_)
        ));
    }

    async fn set_status(
        api: &ApiImpl,
        node_id: &str,
        status: models::NodeStatus,
    ) -> NodesNodeIdPostResponse {
        api.nodes_node_id_post(
            &Method::POST,
            &Host::from(http::uri::Authority::from_static("localhost")),
            &CookieJar::new(),
            &super::super::Claims,
            &models::NodesNodeIdPostPathParams {
                node_id: node_id.to_string(),
            },
            &models::NodesNodeIdPostQueryParams { cluster_id: None },
            &models::NodeStatusChange {
                cluster_id: None,
                status,
            },
        )
        .await
        .expect("the handler answers")
    }

    #[tokio::test]
    async fn a_status_change_for_an_unobserved_node_is_a_404() {
        let api = api(Some(observing(&["node-a"]))).await;

        assert!(matches!(
            set_status(&api, "node-z", models::NodeStatus::NodeStatusDraining).await,
            NodesNodeIdPostResponse::Status404_NotFound(_)
        ));
    }

    #[tokio::test]
    async fn a_scheduler_derived_status_is_refused_before_the_node_is_dialed() {
        // The fixture endpoints resolve nowhere; answering 409 rather than a
        // dial failure proves the refusal happens without a dial.
        let api = api(Some(observing(&["node-a"]))).await;

        assert!(matches!(
            set_status(&api, "node-a", models::NodeStatus::NodeStatusUnhealthy).await,
            NodesNodeIdPostResponse::Status409_Conflict(_)
        ));
    }

    #[tokio::test]
    async fn an_unreachable_observed_node_surfaces_as_a_500_not_a_local_flip() {
        let registry = Arc::new(AtomicNodeRegistry::new(
            vec![DiscoveredNode {
                id: "node-a".to_string(),
                // Nothing listens on port 1: the dial must fail, and the
                // failure must surface instead of flipping this process.
                endpoint: "http://127.0.0.1:1".to_string(),
                pod_name: "node-a".to_string(),
            }],
            Duration::from_secs(30),
        ));
        registry
            .heartbeat(
                &scheduler_proto::HeartbeatRequest {
                    node_id: "node-a".to_string(),
                    service_instance_id: "svc-node-a".to_string(),
                    snapshot: Some(scheduler_proto::NodeSnapshot {
                        status: scheduler_proto::NodeStatus::Ready as i32,
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                SystemTime::now(),
            )
            .expect("the heartbeat lands");
        let api = api(Some(registry)).await;

        assert!(matches!(
            set_status(&api, "node-a", models::NodeStatus::NodeStatusDraining).await,
            NodesNodeIdPostResponse::Status500_ServerError(_)
        ));
    }

    #[test]
    fn only_ready_and_draining_are_operator_settable() {
        use super::scheduling_override;

        assert!(!scheduling_override(models::NodeStatus::NodeStatusReady).expect("settable"));
        assert!(scheduling_override(models::NodeStatus::NodeStatusDraining).expect("settable"));
        assert!(scheduling_override(models::NodeStatus::NodeStatusUnhealthy).is_err());
        assert!(scheduling_override(models::NodeStatus::NodeStatusConnecting).is_err());
    }
}
