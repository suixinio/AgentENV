//! What happens when the data plane names a sandbox the routing projection
//! does not know: answer it if it is running, otherwise rebuild it from its
//! snapshot row and say where it landed.
//!
//! The gateway reaches this through `apiproxy.ResumeSandbox`. A sandbox that
//! is running is answered before any policy check; a paused one is woken only
//! when its row allows traffic-triggered wake-ups and the caller presented the
//! sandbox's envd token, when it has one.

use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use tonic::Code;
use tracing::{debug, info, warn};

use super::ApiImpl;
use crate::binding_store::lookup::LookupResultLabel;
use crate::cfg::ConfigManager;
use crate::node_registry::grpc_service::NodeRegistryGrpcService;
use crate::orchestrator::{
    NewTimeout, OrchestratorError, SandboxMetadata, SandboxState, StoreError,
};
use crate::snapshot::SnapshotRecord;
use crate::types::{ExecutionId, SandboxId};

/// A node the placement source named.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::api) struct PlacedNode {
    pub node_id: String,
    /// Where the gateway should send the request that triggered the wake-up.
    /// May be empty: a placement source that knows the node's identity but
    /// not its address is answering half a question, and half an answer is
    /// not an address. The gateway resolves the rest.
    pub address: String,
}

/// What the cluster knows about where a sandbox is, before anything is woken.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::api) enum ResumePlacement {
    /// The sandbox is running on `node` right now under `execution_id`, an
    /// incarnation the placement source vouches for. Nothing to wake.
    Running {
        node: PlacedNode,
        execution_id: ExecutionId,
        /// The answer came from the routing projection itself, so writing it
        /// back would only churn the record.
        from_projection: bool,
    },
    /// No node is running it. It is paused, or it does not exist; the snapshot
    /// row decides which.
    NotRunning,
}

/// Why the placement source could not name a node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::api) enum PlacementRefusal {
    /// The placement source could not be asked, or has not finished seeding.
    /// Retryable, and never an answer about whether the sandbox exists.
    Unavailable(String),
    /// Anything else the placement source said.
    Failed(String),
}

/// Source of the cluster's view of where a sandbox is running.
#[async_trait]
pub(in crate::api) trait ResumePlacementSource: Send + Sync {
    async fn locate(&self, sandbox_id: SandboxId) -> Result<ResumePlacement, PlacementRefusal>;

    /// The data-plane address of a node this process knows by identity.
    async fn address_of(&self, node_id: &str) -> Option<String>;
}

/// Writes the routing projection the data plane reads first.
///
/// Best effort: a failed write costs the next request a resume RPC, and the
/// node's next heartbeat repairs the record.
#[async_trait]
pub(in crate::api) trait RoutingProjectionWriter: Send + Sync {
    async fn record_running(
        &self,
        sandbox_id: SandboxId,
        node: &PlacedNode,
        execution_id: ExecutionId,
        projection_ttl_secs: u32,
    ) -> Result<(), String>;
}

/// Whether this process runs sandboxes on its own machine, and which machine
/// that is.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::api) enum WakeSite {
    /// The orchestration surface behind this `ApiImpl` runs sandboxes on this
    /// machine, named here. It holds no snapshot catalog, so it can only
    /// answer for what is running.
    Local(String),
    /// The orchestration surface places wake-ups on their selected machines.
    Remote,
}

/// Everything the resume surface needs beyond what `ApiImpl` already holds.
#[derive(Clone)]
pub struct ResumeWiring {
    placement: Option<Arc<dyn ResumePlacementSource>>,
    /// `None` where this process holds no projection to write: the node half,
    /// and test wirings that only inject a placement answer.
    projection: Option<Arc<dyn RoutingProjectionWriter>>,
    wake_site: WakeSite,
}

impl ResumeWiring {
    /// Constructor seam for injected placement and wake-site policy.
    #[cfg(test)]
    pub(in crate::api) fn new(
        placement: Option<Arc<dyn ResumePlacementSource>>,
        projection: Option<Arc<dyn RoutingProjectionWriter>>,
        wake_site: WakeSite,
    ) -> Self {
        Self {
            placement,
            projection,
            wake_site,
        }
    }

    /// Whether this wiring's orchestration surface runs sandboxes in process.
    pub(in crate::api) fn runs_sandboxes_here(&self) -> bool {
        matches!(self.wake_site, WakeSite::Local(_))
    }

    /// The node half: runs sandboxes here, consults nothing beyond its own
    /// records.
    pub fn node_local(node_id: impl Into<String>) -> Self {
        Self {
            placement: None,
            projection: None,
            wake_site: WakeSite::Local(node_id.into()),
        }
    }

    /// The api half with no cluster wired: tests that need the role only.
    pub fn api_half_for_test() -> Self {
        Self {
            placement: None,
            projection: None,
            wake_site: WakeSite::Remote,
        }
    }

    /// The api half over its own in-process node registry.
    pub fn cluster_in_process(local: NodeRegistryGrpcService) -> Self {
        let source = Arc::new(NativePlacementSource { local });
        Self {
            placement: Some(Arc::clone(&source) as Arc<dyn ResumePlacementSource>),
            projection: Some(source as Arc<dyn RoutingProjectionWriter>),
            wake_site: WakeSite::Remote,
        }
    }
}

/// The in-process node registry as the placement source.
struct NativePlacementSource {
    local: NodeRegistryGrpcService,
}

#[async_trait]
impl ResumePlacementSource for NativePlacementSource {
    async fn locate(&self, sandbox_id: SandboxId) -> Result<ResumePlacement, PlacementRefusal> {
        match self.local.lookup_sandbox(&sandbox_id.to_string()).await {
            Ok(answer) => {
                let execution_id = ExecutionId::parse_str(&answer.execution_id).map_err(|err| {
                    PlacementRefusal::Failed(format!(
                        "the placement source names sandbox {sandbox_id} running under an \
                             incarnation it cannot spell: {err}"
                    ))
                })?;
                Ok(ResumePlacement::Running {
                    node: PlacedNode {
                        node_id: answer.node.id,
                        address: answer.node.endpoint,
                    },
                    execution_id,
                    from_projection: answer.label == LookupResultLabel::BoundBinding,
                })
            }
            Err(status) => refusal_from_status(&status),
        }
    }

    async fn address_of(&self, node_id: &str) -> Option<String> {
        self.local.node_endpoint(node_id)
    }
}

#[async_trait]
impl RoutingProjectionWriter for NativePlacementSource {
    async fn record_running(
        &self,
        sandbox_id: SandboxId,
        node: &PlacedNode,
        execution_id: ExecutionId,
        projection_ttl_secs: u32,
    ) -> Result<(), String> {
        self.local
            .record_running(
                &sandbox_id.to_string(),
                &node.node_id,
                &execution_id.to_string(),
                projection_ttl_secs,
            )
            .await
            .map_err(|status| status.to_string())
    }
}

fn refusal_from_status(status: &tonic::Status) -> Result<ResumePlacement, PlacementRefusal> {
    match status.code() {
        Code::NotFound => Ok(ResumePlacement::NotRunning),
        Code::Unavailable => Err(PlacementRefusal::Unavailable(status.message().to_string())),
        _ => Err(PlacementRefusal::Failed(status.to_string())),
    }
}

/// What the caller presented about itself.
pub(in crate::api) struct DataPlaneResumeRequest {
    pub sandbox_id: SandboxId,
    /// The port the data-plane request was addressed to, when the caller said
    /// which. `None` means the caller did not say, which is treated as
    /// "possibly envd" — the strict direction.
    pub target_port: Option<u16>,
    /// The envd access token the caller presented, empty when it presented
    /// none.
    pub envd_access_token: String,
}

/// How a wake-up ended.
#[derive(Debug, PartialEq, Eq)]
pub(in crate::api) enum DataPlaneResume {
    /// The sandbox is running, here or wherever the placement said.
    Woken {
        node_id: String,
        node_address: String,
        execution_id: ExecutionId,
        /// It was running before this call; nothing was woken.
        already_running: bool,
    },
    /// The caller did not present the sandbox's envd access token.
    Unauthorized,
    /// Paused sandbox exists but policy forbids traffic-triggered wake-up.
    ///
    /// This is not `NotFound`; user-initiated resume remains valid.
    AutoResumeDisabled,
    /// Nothing anywhere knows this sandbox.
    NotFound,
    /// Another operation on the sandbox is in flight. Retryable in a moment.
    TransitionInProgress { holder: String },
    /// The cluster has no room.
    Exhausted(String),
    /// Nobody could be asked; never evidence that the sandbox is absent.
    Undecided(String),
    /// The wake-up was attempted and failed.
    Failed(String),
    /// Wake-up timed out; distinct from failure for metrics, not wire status.
    TimedOut,
}

impl ApiImpl {
    /// Wakes a paused sandbox for data-plane traffic.
    ///
    /// A sandbox already running is answered as it stands, before
    /// authorization and the auto-resume gate: the same answer a projection hit
    /// gives, and envd enforces the access token on the node.
    pub(in crate::api) async fn resume_for_data_plane(
        &self,
        request: DataPlaneResumeRequest,
    ) -> DataPlaneResume {
        let sandbox_id = request.sandbox_id;

        match self.locate_for_resume(sandbox_id).await {
            Ok(ResumePlacement::Running {
                node,
                execution_id,
                from_projection,
            }) => {
                if !from_projection {
                    // The projection missed a running sandbox: repair it now so the
                    // next request is a hit, budgeted from the sandbox's own record.
                    let projection_ttl_secs = self
                        .orchestrator
                        .get_sandbox(&sandbox_id)
                        .await
                        .ok()
                        .flatten()
                        .map(|metadata| metadata.projection_ttl_secs(SystemTime::now()))
                        .unwrap_or(0);
                    self.project_running(sandbox_id, &node, execution_id, projection_ttl_secs)
                        .await;
                }
                return DataPlaneResume::Woken {
                    node_id: node.node_id,
                    node_address: node.address,
                    execution_id,
                    already_running: true,
                };
            }
            Ok(ResumePlacement::NotRunning) => {}
            Err(refusal) => return refusal.into(),
        }

        // The placement source can lag a record this process wrote itself.
        match self.orchestrator.get_sandbox(&sandbox_id).await {
            Ok(Some(metadata)) => {
                if let Some(answer) = self.answer_recorded(metadata).await {
                    return answer;
                }
            }
            Ok(None) => {}
            Err(err) => return DataPlaneResume::Undecided(err.to_string()),
        }

        // Only the api half reads the catalog; a node that finds nothing
        // running has nothing more to say.
        let WakeSite::Remote = &self.resume_wiring.wake_site else {
            return DataPlaneResume::NotFound;
        };

        let record = match self.latest_paused_snapshot(sandbox_id).await {
            Ok(Some(record)) => record,
            Ok(None) => return DataPlaneResume::NotFound,
            Err(err) => {
                warn!(
                    %sandbox_id,
                    error = %format_args!("{err:#}"),
                    "could not read the snapshot catalog to wake a sandbox"
                );
                return DataPlaneResume::Undecided(err.to_string());
            }
        };
        let Some(paused) = record.paused_sandbox() else {
            return DataPlaneResume::NotFound;
        };
        if !self.authorize_envd(&request, paused.secure) {
            return DataPlaneResume::Unauthorized;
        }
        if !paused.auto_resume {
            return DataPlaneResume::AutoResumeDisabled;
        }

        self.wake(record).await
    }

    /// Answers for a sandbox this process records but the placement source
    /// did not name. `None` is a pause that finished while this call waited:
    /// the record is gone and the catalog row speaks for the sandbox.
    async fn answer_recorded(&self, metadata: SandboxMetadata) -> Option<DataPlaneResume> {
        let sandbox_id = metadata.id;
        let metadata = if metadata.state == SandboxState::Pausing {
            // The pause ends in the row this call would wake from.
            match self.orchestrator.wait_for_pause_to_settle(sandbox_id).await {
                Ok(Some(metadata)) => metadata,
                Ok(None) => return None,
                Err(OrchestratorError::InvalidSandboxState { .. }) => {
                    return Some(DataPlaneResume::TransitionInProgress {
                        holder: SandboxState::Pausing.to_string(),
                    })
                }
                Err(err) => return Some(DataPlaneResume::Undecided(err.to_string())),
            }
        } else {
            metadata
        };
        Some(match metadata.state {
            SandboxState::Running => {
                let node = self.node_running(sandbox_id).await;
                self.project_running(
                    sandbox_id,
                    &node,
                    metadata.execution_id,
                    metadata.projection_ttl_secs(SystemTime::now()),
                )
                .await;
                DataPlaneResume::Woken {
                    node_id: node.node_id,
                    node_address: node.address,
                    execution_id: metadata.execution_id,
                    already_running: true,
                }
            }
            SandboxState::Killing => DataPlaneResume::NotFound,
            state => {
                debug!(%sandbox_id, %state, "refusing to wake a sandbox mid-transition");
                DataPlaneResume::TransitionInProgress {
                    holder: state.to_string(),
                }
            }
        })
    }

    /// Rebuilds the sandbox from its row, bounded by the wake-up deadline.
    async fn wake(&self, record: SnapshotRecord) -> DataPlaneResume {
        let sandbox_id = match super::paused::paused_sandbox_id(&record) {
            Some(sandbox_id) => sandbox_id,
            None => return DataPlaneResume::NotFound,
        };
        let request = match Self::restore_request(record.clone(), Self::wake_timeout()) {
            Ok(request) => request,
            Err(err) => return DataPlaneResume::Failed(err.to_string()),
        };
        let attempt = tokio::time::timeout(
            auto_resume_deadline(),
            self.orchestrator()
                .restore_or_join_launch(sandbox_id, request),
        )
        .await;
        let attempt = match attempt {
            Ok(attempt) => attempt,
            Err(_) => {
                warn!(
                    %sandbox_id,
                    timeout_ms = auto_resume_deadline().as_millis(),
                    "waking a sandbox for the data plane timed out"
                );
                return DataPlaneResume::TimedOut;
            }
        };
        match attempt {
            Ok(crate::orchestrator::RestoredSandbox { metadata, joined }) => {
                if !joined {
                    self.note_resume_landing(&record, &metadata).await;
                }
                let node = self.node_running(sandbox_id).await;
                self.project_running(
                    sandbox_id,
                    &node,
                    metadata.execution_id,
                    metadata.projection_ttl_secs(SystemTime::now()),
                )
                .await;
                if joined {
                    info!(%sandbox_id, node_id = %node.node_id, "joined a wake-up of this sandbox already in flight");
                } else {
                    info!(%sandbox_id, node_id = %node.node_id, "woke a paused sandbox from its snapshot");
                }
                DataPlaneResume::Woken {
                    node_id: node.node_id,
                    node_address: node.address,
                    execution_id: metadata.execution_id,
                    already_running: joined,
                }
            }
            // Another wake-up or resume is already building this sandbox.
            Err(OrchestratorError::StoreOperationFailed(StoreError::SandboxAlreadyExists {
                ..
            })) => DataPlaneResume::TransitionInProgress {
                holder: "resume".to_string(),
            },
            Err(OrchestratorError::NotAcceptingNewWork) => {
                DataPlaneResume::Exhausted("no node is accepting new sandboxes".to_string())
            }
            Err(err) => {
                warn!(%sandbox_id, error = %err, "waking a sandbox for the data plane failed");
                DataPlaneResume::Failed(err.to_string())
            }
        }
    }

    /// The timeout floor a traffic-triggered wake-up gives the sandbox it starts.
    fn wake_timeout() -> NewTimeout {
        NewTimeout::EnsureMinimum(auto_resume_min_sandbox_timeout())
    }

    /// The node now running a sandbox this process placed or runs, with the
    /// address the placement source has for it.
    async fn node_running(&self, sandbox_id: SandboxId) -> PlacedNode {
        let node_id = match &self.resume_wiring.wake_site {
            WakeSite::Local(here) => here.clone(),
            WakeSite::Remote => self
                .orchestrator()
                .sandbox_holding_node_id(&sandbox_id)
                .await
                .unwrap_or_default(),
        };
        let address = match self.resume_wiring.placement.as_ref() {
            Some(source) if !node_id.is_empty() => {
                source.address_of(&node_id).await.unwrap_or_default()
            }
            _ => String::new(),
        };
        PlacedNode { node_id, address }
    }

    /// Records where a resume landed on the row it came from, so the next
    /// resume prefers the node whose cache is warm.
    pub(in crate::api) async fn note_resume_landing(
        &self,
        record: &SnapshotRecord,
        metadata: &SandboxMetadata,
    ) {
        let landed_on = match &self.resume_wiring.wake_site {
            WakeSite::Local(here) => Some(here.clone()),
            WakeSite::Remote => {
                self.orchestrator()
                    .sandbox_holding_node_id(&metadata.id)
                    .await
            }
        };
        let Some(landed_on) = landed_on else {
            return;
        };
        if record.origin_node_id.as_deref() == Some(landed_on.as_str()) {
            return;
        }
        if let Err(err) = self
            .snapshot_manager
            .set_origin_node_id(&record.id, &landed_on)
            .await
        {
            warn!(
                sandbox_id = %metadata.id,
                snapshot_id = %record.id,
                node_id = %landed_on,
                error = %format_args!("{err:#}"),
                "could not record where the resume landed; the next resume prefers the old node"
            );
        }
    }

    /// Records where a sandbox is running, when this process holds the projection.
    ///
    /// Never fails the caller: the answer still names the node, and the node's
    /// next heartbeat rewrites the record.
    async fn project_running(
        &self,
        sandbox_id: SandboxId,
        node: &PlacedNode,
        execution_id: ExecutionId,
        projection_ttl_secs: u32,
    ) {
        let Some(writer) = self.resume_wiring.projection.as_ref() else {
            return;
        };
        if node.node_id.is_empty() {
            return;
        }
        if let Err(error) = writer
            .record_running(sandbox_id, node, execution_id, projection_ttl_secs)
            .await
        {
            warn!(
                %sandbox_id,
                node_id = %node.node_id,
                %execution_id,
                error = %error,
                "could not write the routing projection for a running sandbox; the next \
                 request pays a resume RPC until the node's heartbeat repairs it"
            );
        }
    }

    async fn locate_for_resume(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<ResumePlacement, PlacementRefusal> {
        match self.resume_wiring.placement.as_ref() {
            Some(source) => source.locate(sandbox_id).await,
            None => Ok(ResumePlacement::NotRunning),
        }
    }

    /// Whether the caller may wake a sandbox: always for a port that is not
    /// envd's, and for envd only with the sandbox's token when it has one.
    fn authorize_envd(&self, request: &DataPlaneResumeRequest, secure: bool) -> bool {
        let control_plane_port = ConfigManager::global_config().tools.control_plane_port;
        // Missing target-port metadata is treated as possibly envd.
        if request
            .target_port
            .is_some_and(|port| port != control_plane_port)
        {
            return true;
        }
        if !secure {
            return true;
        }
        self.orchestrator
            .validate_envd_access_token(request.sandbox_id, &request.envd_access_token)
    }
}

impl From<PlacementRefusal> for DataPlaneResume {
    fn from(refusal: PlacementRefusal) -> Self {
        match refusal {
            PlacementRefusal::Unavailable(reason) => Self::Undecided(reason),
            PlacementRefusal::Failed(reason) => Self::Failed(reason),
        }
    }
}

/// Bounds request-driven wake-up before the caller is told it failed.
fn auto_resume_deadline() -> std::time::Duration {
    #[cfg(test)]
    const DEADLINE: std::time::Duration = std::time::Duration::from_millis(100);
    #[cfg(not(test))]
    const DEADLINE: std::time::Duration = std::time::Duration::from_secs(60);

    DEADLINE
}

/// The floor a woken sandbox's timeout is raised to.
fn auto_resume_min_sandbox_timeout() -> std::time::Duration {
    static AUTO_RESUME_MIN_SANDBOX_TIMEOUT: std::sync::OnceLock<std::time::Duration> =
        std::sync::OnceLock::new();

    *AUTO_RESUME_MIN_SANDBOX_TIMEOUT.get_or_init(|| {
        std::time::Duration::from_secs(
            ConfigManager::global_config()
                .orchestrator
                .auto_resume_min_sandbox_timeout_secs,
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_not_found_lookup_is_not_running_and_the_rest_are_refusals() {
        assert_eq!(
            refusal_from_status(&tonic::Status::not_found("x")),
            Ok(ResumePlacement::NotRunning)
        );
        assert_eq!(
            refusal_from_status(&tonic::Status::unavailable("seeding")),
            Err(PlacementRefusal::Unavailable("seeding".to_string()))
        );
        assert!(matches!(
            refusal_from_status(&tonic::Status::internal("boom")),
            Err(PlacementRefusal::Failed(_))
        ));
    }

    #[test]
    fn only_the_node_half_runs_sandboxes_here() {
        assert!(ResumeWiring::node_local("node-a").runs_sandboxes_here());
        assert!(!ResumeWiring::api_half_for_test().runs_sandboxes_here());
    }
}
