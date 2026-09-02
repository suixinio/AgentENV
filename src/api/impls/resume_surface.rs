//! Data-plane wake-up decisions for paused sandboxes.
//!
//! The API half arbitrates and starts sandboxes; nodes only forward data.
//! A sandbox the cluster already has running is answered as it stands, before
//! any wake-up policy; published snapshots may prefer an origin, while
//! unpublished captures are pinned to their sole machine. Placement therefore
//! consumes the fully wired in-process sandbox lookup rather than
//! reimplementing its policy, and the api half writes the routing projection
//! itself on every wake and on every running answer the projection missed.

use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use tracing::{debug, warn};

use super::paused_recovery::{CrossNodeResume, MissingLocalResume};
use super::{ApiImpl, ResumeArbitration};
use crate::binding_store::lookup::{
    ExecutionAuthority, LookupAnswer, LookupResultLabel, SandboxLocation,
};
use crate::cfg::ConfigManager;
use crate::node_registry::grpc_service::NodeRegistryGrpcService;
use crate::orchestrator::{
    ClaimedExecution, NewTimeout, OrchestratorError, PausedSandboxEntry, SandboxMetadata,
    SandboxState,
};
use crate::types::{ExecutionId, SandboxId};

/// A node the placement source named.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::api) struct PlacedNode {
    pub node_id: String,
    /// Where the gateway should send the request that triggered the wake-up.
    /// May be empty: a scheduler that knows the node's identity but not its
    /// endpoint is answering half a question, and half an answer is not an
    /// address.
    pub address: String,
}

/// Cluster constraint on where a paused sandbox may wake.
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
    /// The only copy of the bytes is on this node. Wake it there or not at all.
    Pinned { node: PlacedNode },
    /// The snapshot is in shared storage, so any node can rebuild it. `node` is
    /// where the cluster would rather it happened.
    Preferred {
        node: PlacedNode,
        origin_node_id: String,
    },
    /// No placement source is configured, so nothing constrains this.
    Unconstrained,
}

/// Stable wire reasons for refusing a pinned wake-up.
///
/// Keep the `Origin` prefix aligned with metric and trailer values.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(clippy::enum_variant_names)]
pub(in crate::api) enum PinRefusalReason {
    /// The node holding the only copy has not been heard from.
    OriginNotReporting,
    /// The node holding the only copy is draining.
    OriginNotAcceptingWork,
    /// This process cannot wake the sandbox on the pinned remote machine.
    OriginNotReachableFromHere,
    /// The placement source returned an unrecognized pin refusal.
    OriginUnclassified,
}

impl PinRefusalReason {
    /// The wire spelling. A closed set: it is a metric label and a trailer.
    pub(in crate::api) fn as_str(self) -> &'static str {
        match self {
            Self::OriginNotReporting => "origin_not_reporting",
            Self::OriginNotAcceptingWork => "origin_not_accepting_work",
            Self::OriginNotReachableFromHere => "origin_not_reachable_from_here",
            Self::OriginUnclassified => "origin_unclassified",
        }
    }
}

/// Why the placement source could not name a node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::api) enum PlacementRefusal {
    /// The sandbox is pinned to a node that cannot serve it.
    Pinned {
        reason: PinRefusalReason,
        origin_node_id: String,
        detail: String,
    },
    /// The placement source has never heard of this sandbox.
    NotFound,
    /// The placement source could not be asked, or had no node to offer.
    /// Retryable, and never an answer about whether the sandbox exists.
    Unavailable(String),
    /// The cluster has no room.
    Exhausted(String),
    /// Anything else the placement source said.
    Failed(String),
}

/// Source of the cluster's wake-up placement decision.
#[async_trait]
pub(in crate::api) trait ResumePlacementSource: Send + Sync {
    async fn locate(&self, sandbox_id: SandboxId) -> Result<ResumePlacement, PlacementRefusal>;
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

/// Whether this process wakes sandboxes on its own machine, and which machine
/// that is.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::api) enum WakeSite {
    /// The orchestration surface behind this `ApiImpl` runs sandboxes on this
    /// machine, named here. A pin naming any other node cannot be honoured.
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
    #[allow(dead_code)]
    pub(in crate::api) fn new(
        placement: Option<Arc<dyn ResumePlacementSource>>,
        wake_site: WakeSite,
    ) -> Self {
        Self {
            placement,
            projection: None,
            wake_site,
        }
    }

    /// Whether this wiring's orchestration surface runs sandboxes in process.
    pub(in crate::api) fn runs_sandboxes_here(&self) -> bool {
        matches!(self.wake_site, WakeSite::Local(_))
    }

    /// The wiring for a process with no cluster to ask: every placement is
    /// unconstrained and the wake happens here.
    pub fn node_local(node_id: impl Into<String>) -> Self {
        Self {
            placement: None,
            projection: None,
            wake_site: WakeSite::Local(node_id.into()),
        }
    }

    /// Test-only API-half wiring without a node-registry placement source.
    #[cfg(any(test, feature = "test-support"))]
    pub fn api_half_for_test() -> Self {
        Self {
            placement: None,
            projection: None,
            wake_site: WakeSite::Remote,
        }
    }

    /// API-half wiring using the fully configured in-process sandbox lookup.
    ///
    /// The remote orchestration surface honors any returned pin, and the same
    /// service writes the projection back.
    pub fn cluster_in_process(local: NodeRegistryGrpcService) -> Self {
        let source = Arc::new(NativePlacementSource { local });
        Self {
            placement: Some(Arc::clone(&source) as Arc<dyn ResumePlacementSource>),
            projection: Some(source),
            // Remote orchestration honors the selected machine.
            wake_site: WakeSite::Remote,
        }
    }
}

/// Placement source over the registry's in-process sandbox lookup, preserving
/// pin/prefer policy.
struct NativePlacementSource {
    local: NodeRegistryGrpcService,
}

#[async_trait]
impl ResumePlacementSource for NativePlacementSource {
    async fn locate(&self, sandbox_id: SandboxId) -> Result<ResumePlacement, PlacementRefusal> {
        match self.local.lookup_sandbox(&sandbox_id.to_string()).await {
            Ok(answer) => {
                let answered_by_binding = answer.label == LookupResultLabel::BoundBinding;
                placement_from_lookup(answer, answered_by_binding)
            }
            Err(status) => Err(refusal_from_status(&status)),
        }
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

/// Converts a sandbox lookup answer into wake-up placement.
///
/// `Bound` under registry authority is a sandbox running right now; any other
/// `Bound` is a preference, not an unpublished-data pin. `answered_by_binding`
/// says the answer came from the routing projection rather than the roster
/// or the paused registry.
fn placement_from_lookup(
    answer: LookupAnswer,
    answered_by_binding: bool,
) -> Result<ResumePlacement, PlacementRefusal> {
    let node = PlacedNode {
        node_id: answer.node.id,
        address: answer.node.endpoint,
    };

    match answer.location {
        SandboxLocation::Pinned => Ok(ResumePlacement::Pinned { node }),
        SandboxLocation::Bound if answer.execution_authority == ExecutionAuthority::Registry => {
            match ExecutionId::parse_str(&answer.execution_id) {
                Ok(execution_id) => Ok(ResumePlacement::Running {
                    node,
                    execution_id,
                    from_projection: answered_by_binding,
                }),
                Err(err) => {
                    // Registry authority never carries an unusable id; treat the
                    // answer as the preference it would have been before the
                    // incarnation was recorded.
                    warn!(
                        execution_id = %answer.execution_id,
                        error = %err,
                        "the placement source vouched for an incarnation it cannot spell; \
                         taking the bound node as a preference instead"
                    );
                    Ok(ResumePlacement::Preferred {
                        node,
                        origin_node_id: answer.origin_node_id,
                    })
                }
            }
        }
        SandboxLocation::Placed | SandboxLocation::Bound => Ok(ResumePlacement::Preferred {
            node,
            origin_node_id: answer.origin_node_id,
        }),
    }
}

/// Message fragments currently used to classify structured pin-refusal reasons.
///
/// Unmatched preconditions remain safe, unclassified pin refusals.
const SCHEDULER_NOT_REPORTING: &str = "is not reporting";
const SCHEDULER_NOT_ACCEPTING_WORK: &str = "is not accepting work";

fn refusal_from_status(status: &tonic::Status) -> PlacementRefusal {
    let message = status.message().to_string();
    match status.code() {
        tonic::Code::NotFound => PlacementRefusal::NotFound,
        tonic::Code::FailedPrecondition => {
            let reason = if message.contains(SCHEDULER_NOT_REPORTING) {
                PinRefusalReason::OriginNotReporting
            } else if message.contains(SCHEDULER_NOT_ACCEPTING_WORK) {
                PinRefusalReason::OriginNotAcceptingWork
            } else {
                warn!(
                    refusal = %message,
                    "the placement source refused a pin in words this build does not \
                     recognise; refusing the wake-up rather than trying another node"
                );
                PinRefusalReason::OriginUnclassified
            };
            PlacementRefusal::Pinned {
                reason,
                origin_node_id: quoted_node_id(&message).unwrap_or_default(),
                detail: message,
            }
        }
        tonic::Code::ResourceExhausted => PlacementRefusal::Exhausted(message),
        tonic::Code::Unavailable | tonic::Code::DeadlineExceeded => {
            PlacementRefusal::Unavailable(message)
        }
        _ => PlacementRefusal::Failed(format!("{}: {message}", status.code())),
    }
}

/// Best-effort node id extraction for operator diagnostics.
fn quoted_node_id(message: &str) -> Option<String> {
    let (_, rest) = message.split_once('"')?;
    let (node, _) = rest.split_once('"')?;
    (!node.is_empty()).then(|| node.to_string())
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
    /// Another resume is in flight. Retryable in a moment.
    TransitionInProgress { holder: String },
    /// The only copy of the bytes is somewhere that cannot serve it.
    PinRefused {
        reason: PinRefusalReason,
        origin_node_id: String,
        detail: String,
    },
    /// The cluster has no room.
    Exhausted(String),
    /// Nobody could be asked; never evidence that the sandbox is absent.
    Undecided(String),
    /// The wake-up was attempted and failed.
    Failed(String),
    /// Wake-up timed out; distinct from failure for metrics, not wire status.
    TimedOut,
}

/// Three-state envd authorization when local metadata may be absent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EnvdAuthorization {
    Authorized,
    Rejected,
    Unknown,
}

/// Three-state auto-resume policy when local metadata may be absent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AutoResume {
    Allowed,
    Refused,
    Unknown,
}

/// Reads auto-resume policy from the supplied record without performing a lookup.
fn wakes_on_traffic(metadata: Option<&SandboxMetadata>) -> AutoResume {
    match metadata {
        None => AutoResume::Unknown,
        Some(metadata) if metadata.auto_resume => AutoResume::Allowed,
        Some(_) => AutoResume::Refused,
    }
}

impl EnvdAuthorization {
    /// Whether authorization still requires the cluster row.
    ///
    /// Only `Unknown` requires it; rechecking an authorized caller against stale
    /// metadata could overturn a valid token.
    fn needs_cluster_record(self) -> bool {
        matches!(self, Self::Unknown)
    }
}

impl ApiImpl {
    /// Wakes a paused sandbox for data-plane traffic.
    ///
    /// A sandbox already running is answered as it stands, before
    /// authorization and the auto-resume gate: the same answer a projection hit
    /// gives, and envd enforces the access token on the node. Placement and pin
    /// checks precede authorization and shared resume arbitration for the rest.
    pub(in crate::api) async fn resume_for_data_plane(
        &self,
        request: DataPlaneResumeRequest,
    ) -> DataPlaneResume {
        let sandbox_id = request.sandbox_id;

        let placement = match self.locate_for_resume(sandbox_id).await {
            Ok(placement) => placement,
            Err(refusal) => return refusal.into(),
        };
        if let ResumePlacement::Running {
            node,
            execution_id,
            from_projection,
        } = &placement
        {
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
                self.project_running(sandbox_id, node, *execution_id, projection_ttl_secs)
                    .await;
            }
            return DataPlaneResume::Woken {
                node_id: node.node_id.clone(),
                node_address: node.address.clone(),
                execution_id: *execution_id,
                already_running: true,
            };
        }
        if let Some(refused) = self.refuse_unhonourable_pin(sandbox_id, &placement) {
            return refused;
        }

        // Check local credentials before taking any claim.
        let local = self
            .orchestrator
            .get_sandbox(&sandbox_id)
            .await
            .ok()
            .flatten();
        let authorized = self.authorize_envd(&request, local.as_ref());
        if authorized == EnvdAuthorization::Rejected {
            return DataPlaneResume::Unauthorized;
        }

        // Pre-claim auto-resume policy applies only to a currently paused local sandbox.
        if let Some(metadata) = local.as_ref() {
            if metadata.state == SandboxState::Paused
                && wakes_on_traffic(Some(metadata)) == AutoResume::Refused
            {
                return DataPlaneResume::AutoResumeDisabled;
            }
        }

        // Share the same arbitration point as user-initiated resume.
        let (entry, claimed, held) = match self.arbitrate_resume(sandbox_id).await {
            ResumeArbitration::Proceed(claimed) => (None, claimed, None),
            ResumeArbitration::Held(entry, claimed) => {
                let generation = entry.generation;
                (Some(entry), claimed, Some(generation))
            }
            ResumeArbitration::Blocked { origin_node_id }
            | ResumeArbitration::NotReady { origin_node_id } => {
                debug!(
                    %sandbox_id,
                    holder = %origin_node_id,
                    "refusing to wake a sandbox the cluster holds elsewhere"
                );
                return DataPlaneResume::TransitionInProgress {
                    holder: origin_node_id,
                };
            }
            ResumeArbitration::Unavailable { reason } => {
                warn!(
                    %sandbox_id,
                    error = %reason,
                    "refusing to wake a sandbox the cluster could not be asked about"
                );
                return DataPlaneResume::Undecided(reason);
            }
        };

        // Complete authorization using metadata carried by the granted claim.
        if authorized.needs_cluster_record() {
            let from_entry = entry.as_ref().and_then(|entry| entry.metadata.as_ref());
            if self.authorize_envd(&request, from_entry) == EnvdAuthorization::Rejected {
                if let Some(generation) = held {
                    self.abandon_claim(sandbox_id, generation).await;
                }
                return DataPlaneResume::Unauthorized;
            }
        }

        // The claimed row supplies authoritative cold-path policy; check it only
        // after authorization and without trusting its historical state field.
        if wakes_on_traffic(entry.as_ref().and_then(|entry| entry.metadata.as_ref()))
            == AutoResume::Refused
        {
            if let Some(generation) = held {
                self.abandon_claim(sandbox_id, generation).await;
            }
            return DataPlaneResume::AutoResumeDisabled;
        }

        self.wake(sandbox_id, entry, claimed, held, &placement)
            .await
    }

    /// Attempts local resume first, then snapshot-backed restoration.
    async fn wake(
        &self,
        sandbox_id: SandboxId,
        entry: Option<Box<PausedSandboxEntry>>,
        claimed: ClaimedExecution,
        held: Option<i64>,
        placement: &ResumePlacement,
    ) -> DataPlaneResume {
        let timeout = NewTimeout::EnsureMinimum(auto_resume_min_sandbox_timeout());
        // Bound wake-up wall time and release any claim on timeout.
        let attempt = tokio::time::timeout(
            crate::api::proxy::auto_resume_deadline(),
            self.orchestrator()
                .resume_sandbox(sandbox_id, timeout, claimed),
        )
        .await;
        let attempt = match attempt {
            Ok(attempt) => attempt,
            Err(_) => {
                warn!(
                    %sandbox_id,
                    timeout_ms = crate::api::proxy::auto_resume_deadline().as_millis(),
                    "waking a sandbox for the data plane timed out"
                );
                if let Some(generation) = held {
                    self.abandon_claim(sandbox_id, generation).await;
                }
                return DataPlaneResume::TimedOut;
            }
        };
        match attempt {
            Ok(metadata) => {
                // Confirm the actual machine for idempotent already-running outcomes too.
                if held.is_some() {
                    let holding_node_id = self
                        .orchestrator()
                        .sandbox_holding_node_id(&sandbox_id)
                        .await;
                    self.paused
                        .mark_sandbox_running(
                            sandbox_id,
                            metadata.execution_id,
                            metadata.expires_at,
                            holding_node_id,
                        )
                        .await;
                }
                self.woken(metadata, placement).await
            }
            Err(OrchestratorError::SandboxNotFound(_)) => {
                // With a claim row, rebuild from its snapshot; otherwise settle absence.
                let Some(entry) = entry else {
                    return self.resolve_missing(sandbox_id, placement).await;
                };
                let rebuilt = self
                    .restore_claimed_sandbox(*entry, Self::wake_timeout())
                    .await;

                self.woken_from_rebuild(rebuilt, placement).await
            }
            Err(err) => {
                // The origin cannot serve this reopen, and the row names a snapshot.
                if let Some(entry) = entry.filter(|_| err.paused_resume_warrants_rebuild()) {
                    let rebuilt = self
                        .rebuild_instead_of_reopening(entry, Self::wake_timeout())
                        .await;

                    return self.woken_from_rebuild(rebuilt, placement).await;
                }

                warn!(%sandbox_id, error = %err, "waking a sandbox for the data plane failed");
                if let Some(generation) = held {
                    self.abandon_claim(sandbox_id, generation).await;
                }
                DataPlaneResume::Failed(err.to_string())
            }
        }
    }

    /// The timeout floor a traffic-triggered wake-up gives the sandbox it starts.
    fn wake_timeout() -> NewTimeout {
        NewTimeout::EnsureMinimum(auto_resume_min_sandbox_timeout())
    }

    async fn woken_from_rebuild(
        &self,
        rebuilt: CrossNodeResume,
        placement: &ResumePlacement,
    ) -> DataPlaneResume {
        match rebuilt {
            CrossNodeResume::Restored(metadata) => self.woken(*metadata, placement).await,
            CrossNodeResume::NotFound => DataPlaneResume::NotFound,
            CrossNodeResume::Failed(reason) => DataPlaneResume::Failed(reason),
        }
    }

    /// Resolves a no-local-copy, no-claim outcome without turning races into 404.
    async fn resolve_missing(
        &self,
        sandbox_id: SandboxId,
        placement: &ResumePlacement,
    ) -> DataPlaneResume {
        match self
            .resolve_missing_local_resume(sandbox_id, Self::wake_timeout())
            .await
        {
            MissingLocalResume::Unknown => DataPlaneResume::NotFound,
            MissingLocalResume::Resumed(metadata) => self.woken(*metadata, placement).await,
            MissingLocalResume::Busy { holder } => DataPlaneResume::TransitionInProgress { holder },
            MissingLocalResume::Undecided(reason) => DataPlaneResume::Undecided(reason),
            MissingLocalResume::Failed(reason) => DataPlaneResume::Failed(reason),
        }
    }

    /// Returns the actual node and incarnation now serving the sandbox, and
    /// writes that answer into the routing projection before it is returned.
    ///
    /// Local wake-ups report this process; remote orchestration reports its placement.
    async fn woken(
        &self,
        metadata: SandboxMetadata,
        placement: &ResumePlacement,
    ) -> DataPlaneResume {
        let (node_id, node_address) = match &self.resume_wiring.wake_site {
            WakeSite::Local(here) => {
                // Reuse a placement address only when it names this machine; otherwise
                // leave it empty for the gateway to resolve.
                let address = match placement {
                    ResumePlacement::Running { node, .. }
                    | ResumePlacement::Pinned { node }
                    | ResumePlacement::Preferred { node, .. }
                        if node.node_id == *here =>
                    {
                        node.address.clone()
                    }
                    _ => String::new(),
                };
                (here.clone(), address)
            }
            WakeSite::Remote => match placement {
                ResumePlacement::Running { node, .. }
                | ResumePlacement::Pinned { node }
                | ResumePlacement::Preferred { node, .. } => {
                    (node.node_id.clone(), node.address.clone())
                }
                ResumePlacement::Unconstrained => {
                    (self.paused.node_id().to_string(), String::new())
                }
            },
        };

        let node = PlacedNode {
            node_id,
            address: node_address,
        };
        self.project_running(
            metadata.id,
            &node,
            metadata.execution_id,
            metadata.projection_ttl_secs(SystemTime::now()),
        )
        .await;

        DataPlaneResume::Woken {
            node_id: node.node_id,
            node_address: node.address,
            execution_id: metadata.execution_id,
            already_running: false,
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
            None => Ok(ResumePlacement::Unconstrained),
        }
    }

    /// Refuses a pinned wake-up this process cannot honor, preventing snapshot rewind.
    fn refuse_unhonourable_pin(
        &self,
        sandbox_id: SandboxId,
        placement: &ResumePlacement,
    ) -> Option<DataPlaneResume> {
        let ResumePlacement::Pinned { node } = placement else {
            return None;
        };
        let WakeSite::Local(here) = &self.resume_wiring.wake_site else {
            // Remote orchestration carries and honors the pin itself.
            return None;
        };
        if node.node_id == *here {
            return None;
        }

        warn!(
            %sandbox_id,
            origin_node_id = %node.node_id,
            this_node = %here,
            "refusing to wake a sandbox whose only copy is on another machine; waking it \
             here would rebuild it from an older snapshot and silently lose the last pause"
        );
        Some(DataPlaneResume::PinRefused {
            reason: PinRefusalReason::OriginNotReachableFromHere,
            origin_node_id: node.node_id.clone(),
            detail: format!(
                "the only copy of this sandbox is on node '{}', which this process cannot \
                 wake sandboxes on",
                node.node_id
            ),
        })
    }

    /// Checks envd credentials using the metadata currently available.
    fn authorize_envd(
        &self,
        request: &DataPlaneResumeRequest,
        metadata: Option<&SandboxMetadata>,
    ) -> EnvdAuthorization {
        let control_plane_port = ConfigManager::global_config().tools.control_plane_port;
        // Missing target-port metadata is treated as possibly envd.
        if request
            .target_port
            .is_some_and(|port| port != control_plane_port)
        {
            return EnvdAuthorization::Authorized;
        }
        let Some(metadata) = metadata else {
            return EnvdAuthorization::Unknown;
        };
        if !metadata.secure {
            return EnvdAuthorization::Authorized;
        }
        if self
            .orchestrator
            .validate_envd_access_token(request.sandbox_id, &request.envd_access_token)
        {
            EnvdAuthorization::Authorized
        } else {
            EnvdAuthorization::Rejected
        }
    }
}

impl From<PlacementRefusal> for DataPlaneResume {
    fn from(refusal: PlacementRefusal) -> Self {
        match refusal {
            PlacementRefusal::Pinned {
                reason,
                origin_node_id,
                detail,
            } => Self::PinRefused {
                reason,
                origin_node_id,
                detail,
            },
            PlacementRefusal::NotFound => Self::NotFound,
            PlacementRefusal::Unavailable(reason) => Self::Undecided(reason),
            PlacementRefusal::Exhausted(reason) => Self::Exhausted(reason),
            PlacementRefusal::Failed(reason) => Self::Failed(reason),
        }
    }
}

/// The floor a woken sandbox's timeout is raised to.
///
/// Declared in `crate::api::proxy` beside the other `auto_resume_*` settings
/// and read here rather than restated: a traffic-triggered wake-up hands out
/// this one lifetime whether it reopens a capture or rebuilds from a snapshot,
/// and the projection this surface writes afterwards is budgeted from the
/// record that lifetime lands in.
fn auto_resume_min_sandbox_timeout() -> std::time::Duration {
    crate::api::proxy::auto_resume_min_sandbox_timeout()
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::proto::scheduler;

    use crate::binding_store::BindingStore as _;
    use crate::orchestrator::PausedRegistryState;

    fn scheduler_not_reporting(state: &str, node: &str) -> String {
        format!("sandbox is {state} on node {node:?}, which is not reporting")
    }

    fn scheduler_not_accepting_work(state: &str, node: &str) -> String {
        format!("sandbox is {state} on node {node:?}, which is not accepting work")
    }

    fn answer(
        (node_id, endpoint): (&str, &str),
        location: SandboxLocation,
        origin_node_id: &str,
    ) -> LookupAnswer {
        LookupAnswer {
            node: crate::node_registry::types::Node {
                id: node_id.to_string(),
                endpoint: endpoint.to_string(),
                pod_name: String::new(),
            },
            location,
            origin_node_id: origin_node_id.to_string(),
            execution_id: String::new(),
            execution_authority: ExecutionAuthority::Unknown,
            label: LookupResultLabel::BoundRegistry,
        }
    }

    #[test]
    fn only_a_pinned_location_pins_and_the_other_two_are_preferences() {
        let pinned = placement_from_lookup(
            answer(
                ("origin", "http://origin:8000"),
                SandboxLocation::Pinned,
                "origin",
            ),
            false,
        )
        .expect("a pinned answer names a node");
        assert_eq!(
            pinned,
            ResumePlacement::Pinned {
                node: PlacedNode {
                    node_id: "origin".to_string(),
                    address: "http://origin:8000".to_string(),
                },
            },
            "publishing/local_only rows have exactly one copy of the bytes"
        );

        for location in [SandboxLocation::Placed, SandboxLocation::Bound] {
            let placement = placement_from_lookup(
                answer(("chosen", "http://chosen:8000"), location, "origin"),
                false,
            )
            .expect("a non-pinned answer still names a node");
            assert_eq!(
                placement,
                ResumePlacement::Preferred {
                    node: PlacedNode {
                        node_id: "chosen".to_string(),
                        address: "http://chosen:8000".to_string(),
                    },
                    origin_node_id: "origin".to_string(),
                },
                "{location:?} is a preference, and origin travels with it as a hint"
            );
        }
    }

    #[test]
    fn an_answer_with_an_empty_address_is_still_a_placement() {
        assert!(
            placement_from_lookup(
                answer(("chosen", ""), SandboxLocation::Placed, "origin"),
                false,
            )
            .is_ok(),
            "an empty address is fine: the gateway resolves the node itself"
        );
    }

    #[test]
    fn every_failed_precondition_stays_a_pin_refusal_and_the_two_known_ones_are_named() {
        for (message, expected) in [
            (
                scheduler_not_reporting("local_only", "node-a"),
                PinRefusalReason::OriginNotReporting,
            ),
            (
                scheduler_not_accepting_work("publishing", "node-a"),
                PinRefusalReason::OriginNotAcceptingWork,
            ),
            (
                // A wording this build has never seen.
                "sandbox is local_only on node \"node-a\", which has been eaten by a grue"
                    .to_string(),
                PinRefusalReason::OriginUnclassified,
            ),
        ] {
            let refusal = refusal_from_status(&tonic::Status::failed_precondition(message.clone()));
            let PlacementRefusal::Pinned {
                reason,
                origin_node_id,
                detail,
            } = refusal
            else {
                panic!(
                    "🔴 every FailedPrecondition must stay a pin refusal, or an \
                     unpublished sandbox gets woken on a machine without its bytes: \
                     {message} became {refusal:?}"
                );
            };
            assert_eq!(reason, expected, "classifying {message}");
            assert_eq!(
                origin_node_id, "node-a",
                "the node name is lifted out of the %q for the operator's log"
            );
            assert_eq!(
                detail, message,
                "the scheduler's own words reach the caller"
            );
        }
    }

    #[test]
    fn statuses_that_are_not_pin_refusals_keep_their_own_meanings() {
        assert!(matches!(
            refusal_from_status(&tonic::Status::not_found("no such sandbox")),
            PlacementRefusal::NotFound
        ));
        assert!(matches!(
            refusal_from_status(&tonic::Status::unavailable("scheduler is seeding")),
            PlacementRefusal::Unavailable(_)
        ));
        assert!(matches!(
            refusal_from_status(&tonic::Status::deadline_exceeded("too slow")),
            PlacementRefusal::Unavailable(_)
        ));
        assert!(matches!(
            refusal_from_status(&tonic::Status::resource_exhausted("no nodes available")),
            PlacementRefusal::Exhausted(_)
        ));
        assert!(matches!(
            refusal_from_status(&tonic::Status::internal("boom")),
            PlacementRefusal::Failed(_)
        ));
    }

    /// The wire spellings, which are simultaneously a metric label and a
    /// trailer value. A rename on either side silently unjoins the gateway's
    /// log, this half's log, and the scrape.
    #[test]
    fn pin_refusal_reasons_have_stable_distinct_wire_spellings() {
        let spellings = [
            PinRefusalReason::OriginNotReporting.as_str(),
            PinRefusalReason::OriginNotAcceptingWork.as_str(),
            PinRefusalReason::OriginNotReachableFromHere.as_str(),
            PinRefusalReason::OriginUnclassified.as_str(),
        ];
        assert_eq!(
            spellings,
            [
                "origin_not_reporting",
                "origin_not_accepting_work",
                "origin_not_reachable_from_here",
                "origin_unclassified",
            ]
        );

        let mut unique = spellings.to_vec();
        unique.sort_unstable();
        unique.dedup();
        assert_eq!(
            unique.len(),
            spellings.len(),
            "two reasons sharing a spelling would make a caller back off wrongly \
             in one direction or the other"
        );
    }

    /// Best effort by construction, so the cases that cannot yield a name must
    /// yield `None` rather than a wrong one — it is only ever used to make a
    /// log line nameable.
    #[test]
    fn the_node_name_is_lifted_out_of_a_quoted_message_or_not_at_all() {
        assert_eq!(
            quoted_node_id(&scheduler_not_reporting("local_only", "node-7")),
            Some("node-7".to_string())
        );
        assert_eq!(quoted_node_id("no quotes here"), None);
        assert_eq!(quoted_node_id("one \"unterminated"), None);
        assert_eq!(quoted_node_id("empty \"\" name"), None);
    }

    /// A `PausedSandboxRegistry` test double for stage 3 of `lookup_node`:
    /// `get`/`is_cluster_backed` answer from one fixed row, everything else
    /// panics. `lookup_node` never calls the other twelve methods — a test
    /// that (incorrectly) drove this into a write path fails loudly instead of
    /// silently getting a made-up default. Same shape and same reasoning as
    /// `src/node_registry/grpc_service.rs`'s own `FakePausedRegistry`.
    struct OneRowRegistry {
        entry: PausedSandboxEntry,
    }

    #[async_trait]
    impl crate::orchestrator::PausedSandboxRegistry for OneRowRegistry {
        async fn get(
            &self,
            sandbox_id: &SandboxId,
        ) -> crate::orchestrator::RegistryResult<Option<PausedSandboxEntry>> {
            Ok((self.entry.sandbox_id == *sandbox_id).then(|| self.entry.clone()))
        }

        fn is_cluster_backed(&self) -> bool {
            true
        }

        async fn begin_pause(
            &self,
            _entry: &PausedSandboxEntry,
        ) -> crate::orchestrator::RegistryResult<crate::orchestrator::BeganPause> {
            unimplemented!("lookup_node never calls this")
        }
        async fn complete_pause(
            &self,
            _sandbox_id: &SandboxId,
            _generation: i64,
            _snapshot_id: &crate::snapshot::SnapshotId,
        ) -> crate::orchestrator::RegistryResult<()> {
            unimplemented!("lookup_node never calls this")
        }
        async fn mark_local_only(
            &self,
            _sandbox_id: &SandboxId,
            _generation: i64,
        ) -> crate::orchestrator::RegistryResult<()> {
            unimplemented!("lookup_node never calls this")
        }
        async fn get_many(
            &self,
            _sandbox_ids: &[SandboxId],
        ) -> crate::orchestrator::RegistryResult<crate::orchestrator::PausedRegistryRows> {
            unimplemented!("lookup_node never calls this")
        }
        async fn claim_for_resume(
            &self,
            _sandbox_id: &SandboxId,
            _node_id: &str,
            _execution_id: ExecutionId,
        ) -> crate::orchestrator::RegistryResult<crate::orchestrator::ResumeClaim> {
            unimplemented!("lookup_node never calls this")
        }
        async fn release_claim(
            &self,
            _sandbox_id: &SandboxId,
            _generation: i64,
        ) -> crate::orchestrator::RegistryResult<bool> {
            unimplemented!("lookup_node never calls this")
        }
        async fn renew_lease(
            &self,
            _node_id: &str,
            _held: &[crate::orchestrator::HeldSandbox],
        ) -> crate::orchestrator::RegistryResult<u64> {
            unimplemented!("lookup_node never calls this")
        }
        async fn reclaim_expired_holdings(
            &self,
        ) -> crate::orchestrator::RegistryResult<crate::orchestrator::ReclaimedHoldings> {
            unimplemented!("lookup_node never calls this")
        }
        async fn mark_running(
            &self,
            _sandbox_id: &SandboxId,
            _node_id: &str,
            _holder_node_id: &str,
            _execution_id: ExecutionId,
            _expires_at: Option<std::time::SystemTime>,
        ) -> crate::orchestrator::RegistryResult<crate::orchestrator::MarkRunningOutcome> {
            unimplemented!("lookup_node never calls this")
        }
        async fn renew_sandbox_deadline(
            &self,
            _sandbox_id: &SandboxId,
            _execution_id: ExecutionId,
            _expires_at: Option<std::time::SystemTime>,
        ) -> crate::orchestrator::RegistryResult<crate::orchestrator::DeadlineRenewalOutcome>
        {
            unimplemented!("lookup_node never calls this")
        }
        async fn release_node_holdings(
            &self,
            _node_id: &str,
        ) -> crate::orchestrator::RegistryResult<crate::orchestrator::ReleasedHoldings> {
            unimplemented!("lookup_node never calls this")
        }
        async fn remove(
            &self,
            _sandbox_id: &SandboxId,
            _generation: i64,
        ) -> crate::orchestrator::RegistryResult<bool> {
            unimplemented!("lookup_node never calls this")
        }
        async fn list_all(
            &self,
        ) -> crate::orchestrator::RegistryResult<crate::orchestrator::PausedRegistryListing>
        {
            unimplemented!("lookup_node never calls this")
        }
    }

    fn discovered(id: &str, endpoint: &str) -> crate::node_registry::types::Node {
        crate::node_registry::types::Node {
            id: id.to_string(),
            endpoint: endpoint.to_string(),
            pod_name: String::new(),
        }
    }

    /// A gate that is already warm: its deadline sits at the Unix epoch and it
    /// has been told a node reported, which is what `warmed_up` requires
    /// besides the clock. Mirrors `grpc_service.rs`'s own `warm_gate`.
    fn warm_gate(
        registry: &Arc<crate::node_registry::registry::AtomicNodeRegistry>,
    ) -> Arc<crate::node_registry::warmup::WarmupGate> {
        let gate = Arc::new(crate::node_registry::warmup::WarmupGate::new(
            Arc::clone(registry) as Arc<dyn crate::node_registry::registry::NodeRegistry>,
            std::time::Duration::from_secs(1),
            std::time::SystemTime::UNIX_EPOCH,
        ));
        gate.reported_in(std::time::SystemTime::now());
        gate
    }

    fn a_heartbeat(node_id: &str, status: scheduler::NodeStatus) -> scheduler::HeartbeatRequest {
        scheduler::HeartbeatRequest {
            node_id: node_id.to_string(),
            cluster_id: "cluster-a".to_string(),
            service_instance_id: format!("{node_id}-instance"),
            snapshot: Some(scheduler::NodeSnapshot {
                status: status as i32,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn registry_row(
        sandbox_id: SandboxId,
        state: PausedRegistryState,
        origin_node_id: &str,
        execution_id: Option<ExecutionId>,
    ) -> PausedSandboxEntry {
        let now = chrono::Utc::now();
        PausedSandboxEntry {
            sandbox_id,
            cluster_id: uuid::Uuid::nil(),
            state,
            generation: 1,
            origin_node_id: origin_node_id.to_string(),
            claimed_by_node_id: None,
            snapshot_id: None,
            metadata: None,
            execution_id,
            paused_at: now,
            updated_at: now,
        }
    }

    fn empty_binding_store() -> Arc<dyn crate::binding_store::BindingStore> {
        Arc::new(crate::binding_store::InMemoryBindingStore::new(
            crate::binding_store::BindingStoreSettings::default(),
        ))
    }

    /// The service exactly as `assemble_api` builds it: every builder applied,
    /// in the order that function applies them.
    fn wired_service(
        registry: &Arc<crate::node_registry::registry::AtomicNodeRegistry>,
        binding_store: Arc<dyn crate::binding_store::BindingStore>,
        paused: Arc<dyn crate::orchestrator::PausedSandboxRegistry>,
    ) -> NodeRegistryGrpcService {
        NodeRegistryGrpcService::new(Arc::clone(registry), warm_gate(registry))
            .with_binding_store(binding_store, false, std::time::Duration::ZERO)
            .with_artifact_store(Arc::new(
                crate::binding_store::artifact_index::InMemoryArtifactStore::new(8),
            ))
            .with_paused_registry(paused)
    }

    fn source_over(service: NodeRegistryGrpcService) -> NativePlacementSource {
        NativePlacementSource { local: service }
    }

    #[tokio::test]
    async fn the_in_process_lookup_pins_an_unpublished_row_and_prefers_a_binding() {
        let sandbox_id = SandboxId::new();
        let registry = Arc::new(crate::node_registry::registry::AtomicNodeRegistry::new(
            vec![discovered("node-a", "http://10.0.0.1:8000")],
            std::time::Duration::from_secs(30),
        ));
        crate::node_registry::registry::NodeRegistry::heartbeat(
            registry.as_ref(),
            &a_heartbeat("node-a", scheduler::NodeStatus::Ready),
            std::time::SystemTime::now(),
        )
        .expect("node-a is in discovery");

        let pinned = source_over(wired_service(
            &registry,
            empty_binding_store(),
            Arc::new(OneRowRegistry {
                entry: registry_row(sandbox_id, PausedRegistryState::LocalOnly, "node-a", None),
            }),
        ))
        .locate(sandbox_id)
        .await
        .expect("origin is live and schedulable, so the pin can be honoured");
        assert_eq!(
            pinned,
            ResumePlacement::Pinned {
                node: PlacedNode {
                    node_id: "node-a".to_string(),
                    address: "http://10.0.0.1:8000".to_string(),
                },
            },
            "🔴 a local_only row has exactly one copy of the bytes; reading it as \
             a preference wakes the sandbox somewhere else and silently rewinds it"
        );

        // The same sandbox id, now with a live binding — stage 1 of the same
        // call, which answers BOUND.
        let binding_store = empty_binding_store();
        let execution_id = ExecutionId::new();
        binding_store
            .record(
                &sandbox_id.to_string(),
                crate::binding_store::Binding {
                    node: discovered("node-a", "http://10.0.0.1:8000"),
                    execution_id: execution_id.to_string(),
                    projection_ttl: std::time::Duration::ZERO,
                    state: crate::binding_store::BindingState::Confirmed,
                },
                std::time::SystemTime::now(),
            )
            .await
            .expect("install the binding");
        let bound = source_over(wired_service(
            &registry,
            binding_store,
            Arc::new(OneRowRegistry {
                entry: registry_row(sandbox_id, PausedRegistryState::LocalOnly, "node-a", None),
            }),
        ))
        .locate(sandbox_id)
        .await
        .expect("a binding names a node");
        assert_eq!(
            bound,
            ResumePlacement::Running {
                node: PlacedNode {
                    node_id: "node-a".to_string(),
                    address: "http://10.0.0.1:8000".to_string(),
                },
                execution_id,
                from_projection: true,
            },
            "🔴 BOUND is a hint about where the layers are, not a claim that the \
             bytes exist nowhere else; reading it as a pin refuses ordinary \
             resumes off a node that has since drained"
        );
    }

    #[tokio::test]
    async fn the_in_process_lookup_classifies_every_pin_refusal_it_can_emit() {
        for (status, expected, note) in [
            (
                scheduler::NodeStatus::Unspecified,
                PinRefusalReason::OriginNotReporting,
                "no heartbeat at all",
            ),
            (
                scheduler::NodeStatus::Draining,
                PinRefusalReason::OriginNotAcceptingWork,
                "heartbeating, but refusing new work",
            ),
        ] {
            let sandbox_id = SandboxId::new();
            let registry = Arc::new(crate::node_registry::registry::AtomicNodeRegistry::new(
                vec![discovered("node-a", "http://10.0.0.1:8000")],
                std::time::Duration::from_secs(30),
            ));
            // `Unspecified` stands for "never reported": the node is in
            // discovery but has no roster, which is what `live_node` misses on.
            if status != scheduler::NodeStatus::Unspecified {
                crate::node_registry::registry::NodeRegistry::heartbeat(
                    registry.as_ref(),
                    &a_heartbeat("node-a", status),
                    std::time::SystemTime::now(),
                )
                .expect("node-a is in discovery");
            }

            let refusal = source_over(wired_service(
                &registry,
                empty_binding_store(),
                Arc::new(OneRowRegistry {
                    entry: registry_row(sandbox_id, PausedRegistryState::LocalOnly, "node-a", None),
                }),
            ))
            .locate(sandbox_id)
            .await
            .expect_err("the only copy is on a node that cannot serve it");

            let PlacementRefusal::Pinned { reason, detail, .. } = refusal else {
                panic!(
                    "🔴 every FailedPrecondition must stay a pin refusal, or an \
                     unpublished sandbox gets woken on a machine without its \
                     bytes: {note} became {refusal:?}"
                );
            };
            assert_eq!(reason, expected, "classifying the refusal for: {note}");
            assert!(
                detail.contains("node-a"),
                "the producer's own words reach the caller: {detail}"
            );
        }

        let reworded = refusal_from_status(&tonic::Status::failed_precondition(
            "sandbox is local_only on node node-a, which has been eaten by a grue",
        ));
        assert!(
            matches!(
                reworded,
                PlacementRefusal::Pinned {
                    reason: PinRefusalReason::OriginUnclassified,
                    ..
                }
            ),
            "a reworded refusal must degrade to \"do not try anywhere else\", \
             not to \"this was not a pin refusal\": {reworded:?}"
        );
    }

    #[tokio::test]
    async fn an_empty_sandbox_id_is_still_invalid_argument_in_process() {
        let registry = Arc::new(crate::node_registry::registry::AtomicNodeRegistry::new(
            Vec::new(),
            std::time::Duration::from_secs(30),
        ));
        let service = wired_service(
            &registry,
            empty_binding_store(),
            Arc::new(OneRowRegistry {
                entry: registry_row(
                    SandboxId::new(),
                    PausedRegistryState::Paused,
                    "node-a",
                    None,
                ),
            }),
        );

        let status = service
            .lookup_sandbox("   ")
            .await
            .expect_err("a blank sandbox id is not a lookup");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(matches!(
            refusal_from_status(&status),
            PlacementRefusal::Failed(_)
        ));
    }

    #[tokio::test]
    async fn a_partly_wired_service_cannot_answer_a_pin_and_says_so_differently() {
        let sandbox_id = SandboxId::new();
        let registry = Arc::new(crate::node_registry::registry::AtomicNodeRegistry::new(
            vec![discovered("node-a", "http://10.0.0.1:8000")],
            std::time::Duration::from_secs(30),
        ));
        crate::node_registry::registry::NodeRegistry::heartbeat(
            registry.as_ref(),
            &a_heartbeat("node-a", scheduler::NodeStatus::Ready),
            std::time::SystemTime::now(),
        )
        .expect("node-a is in discovery");
        let row = || {
            Arc::new(OneRowRegistry {
                entry: registry_row(sandbox_id, PausedRegistryState::LocalOnly, "node-a", None),
            }) as Arc<dyn crate::orchestrator::PausedSandboxRegistry>
        };

        // No binding store: the lookup is `Unimplemented` before it looks at
        // anything.
        let bare = source_over(NodeRegistryGrpcService::new(
            Arc::clone(&registry),
            warm_gate(&registry),
        ))
        .locate(sandbox_id)
        .await
        .expect_err("nothing is wired, so nothing can be answered");
        assert!(
            matches!(
                bare,
                PlacementRefusal::Failed(ref reason) if reason.contains("needs a binding store")
            ),
            "an unwired service must not look like an answer: {bare:?}"
        );

        // A binding store but no paused registry: stage 3 never runs, so the
        // pinned row is invisible and the answer is `NotFound`.
        let no_stage_three = source_over(
            NodeRegistryGrpcService::new(Arc::clone(&registry), warm_gate(&registry))
                .with_binding_store(empty_binding_store(), false, std::time::Duration::ZERO),
        )
        .locate(sandbox_id)
        .await
        .expect_err("no registry leg, so the row cannot be seen");
        assert_eq!(
            no_stage_three,
            PlacementRefusal::NotFound,
            "🔴 without with_paused_registry the pin is invisible, and NotFound \
             tells the platform to rebuild the sandbox from its template"
        );

        // Fully wired, same inputs: the pin appears.
        let wired = source_over(wired_service(&registry, empty_binding_store(), row()))
            .locate(sandbox_id)
            .await
            .expect("the fully wired service can see the row");
        assert!(
            matches!(wired, ResumePlacement::Pinned { .. }),
            "the three assertions above are only about wiring if this one holds: {wired:?}"
        );
    }

    #[test]
    fn only_an_unknown_authorization_still_needs_the_cluster_record() {
        assert!(
            EnvdAuthorization::Unknown.needs_cluster_record(),
            "🔴 the cold path is asked about sandboxes this process has never \
             run; skipping the second pass there is an authorization bypass"
        );
        assert!(
            !EnvdAuthorization::Authorized.needs_cluster_record(),
            "already decided against a real record; a second look could only \
             overturn it with a worse one"
        );
        assert!(
            !EnvdAuthorization::Rejected.needs_cluster_record(),
            "a rejection has already returned by this point"
        );
    }

    /// The woken sandbox's timeout floor is the one `crate::api::proxy`
    /// declares, not zero.
    ///
    /// Read rather than restated, so this wake-up cannot drift away from the
    /// lifetime the data plane's own wake-up used to hand out. A zero floor
    /// would hand every woken sandbox whatever it had left, which for a sandbox
    /// that was paused past its deadline is nothing.
    #[test]
    fn the_wake_up_timeout_floor_is_the_one_the_proxy_module_declares() {
        let floor = auto_resume_min_sandbox_timeout();
        assert_eq!(floor, crate::api::proxy::auto_resume_min_sandbox_timeout());
        assert!(
            !floor.is_zero(),
            "a zero floor raises nothing, so a sandbox woken at the end of its \
             life would be evicted again immediately"
        );
    }

    /// A binding store that counts writes and can be told to refuse them, so a
    /// test can tell "answered without writing" from "wrote the same thing".
    struct CountingBindingStore {
        inner: Arc<dyn crate::binding_store::BindingStore>,
        writes: std::sync::atomic::AtomicUsize,
        refuse_writes: bool,
    }

    impl CountingBindingStore {
        fn over(
            inner: Arc<dyn crate::binding_store::BindingStore>,
            refuse_writes: bool,
        ) -> Arc<Self> {
            Arc::new(Self {
                inner,
                writes: std::sync::atomic::AtomicUsize::new(0),
                refuse_writes,
            })
        }

        fn writes(&self) -> usize {
            self.writes.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl crate::binding_store::BindingStore for CountingBindingStore {
        async fn get(
            &self,
            sandbox_id: &str,
            now: std::time::SystemTime,
        ) -> Result<Option<crate::binding_store::Binding>, crate::binding_store::BindingStoreError>
        {
            self.inner.get(sandbox_id, now).await
        }

        async fn record(
            &self,
            sandbox_id: &str,
            binding: crate::binding_store::Binding,
            now: std::time::SystemTime,
        ) -> Result<crate::binding_store::BindingDecision, crate::binding_store::BindingStoreError>
        {
            self.writes
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if self.refuse_writes {
                return Err(crate::binding_store::BindingStoreError::new(
                    "redis is away",
                ));
            }
            self.inner.record(sandbox_id, binding, now).await
        }

        async fn reconcile_node(
            &self,
            node: crate::node_registry::types::Node,
            roster: Vec<crate::node_registry::types::RosterEntry>,
            now: std::time::SystemTime,
        ) -> Result<
            Vec<(String, crate::binding_store::BindingDecision)>,
            crate::binding_store::BindingStoreError,
        > {
            self.inner.reconcile_node(node, roster, now).await
        }

        async fn delete(
            &self,
            sandbox_id: &str,
            execution_id: &str,
            now: std::time::SystemTime,
        ) -> Result<
            crate::binding_store::BindingDeleteOutcome,
            crate::binding_store::BindingStoreError,
        > {
            self.inner.delete(sandbox_id, execution_id, now).await
        }

        async fn release_reservation(
            &self,
            sandbox_id: &str,
            execution_id: &str,
            now: std::time::SystemTime,
        ) -> Result<
            crate::binding_store::BindingDeleteOutcome,
            crate::binding_store::BindingStoreError,
        > {
            self.inner
                .release_reservation(sandbox_id, execution_id, now)
                .await
        }
    }

    const NODE_A: &str = "node-a";
    const NODE_A_ADDRESS: &str = "http://10.0.0.1:8000";

    /// A registry with node-a discovered and reporting, the way every cluster
    /// test in this module starts.
    fn registry_with_node_a() -> Arc<crate::node_registry::registry::AtomicNodeRegistry> {
        let registry = Arc::new(crate::node_registry::registry::AtomicNodeRegistry::new(
            vec![discovered(NODE_A, NODE_A_ADDRESS)],
            std::time::Duration::from_secs(30),
        ));
        crate::node_registry::registry::NodeRegistry::heartbeat(
            registry.as_ref(),
            &a_heartbeat(NODE_A, scheduler::NodeStatus::Ready),
            std::time::SystemTime::now(),
        )
        .expect("node-a is in discovery");
        registry
    }

    /// `wired_service` with the projection made authoritative, so the TTL a
    /// write carries is the one the store keeps.
    fn wired_authoritative_service(
        registry: &Arc<crate::node_registry::registry::AtomicNodeRegistry>,
        binding_store: Arc<dyn crate::binding_store::BindingStore>,
        paused: Arc<dyn crate::orchestrator::PausedSandboxRegistry>,
    ) -> NodeRegistryGrpcService {
        NodeRegistryGrpcService::new(Arc::clone(registry), warm_gate(registry))
            .with_binding_store(binding_store, true, std::time::Duration::ZERO)
            .with_artifact_store(Arc::new(
                crate::binding_store::artifact_index::InMemoryArtifactStore::new(8),
            ))
            .with_paused_registry(paused)
    }

    /// The api half over an in-memory orchestrator, wired the way `aenv-api`
    /// wires it: the placement source and the projection writer are the one
    /// in-process registry service.
    async fn api_half_over(service: NodeRegistryGrpcService) -> Arc<ApiImpl> {
        let orchestrator = crate::orchestrator::Orchestrator::new(
            crate::sandbox::AccessTokenSeedPolicy::MayGenerate,
            crate::orchestrator::InMemoryMetadataStore::new(),
            crate::sandbox::mock::MockBackendFactory::new(),
            crate::orchestrator::DisabledSandboxPersister,
            crate::image::DisabledRuntimeImageRefs::shared(),
        )
        .await
        .expect("an in-memory orchestrator");
        let snapshot_manager = Arc::new(crate::snapshot::mock::mock_snapshot_manager());
        Arc::new(ApiImpl::new(
            orchestrator,
            Arc::clone(&snapshot_manager),
            None,
            crate::api::PausedSandboxWiring::new(
                Arc::new(crate::orchestrator::DisabledPausedSandboxRegistry),
                snapshot_manager,
                &crate::identity::NodeIdentity::from_config(&Default::default()),
            ),
            Vec::new(),
            ResumeWiring::cluster_in_process(service),
        ))
    }

    fn data_plane_request(sandbox_id: SandboxId) -> DataPlaneResumeRequest {
        DataPlaneResumeRequest {
            sandbox_id,
            target_port: None,
            envd_access_token: String::new(),
        }
    }

    async fn seed_paused(api: &ApiImpl, sandbox_id: SandboxId, auto_resume: bool) {
        api.orchestrator()
            .set_metadata_state_for_test(sandbox_id, SandboxState::Paused)
            .await
            .expect("seed a paused sandbox");
        api.orchestrator()
            .set_auto_resume_for_test(&sandbox_id, auto_resume)
            .await
            .expect("set the flag");
    }

    async fn bind_running(
        store: &dyn crate::binding_store::BindingStore,
        sandbox_id: SandboxId,
        execution_id: ExecutionId,
    ) {
        store
            .record(
                &sandbox_id.to_string(),
                crate::binding_store::Binding {
                    node: discovered(NODE_A, NODE_A_ADDRESS),
                    execution_id: execution_id.to_string(),
                    projection_ttl: std::time::Duration::ZERO,
                    state: crate::binding_store::BindingState::Confirmed,
                },
                std::time::SystemTime::now(),
            )
            .await
            .expect("install the binding");
    }

    fn paused_row_registry(
        sandbox_id: SandboxId,
    ) -> Arc<dyn crate::orchestrator::PausedSandboxRegistry> {
        Arc::new(OneRowRegistry {
            entry: registry_row(sandbox_id, PausedRegistryState::Paused, NODE_A, None),
        })
    }

    #[test]
    fn a_bound_answer_under_registry_authority_is_running_and_says_where_it_came_from() {
        let execution_id = ExecutionId::new();
        let running = |answered_by_binding: bool| {
            placement_from_lookup(
                LookupAnswer {
                    execution_id: execution_id.to_string(),
                    execution_authority: ExecutionAuthority::Registry,
                    ..answer((NODE_A, NODE_A_ADDRESS), SandboxLocation::Bound, "")
                },
                answered_by_binding,
            )
            .expect("a bound answer names a node")
        };
        for from_projection in [true, false] {
            assert_eq!(
                running(from_projection),
                ResumePlacement::Running {
                    node: PlacedNode {
                        node_id: NODE_A.to_string(),
                        address: NODE_A_ADDRESS.to_string(),
                    },
                    execution_id,
                    from_projection,
                }
            );
        }

        // Registry authority with an id that is not an incarnation is the
        // preference it would have been before incarnations were recorded.
        let unspellable = placement_from_lookup(
            LookupAnswer {
                execution_id: "not-a-uuid".to_string(),
                execution_authority: ExecutionAuthority::Registry,
                ..answer((NODE_A, NODE_A_ADDRESS), SandboxLocation::Bound, "")
            },
            true,
        )
        .expect("still names a node");
        assert!(
            matches!(unspellable, ResumePlacement::Preferred { .. }),
            "{unspellable:?}"
        );
    }

    #[tokio::test]
    async fn a_sandbox_the_projection_says_is_running_is_answered_before_the_auto_resume_gate() {
        let sandbox_id = SandboxId::new();
        let execution_id = ExecutionId::new();
        let registry = registry_with_node_a();
        let store = CountingBindingStore::over(empty_binding_store(), false);
        bind_running(store.as_ref(), sandbox_id, execution_id).await;
        let writes_before = store.writes();
        let api = api_half_over(wired_service(
            &registry,
            Arc::clone(&store) as Arc<dyn crate::binding_store::BindingStore>,
            paused_row_registry(sandbox_id),
        ))
        .await;
        // The record this process holds says paused with the flag off, which
        // is what refuses the wake-up below; the binding says the cluster has
        // it running, and running wins.
        seed_paused(&api, sandbox_id, false).await;

        let outcome = api
            .resume_for_data_plane(data_plane_request(sandbox_id))
            .await;

        assert_eq!(
            outcome,
            DataPlaneResume::Woken {
                node_id: NODE_A.to_string(),
                node_address: NODE_A_ADDRESS.to_string(),
                execution_id,
                already_running: true,
            },
            "🔴 autoResume governs starting a sandbox, never routing to one that is \
             running; a refusal here is the flag silently breaking the data plane"
        );
        assert_eq!(
            store.writes(),
            writes_before,
            "the binding answered, so writing it back would only churn the record"
        );
    }

    #[tokio::test]
    async fn a_sandbox_running_only_in_the_heartbeat_ledger_is_answered_and_projected() {
        let sandbox_id = SandboxId::new();
        let execution_id = ExecutionId::new();
        let registry = registry_with_node_a();
        crate::node_registry::registry::NodeRegistry::heartbeat(
            registry.as_ref(),
            &scheduler::HeartbeatRequest {
                roster: vec![scheduler::SandboxRosterEntry {
                    sandbox_id: sandbox_id.to_string(),
                    execution_id: execution_id.to_string(),
                    projection_ttl_secs: 0,
                    paused: false,
                }],
                ..a_heartbeat(NODE_A, scheduler::NodeStatus::Ready)
            },
            std::time::SystemTime::now(),
        )
        .expect("node-a reports the sandbox");
        let store = CountingBindingStore::over(empty_binding_store(), false);
        let api = api_half_over(wired_service(
            &registry,
            Arc::clone(&store) as Arc<dyn crate::binding_store::BindingStore>,
            paused_row_registry(sandbox_id),
        ))
        .await;
        seed_paused(&api, sandbox_id, false).await;

        let outcome = api
            .resume_for_data_plane(data_plane_request(sandbox_id))
            .await;

        assert_eq!(
            outcome,
            DataPlaneResume::Woken {
                node_id: NODE_A.to_string(),
                node_address: NODE_A_ADDRESS.to_string(),
                execution_id,
                already_running: true,
            }
        );
        assert_eq!(store.writes(), 1, "a running miss is repaired on the spot");
        let binding = store
            .get(&sandbox_id.to_string(), std::time::SystemTime::now())
            .await
            .expect("the store answers")
            .expect("the projection now names the node");
        assert_eq!(binding.node.id, NODE_A);
        assert_eq!(binding.execution_id, execution_id.to_string());
    }

    /// node-a reporting the sandbox parked, as its roster does from the first
    /// heartbeat after a pause until the api tells it to forget the record.
    fn node_a_parking(
        sandbox_id: SandboxId,
    ) -> Arc<crate::node_registry::registry::AtomicNodeRegistry> {
        let registry = registry_with_node_a();
        crate::node_registry::registry::NodeRegistry::heartbeat(
            registry.as_ref(),
            &scheduler::HeartbeatRequest {
                roster: vec![scheduler::SandboxRosterEntry {
                    sandbox_id: sandbox_id.to_string(),
                    execution_id: ExecutionId::new().to_string(),
                    projection_ttl_secs: 0,
                    paused: true,
                }],
                ..a_heartbeat(NODE_A, scheduler::NodeStatus::Ready)
            },
            std::time::SystemTime::now(),
        )
        .expect("node-a reports the sandbox");
        registry
    }

    #[tokio::test]
    async fn a_sandbox_the_heartbeat_ledger_says_is_paused_is_not_answered_as_running() {
        let refused_id = SandboxId::new();
        let store = CountingBindingStore::over(empty_binding_store(), false);
        let api = api_half_over(wired_service(
            &node_a_parking(refused_id),
            Arc::clone(&store) as Arc<dyn crate::binding_store::BindingStore>,
            paused_row_registry(refused_id),
        ))
        .await;
        seed_paused(&api, refused_id, false).await;

        assert_eq!(
            api.resume_for_data_plane(data_plane_request(refused_id))
                .await,
            DataPlaneResume::AutoResumeDisabled,
            "🔴 a parked roster entry is where the bytes are, not a VM: answering \
             it as running skips the owner's auto-resume choice and hands the \
             data plane a node that answers 410"
        );
        assert_eq!(
            store.writes(),
            0,
            "a projection was written for a sandbox that has no VM to route to"
        );

        // With the flag on, the roster entry changes nothing: the sandbox takes
        // the same path as one the roster never mentioned.
        let woken_id = SandboxId::new();
        let via_roster = api_half_over(wired_service(
            &node_a_parking(woken_id),
            empty_binding_store(),
            paused_row_registry(woken_id),
        ))
        .await;
        seed_paused(&via_roster, woken_id, true).await;
        let via_registry = api_half_over(wired_service(
            &registry_with_node_a(),
            empty_binding_store(),
            paused_row_registry(woken_id),
        ))
        .await;
        seed_paused(&via_registry, woken_id, true).await;

        let outcome = via_roster
            .resume_for_data_plane(data_plane_request(woken_id))
            .await;
        assert_ne!(outcome, DataPlaneResume::AutoResumeDisabled);
        assert!(
            !matches!(outcome, DataPlaneResume::Woken { .. }),
            "the mock backend cannot bring a paused sandbox up: {outcome:?}"
        );
        assert_eq!(
            outcome,
            via_registry
                .resume_for_data_plane(data_plane_request(woken_id))
                .await,
            "the roster entry must leave the wake path exactly where the registry alone puts it"
        );
    }

    #[tokio::test]
    async fn a_paused_sandbox_with_auto_resume_off_is_refused_and_with_it_on_reaches_the_wake_path()
    {
        let refused_id = SandboxId::new();
        let registry = registry_with_node_a();
        let api = api_half_over(wired_service(
            &registry,
            empty_binding_store(),
            paused_row_registry(refused_id),
        ))
        .await;
        seed_paused(&api, refused_id, false).await;

        assert_eq!(
            api.resume_for_data_plane(data_plane_request(refused_id))
                .await,
            DataPlaneResume::AutoResumeDisabled,
            "🔴 the sandbox exists and still resumes through the REST route; \
             anything else here either wakes it against its owner's word or \
             tells the platform it is gone"
        );

        let woken_id = SandboxId::new();
        let registry = registry_with_node_a();
        let api = api_half_over(wired_service(
            &registry,
            empty_binding_store(),
            paused_row_registry(woken_id),
        ))
        .await;
        seed_paused(&api, woken_id, true).await;

        let outcome = api
            .resume_for_data_plane(data_plane_request(woken_id))
            .await;
        assert_ne!(
            outcome,
            DataPlaneResume::AutoResumeDisabled,
            "🔴 with the flag on the same path must get past the gate and fail, \
             if at all, on the wake-up itself; sharing an outcome with the \
             refused case would let a build that refuses everything pass above"
        );
        assert!(
            !matches!(outcome, DataPlaneResume::Woken { .. }),
            "the mock backend cannot bring a paused sandbox up: {outcome:?}"
        );
    }

    #[tokio::test]
    async fn a_wake_up_writes_the_projection_with_the_sandbox_s_own_budget() {
        let sandbox_id = SandboxId::new();
        let registry = registry_with_node_a();
        let store = CountingBindingStore::over(
            Arc::new(crate::binding_store::InMemoryBindingStore::new(
                crate::binding_store::BindingStoreSettings {
                    binding_ttl: std::time::Duration::from_secs(30),
                    projection_authoritative: true,
                },
            )),
            false,
        );
        let api = api_half_over(wired_authoritative_service(
            &registry,
            Arc::clone(&store) as Arc<dyn crate::binding_store::BindingStore>,
            paused_row_registry(sandbox_id),
        ))
        .await;
        // Running already, so the wake-up is the idempotent success; the row
        // above keeps the lookup from answering it as running outright.
        api.orchestrator()
            .set_proxy_target_for_test(
                sandbox_id,
                crate::orchestrator::ProxyTarget::new(std::net::Ipv4Addr::LOCALHOST),
                SandboxState::Running,
            )
            .await;
        api.orchestrator()
            .set_max_lifetime_for_test(&sandbox_id, std::time::Duration::from_secs(600))
            .await
            .expect("cap the lifetime");
        let started = std::time::SystemTime::now();

        let outcome = api
            .resume_for_data_plane(data_plane_request(sandbox_id))
            .await;

        let DataPlaneResume::Woken {
            node_id,
            execution_id,
            ..
        } = outcome
        else {
            panic!("a running sandbox is a successful wake-up: {outcome:?}");
        };
        assert_eq!(node_id, NODE_A);
        assert_eq!(store.writes(), 1, "the wake-up wrote the projection once");
        let key = sandbox_id.to_string();
        let binding = store
            .get(&key, started)
            .await
            .expect("the store answers")
            .expect("the projection names the node the sandbox woke on");
        assert_eq!(binding.node.id, NODE_A);
        assert_eq!(binding.execution_id, execution_id.to_string());
        // 🔴 The budget is the sandbox's remaining lifetime, not the store's
        // 30 s default: a projection that expires before the sandbox does
        // costs a resume RPC on every request after its first half minute.
        assert!(
            store
                .get(&key, started + std::time::Duration::from_secs(300))
                .await
                .expect("the store answers")
                .is_some(),
            "the projection expired on the store's default TTL"
        );
        assert!(
            store
                .get(
                    &key,
                    started + std::time::Duration::from_secs(600 + 86_400 + 1)
                )
                .await
                .expect("the store answers")
                .is_none(),
            "the projection outlives every lifetime the sandbox could have"
        );
    }

    #[tokio::test]
    async fn a_projection_write_that_fails_does_not_fail_the_wake_up() {
        let sandbox_id = SandboxId::new();
        let registry = registry_with_node_a();
        let store = CountingBindingStore::over(empty_binding_store(), true);
        let api = api_half_over(wired_service(
            &registry,
            Arc::clone(&store) as Arc<dyn crate::binding_store::BindingStore>,
            paused_row_registry(sandbox_id),
        ))
        .await;
        api.orchestrator()
            .set_proxy_target_for_test(
                sandbox_id,
                crate::orchestrator::ProxyTarget::new(std::net::Ipv4Addr::LOCALHOST),
                SandboxState::Running,
            )
            .await;

        let outcome = api
            .resume_for_data_plane(data_plane_request(sandbox_id))
            .await;

        assert!(
            matches!(outcome, DataPlaneResume::Woken { ref node_id, .. } if node_id == NODE_A),
            "🔴 the answer still names the node; the store being away costs the \
             next request a resume RPC, not this one its sandbox: {outcome:?}"
        );
        assert_eq!(store.writes(), 1, "the write was attempted");
    }
}
