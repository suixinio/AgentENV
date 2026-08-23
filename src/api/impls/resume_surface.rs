//! Waking a paused sandbox on behalf of the data plane.
//!
//! # Why this module exists
//!
//! It used to be a function in the local reverse proxy. `try_auto_resume` sat
//! on `src/api/proxy.rs`'s request path and took three decisions — arbitrate,
//! start, hand the claim back — on the node the traffic happened to arrive at.
//! That is the arrangement `--role node` exists to end: a node that decides
//! when a sandbox should be alive is not an executor, and it needs a
//! decision-making orchestrator to be one.
//!
//! So the decision moved here, to the half that owns sandboxes, and the data
//! plane reaches it over one gRPC call (`crate::api::grpc::resume`). The local
//! reverse proxy keeps forwarding bytes and keeps its execution fencing; what
//! it no longer does is start anything.
//!
//! # 🔴 What did *not* move
//!
//! `try_auto_resume` is still in `src/api/proxy.rs`, still compiled, still
//! reached — under `--role all`, which is the rollback target and is defined as
//! today's behaviour verbatim. The call site became a role branch rather than a
//! deletion (`_sd-impl-phase3-role.md` §11.3). Deleting it belongs to a release
//! after the switch, not to the switch.
//!
//! # 🔴 Pin and prefer are two different answers
//!
//! A paused sandbox whose snapshot reached shared storage can be rebuilt
//! anywhere; origin is a preference. A paused sandbox whose snapshot did *not*
//! — `publishing`, or `local_only` after a failed upload — exists as bytes on
//! exactly one disk, and waking it anywhere else does not fail. It *succeeds*,
//! by rewinding the sandbox to whatever older snapshot did reach storage, and
//! the user sees a workspace that has silently lost work.
//!
//! Arbitration cannot tell those apart: `claim_for_resume` grants a lapsed
//! `local_only` row to any node that asks, and logs the rewind. The two-tier
//! decision lives in the scheduler's `LookupNode`
//! (`services/scheduler/internal/lookup.go`), which answers `PINNED` for the
//! unpublished states and `PLACED` for the published one — so this module
//! consumes it rather than reimplementing it, and refuses a pin it cannot
//! honour instead of falling back to "some node".

use std::sync::Arc;

use async_trait::async_trait;
use tonic::transport::{Channel, Endpoint};
use tracing::{debug, warn};

use super::paused_recovery::{CrossNodeResume, MissingLocalResume};
use super::{ApiImpl, ResumeArbitration};
use crate::cfg::ConfigManager;
use crate::orchestrator::{
    ClaimedExecution, NewTimeout, OrchestratorError, PausedSandboxEntry, SandboxMetadata,
};
use crate::proto::scheduler::{self, scheduler_client::SchedulerClient};
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

/// Where the cluster says a paused sandbox may be woken.
///
/// 🔴 Three answers, not two. "Origin is required", "origin is preferred" and
/// "there is nobody to ask" are three different things, and the third is not a
/// degenerate case of either: a single-node deployment has no placement source
/// at all, and collapsing it into "unconstrained placement on origin" would
/// invent an origin that no row names.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::api) enum ResumePlacement {
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

/// Why a pinned sandbox cannot be woken.
///
/// 🔴 These are the strings on the wire, and they are the whole reason the
/// refusal is structured rather than a message: the first says "wait a moment",
/// the rest say "wait for a machine, possibly forever". A caller that cannot
/// tell them apart either hammers a node that is never coming back or gives up
/// on one that is three seconds away.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
// 🔴 The shared `Origin` prefix is the point, not an accident: each variant is
// spelled on the wire as `origin_*` (see `as_str`), and that spelling is
// simultaneously a metric label and a trailer value the gateway matches on.
// Dropping the prefix to satisfy the lint would unjoin the enum from the
// contract it exists to name.
#[allow(clippy::enum_variant_names)]
pub(in crate::api) enum PinRefusalReason {
    /// The node holding the only copy has not been heard from.
    OriginNotReporting,
    /// The node holding the only copy is draining.
    OriginNotAcceptingWork,
    /// The node holding the only copy is fine, but this process cannot wake a
    /// sandbox on another machine.
    ///
    /// 🔴 Reachable only in the shape where the API half runs inside a process
    /// that also runs sandboxes, i.e. `--role all`. Once the remote backend
    /// factory lands, the API half drives any node and this stops being
    /// possible. It is a refusal and not a fallback on purpose — the fallback
    /// is the rewind.
    OriginNotReachableFromHere,
    /// The placement source refused a pin for a reason this build did not
    /// recognise.
    ///
    /// 🔴 The classification is a match on the placement source's message —
    /// the scheduler emits those two refusals with no structured field — so a
    /// reworded message has to degrade to "do not try anywhere else" rather
    /// than to "this was not a pin refusal at all". This variant is that
    /// degradation, and a non-zero count of it is the signal that the two ends
    /// have drifted.
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

/// Who answers "where may this sandbox be woken".
///
/// A trait rather than a concrete scheduler client for two reasons: the answer
/// is the load-bearing input to a decision that can silently lose a user's
/// work, so it has to be drivable from a test; and phase 4 replaces the
/// scheduler with `src/orchestrator/placement/`, at which point this is the
/// seam that moves.
#[async_trait]
pub(in crate::api) trait ResumePlacementSource: Send + Sync {
    async fn locate(&self, sandbox_id: SandboxId) -> Result<ResumePlacement, PlacementRefusal>;
}

/// Whether this process wakes sandboxes on its own machine, and which machine
/// that is.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::api) enum WakeSite {
    /// The orchestration surface behind this `ApiImpl` runs sandboxes on this
    /// machine, named here. A pin naming any other node cannot be honoured.
    Local(String),
    /// The orchestration surface places the wake-up on whichever machine it
    /// decides, so the pin is that surface's to honour.
    ///
    /// 🔴 Nothing in `src/bin/` constructs this yet. It is what `--role api`
    /// will pass once the remote backend factory exists, and the factory has to
    /// take the placement with it — resume places through `LookupNode`, create
    /// places through `Schedule` (`_sd-impl-phase3-role.md` §6.6). Leaving the
    /// pin unenforced here without that is the rewind, so the two land
    /// together.
    ///
    /// The allow is the §15.3 shape stated out loud: this is code that lands
    /// before its driver. It is not unreachable — `refuse_unhonourable_pin`'s
    /// delegation branch is exercised through it by
    /// `a_pin_is_enforced_here_only_when_this_process_is_the_one_placing_the_wake_up`,
    /// so the branch is wrong in a test rather than in production on the day
    /// the factory is wired.
    #[allow(dead_code)]
    Remote,
}

/// Everything the resume surface needs beyond what `ApiImpl` already holds.
#[derive(Clone)]
pub struct ResumeWiring {
    placement: Option<Arc<dyn ResumePlacementSource>>,
    wake_site: WakeSite,
}

impl ResumeWiring {
    /// The general constructor, and the seam a test injects a placement source
    /// through.
    ///
    /// Production builds this through [`Self::from_config`] or
    /// [`Self::node_local`]; this one exists for the two cases neither covers —
    /// a stub placement source, and the `WakeSite::Remote` that arrives with
    /// the remote backend factory.
    #[allow(dead_code)]
    pub(in crate::api) fn new(
        placement: Option<Arc<dyn ResumePlacementSource>>,
        wake_site: WakeSite,
    ) -> Self {
        Self {
            placement,
            wake_site,
        }
    }

    /// The wiring for a process with no cluster to ask: every placement is
    /// unconstrained and the wake happens here.
    pub fn node_local(node_id: impl Into<String>) -> Self {
        Self {
            placement: None,
            wake_site: WakeSite::Local(node_id.into()),
        }
    }

    /// Builds the scheduler-backed wiring from configuration, or the node-local
    /// one when no scheduler endpoint is configured.
    ///
    /// 🔴 Lazy connect. `Endpoint::connect_lazy` does not touch the network, so
    /// a scheduler that is down at startup delays a resume rather than a
    /// process.
    pub fn from_config(node_id: impl Into<String>) -> anyhow::Result<Self> {
        let node_id = node_id.into();
        let Some(endpoint) = ConfigManager::global_config()
            .cluster
            .scheduler_endpoint
            .as_deref()
            .map(str::trim)
            .filter(|endpoint| !endpoint.is_empty())
        else {
            return Ok(Self::node_local(node_id));
        };

        let channel = Endpoint::from_shared(qualified_endpoint(endpoint))?.connect_lazy();
        Ok(Self {
            placement: Some(Arc::new(SchedulerPlacementSource::new(channel))),
            wake_site: WakeSite::Local(node_id),
        })
    }
}

fn qualified_endpoint(endpoint: &str) -> String {
    if endpoint.contains("://") {
        endpoint.to_string()
    } else {
        format!("http://{endpoint}")
    }
}

/// The scheduler's `LookupNode`, read as a placement answer.
struct SchedulerPlacementSource {
    channel: Channel,
}

impl SchedulerPlacementSource {
    fn new(channel: Channel) -> Self {
        Self { channel }
    }
}

#[async_trait]
impl ResumePlacementSource for SchedulerPlacementSource {
    async fn locate(&self, sandbox_id: SandboxId) -> Result<ResumePlacement, PlacementRefusal> {
        let mut client = SchedulerClient::new(self.channel.clone());
        match client
            .lookup_node(scheduler::LookupNodeRequest {
                sandbox_id: sandbox_id.to_string(),
            })
            .await
        {
            Ok(response) => placement_from_lookup(response.into_inner()),
            Err(status) => Err(refusal_from_status(&status)),
        }
    }
}

/// Reads a `LookupNode` answer as a placement.
///
/// 🔴 `BOUND` is a preference and not a pin. It says a node is believed to hold
/// the sandbox, which for a wake-up is a hint about where the layers are, not a
/// statement that the bytes exist nowhere else — the row behind it is `running`
/// or `resuming`, and both of those name a *published* sandbox. Treating it as
/// a pin would refuse ordinary resumes whenever a stale binding pointed at a
/// node that had since drained.
fn placement_from_lookup(
    response: scheduler::LookupNodeResponse,
) -> Result<ResumePlacement, PlacementRefusal> {
    let location = response.location();
    let node = response.node.map(|node| PlacedNode {
        node_id: node.node_id,
        address: node.endpoint,
    });
    let Some(node) = node else {
        // A success carrying no node is not an answer. Saying so is the
        // difference between a retry and a resume on a node called "".
        return Err(PlacementRefusal::Unavailable(
            "the placement source answered without naming a node".to_string(),
        ));
    };

    match location {
        scheduler::SandboxLocation::Pinned => Ok(ResumePlacement::Pinned { node }),
        scheduler::SandboxLocation::Placed
        | scheduler::SandboxLocation::Bound
        | scheduler::SandboxLocation::Unspecified => Ok(ResumePlacement::Preferred {
            node,
            origin_node_id: response.origin_node_id,
        }),
    }
}

/// The message fragments the scheduler refuses a pin with.
///
/// 🔴 Fragments of a `status.Errorf` format string in another language's
/// repository, matched here. That is as fragile as it looks, and it is the only
/// channel available: `LookupNode` has no structured refusal field, adding one
/// is the scheduler's change to make, and this half still has to tell "wait a
/// moment" from "wait for a machine". The degradation is deliberate — an
/// unmatched `FailedPrecondition` stays a pin refusal
/// ([`PinRefusalReason::OriginUnclassified`]) and is therefore still never
/// retried on another node.
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

/// Pulls the node name out of the scheduler's `%q`-formatted refusal.
///
/// Best effort by construction: it is used to make an operator's log line
/// nameable, never to decide anything.
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
    },
    /// The caller did not present the sandbox's envd access token.
    Unauthorized,
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
    /// Nobody could be asked. 🔴 Never an answer about whether the sandbox
    /// exists: the downstream contract for "it does not exist" is "rebuild it
    /// from its template", which resets a user's workspace.
    Undecided(String),
    /// The wake-up was attempted and failed.
    Failed(String),
    /// The wake-up was attempted and did not finish in time.
    ///
    /// 🔴 Kept apart from [`Self::Failed`] for the metric and the log, not for
    /// the wire: both answer the caller the same way, which is what
    /// `try_auto_resume` did (`AutoResumeFailed` and `AutoResumeTimedOut` are
    /// both a 502). A wedged wake-up and a wake-up that returned an error need
    /// different investigations, and only the counter can tell them apart.
    TimedOut,
}

/// Whether the caller may wake this sandbox.
///
/// 🔴 Three answers. "Not authorized" and "there is no record here to check
/// against" are different, and the second is the ordinary case on the cold
/// path: the whole point of this surface is that it is asked about sandboxes
/// the asking process has never run. Collapsing them would either reject every
/// cross-node wake-up or accept every unauthenticated one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EnvdAuthorization {
    Authorized,
    Rejected,
    Unknown,
}

impl ApiImpl {
    /// Wakes a paused sandbox on behalf of the data plane.
    ///
    /// This is the cold path: the gateway's routing projection had no answer,
    /// so the sandbox is either paused, gone, or somewhere the projection has
    /// not caught up with. All three end here.
    ///
    /// # Order
    ///
    /// Placement first, because it is the only step that can refuse a pin, and
    /// a pin refused after the claim has been taken is a claim to hand back for
    /// nothing. Authorization next, as far as it can be taken without a record.
    /// Arbitration third, because it is the single point that grants the right
    /// to start — the same point the REST resume route goes through, which is
    /// what keeps the two from ever bringing up two copies of one sandbox.
    pub(in crate::api) async fn resume_for_data_plane(
        &self,
        request: DataPlaneResumeRequest,
    ) -> DataPlaneResume {
        let sandbox_id = request.sandbox_id;

        let placement = match self.locate_for_resume(sandbox_id).await {
            Ok(placement) => placement,
            Err(refusal) => return refusal.into(),
        };
        if let Some(refused) = self.refuse_unhonourable_pin(sandbox_id, &placement) {
            return refused;
        }

        // As much of the credential check as can be done before anything is
        // claimed. On a node that already has the record — the warm case, and
        // the one an unauthenticated caller would be probing — this is the
        // whole check.
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

        // 🔴 The same call the REST resume route makes, and deliberately not a
        // second way of asking. Both paths end with a live sandbox, so a resume
        // that arbitrated separately would be a second place two nodes could be
        // told "yes" from.
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

        // The rest of the credential check, now that the cluster row may have
        // supplied the record this process did not have.
        if authorized == EnvdAuthorization::Unknown {
            let from_entry = entry.as_ref().and_then(|entry| entry.metadata.as_ref());
            if self.authorize_envd(&request, from_entry) == EnvdAuthorization::Rejected {
                if let Some(generation) = held {
                    self.abandon_claim(sandbox_id, generation).await;
                }
                return DataPlaneResume::Unauthorized;
            }
        }

        self.wake(sandbox_id, entry, claimed, held, &placement)
            .await
    }

    /// Runs the wake-up itself: local artifacts first, the cluster's snapshot
    /// second.
    async fn wake(
        &self,
        sandbox_id: SandboxId,
        entry: Option<Box<PausedSandboxEntry>>,
        claimed: ClaimedExecution,
        held: Option<i64>,
        placement: &ResumePlacement,
    ) -> DataPlaneResume {
        let timeout = NewTimeout::EnsureMinimum(auto_resume_min_sandbox_timeout());
        // 🔴 A wall-clock bound, because the function this replaced had one.
        //
        // `try_auto_resume` wrapped exactly this call in
        // `PROXY_AUTO_RESUME_TIMEOUT` and, on expiry, handed the claim back
        // before answering. Without it a wake-up that wedges holds the
        // gateway's request open and — the half that actually costs something —
        // leaves the cluster row sitting in `resuming` with this node's name on
        // it until the lease lapses, which blocks every later attempt to wake
        // the same sandbox anywhere.
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
                // The orchestrator repoints the cluster record for a resume it
                // performed, but not for one that found the sandbox already
                // running. Saying it again is idempotent and keeps a claim from
                // sitting in `resuming` until its lease lapses.
                if held.is_some() {
                    self.paused
                        .mark_sandbox_running(
                            sandbox_id,
                            metadata.execution_id,
                            metadata.expires_at,
                        )
                        .await;
                }
                self.woken(metadata, placement)
            }
            Err(OrchestratorError::SandboxNotFound(_)) => {
                // Nothing local. With a row in hand the cluster still has a
                // snapshot to rebuild from, under the same id.
                let Some(entry) = entry else {
                    return self.resolve_missing(sandbox_id, placement).await;
                };
                match self
                    .restore_claimed_sandbox(
                        *entry,
                        NewTimeout::EnsureMinimum(auto_resume_min_sandbox_timeout()),
                    )
                    .await
                {
                    CrossNodeResume::Restored(metadata) => self.woken(*metadata, placement),
                    CrossNodeResume::NotFound => DataPlaneResume::NotFound,
                    CrossNodeResume::Failed(reason) => DataPlaneResume::Failed(reason),
                }
            }
            Err(err) => {
                warn!(%sandbox_id, error = %err, "waking a sandbox for the data plane failed");
                if let Some(generation) = held {
                    self.abandon_claim(sandbox_id, generation).await;
                }
                DataPlaneResume::Failed(err.to_string())
            }
        }
    }

    /// The answer when this process has no local copy and took no claim.
    ///
    /// 🔴 Not a 404 by default. Under concurrency the loser of a race arrives
    /// here while the winner is bringing the same sandbox up, and the
    /// downstream contract for "it does not exist" is "rebuild it from its
    /// template" — which resets the user's workspace.
    async fn resolve_missing(
        &self,
        sandbox_id: SandboxId,
        placement: &ResumePlacement,
    ) -> DataPlaneResume {
        match self.resolve_missing_local_resume(sandbox_id).await {
            MissingLocalResume::Unknown => DataPlaneResume::NotFound,
            MissingLocalResume::Resumed(metadata) => self.woken(*metadata, placement),
            MissingLocalResume::Busy { holder } => DataPlaneResume::TransitionInProgress { holder },
            MissingLocalResume::Undecided(reason) => DataPlaneResume::Undecided(reason),
        }
    }

    /// Names the node and incarnation the caller should now address.
    ///
    /// 🔴 Where the sandbox **actually woke**, which is not always where the
    /// placement wanted it. `node_id` is documented on the wire as "the node
    /// the sandbox is running on now", and the gateway forwards the request
    /// that triggered the wake-up straight at it — so naming a preference
    /// instead of a fact sends that request to a machine the sandbox is not on,
    /// where it fails, immediately after a wake-up that succeeded.
    ///
    /// The two cases differ in who did the placing:
    /// - [`WakeSite::Local`] — this process woke it, on its own machine. A
    ///   `Preferred` placement naming another node did not get its preference:
    ///   origin is a hint for a published snapshot, and this is the path that
    ///   ignores the hint. Answer with this machine.
    /// - [`WakeSite::Remote`] — the orchestration surface chose the machine, so
    ///   the placement is where it went.
    fn woken(&self, metadata: SandboxMetadata, placement: &ResumePlacement) -> DataPlaneResume {
        let (node_id, node_address) = match &self.resume_wiring.wake_site {
            WakeSite::Local(here) => {
                // The address is only usable when the placement is talking
                // about this same machine; otherwise it belongs to the node
                // that did not get the wake-up.
                //
                // 🔴 An empty address rather than a guess. Nothing in a
                // single-node deployment knows this process's routable address
                // — the listen address may be a wildcard — and a caller that
                // proxied to a guessed one would fail in a way that looks like
                // the sandbox is broken. The gateway reads an empty address as
                // "look it up yourself".
                let address = match placement {
                    ResumePlacement::Pinned { node } | ResumePlacement::Preferred { node, .. }
                        if node.node_id == *here =>
                    {
                        node.address.clone()
                    }
                    _ => String::new(),
                };
                (here.clone(), address)
            }
            WakeSite::Remote => match placement {
                ResumePlacement::Pinned { node } | ResumePlacement::Preferred { node, .. } => {
                    (node.node_id.clone(), node.address.clone())
                }
                ResumePlacement::Unconstrained => {
                    (self.paused.node_id().to_string(), String::new())
                }
            },
        };

        DataPlaneResume::Woken {
            node_id,
            node_address,
            execution_id: metadata.execution_id,
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

    /// Refuses a pin this process cannot honour.
    ///
    /// 🔴 The only thing standing between an unpublished pause and a silent
    /// rewind. `claim_for_resume` grants a lapsed `local_only` row to whoever
    /// asks — by design, so a dead node's sandboxes are not stranded forever —
    /// and the rebuild that follows uses the last snapshot that *did* reach
    /// storage. On the wake-up path that is not a recovery, it is data loss
    /// with a success status.
    fn refuse_unhonourable_pin(
        &self,
        sandbox_id: SandboxId,
        placement: &ResumePlacement,
    ) -> Option<DataPlaneResume> {
        let ResumePlacement::Pinned { node } = placement else {
            return None;
        };
        let WakeSite::Local(here) = &self.resume_wiring.wake_site else {
            // The orchestration surface places this one; the pin travels with
            // it. See `WakeSite::Remote`.
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

    /// The envd credential check, as far as the record in hand allows.
    fn authorize_envd(
        &self,
        request: &DataPlaneResumeRequest,
        metadata: Option<&SandboxMetadata>,
    ) -> EnvdAuthorization {
        let control_plane_port = ConfigManager::global_config().tools.control_plane_port;
        // 🔴 `None` is treated as envd traffic, not as "some other port". A
        // caller that omits the port must not thereby skip the check.
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
/// Shared with the local reverse proxy's `try_auto_resume` rather than
/// duplicated: a wake-up that came through the gateway and one that came
/// through a node's own proxy must not leave the sandbox with different
/// lifetimes.
fn auto_resume_min_sandbox_timeout() -> std::time::Duration {
    crate::api::proxy::auto_resume_min_sandbox_timeout()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 🔴 The scheduler's refusal format strings, reproduced verbatim from
    /// `services/scheduler/internal/lookup.go:294` and `:303`.
    ///
    /// This is the whole reason these tests exist. The classification is a
    /// substring match on a `status.Errorf` format string that lives in another
    /// language, in another module, with no compiler edge between the two ends
    /// — so nothing but a test notices when somebody rewords it. If these ever
    /// stop matching, the tests below fail on the classification rather than on
    /// the wording, which is the failure that matters.
    fn scheduler_not_reporting(state: &str, node: &str) -> String {
        format!("sandbox is {state} on node {node:?}, which is not reporting")
    }

    fn scheduler_not_accepting_work(state: &str, node: &str) -> String {
        format!("sandbox is {state} on node {node:?}, which is not accepting work")
    }

    fn response(
        node: Option<(&str, &str)>,
        location: scheduler::SandboxLocation,
        origin_node_id: &str,
    ) -> scheduler::LookupNodeResponse {
        scheduler::LookupNodeResponse {
            node: node.map(|(node_id, endpoint)| scheduler::Node {
                node_id: node_id.to_string(),
                endpoint: endpoint.to_string(),
            }),
            location: location as i32,
            origin_node_id: origin_node_id.to_string(),
            ..Default::default()
        }
    }

    // -----------------------------------------------------------------------
    // 🔴 Pin and prefer (§6.6)
    // -----------------------------------------------------------------------

    /// 🔴 `PINNED` is the only location that pins. Asserted next to the three
    /// that do not, in one test, because "everything is a pin" and "nothing is
    /// a pin" each pass half of this on their own — and the first turns every
    /// node rolling-restart into a batch of unrecoverable sandboxes, while the
    /// second silently rewinds unpublished ones.
    #[test]
    fn only_a_pinned_location_pins_and_the_other_three_are_preferences() {
        let pinned = placement_from_lookup(response(
            Some(("origin", "http://origin:8000")),
            scheduler::SandboxLocation::Pinned,
            "origin",
        ))
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

        // 🔴 BOUND especially. A bound row is `running` or `resuming`, both of
        // which name a *published* sandbox — so it is a hint about where the
        // layers are, not a claim that the bytes exist nowhere else. Reading it
        // as a pin would refuse ordinary resumes whenever a stale binding
        // pointed at a node that had since drained.
        for location in [
            scheduler::SandboxLocation::Placed,
            scheduler::SandboxLocation::Bound,
            scheduler::SandboxLocation::Unspecified,
        ] {
            let placement = placement_from_lookup(response(
                Some(("chosen", "http://chosen:8000")),
                location,
                "origin",
            ))
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

    /// 🔴 A success that names no node is not an answer.
    ///
    /// Paired with the same location carrying a node, so this is a statement
    /// about the missing node rather than about the location.
    #[test]
    fn an_answer_with_no_node_is_unavailable_rather_than_a_placement_on_nobody() {
        let nameless =
            placement_from_lookup(response(None, scheduler::SandboxLocation::Placed, "origin"))
                .expect_err("half an answer is not an address");
        assert!(
            matches!(nameless, PlacementRefusal::Unavailable(_)),
            "a resume placed on a node called \"\" fails in a way that looks like \
             the sandbox is broken; {nameless:?}"
        );

        assert!(
            placement_from_lookup(response(
                Some(("chosen", "")),
                scheduler::SandboxLocation::Placed,
                "origin",
            ))
            .is_ok(),
            "an empty *address* is fine — the gateway resolves the node itself. \
             It is a missing node that is not an answer"
        );
    }

    // -----------------------------------------------------------------------
    // 🔴 Classifying the scheduler's refusals
    // -----------------------------------------------------------------------

    /// 🔴 The two refusals a caller must be able to tell apart, plus the
    /// degradation for a third the build does not recognise.
    ///
    /// All three stay `Pinned`. That is the load-bearing half: for an
    /// unpublished pause there is no second copy of the bytes, so "retry
    /// somewhere else" does not fail — it *succeeds*, by rewinding the sandbox
    /// to whatever older snapshot did reach shared storage. A misclassification
    /// that turned any of these into a retryable answer would be data loss with
    /// a success status.
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

    /// The reasons that are not pin refusals, so the test above is a statement
    /// about `FailedPrecondition` rather than about every status.
    ///
    /// 🔴 `Unavailable` and `DeadlineExceeded` must never become `NotFound`.
    /// The downstream contract for "it does not exist" is "rebuild it from its
    /// template", which resets a user's workspace — so "nobody could be asked"
    /// arriving as "it is gone" costs the user their files.
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

    // -----------------------------------------------------------------------
    // Endpoint normalisation
    // -----------------------------------------------------------------------

    /// A bare `host:port` is what `[cluster].scheduler_endpoint` usually holds,
    /// and `Endpoint::from_shared` rejects it without a scheme.
    #[test]
    fn a_scheme_is_added_only_when_one_is_missing() {
        assert_eq!(
            qualified_endpoint("scheduler:9090"),
            "http://scheduler:9090"
        );
        assert_eq!(
            qualified_endpoint("http://scheduler:9090"),
            "http://scheduler:9090"
        );
        assert_eq!(
            qualified_endpoint("https://scheduler:9090"),
            "https://scheduler:9090",
            "a configured TLS endpoint must not be downgraded to plaintext"
        );
    }
}
