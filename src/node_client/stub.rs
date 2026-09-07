//! Drives one sandbox on another node.
//!
//! Transport failure never means the sandbox is absent. A remote pause hands back
//! the snapshot the node staged; the api half commits it and the sandbox is gone
//! from the node. Runtime artifacts are local-only and therefore empty on the
//! deciding half.

use std::future::Future;
use std::net::Ipv4Addr;
use std::pin::Pin;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use tonic::transport::Endpoint;
use tracing::{info, warn};

use crate::proto::node as pb;
use crate::sandbox::{
    CapturedSandboxSnapshot, CustomExtensionParams, ResolvedImageFacts, RuntimeArtifactSet,
    RuntimeConfirmedGone, SandboxBackend, SandboxCaptureError, SandboxCaptureResult,
    SandboxForkResult, SandboxForkSpec, SandboxNetworkPolicy, SandboxRuntimeInfo,
};
use crate::snapshot::repository::interfaces::StagedSnapshot;
use crate::types::{ExecutionId, SandboxId, SandboxResources};

use super::placement::{NodeEndpoint, NodeMembership, NodePlacement, PlacementNeeds};
use super::wire;

use std::sync::Arc;

/// A launch deferred until asynchronous `start`; construction performs no network I/O.
pub enum PendingLaunch {
    /// A create request ready to send.
    Launch {
        request: Box<pb::SandboxCreateRequest>,
        /// Where the sandbox's bytes were last warm, if anywhere.
        preferred_node_id: Option<String>,
        /// Node capabilities the launch needs, read off the launch config.
        needs: PlacementNeeds,
    },
    /// A fork child the node already started.
    AlreadyStarted,
    /// Attaches to a sandbox already running on an as-yet unresolved node.
    Attach,
}

/// One sandbox on another node.
/// How many nodes one launch may try before its last refusal is the answer.
const PLACEMENT_ATTEMPTS: usize = 3;

pub struct RemoteSandboxStub {
    sandbox_id: SandboxId,
    execution_id: ExecutionId,
    resources: SandboxResources,
    placement: Arc<dyn NodePlacement>,
    pending: PendingLaunch,
    /// Who the deciding half's record says owns this id, consulted only when a
    /// node answers a create with a copy of its own.
    record_owner: Arc<dyn super::record_owner::SandboxRecordOwner>,
    placed: Option<Placed>,
    /// Suppresses `Delete` after pause because the node's pause already stopped the VM
    /// and `Delete` would destroy its only capture.
    paused: bool,
    /// Budget of the routing record `announce_placement` writes; zero until the
    /// orchestrator sets it from the sandbox's own record.
    projection_ttl_secs: u32,
}

struct Placed {
    node: NodeEndpoint,
    client: super::NodeClient,
    host_interaction_ip: Option<Ipv4Addr>,
    rootfs_virtual_size: Option<u64>,
    /// Image facts learned from an image-source create reply.
    resolved_image_facts: Option<ResolvedImageFacts>,
}

/// Facts reported by a live sandbox handle; two `None`s are still a valid answer.
#[derive(Default)]
struct LiveFacts {
    host_interaction_ip: Option<Ipv4Addr>,
    rootfs_virtual_size: Option<u64>,
}

impl RemoteSandboxStub {
    pub fn pending(
        sandbox_id: SandboxId,
        execution_id: ExecutionId,
        resources: SandboxResources,
        placement: Arc<dyn NodePlacement>,
        pending: PendingLaunch,
    ) -> Self {
        Self {
            sandbox_id,
            execution_id,
            resources,
            placement,
            pending,
            record_owner: super::record_owner::UnknownRecordOwner::shared(),
            placed: None,
            paused: false,
            projection_ttl_secs: 0,
        }
    }

    pub fn already_running(
        sandbox_id: SandboxId,
        execution_id: ExecutionId,
        resources: SandboxResources,
        placement: Arc<dyn NodePlacement>,
        node: NodeEndpoint,
        client: super::NodeClient,
        ack: &pb::SandboxCreateResponse,
    ) -> Self {
        Self {
            sandbox_id,
            execution_id,
            resources,
            placement,
            pending: PendingLaunch::AlreadyStarted,
            record_owner: super::record_owner::UnknownRecordOwner::shared(),
            paused: false,
            projection_ttl_secs: 0,
            placed: Some(Placed {
                node,
                client,
                host_interaction_ip: wire::host_ip(&ack.host_interaction_ip),
                rootfs_virtual_size: (ack.rootfs_virtual_size > 0)
                    .then_some(ack.rootfs_virtual_size),
                resolved_image_facts: None,
            }),
        }
    }

    /// Names who to ask when a node answers a create with a copy of its own.
    pub fn with_record_owner(
        mut self,
        record_owner: Arc<dyn super::record_owner::SandboxRecordOwner>,
    ) -> Self {
        self.record_owner = record_owner;
        self
    }

    /// Builds a stub that resolves and attaches during `start`, without creating a VM.
    pub fn attaching(
        sandbox_id: SandboxId,
        execution_id: ExecutionId,
        resources: SandboxResources,
        placement: Arc<dyn NodePlacement>,
    ) -> Self {
        Self::pending(
            sandbox_id,
            execution_id,
            resources,
            placement,
            PendingLaunch::Attach,
        )
    }

    /// Resolves the existing sandbox and reads live facts before marking it placed.
    ///
    /// Incarnation fencing remains on every subsequent node RPC.
    async fn attach(&mut self) -> Result<()> {
        let sandbox_id = self.sandbox_id;
        let node = self
            .placement
            .place_existing(sandbox_id)
            .await
            .with_context(|| format!("locate the machine running sandbox {sandbox_id}"))?
            // A complete lookup that finds nothing is a verdict, not a gap: an
            // assignment is reserved before the node is asked to start anything and
            // is refreshed by the node's roster for as long as it runs, so nothing
            // this answer could be about is still running anywhere.
            .ok_or_else(|| {
                RuntimeConfirmedGone(anyhow!(
                    "the placement source has a complete view and no record of sandbox \
                     {sandbox_id}, so no machine is running it"
                ))
            })?;
        let mut client = match Self::connect(&node.endpoint).await {
            Ok(client) => client,
            Err(dial_error) => {
                return Err(self.confirm_or_defer(node, sandbox_id, dial_error).await)
            }
        };

        // Read live facts before publishing `placed`.
        let facts = Self::live_facts(&mut client, &node.node_id, sandbox_id).await?;

        self.placed = Some(Placed {
            node,
            client,
            host_interaction_ip: facts.host_interaction_ip,
            rootfs_virtual_size: facts.rootfs_virtual_size,
            resolved_image_facts: None,
        });
        Ok(())
    }

    /// Converts an unreachable runtime to confirmed-gone only when node discovery
    /// independently reports that the node has left the cluster.
    async fn confirm_or_defer(
        &self,
        node: NodeEndpoint,
        sandbox_id: SandboxId,
        dial_error: anyhow::Error,
    ) -> anyhow::Error {
        let unreachable = dial_error.context(format!(
            "reach node {} for sandbox {sandbox_id}",
            node.node_id
        ));
        match self.placement.node_membership(&node.node_id).await {
            Ok(NodeMembership::Gone) => RuntimeConfirmedGone(unreachable.context(format!(
                "node {} is no longer part of the cluster",
                node.node_id
            )))
            .into(),
            Ok(NodeMembership::Present) | Err(_) => unreachable,
        }
    }

    /// Reads facts from the node's live handle.
    ///
    /// `NotFound` is an empty answer; transport failure remains an error.
    async fn live_facts(
        client: &mut super::NodeClient,
        node_id: &str,
        sandbox_id: SandboxId,
    ) -> Result<LiveFacts> {
        let response = match client
            .describe(pb::SandboxDescribeRequest {
                sandbox_id: sandbox_id.to_string(),
            })
            .await
        {
            Ok(response) => response.into_inner(),
            Err(status) if status.code() == tonic::Code::NotFound => {
                return Ok(LiveFacts::default())
            }
            Err(status) => {
                return Err(wire::into_error(status)).with_context(|| {
                    format!("ask node {node_id} what it is running for sandbox {sandbox_id}")
                })
            }
        };

        // Record-only facts are incomplete while the live handle is busy; retry instead.
        if !response.facts_from_handle {
            bail!(
                "node {node_id} could not read sandbox {sandbox_id}'s live facts: its handle was \
                 busy, so the address and rootfs size it answered with are blanks rather than \
                 that sandbox's"
            );
        }

        Ok(LiveFacts {
            host_interaction_ip: wire::host_ip(&response.host_interaction_ip),
            rootfs_virtual_size: (response.rootfs_virtual_size > 0)
                .then_some(response.rootfs_virtual_size),
        })
    }

    fn placed(&self) -> Result<&Placed> {
        self.placed.as_ref().ok_or_else(|| {
            anyhow!(
                "sandbox {} has not been started on any node yet",
                self.sandbox_id
            )
        })
    }

    /// Best-effort announces the node after it has acknowledged the sandbox.
    ///
    /// Failure is recoverable because the next heartbeat rebuilds the binding.
    async fn announce_placement(&self, node: &NodeEndpoint) {
        if let Err(error) = self
            .placement
            .record_placement(
                self.sandbox_id,
                self.execution_id,
                node,
                self.projection_ttl_secs,
            )
            .await
        {
            // The next heartbeat repairs this transient binding gap.
            warn!(
                sandbox_id = %self.sandbox_id,
                node_id = %node.node_id,
                execution_id = %self.execution_id,
                error = %error,
                "could not tell the cluster which machine this sandbox is on; it stays \
                 unroutable until the node's next heartbeat says so"
            );
        }
    }

    // Best-effort: a reservation that outlives this call expires on its own, and
    // one the node meanwhile confirmed names a runtime that really is there.
    /// Whether a node's answer to a create means "not here, not now" rather
    /// than something wrong with the request: unavailable (isolated, shutting
    /// down, or unreachable), busy with a conflicting state, or full.
    fn refuses_the_launch(status: &tonic::Status) -> bool {
        matches!(
            status.code(),
            tonic::Code::Unavailable
                | tonic::Code::FailedPrecondition
                | tonic::Code::ResourceExhausted
        )
    }

    /// Clears a copy of this sandbox the node still holds, then creates again.
    ///
    /// The node's copy is an orphan only when the deciding half's own record is
    /// absent or names this very launch. A record naming another incarnation
    /// means a newer launch owns the id, and this one touches nothing.
    async fn take_over_orphan(
        &self,
        node: &NodeEndpoint,
        client: &mut super::NodeClient,
        request: &pb::SandboxCreateRequest,
    ) -> Result<pb::SandboxCreateResponse> {
        let sandbox_id = self.sandbox_id;
        let recorded = self
            .record_owner
            .recorded_execution(sandbox_id)
            .await
            .with_context(|| {
                format!(
                    "read the record of sandbox {sandbox_id} after node {} answered that it \
                     already holds it",
                    node.node_id
                )
            })?;
        if let Some(recorded) = recorded {
            if recorded != self.execution_id {
                bail!(
                    "node {} already holds sandbox {sandbox_id}, and the record under that id \
                     is incarnation {recorded}, not this launch's {}",
                    node.node_id,
                    self.execution_id
                );
            }
        }

        let held = client
            .describe(pb::SandboxDescribeRequest {
                sandbox_id: sandbox_id.to_string(),
            })
            .await;
        let held_execution_id = match held {
            Ok(response) => response.into_inner().execution_id,
            // The copy went away between the create and this call.
            Err(status) if status.code() == tonic::Code::NotFound => String::new(),
            Err(status) => {
                return Err(wire::into_error(status)).with_context(|| {
                    format!(
                        "ask node {} which incarnation of sandbox {sandbox_id} it holds",
                        node.node_id
                    )
                })
            }
        };

        if !held_execution_id.is_empty() {
            warn!(
                %sandbox_id,
                node = %node.node_id,
                held_execution_id = %held_execution_id,
                execution_id = %self.execution_id,
                "the node holds a copy of this sandbox that nothing routes to; deleting it \
                 before starting this launch"
            );
            client
                .delete(pb::SandboxDeleteRequest {
                    sandbox_id: sandbox_id.to_string(),
                    execution_id: held_execution_id,
                })
                .await
                .map_err(wire::into_error)
                .with_context(|| {
                    format!(
                        "delete the orphaned copy of sandbox {sandbox_id} on node {}",
                        node.node_id
                    )
                })?;
        }

        client
            .create(request.clone())
            .await
            .map(|ack| ack.into_inner())
            .map_err(wire::into_error)
            .with_context(|| {
                format!(
                    "create sandbox {sandbox_id} on node {} after clearing its orphaned copy",
                    node.node_id
                )
            })
    }

    fn fork_child(
        result: pb::ForkChildResult,
        requested: &SandboxForkSpec,
        resources: SandboxResources,
        placement: &Arc<dyn NodePlacement>,
        node: &NodeEndpoint,
        client: &super::NodeClient,
    ) -> Result<RemoteSandboxStub> {
        if result.sandbox_id != requested.sandbox_id.to_string() {
            bail!(
                "node {} answered for sandbox {} where {} was asked for",
                node.node_id,
                result.sandbox_id,
                requested.sandbox_id
            );
        }
        match result.outcome {
            Some(pb::fork_child_result::Outcome::Started(ack)) => {
                let execution_id =
                    ExecutionId::parse_str(&ack.execution_id).with_context(|| {
                        format!("fork child {} returned no incarnation", result.sandbox_id)
                    })?;
                Ok(RemoteSandboxStub::already_running(
                    requested.sandbox_id,
                    execution_id,
                    resources,
                    Arc::clone(placement),
                    node.clone(),
                    client.clone(),
                    &ack,
                ))
            }
            Some(pb::fork_child_result::Outcome::Error(message)) => bail!(
                "node {} failed to fork {}: {message}",
                node.node_id,
                requested.sandbox_id
            ),
            None => bail!(
                "node {} returned neither an outcome nor an error for {}",
                node.node_id,
                requested.sandbox_id
            ),
        }
    }

    // Best-effort, as for a create: the node's next heartbeat repairs a gap.
    async fn announce_child_placement(
        placement: &Arc<dyn NodePlacement>,
        sandbox_id: SandboxId,
        execution_id: ExecutionId,
        node: &NodeEndpoint,
        projection_ttl_secs: u32,
    ) {
        if let Err(error) = placement
            .record_placement(sandbox_id, execution_id, node, projection_ttl_secs)
            .await
        {
            warn!(
                %sandbox_id,
                node_id = %node.node_id,
                %execution_id,
                error = %error,
                "could not tell the cluster which machine this fork child is on; it stays \
                 unroutable until the node's next heartbeat says so"
            );
        }
    }

    async fn withdraw_reservations(
        placement: &Arc<dyn NodePlacement>,
        reserved: &[(SandboxId, ExecutionId)],
    ) {
        for (sandbox_id, execution_id) in reserved {
            if let Err(error) = placement
                .release_placement_reservation(*sandbox_id, *execution_id)
                .await
            {
                warn!(
                    %sandbox_id,
                    %execution_id,
                    error = %error,
                    "could not withdraw the routing reservation of a fork child that never \
                     started; it expires on its own"
                );
            }
        }
    }

    async fn withdraw_reservation(&self) {
        if let Err(error) = self
            .placement
            .release_placement_reservation(self.sandbox_id, self.execution_id)
            .await
        {
            warn!(
                sandbox_id = %self.sandbox_id,
                execution_id = %self.execution_id,
                error = %error,
                "could not withdraw the routing reservation of a launch that failed; it expires \
                 on its own"
            );
        }
    }

    /// Connects with [`STUB_CONNECT_TIMEOUT`], which also bounds tonic reconnects.
    pub async fn connect(endpoint: &str) -> Result<super::NodeClient> {
        let channel = Endpoint::from_shared(endpoint.to_string())
            .with_context(|| format!("node endpoint {endpoint:?} is not a URI"))?
            .connect_timeout(STUB_CONNECT_TIMEOUT)
            .connect()
            .await
            .with_context(|| format!("connect to node service at {endpoint}"))?;
        Ok(super::client(channel))
    }

    /// Re-resolves the same node through discovery after its cached address fails.
    ///
    /// Sandbox facts survive because only the connection address changes.
    async fn reresolve_placement(&mut self) -> Result<()> {
        let sandbox_id = self.sandbox_id;
        let stale = self
            .placed
            .take()
            .ok_or_else(|| anyhow!("sandbox {sandbox_id} has not been started on any node yet"))?;
        let node_id = stale.node.node_id.clone();
        let node = self
            .placement
            .resolve_node(&node_id)
            .await
            .with_context(|| format!("re-resolve node {node_id} for sandbox {sandbox_id}"))?;
        let client = Self::connect(&node.endpoint)
            .await
            .with_context(|| format!("reconnect to node {node_id} for sandbox {sandbox_id}"))?;
        self.placed = Some(Placed {
            node,
            client,
            host_interaction_ip: stale.host_interaction_ip,
            rootfs_virtual_size: stale.rootfs_virtual_size,
            // Preserve sandbox facts across an address-only reconnect.
            resolved_image_facts: stale.resolved_image_facts,
        });
        Ok(())
    }

    /// Retries once, within [`STALE_PLACEMENT_RETRY_BUDGET`], only when no node
    /// response was received; node-returned statuses are never replayed.
    async fn call_with_stale_placement_retry<T, F>(
        &mut self,
        operation: &'static str,
        mut attempt: F,
    ) -> Result<tonic::Response<T>, tonic::Status>
    where
        F: for<'a> FnMut(
            &'a mut super::NodeClient,
        ) -> Pin<
            Box<dyn Future<Output = Result<tonic::Response<T>, tonic::Status>> + Send + 'a>,
        >,
    {
        let sandbox_id = self.sandbox_id;
        let stale_node_id = match self.placed.as_ref() {
            Some(placed) => placed.node.node_id.clone(),
            None => {
                return Err(tonic::Status::internal(format!(
                    "sandbox {sandbox_id} has not been started on any node yet"
                )))
            }
        };

        let first = {
            let client = &mut self.placed.as_mut().expect("checked above").client;
            attempt(client).await
        };
        let status = match first {
            Ok(response) => return Ok(response),
            Err(status) => status,
        };
        if !wire::is_unreachable(&status) {
            return Err(status);
        }

        // Bound re-resolution, reconnect, and the single retried call together.
        match tokio::time::timeout(
            STALE_PLACEMENT_RETRY_BUDGET,
            self.retry_after_reresolve(&mut attempt),
        )
        .await
        {
            Ok(Ok(response)) => {
                let new_endpoint = self
                    .placed
                    .as_ref()
                    .map(|placed| placed.node.endpoint.clone())
                    .unwrap_or_default();
                info!(
                    %sandbox_id,
                    node_id = %stale_node_id,
                    new_endpoint = %new_endpoint,
                    operation,
                    "a stale node address failed to connect; re-resolved the node's address \
                     and retried, and the retry succeeded"
                );
                Ok(response)
            }
            // A node-returned error is an answer and is not retried.
            Ok(Err(retry_error)) => {
                if let Some(retried_status) = retry_error.downcast_ref::<tonic::Status>() {
                    if wire::is_unreachable(retried_status) {
                        // The single retry also failed before reaching the node.
                        info!(
                            %sandbox_id,
                            node_id = %stale_node_id,
                            operation,
                            error = %retried_status,
                            "a stale node address failed to connect; re-resolved the node's \
                             address and retried, and the re-resolved address could not be \
                             reached either"
                        );
                    } else {
                        info!(
                            %sandbox_id,
                            node_id = %stale_node_id,
                            operation,
                            error = %retried_status,
                            "a stale node address failed to connect; re-resolved the node's \
                             address and retried, and the node answered"
                        );
                    }
                    Err(retried_status.clone())
                } else {
                    warn!(
                        %sandbox_id,
                        node_id = %stale_node_id,
                        operation,
                        error = %retry_error,
                        "a stale node address failed to connect, and re-resolving it failed too"
                    );
                    Err(status)
                }
            }
            Err(_elapsed) => {
                warn!(
                    %sandbox_id,
                    node_id = %stale_node_id,
                    operation,
                    budget_secs = STALE_PLACEMENT_RETRY_BUDGET.as_secs(),
                    "a stale node address failed to connect, and re-resolving it did not \
                     finish within budget"
                );
                Err(status)
            }
        }
    }

    /// Re-resolves and retries within the caller's timeout-bounded borrow.
    async fn retry_after_reresolve<T, F>(&mut self, attempt: &mut F) -> Result<tonic::Response<T>>
    where
        F: for<'a> FnMut(
            &'a mut super::NodeClient,
        ) -> Pin<
            Box<dyn Future<Output = Result<tonic::Response<T>, tonic::Status>> + Send + 'a>,
        >,
    {
        self.reresolve_placement().await?;
        let client = &mut self
            .placed
            .as_mut()
            .expect("reresolve_placement just set this")
            .client;
        attempt(client).await.map_err(anyhow::Error::new)
    }
}

/// Maximum node dial time, including tonic reconnects.
///
/// Keep below [`STALE_PLACEMENT_RETRY_BUDGET`] so re-resolution and the retried RPC
/// retain time within that budget.
pub const STUB_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

/// Maximum time for re-resolution, reconnect, and one retried RPC.
///
/// Keep above [`STUB_CONNECT_TIMEOUT`] so a full dial does not consume the budget.
pub const STALE_PLACEMENT_RETRY_BUDGET: Duration = Duration::from_secs(5);

/// Decodes image facts only when both context and image configs are present.
fn decode_resolved_image_facts(
    ack: &pb::SandboxCreateResponse,
) -> Result<Option<ResolvedImageFacts>> {
    let context: Option<crate::snapshot::CommandContext> =
        wire::serialized(ack.context.as_ref(), "resolved context")?;
    let image_configs: Option<crate::types::ImageConfigs> =
        wire::serialized(ack.image_configs.as_ref(), "resolved image configs")?;
    Ok(match (context, image_configs) {
        (Some(context), Some(image_configs)) => Some(ResolvedImageFacts {
            context,
            image_configs,
        }),
        _ => None,
    })
}

#[async_trait]
impl SandboxBackend for RemoteSandboxStub {
    fn execution_id(&self) -> ExecutionId {
        self.execution_id
    }

    async fn start(&mut self) -> Result<()> {
        if self.placed.is_some() {
            return Ok(());
        }
        let (request, preferred_node_id, needs) = match &self.pending {
            PendingLaunch::AlreadyStarted => return Ok(()),
            // Attach resolves an existing sandbox rather than starting one.
            PendingLaunch::Attach => return self.attach().await,
            PendingLaunch::Launch {
                request,
                preferred_node_id,
                needs,
            } => ((**request).clone(), preferred_node_id.clone(), *needs),
        };

        // A node that refuses the launch is excluded and placement is asked
        // again, so a preference the placement source is stale about (a node
        // just put into draining) costs one round trip, not the launch.
        let mut excluded_node_ids: Vec<String> = Vec::new();
        let mut last_refusal: Option<anyhow::Error> = None;
        let (node, mut client, ack) = loop {
            let node = self
                .placement
                .place_new_with(
                    self.sandbox_id,
                    self.resources,
                    preferred_node_id.as_deref(),
                    &excluded_node_ids,
                    needs,
                )
                .await
                .with_context(|| format!("choose a node for sandbox {}", self.sandbox_id))?;
            if excluded_node_ids.contains(&node.node_id) {
                // Placement has nowhere else; the refusal stands.
                return Err(last_refusal.expect("an excluded node was refused first"));
            }

            // Reserve before asking for the runtime, never after: a lookup that finds
            // no record of a sandbox is read as a verdict that it is gone, so a runtime
            // must not be able to exist before its record does.
            self.placement
                .reserve_placement(self.sandbox_id, self.execution_id, &node)
                .await
                .with_context(|| {
                    format!(
                        "reserve a routing record for sandbox {} on node {} before starting it",
                        self.sandbox_id, node.node_id
                    )
                })?;

            let (retryable, error) = match Self::connect(&node.endpoint).await {
                Ok(mut client) => match client.create(request.clone()).await {
                    Ok(ack) => break (node, client, ack.into_inner()),
                    // A node that already holds this id is either holding an
                    // orphan of this launch's or serving a newer one.
                    Err(status) if wire::is_sandbox_already_on_node(&status) => {
                        match self.take_over_orphan(&node, &mut client, &request).await {
                            Ok(ack) => break (node, client, ack),
                            Err(error) => {
                                self.withdraw_reservation().await;
                                return Err(error);
                            }
                        }
                    }
                    Err(status) => (
                        Self::refuses_the_launch(&status),
                        wire::into_error(status).context(format!(
                            "create sandbox {} on node {}",
                            self.sandbox_id, node.node_id
                        )),
                    ),
                },
                Err(error) => (true, error),
            };
            self.withdraw_reservation().await;
            if !retryable || excluded_node_ids.len() + 1 >= PLACEMENT_ATTEMPTS {
                return Err(error);
            }
            warn!(
                sandbox_id = %self.sandbox_id,
                node = %node.node_id,
                error = %format_args!("{error:#}"),
                "node refused the launch; placing the sandbox elsewhere"
            );
            excluded_node_ids.push(node.node_id);
            last_refusal = Some(error);
        };

        // The node must start the incarnation already recorded by the caller.
        if ack.execution_id != self.execution_id.to_string() {
            let _ = client
                .delete(pb::SandboxDeleteRequest {
                    sandbox_id: self.sandbox_id.to_string(),
                    execution_id: ack.execution_id.clone(),
                })
                .await;
            self.withdraw_reservation().await;
            bail!(
                "node {} started sandbox {} as execution {}, and this launch is execution {}",
                node.node_id,
                self.sandbox_id,
                ack.execution_id,
                self.execution_id
            );
        }

        // Announce only after the incarnation check that may tear down the create.
        self.announce_placement(&node).await;

        // Decoding is best-effort because the sandbox is already running.
        let resolved_image_facts = match decode_resolved_image_facts(&ack) {
            Ok(facts) => facts,
            Err(err) => {
                warn!(
                    sandbox_id = %self.sandbox_id,
                    node = %node.node_id,
                    error = %format_args!("{err:#}"),
                    "could not decode the node's resolved context/image configs; the sandbox is \
                     up, but the orchestrator's own record of it may show stale context until \
                     this sandbox is next paused and republished"
                );
                None
            }
        };

        self.placed = Some(Placed {
            node,
            client,
            host_interaction_ip: wire::host_ip(&ack.host_interaction_ip),
            rootfs_virtual_size: (ack.rootfs_virtual_size > 0).then_some(ack.rootfs_virtual_size),
            resolved_image_facts,
        });
        Ok(())
    }

    /// Starts and waits in the same remote round trip.
    async fn start_nowait(&mut self) -> Result<()> {
        self.start().await
    }

    async fn wait_for_ready(&self) -> Result<()> {
        self.placed().map(|_| ())
    }

    /// Pauses on the node, which stages the capture and forgets the sandbox.
    ///
    /// The node's answer is the staged snapshot; the api half commits it. A
    /// pause that stopped the VM without staging anything left nothing to
    /// resume, so it is reported as terminal.
    async fn pause(&mut self) -> SandboxCaptureResult<CapturedSandboxSnapshot> {
        let sandbox_id = self.sandbox_id;
        let execution_id = self.execution_id;
        let node_id = self
            .placed()
            .map_err(SandboxCaptureError::recoverable)?
            .node
            .node_id
            .clone();

        // Retry only when the pause never reached the node.
        let response = self
            .call_with_stale_placement_retry("pause", |client| {
                Box::pin(client.pause(pb::SandboxPauseRequest {
                    sandbox_id: sandbox_id.to_string(),
                    execution_id: execution_id.to_string(),
                }))
            })
            .await
            .map_err(wire::into_capture_error)?
            .into_inner();

        let staged_value = response.staged.and_then(|staged| staged.value);
        let staged: StagedSnapshot = wire::serialized(staged_value.as_ref(), "staged snapshot")
            .map_err(SandboxCaptureError::terminal)?
            .ok_or_else(|| {
                SandboxCaptureError::terminal(anyhow!(
                    "node {node_id} paused sandbox {sandbox_id} and staged nothing for it"
                ))
            })?;

        // `stop` runs next; the node has already stopped and forgotten the VM.
        self.paused = true;

        Ok(CapturedSandboxSnapshot::staged(staged))
    }

    /// A remote pause cannot be undone: the node stops the VM as part of it.
    async fn resume(&mut self) -> Result<()> {
        bail!(
            "sandbox {} cannot be resumed in place from here: the node that paused it has \
             already stopped it, and only a resume from its snapshot brings it back",
            self.sandbox_id
        )
    }

    async fn snapshot(&mut self) -> SandboxCaptureResult<CapturedSandboxSnapshot> {
        let sandbox_id = self.sandbox_id;
        let execution_id = self.execution_id;
        let node_id = self
            .placed()
            .map_err(SandboxCaptureError::recoverable)?
            .node
            .node_id
            .clone();

        // Retry only when the snapshot call never reached the node.
        let response = self
            .call_with_stale_placement_retry("snapshot", |client| {
                Box::pin(client.checkpoint(pb::SandboxCheckpointRequest {
                    sandbox_id: sandbox_id.to_string(),
                    execution_id: execution_id.to_string(),
                }))
            })
            .await
            .map_err(wire::into_capture_error)?
            .into_inner();

        let staged = response
            .staged
            .and_then(|staged| staged.value)
            .ok_or_else(|| {
                SandboxCaptureError::recoverable(anyhow!(
                    "node {node_id} checkpointed sandbox {sandbox_id} and returned nothing to \
                     commit"
                ))
            })?;
        let staged: crate::snapshot::repository::StagedSnapshot =
            wire::serialized(Some(&staged), "staged snapshot")
                .map_err(SandboxCaptureError::recoverable)?
                .ok_or_else(|| {
                    SandboxCaptureError::recoverable(anyhow!(
                        "node {node_id} returned an empty staged snapshot for {sandbox_id}"
                    ))
                })?;

        Ok(CapturedSandboxSnapshot::staged(staged))
    }

    async fn fork(
        &mut self,
        spec: &[SandboxForkSpec],
    ) -> SandboxCaptureResult<Vec<SandboxForkResult>> {
        let sandbox_id = self.sandbox_id;
        let execution_id = self.execution_id;
        let resources = self.resources;
        let projection_ttl_secs = self.projection_ttl_secs;
        let placement = Arc::clone(&self.placement);
        let node_before = self
            .placed()
            .map_err(SandboxCaptureError::recoverable)?
            .node
            .clone();

        // Children get routing records before the node is asked for them, as a
        // create does: a lookup that finds no record is read as a verdict that
        // the sandbox is gone. A retry that re-resolves the parent's node
        // confirms the children on the node it actually used, below.
        let mut reserved: Vec<(SandboxId, ExecutionId)> = Vec::new();
        for child in spec {
            if let Err(error) = placement
                .reserve_placement(child.sandbox_id, child.execution_id, &node_before)
                .await
            {
                Self::withdraw_reservations(&placement, &reserved).await;
                return Err(SandboxCaptureError::recoverable(error.context(format!(
                    "reserve a routing record for fork child {} before starting it",
                    child.sandbox_id
                ))));
            }
            reserved.push((child.sandbox_id, child.execution_id));
        }

        // Retry only when the fork call never reached the node.
        let response = match self
            .call_with_stale_placement_retry("fork", |client| {
                Box::pin(
                    client.fork(pb::SandboxForkRequest {
                        source_sandbox_id: sandbox_id.to_string(),
                        source_execution_id: execution_id.to_string(),
                        children: spec
                            .iter()
                            .map(|child| pb::ForkChildSpec {
                                sandbox_id: child.sandbox_id.to_string(),
                                // Run the child under the caller-minted incarnation.
                                execution_id: child.execution_id.to_string(),
                                // No child record exists yet from which to derive ownership metadata.
                                control_plane_config: Vec::new(),
                            })
                            .collect(),
                        timeout_ms: 0,
                    }),
                )
            })
            .await
        {
            Ok(response) => response.into_inner(),
            Err(status) => {
                Self::withdraw_reservations(&placement, &reserved).await;
                return Err(wire::into_capture_error(status));
            }
        };

        // A retry may have replaced the connection; use the current placement.
        let placed = self.placed().map_err(SandboxCaptureError::recoverable)?;
        let node = placed.node.clone();
        let client = placed.client.clone();

        // Results pair positionally with child specs.
        if response.children.len() != spec.len() {
            Self::withdraw_reservations(&placement, &reserved).await;
            return Err(SandboxCaptureError::terminal(anyhow!(
                "node {} answered a fork of {} children with {} results",
                node.node_id,
                spec.len(),
                response.children.len()
            )));
        }

        let mut children = Vec::with_capacity(spec.len());
        for (result, requested) in response.children.into_iter().zip(spec) {
            let child = Self::fork_child(result, requested, resources, &placement, &node, &client);
            match &child {
                Ok(child) => {
                    Self::announce_child_placement(
                        &placement,
                        requested.sandbox_id,
                        child.execution_id(),
                        &node,
                        projection_ttl_secs,
                    )
                    .await
                }
                Err(_) => {
                    Self::withdraw_reservations(
                        &placement,
                        &[(requested.sandbox_id, requested.execution_id)],
                    )
                    .await
                }
            }
            children.push(child.map(|child| Box::new(child) as Box<dyn SandboxBackend>));
        }
        Ok(children)
    }

    /// Deletes the remote sandbox unless pause already stopped it and left a capture.
    async fn stop(&mut self) -> Result<()> {
        let sandbox_id = self.sandbox_id;
        let execution_id = self.execution_id;
        if self.paused {
            return Ok(());
        }
        let Some(placed) = self.placed.as_ref() else {
            // This process never placed the sandbox, so no teardown is needed.
            return Ok(());
        };
        let node_id = placed.node.node_id.clone();

        // Retry only when delete never reached the node.
        match self
            .call_with_stale_placement_retry("stop", |client| {
                Box::pin(client.delete(pb::SandboxDeleteRequest {
                    sandbox_id: sandbox_id.to_string(),
                    execution_id: execution_id.to_string(),
                }))
            })
            .await
        {
            Ok(_) => Ok(()),
            // Node-confirmed absence makes stop idempotent.
            Err(status) if status.code() == tonic::Code::NotFound => Ok(()),
            // Unreachability cannot be treated as absence.
            Err(status) => Err(wire::into_error(status))
                .with_context(|| format!("stop sandbox {sandbox_id} on node {node_id}")),
        }
    }

    /// Asks the node whether it is still running this incarnation.
    ///
    /// A node that answers for another incarnation is not running this one.
    async fn is_still_running(&mut self) -> Result<bool> {
        let placed = self.placed()?;
        let sandbox_id = self.sandbox_id;
        let node_id = placed.node.node_id.clone();
        let mut client = placed.client.clone();
        match client
            .describe(pb::SandboxDescribeRequest {
                sandbox_id: sandbox_id.to_string(),
            })
            .await
        {
            Ok(response) => Ok(response.into_inner().execution_id == self.execution_id.to_string()),
            Err(status) if status.code() == tonic::Code::NotFound => Ok(false),
            Err(status) => Err(wire::into_error(status)).with_context(|| {
                format!("ask node {node_id} whether it is still running sandbox {sandbox_id}")
            }),
        }
    }

    fn host_interaction_ip(&self) -> Option<Ipv4Addr> {
        self.placed
            .as_ref()
            .and_then(|placed| placed.host_interaction_ip)
    }

    fn runtime_info(&self) -> SandboxRuntimeInfo {
        SandboxRuntimeInfo {
            rootfs_virtual_size: self
                .placed
                .as_ref()
                .and_then(|placed| placed.rootfs_virtual_size),
            runtime_artifacts: RuntimeArtifactSet::empty(),
            resolved_image_facts: self
                .placed
                .as_ref()
                .and_then(|placed| placed.resolved_image_facts.clone()),
        }
    }

    fn startup_artifacts(&self) -> RuntimeArtifactSet {
        RuntimeArtifactSet::empty()
    }

    /// Returns the node from the established placement, or `None` before placement.
    fn holding_node_id(&self) -> Option<&str> {
        self.placed
            .as_ref()
            .map(|placed| placed.node.node_id.as_str())
    }

    fn set_projection_budget(&mut self, projection_ttl_secs: u32) {
        self.projection_ttl_secs = projection_ttl_secs;
    }

    async fn update_network_policy(&mut self, policy: Option<SandboxNetworkPolicy>) -> Result<()> {
        let sandbox_id = self.sandbox_id;
        let execution_id = self.execution_id;
        let encoded = policy
            .map(|policy| wire::serialize(&policy, "network policy"))
            .transpose()?;
        self.placed()?;

        // Retry only when the update never reached the node.
        self.call_with_stale_placement_retry("update_network_policy", |client| {
            Box::pin(client.update_network(pb::SandboxNetworkRequest {
                sandbox_id: sandbox_id.to_string(),
                execution_id: execution_id.to_string(),
                network_policy: encoded.clone(),
            }))
        })
        .await
        .map_err(wire::into_error)?;
        Ok(())
    }

    /// Awaits the remote update so failure reaches the caller.
    async fn update_custom_extension_params(
        &mut self,
        params: Option<CustomExtensionParams>,
    ) -> Result<()> {
        let sandbox_id = self.sandbox_id;
        let execution_id = self.execution_id;
        let encoded = params
            .map(|params| wire::serialize(&params, "custom extension params"))
            .transpose()?;
        self.placed()?;

        // Retry only when the update never reached the node.
        self.call_with_stale_placement_retry("update_custom_extension_params", |client| {
            Box::pin(client.update_params(pb::SandboxParamsRequest {
                sandbox_id: sandbox_id.to_string(),
                execution_id: execution_id.to_string(),
                custom_extension_params: encoded.clone(),
            }))
        })
        .await
        .map_err(wire::into_error)?;
        Ok(())
    }
}
