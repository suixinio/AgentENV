//! Drives one sandbox on another node.
//!
//! Transport failure never means the sandbox is absent. Remote pause captures carry
//! their origin node and incarnation; resume must reopen that pinned capture there.
//! Runtime artifacts are local-only and therefore empty on the deciding half.

use std::future::Future;
use std::net::Ipv4Addr;
use std::pin::Pin;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use tonic::transport::{Channel, Endpoint};
use tracing::{debug, info, warn};

use crate::proto::node as pb;
use crate::proto::node::node_sandbox_service_client::NodeSandboxServiceClient;
use crate::sandbox::{
    CapturedSandboxSnapshot, CustomExtensionParams, PausedSandboxCapture, ResolvedImageFacts,
    RuntimeArtifactSet, RuntimeConfirmedGone, SandboxBackend, SandboxCaptureError,
    SandboxCaptureResult, SandboxForkResult, SandboxForkSpec, SandboxNetworkPolicy,
    SandboxRuntimeInfo,
};
use crate::types::{ExecutionId, SandboxId, SandboxResources};

use super::paused_state::RemotePausedState;
use super::placement::{NodeEndpoint, NodeMembership, NodePlacement};
use super::wire::{self, RemoteResumeFailure};

use std::sync::Arc;

/// A launch deferred until asynchronous `start`; construction performs no network I/O.
pub enum PendingLaunch {
    /// A create request ready to send.
    Launch {
        request: Box<pb::SandboxCreateRequest>,
    },
    /// Reopens a paused sandbox on the node holding its capture.
    Resume {
        request: Box<pb::SandboxResumeRequest>,
        origin_node_id: String,
    },
    /// A fork child the node already started.
    AlreadyStarted,
    /// Attaches to a sandbox already running on an as-yet unresolved node.
    Attach,
}

/// One sandbox on another node.
pub struct RemoteSandboxStub {
    sandbox_id: SandboxId,
    execution_id: ExecutionId,
    resources: SandboxResources,
    placement: Arc<dyn NodePlacement>,
    pending: PendingLaunch,
    placed: Option<Placed>,
    /// Suppresses `Delete` after pause because the node's pause already stopped the VM
    /// and `Delete` would destroy its only capture.
    paused: bool,
}

struct Placed {
    node: NodeEndpoint,
    client: NodeSandboxServiceClient<Channel>,
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
            placed: None,
            paused: false,
        }
    }

    pub fn already_running(
        sandbox_id: SandboxId,
        execution_id: ExecutionId,
        resources: SandboxResources,
        placement: Arc<dyn NodePlacement>,
        node: NodeEndpoint,
        client: NodeSandboxServiceClient<Channel>,
        ack: &pb::SandboxCreateResponse,
    ) -> Self {
        Self {
            sandbox_id,
            execution_id,
            resources,
            placement,
            pending: PendingLaunch::AlreadyStarted,
            paused: false,
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
        client: &mut NodeSandboxServiceClient<Channel>,
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

    /// Reopens a local capture only on its recorded origin node.
    ///
    /// Missing placement falls back to resolving that origin node; any different holder
    /// is rejected. Cluster resume claims, store CAS, and execution IDs provide fencing.
    async fn reopen(
        &mut self,
        request: pb::SandboxResumeRequest,
        origin_node_id: String,
    ) -> Result<()> {
        let sandbox_id = self.sandbox_id;
        let placed = match self.placement.place_existing(sandbox_id).await {
            Ok(placed) => placed,
            // A scheduler that answered about this sandbox and refused has told us
            // the holder cannot serve it; one that could not answer has not.
            Err(err) if crate::node_client::wire::placement_gave_a_verdict(&err) => {
                return Err(anyhow::Error::new(RemoteResumeFailure::origin_unavailable(
                    &origin_node_id,
                    sandbox_id,
                    format!("{err:#}"),
                )))
            }
            Err(err) => {
                return Err(err.context(format!("locate the machine holding sandbox {sandbox_id}")))
            }
        };
        let node = match placed {
            Some(node) => {
                if node.node_id != origin_node_id {
                    return Err(anyhow::Error::new(RemoteResumeFailure::origin_unavailable(
                        &origin_node_id,
                        sandbox_id,
                        format!(
                            "placement chose node {}: reopening a capture happens on the machine \
                             holding it, and rebuilding this sandbox somewhere else is a create \
                             from a published snapshot rather than this call",
                            node.node_id
                        ),
                    )));
                }
                node
            }
            None => {
                debug!(
                    %sandbox_id,
                    %origin_node_id,
                    "the placement source has no record of this sandbox; reopening its capture on \
                     the machine the capture names"
                );
                let node = self
                    .placement
                    .resolve_node(&origin_node_id)
                    .await
                    .map_err(|err| {
                        anyhow::Error::new(RemoteResumeFailure::origin_unavailable(
                            &origin_node_id,
                            sandbox_id,
                            format!("its address could not be found: {err:#}"),
                        ))
                    })?;
                // Recheck the fallback result before dialing the pinned origin.
                if node.node_id != origin_node_id {
                    return Err(anyhow::Error::new(RemoteResumeFailure::origin_unavailable(
                        &origin_node_id,
                        sandbox_id,
                        format!("the placement source answered with node {}", node.node_id),
                    )));
                }
                node
            }
        };

        let mut client = Self::connect(&node.endpoint).await.map_err(|err| {
            anyhow::Error::new(RemoteResumeFailure::unreachable(
                &node.node_id,
                sandbox_id,
                format!("{err:#}"),
            ))
        })?;

        let response = client
            .resume(request)
            .await
            .map_err(|status| {
                anyhow::Error::new(RemoteResumeFailure::from_status(
                    &node.node_id,
                    sandbox_id,
                    status,
                ))
            })?
            .into_inner();

        // A successful resume must identify the run now executing.
        let started = response.started.ok_or_else(|| {
            anyhow!(
                "node {} reopened sandbox {sandbox_id} and said nothing about the run it started",
                node.node_id
            )
        })?;

        // Never delete the only capture after a resume incarnation mismatch.
        if started.execution_id != self.execution_id.to_string() {
            bail!(
                "node {} reopened sandbox {sandbox_id} as execution {}, and this resume claimed \
                 execution {}",
                node.node_id,
                started.execution_id,
                self.execution_id
            );
        }

        // Resume resources come from the node's record.
        if let Some(resources) = started.resources.as_ref() {
            self.resources = SandboxResources {
                cpu_count: resources.cpu_count,
                memory_mib: resources.memory_mib,
                disk_size_mib: resources.disk_size_mib,
            };
        }
        // Announce resume because this process did not retain the old live binding.
        self.announce_placement(&node).await;

        self.placed = Some(Placed {
            node,
            client,
            host_interaction_ip: wire::host_ip(&started.host_interaction_ip),
            rootfs_virtual_size: (started.rootfs_virtual_size > 0)
                .then_some(started.rootfs_virtual_size),
            resolved_image_facts: None,
        });
        Ok(())
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
            .record_placement(self.sandbox_id, self.execution_id, node)
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
    pub async fn connect(endpoint: &str) -> Result<NodeSandboxServiceClient<Channel>> {
        let channel = Endpoint::from_shared(endpoint.to_string())
            .with_context(|| format!("node endpoint {endpoint:?} is not a URI"))?
            .connect_timeout(STUB_CONNECT_TIMEOUT)
            .connect()
            .await
            .with_context(|| format!("connect to node service at {endpoint}"))?;
        Ok(NodeSandboxServiceClient::new(channel))
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
            &'a mut NodeSandboxServiceClient<Channel>,
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
            &'a mut NodeSandboxServiceClient<Channel>,
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
        let request = match &self.pending {
            PendingLaunch::AlreadyStarted => return Ok(()),
            // Attach resolves an existing sandbox rather than starting one.
            PendingLaunch::Attach => return self.attach().await,
            PendingLaunch::Launch { request } => (**request).clone(),
            PendingLaunch::Resume {
                request,
                origin_node_id,
            } => {
                let request = (**request).clone();
                let origin_node_id = origin_node_id.clone();
                return self.reopen(request, origin_node_id).await;
            }
        };

        let node = self
            .placement
            .place_new(self.sandbox_id, self.resources)
            .await
            .with_context(|| format!("choose a node for sandbox {}", self.sandbox_id))?;

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

        let mut client = match Self::connect(&node.endpoint).await {
            Ok(client) => client,
            Err(error) => {
                self.withdraw_reservation().await;
                return Err(error);
            }
        };

        let ack = match client.create(request).await {
            Ok(ack) => ack.into_inner(),
            Err(status) => {
                self.withdraw_reservation().await;
                return Err(wire::into_error(status)).with_context(|| {
                    format!(
                        "create sandbox {} on node {}",
                        self.sandbox_id, node.node_id
                    )
                });
            }
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

    async fn pause(
        &mut self,
        _artifact_root: Option<&std::path::Path>,
        committer_waiting: bool,
    ) -> SandboxCaptureResult<PausedSandboxCapture> {
        // The node chooses the capture path on the disk that stores the bytes.
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
                    // Stage durable bytes only when a publisher will commit them.
                    publish: committer_waiting,
                }))
            })
            .await
            .map_err(wire::into_capture_error)?
            .into_inner();

        let paused = response.paused_state.ok_or_else(|| {
            // Missing state after a successful pause makes the sandbox unrecoverable.
            SandboxCaptureError::terminal(anyhow!(
                "node {node_id} paused sandbox {sandbox_id} and returned no paused state"
            ))
        })?;
        let state = wire::serialized_value(paused.state.as_ref(), "paused state")
            .map_err(SandboxCaptureError::terminal)?
            .unwrap_or(serde_json::Value::Null);

        // Mark the local stub paused even if publication metadata cannot be decoded.
        if !response.staging_error.is_empty() {
            // The local capture remains usable when shared staging fails.
            warn!(
                %sandbox_id,
                node_id,
                error = %response.staging_error,
                "a paused sandbox could not be staged for publication; it is resumable only on \
                 the node that holds it"
            );
        }
        let staged_value = response.staged.and_then(|staged| staged.value);
        if !committer_waiting && staged_value.is_some() {
            // Unexpected unannounced bytes have no reader; surface the leak.
            warn!(
                %sandbox_id,
                node_id,
                "node staged a snapshot for a pause that did not ask to publish; its bytes are \
                 durable there and nothing will announce them"
            );
        }
        let publishable = match staged_value.filter(|_| committer_waiting) {
            Some(value) => {
                let staged: crate::snapshot::repository::StagedSnapshot =
                    wire::serialized(Some(&value), "staged snapshot")
                        .map_err(SandboxCaptureError::recoverable)?
                        .ok_or_else(|| {
                            SandboxCaptureError::recoverable(anyhow!(
                                "node {node_id} returned an empty staged snapshot for {sandbox_id}"
                            ))
                        })?;
                Some(CapturedSandboxSnapshot::staged(staged))
            }
            // Idempotent pause may have no new staged capture.
            None => None,
        };

        // `stop` runs next and must preserve this capture.
        self.paused = true;

        Ok(PausedSandboxCapture {
            // Fence the capture with the incarnation this stub paused.
            state: Arc::new(RemotePausedState::new(
                node_id,
                paused.artifact_root,
                execution_id,
                state,
            )),
            // The node already staged the bytes; publication commits this row directly.
            publishable,
        })
    }

    /// Rejects in-place resume because this remote backend owns no local persistence.
    async fn resume(&mut self) -> Result<()> {
        bail!(
            "sandbox {} cannot be resumed in place from here: this backend drives a sandbox on \
             another machine, and reopening a paused capture happens on the machine holding it",
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
        let placement = Arc::clone(&self.placement);
        self.placed().map_err(SandboxCaptureError::recoverable)?;

        // Retry only when the fork call never reached the node.
        let response = self
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
            .map_err(wire::into_capture_error)?
            .into_inner();

        // A retry may have replaced the connection; use the current placement.
        let placed = self.placed().map_err(SandboxCaptureError::recoverable)?;
        let node = placed.node.clone();
        let client = placed.client.clone();

        // Results pair positionally with child specs.
        if response.children.len() != spec.len() {
            return Err(SandboxCaptureError::terminal(anyhow!(
                "node {} answered a fork of {} children with {} results",
                node.node_id,
                spec.len(),
                response.children.len()
            )));
        }

        Ok(response
            .children
            .into_iter()
            .zip(spec)
            .map(|(result, requested)| {
                if result.sandbox_id != requested.sandbox_id.to_string() {
                    return Err(anyhow!(
                        "node {} answered for sandbox {} where {} was asked for",
                        node.node_id,
                        result.sandbox_id,
                        requested.sandbox_id
                    ));
                }
                match result.outcome {
                    Some(pb::fork_child_result::Outcome::Started(ack)) => {
                        let execution_id =
                            ExecutionId::parse_str(&ack.execution_id).with_context(|| {
                                format!("fork child {} returned no incarnation", result.sandbox_id)
                            })?;
                        Ok(Box::new(RemoteSandboxStub::already_running(
                            requested.sandbox_id,
                            execution_id,
                            resources,
                            Arc::clone(&placement),
                            node.clone(),
                            client.clone(),
                            &ack,
                        )) as Box<dyn SandboxBackend>)
                    }
                    Some(pb::fork_child_result::Outcome::Error(message)) => Err(anyhow!(
                        "node {} failed to fork {}: {message}",
                        node.node_id,
                        requested.sandbox_id
                    )),
                    None => Err(anyhow!(
                        "node {} returned neither an outcome nor an error for {}",
                        node.node_id,
                        requested.sandbox_id
                    )),
                }
            })
            .collect())
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
