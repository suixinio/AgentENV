//! One sandbox, driven on another machine.
//!
//! # 🔴 What a stub returns in place of each in-process handle
//!
//! Three of [`SandboxBackend`]'s return types cannot cross a process boundary,
//! and the substitution is different for each:
//!
//! | in process | over the wire | why |
//! |---|---|---|
//! | `PausedSandboxCapture.state` — a live object a resume reopens | [`RemotePausedState`] — the node's own encoding plus the path and the machine it is on | the encoding already exists: `PausedSandboxState::encode` is what the node writes to its own disk |
//! | `PausedSandboxCapture.publishable` / `CapturedSandboxSnapshot` — a value keeping a temporary directory alive until publication finishes | a staged snapshot: the bytes are already durable on the node, and what comes back is the row that has not been announced yet | there is nothing left to keep alive by the time the reply is written |
//! | `RuntimeArtifactSet` — the local overlaybd configs a running sandbox has open | empty | it is the input to image-liveness, which keeps *local* layers from being reclaimed. The deciding half has none. That is a fact about it, not a gap |
//!
//! # 🔴 Nothing here reads an unreachable node as an absent sandbox
//!
//! A timeout, a refused connection or a transport error is an error. The
//! temptation is strongest on `stop`, where "the node did not answer" and "the
//! sandbox is gone" both end with nothing to do — and taking the first for the
//! second is how a cluster stops accounting for a VM that is still running.

use std::net::Ipv4Addr;

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use tonic::transport::{Channel, Endpoint};
use tracing::error;

use crate::proto::node as pb;
use crate::proto::node::node_sandbox_service_client::NodeSandboxServiceClient;
use crate::sandbox::{
    CapturedSandboxSnapshot, CustomExtensionParams, PausedSandboxCapture, RuntimeArtifactSet,
    SandboxBackend, SandboxCaptureError, SandboxCaptureResult, SandboxForkResult, SandboxForkSpec,
    SandboxNetworkPolicy, SandboxRuntimeInfo,
};
use crate::types::{ExecutionId, SandboxId, SandboxResources};

use super::paused_state::RemotePausedState;
use super::placement::{NodeEndpoint, NodePlacement};
use super::wire;

use std::sync::Arc;

/// What a stub was told to start, kept until `start` is called.
///
/// 🔴 `SandboxBackendFactory::build*` is synchronous and `start` is not, so
/// nothing may be done here that touches a network. Choosing a node and asking
/// it to create the sandbox both happen in `start`, and that is the property
/// that lets a remote factory satisfy a trait written for a local one.
pub(super) enum PendingLaunch {
    FromSnapshot {
        request: Box<pb::SandboxCreateRequest>,
    },
    /// A child of a fork that has already happened: the node started it, so
    /// there is nothing left to launch.
    AlreadyStarted,
}

/// One sandbox on another node.
pub struct RemoteSandboxStub {
    sandbox_id: SandboxId,
    execution_id: ExecutionId,
    resources: SandboxResources,
    placement: Arc<dyn NodePlacement>,
    pending: PendingLaunch,
    /// Set once the sandbox is running somewhere.
    placed: Option<Placed>,
}

struct Placed {
    node: NodeEndpoint,
    client: NodeSandboxServiceClient<Channel>,
    host_interaction_ip: Option<Ipv4Addr>,
    rootfs_virtual_size: Option<u64>,
}

impl RemoteSandboxStub {
    pub(super) fn pending(
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
        }
    }

    /// A stub for a sandbox that is already running on a known node.
    pub(super) fn already_running(
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
            placed: Some(Placed {
                node,
                client,
                host_interaction_ip: wire::host_ip(&ack.host_interaction_ip),
                rootfs_virtual_size: (ack.rootfs_virtual_size > 0)
                    .then_some(ack.rootfs_virtual_size),
            }),
        }
    }

    fn placed(&self) -> Result<&Placed> {
        self.placed.as_ref().ok_or_else(|| {
            anyhow!(
                "sandbox {} has not been started on any node yet",
                self.sandbox_id
            )
        })
    }

    fn placed_mut(&mut self) -> Result<&mut Placed> {
        let sandbox_id = self.sandbox_id;
        self.placed
            .as_mut()
            .ok_or_else(|| anyhow!("sandbox {sandbox_id} has not been started on any node yet"))
    }

    pub(super) async fn connect(endpoint: &str) -> Result<NodeSandboxServiceClient<Channel>> {
        let channel = Endpoint::from_shared(endpoint.to_string())
            .with_context(|| format!("node endpoint {endpoint:?} is not a URI"))?
            .connect()
            .await
            .with_context(|| format!("connect to node service at {endpoint}"))?;
        Ok(NodeSandboxServiceClient::new(channel))
    }
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
            PendingLaunch::FromSnapshot { request } => (**request).clone(),
        };

        let node = self
            .placement
            .place_new(self.sandbox_id, self.resources)
            .await
            .with_context(|| format!("choose a node for sandbox {}", self.sandbox_id))?;
        let mut client = Self::connect(&node.endpoint).await?;

        let ack = client
            .create(request)
            .await
            .map_err(wire::into_error)
            .with_context(|| {
                format!(
                    "create sandbox {} on node {}",
                    self.sandbox_id, node.node_id
                )
            })?
            .into_inner();

        // 🔴 The node has to be running the incarnation this backend was built
        // for. The caller has already written that value into its own record of
        // the sandbox, so adopting a different one here would leave two records
        // of one sandbox naming two different runs — and fencing compares
        // exactly that value.
        if ack.execution_id != self.execution_id.to_string() {
            let _ = client
                .delete(pb::SandboxDeleteRequest {
                    sandbox_id: self.sandbox_id.to_string(),
                    execution_id: ack.execution_id.clone(),
                })
                .await;
            bail!(
                "node {} started sandbox {} as execution {}, and this launch is execution {}",
                node.node_id,
                self.sandbox_id,
                ack.execution_id,
                self.execution_id
            );
        }

        self.placed = Some(Placed {
            node,
            client,
            host_interaction_ip: wire::host_ip(&ack.host_interaction_ip),
            rootfs_virtual_size: (ack.rootfs_virtual_size > 0).then_some(ack.rootfs_virtual_size),
        });
        Ok(())
    }

    /// The same call as [`start`](Self::start).
    ///
    /// 🔴 The local split into "start" and "wait for ready" exists so a caller
    /// can overlap boot with other work in the same process. Over a wire there
    /// is one round trip either way, and a second RPC that only waited would
    /// need its own timeout, its own retry story and its own answer for what
    /// happens when nobody asks — for no gain.
    async fn start_nowait(&mut self) -> Result<()> {
        self.start().await
    }

    /// Already true by the time `start` returned.
    async fn wait_for_ready(&self) -> Result<()> {
        self.placed().map(|_| ())
    }

    async fn pause(
        &mut self,
        _artifact_root: Option<&std::path::Path>,
    ) -> SandboxCaptureResult<PausedSandboxCapture> {
        // 🔴 The directory is not ours to choose. It has to be on the disk the
        // bytes are written to, which is the node's, and the node allocates it
        // with its own persister. A path from here would name a directory on a
        // machine that is not doing the writing.
        let sandbox_id = self.sandbox_id;
        let execution_id = self.execution_id;
        let placed = self
            .placed_mut()
            .map_err(SandboxCaptureError::recoverable)?;
        let node_id = placed.node.node_id.clone();

        let response = placed
            .client
            .pause(pb::SandboxPauseRequest {
                sandbox_id: sandbox_id.to_string(),
                execution_id: execution_id.to_string(),
                publish: true,
            })
            .await
            .map_err(wire::into_capture_error)?
            .into_inner();

        let paused = response.paused_state.ok_or_else(|| {
            // 🔴 Terminal. The node reported a successful pause, which means it
            // has already stopped the VM; a reply without the state to reopen
            // it is a sandbox that is down and cannot be brought back, and
            // treating that as recoverable would have the caller mark it
            // running again.
            SandboxCaptureError::terminal(anyhow!(
                "node {node_id} paused sandbox {sandbox_id} and returned no paused state"
            ))
        })?;
        let state = wire::serialized_value(paused.state.as_ref(), "paused state")
            .map_err(SandboxCaptureError::terminal)?
            .unwrap_or(serde_json::Value::Null);

        Ok(PausedSandboxCapture {
            state: Arc::new(RemotePausedState::new(node_id, paused.artifact_root, state)),
            // 🔴 Always `None`, and it is not a gap. `publishable` exists so
            // that a caller holding live capture artifacts can hand them to a
            // repository before they are reclaimed. By the time this reply
            // exists the node has already written the bytes and staged the row,
            // and what the caller needs is the staged row — which travels in
            // the reply rather than inside a handle that owns a directory on
            // somebody else's disk.
            publishable: None,
        })
    }

    /// Never reachable from the deciding half, and an error rather than a
    /// silent success.
    ///
    /// 🔴 `resume` means "reopen the sandbox this backend is still holding, in
    /// place". Its one caller is the rollback after a pause failed to persist,
    /// and the deciding half persists nothing — it hands the record to a
    /// cluster store instead — so that rollback cannot run here. Answering `Ok`
    /// would tell a caller a VM had been brought back when nothing was asked of
    /// anyone.
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
        let placed = self
            .placed_mut()
            .map_err(SandboxCaptureError::recoverable)?;
        let node_id = placed.node.node_id.clone();

        let response = placed
            .client
            .checkpoint(pb::SandboxCheckpointRequest {
                sandbox_id: sandbox_id.to_string(),
                execution_id: execution_id.to_string(),
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

        Ok(CapturedSandboxSnapshot::new(staged))
    }

    async fn fork(
        &mut self,
        spec: &[SandboxForkSpec],
    ) -> SandboxCaptureResult<Vec<SandboxForkResult>> {
        let sandbox_id = self.sandbox_id;
        let execution_id = self.execution_id;
        let resources = self.resources;
        let placement = Arc::clone(&self.placement);
        let placed = self
            .placed_mut()
            .map_err(SandboxCaptureError::recoverable)?;
        let node = placed.node.clone();

        let response = placed
            .client
            .fork(pb::SandboxForkRequest {
                source_sandbox_id: sandbox_id.to_string(),
                source_execution_id: execution_id.to_string(),
                children: spec
                    .iter()
                    .map(|child| pb::ForkChildSpec {
                        sandbox_id: child.sandbox_id.to_string(),
                        // 🔴 The orchestrator above this backend has already
                        // minted the child's incarnation and is about to write
                        // it into the child's record, so the node has to run
                        // under it rather than choose its own.
                        execution_id: child.execution_id.to_string(),
                        // 🔴 A fork child reaches the node unmarked, and that
                        // is a known gap rather than a decision.
                        //
                        // A create is stamped by
                        // `Orchestrator::stamp_control_plane_ownership`,
                        // which works because the sandbox's record exists —
                        // as a launch plan — before the backend is built. A
                        // fork child's record does not: it is a clone of the
                        // parent's, taken *after* the node has answered, so
                        // there is nothing to encode at this point.
                        // `ForkChildAssignment` already has the field for it,
                        // and filling it belongs to the surface that decides
                        // what a child's record is.
                        //
                        // The direction this fails in is the safe one: an
                        // unmarked child is a sandbox no control plane claims,
                        // so reconciliation leaves it alone rather than tearing
                        // it down. What it costs is that a fork child started
                        // by the API half is absent from `ListSandboxes`, and
                        // would be leaked rather than reclaimed if the control
                        // plane's record of it were lost.
                        control_plane_config: Vec::new(),
                    })
                    .collect(),
                timeout_ms: 0,
            })
            .await
            .map_err(wire::into_capture_error)?
            .into_inner();

        // 🔴 One result per spec, in order, or the whole fork is a failure.
        // The caller pairs these positionally with the children it asked for
        // and writes a record for each; a short or reordered list would give
        // some child another child's record.
        if response.children.len() != spec.len() {
            return Err(SandboxCaptureError::terminal(anyhow!(
                "node {} answered a fork of {} children with {} results",
                node.node_id,
                spec.len(),
                response.children.len()
            )));
        }

        let client = placed.client.clone();
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

    async fn stop(&mut self) -> Result<()> {
        let sandbox_id = self.sandbox_id;
        let execution_id = self.execution_id;
        let Some(placed) = self.placed.as_mut() else {
            // Never started anywhere, so there is nothing to tear down. This is
            // the *only* branch that treats "no sandbox" as success, and it is
            // safe because it is decided from this process's own state rather
            // than from a node's answer.
            return Ok(());
        };

        match placed
            .client
            .delete(pb::SandboxDeleteRequest {
                sandbox_id: sandbox_id.to_string(),
                execution_id: execution_id.to_string(),
            })
            .await
        {
            Ok(_) => Ok(()),
            // 🔴 The node says it does not have this sandbox. That is an
            // answer, and it is the one that makes a stop idempotent — which
            // the trait requires.
            Err(status) if status.code() == tonic::Code::NotFound => Ok(()),
            // 🔴 Everything else, including a node that did not answer at all.
            // A stop that reported success because the node was unreachable
            // would take the cluster's last record of a running VM with it.
            Err(status) => Err(wire::into_error(status)).with_context(|| {
                format!("stop sandbox {sandbox_id} on node {}", placed.node.node_id)
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
            // See the table at the top of this file.
            runtime_artifacts: RuntimeArtifactSet::empty(),
        }
    }

    fn startup_artifacts(&self) -> RuntimeArtifactSet {
        RuntimeArtifactSet::empty()
    }

    async fn update_network_policy(&mut self, policy: Option<SandboxNetworkPolicy>) -> Result<()> {
        let sandbox_id = self.sandbox_id;
        let execution_id = self.execution_id;
        let encoded = policy
            .map(|policy| wire::serialize(&policy, "network policy"))
            .transpose()?;
        let placed = self.placed_mut()?;

        placed
            .client
            .update_network(pb::SandboxNetworkRequest {
                sandbox_id: sandbox_id.to_string(),
                execution_id: execution_id.to_string(),
                network_policy: encoded,
            })
            .await
            .map_err(wire::into_error)?;
        Ok(())
    }

    /// 🔴 The one method on this trait that cannot be honoured over a wire, and
    /// it is worth being explicit about rather than quietly approximating.
    ///
    /// Locally this is a plain assignment and cannot fail, which is why the
    /// signature has no error to return. Remotely it is a round trip that can
    /// time out, and there is nowhere to say so: the caller has already told
    /// the custom extension that the new value is in force. The value is
    /// therefore sent in the background and a failure is logged loudly, and the
    /// real fix is for the surface above to have a fallible assignment.
    fn update_custom_extension_params(&mut self, params: Option<CustomExtensionParams>) {
        let sandbox_id = self.sandbox_id;
        let execution_id = self.execution_id;
        let Some(placed) = self.placed.as_mut() else {
            error!(
                %sandbox_id,
                "custom extension params were assigned to a sandbox that is not running anywhere"
            );
            return;
        };
        let node_id = placed.node.node_id.clone();
        let mut client = placed.client.clone();
        let encoded = params
            .map(|params| wire::serialize(&params, "custom extension params"))
            .transpose();

        tokio::spawn(async move {
            let encoded = match encoded {
                Ok(encoded) => encoded,
                Err(err) => {
                    error!(%sandbox_id, error = %err, "failed to encode custom extension params");
                    return;
                }
            };
            if let Err(status) = client
                .update_params(pb::SandboxParamsRequest {
                    sandbox_id: sandbox_id.to_string(),
                    execution_id: execution_id.to_string(),
                    custom_extension_params: encoded,
                })
                .await
            {
                error!(
                    %sandbox_id,
                    %node_id,
                    error = %status,
                    "custom extension params were accepted here and never reached the node"
                );
            }
        });
    }
}
