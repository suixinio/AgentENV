//! One sandbox, driven on another machine.
//!
//! # 🔴 What a stub returns in place of each in-process handle
//!
//! Three of [`SandboxBackend`]'s return types cannot cross a process boundary,
//! and the substitution is different for each:
//!
//! | in process | over the wire | why |
//! |---|---|---|
//! | `PausedSandboxCapture.state` — a live object a resume reopens | [`RemotePausedState`] — the node's own encoding plus the path, the machine it is on, and the run it captured | the encoding already exists: `PausedSandboxState::encode` is what the node writes to its own disk. What this half adds is what makes the capture *addressable*: which machine to ask, and which of its captures to ask for |
//! | `PausedSandboxCapture.publishable` / `CapturedSandboxSnapshot` — a value keeping a temporary directory alive until publication finishes | a staged snapshot: the bytes are already durable on the node, and what comes back is the row that has not been announced yet | there is nothing left to keep alive by the time the reply is written |
//! | `RuntimeArtifactSet` — the local overlaybd configs a running sandbox has open | empty | it is the input to image-liveness, which keeps *local* layers from being reclaimed. The deciding half has none. That is a fact about it, not a gap |
//!
//! # 🔴 Nothing here reads an unreachable node as an absent sandbox
//!
//! A timeout, a refused connection or a transport error is an error. The
//! temptation is strongest on `stop`, where "the node did not answer" and "the
//! sandbox is gone" both end with nothing to do — and taking the first for the
//! second is how a cluster stops accounting for a VM that is still running.

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
    RuntimeArtifactSet, SandboxBackend, SandboxCaptureError, SandboxCaptureResult,
    SandboxForkResult, SandboxForkSpec, SandboxNetworkPolicy, SandboxRuntimeInfo,
};
use crate::types::{ExecutionId, SandboxId, SandboxResources};

use super::paused_state::RemotePausedState;
use super::placement::{NodeEndpoint, NodePlacement};
use super::wire::{self, RemoteResumeFailure};

use std::sync::Arc;

/// What a stub was told to start, kept until `start` is called.
///
/// 🔴 `SandboxBackendFactory::build*` is synchronous and `start` is not, so
/// nothing may be done here that touches a network. Choosing a node and asking
/// it to create the sandbox both happen in `start`, and that is the property
/// that lets a remote factory satisfy a trait written for a local one.
pub(super) enum PendingLaunch {
    /// A `Create` request built and ready to send — from a `Source::Snapshot`
    /// or a `Source::Image`, whichever `SandboxBackendFactory::build_from_snapshot`
    /// or `build_from_image_ref` built it as. `start` treats the two exactly
    /// alike: neither carries anything this process needs to interpret before
    /// sending it.
    Launch {
        request: Box<pb::SandboxCreateRequest>,
    },
    /// A paused sandbox to be reopened on the machine that holds its capture.
    ///
    /// 🔴 The origin node is kept beside the request rather than inside it. The
    /// node does not need telling which machine it is; this half needs it to
    /// decide whether the machine placement named is the one that can answer at
    /// all.
    Resume {
        request: Box<pb::SandboxResumeRequest>,
        origin_node_id: String,
    },
    /// A child of a fork that has already happened: the node started it, so
    /// there is nothing left to launch.
    AlreadyStarted,
    /// A sandbox that is already running on some machine, reached by a process
    /// that never started it and so was never handed a reply naming one.
    ///
    /// 🔴 Distinct from [`AlreadyStarted`](Self::AlreadyStarted), which carries
    /// the reply: that variant knows the machine, the address and the size,
    /// because the call that produced it said so. This one knows only the
    /// sandbox, and has to go and ask. Collapsing the two would mean a stub
    /// that quietly reports "not placed anywhere" for a sandbox that is up.
    Attach,
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
    /// Set once this sandbox's capture has been taken and the node has stopped
    /// the VM.
    ///
    /// # 🔴 What `stop` means changes when this is set, and getting it wrong
    /// destroys the user's sandbox
    ///
    /// Locally, `pause` and `stop` are two different things done to one
    /// machine: the capture is written to disk, and then the VM process is torn
    /// down and the capture stays. Over a wire there is no call that means
    /// "tear the VM down and keep everything" — the node already did that
    /// inside its own pause — and the only teardown this service has is
    /// `Delete`, which takes the paused record and its artifacts with it.
    ///
    /// `Orchestrator::pause_sandbox` calls `stop` on the backend immediately
    /// after a successful pause, "to free up resources". Sent as a `Delete`,
    /// that erases the capture the pause has just promised the user, moments
    /// after the pause reported success — and the sandbox then comes back as
    /// `NotFound` from the one machine that had it, which reads exactly like
    /// "the only copy is gone".
    paused: bool,
}

struct Placed {
    node: NodeEndpoint,
    client: NodeSandboxServiceClient<Channel>,
    host_interaction_ip: Option<Ipv4Addr>,
    rootfs_virtual_size: Option<u64>,
    /// Set only by [`start`](RemoteSandboxStub::start)'s image-source arm,
    /// decoded from the node's `Create` reply: this process built the request
    /// from an [`UnresolvedImageBuildSpec`][crate::sandbox::UnresolvedImageBuildSpec]
    /// and so had no context or image configs of its own to put in the
    /// orchestrator's transitional record. `None` everywhere else — a
    /// snapshot-source create, an attach, and a resume all start from a
    /// record that already carries the right values, and re-deriving them
    /// here would be a second copy to keep in step. See
    /// [`SandboxRuntimeInfo::resolved_image_facts`].
    resolved_image_facts: Option<ResolvedImageFacts>,
}

/// What a machine reports about a sandbox it is running.
///
/// 🔴 Both fields `None` is an answer here and not an absence: it is what a
/// machine that is running nothing under this id reports. See
/// [`RemoteSandboxStub::live_facts`].
#[derive(Default)]
struct LiveFacts {
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
            paused: false,
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
            paused: false,
            placed: Some(Placed {
                node,
                client,
                host_interaction_ip: wire::host_ip(&ack.host_interaction_ip),
                rootfs_virtual_size: (ack.rootfs_virtual_size > 0)
                    .then_some(ack.rootfs_virtual_size),
                // This constructor is for a fork child: the caller already
                // has the parent's context and image configs locally, so
                // there is nothing here for the orchestrator to patch.
                resolved_image_facts: None,
            }),
        }
    }

    /// A stub for a sandbox that is already running somewhere, built by a
    /// process that did not start it.
    ///
    /// # 🔴 This is what a replicated deciding half has instead of a handle
    ///
    /// `Orchestrator` keeps its backends in a process-local map, which is the
    /// whole truth when there is one process. `--role api` runs several behind
    /// a load balancer with no session affinity, so the replica a call lands on
    /// is *not* the replica that started the sandbox — and the one that did not
    /// start it has an empty map and a perfectly good record. Without this it
    /// reads that pair as "the sandbox is gone".
    ///
    /// 🔴 It is placed nowhere until [`start`](SandboxBackend::start) is
    /// called, exactly like every other stub in this file, and for the same
    /// reason: the factory method that produces it is synchronous and may not
    /// touch a network. `start` on this variant starts nothing — it finds the
    /// machine the sandbox is already on and opens a channel to it.
    pub(super) fn attaching(
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

    /// Finds the machine a sandbox that is already running is on, and connects.
    ///
    /// 🔴 `place_existing`, for the reason written on [`reopen`](Self::reopen):
    /// this asks *where this sandbox is*, and `place_new` answers *which
    /// machine has room*. A teardown sent on the second answer would be sent to
    /// a machine that has never seen the sandbox, which comes back `NotFound` —
    /// and a caller that read that as "already gone" would leave a VM running
    /// with nothing accounting for it.
    ///
    /// 🔴 Nothing here checks an incarnation, and nothing needs to: every call
    /// this stub goes on to make carries `execution_id` as its fence, and the
    /// node refuses a call that names a run it is not running. Checking here as
    /// well would be a second answer to the same question, taken a round trip
    /// earlier.
    ///
    /// 🔴 And it asks the machine what it is running, rather than placing the
    /// stub with the two fields only a live handle knows left blank. See
    /// [`live_facts`](Self::live_facts) for why a blank was the wrong shape of
    /// silence.
    async fn attach(&mut self) -> Result<()> {
        let sandbox_id = self.sandbox_id;
        let node = self
            .placement
            .place_existing(sandbox_id)
            .await
            .with_context(|| format!("locate the machine running sandbox {sandbox_id}"))?
            // 🔴 An error, and there is no fallback here — unlike
            // [`reopen`](Self::reopen), which has one. The difference is what
            // the two calls hold: a resume is built from a paused state that
            // names the machine its bytes are on, so "the cluster has no record
            // of this sandbox" still leaves exactly one machine it could be
            // talking about. An attach holds a sandbox id and nothing else, and
            // the record it came from carries no node. Picking a machine from
            // that would be guessing which host a running VM is on, and the two
            // ways of guessing wrong are a teardown sent to a stranger and a
            // teardown that never reaches the VM at all.
            .ok_or_else(|| {
                anyhow!(
                    "the placement source has no record of sandbox {sandbox_id}, so there is no \
                     machine to drive it on: a create records the assignment as soon as the node \
                     acknowledges it, and the node's heartbeat roster re-seeds it every interval, \
                     so this is a retry rather than a verdict"
                )
            })?;
        let mut client = Self::connect(&node.endpoint)
            .await
            .with_context(|| format!("reach node {} for sandbox {sandbox_id}", node.node_id))?;

        // 🔴 Before the stub is placed, not after. `placed` is what every other
        // method on this type reads its address out of, and a stub that were
        // placed first and enriched second would have a window — and, if this
        // failed, a permanent state — where it is placed and answers `None` to
        // a question it never asked.
        let facts = Self::live_facts(&mut client, &node.node_id, sandbox_id).await?;

        self.placed = Some(Placed {
            node,
            client,
            host_interaction_ip: facts.host_interaction_ip,
            rootfs_virtual_size: facts.rootfs_virtual_size,
            // Attaching to a sandbox this process never started: whatever
            // record it has is the one the orchestrator already trusts, and
            // this call learns nothing new about it.
            resolved_image_facts: None,
        });
        Ok(())
    }

    /// The live facts for a sandbox this process did not start, read off the
    /// machine that is running it.
    ///
    /// # 🔴 Asked for, because there is no reply lying around that has them
    ///
    /// Every other way a stub becomes [`Placed`] is a reply to a call that
    /// started something: a create's ack, a resume's `started`, a fork child's
    /// ack. Each carries the address the sandbox reaches the host on and the
    /// size its rootfs turned out to be, because only the live handle on the
    /// node knows either. An attach has no such call — it finds a sandbox that
    /// was already up — so it asks, and that keeps the invariant this type
    /// depends on: **a `Placed` is only ever built out of an answer from the
    /// machine.**
    ///
    /// This used to be left as `None`, on the reasoning that guessing an
    /// address is worse than admitting to none. That is true and it is not the
    /// whole choice: `None` is *also* what a node reports for a sandbox it has
    /// no address for, and that value is a loud refusal —
    /// `proxy_target_from_sandbox` turns it into "sandbox missing host
    /// interaction IP after start" and the caller tears the sandbox down. So
    /// the blank did not read as "nobody asked"; it read as a verdict about a
    /// healthy sandbox, waiting for the first caller to consult it.
    ///
    /// # 🔴 Three answers, kept apart
    ///
    /// * the facts — the node looked at its live handle and reported them;
    /// * `NOT_FOUND` — the node looked and is running nothing under this id.
    ///   There is no address because there is no sandbox there, and that is a
    ///   fact about the machine rather than a gap in this reply. It is **not**
    ///   an error: the caller above may well be a delete, whose whole job is
    ///   to reconcile a record with a machine that no longer has the sandbox,
    ///   and failing here would leave such a record undeletable.
    /// * anything else, including no answer at all — the node could not be
    ///   asked. That is an error, for the reason written at the top of this
    ///   file: an unreachable machine is not an absent sandbox.
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

        // 🔴 The node answered from its record because the sandbox's handle was
        // mid-operation, and its record holds neither of these fields. Taking
        // the blanks would be the thing this whole method exists to stop, one
        // round trip further along: a sandbox that is up, described as having
        // no address. The sandbox is not going anywhere and a retry costs one
        // call, so this says so instead.
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

    /// Asks the machine holding this sandbox's capture to reopen it.
    ///
    /// # 🔴 `place_existing`, never `place_new`
    ///
    /// A create asks *which machine has room*; this asks *where this sandbox
    /// may go*, and the answers are not interchangeable. A resume sent to a
    /// machine chosen for its free capacity lands somewhere the bytes are not.
    ///
    /// # 🔴 And the answer is checked against the machine the capture is on
    ///
    /// Placement can legitimately name another node — that is what it does for
    /// a paused sandbox whose capture was published to shared storage, which
    /// any machine can rebuild. This call cannot do that: it reopens a local
    /// capture and nothing else. So a placement that named a different machine
    /// is refused here rather than sent, because sending it produces a
    /// `NotFound` from a node that has simply never seen the sandbox — an
    /// answer that reads exactly like "the only copy is gone".
    ///
    /// # 🔴 An absent record is not an answer, and is not treated as one
    ///
    /// The check above accepts exactly one node — `origin_node_id`, which this
    /// call was *handed* — and refuses every other. It follows that a placement
    /// source with no record of the sandbox cannot be answering the question:
    /// there is nothing for it to disagree with. It used to fail the resume
    /// anyway, and on a cluster that meant a sandbox created and paused inside
    /// one heartbeat interval could not be woken until a heartbeat had listed
    /// it — the placement source had never been told the sandbox existed, and
    /// this half asked it to confirm a value it was already holding.
    ///
    /// So `Ok(None)` falls back to resolving `origin_node_id`'s address and
    /// reopening there. **Why that cannot wake a sandbox in two places:**
    ///
    /// 1. It never reaches a machine this call would otherwise have refused.
    ///    The set of acceptable nodes is one node wide either way, and the
    ///    fallback produces that same node. Nothing about *which* machine is
    ///    decided differently — only whether the call happens at all.
    /// 2. It is entered on absence and on nothing else. Every answer naming
    ///    another holder — a binding, a heartbeat roster, or a registry row in
    ///    `running`/`resuming` — is an answer, and is still refused above.
    ///    Every outcome where the source could not be consulted is still an
    ///    error: `place_existing` maps `NOT_FOUND` alone to `Ok(None)`, and the
    ///    scheduler reaches `NOT_FOUND` only after the bindings, the rosters
    ///    and the paused registry have each been read and each held no row.
    /// 3. It is not where the fence lives, and could not be. This comparison is
    ///    against a value the caller already holds, so it can never detect a
    ///    second waker on the *same* machine — which is the only shape a double
    ///    wake of a pinned capture can take. What actually prevents one is, in
    ///    order: the paused registry's `claim_for_resume`, which grants one
    ///    resume at a time across the cluster and mints the incarnation this
    ///    call carries; the store's `Paused -> Resuming` compare-and-set; and
    ///    the node's own check that `execution_id` names a capture it still
    ///    holds — a capture that has already been reopened is gone, and the
    ///    node answers `CaptureAbsent` rather than starting a second copy.
    ///
    /// The resolved address is checked against `origin_node_id` again before it
    /// is dialled, so a placement source that answered about some other machine
    /// is refused rather than followed.
    ///
    /// # 🔴 Which `origin_node_id` this is
    ///
    /// There is more than one value by that name in this system and they do not
    /// all come from the same place. This one is
    /// [`RemotePausedState::origin_node_id`], written by [`pause`](Self::pause)
    /// from `Placed::node.node_id` — the machine this stub was driving when it
    /// took the capture, as the placement source named it at the time. It is
    /// *not* the paused registry row's `origin_node_id`, and it is not any
    /// process's reading of its own identity: nothing here consults
    /// `crate::identity`, `NodeIdentity`, or the API half's own node id. So the
    /// fallback below names the machine that actually ran the VM, which is the
    /// machine whose disk the bytes are on, whatever any other record of that
    /// question happens to say.
    async fn reopen(
        &mut self,
        request: pb::SandboxResumeRequest,
        origin_node_id: String,
    ) -> Result<()> {
        let sandbox_id = self.sandbox_id;
        let placed = self
            .placement
            .place_existing(sandbox_id)
            .await
            .with_context(|| format!("locate the machine holding sandbox {sandbox_id}"))?;
        let node = match placed {
            Some(node) => {
                if node.node_id != origin_node_id {
                    bail!(
                        "sandbox {sandbox_id}'s capture is on node {origin_node_id} and placement \
                         chose node {}: reopening a capture happens on the machine holding it, \
                         and rebuilding this sandbox somewhere else is a create from a published \
                         snapshot rather than this call",
                        node.node_id
                    );
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
                    .with_context(|| {
                        format!(
                            "find the address of node {origin_node_id}, which holds sandbox \
                             {sandbox_id}'s capture"
                        )
                    })?;
                // 🔴 The same equality the answered branch enforces, applied to
                // the fallback's answer. Without it the guarantee above would
                // rest on every `NodePlacement` implementation being careful,
                // rather than on this call refusing anything that is not the
                // one machine it is allowed to talk to.
                if node.node_id != origin_node_id {
                    bail!(
                        "sandbox {sandbox_id}'s capture is on node {origin_node_id} and the \
                         placement source answered with node {}",
                        node.node_id
                    );
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

        // 🔴 An empty reply is a failure and not an empty answer. The node said
        // the resume succeeded, which means a VM is up over there; a reply that
        // does not say which run it is running leaves this half unable to fence
        // anything it sends next.
        let started = response.started.ok_or_else(|| {
            anyhow!(
                "node {} reopened sandbox {sandbox_id} and said nothing about the run it started",
                node.node_id
            )
        })?;

        // 🔴 The node has to be running the incarnation the resume claim
        // allocated, and — unlike a create — a mismatch is *not* followed by a
        // teardown. A create that went wrong can be undone because the sandbox
        // did not exist before the call; this sandbox did, its capture has just
        // been consumed by whatever the node started, and deleting it over a
        // protocol disagreement would destroy the user's only copy of their
        // work. So it fails loudly and leaves the sandbox where it is.
        if started.execution_id != self.execution_id.to_string() {
            bail!(
                "node {} reopened sandbox {sandbox_id} as execution {}, and this resume claimed \
                 execution {}",
                node.node_id,
                started.execution_id,
                self.execution_id
            );
        }

        // 🔴 Learned from the reply rather than carried in. A resume stub is
        // built from a paused state, which says what the sandbox *was*, not
        // what it is worth: `SandboxBackendFactory::build_from_paused_state` is
        // handed no resources. The node's record has them, and this is where
        // they arrive.
        if let Some(resources) = started.resources.as_ref() {
            self.resources = SandboxResources {
                cpu_count: resources.cpu_count,
                memory_mib: resources.memory_mib,
                disk_size_mib: resources.disk_size_mib,
            };
        }
        // 🔴 A resume needs this as much as a create does, and for a reason a
        // create does not have: a pause takes the sandbox off the node's
        // *running* set but leaves it in the node's record store, so the
        // heartbeat roster goes on carrying it and the binding survives. What
        // does not survive is a binding that was never written — and the
        // sandbox this call is waking is, by construction, one whose handle
        // this process no longer had.
        self.announce_placement(&node).await;

        self.placed = Some(Placed {
            node,
            client,
            host_interaction_ip: wire::host_ip(&started.host_interaction_ip),
            rootfs_virtual_size: (started.rootfs_virtual_size > 0)
                .then_some(started.rootfs_virtual_size),
            // A resume reopens the record the pause left behind, which
            // already carries the sandbox's context and image configs.
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

    /// Tells the placement source which machine this sandbox ended up on.
    ///
    /// # 🔴 Logged and swallowed, and that is the whole contract
    ///
    /// By the time this runs the node has acknowledged the sandbox: it is up
    /// over there whatever the placement source says next. Failing the
    /// operation here would tear down a working sandbox to keep a cache
    /// honest, and answering the user an error for a sandbox that exists is
    /// worse than the window this write exists to close.
    ///
    /// # 🔴 Why the write exists at all
    ///
    /// Nothing in this process used to make it. The gateway did, by reading the
    /// node off the create it had just routed — and it stopped being able to
    /// the day user-facing REST began going to the API half, which is the
    /// deployment this whole module is for. Its own comment says so and names
    /// this half as the owner of the write it gave up
    /// (`services/gateway/internal/rest_upstream.go`).
    ///
    /// What that cost, measured on a cluster: for the interval between a create
    /// and the node's next heartbeat, the cluster could not say where the new
    /// sandbox was. A replica still holding the handle never noticed, because
    /// it never asks. The moment the handle went — a pause — every call that
    /// has to ask failed: a delete 0.2 seconds after a pause answered 500 and
    /// the same delete a minute later answered 204, and a resume did the same.
    async fn announce_placement(&self, node: &NodeEndpoint) {
        if let Err(error) = self
            .placement
            .record_placement(self.sandbox_id, self.execution_id, node)
            .await
        {
            // 🔴 `warn`, not `error`: this is recoverable without anyone doing
            // anything. The node's next heartbeat roster carries the sandbox
            // and re-seeds the binding, so what was lost is one heartbeat
            // interval of routability rather than the sandbox.
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

    /// # 🔴 Why this dial carries an explicit `connect_timeout`
    ///
    /// `tonic::transport::Endpoint::connect_timeout` defaults to `None`
    /// (`tonic::transport::channel::Endpoint::new`), and with no timeout set
    /// `Endpoint::http_connector` passes `None` straight into
    /// `hyper_util::client::legacy::connect::HttpConnector::set_connect_timeout`.
    /// With that unset, `hyper-util`'s connect future is the bare
    /// `TcpSocket::connect(addr).await` — no `tokio::time::timeout` wrapper of
    /// any kind (`hyper-util`'s `connect()` free function: `Some(dur) =>
    /// timeout(dur, connect).await`, `None => connect.await`). So an
    /// unbounded dial is not idle — it is a real `connect(2)` syscall left to
    /// the kernel, and its ceiling is `net.ipv4.tcp_syn_retries` (Linux
    /// default 6, an exponential-backoff SYN retransmit schedule capped
    /// around two minutes). A node whose pod has been deleted — gone from the
    /// cluster's routing entirely rather than merely refusing the
    /// connection — leaves every SYN unanswered, and this is what a cluster
    /// incident measured at ~71 seconds before this fix: not a queue, not a
    /// gRPC-level setting, the kernel's own SYN retry clock. `Endpoint`'s
    /// `get_connect_timeout` and the error text this failure carries
    /// (`"tcp connect error"`, from `ConnectError::m` in `hyper-util`'s
    /// `connect()`) both match the production log line this fixes.
    ///
    /// `node_client::build::build_template_on_a_node` already carries this
    /// exact lesson as `CONNECT_TIMEOUT` (`src/node_client/build.rs`) for the
    /// one call in this crate that dials with its own fresh `Endpoint`
    /// outside this function. This is the shared dial every other remote
    /// call in this module goes through — `start`, `attach`, `reopen`, and
    /// `reresolve_placement` (used by [`call_with_stale_placement_retry`]'s
    /// post-re-resolve reconnect) — and, less obviously, also what a
    /// `Channel` built here goes back through on its own: tonic's
    /// `Reconnect` middleware
    /// (`tonic::transport::channel::service::reconnect::Reconnect`) redials
    /// with the very same connector — timeout and all — whenever a call
    /// finds the connection this channel had open is gone. That is exactly
    /// [`call_with_stale_placement_retry`]'s *first*, unretried `attempt`:
    /// the node died out from under an already-`Placed` stub's open
    /// connection, and the reconnect that call silently triggers was, until
    /// this fix, the unbounded dial above — paid in full before the bounded
    /// re-resolve-and-retry path this function's caller wraps in
    /// [`STALE_PLACEMENT_RETRY_BUDGET`] ever got a turn. See that constant's
    /// doc for how the two are sized together.
    pub(super) async fn connect(endpoint: &str) -> Result<NodeSandboxServiceClient<Channel>> {
        let channel = Endpoint::from_shared(endpoint.to_string())
            .with_context(|| format!("node endpoint {endpoint:?} is not a URI"))?
            .connect_timeout(STUB_CONNECT_TIMEOUT)
            .connect()
            .await
            .with_context(|| format!("connect to node service at {endpoint}"))?;
        Ok(NodeSandboxServiceClient::new(channel))
    }

    /// Drops a `Placed` that a call just found completely unreachable, and
    /// rebuilds one against a freshly resolved address for the *same* node.
    ///
    /// # 🔴 `resolve_node`, never `place_existing`
    ///
    /// The stale address a call just failed against came from whichever
    /// lookup produced this stub's current `Placed` — for the case this
    /// exists to fix, that is the scheduler's per-sandbox binding cache,
    /// answered by `LookupNode`/`place_existing`. That cache is refreshed
    /// only by `ReconcileNode`, which a node's own heartbeat can be skipped
    /// past for the exact window a rolling restart opens (see the module-level
    /// incident this fixes). Asking `place_existing` again would consult the
    /// same stale cache and could easily get the same stale answer back.
    ///
    /// `resolve_node` asks a different question — *where is the node whose
    /// identity I already hold* — and on the cluster placement source that
    /// question goes to `GetNode`, which reads node discovery directly rather
    /// than the binding cache. This stub already knows which node it is
    /// entitled to talk to (it got there via a `Placed` built from an earlier,
    /// trusted placement decision); what is stale is only the address, and
    /// `resolve_node` is the call built for exactly that gap — see its doc on
    /// [`NodePlacement::resolve_node`].
    ///
    /// # 🔴 The old facts survive
    ///
    /// `host_interaction_ip` and `rootfs_virtual_size` are properties of the
    /// sandbox as the node last reported them, not of the TCP connection that
    /// happened to be open when it did. Losing them here would turn a
    /// reconnect into a regression for any caller that reads them later.
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
            // Carried over rather than dropped: a re-resolve is a reconnect,
            // not a new placement, and whatever this stub had already learned
            // stays true of the same sandbox.
            resolved_image_facts: stale.resolved_image_facts,
        });
        Ok(())
    }

    /// Runs one RPC against this stub's current placement, and if it never
    /// reached the node at all, re-resolves the node's address once and tries
    /// exactly once more.
    ///
    /// # 🔴 Never on an answer, only on silence
    ///
    /// `attempt` is retried only when its failure satisfies
    /// [`wire::is_unreachable`] — a status this process manufactured because
    /// the call never produced a response, never a status the node sent. A
    /// node that answered `NotFound`, `FailedPrecondition`, or anything else
    /// of its own has been asked exactly once by the time this returns, the
    /// same as before this retry existed. Replaying such an answer against a
    /// freshly resolved address would not be a retry of a failed connection;
    /// it would be a second, uninvited delivery of a request the node has
    /// already ruled on.
    ///
    /// # 🔴 One retry, and it is bounded
    ///
    /// At most one re-resolve-and-reconnect happens (`retried` below never
    /// lets a second one start). The re-resolve, the reconnect and the retried
    /// call together are bounded by [`STALE_PLACEMENT_RETRY_BUDGET`], so a
    /// node that is genuinely gone costs this call one extra, short round trip
    /// rather than turning a fast failure into a slow one. The *first*
    /// attempt is not part of that budget — it is the caller's ordinary RPC,
    /// unretried, exactly as it behaved before this existed.
    ///
    /// `attempt` is handed a fresh `&mut NodeSandboxServiceClient<Channel>`
    /// each time it runs — never the same client object it saw before a
    /// retry — because a retry that kept the old client would still be
    /// dialling the connection this whole method exists to stop using.
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

        // Bounded so that a node which is genuinely gone costs this call one
        // extra, short round trip rather than turning a fast failure into a
        // slow one. `retry_after_reresolve` is an ordinary method call — its
        // future naturally reborrows `self` for its own duration and releases
        // it as soon as this `.await` resolves, whether by success, an
        // answer, or the timeout below, so `self` is free to use again in
        // every arm.
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
            // The retried attempt itself ran and the node answered — even an
            // answer that is itself an error is not this method's to retry
            // again; see the doc above.
            Ok(Err(retry_error)) => {
                if let Some(retried_status) = retry_error.downcast_ref::<tonic::Status>() {
                    if wire::is_unreachable(retried_status) {
                        // The freshly resolved address could not be reached
                        // either. This is still exactly one retry — the
                        // *node* has not answered at all, on either address —
                        // and it is surfaced rather than tried a third time.
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

    /// The re-resolve-and-retry half of
    /// [`call_with_stale_placement_retry`](Self::call_with_stale_placement_retry),
    /// split out so that half can be wrapped in a timeout without fighting the
    /// borrow checker over holding `self` across a manually written `async`
    /// block: an ordinary method call's future reborrows `self` for exactly
    /// its own duration, which is what lets the caller use `self` again in
    /// every arm of the `match` on this method's result.
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

/// How long [`RemoteSandboxStub::connect`] may spend dialing before it gives
/// up on that address.
///
/// # 🔴 Sized for an interactive call, not a build
///
/// This governs every ordinary remote call this module makes — `create`,
/// `pause`, `snapshot`, `fork`, `stop`, the network/params updates — and,
/// through tonic's `Reconnect` middleware, every silent redial an
/// already-open `Channel` performs when the connection under it has died.
/// The one longer-lived call in this crate,
/// `node_client::build::build_template_on_a_node`, dials with its own
/// `Endpoint` and its own ten-*minute* `CONNECT_TIMEOUT` for a reason that
/// does not apply here — see that constant's doc — so it is deliberately
/// not reused.
///
/// A TCP handshake to a node that is actually up completes in single-digit
/// milliseconds on a cluster network: the kernel answers a SYN before the
/// answer ever reaches the node's own workload, so this value being short
/// does not risk mistaking a busy node for a dead one. What it is short
/// *against* is [`Endpoint::connect_timeout`]'s default of `None` — an
/// unbounded dial, gated only by the kernel's own SYN-retry ceiling
/// (`net.ipv4.tcp_syn_retries`, roughly two minutes at Linux's default of 6)
/// — which is what turned a node whose pod had already been deleted into a
/// ~71-second wait before this fix. See [`RemoteSandboxStub::connect`]'s doc
/// for the mechanism this closes.
///
/// # 🔴 Paired with [`STALE_PLACEMENT_RETRY_BUDGET`], not chosen alone
///
/// [`call_with_stale_placement_retry`](RemoteSandboxStub::call_with_stale_placement_retry)'s
/// *first* attempt — the caller's ordinary RPC against a stub's existing
/// `Placed` connection — is not wrapped in `STALE_PLACEMENT_RETRY_BUDGET` at
/// all; this constant is the only bound on it, by way of the `Channel`'s own
/// reconnect. So the worst-case wall time a caller can see from a genuinely
/// unreachable node is approximately
/// `STUB_CONNECT_TIMEOUT + STALE_PLACEMENT_RETRY_BUDGET` — currently
/// `3s + 5s = 8s` — and every increase to either constant widens that sum
/// directly. `STUB_CONNECT_TIMEOUT` is kept smaller than
/// `STALE_PLACEMENT_RETRY_BUDGET`, and by more than a rounding margin: the
/// budget's own reconnect (inside `reresolve_placement`) dials through this
/// same timeout too, and it still has to leave room, inside that one budget,
/// for the `resolve_node` RPC and the retried call that follow the reconnect
/// in the same window. A `STUB_CONNECT_TIMEOUT` at or above the budget would
/// leave that reconnect free to consume the entire budget by itself.
pub(super) const STUB_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

/// How long a re-resolve-and-reconnect retry after a stale node address may
/// take before the original failure is surfaced instead.
///
/// 🔴 Deliberately short: this budget exists so that a node which is
/// genuinely gone costs one bounded extra round trip rather than turning a
/// fast failure into a slow one. It covers the re-resolve RPC, the reconnect,
/// and the retried call together — not the first attempt, which is the
/// caller's ordinary, unretried RPC.
///
/// # 🔴 Kept greater than [`STUB_CONNECT_TIMEOUT`], with headroom
///
/// The reconnect inside this budget dials through
/// [`RemoteSandboxStub::connect`], which is itself bounded by
/// `STUB_CONNECT_TIMEOUT`. This budget has to stay comfortably larger than
/// that: a worst-case reconnect can burn the full `STUB_CONNECT_TIMEOUT`
/// before the `resolve_node` RPC and the retried call even start, and both
/// still have to finish inside what is left of this budget. Sized equal to
/// or smaller than `STUB_CONNECT_TIMEOUT`, this budget would degenerate into
/// "however long the reconnect alone takes," with nothing left over for
/// either of those. See `STUB_CONNECT_TIMEOUT`'s doc for the sum the two
/// constants together bound.
pub(super) const STALE_PLACEMENT_RETRY_BUDGET: Duration = Duration::from_secs(5);

/// Decodes a `Create` reply's resolved context and image configs, when the
/// node sent both.
///
/// 🔴 `Ok(None)` covers an older node that predates these fields as well as
/// one that, for whatever reason, sent only one of the two — both are read
/// the same way a missing field is read everywhere else on this service: as
/// "nothing to patch", not as a partial answer to trust half of.
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
            // 🔴 Nothing is started here. The sandbox is up; what this call
            // does is find out where.
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

        // 🔴 After the incarnation check above and not before it. The check can
        // still tear this sandbox down, and a binding written for a sandbox
        // that is about to be deleted would point the cluster at a VM that is
        // going away.
        self.announce_placement(&node).await;

        // 🔴 Best-effort, like `host_ip` just above: the sandbox is already up
        // on the node by this point, and failing the whole create over a
        // decode problem in this one reply field would trade a working
        // sandbox for none. See `resolved_image_facts` on `Placed`.
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
        committer_waiting: bool,
    ) -> SandboxCaptureResult<PausedSandboxCapture> {
        // 🔴 The directory is not ours to choose. It has to be on the disk the
        // bytes are written to, which is the node's, and the node allocates it
        // with its own persister. A path from here would name a directory on a
        // machine that is not doing the writing.
        let sandbox_id = self.sandbox_id;
        let execution_id = self.execution_id;
        let node_id = self
            .placed()
            .map_err(SandboxCaptureError::recoverable)?
            .node
            .node_id
            .clone();

        // 🔴 Retried once on a connection that never reached `node_id` at
        // all — see `call_with_stale_placement_retry`'s doc. A pause that the
        // node actually answered, even with an error, is never replayed here.
        let response = self
            .call_with_stale_placement_retry("pause", |client| {
                Box::pin(client.pause(pb::SandboxPauseRequest {
                    sandbox_id: sandbox_id.to_string(),
                    execution_id: execution_id.to_string(),
                    // 🔴 The caller's promise, forwarded rather than assumed.
                    // This is the one backend for which producing a
                    // publishable capture costs a durable write on another
                    // machine, and bytes staged for a publisher that is not
                    // going to commit are unreachable forever — no read path
                    // resolves an unannounced snapshot. So the question "will
                    // anybody commit this" is answered by the publisher above
                    // this backend, before the node is asked to spend. See
                    // `SandboxBackend::pause`.
                    publish: committer_waiting,
                }))
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

        // 🔴 Decoded before `paused` is set, because a staged row this half
        // cannot read is a failure of the pause's *publication*, not of the
        // pause: the sandbox is down on the node either way and `stop` must
        // still be suppressed. Hence the decode failures below are recoverable
        // rather than terminal — they cost the cluster copy, never the sandbox.
        if !response.staging_error.is_empty() {
            // 🔴 A warning and not an error, matching what the node decided:
            // the sandbox is paused on that machine and reopenable there, and
            // what was lost is the copy that would have let it come back
            // anywhere else. Failing here would hand the layer above a
            // classification that is wrong in both directions — see
            // `SandboxPauseResponse.staging_error`.
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
            // 🔴 Said out loud rather than dropped quietly. Nothing asked this
            // node to stage anything, so whatever it wrote is bytes with no row
            // and no reader — the exact leak `publish` exists to prevent — and
            // the only account of it anyone will ever get is this line.
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
                Some(CapturedSandboxSnapshot::new(staged))
            }
            // 🔴 Not an error even when `publish` was asked for. The node
            // answers with nothing when its pause found the sandbox already
            // paused, and `Pause` has always documented an absent `staged` as
            // "the backend had nothing publishable to offer".
            None => None,
        };

        // 🔴 Set before the value is handed back, because the very next thing
        // the caller does with this backend is `stop` it.
        self.paused = true;

        Ok(PausedSandboxCapture {
            // 🔴 The incarnation is this stub's own, and it is the run that was
            // just paused rather than anything the node said. It is what a
            // later resume fences on, so a value read back from the reply would
            // let a node hand back a capture of some other run and have this
            // half record it as a capture of this one.
            state: Arc::new(RemotePausedState::new(
                node_id,
                paused.artifact_root,
                execution_id,
                state,
            )),
            // 🔴 The staged row, wearing the capture's clothes. `publishable`
            // exists so a caller holding live capture artifacts can hand them
            // to a repository before they are reclaimed; by the time this reply
            // exists the node has already written the bytes and there is
            // nothing left to reclaim. What the publisher above needs is the
            // row, and `SnapshotManager::stage_captured` recognises a capture
            // that is already staged and commits it instead of staging it
            // again — which it could not do anyway, having none of the files.
            //
            // 🔴 `None` when the node offered nothing, and that stays a
            // meaningful answer rather than an error: a node whose pause was
            // idempotent — the sandbox was already paused, or this call joined
            // one in flight — has no capture, because the capture belongs to
            // the pause that made it. Asking for `publish` and being given
            // nothing is not the same as being refused.
            publishable,
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
        let node_id = self
            .placed()
            .map_err(SandboxCaptureError::recoverable)?
            .node
            .node_id
            .clone();

        // 🔴 Retried once on a connection that never reached `node_id` at
        // all — see `call_with_stale_placement_retry`'s doc.
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
        self.placed().map_err(SandboxCaptureError::recoverable)?;

        // 🔴 Retried once on a connection that never reached the node at
        // all — see `call_with_stale_placement_retry`'s doc.
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
                    }),
                )
            })
            .await
            .map_err(wire::into_capture_error)?
            .into_inner();

        // 🔴 Read *after* the call, not before it and not from a value
        // captured earlier: a retry inside the call above may have rebuilt
        // `self.placed` against a freshly resolved address, and every child
        // stub this returns must be handed that live connection — the one the
        // retry, if any, actually used — rather than the one it has already
        // abandoned.
        let placed = self.placed().map_err(SandboxCaptureError::recoverable)?;
        let node = placed.node.clone();
        let client = placed.client.clone();

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

    /// Tears the sandbox down on the node — unless it has just been paused
    /// there.
    ///
    /// 🔴 The exception is not an optimisation. See
    /// [`RemoteSandboxStub::paused`]: after a successful pause the node has
    /// already stopped the VM and is holding the capture, and the only teardown
    /// this service has would delete it.
    ///
    /// It is decided from this process's own state — this stub sent the pause
    /// and read the reply — rather than from anything a node said, which is
    /// what keeps it from being the "an unreachable node means the sandbox is
    /// gone" mistake in another costume.
    async fn stop(&mut self) -> Result<()> {
        let sandbox_id = self.sandbox_id;
        let execution_id = self.execution_id;
        if self.paused {
            return Ok(());
        }
        let Some(placed) = self.placed.as_ref() else {
            // Never started anywhere, so there is nothing to tear down. This is
            // the *only* branch that treats "no sandbox" as success, and it is
            // safe because it is decided from this process's own state rather
            // than from a node's answer.
            return Ok(());
        };
        let node_id = placed.node.node_id.clone();

        // 🔴 Retried once, and only when the failure means the call never
        // reached `node_id` at all — never when it means the node answered.
        // See `call_with_stale_placement_retry`'s doc, and the module-level
        // incident this exists to fix: a delete for a sandbox paused before a
        // rolling node restart can find `self.placed` freshly rebuilt (see
        // `absent_handle`/`attach`) from a cluster binding cache that has not
        // yet been reconciled to the restarted node's new address, and every
        // such delete failed with exactly the transport error this retries.
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
            // 🔴 The node says it does not have this sandbox. That is an
            // answer, and it is the one that makes a stop idempotent — which
            // the trait requires.
            Err(status) if status.code() == tonic::Code::NotFound => Ok(()),
            // 🔴 Everything else, including a node that did not answer at all.
            // A stop that reported success because the node was unreachable
            // would take the cluster's last record of a running VM with it.
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
            // See the table at the top of this file.
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

    /// The node this stub is actually driving.
    ///
    /// 🔴 `self.placed`, not `self.placement` or anything derived from this
    /// process's own identity: this backend's entire reason to exist is that
    /// the sandbox runs somewhere else, and once `start`/`reopen` have
    /// answered, that somewhere is exactly `placed.node.node_id`. `None` before
    /// placement — a backend nobody has started yet has no machine to name —
    /// which callers must not read as "runs locally": the default this
    /// overrides means that, this override never does.
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

        // 🔴 Retried once on a connection that never reached the node at
        // all — see `call_with_stale_placement_retry`'s doc.
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

    /// 🔴 This used to be the one method on this trait answered from a spawned
    /// task instead of a round trip: locally it is a plain assignment and
    /// cannot fail, so the trait had no error to return, and the fix chosen
    /// here — rather than fire the RPC and forget it — is the same one
    /// `update_network_policy` already uses for the same shape of problem: an
    /// `async fn ... -> Result<()>` that awaits the reply. A caller that gets
    /// `Err` now knows the running sandbox never saw the value, instead of
    /// finding out from an `error!` line on a different Pod after `GET` had
    /// already started reporting it.
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

        // 🔴 Retried once on a connection that never reached the node at
        // all — see `call_with_stale_placement_retry`'s doc.
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
