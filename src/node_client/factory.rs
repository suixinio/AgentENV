//! Building sandboxes that run somewhere else.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{anyhow, bail, Result};
use serde_json::Value;

use crate::proto::node as pb;
use crate::runtime_snapshot::RunnableSnapshot;
use crate::sandbox::{
    EnvdAccessToken, FreshSandboxBuildSpec, PausedSandboxState, SandboxBackend,
    SandboxBackendFactory, SandboxLaunchConfig, UnresolvedImageBuildSpec,
};
use crate::snapshot::SnapshotRecord;
use crate::types::{ExecutionId, SandboxId, SandboxResources};

use super::paused_state::RemotePausedState;
use super::placement::NodePlacement;
use super::stub::{PendingLaunch, RemoteSandboxStub};
use super::wire;

/// A [`SandboxBackendFactory`] whose sandboxes run on other machines.
///
/// # 🔴 Why swapping this one type parameter is not the whole job
///
/// The obvious reading of the split is that an orchestrator becomes a cluster
/// orchestrator by being handed this instead of the Firecracker factory. That
/// is the right shape and it is not sufficient, and the three reasons are worth
/// having written down where the code is:
///
/// 1. **Placement.** [`SandboxBackendFactory::build`] is synchronous and
///    returns a backend; the asynchronous `start` is on the backend. So nothing
///    here may touch a network, and choosing a node happens in
///    [`RemoteSandboxStub::start`]. That is what makes the seam hold — but it
///    also means the deciding half still has to ask something where a sandbox
///    should go, and that something is the cluster scheduler.
/// 2. **The persister has to change too.** A deciding half that kept a
///    file-backed persister would be writing paused-sandbox artifacts to its own
///    disk for sandboxes whose bytes are on other machines. It wants the
///    disabled one: the durable record of a paused sandbox is the cluster
///    store's row, not a file here.
/// 3. 🔴 **A cold create has nothing left to send.** `build` is handed a
///    [`FreshSandboxBuildSpec`] whose image config path and extra drives have
///    *already been resolved* into paths on the local disk, and the user's
///    image reference is gone by then. A remote factory needs the reference,
///    because resolving an image is the node's job. So this arm cannot work
///    until resolution moves behind the factory — and it refuses rather than
///    inventing something.
pub struct RemoteSandboxBackendFactory {
    placement: Arc<dyn NodePlacement>,
}

impl RemoteSandboxBackendFactory {
    pub fn new(placement: Arc<dyn NodePlacement>) -> Self {
        Self { placement }
    }
}

impl SandboxBackendFactory for RemoteSandboxBackendFactory {
    /// 🔴 Yes: every sandbox this factory builds runs on a machine that is not
    /// this process, so the machine has to be told whose sandbox it is. Without
    /// this the node stores no marker, `ListSandboxes` reports nothing, and the
    /// reconciliation that decides which bindings are still live sees an empty
    /// cluster — which reads exactly like a cluster with nothing running on it.
    fn stamps_control_plane_ownership(&self) -> bool {
        true
    }

    /// 🔴 Refused; see reason 3 on the type.
    fn build(
        &self,
        _build_spec: FreshSandboxBuildSpec,
        _launch_config: SandboxLaunchConfig,
        _execution_id: ExecutionId,
    ) -> Result<Box<dyn SandboxBackend>> {
        bail!(
            "a cold sandbox cannot be started on another node from here: the build spec this \
             factory is handed has already been resolved into local paths, and the image \
             reference a node would need is no longer in it"
        )
    }

    fn build_from_snapshot(
        &self,
        snapshot: &RunnableSnapshot,
        launch_config: SandboxLaunchConfig,
        execution_id: ExecutionId,
    ) -> Result<Box<dyn SandboxBackend>> {
        // 🔴 Everything this arm ever read out of the `RunnableSnapshot` is the
        // catalog row inside it. The manifest, the local artifact paths and the
        // cache lease resolving produced are dropped here unread — which is
        // what `build_from_snapshot_record` below exists to let a caller skip
        // paying for. Delegating rather than duplicating is what makes "the two
        // produce the same request" a fact of construction instead of a claim.
        self.build_from_snapshot_record(snapshot.record(), launch_config, execution_id)
    }

    /// The unresolved counterpart of [`build_from_snapshot`](Self::build_from_snapshot).
    ///
    /// # 🔴 Why this one does not refuse
    ///
    /// Same argument as [`build_from_image_ref`](Self::build_from_image_ref),
    /// one rung further in. A factory that boots the VM itself has to resolve
    /// the row before it can build anything; this one never touches the bytes,
    /// so resolving on this machine downloads a `vm_state.bin` and materializes
    /// two overlaybd image configs that nothing on this machine will open. The
    /// node does its own `resolve_runnable` on the row it is sent
    /// (`NodeSandboxService::create`), against the cache its VM actually reads.
    fn build_from_snapshot_record(
        &self,
        record: &SnapshotRecord,
        launch_config: SandboxLaunchConfig,
        execution_id: ExecutionId,
    ) -> Result<Box<dyn SandboxBackend>> {
        let resources = record.resources;

        let request = pb::SandboxCreateRequest {
            sandbox_id: launch_config.sandbox_id.to_string(),
            execution_id: execution_id.to_string(),
            source: Some(pb::sandbox_create_request::Source::Snapshot(
                pb::SnapshotSource {
                    snapshot_id: record.id.to_string(),
                    // 🔴 The catalog row this half already holds, so the node
                    // does not have to ask its own catalog for it — see
                    // `SnapshotSource.resolved_record`'s own doc in
                    // node.proto. Forwarding it is free: whichever caller got
                    // here already paid for the lookup that produced it, and
                    // the row is the *whole* of what this request carries
                    // about the snapshot — which is why an api half that never
                    // resolved it sends byte-for-byte the same bytes as one
                    // that did.
                    resolved_record: Some(wire::serialize(record, "resolved snapshot record")?),
                },
            )),
            // 🔴 **Said out loud, because it used to be said by omission.**
            // The sandbox's deadline is decided by the orchestrator above this
            // factory and written into *its* record, together with the expiry
            // index and the eviction loop that act on it. The node is to keep
            // none.
            //
            // Sending nothing used to be how that was expressed, and the node
            // read it as "the caller named no deadline, so use mine": a
            // 15-second `default_sandbox_timeout_secs` then paused the VM while
            // this half went on answering `running`, and every later call on
            // the sandbox failed naming something else. There is now no way to
            // leave the question open — see `SandboxCreateRequest.expiry`.
            expiry: Some(pb::sandbox_create_request::Expiry::CallerKept(
                pb::CallerKeptExpiry {},
            )),
            timeout_action: pb::TimeoutAction::Pause as i32,
            auto_resume: false,
            secure: launch_config.envd_access_token.is_some(),
            user_metadata: Default::default(),
            env_vars: launch_config.env_vars.clone().unwrap_or_default(),
            network_policy: launch_config
                .network
                .as_ref()
                .map(|policy| wire::serialize(policy, "network policy"))
                .transpose()?,
            custom_extension_params: launch_config
                .custom_extension_params
                .as_ref()
                .map(|params| wire::serialize(params, "custom extension params"))
                .transpose()?,
            // The control plane's own record of this sandbox, put on the
            // launch config by `Orchestrator::stamp_control_plane_ownership`
            // because that is where the record becomes complete.
            //
            // 🔴 Not built here, and the reason is that this factory has no
            // per-sandbox knowledge: the marker names one sandbox and is
            // written before it exists, so a constant on a factory shared by
            // every create could never be it.
            //
            // 🔴 `unwrap_or_default` produces an empty vector, and an empty
            // marker is *not* a marker — the node reads it back as "no control
            // plane owns this" and leaves the sandbox out of `ListSandboxes`.
            // That is the fail-closed direction and the right one for a create
            // that somehow arrived unstamped: a sandbox the control plane does
            // not recognise is left running, where a sandbox wrongly claimed
            // would be reconciled away. `the_marker_the_orchestrator_stamped_is_the_marker_on_the_wire`
            // pins that an ordinary create is stamped, so the empty case stays
            // the exception it is meant to be.
            control_plane_config: launch_config.control_plane_config.unwrap_or_default(),
        };

        Ok(Box::new(RemoteSandboxStub::pending(
            launch_config.sandbox_id,
            execution_id,
            resources,
            Arc::clone(&self.placement),
            PendingLaunch::Launch {
                request: Box::new(request),
            },
        )))
    }

    /// The cold-create counterpart of [`build_from_snapshot`](Self::build_from_snapshot)
    /// above: builds the same kind of pending `Create` request, from a
    /// `Source::Image` instead of a `Source::Snapshot`.
    ///
    /// # 🔴 Why this one does not refuse
    ///
    /// [`build`](Self::build) above refuses because the [`FreshSandboxBuildSpec`]
    /// it is handed has already lost the user's image reference to local
    /// resolution. [`UnresolvedImageBuildSpec`] is the type that exists so
    /// that does not happen here: `aenv-api`'s `sandboxes_cold_post`
    /// constructs one instead of resolving anything, and everything in it —
    /// the reference, the attached drives' references, the already-computed
    /// resources — travels to the node as-is. The node resolves it, the same
    /// way a local cold create resolves it before ever reaching a factory.
    fn build_from_image_ref(
        &self,
        spec: UnresolvedImageBuildSpec,
        launch_config: SandboxLaunchConfig,
        execution_id: ExecutionId,
    ) -> Result<Box<dyn SandboxBackend>> {
        let resources = spec.resources;
        let attached_drives = spec
            .attached_drives
            .iter()
            .map(|drive| pb::AttachedDrive {
                image_ref: drive.image_ref.clone(),
                mount_path: drive.mount_path.display().to_string(),
                drive_id: drive.drive_id.clone(),
                read_only: drive.read_only,
                sub_path: drive
                    .sub_path
                    .as_ref()
                    .map(|p| p.display().to_string())
                    .unwrap_or_default(),
                virtual_size_bytes: drive.virtual_size.unwrap_or_default(),
            })
            .collect();

        let request = pb::SandboxCreateRequest {
            sandbox_id: launch_config.sandbox_id.to_string(),
            execution_id: execution_id.to_string(),
            source: Some(pb::sandbox_create_request::Source::Image(pb::ImageSource {
                image_ref: spec.image_ref,
                resources: Some(pb::SandboxResources {
                    cpu_count: resources.cpu_count,
                    memory_mib: resources.memory_mib,
                    disk_size_mib: resources.disk_size_mib,
                }),
                attached_drives,
                extra_boot_args: spec.extra_boot_args.unwrap_or_default(),
            })),
            // 🔴 Same reasoning as `build_from_snapshot`'s `expiry`, verbatim:
            // this orchestrator keeps the deadline, so the node keeps none.
            expiry: Some(pb::sandbox_create_request::Expiry::CallerKept(
                pb::CallerKeptExpiry {},
            )),
            timeout_action: pb::TimeoutAction::Pause as i32,
            auto_resume: false,
            secure: launch_config.envd_access_token.is_some(),
            user_metadata: Default::default(),
            env_vars: launch_config.env_vars.clone().unwrap_or_default(),
            network_policy: launch_config
                .network
                .as_ref()
                .map(|policy| wire::serialize(policy, "network policy"))
                .transpose()?,
            custom_extension_params: launch_config
                .custom_extension_params
                .as_ref()
                .map(|params| wire::serialize(params, "custom extension params"))
                .transpose()?,
            // See `build_from_snapshot`'s note on the same field: written by
            // `Orchestrator::stamp_control_plane_ownership`, not here.
            control_plane_config: launch_config.control_plane_config.unwrap_or_default(),
        };

        Ok(Box::new(RemoteSandboxStub::pending(
            launch_config.sandbox_id,
            execution_id,
            resources,
            Arc::clone(&self.placement),
            PendingLaunch::Launch {
                request: Box::new(request),
            },
        )))
    }

    /// 🔴 Yes: the VMs are on other machines, and those machines are not going
    /// anywhere when this process does.
    ///
    /// What this buys is that a rolled api replica does not pause the cluster.
    /// The shutdown path preserves everything in the record store by pausing
    /// it, which is right for a machine that is about to stop running VMs and
    /// catastrophic for a replica whose store is the whole cluster's ledger.
    fn sandboxes_outlive_this_process(&self) -> bool {
        true
    }

    /// Yes: a sandbox this factory built goes on running whether or not this
    /// process happens to be holding a handle for it.
    ///
    /// # 🔴 The assumption this exists to retire
    ///
    /// `Orchestrator` keeps live backends in a process-local map and, for a
    /// single process, "not in the map" and "not running" are the same fact.
    /// `aenv-api` is deployed as replicas behind a Service with no session
    /// affinity, so the two come apart on the first request: the replica a
    /// pause lands on is not usually the replica that created the sandbox, and
    /// the one that did not create it has an empty map and the cluster's own
    /// record in front of it.
    ///
    /// What that cost in production is worth writing down, because it is what
    /// makes this method's existence non-negotiable: the pause path read the
    /// missing handle as a sandbox that had gone and **deleted the shared
    /// record**, leaving the VM running on its node with nothing left that
    /// could name it. Two of those filled half a machine and could not be
    /// deleted through the API at all.
    ///
    /// 🔴 The stub is deliberately built with no node in it. Which machine a
    /// sandbox is on is a question for the scheduler
    /// ([`NodePlacement::place_existing`][super::NodePlacement::place_existing]),
    /// this method is synchronous, and a factory that took a network round trip
    /// would break the seam described at the top of this type.
    fn adopt_running(
        &self,
        sandbox_id: SandboxId,
        execution_id: ExecutionId,
        resources: SandboxResources,
    ) -> Result<Option<Box<dyn SandboxBackend>>> {
        Ok(Some(Box::new(RemoteSandboxStub::attaching(
            sandbox_id,
            execution_id,
            resources,
            Arc::clone(&self.placement),
        ))))
    }

    /// Reads back what [`RemotePausedState::encode`] wrote.
    ///
    /// 🔴 `artifact_root` is ignored, and the one inside the encoding is used
    /// instead. The parameter is the directory *this* process would have
    /// written the capture into, and this process wrote nothing: the bytes are
    /// on the node that paused the sandbox, under the path that node chose.
    fn decode_paused_state(
        &self,
        _artifact_root: PathBuf,
        state: Value,
    ) -> Result<Arc<dyn PausedSandboxState>> {
        let origin_node_id = state
            .get("origin_node_id")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                // 🔴 Refused rather than defaulted to "somewhere". A paused
                // state with no machine attached is a resume that would be sent
                // to whichever node placement happened to pick, and it would
                // fail there in a way that looks like the bytes are corrupt.
                anyhow!("paused state does not say which node holds its artifacts")
            })?
            .to_string();
        let artifact_root = state
            .get("artifact_root")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("paused state has no artifact root"))?
            .to_string();
        // 🔴 Required, and refused rather than defaulted to "whatever that node
        // is holding". This is the fence a resume carries: without it the call
        // says only *which sandbox*, and a node that kept a stale paused record
        // — one whose sandbox was resumed elsewhere and paused there — would
        // reopen a run the user abandoned, with their newer work left on
        // another disk. Defaulting it would make that the normal path.
        let paused_execution_id = state
            .get("execution_id")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("paused state does not say which run it captured"))
            .and_then(|raw| {
                ExecutionId::parse_str(raw).map_err(|err| {
                    anyhow!("paused state names {raw:?} as the run it captured: {err}")
                })
            })?;
        let inner = state
            .get("state")
            .cloned()
            .ok_or_else(|| anyhow!("paused state has no backend state"))?;

        Ok(Arc::new(RemotePausedState::new(
            origin_node_id,
            artifact_root,
            paused_execution_id,
            inner,
        )))
    }

    /// Builds a stub that will ask the machine holding the capture to reopen
    /// it.
    ///
    /// # 🔴 Locally this rebuilds a sandbox; here it addresses a machine
    ///
    /// The local factory is handed live state its own process captured and
    /// turns it back into a running VM. This one is handed a *reference*: a
    /// path on somebody else's disk, that machine's own encoding of its backend
    /// state, and — the two fields that make it addressable — which machine and
    /// which run. Nothing here can reopen anything, and nothing needs to: the
    /// bytes never moved, so what the node is sent is an identity, a fence and
    /// the run to start.
    ///
    /// 🔴 This is the *pinned* arm only. A paused sandbox whose capture reached
    /// shared storage can be rebuilt on any machine, and that is a create from
    /// a published snapshot — a different call, taken by the surface that knows
    /// whether the capture was published. Nothing here silently falls back to
    /// it: a stub that quietly rebuilt from an older snapshot when the capture
    /// could not be found would answer 200 and hand the user back a sandbox
    /// missing everything they had done since.
    ///
    /// # 🔴 What reaches this method, and what it is still missing
    ///
    /// The round trip is closed: a pause driven from here leaves a record on
    /// the node and a reference in the cluster store, and
    /// `Orchestrator::resume_sandbox` reads that reference back through
    /// `MetadataStore::paused_handle` and decodes it with this factory —
    /// rather than through `SandboxMetadata::paused_state`, which is
    /// `#[serde(skip)]` and is `None` on every store that writes its records
    /// out.
    ///
    /// What a sandbox paused from this half still does not have is a copy
    /// anywhere but the origin node's disk: `Pause` is sent with
    /// `publish: false` because nothing here commits a staged snapshot row. So
    /// [`RemoteResumeFailure::CaptureAbsent`][super::wire] means the sandbox is
    /// gone rather than "rebuild it from its published snapshot", and it will
    /// go on meaning that until publication is wired up.
    fn build_from_paused_state(
        &self,
        sandbox_id: SandboxId,
        execution_id: ExecutionId,
        state: &dyn PausedSandboxState,
        // 🔴 Ignored, and not dropped on the floor: the node derives this
        // sandbox's envd token itself, from the cluster-wide seed both halves
        // are configured with. Sending one would put a credential on a wire to
        // set a value the receiver was going to compute anyway — and would make
        // a resume fail confusingly if the two seeds ever disagreed, instead of
        // failing where the disagreement is.
        _envd_access_token: Option<EnvdAccessToken>,
    ) -> Result<Box<dyn SandboxBackend>> {
        let state = state.downcast_ref::<RemotePausedState>().ok_or_else(|| {
            // 🔴 Refused rather than sent to whichever node placement points
            // at. A paused state this factory did not produce says nothing
            // about which machine holds the bytes, and a resume sent on that
            // basis fails on the far side in a way that looks like the capture
            // is corrupt.
            anyhow!(
                "sandbox {sandbox_id} cannot be resumed from here: its paused state was not \
                 produced by this factory, so nothing in it says which machine holds the capture"
            )
        })?;

        let paused_execution_id = state.paused_execution_id();
        if paused_execution_id == execution_id {
            bail!(
                "sandbox {sandbox_id} would be resumed as the same run it was paused under \
                 ({paused_execution_id}): a resume starts a new one, and reusing the paused \
                 run's identity leaves commands written before the pause indistinguishable from \
                 commands written after it"
            );
        }

        let request = pb::SandboxResumeRequest {
            sandbox_id: sandbox_id.to_string(),
            // The run the capture is of. The node checks it against its own
            // record before reopening anything.
            execution_id: paused_execution_id.to_string(),
            // The run the resume claim allocated, which the node must adopt
            // rather than mint its own.
            resumed_execution_id: execution_id.to_string(),
            // 🔴 Zero, meaning "keep what it was paused with" — which for a
            // sandbox this half created is no deadline at all, because the
            // create said `caller_kept`. Unlike a create, this zero has only
            // ever had one reading: a resume has an existing deadline to keep,
            // so there was never a second meaning for it to be confused with.
            timeout_ms: 0,
        };

        Ok(Box::new(RemoteSandboxStub::pending(
            sandbox_id,
            execution_id,
            // 🔴 A placeholder until the node answers, and it is never used to
            // place anything: a resume goes to the machine holding the capture,
            // so nothing here asks which machine has room. The real values come
            // back on the reply — this trait method is handed no resources, and
            // the node's record is what has them.
            SandboxResources::default(),
            Arc::clone(&self.placement),
            PendingLaunch::Resume {
                request: Box::new(request),
                origin_node_id: state.origin_node_id().to_string(),
            },
        )))
    }
}
