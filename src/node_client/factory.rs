//! Building sandboxes that run somewhere else.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{anyhow, bail, Result};
use serde_json::Value;

use crate::proto::node as pb;
use crate::sandbox::{
    EnvdAccessToken, FreshSandboxBuildSpec, PausedSandboxState, SandboxBackend,
    SandboxBackendFactory, SandboxLaunchConfig,
};
use crate::snapshot::RunnableSnapshot;
use crate::types::{ExecutionId, SandboxId};

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
        let record = snapshot.record();
        let resources = *snapshot.resources();

        let request = pb::SandboxCreateRequest {
            sandbox_id: launch_config.sandbox_id.to_string(),
            execution_id: execution_id.to_string(),
            source: Some(pb::sandbox_create_request::Source::Snapshot(
                pb::SnapshotSource {
                    snapshot_id: record.id.to_string(),
                },
            )),
            // 🔴 Left to the node's default rather than guessed at. The
            // sandbox's timeout is decided by the orchestrator above this
            // factory and written into its own record; it does not reach a
            // backend, and inventing one here would give the node a deadline
            // nobody agreed to.
            timeout_ms: 0,
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
            // 🔴 Empty here, and that is a placeholder with a date on it
            // rather than an omission or a decision.
            //
            // The ownership marker is *per sandbox* — it is the control plane's
            // own record of this sandbox, written before the sandbox exists and
            // naming it (`ForkChildAssignment::control_plane_config`) — so it
            // cannot be a property of this factory, and there is nothing on the
            // launch config to carry it. It belongs to a caller that decides
            // what a sandbox's record is, and that caller is `--role api`,
            // which `assemble_api` refuses to build at all.
            //
            // 🔴 So the consequence is worth stating plainly, because a grep
            // for who sets the marker comes back empty and that reads like a
            // defect: **no sandbox anywhere carries an ownership marker
            // today**, because the only half that would attach one cannot
            // start. `ListSandboxes` therefore admits nothing on a real
            // cluster, and the shadow-phase probe that expects to see a
            // sandbox pushed up through it cannot be run yet.
            // `the_blank_ownership_marker_outlives_only_an_api_role_that_cannot_start`
            // ties those two facts together so they stop being true at the same
            // time.
            control_plane_config: Vec::new(),
        };

        Ok(Box::new(RemoteSandboxStub::pending(
            launch_config.sandbox_id,
            execution_id,
            resources,
            Arc::clone(&self.placement),
            PendingLaunch::FromSnapshot {
                request: Box::new(request),
            },
        )))
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
        let inner = state
            .get("state")
            .cloned()
            .ok_or_else(|| anyhow!("paused state has no backend state"))?;

        Ok(Arc::new(RemotePausedState::new(
            origin_node_id,
            artifact_root,
            inner,
        )))
    }

    /// 🔴 Refused, because a resume is not something this half does to a
    /// backend.
    ///
    /// Locally this rebuilds the sandbox from state the same machine captured.
    /// Remotely there is no such thing: the state names a machine, and bringing
    /// the sandbox back means asking *that* machine to create it again from the
    /// capture it is holding. That is a create with a different source, and it
    /// needs a `Resume` on the node service to carry it — which the node does
    /// not serve yet, because nothing on the node side stages or reopens a
    /// capture across this boundary.
    fn build_from_paused_state(
        &self,
        sandbox_id: SandboxId,
        _execution_id: ExecutionId,
        state: &dyn PausedSandboxState,
        _envd_access_token: Option<EnvdAccessToken>,
    ) -> Result<Box<dyn SandboxBackend>> {
        let origin = state
            .downcast_ref::<RemotePausedState>()
            .map(|state| state.origin_node_id().to_string())
            .unwrap_or_else(|| "an unknown node".to_string());
        bail!(
            "sandbox {sandbox_id} cannot be resumed from here: its capture is on {origin}, and \
             the node service has no call that reopens one"
        )
    }
}
