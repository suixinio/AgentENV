//! Dispatching a template build to a node, for `--role api`.
//!
//! # 🔴 The one place in this module that reaches a staged row
//!
//! The module doc one file up names two gaps this crate still has: a cold
//! create refuses, and a pause's `publish` arm refuses because "nothing here
//! commits a staged snapshot row". A template build cannot refuse the same
//! way and still leave the caller anything to do — there is no local
//! execution to fall back to once `ServerRole::runs_sandbox_runtime()` is
//! false, so `--role api` either drives this or template builds stop working
//! for the whole cluster. This file is the first thing in `node_client` that
//! actually reaches a staged row, and it does so by reusing the mechanism a
//! checkpoint already relies on rather than inventing a second one — see
//! `crate::api::impls::template::run_the_build_on_a_node`, this function's one
//! caller, for how `SnapshotManager::stage_captured`'s downcast onto an
//! already-`StagedSnapshot` value (dispatching straight to `adopt_staged`)
//! turns the value this function returns into a committed row.

use std::time::Duration;

use prost::Message as _;
use tonic::transport::Endpoint;

use crate::proto::node::{self as pb, node_sandbox_service_client::NodeSandboxServiceClient};
use crate::snapshot::repository::StagedSnapshot;
use crate::snapshot::TemplateBuildErrorReason;
use crate::types::{SandboxId, SandboxResources};

use super::placement::NodePlacement;
use super::wire;

/// Generous headroom over `TemplateBuildRunner::READY_TIMEOUT` (ten minutes,
/// `src/template/runner.rs`): a caller that cut this call off before the
/// runner's own bound could ever fire would tear down a build that was still
/// legitimately waiting on a slow readiness check, and report the runner's
/// own "the build failed" as a transport failure instead of what it is.
const BUILD_CALL_TIMEOUT: Duration = Duration::from_secs(20 * 60);

/// How long a dial to an unreachable node may hang before this call gives up.
/// Distinct from [`BUILD_CALL_TIMEOUT`] on purpose: a node that is not there
/// at all should fail fast, not wait out the budget a build that is
/// legitimately running gets.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// How often an idle HTTP/2 connection carrying this call pings its peer, and
/// how long it waits for the reply before giving up on it.
///
/// 🔴 Every other RPC `node_client` sends (`RemoteSandboxStub::connect`) is a
/// handful of round trips and has never needed this. This one can sit with
/// nothing on the wire for up to ten minutes while the node runs a build
/// sandbox — exactly the shape an idle-connection reaper between the two
/// Pods (a NAT table, a conntrack entry, an L4 load balancer) reclaims. A
/// connection a reaper has already dropped is then indistinguishable from a
/// slow build until `BUILD_CALL_TIMEOUT` expires long after the fact.
/// Keepalive pings keep bytes moving so a reaper never mistakes this
/// connection for idle, and a missed pong closes it — failing this call —
/// within `KEEPALIVE_INTERVAL + KEEPALIVE_TIMEOUT` instead of twenty minutes.
/// The node's own server applies the matching setting to every connection it
/// accepts; see `src/node_server/mod.rs`.
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);
const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(10);

/// Runs one template build on a node this process picks, and returns the row
/// it staged there — not yet committed.
///
/// 🔴 A fresh connection for this one call, matching every other RPC in this
/// module (`RemoteSandboxStub::connect`) rather than a shared pool: the two
/// never share a channel today, so this call's unusually generous timeout and
/// keepalive settings have nothing else to accidentally apply to.
///
/// 🔴 Returns `TemplateBuildErrorReason` rather than `anyhow::Error`, unlike
/// every other function in this module. That type is what a failed build's
/// `step` travels in from here on — `models::BuildStatusReason.step`, read
/// off a template's build-status API — and building it here, where the
/// `Status` with the structured detail carrying it is still in scope, is the
/// one place that value can still be recovered. Flattening it into a plain
/// `anyhow::Error` string first, the way `wire::into_error` does for calls
/// that do not carry per-step detail, would lose it before this function's
/// one caller ever saw it.
pub(crate) async fn build_template_on_a_node(
    placement: &dyn NodePlacement,
    resources: SandboxResources,
    request: pb::TemplateBuildRequest,
) -> Result<StagedSnapshot, TemplateBuildErrorReason> {
    // 🔴 A synthetic id: `place_new` takes one because a sandbox create has
    // one to give, and every implementation in this tree
    // (`SchedulerNodePlacement`, `FixedNodePlacement`) ignores it. A build has
    // no sandbox to name; this exists only to satisfy the signature.
    let sandbox_id = SandboxId::new();
    let node = placement
        .place_new(sandbox_id, resources)
        .await
        .map_err(|err| {
            TemplateBuildErrorReason::new(format!("choose a node for a template build: {err:#}"))
        })?;

    let channel = Endpoint::from_shared(node.endpoint.clone())
        .map_err(|err| {
            TemplateBuildErrorReason::new(format!(
                "node endpoint {:?} is not a URI: {err}",
                node.endpoint
            ))
        })?
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(BUILD_CALL_TIMEOUT)
        .http2_keep_alive_interval(KEEPALIVE_INTERVAL)
        .keep_alive_timeout(KEEPALIVE_TIMEOUT)
        .keep_alive_while_idle(true)
        .connect()
        .await
        .map_err(|err| {
            TemplateBuildErrorReason::new(format!(
                "connect to node {} at {}: {err}",
                node.node_id, node.endpoint
            ))
        })?;

    let mut client = NodeSandboxServiceClient::new(channel);
    let response = client
        .build_template(request)
        .await
        .map_err(|status| build_failure_reason(&status, &node.node_id))?
        .into_inner();

    let staged = response.staged.and_then(|staged| staged.value);
    wire::serialized(staged.as_ref(), "staged template build")
        .map_err(|err| {
            TemplateBuildErrorReason::new(format!(
                "decode the staged template build node {} returned: {err:#}",
                node.node_id
            ))
        })?
        .ok_or_else(|| {
            TemplateBuildErrorReason::new(format!(
                "node {} answered a template build with no staged snapshot",
                node.node_id
            ))
        })
}

/// Turns a failed `BuildTemplate` call's status into the reason a template's
/// build-status API reports, recovering `step` from the status details when
/// the node attached one (`TemplateBuildFailureDetail`,
/// `crate::proto::node::build_failure_status`). A status with no details, or
/// details this build cannot decode, is reported message-only — the same
/// reading `NodeSandboxService::build_template` gives a failure it cannot
/// scope to one step.
fn build_failure_reason(status: &tonic::Status, node_id: &str) -> TemplateBuildErrorReason {
    let message = format!("node {node_id} refused the template build: {status}");
    let step = (!status.details().is_empty())
        .then(|| pb::TemplateBuildFailureDetail::decode(status.details()).ok())
        .flatten()
        .map(|detail| detail.step)
        .filter(|step| !step.is_empty());
    match step {
        Some(step) => TemplateBuildErrorReason::with_step(message, step),
        None => TemplateBuildErrorReason::new(message),
    }
}
