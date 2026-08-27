//! The one RPC the data plane asks the control plane for.
//!
//! The gateway resolves a sandbox to a node out of its own routing projection.
//! When that misses, the sandbox is paused, gone, or somewhere the projection
//! has not caught up with — and only the half that owns sandboxes can tell
//! which. This service is that question, and the answer is either a node to
//! forward to or a refusal precise enough to back off on.
//!
//! # 🔴 What this is not
//!
//! It is not a remote form of the REST resume route. That route serves a user
//! who asked for a resume and can be told "409, try again"; this one serves a
//! request that is already in flight towards a sandbox. The difference shows up
//! in two places, both inherited from the local reverse proxy's
//! `try_auto_resume` rather than from the REST route:
//!
//! - the lifetime the woken sandbox gets is the auto-resume floor, not a
//!   caller-supplied one;
//! - the wake-up is bounded by `proxy::auto_resume_deadline()`, and the claim
//!   is handed back when that bound is hit — a request already in flight cannot
//!   wait indefinitely, and a claim left behind blocks the next attempt from
//!   anywhere.
//!
//! Everything else is the same path. Both go through
//! `ApiImpl::arbitrate_resume`, which is the single point a resume can acquire
//! the right to start.

use tonic::{Request, Response, Status};
use tracing::{debug, info, warn};

use crate::api::impls::{DataPlaneResume, DataPlaneResumeRequest, PinRefusalReason};
use crate::api::ApiImpl;
use crate::proto::apiproxy::{
    self as pb, sandbox_resume_service_server::SandboxResumeService as SandboxResumeServiceTrait,
};
use crate::types::SandboxId;

/// Reason values that are not pin refusals but still travel in the refusal
/// trailer, so the gateway has one key to read rather than two.
const REASON_TRANSITION_IN_PROGRESS: &str = "transition_in_progress";

/// Serves [`pb::sandbox_resume_service_server::SandboxResumeService`] out of one
/// `ApiImpl`.
pub struct SandboxResumeService<I> {
    api_impl: I,
}

impl<I> SandboxResumeService<I>
where
    I: AsRef<ApiImpl> + Send + Sync + 'static,
{
    pub fn new(api_impl: I) -> Self {
        Self { api_impl }
    }
}

#[tonic::async_trait]
impl<I> SandboxResumeServiceTrait for SandboxResumeService<I>
where
    I: AsRef<ApiImpl> + Send + Sync + 'static,
{
    async fn resume_sandbox(
        &self,
        request: Request<pb::SandboxResumeRequest>,
    ) -> Result<Response<pb::SandboxResumeResponse>, Status> {
        let target_port = target_port_of(&request);
        let envd_access_token = string_metadata(&request, pb::ACCESS_TOKEN_METADATA);
        let sandbox_id = request.into_inner().sandbox_id;
        let Ok(sandbox_id) = SandboxId::parse_str(&sandbox_id) else {
            // 🔴 InvalidArgument and not NotFound. A malformed id is a caller
            // bug, and answering "no such sandbox" would tell the gateway to
            // report a sandbox gone that it never named.
            record("invalid_argument");
            return Err(Status::invalid_argument(format!(
                "'{sandbox_id}' is not a sandbox id"
            )));
        };

        let outcome = self
            .api_impl
            .as_ref()
            .resume_for_data_plane(DataPlaneResumeRequest {
                sandbox_id,
                target_port,
                envd_access_token,
            })
            .await;

        match outcome {
            DataPlaneResume::Woken {
                node_id,
                node_address,
                execution_id,
            } => {
                info!(
                    target: "agentenv",
                    %sandbox_id,
                    %node_id,
                    %execution_id,
                    "woke a paused sandbox for the data plane"
                );
                record("ok");
                Ok(Response::new(pb::SandboxResumeResponse {
                    node_id,
                    node_address,
                    execution_id: execution_id.to_string(),
                }))
            }
            DataPlaneResume::Unauthorized => {
                debug!(%sandbox_id, "refused a wake-up that presented no valid envd token");
                record("permission_denied");
                Err(Status::permission_denied(
                    "invalid or missing envd access token",
                ))
            }
            // 🔴 The only answer that means "this sandbox is gone". Everything
            // else on this surface is retryable, because the gateway turns this
            // one into the 404 the platform reads as "rebuild it from its
            // template" — which resets a user's workspace.
            DataPlaneResume::NotFound => {
                record("not_found");
                Err(Status::not_found(format!("sandbox {sandbox_id} not found")))
            }
            DataPlaneResume::TransitionInProgress { holder } => {
                record("transition_in_progress");
                Err(refusal(
                    format!("sandbox is being resumed by node '{holder}'"),
                    REASON_TRANSITION_IN_PROGRESS,
                    &holder,
                ))
            }
            DataPlaneResume::PinRefused {
                reason,
                origin_node_id,
                detail,
            } => {
                // 🔴 Warn, not debug, and this is the line `_sd-recon-env.md`
                // §8's outstanding item asked for: the scheduler side of the
                // same refusal has a counter and no log, so an operator
                // watching a sandbox that will not wake has nothing to grep.
                warn!(
                    target: "agentenv",
                    %sandbox_id,
                    %origin_node_id,
                    reason = reason.as_str(),
                    "refusing to wake a sandbox pinned to a node that cannot serve it; \
                     no other node has its bytes, so this is not retried elsewhere"
                );
                record(pin_result_label(reason));
                Err(refusal(detail, reason.as_str(), &origin_node_id))
            }
            DataPlaneResume::Exhausted(reason) => {
                record("resource_exhausted");
                Err(Status::resource_exhausted(reason))
            }
            // 🔴 Unavailable and never NotFound. "Nobody could be asked" is not
            // an answer about whether the sandbox exists, and the two have
            // different consequences all the way down to the user's files.
            DataPlaneResume::Undecided(reason) => {
                warn!(
                    target: "agentenv",
                    %sandbox_id,
                    error = %reason,
                    "could not determine whether a sandbox may be woken"
                );
                record("unavailable");
                Err(Status::unavailable(reason))
            }
            DataPlaneResume::Failed(reason) => {
                record("internal");
                Err(Status::internal(reason))
            }
            // 🔴 The same status as `Failed` and a different label. §6.3's table
            // sends everything unclassified to `Internal`, and the local reverse
            // proxy answered a timed-out auto-resume with the same 502 it gave a
            // failed one — so answering differently here would make the pre-split single process
            // and the cold path disagree, which is the one thing the rollback
            // story cannot afford. The counter is where the two separate.
            DataPlaneResume::TimedOut => {
                record("timed_out");
                Err(Status::internal("the wake-up did not finish in time"))
            }
        }
    }
}

/// A `FailedPrecondition` carrying its reason where the gateway can read it.
///
/// See `crate::proto::apiproxy` for why the reason is a trailer and not a
/// status detail.
fn refusal(message: impl Into<String>, reason: &str, origin_node_id: &str) -> Status {
    let mut metadata = tonic::metadata::MetadataMap::new();
    if let Ok(value) = reason.parse() {
        metadata.insert(pb::REFUSAL_REASON_TRAILER, value);
    }
    if let Ok(value) = origin_node_id.parse() {
        metadata.insert(pb::REFUSAL_ORIGIN_TRAILER, value);
    }
    Status::with_metadata(tonic::Code::FailedPrecondition, message, metadata)
}

/// The metric label for a pin refusal. Identical to the wire reason on purpose:
/// one grep joins the gateway's log, this half's log, and the scrape.
fn pin_result_label(reason: PinRefusalReason) -> &'static str {
    reason.as_str()
}

fn record(result: &'static str) {
    metrics::counter!("agentenv_api_resume_grpc_total", "result" => result).increment(1);
}

/// Publishes every outcome of this service at zero.
///
/// 🔴 Called when the service is built, not when it is first used. The
/// acceptance probe for this whole move compares the gateway's attempt counter
/// against this one "逐条相等" — and two counters cannot be compared when one
/// of them does not exist until something goes right.
pub fn describe_metrics() {
    for result in [
        "ok",
        "invalid_argument",
        "permission_denied",
        "not_found",
        "transition_in_progress",
        "resource_exhausted",
        "unavailable",
        "internal",
        "timed_out",
        PinRefusalReason::OriginNotReporting.as_str(),
        PinRefusalReason::OriginNotAcceptingWork.as_str(),
        PinRefusalReason::OriginNotReachableFromHere.as_str(),
        PinRefusalReason::OriginUnclassified.as_str(),
    ] {
        metrics::counter!("agentenv_api_resume_grpc_total", "result" => result).increment(0);
    }
}

/// The port the data-plane request that triggered this was addressed to.
///
/// 🔴 Absent and unparseable are the same answer here, and that answer is
/// `None`, which [`ApiImpl::resume_for_data_plane`] treats as "possibly envd" —
/// the strict direction. A caller that could skip the credential check by
/// sending a port of `banana` would be a hole shaped exactly like the one the
/// check exists to close.
fn target_port_of<T>(request: &Request<T>) -> Option<u16> {
    request
        .metadata()
        .get(pb::TARGET_PORT_METADATA)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse().ok())
}

fn string_metadata<T>(request: &Request<T>, key: &str) -> String {
    request
        .metadata()
        .get(key)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string()
}
