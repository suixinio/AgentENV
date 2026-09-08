//! Data-plane resume RPC.
//!
//! It returns a node to forward to or a structured refusal. Wake-ups use the
//! auto-resume lifetime floor and deadline, and share arbitration with REST resume.

use tonic::{Request, Response, Status};
use tracing::{debug, info, warn};

use crate::api::impls::{DataPlaneResume, DataPlaneResumeRequest};
use crate::api::ApiImpl;
use crate::proto::apiproxy::{
    self as pb, sandbox_resume_service_server::SandboxResumeService as SandboxResumeServiceTrait,
};
use crate::types::SandboxId;

/// Trailer reason for an in-progress transition.
const REASON_TRANSITION_IN_PROGRESS: &str = "transition_in_progress";

/// Trailer reason for a sandbox created with `autoResume` disabled.
///
/// Keep synchronized with `resumeReasonAutoResumeDisabled` in the gateway.
const REASON_AUTO_RESUME_DISABLED: &str = "auto_resume_disabled";

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
            // A malformed id is a caller error, not evidence that a sandbox is gone.
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
                already_running,
            } => {
                if already_running {
                    info!(
                        target: "agentenv",
                        %sandbox_id,
                        %node_id,
                        %execution_id,
                        "answered a running sandbox for the data plane"
                    );
                } else {
                    info!(
                        target: "agentenv",
                        %sandbox_id,
                        %node_id,
                        %execution_id,
                        "woke a paused sandbox for the data plane"
                    );
                }
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
            // This is the only outcome that tells the platform the sandbox is gone.
            DataPlaneResume::NotFound => {
                record("not_found");
                Err(Status::not_found(format!("sandbox {sandbox_id} not found")))
            }
            DataPlaneResume::AutoResumeDisabled => {
                // Expected, high-volume traffic for sandboxes with auto-resume disabled.
                debug!(
                    %sandbox_id,
                    "refusing to wake a sandbox created with auto-resume off"
                );
                record("auto_resume_disabled");
                Err(refusal(
                    "this sandbox was created with auto-resume off and does not wake on \
                     data-plane traffic",
                    REASON_AUTO_RESUME_DISABLED,
                    "",
                ))
            }
            DataPlaneResume::TransitionInProgress { holder } => {
                record("transition_in_progress");
                Err(refusal(
                    format!("another operation holds the sandbox: {holder}"),
                    REASON_TRANSITION_IN_PROGRESS,
                    &holder,
                ))
            }
            DataPlaneResume::Exhausted(reason) => {
                record("resource_exhausted");
                Err(Status::resource_exhausted(reason))
            }
            // Failure to decide is not evidence that the sandbox is gone.
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
            // Timed-out and failed wake-ups share a status but retain distinct metrics.
            DataPlaneResume::TimedOut => {
                record("timed_out");
                Err(Status::internal("the wake-up did not finish in time"))
            }
        }
    }
}

/// Builds a `FailedPrecondition` with refusal metadata consumed by the gateway.
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

fn record(result: &'static str) {
    metrics::counter!("agentenv_api_resume_grpc_total", "result" => result).increment(1);
}

/// Initializes every service outcome metric at zero.
pub fn describe_metrics() {
    for result in [
        "ok",
        "invalid_argument",
        "permission_denied",
        "not_found",
        "transition_in_progress",
        REASON_AUTO_RESUME_DISABLED,
        "resource_exhausted",
        "unavailable",
        "internal",
        "timed_out",
    ] {
        metrics::counter!("agentenv_api_resume_grpc_total", "result" => result).increment(0);
    }
}

/// Parses the request's target port; absent or invalid metadata yields `None`.
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
