//! Credential gate for the node's own control-plane routes.
//!
//! The layer attaches to the generated router before the data-plane proxy is
//! merged, so proxy traffic remains outside this gate. It is attached only on
//! the half that does not own user-facing REST — see `server::assemble`.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use axum::{
    body::Body,
    extract::{Request, State},
    http::{header, HeaderValue, Method, Response, StatusCode},
    middleware::Next,
};
use tracing::{info, warn};

use crate::cfg::ConfigManager;

/// Header carrying the control-plane credential.
pub const CONTROL_PLANE_HEADER: &str = "x-agentenv-control-plane";

/// Returns whether a route must remain reachable without a credential.
///
/// Only kubelet's `/health` probe is exempt.
fn is_exempt(_method: &Method, path: &str) -> bool {
    path == "/health"
}

/// Credentials accepted from static configuration and a reloadable file.
pub struct ControlPlaneGate {
    static_tokens: Vec<String>,
    token_file: Option<PathBuf>,
    file_state: Mutex<TokenFileState>,
    announced_disabled: AtomicBool,
}

#[derive(Default)]
struct TokenFileState {
    fingerprint: Option<(SystemTime, u64)>,
    /// Last successfully read tokens; read failures must not disable the gate.
    tokens: Option<Vec<String>>,
}

/// Closed metric-label outcomes for one gate decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GateDecision {
    /// No credentials are configured.
    Disabled,
    /// The path is exempt.
    Exempt,
    /// The presented credential is accepted.
    Allowed,
    /// The presented credential is not accepted.
    Refused,
}

impl GateDecision {
    fn label(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Exempt => "exempt",
            Self::Allowed => "allowed",
            Self::Refused => "refused",
        }
    }
}

impl ControlPlaneGate {
    pub fn from_global_config() -> Self {
        let config = &ConfigManager::global_config().api;
        Self::new(
            config.control_plane_tokens.clone(),
            config.control_plane_token_file.clone(),
        )
    }

    pub fn new(static_tokens: Vec<String>, token_file: impl Into<String>) -> Self {
        let token_file = token_file.into();
        let token_file = token_file.trim();

        let gate = Self {
            static_tokens: normalize(static_tokens),
            token_file: (!token_file.is_empty()).then(|| PathBuf::from(token_file)),
            file_state: Mutex::new(TokenFileState::default()),
            announced_disabled: AtomicBool::new(false),
        };

        // Publish before traffic so disabled and unscraped nodes remain distinguishable.
        metrics::gauge!("agentenv_api_control_plane_gate_enabled")
            .set(if gate.accepted().is_empty() { 0.0 } else { 1.0 });

        gate
    }

    fn accepted(&self) -> Vec<String> {
        let mut accepted = self.static_tokens.clone();
        accepted.extend(self.file_tokens());
        accepted
    }

    fn file_tokens(&self) -> Vec<String> {
        let Some(path) = self.token_file.as_ref() else {
            return Vec::new();
        };

        let mut state = self
            .file_state
            .lock()
            .expect("token file state is poisoned");

        // Avoid rereading an unchanged file.
        let fingerprint = std::fs::metadata(path)
            .and_then(|meta| Ok((meta.modified()?, meta.len())))
            .ok();
        if let (Some(fingerprint), Some(held), Some(tokens)) =
            (fingerprint, state.fingerprint, state.tokens.as_ref())
        {
            if fingerprint == held {
                metrics::counter!(
                    "agentenv_api_control_plane_token_reload_total",
                    "result" => "unchanged",
                )
                .increment(1);

                return tokens.clone();
            }
        }

        match std::fs::read_to_string(path) {
            Ok(contents) => {
                let tokens = normalize(contents.lines().map(str::to_string).collect());
                state.fingerprint = fingerprint;
                state.tokens = Some(tokens.clone());
                metrics::counter!(
                    "agentenv_api_control_plane_token_reload_total",
                    "result" => "loaded",
                )
                .increment(1);

                tokens
            }
            Err(err) => match state.tokens.as_ref() {
                // Keep the last known-good credential rather than fail open.
                Some(tokens) => {
                    metrics::counter!(
                        "agentenv_api_control_plane_token_reload_total",
                        "result" => "error_kept_previous",
                    )
                    .increment(1);
                    warn!(
                        path = %path.display(),
                        error = %err,
                        "cannot read the control-plane credential file; keeping the last one that \
                         was read successfully. Clear the file to turn the gate off deliberately \
                         — an unreadable file is not the same thing as an empty one"
                    );

                    tokens.clone()
                }
                // A file never read successfully leaves the gate disabled.
                None => Vec::new(),
            },
        }
    }

    fn decide(&self, method: &Method, path: &str, presented: Option<&str>) -> GateDecision {
        if is_exempt(method, path) {
            return GateDecision::Exempt;
        }

        let accepted = self.accepted();
        if accepted.is_empty() {
            if !self.announced_disabled.swap(true, Ordering::Relaxed) {
                info!(
                    "no control-plane credential is configured; the node's REST API accepts any \
                     caller that can reach it"
                );
            }

            return GateDecision::Disabled;
        }

        let presented = presented.unwrap_or_default();
        if accepted
            .iter()
            .any(|accepted| constant_time_eq(accepted.as_bytes(), presented.as_bytes()))
        {
            GateDecision::Allowed
        } else {
            GateDecision::Refused
        }
    }
}

/// Compares credentials without data-dependent early exit: length first,
/// then every byte, so a mismatch is not something a caller can walk one
/// byte at a time.
pub fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }

    let mut difference = 0_u8;
    for (left, right) in left.iter().zip(right.iter()) {
        difference |= left ^ right;
    }

    difference == 0
}

fn normalize(tokens: Vec<String>) -> Vec<String> {
    tokens
        .into_iter()
        .map(|token| token.trim().to_string())
        .filter(|token| !token.is_empty())
        .collect()
}

/// Refuses control-plane calls that present no accepted credential.
pub async fn require_control_plane(
    State(gate): State<Arc<ControlPlaneGate>>,
    request: Request,
    next: Next,
) -> Response<Body> {
    let presented = request
        .headers()
        .get(CONTROL_PLANE_HEADER)
        .and_then(|value| value.to_str().ok());
    let decision = gate.decide(request.method(), request.uri().path(), presented);

    metrics::counter!(
        "agentenv_api_control_plane_gate_total",
        "decision" => decision.label(),
    )
    .increment(1);
    // Exempt requests do not reveal whether credentials are configured.
    match decision {
        GateDecision::Exempt => {}
        GateDecision::Disabled => {
            metrics::gauge!("agentenv_api_control_plane_gate_enabled").set(0.0)
        }
        GateDecision::Allowed | GateDecision::Refused => {
            metrics::gauge!("agentenv_api_control_plane_gate_enabled").set(1.0)
        }
    }

    if decision != GateDecision::Refused {
        return next.run(request).await;
    }

    warn!(
        method = %request.method(),
        path = %request.uri().path(),
        "refusing a control-plane call that presented no accepted credential"
    );

    Response::builder()
        .status(StatusCode::FORBIDDEN)
        .header(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/plain; charset=utf-8"),
        )
        .body(Body::from("control plane credential required"))
        .expect("static forbidden response is valid")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_prestop_drain_carries_the_control_plane_credential() {
        const MANIFEST: &str = include_str!("../../deploy/k8s/base/agentenv-daemonset.yaml");

        let pre_stop = MANIFEST
            .split_once("preStop:")
            .expect("the daemonset has a preStop hook")
            .1
            .split_once("postStart:")
            .expect("preStop is followed by postStart")
            .0;

        // Count only curl invocations that target the node API.
        let calls: Vec<&str> = pre_stop
            .split("curl ")
            .skip(1)
            .filter(|call| call.contains("http://localhost:8000"))
            .collect();
        let credentials = calls
            .iter()
            .filter(|call| call.contains(CONTROL_PLANE_HEADER))
            .count();

        assert!(
            !calls.is_empty(),
            "the preStop hook still calls the node's API"
        );
        assert_eq!(
            credentials,
            calls.len(),
            "every preStop call to the node's API must carry the control-plane credential; \
             found {} call(s) and {credentials} credential header(s)",
            calls.len()
        );

        // The hook and server must read the same mounted credential file.
        assert!(
            MANIFEST.contains("- name: AENV_API_CONTROL_PLANE_TOKEN_FILE"),
            "the node must be told where to read the control-plane credential"
        );
        assert!(
            MANIFEST
                .matches("/etc/agentenv/control-plane/token")
                .count()
                >= 2,
            "the preStop hook and the server must read the same credential file"
        );
    }

    #[test]
    fn the_gateway_and_the_node_read_different_keys_of_the_credential_secret() {
        const DAEMONSET: &str = include_str!("../../deploy/k8s/base/agentenv-daemonset.yaml");
        const GATEWAY: &str = include_str!("../../deploy/k8s/base/gateway-deployment.yaml");
        const SECRET: &str = "agentenv-control-plane-token";
        const GATEWAY_KEY: &str = "token";
        const NODE_KEY: &str = "node-gate-token";

        assert!(
            DAEMONSET.contains(SECRET) && GATEWAY.contains(SECRET),
            "both sides still read one Secret; splitting it into two is a \
             different design and this test would be the wrong one for it"
        );

        // Project only the node's named key from the shared Secret.
        let after_marker = DAEMONSET
            .rsplit_once("- name: control-plane-token")
            .expect("the daemonset mounts the control-plane credential")
            .1;
        // Bound checks to this volume so sibling projections cannot satisfy them.
        let volume = after_marker
            .split_once("\n        - name: ")
            .map_or(after_marker, |(this_volume, _next_sibling)| this_volume);
        assert!(
            volume.contains("items:") && volume.contains(NODE_KEY),
            "the node's credential volume must project `{NODE_KEY}` by name, or creating the \
             Secret opens the node's gate at the same moment it starts the gateway stamping"
        );
        assert!(
            volume.contains("optional: true"),
            "the projection must stay optional, or a Secret without `{NODE_KEY}` fails the \
             mount instead of leaving the gate off"
        );

        assert_ne!(
            NODE_KEY, GATEWAY_KEY,
            "the two halves must be different keys of the Secret"
        );
        assert!(
            GATEWAY.contains("GATEWAY_CONTROL_PLANE_TOKEN"),
            "the gateway still reads the credential it stamps"
        );
        assert!(
            !volume.contains(&format!("key: {GATEWAY_KEY}\n")),
            "the node must not project the gateway's key"
        );
    }

    #[test]
    fn only_the_health_probe_is_exempt() {
        assert!(is_exempt(&Method::GET, "/health"));
        assert!(is_exempt(&Method::POST, "/health"));

        assert!(!is_exempt(&Method::GET, "/sandboxes"));
        assert!(!is_exempt(&Method::GET, "/v2/sandboxes"));
        assert!(!is_exempt(&Method::POST, "/sandboxes"));
        assert!(!is_exempt(&Method::DELETE, "/sandboxes"));
        assert!(!is_exempt(&Method::GET, "/sandboxes/some-id"));
        assert!(!is_exempt(&Method::POST, "/nodes/some-id"));
    }

    #[test]
    fn the_credential_comparison_agrees_with_equality() {
        assert!(constant_time_eq(b"token", b"token"));
        assert!(!constant_time_eq(b"token", b"tokeo"));
        assert!(!constant_time_eq(b"token", b"token-longer"));
        assert!(!constant_time_eq(b"", b"token"));
        assert!(constant_time_eq(b"", b""));
    }
}
