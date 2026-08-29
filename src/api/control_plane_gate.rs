//! Who is allowed to call the node's user-facing REST API.
//!
//! The node's control plane used to accept anything that could reach the port:
//! the generated auth layer checks that a credential is *present*, not what it
//! is. Everything that legitimately calls it arrives through the gateway, and
//! the gateway is the only thing that has to be believed — so the gateway
//! stamps its outbound requests with a shared credential and this gate refuses
//! calls that do not carry it.
//!
//! # Where it is attached, and why that is the whole design
//!
//! The layer goes on the **generated router**, *before* `proxy::router(...)` is
//! merged in. `Router::merge` keeps each router's own layers, so the data plane
//! — `/proxy/*` and the fallback that carries host-routed sandbox traffic — is
//! not covered by this gate as a matter of *assembly order*, not as a matter of
//! this function remembering to check the path.
//!
//! That distinction is the point. The three layers that already sit on this
//! router are applied after the merge and therefore do run on data-plane
//! requests; they each open with a path check to get out of the way again. A
//! fourth layer written that way would put the node's whole data plane behind a
//! string comparison in a function whose job is to refuse things.
//!
//! # What it does not do
//!
//! Nothing about the node acting on its own. TTL eviction, graceful-shutdown
//! pauses, the reclaim upkeep pass and the data plane's own auto-resume all
//! happen without any inbound request, and this gate is invisible to every one
//! of them. Refusing outside callers is worth doing and is not the same thing
//! as the sandbox being safe from a superseded incarnation; that is what the
//! registry's write fencing is for.

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

/// The credential the gateway stamps on its outbound control-plane requests.
///
/// Lowercase, like every other `x-agentenv-*` header. Deliberately not the
/// existing `X-Admin-Token` / `X-API-Key`: those stay exactly as they are, and
/// what they mean stays exactly what it meant. This is a separate question —
/// "did this come through the control plane" — asked outside the generated auth
/// layer rather than instead of it.
pub const CONTROL_PLANE_HEADER: &str = "x-agentenv-control-plane";

/// Paths this gate never applies to.
///
/// 🔴 Two entries, and both are load-bearing. Keep the list this short: every
/// addition is a route that becomes reachable without a credential, and the
/// only defence against that going unnoticed is that the list is small enough
/// to read.
///
/// * `/health` is what kubelet polls, three ways. kubelet does not go through
///   the gateway and has no credential, so gating it stops the pod from ever
///   becoming ready.
///
/// 🔴 `GET /sandboxes` and `GET /v2/sandboxes` used to be exempt too, and are
/// not any more. The exemption existed for one caller: the gateway's
/// cluster-list fan-out, which asked every node for its own rows using the
/// gateway's own HTTP client rather than the reverse proxy, so it never passed
/// through the hook that stamps the credential. Its own retirement condition
/// was written here — *"what retires this exemption is deleting
/// `fetchNodeClusterList`, the off position ceasing to exist"* — and that has
/// happened: `services/gateway/internal/cluster_list.go` is deleted,
/// `isUserFacingRestRequest` claims both routes unconditionally, and
/// `Config.Validate` refuses an empty `gateway.rest_upstream_addr`, so there is
/// no configuration left in which anything reaches a node's listing routes
/// without the credential. Both routes are now gated exactly like every other
/// user-facing REST call; `the_sandbox_listing_is_gated_like_any_other_route`
/// is what fails if the exemption comes back.
fn is_exempt(_method: &Method, path: &str) -> bool {
    path == "/health"
}

/// The credentials this node accepts, and where they come from.
///
/// Two sources, unioned: a static list read once at startup, and a file re-read
/// while the process runs. Both empty means the gate is off.
pub struct ControlPlaneGate {
    /// From configuration/environment. Fixed for the life of the process.
    static_tokens: Vec<String>,
    /// The file to re-read, or `None` when none was configured.
    token_file: Option<PathBuf>,
    file_state: Mutex<TokenFileState>,
    /// Whether the "no credentials configured, gate is off" line has been
    /// logged. It says something an operator needs to see once, not once per
    /// request.
    announced_disabled: AtomicBool,
}

#[derive(Default)]
struct TokenFileState {
    /// Modification time and length of the contents behind `tokens`. Used to
    /// skip re-reading a file that has not changed.
    fingerprint: Option<(SystemTime, u64)>,
    /// The last contents that were read successfully.
    ///
    /// 🔴 Survives a failed read on purpose. A read that fails is not evidence
    /// that the credential was withdrawn — it is evidence of nothing at all —
    /// and treating it as "no credentials configured" would let a single disk
    /// hiccup turn the gate off without anyone being told. Turning the gate off
    /// is done by writing an empty file, which is a *successful* read of zero
    /// credentials.
    tokens: Option<Vec<String>>,
}

/// What the gate did with one request. A closed set: the label goes on a metric
/// and a metric label with unbounded values is a memory leak.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GateDecision {
    /// No credentials configured, so the gate is off and this request went
    /// through the way it would have before the gate existed.
    Disabled,
    /// A path the gate never applies to.
    Exempt,
    /// Carried a credential this node accepts.
    Allowed,
    /// 🔴 Refused. On a healthy cluster this is always zero, because every
    /// legitimate caller reaches the node through the gateway. A non-zero count
    /// is not an attack alert — it is the shortest route to finding the
    /// platform path that is not going through the gateway.
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

        // Publish the gauge before any request arrives. Both the enable step
        // and the rollback are watched by looking at exactly this series, and a
        // series that only appears once traffic happens to arrive is not
        // something anyone can watch — a node whose gate is off would be
        // indistinguishable from a node that has not been scraped yet.
        metrics::gauge!("agentenv_api_control_plane_gate_enabled")
            .set(if gate.accepted().is_empty() { 0.0 } else { 1.0 });

        gate
    }

    /// The credentials in force right now.
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

        // Skip the read when the file is byte-for-byte the one already held.
        // A control-plane request is rare enough that a stat per request costs
        // nothing, and this keeps the common case to exactly that.
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
                // 🔴 Never fail open. Keep serving on the credential that was
                // last known good and say loudly that the file cannot be read;
                // the alternative is that a transient read error silently opens
                // the node's whole control plane.
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
                // Never read successfully, which is what an unconfigured or
                // not-yet-mounted file looks like. That is the off state, and
                // the request below reports it as `disabled` rather than as an
                // error.
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

/// Compares two credentials without leaking where they first differ.
///
/// 🔴 Not `==`. This is a secret, and `==` on a byte slice stops at the first
/// mismatch, which turns "is this the credential" into a byte-at-a-time oracle.
/// Written out rather than pulled from a crate because it is six lines and the
/// crate would be a new direct dependency for them.
fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
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

/// Refuses control-plane calls that did not come through the gateway.
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
    // A gauge rather than something derived from the counters: "is the gate on"
    // has to be answerable without traffic, and the enable step and the
    // rollback are both watched by looking at exactly this.
    //
    // Exempt requests leave it alone: they say nothing about whether a
    // credential is configured, and letting a kubelet probe drive this gauge
    // would make it report "on" on a node where the gate is off.
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
        "refusing a control-plane call that did not come through the gateway"
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

    /// T-A4-6. 🔴 The node's own preStop hook is a caller of its REST API.
    ///
    /// It runs `curl` against `localhost:8000` and swallows its own failures
    /// with `|| echo "... continuing"`, so a missing credential does not show up
    /// as an error anywhere — it shows up weeks later as sandboxes being placed
    /// on a node that is shutting down. Nothing at runtime can catch that, so it
    /// is caught here, against the manifest itself.
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

        // 🔴 An invocation is a `curl` that names the node's own API, not
        // every occurrence of the word. Counting the word made the assertion
        // fire on a comment that mentioned `curl`, which is a failure that
        // teaches the next person to reword their comment rather than to look
        // at their hook.
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

        // ...and the file it reads the credential from is the one the server
        // was pointed at. Two halves of one mount; naming them differently
        // fails silently in exactly the same way.
        //
        // 🔴 Matches the env entry's own `- name: ` line, not the bare
        // variable name: a comment a few lines above this env entry
        // ("...the same reason AENV_API_CONTROL_PLANE_TOKEN_FILE below is a
        // file...") already spells the bare name in prose, so a bare-name
        // scan would stay green even if the real `- name:` entry below it
        // were deleted.
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

    /// 🔴 The two sides of the credential read *different keys of one Secret*,
    /// and the release order depends on nothing else.
    ///
    /// The gateway takes its half from an environment variable and this node
    /// takes its half from a projected file. Point both at the same key and
    /// creating the Secret — the single act that starts the gateway stamping —
    /// also drops the file that opens this node's gate, in the one order the
    /// rollout must never happen in. Nothing fails at that moment; what fails
    /// is every platform request between then and the gateway's rollout
    /// finishing, and the runbook step that says "gateway first, node second"
    /// becomes a sentence with nothing behind it.
    ///
    /// Checked here rather than trusted to review because the regression is
    /// deleting four lines of YAML, and the manifest that results is valid,
    /// renders, applies, and reads exactly like the safe one.
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

        // The node projects one named key, and names it. Without `items:` the
        // whole Secret lands in the directory and the gateway's own key
        // becomes the node's file the moment it is written.
        let after_marker = DAEMONSET
            .rsplit_once("- name: control-plane-token")
            .expect("the daemonset mounts the control-plane credential")
            .1;
        // 🔴 Bounded to this one volume entry, not everything after it to the
        // end of the file. Unbounded, the assertions below could all be
        // satisfied by a later sibling volume's own `items:`/`optional: true`
        // (`heartbeat-config`, right after this one) or by a comment further
        // down that mentions `node-gate-token` in prose — either of which
        // would keep this test green even if this volume's own projection
        // were deleted outright.
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

        // ...and it is not the key the gateway reads.
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

    /// 🔴 The exemption list is `/health` and nothing else.
    ///
    /// The listing routes are named explicitly rather than left to the
    /// catch-all below because they are the ones that *were* exempt: a revert
    /// of that deletion is the realistic way this regresses, and it would
    /// reopen two unauthenticated reads of every sandbox in the cluster.
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

    /// The comparison is constant time, and it is still a comparison.
    #[test]
    fn the_credential_comparison_agrees_with_equality() {
        assert!(constant_time_eq(b"token", b"token"));
        assert!(!constant_time_eq(b"token", b"tokeo"));
        assert!(!constant_time_eq(b"token", b"token-longer"));
        assert!(!constant_time_eq(b"", b"token"));
        assert!(constant_time_eq(b"", b""));
    }
}
