//! Generated-route ownership for split API and node processes.
//!
//! The gate wraps the generated router before the data-plane router is merged,
//! so a node returns the same 404 as an absent user route without gating proxy
//! traffic. It runs before the credential gate.

use axum::{
    body::Body,
    extract::Request,
    http::{Method, Response, StatusCode},
    middleware::{self, Next},
    response::IntoResponse,
    Router,
};
use tracing::debug;

/// Stable metric label for the node-only gate.
const GATE_ROLE_LABEL: &str = "node";

/// Attaches the route gate to node processes and leaves API routers untouched.
pub fn attach(generated: Router, serves_user_facing_rest: bool) -> Router {
    // Publish before traffic so an unattached gate is observable.
    metrics::gauge!("agentenv_api_role_gate_enabled").set(if serves_user_facing_rest {
        0.0
    } else {
        1.0
    });

    if serves_user_facing_rest {
        return generated;
    }

    generated.layer(middleware::from_fn(refuse_outside_role))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RoleDecision {
    Served,
    Refused,
}

impl RoleDecision {
    fn label(self) -> &'static str {
        match self {
            Self::Served => "served",
            Self::Refused => "refused",
        }
    }
}

/// Returns whether a generated route belongs to the node process.
///
/// Nodes serve health probes and node read/drain endpoints; all sandbox,
/// snapshot, and template routes belong to the API process.
fn node_serves(method: &Method, path: &str) -> bool {
    match path {
        // Let the generated router decide unsupported methods on `/health`.
        "/health" => true,
        "/nodes" => method == Method::GET,
        _ => node_detail_id(path).is_some() && (method == Method::GET || method == Method::POST),
    }
}

/// Extracts the single `{nodeID}` segment from `/nodes/{nodeID}`.
fn node_detail_id(path: &str) -> Option<&str> {
    let rest = path.strip_prefix("/nodes/")?;
    if rest.is_empty() || rest.contains('/') {
        return None;
    }
    Some(rest)
}

/// Refuses generated routes outside the node process's surface.
async fn refuse_outside_role(request: Request, next: Next) -> Response<Body> {
    let decision = if node_serves(request.method(), request.uri().path()) {
        RoleDecision::Served
    } else {
        RoleDecision::Refused
    };

    metrics::counter!(
        "agentenv_api_role_gate_total",
        "role" => GATE_ROLE_LABEL,
        "decision" => decision.label(),
    )
    .increment(1);

    if decision == RoleDecision::Served {
        return next.run(request).await;
    }

    // Expected traffic is counted without warning per request.
    debug!(
        role = GATE_ROLE_LABEL,
        method = %request.method(),
        path = %request.uri().path(),
        "refusing a route this half does not serve"
    );

    not_found(request.method(), request.uri().path())
}

/// Returns the same JSON 404 envelope as an unattached generated route.
///
/// This avoids advertising route existence or supported methods.
fn not_found(method: &Method, path: &str) -> Response<Body> {
    let error =
        agentenv_http_server::models::Error::new(404, format!("route not found: {method} {path}"));

    (StatusCode::NOT_FOUND, axum::Json(error)).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    const GENERATED_SERVER: &str = include_str!("generated/src/server/mod.rs");

    fn generated_route_paths() -> Vec<String> {
        GENERATED_SERVER
            .split(".route(")
            .skip(1)
            .filter_map(|tail| {
                let tail = tail.trim_start();
                let quoted = tail.strip_prefix('"')?;
                let (path, _) = quoted.split_once('"')?;
                path.starts_with('/').then(|| path.to_string())
            })
            .collect()
    }

    fn concrete(path: &str) -> String {
        let mut out = String::with_capacity(path.len());
        let mut rest = path;
        while let Some((before, after)) = rest.split_once('{') {
            out.push_str(before);
            let (_, after) = after.split_once('}').expect("a route pattern is balanced");
            out.push_str("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
            rest = after;
        }
        out.push_str(rest);
        out
    }

    #[test]
    fn a_node_serves_its_own_routes_and_refuses_the_users() {
        assert!(node_serves(&Method::GET, "/health"));
        assert!(node_serves(&Method::GET, "/nodes"));
        assert!(node_serves(&Method::GET, "/nodes/ip-10-0-1-7"));
        assert!(
            node_serves(&Method::POST, "/nodes/ip-10-0-1-7"),
            "the preStop hook posts to this to drain the node"
        );

        assert!(!node_serves(&Method::POST, "/sandboxes"));
        assert!(!node_serves(&Method::GET, "/sandboxes"));
        assert!(!node_serves(&Method::GET, "/v2/sandboxes"));
        assert!(!node_serves(&Method::POST, "/sandboxes-cold"));
        assert!(!node_serves(&Method::POST, "/sandboxes/sbx-1/pause"));
        assert!(!node_serves(&Method::DELETE, "/sandboxes/sbx-1"));
        assert!(!node_serves(&Method::GET, "/snapshots"));
        assert!(!node_serves(&Method::DELETE, "/snapshots/snap-1"));
        assert!(!node_serves(&Method::GET, "/templates"));
        assert!(!node_serves(&Method::POST, "/v3/templates"));
    }

    #[test]
    fn every_generated_route_is_either_named_by_the_allowlist_or_refused() {
        let paths = generated_route_paths();
        assert_eq!(
            paths.len(),
            25,
            "the generated route table changed. Partition the new route below and \
             update this count; the routes are {paths:?}"
        );

        let mut served = Vec::new();
        let mut refused = Vec::new();
        for path in &paths {
            let concrete = concrete(path);
            let any = [Method::GET, Method::POST, Method::DELETE, Method::PUT]
                .iter()
                .any(|method| node_serves(method, &concrete));
            if any {
                served.push(path.as_str());
            } else {
                refused.push(path.as_str());
            }
        }

        assert_eq!(
            served,
            vec!["/health", "/nodes", "/nodes/{node_id}"],
            "a aenv-node process serves exactly the three routes §7.2 names"
        );
        assert_eq!(refused.len(), 22);
        for group in ["/sandboxes", "/snapshots", "/templates", "/v2/", "/v3/"] {
            assert!(
                refused.iter().any(|path| path.starts_with(group)),
                "the {group} group must be on the refused side"
            );
        }
    }

    #[test]
    fn only_a_single_segment_follows_the_nodes_prefix() {
        assert_eq!(node_detail_id("/nodes/ip-10-0-1-7"), Some("ip-10-0-1-7"));
        assert_eq!(node_detail_id("/nodes/"), None);
        assert_eq!(node_detail_id("/nodes/a/b"), None);
        assert_eq!(node_detail_id("/nodes/a/"), None);
        assert_eq!(node_detail_id("/nodes"), None);
        assert_eq!(node_detail_id("/sandboxes/a"), None);

        assert!(!node_serves(&Method::GET, "/nodes/a/sandboxes"));
    }

    #[test]
    fn the_node_routes_are_served_under_the_methods_they_were_granted() {
        assert!(node_serves(&Method::GET, "/nodes"));
        assert!(!node_serves(&Method::POST, "/nodes"));
        assert!(!node_serves(&Method::DELETE, "/nodes"));

        assert!(node_serves(&Method::GET, "/nodes/n1"));
        assert!(node_serves(&Method::POST, "/nodes/n1"));
        assert!(!node_serves(&Method::DELETE, "/nodes/n1"));
        assert!(!node_serves(&Method::PUT, "/nodes/n1"));
    }

    use axum::http::Request as HttpRequest;
    use axum::routing::{get, post};
    use tower::ServiceExt;

    /// Generated-router stand-in spanning both sides of the allowlist.
    fn stand_in_generated() -> Router {
        Router::new()
            .route("/health", get(|| async { "ok" }))
            .route("/nodes", get(|| async { "described" }))
            .route(
                "/nodes/{node_id}",
                get(|| async { "described" }).post(|| async { "drained" }),
            )
            .route("/sandboxes", get(|| async { "listed" }))
            .route("/sandboxes", post(|| async { "created" }))
            .route("/v2/sandboxes", get(|| async { "listed" }))
    }

    fn stand_in_data_plane() -> Router {
        Router::new()
            .route("/proxy/{*rest}", get(|| async { "proxied" }))
            .fallback(get(|| async { "fallback" }))
    }

    async fn status(router: Router, method: Method, path: &str) -> StatusCode {
        router
            .oneshot(
                HttpRequest::builder()
                    .method(method)
                    .uri(path)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
            .status()
    }

    #[tokio::test]
    async fn the_attached_layer_refuses_user_rest_and_serves_the_nodes_own_routes() {
        let node = || attach(stand_in_generated(), false);

        assert_eq!(status(node(), Method::GET, "/health").await, StatusCode::OK);
        assert_eq!(status(node(), Method::GET, "/nodes").await, StatusCode::OK);
        assert_eq!(
            status(node(), Method::POST, "/nodes/n1").await,
            StatusCode::OK
        );

        for (method, path) in [
            (Method::POST, "/sandboxes"),
            (Method::GET, "/sandboxes"),
            (Method::GET, "/v2/sandboxes"),
        ] {
            assert_eq!(
                status(node(), method.clone(), path).await,
                StatusCode::NOT_FOUND,
                "{method} {path} must be refused as if it did not exist"
            );
        }
    }

    #[tokio::test]
    async fn a_refusal_says_only_what_an_absent_route_says() {
        let response = attach(stand_in_generated(), false)
            .oneshot(
                HttpRequest::builder()
                    .method(Method::POST)
                    .uri("/sandboxes")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok()),
            Some("application/json"),
            "the refusal must carry the envelope a JSON client can parse"
        );
        let body = axum::body::to_bytes(response.into_body(), 256)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["code"], 404);
        assert_eq!(body["message"], "route not found: POST /sandboxes");
    }

    #[tokio::test]
    async fn the_half_that_serves_everything_gets_no_layer_at_all() {
        let router = || attach(stand_in_generated(), true);
        assert_eq!(
            status(router(), Method::POST, "/sandboxes").await,
            StatusCode::OK,
            "aenv-api must serve the user-facing surface"
        );
        assert_eq!(
            status(router(), Method::GET, "/health").await,
            StatusCode::OK
        );

        assert_eq!(
            status(
                attach(stand_in_generated(), false),
                Method::POST,
                "/sandboxes"
            )
            .await,
            StatusCode::NOT_FOUND,
            "the same call on the half that does get a layer is refused"
        );
    }

    const DAEMONSET: &str = include_str!("../../deploy/k8s/base/agentenv-daemonset.yaml");

    fn prestop_script() -> &'static str {
        DAEMONSET
            .split_once("preStop:")
            .expect("the daemonset has a preStop hook")
            .1
            .split_once("postStart:")
            .expect("preStop is followed by postStart")
            .0
    }

    fn prestop_calls() -> Vec<(Method, String)> {
        prestop_script()
            .split("curl ")
            .skip(1)
            .filter_map(|call| {
                let (flags, rest) = call.split_once("http://localhost:8000")?;
                let path: String = rest
                    .chars()
                    .take_while(|c| !c.is_whitespace() && *c != '"' && *c != '\'')
                    .collect();
                let path = path.replace("${AENV_NODE_ID}", "ip-10-0-1-7");
                let method = if flags.contains("-X POST") {
                    Method::POST
                } else {
                    Method::GET
                };
                Some((method, path))
            })
            .collect()
    }

    #[test]
    fn every_call_the_prestop_hook_makes_is_a_route_a_node_still_serves() {
        let calls = prestop_calls();
        assert_eq!(
            calls.len(),
            2,
            "the hook drains the node and then waits for it: {calls:?}"
        );

        for (method, path) in &calls {
            assert!(
                node_serves(method, path),
                "the preStop hook calls {method} {path}, which an aenv-node process answers \
                 with 404. The hook swallows its own failures, so this does not show up as an \
                 error — it shows up as a pod stuck in Terminating for the whole grace period."
            );
        }

        assert!(!node_serves(&Method::GET, "/sandboxes"));
    }

    #[test]
    fn the_drain_wait_cannot_run_forever() {
        let script = prestop_script();
        assert!(
            script.contains("DRAIN_READ_FAILURES") && script.contains("-ge 10"),
            "a read that keeps failing must give up rather than retry forever"
        );
        assert!(
            script.contains("DRAIN_DEADLINE") && script.contains("date +%s"),
            "a count that never reaches zero must hit a deadline"
        );
        // Both exits hand over rather than exiting the container themselves.
        assert_eq!(
            script
                .matches("handing over to the server's own shutdown")
                .count(),
            2,
            "each bounded exit must say what happens next"
        );
    }

    #[test]
    fn the_drain_counts_running_and_starting_sandboxes_but_not_paused() {
        let script = prestop_script();
        for field in ["sandboxCount", "sandboxStartingCount"] {
            assert!(
                script.contains(field),
                "the drain condition must include {field}"
            );
        }
        // Inspect the jq query rather than explanatory text elsewhere in the script.
        let jq_line = script
            .lines()
            .find(|line| line.contains("sandboxCount|type"))
            .expect("the drain loop reads the count through a jq filter over sandboxCount");
        assert!(
            jq_line.contains(".sandboxCount + .sandboxStartingCount"),
            "the drain condition must sum exactly running + starting: {jq_line}"
        );
        assert!(
            !jq_line.contains("sandboxPausedCount"),
            "a paused sandbox is already durably persisted and outlives this process on \
             purpose; summing it into the drain query stalls every rollout for as long as the \
             cluster holds any paused sandbox at all: {jq_line}"
        );
        // Missing or nonnumeric fields are failed reads, never zero.
        assert!(
            script.contains("jq -e") && script.contains(r#"|type)=="number""#),
            "a missing or non-numeric count must be a failed read, never a zero"
        );
    }

    #[tokio::test]
    async fn the_gate_does_not_reach_what_is_merged_after_it() {
        let router = || attach(stand_in_generated(), false).merge(stand_in_data_plane());

        assert_eq!(
            status(router(), Method::POST, "/sandboxes").await,
            StatusCode::NOT_FOUND,
            "the probe has resolution: a gated route is refused"
        );
        assert_eq!(
            status(router(), Method::GET, "/proxy/anything").await,
            StatusCode::OK,
            "the sandbox data plane must keep answering on a node"
        );
        assert_eq!(
            status(router(), Method::GET, "/not-a-route-at-all").await,
            StatusCode::OK,
            "the fallback carries host-routed sandbox traffic and is not this gate's business"
        );
    }
}
