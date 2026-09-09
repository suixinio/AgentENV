//! The node's HTTP router: its own report, its metrics, and the data plane.

use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{middleware, Json, Router};

use aenv_core::api::{require_control_plane, ControlPlaneGate};
use aenv_core::observability::prometheus;
use agentenv_http_server::models;
use agentenv_observability::metrics_handler;

use super::node_api::{NodeApi, ReportRefusal};

/// The local sandbox HTTP data plane: the proxy routes, and the host
/// classifier that rewrites host-named traffic onto them.
///
/// The classifier is applied around the whole router, inside the metrics layer,
/// so what the metrics see is the classified request.
pub struct DataPlane {
    routes: Router,
    classify: Box<dyn FnOnce(Router) -> Router + Send>,
}

impl DataPlane {
    pub fn new(routes: Router, classify: impl FnOnce(Router) -> Router + Send + 'static) -> Self {
        Self {
            routes,
            classify: Box::new(classify),
        }
    }
}

/// Builds the node's router.
///
/// The user-facing REST surface is absent rather than refused: a node serves
/// no sandbox, snapshot or template route, so an unmatched request falls
/// through to the data plane, which answers routing-header traffic and gives
/// everything else the same not-found envelope an absent route would.
pub fn new(node: Arc<NodeApi>, data_plane: DataPlane) -> Router {
    with_gate(
        node,
        data_plane,
        Arc::new(ControlPlaneGate::from_global_config()),
    )
}

/// [`new`] with the credential the gate admits chosen by the caller.
pub fn with_gate(node: Arc<NodeApi>, data_plane: DataPlane, gate: Arc<ControlPlaneGate>) -> Router {
    let DataPlane { routes, classify } = data_plane;

    let control_plane = Router::new()
        .route("/health", get(health))
        .route("/nodes", get(list_nodes))
        .route("/nodes/{node_id}", get(get_node).post(set_node_status))
        .with_state(Arc::clone(&node))
        .layer(middleware::from_fn_with_state(gate, require_control_plane));

    let router = control_plane
        .route("/metrics", get(metrics_handler))
        .merge(routes);

    classify(router).layer(middleware::from_fn(prometheus::http_metrics_middleware))
}

/// The same JSON envelope every generated route answers errors with.
fn error(code: i32, message: impl Into<String>) -> Json<models::Error> {
    Json(models::Error::new(code, message.into()))
}

/// Admin identity, as the generated routes read it: a non-empty header, not a
/// credential. Access control belongs at the network boundary.
fn admitted(headers: &HeaderMap) -> bool {
    headers
        .get("X-Admin-Token")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| !value.is_empty())
}

impl ReportRefusal {
    fn into_response(self) -> Response {
        match self {
            Self::NotFound(message) => (StatusCode::NOT_FOUND, error(404, message)).into_response(),
            Self::Unavailable(message) => {
                (StatusCode::INTERNAL_SERVER_ERROR, error(500, message)).into_response()
            }
        }
    }
}

async fn health() -> StatusCode {
    StatusCode::NO_CONTENT
}

async fn list_nodes(
    State(node): State<Arc<NodeApi>>,
    headers: HeaderMap,
    Query(query): Query<models::NodesGetQueryParams>,
) -> Response {
    if !admitted(&headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    match node.list_self(query.cluster_id).await {
        Ok(nodes) => (StatusCode::OK, Json(nodes)).into_response(),
        Err(refusal) => refusal.into_response(),
    }
}

async fn get_node(
    State(node): State<Arc<NodeApi>>,
    headers: HeaderMap,
    Path(path): Path<models::NodesNodeIdGetPathParams>,
    Query(query): Query<models::NodesNodeIdGetQueryParams>,
) -> Response {
    if !admitted(&headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    match node.describe_self(&path.node_id, query.cluster_id).await {
        Ok(detail) => (StatusCode::OK, Json(detail)).into_response(),
        Err(refusal) => refusal.into_response(),
    }
}

async fn set_node_status(
    State(node): State<Arc<NodeApi>>,
    headers: HeaderMap,
    Path(path): Path<models::NodesNodeIdPostPathParams>,
    Query(query): Query<models::NodesNodeIdPostQueryParams>,
    body: Result<Json<models::NodeStatusChange>, axum::extract::rejection::JsonRejection>,
) -> Response {
    if !admitted(&headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let Ok(Json(body)) = body else {
        return (StatusCode::BAD_REQUEST, error(400, "invalid request body")).into_response();
    };
    let cluster_id = query.cluster_id.or(body.cluster_id);
    match node
        .set_own_status(&path.node_id, cluster_id, body.status)
        .await
    {
        Ok(Ok(())) => StatusCode::NO_CONTENT.into_response(),
        Ok(Err(_derived)) => (
            StatusCode::CONFLICT,
            error(
                409,
                format!(
                    "node status {} is derived by the scheduler and cannot be set",
                    body.status
                ),
            ),
        )
            .into_response(),
        Err(refusal) => refusal.into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use axum::body::Body;
    use axum::http::{Method, Request as HttpRequest};
    use tower::ServiceExt;

    use crate::orchestrator::Orchestrator;
    use aenv_core::api::CONTROL_PLANE_HEADER;

    const TOKEN: &str = "control-plane-token";
    /// A route this node both serves and gates.
    const NODE_PATH: &str = "/nodes";

    async fn node_api() -> Arc<NodeApi> {
        node_api_reporting_as(None).await
    }

    /// A node that reports itself under `node_id`, the way a running one does.
    async fn node_api_reporting_as(node_id: Option<&str>) -> Arc<NodeApi> {
        let orchestrator =
            Orchestrator::with_in_memory_store(aenv_core::sandbox::mock::MockBackendFactory::new())
                .await;
        let observability = match node_id {
            Some(node_id) => {
                let identity = aenv_core::identity::NodeIdentity {
                    id: node_id.to_string(),
                    ..aenv_core::identity::NodeIdentity::from_config(&Default::default())
                };
                Some(Arc::new(
                    aenv_core::observability::ObservabilityService::new(
                        identity,
                        Arc::clone(&orchestrator)
                            as Arc<dyn crate::orchestrator::SandboxOrchestration>,
                        None,
                        Arc::new(std::sync::RwLock::new(None)),
                    )
                    .await,
                ))
            }
            None => None,
        };
        Arc::new(NodeApi::new(orchestrator, observability, Vec::new()))
    }

    async fn gated(gate: Arc<ControlPlaneGate>) -> Router {
        router_for(node_api().await, gate)
    }

    fn router_for(node: Arc<NodeApi>, gate: Arc<ControlPlaneGate>) -> Router {
        with_gate(
            Arc::clone(&node),
            super::super::proxy::data_plane(node),
            gate,
        )
    }

    async fn status(router: Router, method: Method, path: &str, token: Option<&str>) -> StatusCode {
        let mut request = HttpRequest::builder()
            .method(method)
            .uri(path)
            .header("X-Admin-Token", "probe");
        if let Some(token) = token {
            request = request.header(CONTROL_PLANE_HEADER, token);
        }
        router
            .oneshot(request.body(Body::empty()).unwrap())
            .await
            .unwrap()
            .status()
    }

    fn one_token() -> Arc<ControlPlaneGate> {
        Arc::new(ControlPlaneGate::new(vec![TOKEN.to_string()], ""))
    }

    #[tokio::test]
    async fn the_gate_covers_this_nodes_own_routes_and_nothing_merged_after_them() {
        // The probe has resolution: a gated route with no credential is refused.
        assert_eq!(
            status(gated(one_token()).await, Method::GET, NODE_PATH, None).await,
            StatusCode::FORBIDDEN
        );

        // ...and therefore these two mean the data plane is not behind it.
        assert_ne!(
            status(
                gated(one_token()).await,
                Method::GET,
                "/proxy/anything",
                None
            )
            .await,
            StatusCode::FORBIDDEN,
            "the data plane must not be behind the control-plane gate"
        );
        assert_ne!(
            status(
                gated(one_token()).await,
                Method::GET,
                "/not-a-route-at-all",
                None
            )
            .await,
            StatusCode::FORBIDDEN,
            "the fallback carries host-routed sandbox traffic and must not be gated"
        );
    }

    #[tokio::test]
    async fn a_call_without_the_control_plane_credential_is_refused() {
        assert_ne!(
            status(
                gated(one_token()).await,
                Method::GET,
                NODE_PATH,
                Some(TOKEN)
            )
            .await,
            StatusCode::FORBIDDEN
        );
        for presented in [None, Some(""), Some("wrong"), Some("CONTROL-PLANE-TOKEN")] {
            assert_eq!(
                status(gated(one_token()).await, Method::GET, NODE_PATH, presented).await,
                StatusCode::FORBIDDEN,
                "presented credential {presented:?} must not be accepted"
            );
        }
    }

    #[tokio::test]
    async fn an_empty_configured_token_lets_everything_through() {
        assert_ne!(
            status(
                gated(Arc::new(ControlPlaneGate::new(Vec::new(), ""))).await,
                Method::GET,
                NODE_PATH,
                None
            )
            .await,
            StatusCode::FORBIDDEN
        );
    }

    #[tokio::test]
    async fn health_is_never_gated() {
        assert_eq!(
            status(gated(one_token()).await, Method::GET, "/health", None).await,
            StatusCode::NO_CONTENT,
            "gating /health stops the pod ever becoming ready"
        );
    }

    #[tokio::test]
    async fn the_gate_picks_up_a_token_written_after_startup() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("control-plane-token");
        let gate = Arc::new(ControlPlaneGate::new(
            Vec::new(),
            path.to_str().expect("temp paths are valid utf-8"),
        ));

        assert_ne!(
            status(gated(Arc::clone(&gate)).await, Method::GET, NODE_PATH, None).await,
            StatusCode::FORBIDDEN
        );

        std::fs::write(&path, format!("{TOKEN}\n")).unwrap();
        assert_eq!(
            status(gated(Arc::clone(&gate)).await, Method::GET, NODE_PATH, None).await,
            StatusCode::FORBIDDEN
        );
        assert_ne!(
            status(
                gated(Arc::clone(&gate)).await,
                Method::GET,
                NODE_PATH,
                Some(TOKEN)
            )
            .await,
            StatusCode::FORBIDDEN
        );

        // An empty file is a successful read of zero credentials, which is the
        // deliberate off switch.
        std::fs::write(&path, "").unwrap();
        assert_ne!(
            status(gated(gate).await, Method::GET, NODE_PATH, None).await,
            StatusCode::FORBIDDEN
        );
    }

    #[tokio::test]
    async fn an_unreadable_token_file_keeps_the_last_known_value() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("control-plane-token");
        std::fs::write(&path, format!("{TOKEN}\n")).unwrap();

        // One gate instance throughout: what is being tested is what it
        // remembers, and a fresh instance remembers nothing by construction.
        let gate = Arc::new(ControlPlaneGate::new(
            Vec::new(),
            path.to_str().expect("temp paths are valid utf-8"),
        ));

        assert_eq!(
            status(gated(Arc::clone(&gate)).await, Method::GET, NODE_PATH, None).await,
            StatusCode::FORBIDDEN
        );

        std::fs::remove_file(&path).unwrap();
        assert_eq!(
            status(gated(Arc::clone(&gate)).await, Method::GET, NODE_PATH, None).await,
            StatusCode::FORBIDDEN,
            "an unreadable credential file must not open the control plane"
        );
        assert_ne!(
            status(
                gated(Arc::clone(&gate)).await,
                Method::GET,
                NODE_PATH,
                Some(TOKEN)
            )
            .await,
            StatusCode::FORBIDDEN,
            "the last credential that was read successfully stays in force"
        );

        std::fs::write(&path, "").unwrap();
        assert_ne!(
            status(gated(gate).await, Method::GET, NODE_PATH, None).await,
            StatusCode::FORBIDDEN
        );
    }

    #[tokio::test]
    async fn the_report_routes_want_an_admin_identity() {
        let router = || async { gated(Arc::new(ControlPlaneGate::new(Vec::new(), ""))).await };
        for path in [NODE_PATH, "/nodes/ip-10-0-1-7"] {
            let response = router()
                .await
                .oneshot(
                    HttpRequest::builder()
                        .method(Method::GET)
                        .uri(path)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(
                response.status(),
                StatusCode::UNAUTHORIZED,
                "{path} must want the same admin identity the generated route wanted"
            );
        }
    }

    const GENERATED_SERVER: &str = include_str!("../../../../src/api/generated/src/server/mod.rs");

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

    #[tokio::test]
    async fn out_of_the_whole_generated_table_this_node_serves_only_its_own_three_routes() {
        let paths = generated_route_paths();
        assert_eq!(
            paths.len(),
            28,
            "the generated route table changed. Partition the new route below and \
             update this count; the routes are {paths:?}"
        );

        let mut served = Vec::new();
        for path in &paths {
            let concrete = concrete(path);
            let mut any = false;
            for method in [Method::GET, Method::POST, Method::DELETE, Method::PUT] {
                let answered = status(
                    gated(Arc::new(ControlPlaneGate::new(Vec::new(), ""))).await,
                    method,
                    &concrete,
                    None,
                )
                .await;
                any |= answered != StatusCode::NOT_FOUND;
            }
            if any {
                served.push(path.as_str());
            }
        }

        assert_eq!(
            served,
            vec!["/health", "/nodes", "/nodes/{node_id}"],
            "a node serves exactly its own report and its health probe; every other route in \
             the document belongs to the api half and must not exist here"
        );
    }

    #[tokio::test]
    async fn the_report_routes_are_served_under_the_methods_they_were_granted() {
        let answered = |method: Method, path: &'static str| async move {
            let router = router_for(
                node_api_reporting_as(Some("n1")).await,
                Arc::new(ControlPlaneGate::new(Vec::new(), "")),
            );
            status(router, method, path, None).await
        };

        assert_eq!(answered(Method::GET, "/nodes").await, StatusCode::OK);
        assert_eq!(
            answered(Method::POST, "/nodes").await,
            StatusCode::METHOD_NOT_ALLOWED
        );
        assert_eq!(answered(Method::GET, "/nodes/n1").await, StatusCode::OK);
        assert_ne!(
            answered(Method::POST, "/nodes/n1").await,
            StatusCode::NOT_FOUND,
            "the preStop hook posts to this to drain the node"
        );
        assert_eq!(
            answered(Method::DELETE, "/nodes/n1").await,
            StatusCode::METHOD_NOT_ALLOWED
        );

        // Only a single segment follows the prefix; anything deeper never
        // reaches a node read.
        assert_eq!(
            answered(Method::GET, "/nodes/n1/sandboxes").await,
            StatusCode::NOT_FOUND
        );
    }

    const DAEMONSET: &str = include_str!("../../../../deploy/k8s/base/agentenv-daemonset.yaml");

    fn prestop_script() -> &'static str {
        DAEMONSET
            .split_once("preStop:")
            .expect("the daemonset has a preStop hook")
            .1
            .split_once("postStart:")
            .expect("preStop is followed by postStart")
            .0
    }

    fn prestop_calls() -> Vec<(Method, String, Option<String>)> {
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
                let admin_token = flags
                    .split_once("X-Admin-Token: ")
                    .map(|(_, rest)| rest.chars().take_while(|c| *c != '\'').collect::<String>());
                Some((method, path, admin_token))
            })
            .collect()
    }

    #[tokio::test]
    async fn every_call_the_prestop_hook_makes_reaches_a_route_this_node_serves() {
        let calls = prestop_calls();
        assert_eq!(
            calls.len(),
            2,
            "the hook drains the node and then waits for it: {calls:?}"
        );

        for (method, path, admin_token) in &calls {
            let mut request = HttpRequest::builder().method(method.clone()).uri(path);
            if let Some(token) = admin_token {
                request = request.header("X-Admin-Token", token.as_str());
            }
            let response = router_for(
                node_api_reporting_as(Some("ip-10-0-1-7")).await,
                Arc::new(ControlPlaneGate::new(Vec::new(), "")),
            )
            .oneshot(
                request
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"status":"draining"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
            assert_ne!(
                response.status(),
                StatusCode::NOT_FOUND,
                "the preStop hook calls {method} {path}, which this node answers with 404. \
                 The hook swallows its own failures, so this does not show up as an error — \
                 it shows up as a pod stuck in Terminating for the whole grace period."
            );
            assert_ne!(
                response.status(),
                StatusCode::UNAUTHORIZED,
                "the preStop hook calls {method} {path} without an identity this node accepts"
            );
        }
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
}
