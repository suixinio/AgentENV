use std::sync::Arc;

use axum::{middleware, routing::get, Router};

use super::control_plane_gate::{require_control_plane, ControlPlaneGate};
use super::role_gate;
use super::ApiImpl;
use crate::observability::prometheus;
use agentenv_http_server::apis;
use agentenv_observability::metrics_handler;

/// The local sandbox HTTP data plane a process mounts: the routes that carry
/// sandbox traffic, and the host classifier that rewrites host-named traffic
/// onto them.
///
/// The routes are the running half's; this is the shape the composition needs
/// of them. The classifier is applied where it was composed, inside the
/// metrics layer, so what the metrics see is the classified request.
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

/// Builds the node router with generated control plane and the caller's local
/// sandbox data plane.
pub fn new<I, A, E, C>(api_impl: I, data_plane: DataPlane) -> Router
where
    I: AsRef<A> + AsRef<ApiImpl> + Clone + Send + Sync + 'static,
    A: apis::admin::Admin<E, Claims = C>
        + apis::default::Default<E>
        + apis::sandboxes::Sandboxes<E, Claims = C>
        + apis::secrets::Secrets<E, Claims = C>
        + apis::snapshots::Snapshots<E, Claims = C>
        + apis::templates::Templates<E, Claims = C>
        + apis::ApiKeyAuthHeader<Claims = C>
        + apis::ApiAuthBasic<Claims = C>
        + Send
        + Sync
        + 'static,
    E: std::fmt::Debug + Send + Sync + 'static,
    C: Send + Sync + 'static,
{
    compose::<I, A, E, C>(
        api_impl,
        Some(data_plane),
        Router::new(),
        Arc::new(ControlPlaneGate::from_global_config()),
    )
}

/// Builds the API router without a local sandbox data plane or host
/// classifier, merging `extra_control_plane_routes` beside the generated
/// ones.
///
/// Those extra routes are how a process serves something the OpenAPI document
/// does not describe. They are not counted as part of the user-facing surface
/// and no generated authentication reaches them, so each one carries its own.
pub fn new_control_plane_only<I, A, E, C>(api_impl: I, extra_control_plane_routes: Router) -> Router
where
    I: AsRef<A> + AsRef<ApiImpl> + Clone + Send + Sync + 'static,
    A: apis::admin::Admin<E, Claims = C>
        + apis::default::Default<E>
        + apis::sandboxes::Sandboxes<E, Claims = C>
        + apis::secrets::Secrets<E, Claims = C>
        + apis::snapshots::Snapshots<E, Claims = C>
        + apis::templates::Templates<E, Claims = C>
        + apis::ApiKeyAuthHeader<Claims = C>
        + apis::ApiAuthBasic<Claims = C>
        + Send
        + Sync
        + 'static,
    E: std::fmt::Debug + Send + Sync + 'static,
    C: Send + Sync + 'static,
{
    compose::<I, A, E, C>(
        api_impl,
        None,
        extra_control_plane_routes,
        Arc::new(ControlPlaneGate::from_global_config()),
    )
}

/// Composes generated and extra control-plane routes before applying gates,
/// then merges the independently selected data plane.
fn compose<I, A, E, C>(
    api_impl: I,
    data_plane: Option<DataPlane>,
    extra_control_plane_routes: Router,
    gate: Arc<ControlPlaneGate>,
) -> Router
where
    I: AsRef<A> + AsRef<ApiImpl> + Clone + Send + Sync + 'static,
    A: apis::admin::Admin<E, Claims = C>
        + apis::default::Default<E>
        + apis::sandboxes::Sandboxes<E, Claims = C>
        + apis::secrets::Secrets<E, Claims = C>
        + apis::snapshots::Snapshots<E, Claims = C>
        + apis::templates::Templates<E, Claims = C>
        + apis::ApiKeyAuthHeader<Claims = C>
        + apis::ApiAuthBasic<Claims = C>
        + Send
        + Sync
        + 'static,
    E: std::fmt::Debug + Send + Sync + 'static,
    C: Send + Sync + 'static,
{
    // User-facing route ownership comes from the API implementation itself.
    let serves_user_facing_rest = AsRef::<ApiImpl>::as_ref(&api_impl).owns_sandboxes();

    // An empty data-plane router contributes neither routes nor fallback.
    let (mounted_data_plane, classify) = match data_plane {
        Some(DataPlane { routes, classify }) => (routes, Some(classify)),
        None => (Router::new(), None),
    };

    // Merge all control-plane routes before gates, then merge the data plane.
    let router = assemble(
        agentenv_http_server::server::new::<I, A, E, C>(api_impl.clone())
            .merge(extra_control_plane_routes),
        mounted_data_plane,
        gate,
        serves_user_facing_rest,
    )
    .route("/metrics", get(metrics_handler));

    // Attach host classification only when its proxy routes exist.
    let router = match classify {
        Some(classify) => classify(router),
        None => router,
    };

    router.layer(middleware::from_fn(prometheus::http_metrics_middleware))
}

/// Applies credential then role layers to control-plane routes before merging
/// the ungated data plane; layer order makes role refusal run first.
///
/// The credential gate is attached on the half that does not own user-facing
/// REST. On the half that does, clients reach the REST surface directly and no
/// caller stamps an internal header for them.
fn assemble(
    generated: Router,
    data_plane: Router,
    gate: Arc<ControlPlaneGate>,
    serves_user_facing_rest: bool,
) -> Router {
    let generated = if serves_user_facing_rest {
        generated
    } else {
        generated.layer(middleware::from_fn_with_state(gate, require_control_plane))
    };

    role_gate::attach(generated, serves_user_facing_rest).merge(data_plane)
}

#[cfg(test)]
mod tests {
    use super::*;

    use axum::body::Body;
    use axum::extract::Request as AxumRequest;
    use axum::http::{header, Method, Request as HttpRequest, StatusCode};
    use axum::middleware::Next;
    use axum::response::{IntoResponse, Response as AxumResponse};
    use axum::routing::{any, get, post};
    use tower::ServiceExt;

    use super::super::control_plane_gate::CONTROL_PLANE_HEADER;

    const TOKEN: &str = "control-plane-token";

    /// Two answers nothing else in the composition produces, so mounting the
    /// data plane is observable and leaving it out is too. The routes the
    /// running half actually mounts are `aenv-node`'s.
    const DATA_PLANE_ROUTE: StatusCode = StatusCode::IM_A_TEAPOT;
    const DATA_PLANE_CLASSIFIER: StatusCode = StatusCode::MISDIRECTED_REQUEST;
    const CLASSIFIED_HOST: &str = "8080-sandbox.data-plane.example.invalid";

    async fn classify_stand_in_host(request: AxumRequest, next: Next) -> AxumResponse {
        let host = request
            .headers()
            .get(header::HOST)
            .and_then(|host| host.to_str().ok());
        if host == Some(CLASSIFIED_HOST) {
            return DATA_PLANE_CLASSIFIER.into_response();
        }
        next.run(request).await
    }

    fn stand_in_data_plane_mount() -> DataPlane {
        let routes = Router::new()
            .route("/proxy", any(|| async { DATA_PLANE_ROUTE }))
            .route("/proxy/{*rest}", any(|| async { DATA_PLANE_ROUTE }))
            .fallback(any(|| async { DATA_PLANE_ROUTE }));
        DataPlane::new(routes, |router| {
            router.layer(middleware::from_fn(classify_stand_in_host))
        })
    }

    fn stand_in_control_plane() -> Router {
        Router::new()
            .route("/health", get(|| async { "ok" }))
            .route("/nodes", get(|| async { "described" }))
            .route("/sandboxes", get(|| async { "listed" }))
            .route("/sandboxes", post(|| async { "created" }))
            .route("/v2/sandboxes", get(|| async { "listed" }))
            .route("/sandboxes/{id}/pause", post(|| async { "paused" }))
    }

    fn stand_in_data_plane() -> Router {
        Router::new()
            .route("/proxy/{*rest}", get(|| async { "proxied" }))
            .fallback(get(|| async { "fallback" }))
    }

    /// The half the credential gate is attached on.
    fn gated(tokens: Vec<String>, token_file: &str) -> Router {
        assemble_as(REFUSES_USER_REST, tokens, token_file)
    }

    const SERVES_USER_REST: bool = true;
    const REFUSES_USER_REST: bool = false;

    /// A route the gated half both serves and gates.
    const NODE_PATH: &str = "/nodes";

    fn assemble_as(serves_user_facing_rest: bool, tokens: Vec<String>, token_file: &str) -> Router {
        assemble(
            stand_in_control_plane(),
            stand_in_data_plane(),
            Arc::new(ControlPlaneGate::new(tokens, token_file)),
            serves_user_facing_rest,
        )
    }

    async fn status(router: Router, method: Method, path: &str, token: Option<&str>) -> StatusCode {
        let mut request = HttpRequest::builder().method(method).uri(path);
        if let Some(token) = token {
            request = request.header(CONTROL_PLANE_HEADER, token);
        }

        router
            .oneshot(request.body(Body::empty()).unwrap())
            .await
            .unwrap()
            .status()
    }

    #[tokio::test]
    async fn the_gate_covers_the_control_plane_router_and_nothing_merged_after_it() {
        // The probe has resolution: a gated route with no credential is refused.
        assert_eq!(
            status(
                gated(vec![TOKEN.to_string()], ""),
                Method::GET,
                NODE_PATH,
                None
            )
            .await,
            StatusCode::FORBIDDEN
        );

        // ...and therefore these two mean the data plane is not behind it.
        assert_ne!(
            status(
                gated(vec![TOKEN.to_string()], ""),
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
                gated(vec![TOKEN.to_string()], ""),
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
    async fn a_route_merged_before_assemble_is_gated_and_the_same_route_added_after_is_not() {
        // A path the role gate serves, so the credential gate is what answers.
        const EXTRA: &str = "/nodes/example-debug";
        let extra_route = || Router::new().route(EXTRA, get(|| async { "debug" }));

        let merged_before_assemble = assemble(
            stand_in_control_plane().merge(extra_route()),
            stand_in_data_plane(),
            Arc::new(ControlPlaneGate::new(vec![TOKEN.to_string()], "")),
            REFUSES_USER_REST,
        );
        assert_eq!(
            status(merged_before_assemble, Method::GET, EXTRA, None).await,
            StatusCode::FORBIDDEN,
            "merged into the generated router before assemble runs, this route must be gated \
             exactly like every other control-plane route"
        );

        // The control: the same route, added the way the original bug added
        // it — after assemble's own return value — is not gated. If this
        // assertion ever starts failing, axum's layering semantics changed
        // underneath this file and the argument above no longer holds.
        let added_after_assemble = assemble(
            stand_in_control_plane(),
            stand_in_data_plane(),
            Arc::new(ControlPlaneGate::new(vec![TOKEN.to_string()], "")),
            REFUSES_USER_REST,
        )
        .route(EXTRA, get(|| async { "debug" }));
        assert_ne!(
            status(added_after_assemble, Method::GET, EXTRA, None).await,
            StatusCode::FORBIDDEN,
            "the control: a route added after assemble's own return must not be covered by its \
             layer — this is the bug the assertion above is the fix for"
        );
    }

    #[tokio::test]
    async fn only_the_half_that_does_not_own_user_facing_rest_carries_the_credential_gate() {
        for (method, path) in [
            (Method::GET, "/sandboxes"),
            (Method::POST, "/sandboxes"),
            (Method::GET, "/v2/sandboxes"),
            (
                Method::POST,
                "/sandboxes/0199c9a1-4f2e-7c31-a0b4-6d5e8f2a1c07/pause",
            ),
        ] {
            assert_eq!(
                status(
                    assemble_as(SERVES_USER_REST, vec![TOKEN.to_string()], ""),
                    method.clone(),
                    path,
                    None
                )
                .await,
                StatusCode::OK,
                "a client reaching the REST surface directly stamps no internal header; \
                 {method} {path} must not be refused for the lack of one"
            );
        }

        // The probe has resolution: the same credential, on the half that does
        // carry the gate, still refuses a call that presents nothing.
        assert_eq!(
            status(
                assemble_as(REFUSES_USER_REST, vec![TOKEN.to_string()], ""),
                Method::GET,
                NODE_PATH,
                None
            )
            .await,
            StatusCode::FORBIDDEN
        );
    }

    #[test]
    fn extra_control_plane_routes_are_merged_before_assemble_is_called() {
        let source = include_str!("server.rs");
        let start = source
            .find("fn compose<")
            .expect("compose is no longer in this file");
        let open = source[start..].find('{').expect("a body") + start;
        let mut depth = 0usize;
        let mut body = "";
        for (offset, byte) in source[open..].bytes().enumerate() {
            match byte {
                b'{' => depth += 1,
                b'}' => {
                    depth -= 1;
                    if depth == 0 {
                        body = &source[open..open + offset];
                        break;
                    }
                }
                _ => {}
            }
        }
        assert!(!body.is_empty(), "compose has no closing brace");

        let assemble_call = body
            .find("assemble(")
            .expect("assemble is called in this function");
        let merge_call = body
            .find(".merge(extra_control_plane_routes)")
            .expect("extra_control_plane_routes must be merged somewhere in this function");
        assert!(
            merge_call > assemble_call,
            "the merge must appear at or after `assemble(` starts — it belongs among assemble's \
             own arguments"
        );

        // `assemble(...)`'s own matching close paren, scanned from `assemble(`'s
        // own opening paren rather than the whole body — a naive search for
        // the first `)` would stop inside an earlier argument's own parens.
        let paren_open = assemble_call + "assemble(".len() - 1;
        let mut paren_depth = 0i32;
        let mut assemble_close = None;
        for (offset, byte) in body[paren_open..].bytes().enumerate() {
            match byte {
                b'(' => paren_depth += 1,
                b')' => {
                    paren_depth -= 1;
                    if paren_depth == 0 {
                        assemble_close = Some(paren_open + offset);
                        break;
                    }
                }
                _ => {}
            }
        }
        let assemble_close = assemble_close.expect("assemble(...) has a matching close paren");

        assert!(
            merge_call < assemble_close,
            "extra_control_plane_routes must be merged *inside* the call to assemble — into the \
             `generated` router argument, before the control-plane gate and role gate are \
             attached — not chained onto assemble's return value. Chaining it after is exactly \
             the bug the deleted /debug/node-registry originally had: a route added after every \
             layer runs is a route no layer ever covers."
        );
    }

    async fn build_api_impl_for_gate_test() -> Arc<ApiImpl> {
        let orchestrator = crate::orchestrator::Orchestrator::with_in_memory_store(
            crate::sandbox::mock::MockBackendFactory::new(),
        )
        .await;
        let snapshot_manager = Arc::new(crate::snapshot::mock::mock_snapshot_manager());
        Arc::new(ApiImpl::new(
            orchestrator,
            snapshot_manager,
            None,
            Vec::new(),
            crate::api::ResumeWiring::api_half_for_test(),
        ))
    }

    #[tokio::test]
    async fn compose_leaves_the_user_facing_rest_half_ungated() {
        let api_impl = build_api_impl_for_gate_test().await;
        let gate = Arc::new(ControlPlaneGate::new(vec![TOKEN.to_string()], ""));
        let sandbox_path = "/sandboxes/0199c9a1-4f2e-7c31-a0b4-6d5e8f2a1c07/pause";

        for mounts_data_plane in [true, false] {
            let router = || {
                compose(
                    Arc::clone(&api_impl),
                    mounts_data_plane.then(stand_in_data_plane_mount),
                    Router::new(),
                    Arc::clone(&gate),
                )
            };

            assert_ne!(
                status(router(), Method::POST, sandbox_path, None).await,
                StatusCode::FORBIDDEN,
                "this impl owns user-facing REST, so a configured credential must gate \
                 nothing on it (data plane mounted: {mounts_data_plane})"
            );
            assert_ne!(
                status(router(), Method::GET, "/health", None).await,
                StatusCode::FORBIDDEN,
                "/health must stay ungated regardless (data plane mounted: {mounts_data_plane})"
            );
        }
    }

    #[tokio::test]
    async fn the_sandbox_data_plane_is_mounted_only_where_sandboxes_run() {
        let api_impl = build_api_impl_for_gate_test().await;
        // An explicitly-off gate: what is under test here is which routes and
        // layers the composition carries, and a credential check answering
        // first would mask exactly that.
        let router = |mounted: bool| {
            compose(
                Arc::clone(&api_impl),
                mounted.then(stand_in_data_plane_mount),
                Router::new(),
                Arc::new(ControlPlaneGate::new(Vec::new(), "")),
            )
        };

        // 1. The `/proxy/*` entrypoints. The data plane's own handler answers,
        //    or the route is not there at all.
        assert_eq!(
            status(router(true), Method::GET, "/proxy/hello", None).await,
            DATA_PLANE_ROUTE,
            "aenv-node must serve /proxy/* from the data plane's own handler"
        );
        assert_eq!(
            status(router(false), Method::GET, "/proxy/hello", None).await,
            StatusCode::NOT_FOUND,
            "aenv-api must not carry a /proxy route at all"
        );

        // 2. The host classifier, which only the mounted data plane brings
        //    with it. Without the layer the same request is just an unmatched
        //    path.
        let via_host = |mounted: bool| {
            let router = router(mounted);
            async move {
                router
                    .oneshot(
                        HttpRequest::builder()
                            .method(Method::GET)
                            .uri("/not-a-control-plane-route")
                            .header(header::HOST, CLASSIFIED_HOST)
                            .body(Body::empty())
                            .unwrap(),
                    )
                    .await
                    .unwrap()
                    .status()
            }
        };
        assert_eq!(
            via_host(true).await,
            DATA_PLANE_CLASSIFIER,
            "aenv-node must run the data plane's host classifier"
        );
        assert_eq!(
            via_host(false).await,
            StatusCode::NOT_FOUND,
            "aenv-api must not run the sandbox host classifier: with no /proxy routes to \
             rewrite into, the only thing it can produce is a 404 the router already had"
        );

        // 3. The control group: the port is not what was taken away. Asserted
        //    as "the same answer on both halves" rather than against a fixed
        //    status, because what matters is that dropping the data plane
        //    changed nothing outside it — and a generated route answering the
        //    same way on both is exactly that.
        let health = |mounted| status(router(mounted), Method::GET, "/health", None);
        let health_on_node = health(true).await;
        assert_ne!(
            health_on_node,
            StatusCode::NOT_FOUND,
            "kubelet's probe must exist at all, or the comparison below is vacuous"
        );
        assert_eq!(
            health(false).await,
            health_on_node,
            "the generated routes must answer identically with and without the data plane"
        );

        // 4. ...and the two public entry points differ the same way, which is
        //    the only thing that makes any of the above a fact about the two
        //    binaries rather than about `compose`'s parameter. `new` takes a
        //    data plane and mounts it; `new_control_plane_only` has no
        //    parameter that could carry one.
        assert_eq!(
            status(
                new(Arc::clone(&api_impl), stand_in_data_plane_mount()),
                Method::GET,
                "/proxy/hello",
                None
            )
            .await,
            DATA_PLANE_ROUTE,
            "server::new — what aenv-node calls — must mount the data plane it is handed"
        );
        assert_eq!(
            status(
                new_control_plane_only(Arc::clone(&api_impl), Router::new()),
                Method::GET,
                "/proxy/hello",
                None
            )
            .await,
            StatusCode::NOT_FOUND,
            "server::new_control_plane_only — what aenv-api calls — must not"
        );
    }

    #[tokio::test]
    async fn the_role_gate_answers_before_the_control_plane_gate_does() {
        let node = || assemble_as(REFUSES_USER_REST, vec![TOKEN.to_string()], "");
        let sandbox_path = "/sandboxes/0199c9a1-4f2e-7c31-a0b4-6d5e8f2a1c07/pause";

        for presented in [None, Some(TOKEN), Some("wrong")] {
            assert_eq!(
                status(node(), Method::POST, sandbox_path, presented).await,
                StatusCode::NOT_FOUND,
                "a node must refuse a user-facing route as absent, not as forbidden, \
                 whatever credential is presented ({presented:?})"
            );
        }

        // The control face: the same assembly still runs the control-plane gate
        // on the routes a node does serve, so the 404s above are the role gate
        // answering rather than the control-plane gate having been dropped.
        assert_eq!(
            status(node(), Method::GET, "/health", None).await,
            StatusCode::OK,
            "kubelet's probe stays reachable on a node"
        );
        assert_eq!(
            status(node(), Method::GET, NODE_PATH, None).await,
            StatusCode::FORBIDDEN,
            "a route the node does serve is still gated on the credential, which is what makes \
             the 404s above a statement about the role"
        );
    }

    /// T-A4-2. The credential is checked, not merely counted.
    #[tokio::test]
    async fn a_call_without_the_control_plane_credential_is_refused() {
        assert_eq!(
            status(
                gated(vec![TOKEN.to_string()], ""),
                Method::GET,
                NODE_PATH,
                Some(TOKEN)
            )
            .await,
            StatusCode::OK
        );
        for presented in [None, Some(""), Some("wrong"), Some("CONTROL-PLANE-TOKEN")] {
            assert_eq!(
                status(
                    gated(vec![TOKEN.to_string()], ""),
                    Method::GET,
                    NODE_PATH,
                    presented
                )
                .await,
                StatusCode::FORBIDDEN,
                "presented credential {presented:?} must not be accepted"
            );
        }
    }

    /// T-A4-3. The rollback path, exercised.
    ///
    /// No credential configured means the node behaves exactly as it did before
    /// the gate existed. This is the whole rollback plan, so it is a test rather
    /// than a claim.
    #[tokio::test]
    async fn an_empty_configured_token_lets_everything_through() {
        assert_eq!(
            status(gated(Vec::new(), ""), Method::GET, NODE_PATH, None).await,
            StatusCode::OK
        );
    }

    /// T-A4-4. kubelet has no credential and does not go through the gateway.
    /// Gating `/health` stops the pod becoming ready.
    #[tokio::test]
    async fn health_is_never_gated() {
        assert_ne!(
            status(
                gated(vec![TOKEN.to_string()], ""),
                Method::GET,
                "/health",
                None
            )
            .await,
            StatusCode::FORBIDDEN
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
        let router = || {
            assemble(
                stand_in_control_plane(),
                stand_in_data_plane(),
                Arc::clone(&gate),
                REFUSES_USER_REST,
            )
        };

        // Nothing mounted yet: the node behaves as it did before the gate.
        assert_eq!(
            status(router(), Method::GET, NODE_PATH, None).await,
            StatusCode::OK
        );

        // The operator writes the Secret.
        std::fs::write(&path, format!("{TOKEN}\n")).unwrap();
        assert_eq!(
            status(router(), Method::GET, NODE_PATH, None).await,
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            status(router(), Method::GET, NODE_PATH, Some(TOKEN)).await,
            StatusCode::OK
        );

        // ...and clears it again to roll back. An empty file is a successful
        // read of zero credentials, which is the deliberate off switch.
        std::fs::write(&path, "").unwrap();
        assert_eq!(
            status(router(), Method::GET, NODE_PATH, None).await,
            StatusCode::OK
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
        let router = || {
            assemble(
                stand_in_control_plane(),
                stand_in_data_plane(),
                Arc::clone(&gate),
                REFUSES_USER_REST,
            )
        };

        assert_eq!(
            status(router(), Method::GET, NODE_PATH, None).await,
            StatusCode::FORBIDDEN
        );

        // The file goes away — a volume swap mid-flight, or a bad mount.
        std::fs::remove_file(&path).unwrap();
        assert_eq!(
            status(router(), Method::GET, NODE_PATH, None).await,
            StatusCode::FORBIDDEN,
            "an unreadable credential file must not open the control plane"
        );
        assert_eq!(
            status(router(), Method::GET, NODE_PATH, Some(TOKEN)).await,
            StatusCode::OK,
            "the last credential that was read successfully stays in force"
        );

        // Writing it back empty is the deliberate way to turn the gate off.
        std::fs::write(&path, "").unwrap();
        assert_eq!(
            status(router(), Method::GET, NODE_PATH, None).await,
            StatusCode::OK
        );
    }
}
