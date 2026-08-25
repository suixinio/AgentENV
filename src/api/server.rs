use std::sync::Arc;

use axum::{middleware, routing::get, Router};

use super::control_plane_gate::{require_control_plane, ControlPlaneGate};
use super::role_gate;
use super::{isolation, proxy, ApiImpl};
use crate::observability::prometheus;
use crate::role::ServerRole;
use agentenv_http_server::apis;
use agentenv_observability::metrics_handler;

/// Builds the router this process serves.
///
/// 🔴 `role` is a parameter rather than something read from a global because
/// the failure mode of getting it wrong is a layer that is silently absent —
/// a node serving the full user-facing REST surface, which is exactly the
/// state `--role node` exists to end. Every caller has to say which half it is
/// assembling.
pub fn new<I, A, E, C>(api_impl: I, role: ServerRole) -> Router
where
    I: AsRef<A> + AsRef<ApiImpl> + Clone + Send + Sync + 'static,
    A: apis::admin::Admin<E, Claims = C>
        + apis::default::Default<E>
        + apis::sandboxes::Sandboxes<E, Claims = C>
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
    new_with_control_plane_routes::<I, A, E, C>(api_impl, role, Router::new())
}

/// Same as [`new`], plus `extra_control_plane_routes` merged into the
/// *generated* control-plane router before the control-plane gate and role
/// gate are attached — so anything registered on it is covered by both, the
/// same as every generated route.
///
/// 🔴 P5: `src/bin/server.rs`'s `assemble_api` is the one caller that needs
/// this — `/debug/node-registry`, Stage A's equivalence-dump debug endpoint
/// (`node_registry::dump`'s own module doc), used to be `.route(...)`-ed onto
/// the `Router` *this function itself returns*, i.e. after every layer
/// `new`'s body below applies. axum's `Router::layer` — and, transitively,
/// `require_control_plane`/the role gate wired through `assemble` — only
/// ever covers routes registered before the `.layer` call that attaches it;
/// a `.route` added to the router `new` hands back is a route the control
/// -plane gate has never seen. `GET /sandboxes` needs a credential;
/// `/debug/node-registry` — which answers every node's internal address, its
/// resource allocation and the cluster's CPU-config intersection — did not.
/// It is also reachable through the gateway: `hasSandbox=false` there routes
/// unrecognized paths to the api half's REST surface exactly like a genuine
/// user-facing route (`services/gateway/internal/server.go`), unlike
/// `/metrics`, which the gateway 404s on its public listener explicitly.
///
/// 🔴 Describing this as zero-impact "by default" is not accurate and should
/// not be repeated: `/debug/node-registry` did not exist before Stage A
/// added it, so shipping it is one more reachable endpoint on this process's
/// default HTTP surface regardless of gating, full stop. What moving it here
/// changes is that a deployment that *has* configured a control-plane token
/// (`ControlPlaneGate::from_global_config`) now actually needs it for this
/// route too, matching every other control-plane route — a deployment
/// running with the gate off (`an_empty_configured_token_lets_everything_through`'s
/// own rollback state) answers it exactly as unauthenticated as it did
/// before this change, because an off gate opens everything on this router,
/// not merely this one route.
pub fn new_with_control_plane_routes<I, A, E, C>(
    api_impl: I,
    role: ServerRole,
    extra_control_plane_routes: Router,
) -> Router
where
    I: AsRef<A> + AsRef<ApiImpl> + Clone + Send + Sync + 'static,
    A: apis::admin::Admin<E, Claims = C>
        + apis::default::Default<E>
        + apis::sandboxes::Sandboxes<E, Claims = C>
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
    new_with_control_plane_routes_and_gate::<I, A, E, C>(
        api_impl,
        role,
        extra_control_plane_routes,
        Arc::new(ControlPlaneGate::from_global_config()),
    )
}

/// The actual body of [`new_with_control_plane_routes`], with the gate taken
/// as a parameter rather than built from process-global config.
///
/// 🔴 P5 follow-up. `extra_control_plane_routes_are_merged_before_assemble_is_called`
/// proves the merge call's byte offset falls between `assemble(`'s parens,
/// but `assemble` takes *two* router arguments and the scan cannot tell which
/// one the merge landed on — a merge onto `data_plane` (the argument
/// `assemble`'s own doc says is deliberately never gated) still passes that
/// scan, because it is still textually "inside" the call. That regression is
/// invisible to a source scan by construction; it is not invisible to a real
/// request. Splitting the gate out as a parameter here is what lets a test
/// fire a real HTTP request at this function's actual composition with an
/// injected, request-scoped `ControlPlaneGate` — closing the blind spot
/// without the test having to mutate `ConfigManager`'s process-global state
/// that `ControlPlaneGate::from_global_config` reads.
///
/// Not `pub`: `ControlPlaneGate` does not cross the crate boundary (its
/// defining module is private to `crate::api`), so a function taking one as a
/// parameter cannot be `pub` either without exposing a type `src/bin/server.rs`
/// — a separate crate — cannot name. `new_with_control_plane_routes` stays the
/// only crate-external entry point, unchanged, and forwards here with the
/// default gate.
fn new_with_control_plane_routes_and_gate<I, A, E, C>(
    api_impl: I,
    role: ServerRole,
    extra_control_plane_routes: Router,
    gate: Arc<ControlPlaneGate>,
) -> Router
where
    I: AsRef<A> + AsRef<ApiImpl> + Clone + Send + Sync + 'static,
    A: apis::admin::Admin<E, Claims = C>
        + apis::default::Default<E>
        + apis::sandboxes::Sandboxes<E, Claims = C>
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
    // 🔴 The role now has two carriers: this parameter, which the role gate
    // reads, and the `ApiImpl`, which the data plane's auto-resume arm reads
    // (`crate::api::proxy::resolve_proxy_request`). Every assembly in
    // `src/bin/server.rs` passes one variable to both, and this catches the day
    // one of them stops doing so — a router gated as `node` whose `ApiImpl`
    // still believes it is `all` would refuse user REST while going on waking
    // sandboxes on its own initiative, which is the precise half-landed state
    // `--role node` exists to end, and nothing else would notice.
    debug_assert_eq!(
        role,
        AsRef::<ApiImpl>::as_ref(&api_impl).role(),
        "the role this router is gated on and the role its ApiImpl holds must agree"
    );

    // Keep the generated control-plane API as the primary router, merge the
    // caller's extra gated routes into it *before* `assemble` attaches the
    // control-plane gate and the role gate — so both cover them — then merge
    // in the hand-written `/proxy/*` entrypoints needed for the temporary
    // reverse proxy contract.
    assemble(
        agentenv_http_server::server::new::<I, A, E, C>(api_impl.clone())
            .merge(extra_control_plane_routes),
        proxy::router(api_impl.clone()),
        gate,
        role,
    )
    .route("/metrics", get(metrics_handler))
    // Runs ahead of the generated resume handler: an isolated node answers
    // a resume another node could serve by naming that fact, instead of
    // starting a sandbox it is about to shut down.
    .layer(middleware::from_fn_with_state(
        api_impl.clone(),
        isolation::resume_isolation_gate::<I>,
    ))
    .layer(middleware::from_fn_with_state(
        api_impl,
        proxy::sandbox_proxy_classifier::<I>,
    ))
    .layer(middleware::from_fn(prometheus::http_metrics_middleware))
}

/// Joins the control plane and the data plane, with the control-plane gate on
/// the control plane only.
///
/// 🔴 Split out so the join can be tested. What it relies on is that
/// `Router::merge` keeps each router's own layers rather than applying the
/// outer router's to both — which makes the data plane's exemption a fact about
/// the shape of this expression rather than about anything the gate does. That
/// is the entire reason the gate does not check for `/proxy` itself, and it is
/// a property of axum, not of this crate: if a future version of axum changes
/// it, the data plane silently ends up behind the gate and every sandbox on the
/// node stops answering. `the_gate_covers_the_control_plane_router_and_nothing_merged_after_it`
/// is the only thing standing between that and a very confusing outage.
///
/// 🔴 The role gate is attached *after* the control-plane gate and therefore
/// runs *before* it. Both are on the same router and both refuse; the order
/// decides which refusal a caller sees. A node asked for `POST /sandboxes`
/// without a credential must answer 404 — "no such route here" — and not 403,
/// which would say the route exists and invite a retry with credentials that
/// still would not make it a node's route. `crate::api::role_gate` documents
/// the choice; `the_role_gate_answers_before_the_control_plane_gate_does` is
/// what keeps the two `.layer` calls in this order.
fn assemble(
    generated: Router,
    data_plane: Router,
    gate: Arc<ControlPlaneGate>,
    role: ServerRole,
) -> Router {
    role_gate::attach(
        generated.layer(middleware::from_fn_with_state(gate, require_control_plane)),
        role,
    )
    .merge(data_plane)
}

#[cfg(test)]
mod tests {
    use super::*;

    use axum::body::Body;
    use axum::http::{Method, Request as HttpRequest, StatusCode};
    use axum::routing::{get, post};
    use tower::ServiceExt;

    use super::super::control_plane_gate::CONTROL_PLANE_HEADER;

    const TOKEN: &str = "control-plane-token";

    /// A stand-in for the generated control-plane router.
    ///
    /// Deliberately not the real one: the real one needs the whole `ApiImpl`
    /// and an OpenAPI-shaped auth layer, and what is being tested here is the
    /// shape of the join, not any handler. The paths are the ones the exemption
    /// list cares about plus a route that stands for "everything else".
    fn stand_in_control_plane() -> Router {
        Router::new()
            .route("/health", get(|| async { "ok" }))
            .route("/sandboxes", get(|| async { "listed" }))
            .route("/sandboxes", post(|| async { "created" }))
            .route("/v2/sandboxes", get(|| async { "listed" }))
            .route("/sandboxes/{id}/pause", post(|| async { "paused" }))
    }

    /// A stand-in for the data plane, with the same shape that matters: a
    /// wildcard route plus a fallback.
    fn stand_in_data_plane() -> Router {
        Router::new()
            .route("/proxy/{*rest}", get(|| async { "proxied" }))
            .fallback(get(|| async { "fallback" }))
    }

    fn gated(tokens: Vec<String>, token_file: &str) -> Router {
        assemble_as(ServerRole::All, tokens, token_file)
    }

    fn assemble_as(role: ServerRole, tokens: Vec<String>, token_file: &str) -> Router {
        assemble(
            stand_in_control_plane(),
            stand_in_data_plane(),
            Arc::new(ControlPlaneGate::new(tokens, token_file)),
            role,
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

    /// T-A4-1. 🔴 The one test standing under the whole design.
    ///
    /// What it guards: axum's `Router::merge` keeps each router's own layers rather
    /// than applying the outer one's to both. The gate is attached to the
    /// control-plane router *before* the data plane is merged in, so the data
    /// plane is exempt structurally — nothing in the gate checks for `/proxy`.
    ///
    /// 🔴 If a future axum quietly changes that semantic, the exemption stops
    /// being structural and becomes a coincidence, and the failure mode is that
    /// every sandbox on the node stops answering on the data plane. This test
    /// looks like it is testing a framework, and it is: that is the point. Do
    /// not delete it as redundant.
    ///
    /// It also proves it has teeth before it proves anything else — the first
    /// assertion shows the gate refuses a gated route, so the two that follow
    /// mean something.
    #[tokio::test]
    async fn the_gate_covers_the_control_plane_router_and_nothing_merged_after_it() {
        let sandbox_path = "/sandboxes/0199c9a1-4f2e-7c31-a0b4-6d5e8f2a1c07/pause";

        // The probe has resolution: a gated route with no credential is refused.
        assert_eq!(
            status(
                gated(vec![TOKEN.to_string()], ""),
                Method::POST,
                sandbox_path,
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

    /// 🔴 P5. The mechanism [`new_with_control_plane_routes`] relies on to
    /// fix `/debug/node-registry`'s original exposure: a route merged into
    /// the *generated* router before `assemble` runs is covered by the
    /// control-plane gate; the identical route `.route()`-ed onto
    /// `assemble`'s own return value is not, because `Router::layer` only
    /// ever covers routes registered before it runs. That asymmetry — not
    /// any check this endpoint failed — is why `/debug/node-registry`
    /// answered unauthenticated regardless of a configured control-plane
    /// token before this fix. Proven here on a stand-in so a future
    /// regression in the ordering shows up as a failing unit test rather
    /// than only in a manual check of the real endpoint.
    #[tokio::test]
    async fn a_route_merged_before_assemble_is_gated_and_the_same_route_added_after_is_not() {
        let debug_route = || Router::new().route("/debug/example", get(|| async { "debug" }));

        let merged_before_assemble = assemble(
            stand_in_control_plane().merge(debug_route()),
            stand_in_data_plane(),
            Arc::new(ControlPlaneGate::new(vec![TOKEN.to_string()], "")),
            ServerRole::All,
        );
        assert_eq!(
            status(merged_before_assemble, Method::GET, "/debug/example", None).await,
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
            ServerRole::All,
        )
        .route("/debug/example", get(|| async { "debug" }));
        assert_ne!(
            status(added_after_assemble, Method::GET, "/debug/example", None).await,
            StatusCode::FORBIDDEN,
            "the control: a route added after assemble's own return must not be covered by its \
             layer — this is the bug the assertion above is the fix for"
        );
    }

    /// 🔴 P5, the other half of the guard: the test above proves the
    /// *mechanism* (`assemble` gates whatever `generated` already contains);
    /// this proves `new_with_control_plane_routes_and_gate` — the function
    /// that actually does the composing; `new_with_control_plane_routes` is
    /// now a thin forwarder to it — still hands `assemble` the merged router
    /// rather than merging `extra_control_plane_routes` onto `assemble`'s
    /// return value. Getting that wrong would compile and pass every other
    /// test in this file that predates
    /// `extra_control_plane_routes_require_the_control_plane_credential`
    /// below (none of them exercised a non-empty extra router), silently
    /// reintroducing the exact bug `/debug/node-registry` originally had.
    ///
    /// 🔴 What this scan *cannot* see: `assemble` takes two router arguments,
    /// and a merge onto the wrong one (`data_plane`, deliberately never
    /// gated) is still textually "at or after `assemble(` starts" and still
    /// "inside `assemble(...)`'s matching parens" — every check below would
    /// still pass. That is exactly the blind spot
    /// `extra_control_plane_routes_require_the_control_plane_credential`
    /// exists to close with a real request instead of a source scan; keep
    /// both, not either.
    #[test]
    fn extra_control_plane_routes_are_merged_before_assemble_is_called() {
        let source = include_str!("server.rs");
        let start = source
            .find("fn new_with_control_plane_routes_and_gate")
            .expect("new_with_control_plane_routes_and_gate is no longer in this file");
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
        assert!(
            !body.is_empty(),
            "new_with_control_plane_routes has no closing brace"
        );

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
             the bug /debug/node-registry originally had: a route added after every layer runs \
             is a route no layer ever covers."
        );
    }

    /// A real `ApiImpl`, built the same way `crate::api::proxy`'s own test
    /// module builds one — in-memory metadata store, file-backed persister
    /// over a scratch temp dir, a mock snapshot manager. Needed here (rather
    /// than reusing `proxy`'s copy) because that one is `pub(super)` to
    /// `proxy` and not reachable from this sibling module.
    async fn build_api_impl_for_gate_test() -> Arc<ApiImpl> {
        let root = tempfile::tempdir().unwrap();
        let orchestrator = crate::orchestrator::Orchestrator::new(
            ServerRole::All,
            crate::orchestrator::InMemoryMetadataStore::new(),
            crate::sandbox::FirecrackerSandboxFactory::new(),
            crate::orchestrator::FileBackedSandboxPersister::new_for_test(
                root.path().to_path_buf(),
            ),
        )
        .await
        .unwrap();
        let snapshot_manager = Arc::new(crate::snapshot::mock::mock_snapshot_manager());
        let template_builder = Arc::new(crate::template::TemplateBuilder::new());
        let image_resolver = Arc::new(crate::image::ImageResolver::new(
            &crate::cfg::AppConfig::default(),
        ));
        let identity = crate::identity::NodeIdentity::from_config(&Default::default());
        Arc::new(ApiImpl::new(
            orchestrator,
            Arc::clone(&snapshot_manager),
            template_builder,
            image_resolver,
            None,
            crate::api::PausedSandboxWiring::new(
                Arc::new(crate::orchestrator::DisabledPausedSandboxRegistry),
                snapshot_manager,
                &identity,
            ),
            Vec::new(),
            ServerRole::All,
            crate::api::ResumeWiring::node_local(identity.id),
        ))
    }

    /// A stand-in for `/debug/node-registry`, merged in exactly the way
    /// `assemble_api` (`src/bin/server.rs`) merges the real one.
    fn stand_in_debug_route() -> Router {
        Router::new().route("/debug/example-registry", get(|| async { "debug" }))
    }

    /// 🔴 P5, request-level. Closes the blind spot
    /// `extra_control_plane_routes_are_merged_before_assemble_is_called` cannot
    /// see: that scan only proves the merge call sits textually inside
    /// `assemble(...)`'s parens, and `assemble` takes *two* router arguments —
    /// a merge onto `data_plane` (the one `assemble`'s own doc says is
    /// deliberately never gated) still sits inside those parens and still
    /// passes the scan. A real request against the composed router does not
    /// have that blind spot, which is why this exercises
    /// `new_with_control_plane_routes_and_gate` — the function the bug would
    /// actually live in — with an injected gate, rather than reading source
    /// text or touching `ConfigManager`'s process-global config.
    ///
    /// Four assertions, not one: a debug route wired the intended way must be
    /// gated (this is the fix `/debug/node-registry` needed); an existing
    /// gated route must *stay* gated (so this test cannot pass by the new
    /// composition accidentally opening everything); the correct credential
    /// must reach both; and `/health` must stay reachable regardless, so a
    /// gate that refuses everything cannot pass this either.
    #[tokio::test]
    async fn extra_control_plane_routes_require_the_control_plane_credential() {
        let api_impl = build_api_impl_for_gate_test().await;
        let gate = Arc::new(ControlPlaneGate::new(vec![TOKEN.to_string()], ""));
        let sandbox_path = "/sandboxes/0199c9a1-4f2e-7c31-a0b4-6d5e8f2a1c07/pause";

        let router = || {
            new_with_control_plane_routes_and_gate(
                Arc::clone(&api_impl),
                ServerRole::All,
                stand_in_debug_route(),
                Arc::clone(&gate),
            )
        };

        assert_eq!(
            status(router(), Method::GET, "/debug/example-registry", None).await,
            StatusCode::FORBIDDEN,
            "a route merged in through new_with_control_plane_routes must require the \
             control-plane credential, same as /debug/node-registry"
        );
        assert_eq!(
            status(router(), Method::POST, sandbox_path, None).await,
            StatusCode::FORBIDDEN,
            "regression control: an existing gated route must still be gated"
        );
        assert_ne!(
            status(router(), Method::GET, "/debug/example-registry", Some(TOKEN)).await,
            StatusCode::FORBIDDEN,
            "the correct credential must reach the merged-in debug route"
        );
        assert_ne!(
            status(router(), Method::POST, sandbox_path, Some(TOKEN)).await,
            StatusCode::FORBIDDEN,
            "the correct credential must still reach the pre-existing gated route"
        );
        assert_ne!(
            status(router(), Method::GET, "/health", None).await,
            StatusCode::FORBIDDEN,
            "/health must stay ungated regardless"
        );
    }

    /// 🔴 T-A4-10. The role gate answers before the control-plane gate does.
    ///
    /// Both layers sit on the generated router and both refuse. Under
    /// `--role node` a user-facing route must come back 404 whether or not the
    /// caller has a credential — the route is not part of a node's surface, and
    /// a 403 would say it is, only locked. Getting the two `.layer` calls in the
    /// wrong order does not fail to compile and does not fail any other test
    /// here; it just quietly turns every one of these into a 403.
    ///
    /// Both faces, because a gate that 404s everything would pass the first
    /// three assertions on its own.
    #[tokio::test]
    async fn the_role_gate_answers_before_the_control_plane_gate_does() {
        let node = || assemble_as(ServerRole::Node, vec![TOKEN.to_string()], "");
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
            status(
                assemble_as(ServerRole::All, vec![TOKEN.to_string()], ""),
                Method::POST,
                sandbox_path,
                None
            )
            .await,
            StatusCode::FORBIDDEN,
            "under --role all the same route is still gated on the credential, \
             which is what makes the 404s above a statement about the role"
        );
    }

    /// 🔴 T-A4-11. What `--role node` costs the gateway's cluster listing, said
    /// out loud in the one place that can say it.
    ///
    /// `control_plane_gate::is_exempt` lets `GET /sandboxes` and
    /// `GET /v2/sandboxes` through without a credential because the gateway
    /// fans out to every node with its own HTTP client to build the cluster
    /// -wide list. The role gate runs *first* and refuses both on a node, so on
    /// a `--role node` fleet that fan-out gets the 404s below — and since the
    /// listing is all-or-nothing and the gateway passes a 4xx through verbatim,
    /// the user's `GET /sandboxes` is that same 404.
    ///
    /// That is intended (`_sd-impl-phase3-role.md` §7.4: after phase 2 the list
    /// is one SQL query and the fan-out goes away), and the exemption is left in
    /// place until the fan-out is deleted, in that order — deleting the
    /// exemption first would 403 a fan-out that is still running. The gateway
    /// now skips the fan-out whenever `rest_upstream_addr` is set, which is what
    /// keeps a `--role node` fleet answering this route at all; the empty value
    /// still fans out, so these 404s are what that rollback position costs. This
    /// test is not a preference about either; it is here so that whoever flips a
    /// DaemonSet to `--role node` learns this from a test name rather than from
    /// a 404 on the first listing.
    #[tokio::test]
    async fn a_node_refuses_the_cluster_list_fanout_that_the_control_plane_gate_exempts() {
        for path in ["/sandboxes", "/v2/sandboxes"] {
            assert_eq!(
                status(
                    assemble_as(ServerRole::All, vec![TOKEN.to_string()], ""),
                    Method::GET,
                    path,
                    None
                )
                .await,
                StatusCode::OK,
                "under --role all the fan-out is exempt and reaches the handler: {path}"
            );
            assert_eq!(
                status(
                    assemble_as(ServerRole::Node, vec![TOKEN.to_string()], ""),
                    Method::GET,
                    path,
                    None
                )
                .await,
                StatusCode::NOT_FOUND,
                "under --role node the same fan-out is refused before the exemption \
                 is ever consulted: {path}. Stop the gateway fan-out before rolling \
                 a node to --role node."
            );
        }
    }

    /// T-A4-2. The credential is checked, not merely counted.
    #[tokio::test]
    async fn a_call_without_the_control_plane_credential_is_refused() {
        let path = "/sandboxes/0199c9a1-4f2e-7c31-a0b4-6d5e8f2a1c07/pause";

        assert_eq!(
            status(
                gated(vec![TOKEN.to_string()], ""),
                Method::POST,
                path,
                Some(TOKEN)
            )
            .await,
            StatusCode::OK
        );
        for presented in [None, Some(""), Some("wrong"), Some("CONTROL-PLANE-TOKEN")] {
            assert_eq!(
                status(
                    gated(vec![TOKEN.to_string()], ""),
                    Method::POST,
                    path,
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
            status(
                gated(Vec::new(), ""),
                Method::POST,
                "/sandboxes/0199c9a1-4f2e-7c31-a0b4-6d5e8f2a1c07/pause",
                None
            )
            .await,
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

    /// T-A4-7. 🔴 The cluster listing is a fan-out the gateway makes with its
    /// own client, so it never carries the credential — and it is
    /// all-or-nothing, so one node's refusal is the whole cluster's answer: the
    /// gateway passes a 4xx from any node through verbatim, so a 403 here would
    /// be a 403 on the user's listing.
    ///
    /// The second half is the control group: without it, exempting the entire
    /// `/sandboxes` prefix would pass.
    #[tokio::test]
    async fn the_cluster_list_fanout_is_never_gated() {
        for path in ["/sandboxes", "/v2/sandboxes"] {
            assert_ne!(
                status(gated(vec![TOKEN.to_string()], ""), Method::GET, path, None).await,
                StatusCode::FORBIDDEN,
                "the cluster listing fan-out must stay reachable: {path}"
            );
        }

        assert_eq!(
            status(
                gated(vec![TOKEN.to_string()], ""),
                Method::POST,
                "/sandboxes",
                None
            )
            .await,
            StatusCode::FORBIDDEN,
            "creating a sandbox is not a read and is not exempt"
        );
    }

    /// T-A4-8. 🔴 The credential file is re-read while the process runs.
    ///
    /// This is the only thing making "turn the gate on" a configuration change
    /// instead of a DaemonSet restart, and a restart pauses every sandbox on
    /// the node. Degraded to a startup-only read, the symptom is that an
    /// operator writes the Secret and nothing happens — indistinguishable from
    /// "the volume has not refreshed yet".
    #[tokio::test]
    async fn the_gate_picks_up_a_token_written_after_startup() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("control-plane-token");
        // 🔴 One gate instance for the whole test, deliberately. Building a
        // fresh one per request would let a gate that reads the file exactly
        // once, at construction, pass this test — which is the degradation it
        // exists to catch.
        let gate = Arc::new(ControlPlaneGate::new(
            Vec::new(),
            path.to_str().expect("temp paths are valid utf-8"),
        ));
        let router = || {
            assemble(
                stand_in_control_plane(),
                stand_in_data_plane(),
                Arc::clone(&gate),
                ServerRole::All,
            )
        };
        let sandbox_path = "/sandboxes/0199c9a1-4f2e-7c31-a0b4-6d5e8f2a1c07/pause";

        // Nothing mounted yet: the node behaves as it did before the gate.
        assert_eq!(
            status(router(), Method::POST, sandbox_path, None).await,
            StatusCode::OK
        );

        // The operator writes the Secret.
        std::fs::write(&path, format!("{TOKEN}\n")).unwrap();
        assert_eq!(
            status(router(), Method::POST, sandbox_path, None).await,
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            status(router(), Method::POST, sandbox_path, Some(TOKEN)).await,
            StatusCode::OK
        );

        // ...and clears it again to roll back. An empty file is a successful
        // read of zero credentials, which is the deliberate off switch.
        std::fs::write(&path, "").unwrap();
        assert_eq!(
            status(router(), Method::POST, sandbox_path, None).await,
            StatusCode::OK
        );
    }

    /// T-A4-9. 🔴 A file that cannot be read is not a file that says "no
    /// credentials".
    ///
    /// Degraded to "treat a read error as empty", a single disk hiccup turns
    /// the gate off across the fleet, silently.
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
                ServerRole::All,
            )
        };
        let sandbox_path = "/sandboxes/0199c9a1-4f2e-7c31-a0b4-6d5e8f2a1c07/pause";

        assert_eq!(
            status(router(), Method::POST, sandbox_path, None).await,
            StatusCode::FORBIDDEN
        );

        // The file goes away — a volume swap mid-flight, or a bad mount.
        std::fs::remove_file(&path).unwrap();
        assert_eq!(
            status(router(), Method::POST, sandbox_path, None).await,
            StatusCode::FORBIDDEN,
            "an unreadable credential file must not open the control plane"
        );
        assert_eq!(
            status(router(), Method::POST, sandbox_path, Some(TOKEN)).await,
            StatusCode::OK,
            "the last credential that was read successfully stays in force"
        );

        // Writing it back empty is the deliberate way to turn the gate off.
        std::fs::write(&path, "").unwrap();
        assert_eq!(
            status(router(), Method::POST, sandbox_path, None).await,
            StatusCode::OK
        );
    }
}
