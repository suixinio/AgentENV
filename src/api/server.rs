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

    // Keep the generated control-plane API as the primary router, then merge in
    // the hand-written `/proxy/*` entrypoints needed for the temporary reverse
    // proxy contract.
    assemble(
        agentenv_http_server::server::new::<I, A, E, C>(api_impl.clone()),
        proxy::router(api_impl.clone()),
        Arc::new(ControlPlaneGate::from_global_config()),
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
