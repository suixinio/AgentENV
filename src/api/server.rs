use std::sync::Arc;

use axum::{middleware, routing::get, Router};

use super::control_plane_gate::{require_control_plane, ControlPlaneGate};
use super::{isolation, proxy, ApiImpl};
use crate::observability::prometheus;
use agentenv_http_server::apis;
use agentenv_observability::metrics_handler;

pub fn new<I, A, E, C>(api_impl: I) -> Router
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
    // Keep the generated control-plane API as the primary router, then merge in
    // the hand-written `/proxy/*` entrypoints needed for the temporary reverse
    // proxy contract.
    assemble(
        agentenv_http_server::server::new::<I, A, E, C>(api_impl.clone()),
        proxy::router(api_impl.clone()),
        Arc::new(ControlPlaneGate::from_global_config()),
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
fn assemble(generated: Router, data_plane: Router, gate: Arc<ControlPlaneGate>) -> Router {
    generated
        .layer(middleware::from_fn_with_state(gate, require_control_plane))
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
        assemble(
            stand_in_control_plane(),
            stand_in_data_plane(),
            Arc::new(ControlPlaneGate::new(tokens, token_file)),
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
    /// all-or-nothing, so one refusal is a 502 for the whole cluster.
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
