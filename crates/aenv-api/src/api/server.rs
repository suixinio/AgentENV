use axum::{middleware, routing::get, Router};

use crate::observability::prometheus;
use agentenv_http_server::apis;
use agentenv_observability::metrics_handler;

/// Builds the api half's router, merging `extra_control_plane_routes` beside
/// the generated ones.
///
/// Those extra routes are how a process serves something the OpenAPI document
/// does not describe. They are not counted as part of the user-facing surface
/// and no generated authentication reaches them, so each one carries its own.
///
/// No control-plane credential gate is attached here: clients reach this
/// surface directly and no caller stamps an internal header for them. The gate
/// belongs to the half whose control-plane routes are only ever called by this
/// one.
pub fn new_control_plane_only<I, A, E, C>(api_impl: I, extra_control_plane_routes: Router) -> Router
where
    I: AsRef<A> + Clone + Send + Sync + 'static,
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
    agentenv_http_server::server::new::<I, A, E, C>(api_impl)
        .merge(extra_control_plane_routes)
        .route("/metrics", get(metrics_handler))
        .layer(middleware::from_fn(prometheus::http_metrics_middleware))
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use axum::body::Body;
    use axum::http::{header, Method, Request as HttpRequest, StatusCode};
    use tower::ServiceExt;

    use super::super::ApiImpl;

    async fn api_half() -> Router {
        api_half_with(Router::new()).await
    }

    async fn api_half_with(extra_control_plane_routes: Router) -> Router {
        let orchestrator = aenv_node::orchestrator::Orchestrator::with_in_memory_store(
            crate::sandbox::mock::MockBackendFactory::new(),
        )
        .await;
        let api_impl = Arc::new(ApiImpl::new(
            orchestrator,
            Arc::new(crate::snapshot::mock::mock_snapshot_manager()),
            None,
            Vec::new(),
            crate::api::ResumeWiring::api_half_for_test(),
        ));
        new_control_plane_only(api_impl, extra_control_plane_routes)
    }

    async fn status(router: Router, method: Method, path: &str) -> StatusCode {
        router
            .oneshot(
                HttpRequest::builder()
                    .method(method)
                    .uri(path)
                    .header("x-api-key", "test-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
            .status()
    }

    #[tokio::test]
    async fn the_user_facing_surface_answers_without_an_internal_credential() {
        for (method, path) in [
            (Method::GET, "/sandboxes"),
            (Method::POST, "/sandboxes"),
            (Method::GET, "/v2/sandboxes"),
        ] {
            let answered = status(api_half().await, method.clone(), path).await;
            assert_ne!(
                answered,
                StatusCode::FORBIDDEN,
                "a client reaching the REST surface directly stamps no internal header; \
                 {method} {path} must not be refused for the lack of one"
            );
            assert_ne!(
                answered,
                StatusCode::NOT_FOUND,
                "the probe has resolution: {method} {path} is a route this half serves"
            );
        }
    }

    #[tokio::test]
    async fn this_half_carries_no_sandbox_data_plane() {
        assert_eq!(
            status(api_half().await, Method::GET, "/proxy/hello").await,
            StatusCode::NOT_FOUND,
            "the api half must not carry a /proxy route at all"
        );

        let via_sandbox_host = api_half()
            .await
            .oneshot(
                HttpRequest::builder()
                    .method(Method::GET)
                    .uri("/not-a-control-plane-route")
                    .header(header::HOST, "8080-sandbox.data-plane.example.invalid")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
            .status();
        assert_eq!(
            via_sandbox_host,
            StatusCode::NOT_FOUND,
            "the api half runs no sandbox host classifier: with no /proxy routes to rewrite \
             into, the only thing it could produce is the 404 the router already had"
        );
    }

    #[tokio::test]
    async fn an_extra_route_is_served_beside_the_generated_ones() {
        // The one route this half mounts that the OpenAPI document does not
        // describe carries its own credential layer; the composition must not
        // be what decides whether it is reachable at all.
        const EXTRA: &str = "/internal/example";
        let router =
            api_half_with(Router::new().route(EXTRA, axum::routing::post(|| async { "answered" })))
                .await;
        assert_eq!(
            status(router, Method::POST, EXTRA).await,
            StatusCode::OK,
            "a route merged beside the generated ones must be reachable through the same \
             composition"
        );
        assert_eq!(
            status(api_half().await, Method::POST, EXTRA).await,
            StatusCode::NOT_FOUND,
            "the probe has resolution: without the extra route the same call is a 404"
        );
    }

    #[tokio::test]
    async fn metrics_are_served_beside_the_generated_routes() {
        assert_ne!(
            status(api_half().await, Method::GET, "/metrics").await,
            StatusCode::NOT_FOUND,
            "the scrape endpoint must exist beside the generated routes"
        );
    }
}
