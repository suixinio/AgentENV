use axum::{middleware, routing::get, Router};

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
    agentenv_http_server::server::new::<I, A, E, C>(api_impl.clone())
        .merge(isolation::router(api_impl.clone()))
        .merge(proxy::router(api_impl.clone()))
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
