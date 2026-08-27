//! A fake OCI registry, and the client that talks to it.
//!
//! # 🔴 Not `#[cfg(test)]`, and not shipped either
//!
//! `aenv-node`'s `acr::publisher` tests drive this exact fake server: they
//! publish a snapshot's disk image and then assert what the registry received.
//! Those tests live in another crate now, where this crate's `cfg(test)` is
//! off, so the scaffolding cannot be `#[cfg(test)]` — and it must not be
//! unconditional either, or an axum router full of registry handlers ends up
//! in the shipped binary. It is behind `feature = "test-support"`, which only
//! the sibling crates' `[dev-dependencies]` turn on.

use std::sync::{Arc, Mutex};

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap as AxumHeaderMap, HeaderValue, StatusCode as AxumStatusCode};
use axum::response::IntoResponse;
use axum::routing::{delete, get, head, patch, post, put};
use axum::Router;
use tokio::net::TcpListener;

use std::time::Duration;

use super::client::*;

#[derive(Default)]
pub struct FakeState {
    pub token_scopes: Vec<String>,
    pub uploads: Vec<Vec<u8>>,
    pub upload_completes: usize,
    pub manifest_puts: Vec<Vec<u8>>,
    pub deletes: Vec<String>,
    pub blob_exists: bool,
    pub blob_head_429s_remaining: usize,
    pub manifest_exists: bool,
    pub omit_manifest_digest: bool,
}

impl FakeState {
    pub fn with_existing_blobs() -> Self {
        Self {
            blob_exists: true,
            ..Default::default()
        }
    }
}

pub async fn serve(app: Router) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

pub async fn fake_server(state: Arc<Mutex<FakeState>>) -> String {
    let app = Router::new()
        .route("/token", get(token))
        .route("/v2/ns/repo/blobs/{digest}", head(blob_head))
        .route("/v2/ns/repo/blobs/uploads/", post(upload_start))
        .route("/upload/session", patch(upload_chunk))
        .route("/upload/session", put(upload_complete))
        .route("/v2/ns/repo/manifests/{reference}", head(manifest_head))
        .route("/v2/ns/repo/manifests/{reference}", put(manifest_put))
        .route("/v2/ns/repo/manifests/{reference}", delete(manifest_delete))
        .with_state(state);
    serve(app).await
}

pub fn bearer_challenge(base: &str) -> String {
    format!(r#"Bearer realm="{base}/token",service="registry.test""#)
}

pub fn upload_url(base: &str) -> String {
    format!("{base}/v2/ns/repo/blobs/uploads/")
}

pub fn manifest_url(base: &str, tag: &str) -> String {
    format!("{base}/v2/ns/repo/manifests/{tag}")
}

pub fn repo_blob_url(base: &str) -> String {
    format!("{base}/v2/ns/repo/blobs")
}

pub fn client() -> AcrClient {
    client_with_retry_count(0)
}

pub fn client_with_retry_count(retry_count: usize) -> AcrClient {
    AcrClient::new(
        Some(DockerRegistryCredentials {
            username: "user".to_string(),
            password: "pass".to_string(),
        }),
        AcrClientOptions {
            timeout: Duration::from_secs(5),
            retry_count,
            upload_chunk_size: 4,
            retry_initial_backoff: Duration::from_millis(1),
            allow_insecure_http: true,
        },
    )
    .unwrap()
}

pub async fn token(
    State(state): State<Arc<Mutex<FakeState>>>,
    axum::extract::Query(query): axum::extract::Query<std::collections::HashMap<String, String>>,
    headers: AxumHeaderMap,
) -> impl IntoResponse {
    assert_eq!(headers.get("authorization").unwrap(), "Basic dXNlcjpwYXNz");
    state
        .lock()
        .unwrap()
        .token_scopes
        .push(query.get("scope").cloned().unwrap_or_default());
    (AxumStatusCode::OK, r#"{"token":"push-token"}"#)
}

pub async fn blob_head(
    State(state): State<Arc<Mutex<FakeState>>>,
    headers: AxumHeaderMap,
) -> impl IntoResponse {
    authorized_or_challenge(&headers, &state, |state| {
        if state.blob_head_429s_remaining > 0 {
            state.blob_head_429s_remaining -= 1;
            return (AxumStatusCode::TOO_MANY_REQUESTS, [("Retry-After", "0")]).into_response();
        }
        if state.blob_exists {
            AxumStatusCode::OK.into_response()
        } else {
            AxumStatusCode::NOT_FOUND.into_response()
        }
    })
}

pub async fn upload_start(
    State(state): State<Arc<Mutex<FakeState>>>,
    headers: AxumHeaderMap,
) -> impl IntoResponse {
    authorized_or_challenge(&headers, &state, |_state| {
        (AxumStatusCode::ACCEPTED, [("Location", "/upload/session")]).into_response()
    })
}

pub async fn upload_complete(
    State(state): State<Arc<Mutex<FakeState>>>,
    headers: AxumHeaderMap,
) -> impl IntoResponse {
    authorized_or_challenge(&headers, &state, |state| {
        state.upload_completes += 1;
        AxumStatusCode::CREATED.into_response()
    })
}

pub async fn upload_chunk(
    State(state): State<Arc<Mutex<FakeState>>>,
    headers: AxumHeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    authorized_or_challenge(&headers, &state, |state| {
        state.uploads.push(body.to_vec());
        (AxumStatusCode::ACCEPTED, [("Location", "/upload/session")]).into_response()
    })
}

pub async fn manifest_head(
    State(state): State<Arc<Mutex<FakeState>>>,
    headers: AxumHeaderMap,
) -> impl IntoResponse {
    authorized_or_challenge(&headers, &state, |state| {
        if state.manifest_exists {
            AxumStatusCode::OK.into_response()
        } else {
            AxumStatusCode::NOT_FOUND.into_response()
        }
    })
}

pub async fn manifest_put(
    State(state): State<Arc<Mutex<FakeState>>>,
    headers: AxumHeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    authorized_or_challenge(&headers, &state, |state| {
        state.manifest_puts.push(body.to_vec());
        let mut headers = AxumHeaderMap::new();
        if !state.omit_manifest_digest {
            headers.insert(
                "Docker-Content-Digest",
                HeaderValue::from_static("sha256:manifest"),
            );
        }
        (AxumStatusCode::CREATED, headers).into_response()
    })
}

pub async fn manifest_delete(
    State(state): State<Arc<Mutex<FakeState>>>,
    axum::extract::Path(reference): axum::extract::Path<String>,
    headers: AxumHeaderMap,
) -> impl IntoResponse {
    authorized_or_challenge(&headers, &state, |state| {
        state.deletes.push(reference);
        AxumStatusCode::ACCEPTED.into_response()
    })
}

pub fn authorized_or_challenge(
    headers: &AxumHeaderMap,
    state: &Arc<Mutex<FakeState>>,
    f: impl FnOnce(&mut FakeState) -> axum::response::Response,
) -> axum::response::Response {
    if headers.get("authorization").and_then(|v| v.to_str().ok()) != Some("Bearer push-token") {
        let base = state_base_url(headers);
        let mut headers = AxumHeaderMap::new();
        headers.insert(
            "WWW-Authenticate",
            HeaderValue::from_str(&bearer_challenge(&base)).unwrap(),
        );
        return (AxumStatusCode::UNAUTHORIZED, headers).into_response();
    }
    f(&mut state.lock().unwrap())
}

pub fn state_base_url(headers: &AxumHeaderMap) -> String {
    let host = headers
        .get("host")
        .and_then(|value| value.to_str().ok())
        .unwrap();
    format!("http://{host}")
}
