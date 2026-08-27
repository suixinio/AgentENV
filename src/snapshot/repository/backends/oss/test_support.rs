//! A stand-in S3 for the OSS backend's tests.
//!
//! Shared by the catalog and artifact test modules so both count requests
//! against the same server, which is the only way an assertion like "a list
//! costs 1 + N" means the same thing on either side of the split.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use axum::extract::{Request, State};
use axum::http::header::CONTENT_TYPE;
use axum::http::{Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Router;
use object_store_operator::{CredentialSource, ResolvedCredential};

use super::client::OssClient;

pub const TEST_BUCKET: &str = "bucket";
pub const TEST_PREFIX: &str = "snapshots";

type FakeObjects = Arc<Mutex<BTreeMap<String, Vec<u8>>>>;

/// Minimal S3: enough of `ListObjectsV2` plus object GET / HEAD / PUT / DELETE
/// to run whole catalog reads and writes without a real object store.
async fn fake_s3(State(objects): State<FakeObjects>, request: Request) -> Response {
    let query = request.uri().query().unwrap_or("").to_owned();
    if query.contains("list-type=2") {
        let prefix = query
            .split('&')
            .find_map(|pair| pair.strip_prefix("prefix="))
            .map(percent_decode)
            .unwrap_or_default();
        let mut xml = String::from(
            r#"<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><IsTruncated>false</IsTruncated>"#,
        );
        xml.push_str(&format!("<Name>{TEST_BUCKET}</Name>"));
        let stored = objects.lock().expect("fake s3 objects");
        for (key, body) in stored.iter().filter(|(key, _)| key.starts_with(&prefix)) {
            xml.push_str(&format!(
                "<Contents><Key>{key}</Key><LastModified>2026-01-01T00:00:00.000Z</LastModified><ETag>&quot;etag&quot;</ETag><Size>{}</Size><StorageClass>STANDARD</StorageClass></Contents>",
                body.len()
            ));
        }
        xml.push_str("</ListBucketResult>");
        return ([(CONTENT_TYPE, "application/xml")], xml).into_response();
    }

    let method = request.method().clone();
    let path = request.uri().path().trim_start_matches('/');
    let key = path
        .strip_prefix(&format!("{TEST_BUCKET}/"))
        .unwrap_or(path)
        .to_owned();

    if method == Method::PUT {
        let body = match axum::body::to_bytes(request.into_body(), usize::MAX).await {
            Ok(body) => body.to_vec(),
            Err(_) => return StatusCode::BAD_REQUEST.into_response(),
        };
        objects.lock().expect("fake s3 objects").insert(key, body);
        return StatusCode::OK.into_response();
    }
    if method == Method::DELETE {
        objects.lock().expect("fake s3 objects").remove(&key);
        return StatusCode::NO_CONTENT.into_response();
    }

    let stored = objects.lock().expect("fake s3 objects").get(&key).cloned();
    match stored {
        Some(body) => ([(CONTENT_TYPE, "application/octet-stream")], body).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            "<Error><Code>NoSuchKey</Code></Error>",
        )
            .into_response(),
    }
}

fn percent_decode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut bytes = value.as_bytes().iter().copied();
    while let Some(byte) = bytes.next() {
        if byte != b'%' {
            out.push(byte as char);
            continue;
        }
        let hex: String = [bytes.next(), bytes.next()]
            .into_iter()
            .flatten()
            .map(|byte| byte as char)
            .collect();
        match u8::from_str_radix(&hex, 16) {
            Ok(decoded) => out.push(decoded as char),
            Err(_) => out.push('%'),
        }
    }
    out
}

/// Starts a fake S3 seeded with `objects`, keyed by full bucket-relative key.
pub async fn spawn_fake_s3(objects: BTreeMap<String, Vec<u8>>) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind fake s3");
    let addr = listener.local_addr().expect("fake s3 addr");
    let app = Router::new()
        .fallback(fake_s3)
        .with_state(Arc::new(Mutex::new(objects)));
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    addr
}

/// An `OssClient` pointed at a fake S3.
pub fn fake_s3_client(addr: SocketAddr) -> Arc<OssClient> {
    Arc::new(
        OssClient::new(
            TEST_BUCKET.to_string(),
            format!("http://{addr}"),
            "us-east-1".to_string(),
            TEST_PREFIX.to_string(),
            CredentialSource::Static(ResolvedCredential {
                access_key_id: "test-key".to_string(),
                secret_access_key: "test-secret".to_string(),
                security_token: None,
                expires_at: None,
            }),
        )
        .expect("oss client"),
    )
}
