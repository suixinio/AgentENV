//! The `http` handler: transparent HTTPS interception. A connection whose
//! ClientHello names a domain the sandbox's rules cover is terminated with a
//! leaf the cluster CA signs, its requests get their headers replaced, and
//! the request goes on to the real upstream over TLS. Every other connection
//! is relayed to its original destination as opaque bytes, subject to the
//! sandbox's egress policy.

use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::header::{HeaderName, HeaderValue, HOST};
use hyper::http::uri::PathAndQuery;
use hyper::{Method, Request, Response, StatusCode, Uri, Version};
use hyper_util::rt::TokioIo;
use serde::Deserialize;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tracing::{debug, warn};

use crate::credential::{CredentialError, CredentialSource};
use crate::handler::{ConnCtx, Handler, HandlerError};
use crate::marker::{secret_markers, substitute};
use crate::policy::{UpstreamError, UpstreamGuard};
use crate::sni::{parse_sni, SniError, MAX_CLIENT_HELLO_BYTES};
use crate::tls::CaSigner;
use crate::transport::AsyncStream;

/// Carried on every response the broker synthesizes.
pub const REASON_HEADER: &str = "x-aenv-egress-reason";
pub const HTTPS_PORT: u16 = 443;

#[derive(Deserialize, Default)]
struct RuleJson {
    #[serde(default)]
    transform: TransformJson,
}

#[derive(Deserialize, Default)]
struct TransformJson {
    #[serde(default)]
    headers: BTreeMap<String, String>,
}

type Rules = BTreeMap<String, Vec<RuleJson>>;

pub struct HttpHandler {
    signer: Arc<CaSigner>,
    upstream: tokio_native_tls::TlsConnector,
    peek_timeout: Duration,
}

impl HttpHandler {
    pub const NAME: &'static str = "http";

    /// Upstreams are verified against the system trust store.
    pub fn new(signer: Arc<CaSigner>) -> Result<Self, native_tls::Error> {
        let connector = native_tls::TlsConnector::builder()
            .min_protocol_version(Some(native_tls::Protocol::Tlsv12))
            .build()?;
        Ok(Self {
            signer,
            upstream: tokio_native_tls::TlsConnector::from(connector),
            peek_timeout: Duration::from_secs(10),
        })
    }

    pub fn with_peek_timeout(mut self, timeout: Duration) -> Self {
        self.peek_timeout = timeout;
        self
    }
}

/// Exact key first, then the longest `*.suffix` whose suffix the name is
/// strictly below. Rule lists are not merged across keys; within the chosen
/// list every rule's headers apply, later rules winning on the same name,
/// which is the lowercase spelling a header name has on the wire.
fn rule_headers(rules: &Rules, name: &str) -> Option<BTreeMap<String, String>> {
    let key = if let Some(list) = rules.get(name) {
        Some(list)
    } else {
        rules
            .iter()
            .filter_map(|(key, list)| {
                let suffix = key.strip_prefix("*.")?;
                (name.len() > suffix.len()
                    && name.ends_with(suffix)
                    && name.as_bytes()[name.len() - suffix.len() - 1] == b'.')
                    .then_some((suffix.len(), list))
            })
            .max_by_key(|(len, _)| *len)
            .map(|(_, list)| list)
    }?;
    let mut headers = BTreeMap::new();
    for rule in key {
        for (name, value) in &rule.transform.headers {
            headers.insert(name.to_ascii_lowercase(), value.clone());
        }
    }
    Some(headers)
}

#[async_trait]
impl Handler for HttpHandler {
    fn name(&self) -> &str {
        Self::NAME
    }

    async fn handle(
        &self,
        mut conn: Box<dyn AsyncStream>,
        ctx: ConnCtx,
        creds: Arc<dyn CredentialSource>,
        guard: Arc<UpstreamGuard>,
    ) -> Result<(), HandlerError> {
        let rules: Rules = match ctx.params.get("rules") {
            Some(value) => serde_json::from_value(value.clone())
                .map_err(|err| HandlerError::Protocol(format!("rules params: {err}")))?,
            None => Rules::new(),
        };
        let (peeked, sni) = peek_client_hello(&mut conn, self.peek_timeout).await?;
        let matched = sni.as_deref().and_then(|name| {
            rule_headers(&rules, name).map(|headers| Matched {
                name: name.to_string(),
                headers,
            })
        });
        match matched {
            Some(matched) => {
                metrics::counter!("egress_conns_total", "handler" => "http", "outcome" => "terminated").increment(1);
                self.terminate(conn, peeked, matched, ctx, creds, guard)
                    .await
            }
            None => {
                if sni.is_none() {
                    metrics::counter!("egress_intercept_no_sni_total").increment(1);
                }
                passthrough(conn, peeked, &ctx, &guard).await
            }
        }
    }
}

impl HttpHandler {
    async fn terminate(
        &self,
        conn: Box<dyn AsyncStream>,
        peeked: Vec<u8>,
        matched: Matched,
        ctx: ConnCtx,
        creds: Arc<dyn CredentialSource>,
        guard: Arc<UpstreamGuard>,
    ) -> Result<(), HandlerError> {
        let Matched { name, headers } = matched;
        let leaf = self
            .signer
            .leaf_for(&name, &ctx.sandbox_id)
            .map_err(|err| HandlerError::Protocol(format!("leaf for {name}: {err}")))?;
        let tls = leaf
            .acceptor
            .accept(PrefixedStream::new(peeked, conn))
            .await
            .map_err(|err| HandlerError::Protocol(format!("tls accept for {name}: {err}")))?;
        let request_ctx = Arc::new(RequestContext {
            name,
            headers,
            ctx,
            creds,
            guard,
            upstream: self.upstream.clone(),
        });
        let service = hyper::service::service_fn(move |req| {
            let request_ctx = Arc::clone(&request_ctx);
            async move { Ok::<_, hyper::Error>(handle_request(req, request_ctx).await) }
        });
        hyper::server::conn::http1::Builder::new()
            .keep_alive(true)
            .serve_connection(TokioIo::new(tls), service)
            .with_upgrades()
            .await
            .map_err(|err| HandlerError::Protocol(format!("serving intercepted http: {err}")))
    }
}

/// The rule list a server name selected.
struct Matched {
    name: String,
    headers: BTreeMap<String, String>,
}

struct RequestContext {
    name: String,
    headers: BTreeMap<String, String>,
    ctx: ConnCtx,
    creds: Arc<dyn CredentialSource>,
    guard: Arc<UpstreamGuard>,
    upstream: tokio_native_tls::TlsConnector,
}

type Body = BoxBody<Bytes, hyper::Error>;

fn synthesized(status: StatusCode, reason: &'static str) -> Response<Body> {
    let body = Full::new(Bytes::from_static(b""))
        .map_err(|never| match never {})
        .boxed();
    let mut response = Response::new(body);
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert(REASON_HEADER, HeaderValue::from_static(reason));
    response
}

fn denied_reason(err: &UpstreamError) -> (StatusCode, &'static str) {
    match err {
        UpstreamError::Denied(_) => (StatusCode::FORBIDDEN, err.reason()),
        _ => (StatusCode::BAD_GATEWAY, err.reason()),
    }
}

/// The authority a guest names must be the one whose rules chose the credentials.
fn refuse_foreign_authority<B>(req: &Request<B>, name: &str) -> Option<(StatusCode, &'static str)> {
    if req.method() == Method::CONNECT {
        return Some((StatusCode::METHOD_NOT_ALLOWED, "connect_unsupported"));
    }
    // TRACE echoes the request the broker forwards, injected headers included.
    if req.method() == Method::TRACE {
        return Some((StatusCode::METHOD_NOT_ALLOWED, "method-not-brokered"));
    }
    match req.uri().authority() {
        Some(authority) if !authority.host().eq_ignore_ascii_case(name) => {
            Some((StatusCode::BAD_REQUEST, "authority_mismatch"))
        }
        _ => None,
    }
}

/// Runs after every transform, so nothing else can name another authority.
fn bind_to_name<B>(req: &mut Request<B>, name: &str) -> Result<(), (StatusCode, &'static str)> {
    let host = HeaderValue::from_str(name)
        .map_err(|_| (StatusCode::BAD_GATEWAY, "host_not_representable"))?;
    if req.uri().scheme().is_some() || req.uri().authority().is_some() {
        let origin_form: Uri = req
            .uri()
            .path_and_query()
            .map_or("/", PathAndQuery::as_str)
            .parse()
            .map_err(|_| (StatusCode::BAD_GATEWAY, "target_not_representable"))?;
        *req.uri_mut() = origin_form;
    }
    req.headers_mut().insert(HOST, host);
    Ok(())
}

async fn handle_request(mut req: Request<Incoming>, rc: Arc<RequestContext>) -> Response<Body> {
    if req.version() == Version::HTTP_2 {
        return synthesized(StatusCode::HTTP_VERSION_NOT_SUPPORTED, "http2_unsupported");
    }
    if let Some((status, reason)) = refuse_foreign_authority(&req, &rc.name) {
        metrics::counter!("egress_policy_denied_total", "reason" => reason).increment(1);
        debug!(name = %rc.name, reason, "request refused before any credential");
        return synthesized(status, reason);
    }

    let mut resolved: BTreeMap<String, String> = BTreeMap::new();
    for template in rc.headers.values() {
        for marker in secret_markers(template) {
            if resolved.contains_key(marker.name) {
                continue;
            }
            match rc
                .creds
                .get(&rc.ctx.sandbox_id, &rc.ctx.execution_id, marker.name)
                .await
            {
                Ok(secret) => {
                    // The rule said which name gets this value; the secret
                    // itself says which names may ever get it, so a rule that
                    // names it for another host does not make it reachable.
                    if !secret.may_reach(&rc.name) {
                        metrics::counter!(
                            "egress_policy_denied_total",
                            "reason" => "secret_host_not_allowed"
                        )
                        .increment(1);
                        debug!(
                            name = %rc.name,
                            "a rule named a secret that is pinned to other hosts"
                        );
                        return synthesized(StatusCode::FORBIDDEN, "secret_host_not_allowed");
                    }
                    resolved.insert(
                        marker.name.to_string(),
                        String::from_utf8_lossy(secret.expose()).into_owned(),
                    );
                }
                Err(err) => {
                    let status = match &err {
                        CredentialError::Denied => StatusCode::FORBIDDEN,
                        // A denial is the policy working; an outage is the
                        // operator's, and this line is where the broker says
                        // why (a refused bearer, an unreachable resolver).
                        CredentialError::Unavailable(reason) => {
                            warn!(
                                name = %rc.name,
                                reason = %reason,
                                "a credential could not be resolved; the guest gets a 502"
                            );
                            StatusCode::BAD_GATEWAY
                        }
                    };
                    metrics::counter!("egress_policy_denied_total", "reason" => err.reason())
                        .increment(1);
                    return synthesized(status, err.reason());
                }
            }
        }
    }
    for (name, template) in &rc.headers {
        let value = substitute::<std::convert::Infallible>(template, |secret| {
            Ok(resolved.get(secret).cloned().unwrap_or_default())
        })
        .unwrap_or_else(|never| match never {});
        let (Ok(header), Ok(value)) = (
            HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_str(&value),
        ) else {
            return synthesized(StatusCode::BAD_GATEWAY, "header_not_representable");
        };
        req.headers_mut().insert(header, value);
    }
    if let Err((status, reason)) = bind_to_name(&mut req, &rc.name) {
        warn!(name = %rc.name, reason, "the matched name does not fit a request");
        return synthesized(status, reason);
    }

    let wants_upgrade = req.headers().contains_key(hyper::header::UPGRADE);
    let client_upgrade = wants_upgrade.then(|| hyper::upgrade::on(&mut req));

    let (tcp, _) = match rc
        .guard
        .connect_checked(HttpHandler::NAME, &rc.name, HTTPS_PORT, &rc.ctx.egress)
        .await
    {
        Ok(connected) => connected,
        Err(err) => {
            let (status, reason) = denied_reason(&err);
            metrics::counter!("egress_policy_denied_total", "reason" => reason).increment(1);
            debug!(name = %rc.name, reason, "upstream refused");
            return synthesized(status, reason);
        }
    };
    let tls = match rc.upstream.connect(&rc.name, tcp).await {
        Ok(tls) => tls,
        Err(err) => {
            warn!(name = %rc.name, error = %err, "upstream tls failed");
            return synthesized(StatusCode::BAD_GATEWAY, "upstream_tls");
        }
    };
    let (mut sender, connection) =
        match hyper::client::conn::http1::handshake(TokioIo::new(tls)).await {
            Ok(parts) => parts,
            Err(err) => {
                warn!(name = %rc.name, error = %err, "upstream http handshake failed");
                return synthesized(StatusCode::BAD_GATEWAY, "upstream_error");
            }
        };
    tokio::spawn(async move {
        if let Err(err) = connection.with_upgrades().await {
            debug!(error = %err, "upstream connection ended with an error");
        }
    });
    let mut response = match sender.send_request(req).await {
        Ok(response) => response,
        Err(err) => {
            warn!(name = %rc.name, error = %err, "upstream request failed");
            return synthesized(StatusCode::BAD_GATEWAY, "upstream_error");
        }
    };
    if response.status() == StatusCode::SWITCHING_PROTOCOLS {
        if let Some(client_upgrade) = client_upgrade {
            let upstream_upgrade = hyper::upgrade::on(&mut response);
            tokio::spawn(async move {
                match (client_upgrade.await, upstream_upgrade.await) {
                    (Ok(client), Ok(upstream)) => {
                        let mut client = TokioIo::new(client);
                        let mut upstream = TokioIo::new(upstream);
                        if let Err(err) =
                            tokio::io::copy_bidirectional(&mut client, &mut upstream).await
                        {
                            debug!(error = %err, "upgraded relay ended with an error");
                        }
                    }
                    (client, upstream) => {
                        debug!(
                            client_ok = client.is_ok(),
                            upstream_ok = upstream.is_ok(),
                            "upgrade did not complete on both sides"
                        );
                    }
                }
            });
        }
    }
    response.map(|body| body.boxed())
}

async fn peek_client_hello(
    conn: &mut Box<dyn AsyncStream>,
    timeout: Duration,
) -> Result<(Vec<u8>, Option<String>), HandlerError> {
    let mut buf = Vec::with_capacity(1024);
    let deadline = tokio::time::Instant::now() + timeout;
    let mut chunk = [0u8; 4096];
    loop {
        match parse_sni(&buf) {
            Ok(name) => return Ok((buf, name)),
            Err(SniError::Incomplete) if buf.len() < MAX_CLIENT_HELLO_BYTES => {}
            Err(SniError::Incomplete) | Err(SniError::NotTls) | Err(SniError::Malformed) => {
                return Ok((buf, None));
            }
        }
        let read = tokio::time::timeout_at(deadline, conn.read(&mut chunk))
            .await
            .map_err(|_| HandlerError::Protocol("client hello did not arrive in time".into()))??;
        if read == 0 {
            return Err(HandlerError::Protocol(
                "connection closed before a client hello".into(),
            ));
        }
        buf.extend_from_slice(&chunk[..read]);
    }
}

async fn passthrough(
    mut conn: Box<dyn AsyncStream>,
    peeked: Vec<u8>,
    ctx: &ConnCtx,
    guard: &UpstreamGuard,
) -> Result<(), HandlerError> {
    let Some(dst) = ctx.original_dst else {
        return Err(HandlerError::Protocol(
            "no original destination for a passthrough connection".into(),
        ));
    };
    let mut upstream = match guard
        .connect_checked_addr(HttpHandler::NAME, dst, &ctx.egress)
        .await
    {
        Ok(upstream) => upstream,
        Err(UpstreamError::Denied(reason)) => {
            metrics::counter!("egress_policy_denied_total", "reason" => reason.reason())
                .increment(1);
            metrics::counter!("egress_conns_total", "handler" => "http", "outcome" => "denied")
                .increment(1);
            debug!(%dst, reason = reason.reason(), "passthrough denied");
            return Ok(());
        }
        Err(err) => {
            metrics::counter!("egress_conns_total", "handler" => "http", "outcome" => "upstream_unreachable").increment(1);
            return Err(HandlerError::Upstream(err));
        }
    };
    metrics::counter!("egress_conns_total", "handler" => "http", "outcome" => "passthrough")
        .increment(1);
    upstream.write_all(&peeked).await?;
    tokio::io::copy_bidirectional(&mut conn, &mut upstream).await?;
    Ok(())
}

/// A stream whose first reads yield bytes already taken off the wire.
struct PrefixedStream {
    prefix: Vec<u8>,
    offset: usize,
    inner: Box<dyn AsyncStream>,
}

impl PrefixedStream {
    fn new(prefix: Vec<u8>, inner: Box<dyn AsyncStream>) -> Self {
        Self {
            prefix,
            offset: 0,
            inner,
        }
    }
}

impl AsyncRead for PrefixedStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if self.offset < self.prefix.len() {
            let n = (self.prefix.len() - self.offset).min(buf.remaining());
            let start = self.offset;
            buf.put_slice(&self.prefix[start..start + n]);
            self.offset += n;
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for PrefixedStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, data)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rules(entries: &[(&str, &[(&str, &str)])]) -> Rules {
        entries
            .iter()
            .map(|(domain, headers)| {
                (
                    domain.to_string(),
                    vec![RuleJson {
                        transform: TransformJson {
                            headers: headers
                                .iter()
                                .map(|(k, v)| (k.to_string(), v.to_string()))
                                .collect(),
                        },
                    }],
                )
            })
            .collect()
    }

    #[test]
    fn exact_keys_win_then_the_longest_wildcard_suffix() {
        let rules = rules(&[
            ("api.example.com", &[("Authorization", "exact")]),
            ("*.example.com", &[("Authorization", "wild")]),
            ("*.deep.example.com", &[("Authorization", "deeper")]),
        ]);
        assert_eq!(
            rule_headers(&rules, "api.example.com").unwrap()["authorization"],
            "exact"
        );
        assert_eq!(
            rule_headers(&rules, "other.example.com").unwrap()["authorization"],
            "wild"
        );
        assert_eq!(
            rule_headers(&rules, "a.deep.example.com").unwrap()["authorization"],
            "deeper"
        );
        assert!(
            rule_headers(&rules, "example.com").is_none(),
            "the apex is not below the wildcard"
        );
        assert!(rule_headers(&rules, "notexample.com").is_none());
        assert!(rule_headers(&rules, "evil.com").is_none());
    }

    #[tokio::test]
    async fn a_prefixed_stream_yields_the_prefix_then_the_inner_bytes() {
        let (mut client, server) = tokio::io::duplex(64);
        client.write_all(b"rest").await.unwrap();
        drop(client);
        let mut stream = PrefixedStream::new(b"pre".to_vec(), Box::new(server));
        let mut out = Vec::new();
        stream.read_to_end(&mut out).await.unwrap();
        assert_eq!(out, b"prerest");
    }

    #[test]
    fn header_names_merge_case_insensitively_and_the_later_rule_wins() {
        let rules: Rules = serde_json::from_value(serde_json::json!({
            "api.test": [
                { "transform": { "headers": { "Authorization": "first" } } },
                { "transform": { "headers": { "authorization": "second" } } }
            ]
        }))
        .unwrap();
        let headers = rule_headers(&rules, "api.test").unwrap();
        assert_eq!(headers.len(), 1);
        assert_eq!(headers["authorization"], "second");
    }

    fn request(method: &str, uri: &str, host: &str) -> Request<()> {
        Request::builder()
            .method(method)
            .uri(uri)
            .header(HOST, host)
            .body(())
            .unwrap()
    }

    #[test]
    fn an_absolute_form_authority_other_than_the_matched_name_is_refused() {
        assert_eq!(
            refuse_foreign_authority(
                &request("GET", "https://evil.test/v1/models", "api.test"),
                "api.test"
            ),
            Some((StatusCode::BAD_REQUEST, "authority_mismatch"))
        );
        assert_eq!(
            refuse_foreign_authority(
                &request("GET", "https://API.test/v1/models", "api.test"),
                "api.test"
            ),
            None
        );
        assert_eq!(
            refuse_foreign_authority(&request("GET", "/v1/models", "evil.test"), "api.test"),
            None
        );
    }

    #[test]
    fn connect_is_refused_whatever_authority_it_names() {
        for target in ["api.test:443", "evil.test:443"] {
            let req = Request::builder()
                .method("CONNECT")
                .uri(target)
                .body(())
                .unwrap();
            assert_eq!(
                refuse_foreign_authority(&req, "api.test"),
                Some((StatusCode::METHOD_NOT_ALLOWED, "connect_unsupported"))
            );
        }
    }

    #[test]
    fn trace_is_refused_whatever_authority_it_names() {
        for target in ["/", "https://api.test/"] {
            let req = Request::builder()
                .method("TRACE")
                .uri(target)
                .body(())
                .unwrap();
            assert_eq!(
                refuse_foreign_authority(&req, "api.test"),
                Some((StatusCode::METHOD_NOT_ALLOWED, "method-not-brokered"))
            );
        }
    }

    #[test]
    fn binding_rewrites_the_target_to_origin_form_and_replaces_the_guest_host() {
        let mut absolute = request("GET", "https://api.test:8443/v1/models?a=b", "evil.test");
        bind_to_name(&mut absolute, "api.test").unwrap();
        assert_eq!(absolute.uri().to_string(), "/v1/models?a=b");
        assert_eq!(absolute.headers()[HOST], "api.test");

        let mut rootless = request("GET", "https://api.test", "evil.test");
        bind_to_name(&mut rootless, "api.test").unwrap();
        assert_eq!(rootless.uri().to_string(), "/");

        let mut origin = request("GET", "/v1/models", "evil.test");
        bind_to_name(&mut origin, "api.test").unwrap();
        assert_eq!(origin.uri().to_string(), "/v1/models");
        assert_eq!(origin.headers()[HOST], "api.test");
    }

    #[test]
    fn synthesized_responses_carry_the_reason_header() {
        let response = synthesized(StatusCode::FORBIDDEN, "credential_denied");
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert_eq!(response.headers()[REASON_HEADER], "credential_denied");
    }

    mod broker {
        use std::sync::Arc;
        use std::time::Duration;

        use http_body_util::Empty;
        use hyper::Request;
        use hyper_util::rt::TokioIo;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        use super::super::*;
        use crate::credential::StaticSource;
        use crate::handlers::echo::IdentityEchoHandler;
        use crate::header::test_support::sample_header;
        use crate::header::{EgressPolicySummary, IdentityHeader};
        use crate::policy::BrokerDenyList;
        use crate::runtime::{Options, Runtime};
        use crate::tls::{generate_test_ca, SignerOptions};
        use crate::transport::{BrokerTransport, RemoteTransport};
        use crate::Dispatcher;

        const KEY: &[u8] = b"broker-test-key";

        struct FixedResolver(std::net::IpAddr);

        #[async_trait::async_trait]
        impl crate::policy::Resolver for FixedResolver {
            async fn resolve(&self, _: &str) -> std::io::Result<Vec<std::net::IpAddr>> {
                Ok(vec![self.0])
            }
        }

        struct Broker {
            transport: RemoteTransport,
            ca_pem: Vec<u8>,
        }

        async fn start_broker(
            rules: serde_json::Value,
            closed: bool,
        ) -> (Broker, serde_json::Value, EgressPolicySummary) {
            let (ca_pem, ca_key) = generate_test_ca("broker test ca").unwrap();
            let signer =
                Arc::new(CaSigner::from_pem(&ca_pem, &ca_key, SignerOptions::default()).unwrap());
            let creds = Arc::new(
                StaticSource::default()
                    .with("sbx-1", "exec-1", "openai", b"sk-test")
                    // Pinned to a name no rule in these tests matches.
                    .with_pinned(
                        "sbx-1",
                        "exec-1",
                        "elsewhere",
                        b"sk-elsewhere",
                        &["api.elsewhere.example"],
                    ),
            );
            let guard = Arc::new(
                UpstreamGuard::new(BrokerDenyList::empty())
                    .with_resolver(Arc::new(FixedResolver("203.0.113.9".parse().unwrap()))),
            );
            let dispatcher = Arc::new(
                Dispatcher::new(creds, guard)
                    .with_handler(Arc::new(HttpHandler::new(Arc::clone(&signer)).unwrap()))
                    .with_handler(Arc::new(IdentityEchoHandler)),
            );
            let runtime = Arc::new(Runtime::new(
                Options::new(vec![KEY.to_vec()], Duration::from_secs(30), 1024),
                dispatcher,
            ));
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let server_leaf = signer.leaf_for("localhost", "broker").unwrap();
            tokio::spawn(crate::runtime::run(
                runtime,
                listener,
                server_leaf.acceptor.clone(),
                std::future::pending(),
            ));
            let transport =
                RemoteTransport::new(&addr.to_string(), Some("localhost"), &ca_pem, KEY).unwrap();
            let policy = EgressPolicySummary {
                allow_internet: !closed,
                allowed_cidrs: vec![],
                denied_cidrs: vec![],
            };
            (Broker { transport, ca_pem }, rules, policy)
        }

        fn header(
            handler: &str,
            params: serde_json::Value,
            egress: EgressPolicySummary,
            original_dst: Option<std::net::SocketAddr>,
        ) -> IdentityHeader {
            IdentityHeader {
                handler: handler.into(),
                params,
                egress,
                original_dst,
                issued_at_unix_ms: IdentityHeader::now_unix_ms(),
                nonce: IdentityHeader::fresh_nonce(),
                ..sample_header()
            }
        }

        #[tokio::test]
        async fn the_remote_transport_reaches_the_echo_handler_over_tls() {
            let (broker, _, policy) = start_broker(serde_json::json!({}), false).await;
            let stream = broker
                .transport
                .open(header("echo", serde_json::json!({}), policy, None))
                .await
                .unwrap();
            let mut stream = tokio::io::BufReader::new(stream);
            let mut line = String::new();
            tokio::io::AsyncBufReadExt::read_line(&mut stream, &mut line)
                .await
                .unwrap();
            assert!(line.contains("\"sandbox_id\":\"sbx-1\""), "{line}");
            broker.transport.probe().await.unwrap();
        }

        #[tokio::test]
        async fn a_tampered_key_is_rejected_by_the_remote_broker() {
            let (broker, _, policy) = start_broker(serde_json::json!({}), false).await;
            let wrong = RemoteTransport::new(
                &broker_addr(&broker).await,
                Some("localhost"),
                &broker.ca_pem,
                b"another-key",
            )
            .unwrap();
            let err = wrong
                .open(header("echo", serde_json::json!({}), policy, None))
                .await
                .err()
                .unwrap();
            assert!(
                matches!(err, crate::transport::TransportError::Rejected { reason } if reason == "bad_mac")
            );
        }

        async fn broker_addr(broker: &Broker) -> String {
            // The transport keeps the endpoint private; probe through it and
            // read the address back off a fresh header round trip instead.
            let stream = broker
                .transport
                .open(header(
                    "echo",
                    serde_json::json!({}),
                    EgressPolicySummary {
                        allow_internet: true,
                        allowed_cidrs: vec![],
                        denied_cidrs: vec![],
                    },
                    None,
                ))
                .await
                .unwrap();
            drop(stream);
            broker.transport.endpoint().to_string()
        }

        async fn terminated_request(
            broker: &Broker,
            rules: serde_json::Value,
            policy: EgressPolicySummary,
        ) -> hyper::Response<hyper::body::Incoming> {
            let hdr = header(
                "http",
                serde_json::json!({ "rules": rules }),
                policy,
                Some("203.0.113.9:443".parse().unwrap()),
            );
            let stream = broker.transport.open(hdr).await.unwrap();
            let mut connector = native_tls::TlsConnector::builder();
            connector
                .add_root_certificate(native_tls::Certificate::from_pem(&broker.ca_pem).unwrap());
            let connector = tokio_native_tls::TlsConnector::from(connector.build().unwrap());
            let tls = connector.connect("api.test", stream).await.unwrap();
            let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(tls))
                .await
                .unwrap();
            tokio::spawn(conn);
            let req = Request::builder()
                .uri("/v1/models")
                .header(HOST, "api.test")
                .header("Authorization", "Bearer guest-supplied")
                .body(Empty::<Bytes>::new())
                .unwrap();
            sender.send_request(req).await.unwrap()
        }

        #[tokio::test]
        async fn a_matched_name_is_terminated_and_a_closed_policy_answers_403() {
            let rules = serde_json::json!({
                "api.test": [{ "transform": { "headers": { "Authorization": "Bearer ${aenv.secrets.openai}" } } }]
            });
            let (broker, rules, policy) = start_broker(rules, true).await;
            let response = terminated_request(&broker, rules, policy).await;
            assert_eq!(response.status(), StatusCode::FORBIDDEN);
            assert_eq!(response.headers()[REASON_HEADER], "internet_disabled");
        }

        #[tokio::test]
        async fn a_marker_no_grant_covers_answers_403_before_any_upstream_is_dialled() {
            let rules = serde_json::json!({
                "*.test": [{ "transform": { "headers": { "Authorization": "Bearer ${aenv.secrets.missing}" } } }]
            });
            let (broker, rules, policy) = start_broker(rules, false).await;
            let response = terminated_request(&broker, rules, policy).await;
            assert_eq!(response.status(), StatusCode::FORBIDDEN);
            assert_eq!(response.headers()[REASON_HEADER], "credential_denied");
        }

        #[tokio::test]
        async fn a_rule_naming_a_secret_pinned_to_other_hosts_answers_403() {
            let rules = serde_json::json!({
                "*.test": [{ "transform": { "headers": { "Authorization": "Bearer ${aenv.secrets.elsewhere}" } } }]
            });
            let (broker, rules, policy) = start_broker(rules, false).await;
            let response = terminated_request(&broker, rules, policy).await;
            assert_eq!(response.status(), StatusCode::FORBIDDEN);
            assert_eq!(response.headers()[REASON_HEADER], "secret_host_not_allowed");
        }

        async fn raw_exchange(
            broker: &Broker,
            rules: serde_json::Value,
            policy: EgressPolicySummary,
            request: &str,
        ) -> String {
            let hdr = header(
                "http",
                serde_json::json!({ "rules": rules }),
                policy,
                Some("203.0.113.9:443".parse().unwrap()),
            );
            let stream = broker.transport.open(hdr).await.unwrap();
            let mut connector = native_tls::TlsConnector::builder();
            connector
                .add_root_certificate(native_tls::Certificate::from_pem(&broker.ca_pem).unwrap());
            let connector = tokio_native_tls::TlsConnector::from(connector.build().unwrap());
            let mut tls = connector.connect("api.test", stream).await.unwrap();
            tls.write_all(request.as_bytes()).await.unwrap();
            let mut head = Vec::new();
            let mut byte = [0u8; 1];
            while !head.ends_with(b"\r\n\r\n") {
                if tls.read(&mut byte).await.unwrap() == 0 {
                    break;
                }
                head.push(byte[0]);
            }
            String::from_utf8_lossy(&head).into_owned()
        }

        fn bearer_rules() -> serde_json::Value {
            serde_json::json!({
                "api.test": [{ "transform": { "headers": { "Authorization": "Bearer ${aenv.secrets.openai}" } } }]
            })
        }

        #[tokio::test]
        async fn an_absolute_form_target_naming_another_host_is_refused_with_400() {
            let (broker, rules, policy) = start_broker(bearer_rules(), true).await;
            let head = raw_exchange(
                &broker,
                rules,
                policy,
                "GET https://evil.test/v1/models HTTP/1.1\r\nHost: api.test\r\n\r\n",
            )
            .await;
            assert!(head.starts_with("HTTP/1.1 400 "), "{head}");
            assert!(
                head.contains(&format!("{REASON_HEADER}: authority_mismatch")),
                "{head}"
            );
        }

        #[tokio::test]
        async fn an_absolute_form_target_naming_the_matched_host_reaches_the_upstream_step() {
            let (broker, rules, policy) = start_broker(bearer_rules(), true).await;
            let head = raw_exchange(
                &broker,
                rules,
                policy,
                "GET https://api.test/v1/models HTTP/1.1\r\nHost: evil.test\r\n\r\n",
            )
            .await;
            assert!(head.starts_with("HTTP/1.1 403 "), "{head}");
            assert!(
                head.contains(&format!("{REASON_HEADER}: internet_disabled")),
                "{head}"
            );
        }

        #[tokio::test]
        async fn a_connect_request_is_refused_with_405() {
            let (broker, rules, policy) = start_broker(bearer_rules(), true).await;
            let head = raw_exchange(
                &broker,
                rules,
                policy,
                "CONNECT api.test:443 HTTP/1.1\r\nHost: api.test\r\n\r\n",
            )
            .await;
            assert!(head.starts_with("HTTP/1.1 405 "), "{head}");
            assert!(
                head.contains(&format!("{REASON_HEADER}: connect_unsupported")),
                "{head}"
            );
        }

        #[tokio::test]
        async fn an_unmatched_name_is_relayed_to_its_original_destination() {
            let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let upstream_addr = upstream.local_addr().unwrap();
            tokio::spawn(async move {
                let (mut socket, _) = upstream.accept().await.unwrap();
                let mut first = vec![0u8; 5];
                socket.read_exact(&mut first).await.unwrap();
                socket.write_all(b"passthrough:").await.unwrap();
                socket.write_all(&first).await.unwrap();
                socket.shutdown().await.unwrap();
            });
            let rules = serde_json::json!({ "api.test": [] });
            let (broker, rules, policy) = start_broker(rules, false).await;
            let hdr = header(
                "http",
                serde_json::json!({ "rules": rules }),
                policy,
                Some(upstream_addr),
            );
            let mut stream = broker.transport.open(hdr).await.unwrap();
            let hello = crate::sni::client_hello_with_sni(Some("other.example"));
            stream.write_all(&hello).await.unwrap();
            let mut answer = Vec::new();
            stream.read_to_end(&mut answer).await.unwrap();
            assert_eq!(&answer[..12], b"passthrough:");
            assert_eq!(&answer[12..], &hello[..5]);
        }

        #[tokio::test]
        async fn a_passthrough_to_a_denied_destination_is_closed_without_bytes() {
            let rules = serde_json::json!({ "api.test": [] });
            let (broker, rules, policy) = start_broker(rules, true).await;
            let hdr = header(
                "http",
                serde_json::json!({ "rules": rules }),
                policy,
                Some("203.0.113.9:443".parse().unwrap()),
            );
            let mut stream = broker.transport.open(hdr).await.unwrap();
            stream
                .write_all(&crate::sni::client_hello_with_sni(Some("other.example")))
                .await
                .unwrap();
            let mut answer = Vec::new();
            stream.read_to_end(&mut answer).await.unwrap();
            assert!(answer.is_empty());
        }
    }
}
