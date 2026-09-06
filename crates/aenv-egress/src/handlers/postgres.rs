//! The `postgres` handler: it terminates the guest's startup exchange, binds
//! the credential its endpoint declaration names, and authenticates upstream
//! itself. The guest's DSN carries a placeholder user and password that are
//! never used and never leave the namespace.
//!
//! Fields the credential source did not return are passed through exactly as
//! the guest wrote them, so a guest may name a database on the same instance
//! and be refused by the database's own permissions rather than here.

use std::sync::Arc;

use async_trait::async_trait;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::credential::{CredentialFields, CredentialSource};
use crate::handler::{ConnCtx, Handler, HandlerError};
use crate::handlers::scram::ScramClient;
use crate::policy::UpstreamGuard;
use crate::transport::AsyncStream;

const SSL_REQUEST_CODE: u32 = 80_877_103;
const GSSENC_REQUEST_CODE: u32 = 80_877_104;
const CANCEL_REQUEST_CODE: u32 = 80_877_102;
const PROTOCOL_VERSION_3: u32 = 196_608;

/// Postgres itself refuses a startup packet over 10000 bytes; this leaves
/// room for a long `options` and still bounds what one connection can hold.
const MAX_STARTUP_LEN: usize = 16 * 1024;
const MAX_MESSAGE_LEN: usize = 64 * 1024;

/// SQLSTATE `invalid_authorization_specification`: what the guest is told
/// when no grant covers the credential or it could not be read.
const SQLSTATE_INVALID_AUTHORIZATION: &str = "28000";
/// SQLSTATE `feature_not_supported`.
const SQLSTATE_FEATURE_NOT_SUPPORTED: &str = "0A000";

/// Credential fields that configure the connection rather than name a startup
/// parameter. Everything else the source returns overrides the parameter of
/// the same name.
const CONNECTION_FIELDS: [&str; 4] = ["host", "port", "password", "sslmode"];

pub struct PostgresHandler {
    upstream: tokio_native_tls::TlsConnector,
}

impl PostgresHandler {
    pub const NAME: &'static str = "postgres";

    /// Upstreams are verified against the system trust store, by the name the
    /// credential gave, not by the address the guard dialled.
    pub fn new() -> Result<Self, native_tls::Error> {
        let connector = native_tls::TlsConnector::builder()
            .min_protocol_version(Some(native_tls::Protocol::Tlsv12))
            .build()?;
        Ok(Self {
            upstream: tokio_native_tls::TlsConnector::from(connector),
        })
    }
}

#[async_trait]
impl Handler for PostgresHandler {
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
        match self.serve(&mut conn, &ctx, creds, guard).await {
            Ok(()) => Ok(()),
            Err(Refusal::Told(err)) => Err(err),
            Err(Refusal::Tell(code, message, err)) => {
                let _ = conn.write_all(&error_response(code, &message)).await;
                let _ = conn.flush().await;
                Err(err)
            }
        }
    }
}

/// A failure the guest is owed a Postgres `ErrorResponse` for, versus one it
/// has already been told about or cannot be told about.
enum Refusal {
    Told(HandlerError),
    Tell(&'static str, String, HandlerError),
}

fn tell(code: &'static str, message: impl Into<String>) -> Refusal {
    let message = message.into();
    Refusal::Tell(code, message.clone(), HandlerError::Protocol(message))
}

impl From<std::io::Error> for Refusal {
    fn from(err: std::io::Error) -> Self {
        Refusal::Told(HandlerError::Io(err))
    }
}

impl PostgresHandler {
    async fn serve(
        &self,
        conn: &mut Box<dyn AsyncStream>,
        ctx: &ConnCtx,
        creds: Arc<dyn CredentialSource>,
        guard: Arc<UpstreamGuard>,
    ) -> Result<(), Refusal> {
        let name = ctx
            .params
            .get("credential")
            .and_then(|name| name.as_str())
            .ok_or_else(|| {
                tell(
                    SQLSTATE_INVALID_AUTHORIZATION,
                    "this endpoint declares no credential",
                )
            })?;

        // The guest may open with an SSLRequest or a GSSENCRequest; both are
        // refused so the namespace hop stays plain and the guest needs no CA.
        let mut packet = read_startup(conn).await?;
        while matches!(packet.code, SSL_REQUEST_CODE | GSSENC_REQUEST_CODE) {
            conn.write_all(b"N").await?;
            conn.flush().await?;
            packet = read_startup(conn).await?;
        }

        let fields = match creds
            .get_fields(&ctx.sandbox_id, &ctx.execution_id, name)
            .await
        {
            Ok(fields) => fields,
            Err(err) => {
                let reason = err.reason();
                if let crate::credential::CredentialError::Unavailable(why) = &err {
                    tracing::warn!(
                        endpoint_port = ctx.port,
                        reason = %why,
                        "a credential could not be resolved; the guest gets an auth refusal"
                    );
                }
                return Err(Refusal::Tell(
                    SQLSTATE_INVALID_AUTHORIZATION,
                    format!("the broker holds no usable credential for this sandbox ({reason})"),
                    HandlerError::Credential(err),
                ));
            }
        };
        let (host, port) = upstream_of(&fields)?;
        let password = fields.get("password").ok_or_else(|| {
            tell(
                SQLSTATE_INVALID_AUTHORIZATION,
                "the credential carries no password for the upstream",
            )
        })?;

        if packet.code == CANCEL_REQUEST_CODE {
            // A cancel is its own connection carrying no startup parameters:
            // it is forwarded verbatim and answered by closing.
            let mut upstream = self
                .connect_upstream(&guard, ctx, &fields, host, port)
                .await?;
            upstream.write_all(&packet.raw).await?;
            upstream.flush().await?;
            return Ok(());
        }
        if packet.code != PROTOCOL_VERSION_3 {
            return Err(tell(
                SQLSTATE_FEATURE_NOT_SUPPORTED,
                format!(
                    "this broker speaks the 3.0 frontend protocol only, not {}.{}",
                    packet.code >> 16,
                    packet.code & 0xffff
                ),
            ));
        }

        let guest_params = parse_startup_params(&packet.body)?;
        if guest_params.iter().any(|(key, _)| key == "replication") {
            return Err(tell(
                SQLSTATE_FEATURE_NOT_SUPPORTED,
                "a replication connection is not brokered: it switches to a protocol \
                 this broker does not forward blind",
            ));
        }
        let params = merge_params(guest_params, &fields);
        let user = params
            .iter()
            .find(|(key, _)| key == "user")
            .map(|(_, value)| value.clone())
            .ok_or_else(|| {
                tell(
                    SQLSTATE_INVALID_AUTHORIZATION,
                    "neither the credential nor the connection names a user",
                )
            })?;

        let mut upstream = self
            .connect_upstream(&guard, ctx, &fields, host, port)
            .await?;
        upstream.write_all(&startup_message(&params)).await?;
        upstream.flush().await?;
        self.authenticate(conn, &mut upstream, &user, password)
            .await?;

        conn.write_all(&authentication_ok()).await?;
        conn.flush().await?;
        tokio::io::copy_bidirectional(conn, &mut upstream)
            .await
            .map_err(|err| Refusal::Told(HandlerError::Io(err)))?;
        Ok(())
    }

    async fn connect_upstream(
        &self,
        guard: &UpstreamGuard,
        ctx: &ConnCtx,
        fields: &CredentialFields,
        host: &str,
        port: u16,
    ) -> Result<Box<dyn AsyncStream>, Refusal> {
        let (mut stream, _addr) = guard
            .connect_checked(Self::NAME, host, port, &ctx.egress)
            .await
            .map_err(|err| {
                let reason = err.reason();
                Refusal::Tell(
                    SQLSTATE_INVALID_AUTHORIZATION,
                    format!(
                        "the broker could not reach the upstream for this credential ({reason})"
                    ),
                    HandlerError::Upstream(err),
                )
            })?;
        if !upstream_tls_wanted(ctx, fields) {
            return Ok(Box::new(stream));
        }
        stream.write_all(&ssl_request()).await?;
        stream.flush().await?;
        let mut answer = [0u8; 1];
        stream.read_exact(&mut answer).await?;
        if answer[0] != b'S' {
            return Err(tell(
                SQLSTATE_INVALID_AUTHORIZATION,
                "the upstream refused TLS and this endpoint requires it",
            ));
        }
        let stream = self.upstream.connect(host, stream).await.map_err(|err| {
            tell(
                SQLSTATE_INVALID_AUTHORIZATION,
                format!("the upstream's certificate did not verify for {host}: {err}"),
            )
        })?;
        Ok(Box::new(stream))
    }

    /// Completes the upstream's authentication with the brokered credential.
    /// An `ErrorResponse` from the upstream reaches the guest verbatim, so a
    /// wrong password or a missing database says what it says.
    async fn authenticate(
        &self,
        conn: &mut Box<dyn AsyncStream>,
        upstream: &mut Box<dyn AsyncStream>,
        user: &str,
        password: &str,
    ) -> Result<(), Refusal> {
        let mut scram: Option<ScramClient> = None;
        loop {
            let message = read_message(upstream).await?;
            match message.tag {
                b'E' => {
                    conn.write_all(&message.raw).await?;
                    conn.flush().await?;
                    return Err(Refusal::Told(HandlerError::Protocol(
                        "the upstream refused the brokered credential".into(),
                    )));
                }
                b'R' => {
                    let kind = be_u32(&message.body, 0).ok_or_else(|| {
                        tell(
                            SQLSTATE_INVALID_AUTHORIZATION,
                            "the upstream sent a malformed authentication request",
                        )
                    })?;
                    match kind {
                        0 => return Ok(()),
                        10 => {
                            if !mechanisms(&message.body[4..])
                                .any(|mechanism| mechanism == "SCRAM-SHA-256")
                            {
                                return Err(tell(
                                    SQLSTATE_INVALID_AUTHORIZATION,
                                    "the upstream offers no SCRAM-SHA-256; this broker \
                                     authenticates with nothing weaker",
                                ));
                            }
                            let client = ScramClient::new(user, password, fresh_nonce());
                            let first = client.client_first();
                            upstream
                                .write_all(&sasl_initial_response("SCRAM-SHA-256", &first))
                                .await?;
                            upstream.flush().await?;
                            scram = Some(client);
                        }
                        11 => {
                            let client = scram.as_mut().ok_or_else(|| {
                                tell(
                                    SQLSTATE_INVALID_AUTHORIZATION,
                                    "the upstream continued a SASL exchange that never started",
                                )
                            })?;
                            let server_first = utf8(&message.body[4..])?;
                            let final_message =
                                client.client_final(&server_first).map_err(|err| {
                                    tell(
                                        SQLSTATE_INVALID_AUTHORIZATION,
                                        format!(
                                            "the upstream's SCRAM exchange is not usable: {err}"
                                        ),
                                    )
                                })?;
                            upstream.write_all(&sasl_response(&final_message)).await?;
                            upstream.flush().await?;
                        }
                        12 => {
                            let client = scram.as_mut().ok_or_else(|| {
                                tell(
                                    SQLSTATE_INVALID_AUTHORIZATION,
                                    "the upstream finished a SASL exchange that never started",
                                )
                            })?;
                            let server_final = utf8(&message.body[4..])?;
                            client.verify_server_final(&server_final).map_err(|err| {
                                tell(
                                    SQLSTATE_INVALID_AUTHORIZATION,
                                    format!(
                                        "the upstream did not prove it knows the credential: {err}"
                                    ),
                                )
                            })?;
                        }
                        other => {
                            return Err(tell(
                                SQLSTATE_INVALID_AUTHORIZATION,
                                format!(
                                    "the upstream asked for authentication method {other}; this \
                                     broker sends a credential over SCRAM-SHA-256 only"
                                ),
                            ))
                        }
                    }
                }
                other => {
                    return Err(tell(
                        SQLSTATE_INVALID_AUTHORIZATION,
                        format!(
                            "the upstream sent message {:?} before authenticating",
                            other as char
                        ),
                    ))
                }
            }
        }
    }
}

/// `upstream_tls` defaults to on: a credential reaching an upstream over a
/// network that is not the namespace hop is the case worth defaulting for.
fn upstream_tls_wanted(ctx: &ConnCtx, fields: &CredentialFields) -> bool {
    if let Some(sslmode) = fields.get("sslmode") {
        return !sslmode.eq_ignore_ascii_case("disable");
    }
    ctx.params
        .get("upstream_tls")
        .and_then(|value| value.as_bool())
        .unwrap_or(true)
}

fn upstream_of(fields: &CredentialFields) -> Result<(&str, u16), Refusal> {
    let host = fields.get("host").ok_or_else(|| {
        tell(
            SQLSTATE_INVALID_AUTHORIZATION,
            "the credential names no upstream host",
        )
    })?;
    let port = match fields.get("port") {
        Some(port) => port
            .parse::<u16>()
            .ok()
            .filter(|port| *port != 0)
            .ok_or_else(|| {
                tell(
                    SQLSTATE_INVALID_AUTHORIZATION,
                    "the credential names an invalid upstream port",
                )
            })?,
        None => 5432,
    };
    Ok((host, port))
}

/// Every field the source returned, except the ones that configure the
/// connection, replaces the guest's parameter of that name; the rest of the
/// guest's parameters are kept in the order it sent them.
fn merge_params(
    mut params: Vec<(String, String)>,
    fields: &CredentialFields,
) -> Vec<(String, String)> {
    for name in fields.names() {
        if CONNECTION_FIELDS.contains(&name) {
            continue;
        }
        let Some(value) = fields.get(name) else {
            continue;
        };
        match params.iter_mut().find(|(key, _)| key == name) {
            Some((_, existing)) => *existing = value.to_string(),
            None => params.push((name.to_string(), value.to_string())),
        }
    }
    params
}

struct StartupPacket {
    code: u32,
    body: Vec<u8>,
    raw: Vec<u8>,
}

async fn read_startup(stream: &mut Box<dyn AsyncStream>) -> Result<StartupPacket, Refusal> {
    let mut header = [0u8; 4];
    stream.read_exact(&mut header).await?;
    let len = u32::from_be_bytes(header) as usize;
    if !(8..=MAX_STARTUP_LEN).contains(&len) {
        return Err(tell(
            SQLSTATE_FEATURE_NOT_SUPPORTED,
            format!("a startup packet of {len} bytes is not accepted"),
        ));
    }
    let mut rest = vec![0u8; len - 4];
    stream.read_exact(&mut rest).await?;
    let code = u32::from_be_bytes([rest[0], rest[1], rest[2], rest[3]]);
    let mut raw = header.to_vec();
    raw.extend_from_slice(&rest);
    Ok(StartupPacket {
        code,
        body: rest[4..].to_vec(),
        raw,
    })
}

struct BackendMessage {
    tag: u8,
    body: Vec<u8>,
    raw: Vec<u8>,
}

async fn read_message(stream: &mut Box<dyn AsyncStream>) -> Result<BackendMessage, Refusal> {
    let mut header = [0u8; 5];
    stream.read_exact(&mut header).await?;
    let len = u32::from_be_bytes([header[1], header[2], header[3], header[4]]) as usize;
    if !(4..=MAX_MESSAGE_LEN).contains(&len) {
        return Err(tell(
            SQLSTATE_INVALID_AUTHORIZATION,
            format!("the upstream sent a {len} byte message during authentication"),
        ));
    }
    let mut body = vec![0u8; len - 4];
    stream.read_exact(&mut body).await?;
    let mut raw = header.to_vec();
    raw.extend_from_slice(&body);
    Ok(BackendMessage {
        tag: header[0],
        body,
        raw,
    })
}

/// Null-terminated key/value pairs ending in one empty key. A pair the guest
/// left unterminated is a malformed packet, not a silently dropped parameter.
fn parse_startup_params(body: &[u8]) -> Result<Vec<(String, String)>, Refusal> {
    let mut params = Vec::new();
    let mut parts = body.split(|byte| *byte == 0);
    loop {
        let Some(key) = parts.next() else {
            return Err(tell(
                SQLSTATE_FEATURE_NOT_SUPPORTED,
                "the startup packet ends inside a parameter",
            ));
        };
        if key.is_empty() {
            return Ok(params);
        }
        let Some(value) = parts.next() else {
            return Err(tell(
                SQLSTATE_FEATURE_NOT_SUPPORTED,
                "the startup packet ends inside a parameter",
            ));
        };
        params.push((utf8(key)?, utf8(value)?));
    }
}

fn utf8(bytes: &[u8]) -> Result<String, Refusal> {
    String::from_utf8(bytes.to_vec()).map_err(|_| {
        tell(
            SQLSTATE_FEATURE_NOT_SUPPORTED,
            "a startup parameter is not valid UTF-8",
        )
    })
}

fn startup_message(params: &[(String, String)]) -> Vec<u8> {
    let mut body = PROTOCOL_VERSION_3.to_be_bytes().to_vec();
    for (key, value) in params {
        body.extend_from_slice(key.as_bytes());
        body.push(0);
        body.extend_from_slice(value.as_bytes());
        body.push(0);
    }
    body.push(0);
    let mut message = ((body.len() + 4) as u32).to_be_bytes().to_vec();
    message.extend_from_slice(&body);
    message
}

fn ssl_request() -> Vec<u8> {
    let mut message = 8u32.to_be_bytes().to_vec();
    message.extend_from_slice(&SSL_REQUEST_CODE.to_be_bytes());
    message
}

fn authentication_ok() -> Vec<u8> {
    let mut message = vec![b'R'];
    message.extend_from_slice(&8u32.to_be_bytes());
    message.extend_from_slice(&0u32.to_be_bytes());
    message
}

fn sasl_initial_response(mechanism: &str, response: &str) -> Vec<u8> {
    let mut body = mechanism.as_bytes().to_vec();
    body.push(0);
    body.extend_from_slice(&(response.len() as u32).to_be_bytes());
    body.extend_from_slice(response.as_bytes());
    framed(b'p', &body)
}

fn sasl_response(response: &str) -> Vec<u8> {
    framed(b'p', response.as_bytes())
}

fn framed(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut message = vec![tag];
    message.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
    message.extend_from_slice(body);
    message
}

/// `ErrorResponse` with severity, SQLSTATE and message: the shape every
/// Postgres client reports as a connection failure.
fn error_response(code: &str, message: &str) -> Vec<u8> {
    let mut body = Vec::new();
    for (field, value) in [
        (b'S', "FATAL"),
        (b'V', "FATAL"),
        (b'C', code),
        (b'M', message),
    ] {
        body.push(field);
        body.extend_from_slice(value.as_bytes());
        body.push(0);
    }
    body.push(0);
    framed(b'E', &body)
}

fn mechanisms(body: &[u8]) -> impl Iterator<Item = String> + '_ {
    body.split(|byte| *byte == 0)
        .take_while(|part| !part.is_empty())
        .map(|part| String::from_utf8_lossy(part).into_owned())
}

fn be_u32(bytes: &[u8], at: usize) -> Option<u32> {
    let slice = bytes.get(at..at + 4)?;
    Some(u32::from_be_bytes([slice[0], slice[1], slice[2], slice[3]]))
}

fn fresh_nonce() -> String {
    use base64::Engine as _;
    let raw: [u8; 18] = rand::random();
    base64::engine::general_purpose::STANDARD.encode(raw)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    use base64::Engine as _;
    use hmac::{Hmac, Mac};
    use sha2::{Digest, Sha256};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    use super::*;
    use crate::credential::{CredentialFields, NoCredentials, StaticSource};
    use crate::header::test_support::sample_header;
    use crate::policy::BrokerDenyList;

    type HmacSha256 = Hmac<Sha256>;

    const UPSTREAM_PASSWORD: &str = "operator-password";

    fn mac(key: &[u8], message: &[u8]) -> [u8; 32] {
        let mut mac = HmacSha256::new_from_slice(key).unwrap();
        mac.update(message);
        mac.finalize().into_bytes().into()
    }

    fn pbkdf2(password: &[u8], salt: &[u8], iterations: u32) -> [u8; 32] {
        let mut block = salt.to_vec();
        block.extend_from_slice(&1u32.to_be_bytes());
        let mut current = mac(password, &block);
        let mut result = current;
        for _ in 1..iterations {
            current = mac(password, &current);
            for (out, next) in result.iter_mut().zip(current.iter()) {
                *out ^= next;
            }
        }
        result
    }

    /// What the fake upstream recorded about the connection it served.
    #[derive(Clone, Default)]
    struct Upstream {
        params: Arc<Mutex<Vec<(String, String)>>>,
        raw_first_packet: Arc<Mutex<Vec<u8>>>,
        auth: Arc<Mutex<UpstreamAuth>>,
        connections: Arc<Mutex<usize>>,
    }

    #[derive(Clone, Copy, Default, PartialEq, Eq)]
    enum UpstreamAuth {
        #[default]
        Scram,
        Cleartext,
        RefuseWithError,
    }

    async fn read_exactly(stream: &mut TcpStream, len: usize) -> Vec<u8> {
        let mut buf = vec![0u8; len];
        stream.read_exact(&mut buf).await.unwrap();
        buf
    }

    async fn read_startup_packet(stream: &mut TcpStream) -> (u32, Vec<u8>, Vec<u8>) {
        let header = read_exactly(stream, 4).await;
        let len = u32::from_be_bytes([header[0], header[1], header[2], header[3]]) as usize;
        let rest = read_exactly(stream, len - 4).await;
        let code = u32::from_be_bytes([rest[0], rest[1], rest[2], rest[3]]);
        let mut raw = header.clone();
        raw.extend_from_slice(&rest);
        (code, rest[4..].to_vec(), raw)
    }

    async fn read_tagged(stream: &mut TcpStream) -> (u8, Vec<u8>) {
        let header = read_exactly(stream, 5).await;
        let len = u32::from_be_bytes([header[1], header[2], header[3], header[4]]) as usize;
        (header[0], read_exactly(stream, len - 4).await)
    }

    async fn serve_upstream(listener: TcpListener, upstream: Upstream) {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            *upstream.connections.lock().unwrap() += 1;
            let (code, body, raw) = read_startup_packet(&mut stream).await;
            *upstream.raw_first_packet.lock().unwrap() = raw;
            if code == SSL_REQUEST_CODE {
                stream.write_all(b"N").await.unwrap();
                continue;
            }
            if code == CANCEL_REQUEST_CODE {
                continue;
            }
            let params: Vec<(String, String)> = body
                .split(|byte| *byte == 0)
                .collect::<Vec<_>>()
                .chunks(2)
                .filter(|pair| pair.len() == 2 && !pair[0].is_empty())
                .map(|pair| {
                    (
                        String::from_utf8_lossy(pair[0]).into_owned(),
                        String::from_utf8_lossy(pair[1]).into_owned(),
                    )
                })
                .collect();
            *upstream.params.lock().unwrap() = params;

            let auth = *upstream.auth.lock().unwrap();
            match auth {
                UpstreamAuth::RefuseWithError => {
                    stream
                        .write_all(&error_response("28P01", "password authentication failed"))
                        .await
                        .unwrap();
                    continue;
                }
                UpstreamAuth::Cleartext => {
                    let mut body = 3u32.to_be_bytes().to_vec();
                    body.truncate(4);
                    stream.write_all(&framed(b'R', &body)).await.unwrap();
                    continue;
                }
                UpstreamAuth::Scram => {}
            }

            let mut offer = 10u32.to_be_bytes().to_vec();
            offer.extend_from_slice(b"SCRAM-SHA-256\0\0");
            stream.write_all(&framed(b'R', &offer)).await.unwrap();

            let (_, initial) = read_tagged(&mut stream).await;
            let split = initial.iter().position(|byte| *byte == 0).unwrap();
            let client_first = String::from_utf8(initial[split + 5..].to_vec()).unwrap();
            let client_first_bare = client_first.strip_prefix("n,,").unwrap().to_string();
            let client_nonce = client_first_bare
                .split(',')
                .find_map(|part| part.strip_prefix("r="))
                .unwrap()
                .to_string();

            let salt = b"0123456789abcdef";
            let nonce = format!("{client_nonce}server");
            let server_first = format!(
                "r={nonce},s={},i=4096",
                base64::engine::general_purpose::STANDARD.encode(salt)
            );
            let mut cont = 11u32.to_be_bytes().to_vec();
            cont.extend_from_slice(server_first.as_bytes());
            stream.write_all(&framed(b'R', &cont)).await.unwrap();

            let (_, response) = read_tagged(&mut stream).await;
            let client_final = String::from_utf8(response).unwrap();
            let without_proof = client_final.rsplit_once(",p=").unwrap().0.to_string();
            let proof = base64::engine::general_purpose::STANDARD
                .decode(client_final.rsplit_once(",p=").unwrap().1)
                .unwrap();

            let salted = pbkdf2(UPSTREAM_PASSWORD.as_bytes(), salt, 4096);
            let client_key = mac(&salted, b"Client Key");
            let stored_key: [u8; 32] = Sha256::digest(client_key).into();
            let auth_message = format!("{client_first_bare},{server_first},{without_proof}");
            let client_signature = mac(&stored_key, auth_message.as_bytes());
            let recovered: Vec<u8> = proof
                .iter()
                .zip(client_signature.iter())
                .map(|(p, s)| p ^ s)
                .collect();
            assert_eq!(recovered, client_key, "the client proof must verify");

            let server_key = mac(&salted, b"Server Key");
            let signature = mac(&server_key, auth_message.as_bytes());
            let mut done = 12u32.to_be_bytes().to_vec();
            done.extend_from_slice(
                format!(
                    "v={}",
                    base64::engine::general_purpose::STANDARD.encode(signature)
                )
                .as_bytes(),
            );
            stream.write_all(&framed(b'R', &done)).await.unwrap();
            stream.write_all(&authentication_ok()).await.unwrap();

            // Prove the byte path after authentication: echo whatever follows.
            let mut buf = vec![0u8; 64];
            if let Ok(read) = stream.read(&mut buf).await {
                if read > 0 {
                    stream.write_all(&buf[..read]).await.unwrap();
                }
            }
        }
    }

    async fn upstream() -> (Upstream, u16) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let upstream = Upstream::default();
        tokio::spawn(serve_upstream(listener, upstream.clone()));
        (upstream, port)
    }

    fn ctx_for(credential: Option<&str>) -> ConnCtx {
        let mut header = sample_header();
        header.handler = PostgresHandler::NAME.into();
        header.params = match credential {
            Some(name) => serde_json::json!({"credential": name, "upstream_tls": false}),
            None => serde_json::json!({"upstream_tls": false}),
        };
        ConnCtx::from(&header)
    }

    fn guard() -> Arc<UpstreamGuard> {
        Arc::new(
            UpstreamGuard::new(BrokerDenyList::empty())
                .with_allowlist(PostgresHandler::NAME, &["127.0.0.1/32"])
                .unwrap(),
        )
    }

    fn source(port: u16, extra: &[(&str, &str)]) -> Arc<dyn CredentialSource> {
        let mut fields = vec![
            ("host", "127.0.0.1"),
            ("port", Box::leak(port.to_string().into_boxed_str()) as &str),
            ("password", UPSTREAM_PASSWORD),
        ];
        fields.extend_from_slice(extra);
        Arc::new(StaticSource::default().with_fields("sbx-1", "exec-1", "db", &fields))
    }

    fn startup(params: &[(&str, &str)]) -> Vec<u8> {
        startup_message(
            &params
                .iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect::<Vec<_>>(),
        )
    }

    /// Runs the handler on one end of a duplex and returns the other.
    fn serve(
        creds: Arc<dyn CredentialSource>,
        ctx: ConnCtx,
    ) -> (
        tokio::io::DuplexStream,
        tokio::task::JoinHandle<Result<(), HandlerError>>,
    ) {
        let (mine, theirs) = tokio::io::duplex(8192);
        let served = tokio::spawn(async move {
            PostgresHandler::new()
                .unwrap()
                .handle(Box::new(theirs), ctx, creds, guard())
                .await
        });
        (mine, served)
    }

    async fn read_guest_message(guest: &mut tokio::io::DuplexStream) -> (u8, Vec<u8>) {
        let mut header = [0u8; 5];
        guest.read_exact(&mut header).await.unwrap();
        let len = u32::from_be_bytes([header[1], header[2], header[3], header[4]]) as usize;
        let mut body = vec![0u8; len - 4];
        guest.read_exact(&mut body).await.unwrap();
        (header[0], body)
    }

    fn error_fields(body: &[u8]) -> BTreeMap<char, String> {
        body.split(|byte| *byte == 0)
            .filter(|part| !part.is_empty())
            .map(|part| {
                (
                    part[0] as char,
                    String::from_utf8_lossy(&part[1..]).into_owned(),
                )
            })
            .collect()
    }

    #[tokio::test]
    async fn a_placeholder_dsn_reaches_the_upstream_under_the_credential_account() {
        let (upstream, port) = upstream().await;
        let (mut guest, served) = serve(source(port, &[("user", "rw_app")]), ctx_for(Some("db")));

        guest
            .write_all(&startup(&[("user", "anything"), ("database", "tenant7")]))
            .await
            .unwrap();
        let (tag, body) = read_guest_message(&mut guest).await;
        assert_eq!(tag, b'R');
        assert_eq!(be_u32(&body, 0), Some(0), "the guest is told it is in");

        guest.write_all(b"hello").await.unwrap();
        let mut echoed = [0u8; 5];
        guest.read_exact(&mut echoed).await.unwrap();
        assert_eq!(&echoed, b"hello", "bytes flow after authentication");

        let params = upstream.params.lock().unwrap().clone();
        assert!(
            params.contains(&("user".into(), "rw_app".into())),
            "the credential's user replaces the guest's: {params:?}"
        );
        assert!(
            !params.iter().any(|(_, value)| value == "anything"),
            "the guest's placeholder must not reach the upstream: {params:?}"
        );
        drop(guest);
        let _ = served.await;
    }

    #[tokio::test]
    async fn a_field_the_credential_omits_is_passed_through_as_the_guest_wrote_it() {
        let (upstream, port) = upstream().await;
        let (mut guest, served) = serve(source(port, &[("user", "rw_app")]), ctx_for(Some("db")));

        guest
            .write_all(&startup(&[
                ("user", "anything"),
                ("database", "tenant7"),
                ("options", "-csearch_path=app7"),
                ("application_name", "agent"),
            ]))
            .await
            .unwrap();
        read_guest_message(&mut guest).await;

        let params = upstream.params.lock().unwrap().clone();
        for expected in [
            ("database", "tenant7"),
            ("options", "-csearch_path=app7"),
            ("application_name", "agent"),
        ] {
            assert!(
                params.contains(&(expected.0.into(), expected.1.into())),
                "{expected:?} must survive: {params:?}"
            );
        }
        drop(guest);
        let _ = served.await;
    }

    #[tokio::test]
    async fn a_credential_field_overrides_the_guest_value_of_the_same_name() {
        let (upstream, port) = upstream().await;
        let (mut guest, served) = serve(
            source(port, &[("user", "rw_app"), ("database", "operator_db")]),
            ctx_for(Some("db")),
        );

        guest
            .write_all(&startup(&[("user", "anything"), ("database", "tenant7")]))
            .await
            .unwrap();
        read_guest_message(&mut guest).await;

        let params = upstream.params.lock().unwrap().clone();
        assert!(params.contains(&("database".into(), "operator_db".into())));
        assert!(!params.contains(&("database".into(), "tenant7".into())));
        drop(guest);
        let _ = served.await;
    }

    #[tokio::test]
    async fn an_ssl_request_is_refused_before_the_startup_packet_is_read() {
        let (_upstream, port) = upstream().await;
        let (mut guest, served) = serve(source(port, &[("user", "rw_app")]), ctx_for(Some("db")));

        guest.write_all(&ssl_request()).await.unwrap();
        let mut answer = [0u8; 1];
        guest.read_exact(&mut answer).await.unwrap();
        assert_eq!(&answer, b"N");

        guest.write_all(&startup(&[("user", "x")])).await.unwrap();
        assert_eq!(read_guest_message(&mut guest).await.0, b'R');
        drop(guest);
        let _ = served.await;
    }

    #[tokio::test]
    async fn a_replication_startup_is_refused_and_no_upstream_is_dialled() {
        let (upstream, port) = upstream().await;
        let (mut guest, served) = serve(source(port, &[("user", "rw_app")]), ctx_for(Some("db")));

        guest
            .write_all(&startup(&[("user", "x"), ("replication", "database")]))
            .await
            .unwrap();
        let (tag, body) = read_guest_message(&mut guest).await;
        assert_eq!(tag, b'E');
        let fields = error_fields(&body);
        assert_eq!(fields[&'C'], SQLSTATE_FEATURE_NOT_SUPPORTED);
        assert!(fields[&'M'].contains("replication"), "{:?}", fields[&'M']);
        assert_eq!(*upstream.connections.lock().unwrap(), 0);
        let _ = served.await;
    }

    #[tokio::test]
    async fn without_a_grant_the_guest_is_told_28000_and_nothing_is_dialled() {
        let (upstream, _port) = upstream().await;
        let (mut guest, served) = serve(Arc::new(NoCredentials), ctx_for(Some("db")));

        guest.write_all(&startup(&[("user", "x")])).await.unwrap();
        let (tag, body) = read_guest_message(&mut guest).await;
        assert_eq!(tag, b'E');
        assert_eq!(error_fields(&body)[&'C'], SQLSTATE_INVALID_AUTHORIZATION);
        assert_eq!(*upstream.connections.lock().unwrap(), 0);
        let _ = served.await;
    }

    #[tokio::test]
    async fn an_endpoint_without_a_credential_name_is_told_28000() {
        let (_upstream, port) = upstream().await;
        let (mut guest, served) = serve(source(port, &[("user", "u")]), ctx_for(None));

        guest.write_all(&startup(&[("user", "x")])).await.unwrap();
        let (tag, body) = read_guest_message(&mut guest).await;
        assert_eq!(tag, b'E');
        assert_eq!(error_fields(&body)[&'C'], SQLSTATE_INVALID_AUTHORIZATION);
        let _ = served.await;
    }

    #[tokio::test]
    async fn an_upstream_error_reaches_the_guest_verbatim() {
        let (upstream, port) = upstream().await;
        *upstream.auth.lock().unwrap() = UpstreamAuth::RefuseWithError;
        let (mut guest, served) = serve(source(port, &[("user", "rw_app")]), ctx_for(Some("db")));

        guest.write_all(&startup(&[("user", "x")])).await.unwrap();
        let (tag, body) = read_guest_message(&mut guest).await;
        assert_eq!(tag, b'E');
        let fields = error_fields(&body);
        assert_eq!(fields[&'C'], "28P01");
        assert_eq!(fields[&'M'], "password authentication failed");
        let _ = served.await;
    }

    #[tokio::test]
    async fn an_upstream_asking_for_a_cleartext_password_never_gets_one() {
        let (upstream, port) = upstream().await;
        *upstream.auth.lock().unwrap() = UpstreamAuth::Cleartext;
        let (mut guest, served) = serve(source(port, &[("user", "rw_app")]), ctx_for(Some("db")));

        guest.write_all(&startup(&[("user", "x")])).await.unwrap();
        let (tag, body) = read_guest_message(&mut guest).await;
        assert_eq!(tag, b'E');
        let fields = error_fields(&body);
        assert_eq!(fields[&'C'], SQLSTATE_INVALID_AUTHORIZATION);
        assert!(fields[&'M'].contains("SCRAM-SHA-256"), "{:?}", fields[&'M']);
        let _ = served.await;
    }

    #[tokio::test]
    async fn a_cancel_request_is_forwarded_to_the_same_upstream_verbatim() {
        let (upstream, port) = upstream().await;
        let (mut guest, served) = serve(source(port, &[("user", "rw_app")]), ctx_for(Some("db")));

        let mut cancel = 16u32.to_be_bytes().to_vec();
        cancel.extend_from_slice(&CANCEL_REQUEST_CODE.to_be_bytes());
        cancel.extend_from_slice(&4242u32.to_be_bytes());
        cancel.extend_from_slice(&99u32.to_be_bytes());
        guest.write_all(&cancel).await.unwrap();
        served.await.unwrap().unwrap();

        // The handler is done once it has written; the upstream still has to
        // read what it was sent.
        for _ in 0..200 {
            if !upstream.raw_first_packet.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        assert_eq!(*upstream.raw_first_packet.lock().unwrap(), cancel);
    }

    #[tokio::test]
    async fn a_credential_that_names_no_host_is_told_28000() {
        let source: Arc<dyn CredentialSource> = Arc::new(StaticSource::default().with_fields(
            "sbx-1",
            "exec-1",
            "db",
            &[("user", "u"), ("password", "p")],
        ));
        let (mut guest, served) = serve(source, ctx_for(Some("db")));
        guest.write_all(&startup(&[("user", "x")])).await.unwrap();
        let (tag, body) = read_guest_message(&mut guest).await;
        assert_eq!(tag, b'E');
        let fields = error_fields(&body);
        assert_eq!(fields[&'C'], SQLSTATE_INVALID_AUTHORIZATION);
        assert!(fields[&'M'].contains("host"), "{:?}", fields[&'M']);
        let _ = served.await;
    }

    #[tokio::test]
    async fn a_credential_that_carries_no_password_is_told_28000() {
        let source: Arc<dyn CredentialSource> = Arc::new(StaticSource::default().with_fields(
            "sbx-1",
            "exec-1",
            "db",
            &[("host", "127.0.0.1"), ("user", "u")],
        ));
        let (mut guest, served) = serve(source, ctx_for(Some("db")));
        guest.write_all(&startup(&[("user", "x")])).await.unwrap();
        let (tag, body) = read_guest_message(&mut guest).await;
        assert_eq!(tag, b'E');
        assert!(error_fields(&body)[&'M'].contains("password"));
        let _ = served.await;
    }

    #[test]
    fn a_field_set_overrides_only_the_parameters_it_names() {
        let fields = CredentialFields::new(
            BTreeMap::from([
                ("host".into(), "db.internal".into()),
                ("port".into(), "5432".into()),
                ("password".into(), "p".into()),
                ("user".into(), "rw_app".into()),
                ("options".into(), "-csearch_path=operator".into()),
            ]),
            None,
        );
        let merged = merge_params(
            vec![
                ("user".into(), "placeholder".into()),
                ("database".into(), "tenant7".into()),
                ("options".into(), "-csearch_path=guest".into()),
            ],
            &fields,
        );

        assert_eq!(
            merged,
            vec![
                ("user".to_string(), "rw_app".to_string()),
                ("database".to_string(), "tenant7".to_string()),
                ("options".to_string(), "-csearch_path=operator".to_string()),
            ],
            "connection fields never become startup parameters, and order is the guest's"
        );
    }

    #[test]
    fn a_credential_field_the_guest_never_sent_is_appended() {
        let fields = CredentialFields::new(
            BTreeMap::from([("database".into(), "operator_db".into())]),
            None,
        );
        let merged = merge_params(vec![("user".into(), "u".into())], &fields);
        assert_eq!(
            merged,
            vec![
                ("user".to_string(), "u".to_string()),
                ("database".to_string(), "operator_db".to_string()),
            ]
        );
    }

    #[test]
    fn upstream_tls_is_on_unless_the_declaration_or_the_credential_turns_it_off() {
        let no_fields = CredentialFields::default();
        let mut header = sample_header();
        header.params = serde_json::json!({"credential": "db"});
        assert!(upstream_tls_wanted(&ConnCtx::from(&header), &no_fields));

        header.params = serde_json::json!({"credential": "db", "upstream_tls": false});
        assert!(!upstream_tls_wanted(&ConnCtx::from(&header), &no_fields));

        // The credential is the more specific statement and wins either way.
        let disabled =
            CredentialFields::new(BTreeMap::from([("sslmode".into(), "disable".into())]), None);
        header.params = serde_json::json!({"credential": "db", "upstream_tls": true});
        assert!(!upstream_tls_wanted(&ConnCtx::from(&header), &disabled));

        let required =
            CredentialFields::new(BTreeMap::from([("sslmode".into(), "require".into())]), None);
        header.params = serde_json::json!({"credential": "db", "upstream_tls": false});
        assert!(upstream_tls_wanted(&ConnCtx::from(&header), &required));
    }

    #[test]
    fn a_startup_packet_that_ends_inside_a_parameter_is_refused() {
        assert!(parse_startup_params(b"user\0alice\0").is_ok());
        assert!(parse_startup_params(b"user\0alice").is_err());
        assert!(parse_startup_params(b"user\0").is_err());
    }

    #[test]
    fn an_error_response_carries_the_sqlstate_a_client_reports() {
        let bytes = error_response(SQLSTATE_INVALID_AUTHORIZATION, "no grant");
        assert_eq!(bytes[0], b'E');
        let fields = error_fields(&bytes[5..]);
        assert_eq!(fields[&'C'], "28000");
        assert_eq!(fields[&'S'], "FATAL");
        assert_eq!(fields[&'M'], "no grant");
    }
}
