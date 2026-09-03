use std::sync::Arc;

use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::dispatch::Dispatcher;
use crate::header::IdentityHeader;

/// A bidirectional byte stream a handler can own.
pub trait AsyncStream: AsyncRead + AsyncWrite + Send + Unpin {}

impl<T: AsyncRead + AsyncWrite + Send + Unpin> AsyncStream for T {}

#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    /// The broker answered and said no; `reason` is its wire reason.
    #[error("broker rejected the connection: {reason}")]
    Rejected { reason: String },
    /// The broker could not be reached or did not answer the header.
    #[error("broker unavailable: {0}")]
    Unavailable(String),
}

/// The runtime's way to the broker: one call yields the stream the guest's
/// bytes are relayed over, or the broker's refusal.
#[async_trait]
pub trait BrokerTransport: Send + Sync {
    async fn open(&self, header: IdentityHeader) -> Result<Box<dyn AsyncStream>, TransportError>;
}

/// Runs the dispatcher in this process over an in-memory pipe. The header
/// never leaves the process, so it is not signed or replay-checked here.
pub struct EmbeddedTransport {
    dispatcher: Arc<Dispatcher>,
    buffer: usize,
}

impl EmbeddedTransport {
    pub fn new(dispatcher: Arc<Dispatcher>) -> Self {
        Self {
            dispatcher,
            buffer: 64 * 1024,
        }
    }
}

#[async_trait]
impl BrokerTransport for EmbeddedTransport {
    async fn open(&self, header: IdentityHeader) -> Result<Box<dyn AsyncStream>, TransportError> {
        let handler =
            self.dispatcher
                .accept(&header)
                .map_err(|reject| TransportError::Rejected {
                    reason: reject.reason().to_string(),
                })?;
        let (runtime_side, broker_side) = tokio::io::duplex(self.buffer);
        let dispatcher = self.dispatcher.clone();
        tokio::spawn(async move {
            if let Err(err) = dispatcher
                .serve(handler, &header, Box::new(broker_side))
                .await
            {
                tracing::debug!(
                    sandbox_id = %header.sandbox_id,
                    handler = %header.handler,
                    error = %err,
                    "embedded brokered connection ended with an error"
                );
            }
        });
        Ok(Box::new(runtime_side))
    }
}

/// The runtime's TLS client to the `aenv-egress` deployment: verifies the
/// broker against the cluster CA, signs the header with the shared key and
/// hands the stream back once the broker acknowledged it.
#[cfg(feature = "remote")]
pub struct RemoteTransport {
    endpoint: String,
    server_name: String,
    connector: tokio_native_tls::TlsConnector,
    key: zeroize::Zeroizing<Vec<u8>>,
    connect_timeout: std::time::Duration,
}

#[cfg(feature = "remote")]
impl RemoteTransport {
    /// `endpoint` is `host:port`; the broker certificate is verified for
    /// `host` unless `server_name` overrides it. `ca_pem` may hold several
    /// certificates for a rotation.
    pub fn new(
        endpoint: &str,
        server_name: Option<&str>,
        ca_pem: &[u8],
        shared_secret: &[u8],
    ) -> anyhow::Result<Self> {
        let host = endpoint
            .rsplit_once(':')
            .map(|(host, _)| host.trim_matches(['[', ']']))
            .filter(|host| !host.is_empty())
            .ok_or_else(|| anyhow::anyhow!("egress broker endpoint must be host:port"))?;
        let mut builder = native_tls::TlsConnector::builder();
        builder.min_protocol_version(Some(native_tls::Protocol::Tlsv12));
        builder.disable_built_in_roots(true);
        let mut added = 0;
        for block in pem_blocks(ca_pem) {
            builder.add_root_certificate(native_tls::Certificate::from_pem(block.as_bytes())?);
            added += 1;
        }
        if added == 0 {
            anyhow::bail!("egress broker CA bundle holds no certificate");
        }
        Ok(Self {
            endpoint: endpoint.to_string(),
            server_name: server_name.unwrap_or(host).to_string(),
            connector: tokio_native_tls::TlsConnector::from(builder.build()?),
            key: zeroize::Zeroizing::new(shared_secret.to_vec()),
            connect_timeout: std::time::Duration::from_secs(5),
        })
    }

    pub fn with_connect_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    async fn connect(
        &self,
    ) -> Result<tokio_native_tls::TlsStream<tokio::net::TcpStream>, TransportError> {
        let tcp = tokio::time::timeout(
            self.connect_timeout,
            tokio::net::TcpStream::connect(&self.endpoint),
        )
        .await
        .map_err(|_| {
            TransportError::Unavailable(format!("connecting to {} timed out", self.endpoint))
        })?
        .map_err(|err| {
            TransportError::Unavailable(format!("connecting to {}: {err}", self.endpoint))
        })?;
        tokio::time::timeout(
            self.connect_timeout,
            self.connector.connect(&self.server_name, tcp),
        )
        .await
        .map_err(|_| TransportError::Unavailable("tls handshake with the broker timed out".into()))?
        .map_err(|err| TransportError::Unavailable(format!("tls handshake with the broker: {err}")))
    }

    /// A TLS handshake and nothing else: the heartbeat's reachability check.
    pub async fn probe(&self) -> Result<(), TransportError> {
        let mut tls = self.connect().await?;
        let _ = tokio::io::AsyncWriteExt::shutdown(&mut tls).await;
        Ok(())
    }
}

#[cfg(feature = "remote")]
#[async_trait]
impl BrokerTransport for RemoteTransport {
    async fn open(
        &self,
        mut header: IdentityHeader,
    ) -> Result<Box<dyn AsyncStream>, TransportError> {
        header.sign(&self.key);
        let mut tls = self.connect().await?;
        crate::framing::write_json(&mut tls, &header)
            .await
            .map_err(|err| {
                TransportError::Unavailable(format!("sending the identity header: {err}"))
            })?;
        let ack: crate::header::Ack = crate::framing::read_json(&mut tls).await.map_err(|err| {
            TransportError::Unavailable(format!("reading the broker's answer: {err}"))
        })?;
        if ack.accepted {
            Ok(Box::new(tls))
        } else {
            Err(TransportError::Rejected {
                reason: ack.reason.unwrap_or_else(|| "unspecified".into()),
            })
        }
    }
}

/// Splits a PEM bundle into its certificate blocks.
pub fn pem_blocks(bundle: &[u8]) -> Vec<String> {
    let text = String::from_utf8_lossy(bundle);
    let mut blocks = Vec::new();
    let mut current: Option<String> = None;
    for line in text.lines() {
        if line.starts_with("-----BEGIN CERTIFICATE-----") {
            current = Some(String::new());
        }
        if let Some(block) = current.as_mut() {
            block.push_str(line);
            block.push('\n');
        }
        if line.starts_with("-----END CERTIFICATE-----") {
            if let Some(block) = current.take() {
                blocks.push(block);
            }
        }
    }
    blocks
}
