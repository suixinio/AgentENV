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

/// Runs the dispatcher in this process over an in-memory pipe.
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

/// The runtime's way to the broker on this same node: a Unix socket the
/// broker binds and only the node's uid may open. The header travels
/// unauthenticated because the socket's peer credentials are the identity.
#[cfg(feature = "local")]
pub struct LocalTransport {
    socket_path: std::path::PathBuf,
    connect_timeout: std::time::Duration,
}

#[cfg(feature = "local")]
impl LocalTransport {
    pub fn new(socket_path: impl Into<std::path::PathBuf>) -> Self {
        Self {
            socket_path: socket_path.into(),
            connect_timeout: std::time::Duration::from_secs(5),
        }
    }

    pub fn with_connect_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    pub fn socket_path(&self) -> &std::path::Path {
        &self.socket_path
    }

    async fn connect(&self) -> Result<tokio::net::UnixStream, TransportError> {
        tokio::time::timeout(
            self.connect_timeout,
            tokio::net::UnixStream::connect(&self.socket_path),
        )
        .await
        .map_err(|_| {
            TransportError::Unavailable(format!(
                "connecting to {} timed out",
                self.socket_path.display()
            ))
        })?
        .map_err(|err| {
            TransportError::Unavailable(format!(
                "connecting to {}: {err}",
                self.socket_path.display()
            ))
        })
    }

    /// The heartbeat's reachability check: an empty frame, which the broker
    /// refuses and closes. Connecting alone would succeed against a broker
    /// wedged behind its own backlog; a closed stream is proof that something
    /// read the frame.
    pub async fn probe(&self) -> Result<(), TransportError> {
        let mut stream = self.connect().await?;
        let exchange = async {
            crate::framing::write_frame(&mut stream, &[])
                .await
                .map_err(|err| TransportError::Unavailable(format!("probing the broker: {err}")))?;
            let mut byte = [0u8; 1];
            match tokio::io::AsyncReadExt::read(&mut stream, &mut byte).await {
                // The broker read the frame and refused it, either way.
                Ok(_) => Ok(()),
                Err(err) => Err(TransportError::Unavailable(format!(
                    "probing the broker: {err}"
                ))),
            }
        };
        let answered = tokio::time::timeout(self.connect_timeout, exchange)
            .await
            .map_err(|_| TransportError::Unavailable("the broker did not read the probe".into()))?;
        let _ = tokio::io::AsyncWriteExt::shutdown(&mut stream).await;
        answered
    }
}

#[cfg(feature = "local")]
#[async_trait]
impl BrokerTransport for LocalTransport {
    async fn open(&self, header: IdentityHeader) -> Result<Box<dyn AsyncStream>, TransportError> {
        let mut stream = self.connect().await?;
        crate::framing::write_json(&mut stream, &header)
            .await
            .map_err(|err| {
                TransportError::Unavailable(format!("sending the identity header: {err}"))
            })?;
        let ack: crate::header::Ack =
            crate::framing::read_json(&mut stream)
                .await
                .map_err(|err| {
                    TransportError::Unavailable(format!("reading the broker's answer: {err}"))
                })?;
        if ack.accepted {
            Ok(Box::new(stream))
        } else {
            Err(TransportError::Rejected {
                reason: ack.reason.unwrap_or_else(|| "unspecified".into()),
            })
        }
    }
}
