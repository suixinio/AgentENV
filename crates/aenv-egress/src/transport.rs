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
