use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;

use crate::credential::CredentialSource;
use crate::handler::{ConnCtx, Handler, HandlerError};
use crate::policy::UpstreamGuard;
use crate::transport::AsyncStream;

/// What the `tcp` handler writes as its first line: the identity the broker
/// attributed to the connection. It is what an integration test reads back.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdentityBanner {
    pub sandbox_id: String,
    pub execution_id: String,
    pub template_id: String,
    pub port: u16,
    pub original_dst: Option<SocketAddr>,
}

impl From<&ConnCtx> for IdentityBanner {
    fn from(ctx: &ConnCtx) -> Self {
        Self {
            sandbox_id: ctx.sandbox_id.clone(),
            execution_id: ctx.execution_id.clone(),
            template_id: ctx.template_id.clone(),
            port: ctx.port,
            original_dst: ctx.original_dst,
        }
    }
}

/// Answers with an [`IdentityBanner`] line and then echoes every byte. It
/// opens no upstream and reads no credential; it exists so the embedded
/// transport and the integration tests can prove identity end to end.
pub struct TcpEchoHandler;

impl TcpEchoHandler {
    pub const NAME: &'static str = "tcp";
}

#[async_trait]
impl Handler for TcpEchoHandler {
    fn name(&self) -> &str {
        Self::NAME
    }

    async fn handle(
        &self,
        conn: Box<dyn AsyncStream>,
        ctx: ConnCtx,
        _creds: Arc<dyn CredentialSource>,
        _guard: Arc<UpstreamGuard>,
    ) -> Result<(), HandlerError> {
        let (mut reader, mut writer) = tokio::io::split(conn);
        let mut banner = serde_json::to_vec(&IdentityBanner::from(&ctx))
            .map_err(|err| HandlerError::Protocol(err.to_string()))?;
        banner.push(b'\n');
        writer.write_all(&banner).await?;
        writer.flush().await?;
        tokio::io::copy(&mut reader, &mut writer).await?;
        writer.shutdown().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};

    use super::*;
    use crate::credential::NoCredentials;
    use crate::dispatch::Dispatcher;
    use crate::header::test_support::sample_header;
    use crate::policy::BrokerDenyList;
    use crate::transport::{BrokerTransport, EmbeddedTransport};

    #[tokio::test]
    async fn the_embedded_transport_returns_the_identity_and_echoes_without_a_socket() {
        let dispatcher = Arc::new(
            Dispatcher::new(
                Arc::new(NoCredentials),
                Arc::new(UpstreamGuard::new(BrokerDenyList::default())),
            )
            .with_handler(Arc::new(TcpEchoHandler)),
        );
        let transport = EmbeddedTransport::new(dispatcher);
        let header = sample_header();

        let stream = transport.open(header.clone()).await.unwrap();
        let mut stream = BufReader::new(stream);

        let mut line = String::new();
        stream.read_line(&mut line).await.unwrap();
        let banner: IdentityBanner = serde_json::from_str(&line).unwrap();
        assert_eq!(
            banner,
            IdentityBanner {
                sandbox_id: header.sandbox_id.clone(),
                execution_id: header.execution_id.clone(),
                template_id: header.template_id.clone(),
                port: header.port,
                original_dst: header.original_dst,
            }
        );

        stream.get_mut().write_all(b"ping").await.unwrap();
        stream.get_mut().shutdown().await.unwrap();
        let mut echoed = Vec::new();
        stream.read_to_end(&mut echoed).await.unwrap();
        assert_eq!(echoed, b"ping");
    }

    #[tokio::test]
    async fn the_embedded_transport_refuses_an_unknown_handler_before_opening_a_stream() {
        let dispatcher = Arc::new(Dispatcher::new(
            Arc::new(NoCredentials),
            Arc::new(UpstreamGuard::new(BrokerDenyList::default())),
        ));
        let transport = EmbeddedTransport::new(dispatcher);
        let err = transport.open(sample_header()).await.err().unwrap();
        assert!(matches!(
            err,
            crate::transport::TransportError::Rejected { reason } if reason == "unknown_handler"
        ));
    }
}
