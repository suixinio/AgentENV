use std::collections::HashMap;
use std::sync::Arc;

use crate::credential::CredentialSource;
use crate::handler::{ConnCtx, Handler, HandlerError};
use crate::header::{Ack, IdentityHeader, IDENTITY_HEADER_VERSION};
use crate::policy::UpstreamGuard;
use crate::transport::AsyncStream;

/// Why a header was turned away before any handler ran.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Reject {
    #[error("identity header version {0} is not supported")]
    UnsupportedVersion(u32),
    #[error("no handler named {0:?} is installed")]
    UnknownHandler(String),
}

impl Reject {
    pub fn reason(&self) -> &'static str {
        match self {
            Self::UnsupportedVersion(_) => "unsupported_version",
            Self::UnknownHandler(_) => "unknown_handler",
        }
    }

    pub fn ack(&self) -> Ack {
        Ack::rejected(self.reason())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DispatchError {
    #[error(transparent)]
    Rejected(#[from] Reject),
    #[error(transparent)]
    Handler(#[from] HandlerError),
}

/// Routes an accepted header to the handler it names and lends that handler
/// the shared credential source and upstream guard.
pub struct Dispatcher {
    handlers: HashMap<String, Arc<dyn Handler>>,
    creds: Arc<dyn CredentialSource>,
    guard: Arc<UpstreamGuard>,
}

impl Dispatcher {
    pub fn new(creds: Arc<dyn CredentialSource>, guard: Arc<UpstreamGuard>) -> Self {
        Self {
            handlers: HashMap::new(),
            creds,
            guard,
        }
    }

    pub fn with_handler(mut self, handler: Arc<dyn Handler>) -> Self {
        self.handlers.insert(handler.name().to_string(), handler);
        self
    }

    pub fn handler_names(&self) -> impl Iterator<Item = &str> {
        self.handlers.keys().map(String::as_str)
    }

    /// The decision behind the [`Ack`]: known version and known handler.
    pub fn accept(&self, header: &IdentityHeader) -> Result<Arc<dyn Handler>, Reject> {
        if header.v != IDENTITY_HEADER_VERSION {
            return Err(Reject::UnsupportedVersion(header.v));
        }
        self.handlers
            .get(&header.handler)
            .cloned()
            .ok_or_else(|| Reject::UnknownHandler(header.handler.clone()))
    }

    pub async fn serve(
        &self,
        handler: Arc<dyn Handler>,
        header: &IdentityHeader,
        stream: Box<dyn AsyncStream>,
    ) -> Result<(), HandlerError> {
        handler
            .handle(
                stream,
                ConnCtx::from(header),
                Arc::clone(&self.creds),
                Arc::clone(&self.guard),
            )
            .await
    }

    /// `accept` then `serve`, for callers that already trust the header.
    pub async fn dispatch(
        &self,
        header: &IdentityHeader,
        stream: Box<dyn AsyncStream>,
    ) -> Result<(), DispatchError> {
        let handler = self.accept(header)?;
        Ok(self.serve(handler, header, stream).await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::credential::NoCredentials;
    use crate::handlers::tcp::TcpEchoHandler;
    use crate::header::test_support::sample_header;
    use crate::policy::BrokerDenyList;

    fn dispatcher() -> Dispatcher {
        Dispatcher::new(
            Arc::new(NoCredentials),
            Arc::new(UpstreamGuard::new(BrokerDenyList::default())),
        )
        .with_handler(Arc::new(TcpEchoHandler))
    }

    #[test]
    fn an_unknown_handler_is_rejected_with_a_stable_reason() {
        let mut header = sample_header();
        header.handler = "postgres".into();
        let reject = dispatcher().accept(&header).err().unwrap();
        assert_eq!(reject, Reject::UnknownHandler("postgres".into()));
        assert_eq!(reject.ack(), Ack::rejected("unknown_handler"));
    }

    #[test]
    fn an_unknown_version_is_rejected_before_the_handler_is_looked_up() {
        let mut header = sample_header();
        header.v = 7;
        header.handler = "postgres".into();
        let reject = dispatcher().accept(&header).err().unwrap();
        assert_eq!(reject, Reject::UnsupportedVersion(7));
        assert_eq!(reject.ack(), Ack::rejected("unsupported_version"));
    }

    #[test]
    fn a_known_handler_is_accepted() {
        let handler = dispatcher().accept(&sample_header()).unwrap();
        assert_eq!(handler.name(), "tcp");
    }

    #[tokio::test]
    async fn dispatching_an_unknown_handler_touches_no_stream() {
        let (stream, mut peer) = tokio::io::duplex(64);
        let mut header = sample_header();
        header.handler = "nope".into();
        let err = dispatcher()
            .dispatch(&header, Box::new(stream))
            .await
            .err()
            .unwrap();
        assert!(matches!(
            err,
            DispatchError::Rejected(Reject::UnknownHandler(_))
        ));
        let mut buf = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut peer, &mut buf)
            .await
            .unwrap();
        assert!(buf.is_empty());
    }
}
