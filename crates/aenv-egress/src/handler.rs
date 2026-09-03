use std::net::SocketAddr;

use async_trait::async_trait;

use crate::credential::{CredentialError, CredentialSource};
use crate::header::{EgressPolicySummary, IdentityHeader};
use crate::policy::{UpstreamError, UpstreamGuard};
use crate::transport::AsyncStream;

/// What a handler learns about the connection it serves. Carries no
/// authentication material and nothing that lives in the runtime's process.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConnCtx {
    pub sandbox_id: String,
    pub execution_id: String,
    pub template_id: String,
    pub port: u16,
    pub handler: String,
    pub params: serde_json::Value,
    pub original_dst: Option<SocketAddr>,
    pub egress: EgressPolicySummary,
}

impl From<&IdentityHeader> for ConnCtx {
    fn from(header: &IdentityHeader) -> Self {
        Self {
            sandbox_id: header.sandbox_id.clone(),
            execution_id: header.execution_id.clone(),
            template_id: header.template_id.clone(),
            port: header.port,
            handler: header.handler.clone(),
            params: header.params.clone(),
            original_dst: header.original_dst,
            egress: header.egress.clone(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum HandlerError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Upstream(#[from] UpstreamError),
    #[error(transparent)]
    Credential(#[from] CredentialError),
    #[error("protocol error: {0}")]
    Protocol(String),
}

/// One protocol's worth of brokering. Upstreams are reached only through
/// `guard`; secrets only through `creds`.
#[async_trait]
pub trait Handler: Send + Sync {
    fn name(&self) -> &str;

    async fn handle(
        &self,
        conn: Box<dyn AsyncStream>,
        ctx: ConnCtx,
        creds: &dyn CredentialSource,
        guard: &UpstreamGuard,
    ) -> Result<(), HandlerError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::header::test_support::sample_header;

    #[test]
    fn the_context_is_the_header_minus_its_authentication_and_observability_fields() {
        let mut header = sample_header();
        header.sign(b"key");
        let ctx = ConnCtx::from(&header);

        assert_eq!(ctx.sandbox_id, header.sandbox_id);
        assert_eq!(ctx.execution_id, header.execution_id);
        assert_eq!(ctx.template_id, header.template_id);
        assert_eq!(ctx.port, header.port);
        assert_eq!(ctx.handler, header.handler);
        assert_eq!(ctx.params, header.params);
        assert_eq!(ctx.original_dst, header.original_dst);
        assert_eq!(ctx.egress, header.egress);

        let printed = format!("{ctx:?}");
        assert!(!printed.contains(&header.hmac));
        assert!(!printed.contains("nonce"));
        assert!(!printed.contains("guest_addr"));
    }
}
