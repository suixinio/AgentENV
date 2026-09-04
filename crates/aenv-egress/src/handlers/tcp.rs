use std::sync::Arc;

use async_trait::async_trait;
use tokio::io::AsyncWriteExt;

use crate::credential::CredentialSource;
use crate::handler::{ConnCtx, Handler, HandlerError};
use crate::policy::UpstreamGuard;
use crate::transport::AsyncStream;

/// Relays a guest connection to the upstream the endpoint declaration named.
/// The upstream comes from the declaration, never from the guest, and still
/// has to pass this handler's operator allowlist inside [`UpstreamGuard`].
pub struct TcpRelayHandler;

impl TcpRelayHandler {
    pub const NAME: &'static str = "tcp";
}

#[async_trait]
impl Handler for TcpRelayHandler {
    fn name(&self) -> &str {
        Self::NAME
    }

    async fn handle(
        &self,
        mut conn: Box<dyn AsyncStream>,
        ctx: ConnCtx,
        _creds: Arc<dyn CredentialSource>,
        guard: Arc<UpstreamGuard>,
    ) -> Result<(), HandlerError> {
        let upstream = ctx
            .params
            .get("upstream")
            .and_then(|upstream| upstream.as_str())
            .ok_or_else(|| {
                HandlerError::Protocol("the endpoint declaration names no upstream".into())
            })?;
        let (host, port) = split_authority(upstream)?;
        let (mut upstream, _addr) = guard
            .connect_checked(Self::NAME, host, port, &ctx.egress)
            .await?;
        tokio::io::copy_bidirectional(&mut conn, &mut upstream).await?;
        upstream.shutdown().await?;
        Ok(())
    }
}

/// `host:port` as the declaration's validation left it: an IPv6 literal is
/// bracketed, everything else is a name or an IPv4 literal.
fn split_authority(authority: &str) -> Result<(&str, u16), HandlerError> {
    let (host, port) = authority.rsplit_once(':').ok_or_else(|| {
        HandlerError::Protocol(format!("upstream {authority:?} is not host:port"))
    })?;
    let port = port
        .parse::<u16>()
        .map_err(|_| HandlerError::Protocol(format!("upstream {authority:?} has no valid port")))?;
    let host = host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host);
    if host.is_empty() {
        return Err(HandlerError::Protocol(format!(
            "upstream {authority:?} names no host"
        )));
    }
    Ok((host, port))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;
    use crate::credential::NoCredentials;
    use crate::header::test_support::sample_header;
    use crate::header::EgressPolicySummary;
    use crate::policy::{BrokerDenyList, DenyReason, UpstreamError};

    fn guard_allowing(cidrs: &[&str]) -> Arc<UpstreamGuard> {
        Arc::new(
            UpstreamGuard::new(BrokerDenyList::empty())
                .with_allowlist(TcpRelayHandler::NAME, cidrs)
                .unwrap(),
        )
    }

    fn ctx_for(upstream: &str) -> ConnCtx {
        let mut header = sample_header();
        header.handler = TcpRelayHandler::NAME.into();
        header.params = serde_json::json!({ "upstream": upstream });
        ConnCtx::from(&header)
    }

    async fn upstream_listener() -> (TcpListener, String) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        (listener, format!("127.0.0.1:{port}"))
    }

    async fn serve_uppercase(listener: TcpListener) {
        let (mut accepted, _) = listener.accept().await.unwrap();
        let mut received = Vec::new();
        accepted.read_to_end(&mut received).await.unwrap();
        let answer = String::from_utf8_lossy(&received).to_uppercase();
        accepted.write_all(answer.as_bytes()).await.unwrap();
        accepted.shutdown().await.unwrap();
    }

    /// Runs the handler on one end of a duplex and hands the caller the other,
    /// so a handler error is the caller's error rather than a logged one.
    fn relay(
        guard: Arc<UpstreamGuard>,
        ctx: ConnCtx,
    ) -> (
        tokio::io::DuplexStream,
        tokio::task::JoinHandle<Result<(), HandlerError>>,
    ) {
        let (mine, theirs) = tokio::io::duplex(4096);
        let served = tokio::spawn(async move {
            TcpRelayHandler
                .handle(Box::new(theirs), ctx, Arc::new(NoCredentials), guard)
                .await
        });
        (mine, served)
    }

    #[tokio::test]
    async fn an_allowed_upstream_is_relayed_in_both_directions() {
        let (listener, upstream) = upstream_listener().await;
        let upstream_task = tokio::spawn(serve_uppercase(listener));

        let (mut guest, served) = relay(guard_allowing(&["127.0.0.1/32"]), ctx_for(&upstream));
        guest.write_all(b"ping").await.unwrap();
        guest.shutdown().await.unwrap();
        let mut answered = Vec::new();
        guest.read_to_end(&mut answered).await.unwrap();

        assert_eq!(answered, b"PING");
        served.await.unwrap().unwrap();
        upstream_task.await.unwrap();
    }

    #[tokio::test]
    async fn an_upstream_outside_the_allowlist_is_never_dialled() {
        let (listener, upstream) = upstream_listener().await;
        let (_guest, served) = relay(guard_allowing(&["203.0.113.0/24"]), ctx_for(&upstream));

        let err = served.await.unwrap().err().unwrap();
        assert!(
            matches!(
                err,
                HandlerError::Upstream(UpstreamError::Denied(DenyReason::HandlerDenied))
            ),
            "got {err:?}"
        );
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), listener.accept())
                .await
                .is_err(),
            "nothing may reach an upstream outside the allowlist"
        );
    }

    #[tokio::test]
    async fn a_handler_with_no_allowlist_reaches_nothing() {
        let (_listener, upstream) = upstream_listener().await;
        let guard = Arc::new(UpstreamGuard::new(BrokerDenyList::empty()));
        let (_guest, served) = relay(guard, ctx_for(&upstream));

        let err = served.await.unwrap().err().unwrap();
        assert!(
            matches!(
                err,
                HandlerError::Upstream(UpstreamError::Denied(DenyReason::HandlerUnpinned))
            ),
            "got {err:?}"
        );
    }

    #[tokio::test]
    async fn a_declaration_without_an_upstream_never_touches_the_guard() {
        let mut header = sample_header();
        header.handler = TcpRelayHandler::NAME.into();
        header.params = serde_json::json!({});
        let (_guest, served) = relay(guard_allowing(&["127.0.0.1/32"]), ConnCtx::from(&header));

        let err = served.await.unwrap().err().unwrap();
        assert!(matches!(err, HandlerError::Protocol(reason) if reason.contains("upstream")),);
    }

    #[test]
    fn the_operator_allowlist_is_checked_before_the_sandbox_allow_list() {
        let guard = UpstreamGuard::new(BrokerDenyList::empty())
            .with_allowlist(TcpRelayHandler::NAME, &["198.51.100.0/24"])
            .unwrap();
        let permissive = EgressPolicySummary {
            allow_internet: true,
            allowed_cidrs: vec!["0.0.0.0/0".into()],
            denied_cidrs: vec![],
        };
        assert_eq!(
            guard.check(
                TcpRelayHandler::NAME,
                "203.0.113.7".parse().unwrap(),
                &permissive
            ),
            Err(DenyReason::HandlerDenied)
        );
        assert_eq!(
            guard.check(
                TcpRelayHandler::NAME,
                "198.51.100.7".parse().unwrap(),
                &permissive
            ),
            Ok(())
        );
    }

    #[test]
    fn the_sandbox_policy_still_applies_inside_the_allowlist() {
        let guard = UpstreamGuard::new(BrokerDenyList::empty())
            .with_allowlist(TcpRelayHandler::NAME, &["198.51.100.0/24"])
            .unwrap();
        let closed = EgressPolicySummary {
            allow_internet: false,
            allowed_cidrs: vec![],
            denied_cidrs: vec![],
        };
        assert_eq!(
            guard.check(
                TcpRelayHandler::NAME,
                "198.51.100.7".parse().unwrap(),
                &closed
            ),
            Err(DenyReason::InternetDisabled)
        );
    }

    #[test]
    fn a_handler_outside_the_required_set_keeps_working_without_an_allowlist() {
        let guard = UpstreamGuard::new(BrokerDenyList::empty());
        let open = EgressPolicySummary {
            allow_internet: true,
            allowed_cidrs: vec![],
            denied_cidrs: vec![],
        };
        assert_eq!(
            guard.check("http", "203.0.113.7".parse().unwrap(), &open),
            Ok(())
        );
        assert_eq!(
            guard.check("tcp", "203.0.113.7".parse().unwrap(), &open),
            Err(DenyReason::HandlerUnpinned)
        );
    }

    #[test]
    fn an_authority_splits_into_a_host_and_a_port() {
        assert_eq!(
            split_authority("db.example:5432").unwrap(),
            ("db.example", 5432)
        );
        assert_eq!(
            split_authority("10.0.0.1:5432").unwrap(),
            ("10.0.0.1", 5432)
        );
        assert_eq!(
            split_authority("[2001:db8::1]:5432").unwrap(),
            ("2001:db8::1", 5432)
        );
        assert!(split_authority("db.example").is_err());
        assert!(split_authority("db.example:not-a-port").is_err());
        assert!(split_authority(":5432").is_err());
    }

    #[test]
    fn each_bound_that_can_refuse_an_upstream_has_its_own_reason() {
        assert_eq!(
            UpstreamError::Denied(DenyReason::HandlerDenied).reason(),
            "handler_denied_cidr"
        );
        assert_eq!(
            UpstreamError::Denied(DenyReason::HandlerUnpinned).reason(),
            "handler_no_allowlist"
        );
        assert_eq!(
            UpstreamError::Denied(DenyReason::SandboxDenied).reason(),
            "sandbox_denied_cidr"
        );
    }
}
