//! Brokered endpoints: listeners the runtime opens inside a sandbox's network
//! namespace for the `brokers` its egress policy declares. Every accepted
//! connection is the guest's by construction, so the runtime attaches the
//! sandbox's identity and relays the bytes to the broker through a
//! [`BrokerTransport`]. The runtime parses no application protocol.

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use aenv_egress::credential::NoCredentials;
use aenv_egress::handlers::echo::IdentityEchoHandler;
use aenv_egress::handlers::tcp::TcpRelayHandler;
use aenv_egress::transport::RemoteTransport;
use aenv_egress::{
    BrokerDenyList, BrokerTransport, Dispatcher, EgressPolicySummary, EmbeddedTransport,
    IdentityHeader, TransportError, UpstreamGuard,
};
use anyhow::{bail, Context, Result};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::{JoinHandle, JoinSet};
use tracing::{debug, info, warn};

use crate::cfg::{ConfigManager, EgressBrokerMode};
use crate::observability::EgressBrokerState;
use crate::sandbox::network::policy::BaseSandboxNetworkPolicy;
use crate::sandbox::network::Slot;
use crate::sandbox::SandboxNetworkPolicy;
use crate::types::{ExecutionId, SandboxId};

/// Where a guest with rules finds its extra trust anchor; envd appends the
/// CA bundle from `init` to this file.
pub const GUEST_CA_BUNDLE_PATH: &str = "/etc/ssl/certs/ca-certificates.crt";

/// Environment defaults for guests that get a CA bundle, for language
/// runtimes that do not read the system trust store. A user value of the
/// same name wins.
pub const DEFAULT_TRUST_ENV: [(&str, &str); 5] = [
    ("SSL_CERT_FILE", GUEST_CA_BUNDLE_PATH),
    ("REQUESTS_CA_BUNDLE", GUEST_CA_BUNDLE_PATH),
    ("CURL_CA_BUNDLE", GUEST_CA_BUNDLE_PATH),
    ("NODE_EXTRA_CA_CERTS", GUEST_CA_BUNDLE_PATH),
    ("GIT_SSL_CAINFO", GUEST_CA_BUNDLE_PATH),
];

static RUNTIME: OnceLock<Option<Arc<EgressRuntime>>> = OnceLock::new();

/// The node's one connection to the broker world: the transport, the node
/// wide connection budget and the CA bundle guests must trust.
pub struct EgressRuntime {
    mode: EgressBrokerMode,
    node_id: String,
    transport: Arc<dyn BrokerTransport>,
    node_permits: Arc<Semaphore>,
    per_sandbox_conns: usize,
    open_timeout: Duration,
    ca_bundle: Option<String>,
    /// Whether the last probe of a remote broker succeeded; always true for
    /// the embedded transport.
    reachable: Arc<std::sync::atomic::AtomicBool>,
}

/// How often a remote broker is probed for the heartbeat.
const REMOTE_PROBE_INTERVAL: Duration = Duration::from_secs(5);

impl EgressRuntime {
    /// The process-wide runtime, or `None` when `[egress_broker].mode` is
    /// `disabled` or the configured mode could not be brought up.
    pub fn global() -> Option<Arc<EgressRuntime>> {
        RUNTIME
            .get_or_init(|| match Self::from_global_config() {
                Ok(runtime) => runtime.map(Arc::new),
                Err(err) => {
                    warn!(error = %format_args!("{err:#}"), "egress broker runtime unavailable; sandboxes with rules cannot start here");
                    None
                }
            })
            .clone()
    }

    fn from_global_config() -> Result<Option<Self>> {
        let config = ConfigManager::global_config();
        let egress = &config.egress_broker;
        let transport: Arc<dyn BrokerTransport> = match egress.mode {
            EgressBrokerMode::Disabled => return Ok(None),
            EgressBrokerMode::Embedded => {
                let denied = config.network.egress.always_denied_cidrs.to_vec();
                let guard = UpstreamGuard::new(
                    BrokerDenyList::with_extra(&denied)
                        .context("egress_broker: always_denied_cidrs are not all cidrs")?,
                );
                let guard = guard
                    .with_allowlist(TcpRelayHandler::NAME, &egress.embedded_tcp_allowed_cidrs)
                    .context("egress_broker: embedded_tcp_allowed_cidrs are not all cidrs")?;
                let dispatcher = Arc::new(
                    Dispatcher::new(Arc::new(NoCredentials), Arc::new(guard))
                        .with_handler(Arc::new(IdentityEchoHandler))
                        .with_handler(Arc::new(TcpRelayHandler)),
                );
                Arc::new(EmbeddedTransport::new(dispatcher))
            }
            EgressBrokerMode::Remote => {
                let endpoint = egress
                    .endpoint
                    .as_deref()
                    .context("egress_broker.endpoint is required in remote mode")?;
                let ca_path = egress
                    .ca_cert_path
                    .as_ref()
                    .context("egress_broker.ca_cert_path is required in remote mode")?;
                let ca_pem = std::fs::read(ca_path).with_context(|| {
                    format!("read egress_broker.ca_cert_path {}", ca_path.display())
                })?;
                let secret = egress
                    .shared_secret
                    .as_deref()
                    .context("egress_broker.shared_secret is required in remote mode")?;
                Arc::new(
                    RemoteTransport::new(endpoint, None, &ca_pem, secret.trim().as_bytes())
                        .context("configure the remote broker transport")?
                        .with_connect_timeout(Duration::from_millis(egress.open_timeout_ms)),
                )
            }
        };
        let ca_bundle =
            match egress.ca_cert_path.as_ref() {
                Some(path) => Some(std::fs::read_to_string(path).with_context(|| {
                    format!("read egress_broker.ca_cert_path {}", path.display())
                })?),
                None => None,
            };
        let reachable = Arc::new(std::sync::atomic::AtomicBool::new(
            egress.mode == EgressBrokerMode::Embedded,
        ));
        if egress.mode == EgressBrokerMode::Remote {
            Self::spawn_remote_probe(Arc::clone(&reachable), egress);
        }
        info!(mode = egress.mode.as_str(), "egress broker runtime ready");
        Ok(Some(Self {
            mode: egress.mode,
            node_id: crate::identity::local_node_id(),
            transport,
            node_permits: Arc::new(Semaphore::new(egress.node_conns as usize)),
            per_sandbox_conns: egress.per_sandbox_conns as usize,
            open_timeout: Duration::from_millis(egress.open_timeout_ms),
            ca_bundle,
            reachable,
        }))
    }

    // Runs on its own thread and runtime so `global()` works from any thread.
    fn spawn_remote_probe(
        reachable: Arc<std::sync::atomic::AtomicBool>,
        egress: &crate::cfg::EgressBrokerConfig,
    ) {
        let endpoint = egress.endpoint.clone().unwrap_or_default();
        let ca_path = egress.ca_cert_path.clone();
        let secret = egress.shared_secret.clone().unwrap_or_default();
        let timeout = Duration::from_millis(egress.open_timeout_ms);
        let spawned = std::thread::Builder::new()
            .name("egress-broker-probe".into())
            .spawn(move || {
                let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                else {
                    warn!("could not start the egress broker probe runtime");
                    return;
                };
                runtime.block_on(async move {
                    let Some(ca_pem) = ca_path.and_then(|path| std::fs::read(path).ok()) else {
                        return;
                    };
                    let Ok(probe) =
                        RemoteTransport::new(&endpoint, None, &ca_pem, secret.trim().as_bytes())
                    else {
                        return;
                    };
                    let probe = probe.with_connect_timeout(timeout);
                    loop {
                        let ok = probe.probe().await.is_ok();
                        let was = reachable.swap(ok, Ordering::Relaxed);
                        if was != ok {
                            if ok {
                                info!(endpoint, "egress broker reachable");
                            } else {
                                warn!(
                                    endpoint,
                                    "egress broker unreachable; brokered connections fail fast"
                                );
                            }
                        }
                        tokio::time::sleep(REMOTE_PROBE_INTERVAL).await;
                    }
                });
            });
        if let Err(err) = spawned {
            warn!(error = %err, "could not spawn the egress broker probe thread");
        }
    }

    /// What the heartbeat reports.
    pub fn state(&self) -> EgressBrokerState {
        match self.mode {
            EgressBrokerMode::Disabled => EgressBrokerState::Disabled,
            EgressBrokerMode::Embedded => EgressBrokerState::Embedded,
            EgressBrokerMode::Remote => {
                if self.reachable.load(Ordering::Relaxed) {
                    EgressBrokerState::RemoteOk
                } else {
                    EgressBrokerState::RemoteUnreachable
                }
            }
        }
    }

    /// False while a remote broker fails its probe; the accept loop then
    /// closes new connections at once instead of waiting on the open timeout.
    pub fn is_reachable(&self) -> bool {
        self.reachable.load(Ordering::Relaxed)
    }

    /// The PEM guests with rules must trust, when this node has one.
    pub fn ca_bundle(&self) -> Option<&str> {
        self.ca_bundle.as_deref()
    }

    /// The runtime a policy with brokers needs, or an error naming the
    /// configuration a node must carry to serve one.
    pub fn required() -> Result<Arc<Self>> {
        Self::global().context(
            "the sandbox declares network rules but this node has no egress broker; set [egress_broker].mode (AENV_EGRESS_BROKER_MODE) to \"remote\" and point endpoint, ca_cert_path and shared_secret at aenv-egress",
        )
    }

    /// Refuses a policy whose brokers this node cannot dispatch, so no caller
    /// is told a set of rules is in force that would fail on first use.
    pub fn ensure_serves(&self, policy: &SandboxNetworkPolicy) -> Result<()> {
        for broker in &policy.egress.brokers {
            if !mode_serves_handler(self.mode, &broker.handler) {
                bail!(
                    "this node's {} egress broker does not serve the {:?} handler these rules need",
                    self.mode.as_str(),
                    broker.handler
                );
            }
        }
        Ok(())
    }
}

/// Which handlers a broker mode dispatches. The embedded transport has no
/// credential source and no TLS stack, so it carries the two handlers that
/// need neither: [`IdentityEchoHandler`] and [`TcpRelayHandler`]. Everything
/// else lives in the broker process.
pub fn mode_serves_handler(mode: EgressBrokerMode, handler: &str) -> bool {
    match mode {
        EgressBrokerMode::Disabled => false,
        EgressBrokerMode::Embedded => {
            handler == IdentityEchoHandler::NAME || handler == TcpRelayHandler::NAME
        }
        EgressBrokerMode::Remote => true,
    }
}

/// Who a set of brokered endpoints speaks for.
#[derive(Clone, Debug)]
pub struct SandboxIdentity {
    pub sandbox_id: SandboxId,
    pub execution_id: ExecutionId,
    pub template_id: String,
}

/// The broker's view of a sandbox's egress policy.
pub fn egress_summary(policy: &SandboxNetworkPolicy) -> EgressPolicySummary {
    EgressPolicySummary {
        allow_internet: policy.base_policy != BaseSandboxNetworkPolicy::Deny,
        allowed_cidrs: policy.egress.allowed_cidrs.clone(),
        denied_cidrs: policy.egress.denied_cidrs.clone(),
    }
}

struct AcceptContext {
    runtime: Arc<EgressRuntime>,
    identity: SandboxIdentity,
    listener_port: u16,
    handler: String,
    params: serde_json::Value,
    egress: EgressPolicySummary,
    sandbox_permits: Arc<Semaphore>,
    over_limit: Arc<AtomicU64>,
    relays: Arc<Mutex<JoinSet<()>>>,
}

/// The listeners one sandbox's policy asked for, alive until `shutdown` or
/// drop. Accept loops and relays are separate tasks: replacing the policy
/// stops accepting on the old listeners without cutting connections in
/// flight.
pub struct BrokeredEndpoints {
    accept_tasks: Vec<JoinHandle<()>>,
    relays: Arc<Mutex<JoinSet<()>>>,
    ports: Vec<u16>,
    over_limit: Arc<AtomicU64>,
    sandbox_permits: Arc<Semaphore>,
    /// Set once a successor owns the relay set, so this instance's drop
    /// leaves the connections in flight to it.
    relays_handed_over: bool,
}

impl BrokeredEndpoints {
    /// Opens one listener per broker in `policy` inside `slot`'s namespace,
    /// installs the intercept the policy asks for and starts accepting.
    pub fn spawn(
        runtime: &Arc<EgressRuntime>,
        slot: &Slot,
        identity: SandboxIdentity,
        policy: &SandboxNetworkPolicy,
    ) -> Result<Self> {
        Self::spawn_with(
            runtime,
            slot,
            identity,
            policy,
            Arc::new(Mutex::new(JoinSet::new())),
            Arc::new(Semaphore::new(runtime.per_sandbox_conns)),
        )
    }

    /// Like `spawn`, but connections in flight on `previous` keep running
    /// and keep counting against the sandbox's budget. The new listener and
    /// its intercept are in place before the old loop stops accepting, so
    /// nothing the guest sends in between escapes the intercept.
    pub async fn replace(
        mut previous: Self,
        runtime: &Arc<EgressRuntime>,
        slot: &Slot,
        identity: SandboxIdentity,
        policy: &SandboxNetworkPolicy,
    ) -> Result<Self> {
        let relays = Arc::clone(&previous.relays);
        let permits = Arc::clone(&previous.sandbox_permits);
        let next = Self::spawn_with(runtime, slot, identity, policy, relays, permits)?;
        previous.relays_handed_over = true;
        previous.stop_accepting().await;
        Ok(next)
    }

    fn spawn_with(
        runtime: &Arc<EgressRuntime>,
        slot: &Slot,
        identity: SandboxIdentity,
        policy: &SandboxNetworkPolicy,
        relays: Arc<Mutex<JoinSet<()>>>,
        sandbox_permits: Arc<Semaphore>,
    ) -> Result<Self> {
        let egress = egress_summary(policy);
        // Every task is owned by `endpoints` from the moment it is spawned, so
        // a broker that fails to come up takes the ones before it down.
        let mut endpoints = Self {
            accept_tasks: Vec::new(),
            relays,
            ports: Vec::new(),
            over_limit: Arc::new(AtomicU64::new(0)),
            sandbox_permits,
            relays_handed_over: false,
        };
        // Every listener opens before any intercept is installed: the
        // namespace holds one intercept, so the DNATs go in as one set.
        let mut listeners = Vec::with_capacity(policy.egress.brokers.len());
        let mut intercepts = Vec::new();
        for broker in &policy.egress.brokers {
            let listener = slot.listen_in_namespace(broker.port).with_context(|| {
                format!("open brokered listener for handler {}", broker.handler)
            })?;
            let port = listener
                .local_addr()
                .context("read brokered listener address")?
                .port();
            if let Some(intercept) = &broker.intercept {
                intercepts.push((port, intercept.dports.clone()));
            }
            listeners.push((broker, port, listener));
        }
        if !intercepts.is_empty() {
            slot.install_intercepts(&intercepts)
                .with_context(|| format!("install intercepts {intercepts:?}"))?;
        }
        for (broker, port, listener) in listeners {
            let listener = TcpListener::from_std(listener)
                .context("register brokered listener with the runtime")?;
            let ctx = Arc::new(AcceptContext {
                runtime: Arc::clone(runtime),
                identity: identity.clone(),
                listener_port: port,
                handler: broker.handler.clone(),
                params: broker.params.clone(),
                egress: egress.clone(),
                sandbox_permits: Arc::clone(&endpoints.sandbox_permits),
                over_limit: Arc::clone(&endpoints.over_limit),
                relays: Arc::clone(&endpoints.relays),
            });
            debug!(
                sandbox_id = %identity.sandbox_id,
                port,
                handler = %broker.handler,
                intercept = ?broker.intercept.as_ref().map(|i| &i.dports),
                "brokered listener open"
            );
            endpoints
                .accept_tasks
                .push(tokio::spawn(accept_loop(listener, ctx)));
            endpoints.ports.push(port);
        }
        Ok(endpoints)
    }

    /// Listener ports inside the sandbox namespace, in policy order.
    pub fn ports(&self) -> &[u16] {
        &self.ports
    }

    /// Connections closed because the sandbox or node budget was exhausted.
    pub fn over_limit_count(&self) -> u64 {
        self.over_limit.load(Ordering::Relaxed)
    }

    async fn stop_accepting(&mut self) {
        for task in self.accept_tasks.drain(..) {
            task.abort();
            let _ = task.await;
        }
    }

    /// Stops accepting, ends every relay and waits for all of them, so no
    /// socket of this sandbox outlives the namespace it was opened in. The
    /// caller removes the intercept and releases the slot afterwards.
    pub async fn shutdown(&mut self) {
        self.stop_accepting().await;
        let mut relays =
            std::mem::take(&mut *self.relays.lock().unwrap_or_else(|e| e.into_inner()));
        relays.abort_all();
        while relays.join_next().await.is_some() {}
    }
}

/// A dropped instance must leave no socket bound in a namespace that goes
/// back to the pool: an accept task holds its listener, and a relay task
/// holds the relay set through its context, so neither ends on its own.
impl Drop for BrokeredEndpoints {
    fn drop(&mut self) {
        for task in self.accept_tasks.drain(..) {
            task.abort();
        }
        if !self.relays_handed_over {
            self.relays
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .abort_all();
        }
    }
}

async fn accept_loop(listener: TcpListener, ctx: Arc<AcceptContext>) {
    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(err) => {
                // EMFILE and friends are transient; a closed listener is not.
                if err.kind() == std::io::ErrorKind::Other {
                    warn!(error = %err, "brokered listener accept failed");
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
        };
        reap_finished(&ctx.relays);
        if !ctx.runtime.is_reachable() {
            debug!(sandbox_id = %ctx.identity.sandbox_id, "brokered connection closed: broker unreachable");
            drop(stream);
            continue;
        }
        let Ok(sandbox_permit) = Arc::clone(&ctx.sandbox_permits).try_acquire_owned() else {
            ctx.over_limit.fetch_add(1, Ordering::Relaxed);
            debug!(sandbox_id = %ctx.identity.sandbox_id, "brokered connection refused: sandbox budget exhausted");
            continue;
        };
        let Ok(node_permit) = Arc::clone(&ctx.runtime.node_permits).try_acquire_owned() else {
            ctx.over_limit.fetch_add(1, Ordering::Relaxed);
            debug!(sandbox_id = %ctx.identity.sandbox_id, "brokered connection refused: node budget exhausted");
            continue;
        };
        let ctx = Arc::clone(&ctx);
        let relays = Arc::clone(&ctx.relays);
        relays
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .spawn(relay(stream, peer, ctx, sandbox_permit, node_permit));
    }
}

fn reap_finished(relays: &Mutex<JoinSet<()>>) {
    let mut relays = relays.lock().unwrap_or_else(|e| e.into_inner());
    while relays.try_join_next().is_some() {}
}

async fn relay(
    mut guest: TcpStream,
    peer: SocketAddr,
    ctx: Arc<AcceptContext>,
    _sandbox_permit: OwnedSemaphorePermit,
    _node_permit: OwnedSemaphorePermit,
) {
    let original_dst = original_destination(&guest);
    let header = IdentityHeader {
        v: aenv_egress::header::IDENTITY_HEADER_VERSION,
        node_id: ctx.runtime.node_id.clone(),
        sandbox_id: ctx.identity.sandbox_id.to_string(),
        execution_id: ctx.identity.execution_id.to_string(),
        template_id: ctx.identity.template_id.clone(),
        port: ctx.listener_port,
        handler: ctx.handler.clone(),
        params: ctx.params.clone(),
        original_dst: original_dst.map(SocketAddr::V4),
        egress: ctx.egress.clone(),
        guest_addr: Some(peer),
        issued_at_unix_ms: IdentityHeader::now_unix_ms(),
        nonce: IdentityHeader::fresh_nonce(),
        hmac: String::new(),
    };
    let opened =
        tokio::time::timeout(ctx.runtime.open_timeout, ctx.runtime.transport.open(header)).await;
    let mut broker = match opened {
        Ok(Ok(stream)) => stream,
        Ok(Err(TransportError::Rejected { reason })) => {
            debug!(sandbox_id = %ctx.identity.sandbox_id, reason, "broker rejected the connection");
            return;
        }
        Ok(Err(TransportError::Unavailable(reason))) => {
            warn!(sandbox_id = %ctx.identity.sandbox_id, reason, "broker unavailable");
            return;
        }
        Err(_) => {
            warn!(sandbox_id = %ctx.identity.sandbox_id, "broker did not answer within the open timeout");
            return;
        }
    };
    if let Err(err) = tokio::io::copy_bidirectional(&mut guest, &mut broker).await {
        debug!(sandbox_id = %ctx.identity.sandbox_id, error = %err, "brokered relay ended with an error");
    }
}

/// The address the guest dialled before the DNAT, read from conntrack through
/// the accepted socket. `None` when the connection was not DNATed.
pub fn original_destination(stream: &TcpStream) -> Option<SocketAddrV4> {
    use std::os::fd::AsRawFd;

    let mut raw: libc::sockaddr_in = unsafe { std::mem::zeroed() };
    let mut len = std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t;
    // SAFETY: the fd is a live IPv4 TCP socket owned by `stream`; the buffer
    // and its length describe one sockaddr_in and outlive the call.
    let rc = unsafe {
        libc::getsockopt(
            stream.as_raw_fd(),
            libc::SOL_IP,
            libc::SO_ORIGINAL_DST,
            &mut raw as *mut libc::sockaddr_in as *mut libc::c_void,
            &mut len,
        )
    };
    if rc != 0 || (len as usize) < std::mem::size_of::<libc::sockaddr_in>() {
        return None;
    }
    Some(SocketAddrV4::new(
        Ipv4Addr::from(u32::from_be(raw.sin_addr.s_addr)),
        u16::from_be(raw.sin_port),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::network::policy::{
        DomainRule, HeaderTransform, SandboxNetworkEgressPolicy,
    };

    struct DropMark(Arc<AtomicU64>);

    impl Drop for DropMark {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    // The mark is built by the caller, so it counts a future that is dropped
    // before it is ever polled: an aborted accept loop.
    async fn pending_holding(_mark: DropMark) {
        std::future::pending::<()>().await
    }

    fn endpoints_holding(
        accept_mark: &Arc<AtomicU64>,
        relays: &Arc<Mutex<JoinSet<()>>>,
    ) -> BrokeredEndpoints {
        BrokeredEndpoints {
            accept_tasks: vec![tokio::spawn(pending_holding(DropMark(Arc::clone(
                accept_mark,
            ))))],
            relays: Arc::clone(relays),
            ports: vec![40443],
            over_limit: Arc::new(AtomicU64::new(0)),
            sandbox_permits: Arc::new(Semaphore::new(1)),
            relays_handed_over: false,
        }
    }

    async fn settled(marks: &[&Arc<AtomicU64>]) {
        for _ in 0..1000 {
            if marks.iter().all(|mark| mark.load(Ordering::Relaxed) > 0) {
                return;
            }
            tokio::task::yield_now().await;
        }
    }

    fn rules_policy() -> SandboxNetworkPolicy {
        let rules = std::collections::BTreeMap::from([(
            "api.example.com".to_string(),
            vec![DomainRule {
                transform: HeaderTransform {
                    headers: std::collections::BTreeMap::from([(
                        "authorization".to_string(),
                        "Bearer ${aenv.secrets.k}".to_string(),
                    )]),
                },
            }],
        )]);
        SandboxNetworkPolicy::new(
            BaseSandboxNetworkPolicy::Default,
            SandboxNetworkEgressPolicy::with_rules(None, None, Some(rules)).unwrap(),
        )
    }

    #[tokio::test]
    async fn dropping_the_endpoints_aborts_the_accept_loops_and_the_relays() {
        let accept_mark = Arc::new(AtomicU64::new(0));
        let relay_mark = Arc::new(AtomicU64::new(0));
        let relays = Arc::new(Mutex::new(JoinSet::new()));
        relays
            .lock()
            .unwrap()
            .spawn(pending_holding(DropMark(Arc::clone(&relay_mark))));

        drop(endpoints_holding(&accept_mark, &relays));

        settled(&[&accept_mark, &relay_mark]).await;
        assert_eq!(accept_mark.load(Ordering::Relaxed), 1);
        assert_eq!(relay_mark.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn a_successor_keeps_the_relays_the_dropped_endpoints_handed_it() {
        let accept_mark = Arc::new(AtomicU64::new(0));
        let relay_mark = Arc::new(AtomicU64::new(0));
        let relays = Arc::new(Mutex::new(JoinSet::new()));
        relays
            .lock()
            .unwrap()
            .spawn(pending_holding(DropMark(Arc::clone(&relay_mark))));

        let mut previous = endpoints_holding(&accept_mark, &relays);
        previous.relays_handed_over = true;
        drop(previous);

        settled(&[&accept_mark]).await;
        assert_eq!(accept_mark.load(Ordering::Relaxed), 1);
        assert_eq!(relay_mark.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn the_embedded_broker_does_not_serve_the_handler_public_rules_name() {
        let policy = rules_policy();
        let handler = policy.egress.brokers[0].handler.as_str();

        assert!(!mode_serves_handler(EgressBrokerMode::Embedded, handler));
        assert!(mode_serves_handler(EgressBrokerMode::Remote, handler));
        assert!(!mode_serves_handler(EgressBrokerMode::Disabled, handler));
        assert!(mode_serves_handler(
            EgressBrokerMode::Embedded,
            IdentityEchoHandler::NAME
        ));
        assert!(mode_serves_handler(
            EgressBrokerMode::Embedded,
            TcpRelayHandler::NAME
        ));
    }

    #[test]
    fn the_summary_mirrors_base_policy_and_cidrs() {
        let egress = SandboxNetworkEgressPolicy::new(
            Some(vec!["8.8.8.8".into()]),
            Some(vec!["203.0.113.0/24".into()]),
        )
        .unwrap();
        let open = SandboxNetworkPolicy::new(BaseSandboxNetworkPolicy::Default, egress.clone());
        let closed = SandboxNetworkPolicy::new(BaseSandboxNetworkPolicy::Deny, egress);

        let summary = egress_summary(&open);
        assert!(summary.allow_internet);
        assert_eq!(summary.allowed_cidrs, vec!["8.8.8.8/32"]);
        assert_eq!(summary.denied_cidrs, vec!["203.0.113.0/24"]);
        assert!(!egress_summary(&closed).allow_internet);
    }

    #[tokio::test]
    async fn without_a_dnat_the_original_destination_is_absent_or_the_real_one() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client = TcpStream::connect(addr).await.unwrap();
        let (accepted, _) = listener.accept().await.unwrap();
        // Without a conntrack entry the option is unavailable; with one and
        // no NAT, conntrack reports the address the client actually dialled.
        match original_destination(&accepted) {
            None => {}
            Some(dst) => assert_eq!(SocketAddr::V4(dst), addr),
        }
        drop(client);
    }
}
