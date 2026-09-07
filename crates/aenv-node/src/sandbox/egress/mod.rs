//! Brokered endpoints: listeners the runtime opens inside a sandbox's network
//! namespace for the `brokers` its egress policy declares. Every accepted
//! connection is the guest's by construction, so the runtime attaches the
//! sandbox's identity and relays the bytes to the broker through a
//! [`BrokerTransport`]. The runtime parses no application protocol.

use std::collections::BTreeMap;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use aenv_egress::credential::NoCredentials;
use aenv_egress::handlers::echo::IdentityEchoHandler;
use aenv_egress::transport::LocalTransport;
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
use crate::sandbox::network::policy::{BaseSandboxNetworkPolicy, BrokeredEndpoint};
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
    /// Whether the last probe of the node-local broker succeeded; always true
    /// for the embedded transport.
    reachable: Arc<std::sync::atomic::AtomicBool>,
}

/// How often the node-local broker is probed for the heartbeat.
const LOCAL_PROBE_INTERVAL: Duration = Duration::from_secs(5);

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
                let denied: Vec<String> = config
                    .network
                    .egress
                    .effective_denied_cidrs()
                    .context("egress_broker: the always-denied table is unusable")?
                    .into_iter()
                    .map(|network| network.to_string())
                    .collect();
                let guard = UpstreamGuard::new(
                    BrokerDenyList::with_extra(&denied)
                        .context("egress_broker: the always-denied table is not all cidrs")?,
                );
                let dispatcher = Arc::new(
                    Dispatcher::new(Arc::new(NoCredentials), Arc::new(guard))
                        .with_handler(Arc::new(IdentityEchoHandler)),
                );
                Arc::new(EmbeddedTransport::new(dispatcher))
            }
            EgressBrokerMode::Local => {
                let socket_path = egress
                    .socket_path
                    .as_ref()
                    .context("egress_broker.socket_path is required in local mode")?;
                Arc::new(
                    LocalTransport::new(socket_path)
                        .with_connect_timeout(Duration::from_millis(egress.open_timeout_ms)),
                )
            }
        };
        // Guests trust the root that signs the intercepted-name chain, not
        // this node's own issuer: a sandbox that resumes elsewhere keeps the
        // trust store it loaded before it moved.
        let ca_bundle = match egress.guest_ca_cert_path.as_ref() {
            Some(path) => Some(std::fs::read_to_string(path).with_context(|| {
                format!("read egress_broker.guest_ca_cert_path {}", path.display())
            })?),
            None => None,
        };
        let reachable = Arc::new(std::sync::atomic::AtomicBool::new(
            egress.mode == EgressBrokerMode::Embedded,
        ));
        if egress.mode == EgressBrokerMode::Local {
            Self::spawn_local_probe(Arc::clone(&reachable), egress);
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
    fn spawn_local_probe(
        reachable: Arc<std::sync::atomic::AtomicBool>,
        egress: &crate::cfg::EgressBrokerConfig,
    ) {
        let Some(socket_path) = egress.socket_path.clone() else {
            return;
        };
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
                    let probe = LocalTransport::new(&socket_path).with_connect_timeout(timeout);
                    let socket_path = socket_path.display().to_string();
                    loop {
                        let ok = probe.probe().await.is_ok();
                        let was = reachable.swap(ok, Ordering::Relaxed);
                        if was != ok {
                            if ok {
                                info!(socket_path, "egress broker reachable");
                            } else {
                                warn!(
                                    socket_path,
                                    "egress broker unreachable; brokered connections fail fast"
                                );
                            }
                        }
                        tokio::time::sleep(LOCAL_PROBE_INTERVAL).await;
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
            EgressBrokerMode::Local => {
                if self.reachable.load(Ordering::Relaxed) {
                    EgressBrokerState::LocalOk
                } else {
                    EgressBrokerState::LocalUnreachable
                }
            }
        }
    }

    /// False while the node-local broker fails its probe; the accept loop then
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
            "the sandbox declares network rules but this node has no egress broker; set [egress_broker].mode (AENV_EGRESS_BROKER_MODE) to \"local\" and point egress_broker.socket_path at the aenv-egress socket on this node",
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
/// credential source, no TLS stack and no operator allowlist, so it carries
/// only [`IdentityEchoHandler`]. Everything else lives in the broker process.
pub fn mode_serves_handler(mode: EgressBrokerMode, handler: &str) -> bool {
    match mode {
        EgressBrokerMode::Disabled => false,
        EgressBrokerMode::Embedded => handler == IdentityEchoHandler::NAME,
        EgressBrokerMode::Local => true,
    }
}

/// Who a set of brokered endpoints speaks for.
#[derive(Clone, Debug, Eq, PartialEq)]
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

/// What every listener of one sandbox reads once per accepted connection:
/// who the sandbox speaks for, the broker runtime it opens through, the
/// summary the broker is told, and the endpoint each declared port serves.
///
/// An update publishes a whole new one of these in a single swap, so a
/// connection never sees half of a policy.
#[derive(Clone)]
struct ActivePolicy {
    runtime: Arc<EgressRuntime>,
    identity: SandboxIdentity,
    egress: EgressPolicySummary,
    /// Keyed by the port the policy declared, which is what identifies a
    /// listener. `SandboxNetworkEgressPolicy` declares each port once and
    /// declares 0 — the kernel picks — at most once.
    endpoints: BTreeMap<u16, BrokeredEndpoint>,
}

/// The cell every listener of one sandbox reads its policy through.
type ActiveCell = Arc<Mutex<Arc<ActivePolicy>>>;

fn endpoints_by_port(policy: &SandboxNetworkPolicy) -> BTreeMap<u16, BrokeredEndpoint> {
    policy
        .egress
        .brokers
        .iter()
        .map(|broker| (broker.port, broker.clone()))
        .collect()
}

struct AcceptContext {
    /// The port the policy declared, which is this listener's key into the
    /// active policy.
    declared_port: u16,
    /// The port the kernel gave it, stamped on every header it sends.
    listener_port: u16,
    shared: SandboxEndpointContext,
}

/// What every listener of one sandbox shares and an update never rebuilds.
#[derive(Clone)]
struct SandboxEndpointContext {
    active: ActiveCell,
    sandbox_permits: Arc<Semaphore>,
    over_limit: Arc<AtomicU64>,
    relays: Arc<Mutex<JoinSet<()>>>,
}

impl SandboxEndpointContext {
    fn active(&self) -> Arc<ActivePolicy> {
        Arc::clone(&self.active.lock().unwrap_or_else(|e| e.into_inner()))
    }
}

/// One live listener and the task accepting on it.
struct LiveEndpoint {
    /// The port the policy declared it for. Two policies naming the same
    /// declared port want the same socket, whatever else they changed about
    /// it.
    declared_port: u16,
    /// The port the kernel gave it, which is `declared_port` unless that was 0.
    port: u16,
    accept: JoinHandle<()>,
}

/// What brokered endpoints need from the namespace they live in: a listener
/// bound inside it, and the one intercept set it carries.
///
/// `Sync` because an update holds it across an await.
pub trait EndpointNamespace: Sync {
    fn listen(&self, port: u16) -> Result<std::net::TcpListener>;
    fn install_intercepts(&self, intercepts: &[(u16, Vec<u16>)]) -> Result<()>;
}

impl EndpointNamespace for Slot {
    fn listen(&self, port: u16) -> Result<std::net::TcpListener> {
        self.listen_in_namespace(port)
    }

    fn install_intercepts(&self, intercepts: &[(u16, Vec<u16>)]) -> Result<()> {
        Slot::install_intercepts(self, intercepts)
    }
}

/// The listeners one sandbox's policy asked for, alive until `shutdown` or
/// drop. Accept loops and relays are separate tasks: an update stops
/// accepting on the listeners it dropped without cutting connections in
/// flight.
pub struct BrokeredEndpoints {
    live: Vec<LiveEndpoint>,
    shared: SandboxEndpointContext,
}

impl BrokeredEndpoints {
    /// Opens one listener per broker in `policy` inside `slot`'s namespace,
    /// installs the intercept set the policy asks for and starts accepting.
    pub fn spawn(
        runtime: &Arc<EgressRuntime>,
        slot: &dyn EndpointNamespace,
        identity: SandboxIdentity,
        policy: &SandboxNetworkPolicy,
    ) -> Result<Self> {
        let active = ActivePolicy {
            runtime: Arc::clone(runtime),
            identity,
            egress: egress_summary(policy),
            endpoints: endpoints_by_port(policy),
        };
        let shared = SandboxEndpointContext {
            active: Arc::new(Mutex::new(Arc::new(active))),
            sandbox_permits: Arc::new(Semaphore::new(runtime.per_sandbox_conns)),
            over_limit: Arc::new(AtomicU64::new(0)),
            relays: Arc::new(Mutex::new(JoinSet::new())),
        };
        // Every listener binds before any intercept is installed: the
        // namespace holds one intercept set, so the DNATs go in as one.
        let bound = bind_listeners(slot, &shared.active())?;
        let endpoints = Self {
            live: start_accepting(bound, &shared),
            shared,
        };
        // Unconditionally, empty set included: a slot back from the pool with
        // an intercept of its own must not lend it to its next tenant.
        let intercepts = endpoints.intercepts();
        slot.install_intercepts(&intercepts)
            .with_context(|| format!("install intercepts {intercepts:?}"))?;
        Ok(endpoints)
    }

    /// Moves this sandbox's listeners to `policy`.
    ///
    /// A listener is identified by the port the policy declared it for, and
    /// by nothing else: the handler, the params, the intercept, the identity
    /// and the egress summary are all read from the active policy once per
    /// accepted connection, so changing any of them costs neither the socket
    /// nor the connections on it. Only a declared port the policy dropped
    /// closes a listener, and only one it added binds another.
    ///
    /// Nothing is published until every bind has succeeded, so a refused
    /// update leaves every listener still open serving the identity, the
    /// summary and the endpoint table the sandbox is recorded with. What it
    /// does not leave is all of them: a port the new policy dropped is closed
    /// and its DNAT withdrawn before anything binds, because a policy that
    /// moves a handler off a port needs the port back first. The set that
    /// survives is the recorded policy minus what this call had already
    /// dropped.
    ///
    /// The last step is the exception, and it is fail-closed: an
    /// `install_intercepts` that fails after the swap returns an error with
    /// the new policy published, so the listeners describe it while the
    /// sandbox record still holds the old one.
    pub async fn update(
        &mut self,
        runtime: &Arc<EgressRuntime>,
        slot: &dyn EndpointNamespace,
        identity: SandboxIdentity,
        policy: &SandboxNetworkPolicy,
    ) -> Result<()> {
        let wanted = endpoints_by_port(policy);

        // Tighten before loosening: a DNAT whose listener this update closes,
        // or whose intercept the policy turned off, comes out before the
        // listener does. Nothing has changed yet, so a failure here is a
        // failed update and not a half-applied one.
        let installed = self.intercepts();
        let narrowed = self.surviving_intercepts(&wanted);
        if narrowed != installed {
            slot.install_intercepts(&narrowed)
                .with_context(|| format!("install intercepts {narrowed:?}"))?;
        }

        // Closed before anything new binds: a policy that moved one handler
        // off a port and put another on it needs the port back first.
        let (kept, retired): (Vec<LiveEndpoint>, Vec<LiveEndpoint>) = self
            .live
            .drain(..)
            .partition(|live| wanted.contains_key(&live.declared_port));
        self.live = kept;
        for mut gone in retired {
            gone.accept.abort();
            let _ = (&mut gone.accept).await;
        }

        let additions: Vec<&BrokeredEndpoint> = wanted
            .values()
            .filter(|broker| {
                !self
                    .live
                    .iter()
                    .any(|live| live.declared_port == broker.port)
            })
            .collect();
        // A failure here leaves the listeners that are left serving the
        // policy in the record, with the intercepts the step above put in
        // step with them, and nothing published.
        let bound = bind_listeners_for(slot, &additions)?;

        // Published once, after the last bind. Everything above this line can
        // fail; nothing above it is visible to a connection.
        self.publish(ActivePolicy {
            runtime: Arc::clone(runtime),
            identity,
            egress: egress_summary(policy),
            endpoints: wanted,
        });
        self.live.extend(start_accepting(bound, &self.shared));

        let intercepts = self.intercepts();
        slot.install_intercepts(&intercepts)
            .with_context(|| format!("install intercepts {intercepts:?}"))?;
        Ok(())
    }

    fn active(&self) -> Arc<ActivePolicy> {
        self.shared.active()
    }

    fn publish(&self, active: ActivePolicy) {
        *self.shared.active.lock().unwrap_or_else(|e| e.into_inner()) = Arc::new(active);
    }

    /// The intercept set the namespace should carry for the live listeners
    /// under the policy that is published now.
    fn intercepts(&self) -> Vec<(u16, Vec<u16>)> {
        let active = self.active();
        self.live
            .iter()
            .filter_map(|live| intercept_target(&active.endpoints, live))
            .collect()
    }

    /// The part of the installed intercept set that `wanted` keeps: the
    /// listeners it still declares, with the dports they still ask for.
    fn surviving_intercepts(
        &self,
        wanted: &BTreeMap<u16, BrokeredEndpoint>,
    ) -> Vec<(u16, Vec<u16>)> {
        let active = self.active();
        self.live
            .iter()
            .filter_map(|live| {
                let installed = intercept_target(&active.endpoints, live)?;
                (intercept_target(wanted, live) == Some(installed.clone())).then_some(installed)
            })
            .collect()
    }

    /// Listener ports inside the sandbox namespace, by declared port.
    pub fn ports(&self) -> Vec<u16> {
        self.live.iter().map(|live| live.port).collect()
    }

    /// Connections closed because the sandbox or node budget was exhausted.
    pub fn over_limit_count(&self) -> u64 {
        self.shared.over_limit.load(Ordering::Relaxed)
    }

    async fn stop_accepting(&mut self) {
        for mut live in self.live.drain(..) {
            live.accept.abort();
            let _ = (&mut live.accept).await;
        }
    }

    /// Stops accepting, ends every relay and waits for all of them, so no
    /// socket of this sandbox outlives the namespace it was opened in. The
    /// caller removes the intercept and releases the slot afterwards.
    pub async fn shutdown(&mut self) {
        self.stop_accepting().await;
        let mut relays =
            std::mem::take(&mut *self.shared.relays.lock().unwrap_or_else(|e| e.into_inner()));
        relays.abort_all();
        while relays.join_next().await.is_some() {}
    }
}

/// The DNAT one live listener asks for under `endpoints`, if it asks for one.
fn intercept_target(
    endpoints: &BTreeMap<u16, BrokeredEndpoint>,
    live: &LiveEndpoint,
) -> Option<(u16, Vec<u16>)> {
    let intercept = endpoints.get(&live.declared_port)?.intercept.as_ref()?;
    Some((live.port, intercept.dports.clone()))
}

/// A bound listener that is not accepting yet, so nothing reads a policy that
/// has not been published.
struct BoundListener {
    declared_port: u16,
    port: u16,
    listener: TcpListener,
}

/// Binds one listener per broker in `active`. A failure drops the ones this
/// call bound, so the caller's set is left as it was.
fn bind_listeners(
    slot: &dyn EndpointNamespace,
    active: &Arc<ActivePolicy>,
) -> Result<Vec<BoundListener>> {
    let wanted: Vec<&BrokeredEndpoint> = active.endpoints.values().collect();
    bind_listeners_for(slot, &wanted)
}

fn bind_listeners_for(
    slot: &dyn EndpointNamespace,
    wanted: &[&BrokeredEndpoint],
) -> Result<Vec<BoundListener>> {
    wanted
        .iter()
        .map(|broker| bind_listener(slot, broker))
        .collect()
}

fn bind_listener(slot: &dyn EndpointNamespace, broker: &BrokeredEndpoint) -> Result<BoundListener> {
    let listener = slot
        .listen(broker.port)
        .with_context(|| format!("open brokered listener for handler {}", broker.handler))?;
    let port = listener
        .local_addr()
        .context("read brokered listener address")?
        .port();
    let listener =
        TcpListener::from_std(listener).context("register brokered listener with the runtime")?;
    Ok(BoundListener {
        declared_port: broker.port,
        port,
        listener,
    })
}

/// Starts one accept task per bound listener. Called only after the policy
/// those tasks read has been published.
fn start_accepting(
    bound: Vec<BoundListener>,
    shared: &SandboxEndpointContext,
) -> Vec<LiveEndpoint> {
    bound
        .into_iter()
        .map(|bound| {
            let ctx = Arc::new(AcceptContext {
                declared_port: bound.declared_port,
                listener_port: bound.port,
                shared: shared.clone(),
            });
            debug!(
                sandbox_id = %shared.active().identity.sandbox_id,
                declared_port = bound.declared_port,
                port = bound.port,
                "brokered listener open"
            );
            LiveEndpoint {
                declared_port: bound.declared_port,
                port: bound.port,
                accept: tokio::spawn(accept_loop(bound.listener, ctx)),
            }
        })
        .collect()
}

/// A dropped instance must leave no socket bound in a namespace that goes
/// back to the pool: an accept task holds its listener, and a relay task
/// holds the relay set through its context, so neither ends on its own.
impl Drop for BrokeredEndpoints {
    fn drop(&mut self) {
        for live in self.live.drain(..) {
            live.accept.abort();
        }
        self.shared
            .relays
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .abort_all();
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
        reap_finished(&ctx.shared.relays);
        // One read per connection, and everything this connection is told
        // comes from it: a policy that changed between two connections is
        // seen by the second and not by the first.
        let active = ctx.shared.active();
        let sandbox_id = &active.identity.sandbox_id;
        let Some(endpoint) = active.endpoints.get(&ctx.declared_port) else {
            debug!(%sandbox_id, "brokered connection closed: the policy no longer declares this port");
            drop(stream);
            continue;
        };
        if !active.runtime.is_reachable() {
            debug!(%sandbox_id, "brokered connection closed: broker unreachable");
            drop(stream);
            continue;
        }
        let Ok(sandbox_permit) = Arc::clone(&ctx.shared.sandbox_permits).try_acquire_owned() else {
            ctx.shared.over_limit.fetch_add(1, Ordering::Relaxed);
            debug!(%sandbox_id, "brokered connection refused: sandbox budget exhausted");
            continue;
        };
        let Ok(node_permit) = Arc::clone(&active.runtime.node_permits).try_acquire_owned() else {
            ctx.shared.over_limit.fetch_add(1, Ordering::Relaxed);
            debug!(%sandbox_id, "brokered connection refused: node budget exhausted");
            continue;
        };
        let header = IdentityHeader {
            v: aenv_egress::header::IDENTITY_HEADER_VERSION,
            node_id: active.runtime.node_id.clone(),
            sandbox_id: active.identity.sandbox_id.to_string(),
            execution_id: active.identity.execution_id.to_string(),
            template_id: active.identity.template_id.clone(),
            port: ctx.listener_port,
            handler: endpoint.handler.clone(),
            params: endpoint.params.clone(),
            original_dst: original_destination(&stream).map(SocketAddr::V4),
            egress: active.egress.clone(),
            guest_addr: Some(peer),
            issued_at_unix_ms: IdentityHeader::now_unix_ms(),
        };
        let runtime = Arc::clone(&active.runtime);
        ctx.shared
            .relays
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .spawn(relay(stream, header, runtime, sandbox_permit, node_permit));
    }
}

fn reap_finished(relays: &Mutex<JoinSet<()>>) {
    let mut relays = relays.lock().unwrap_or_else(|e| e.into_inner());
    while relays.try_join_next().is_some() {}
}

async fn relay(
    mut guest: TcpStream,
    header: IdentityHeader,
    runtime: Arc<EgressRuntime>,
    _sandbox_permit: OwnedSemaphorePermit,
    _node_permit: OwnedSemaphorePermit,
) {
    let sandbox_id = header.sandbox_id.clone();
    let opened = tokio::time::timeout(runtime.open_timeout, runtime.transport.open(header)).await;
    let mut broker = match opened {
        Ok(Ok(stream)) => stream,
        Ok(Err(TransportError::Rejected { reason })) => {
            debug!(%sandbox_id, reason, "broker rejected the connection");
            return;
        }
        Ok(Err(TransportError::Unavailable(reason))) => {
            warn!(%sandbox_id, reason, "broker unavailable");
            return;
        }
        Err(_) => {
            warn!(%sandbox_id, "broker did not answer within the open timeout");
            return;
        }
    };
    if let Err(err) = tokio::io::copy_bidirectional(&mut guest, &mut broker).await {
        debug!(%sandbox_id, error = %err, "brokered relay ended with an error");
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

    fn test_identity() -> SandboxIdentity {
        SandboxIdentity {
            sandbox_id: SandboxId::new(),
            execution_id: ExecutionId::new(),
            template_id: "tpl".to_string(),
        }
    }

    fn test_runtime() -> Arc<EgressRuntime> {
        test_runtime_over(Arc::new(aenv_egress::transport::LocalTransport::new(
            "/nonexistent/broker.sock",
        )))
    }

    fn test_runtime_over(transport: Arc<dyn BrokerTransport>) -> Arc<EgressRuntime> {
        Arc::new(EgressRuntime {
            mode: EgressBrokerMode::Local,
            node_id: "node-under-test".to_string(),
            transport,
            node_permits: Arc::new(Semaphore::new(1)),
            per_sandbox_conns: 1,
            open_timeout: Duration::from_millis(50),
            ca_bundle: None,
            reachable: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        })
    }

    /// A transport that records the header of every connection it is asked
    /// to open, and opens none.
    #[derive(Default)]
    struct RecordingTransport {
        headers: Mutex<Vec<IdentityHeader>>,
    }

    impl RecordingTransport {
        async fn first_handler(&self) -> Option<String> {
            for _ in 0..1000 {
                if let Some(header) = self.headers.lock().unwrap().first() {
                    return Some(header.handler.clone());
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            None
        }
    }

    #[async_trait::async_trait]
    impl BrokerTransport for RecordingTransport {
        async fn open(
            &self,
            header: IdentityHeader,
        ) -> std::result::Result<Box<dyn aenv_egress::transport::AsyncStream>, TransportError>
        {
            self.headers.lock().unwrap().push(header);
            Err(TransportError::Unavailable("recorded".into()))
        }
    }

    fn endpoint(port: u16, handler: &str, intercept: Option<Vec<u16>>) -> BrokeredEndpoint {
        BrokeredEndpoint {
            port,
            handler: handler.to_string(),
            params: serde_json::json!({}),
            intercept: intercept
                .map(|dports| crate::sandbox::network::policy::Intercept { dports }),
        }
    }

    fn endpoints_holding(
        accept_mark: &Arc<AtomicU64>,
        relays: &Arc<Mutex<JoinSet<()>>>,
    ) -> BrokeredEndpoints {
        let held = endpoint(40443, "http", None);
        BrokeredEndpoints {
            live: vec![LiveEndpoint {
                declared_port: held.port,
                port: 40443,
                accept: tokio::spawn(pending_holding(DropMark(Arc::clone(accept_mark)))),
            }],
            shared: SandboxEndpointContext {
                active: Arc::new(Mutex::new(Arc::new(ActivePolicy {
                    runtime: test_runtime(),
                    identity: test_identity(),
                    egress: EgressPolicySummary::default(),
                    endpoints: BTreeMap::from([(held.port, held)]),
                }))),
                sandbox_permits: Arc::new(Semaphore::new(1)),
                over_limit: Arc::new(AtomicU64::new(0)),
                relays: Arc::clone(relays),
            },
        }
    }

    /// One intercept set as the namespace received it.
    type InterceptSet = Vec<(u16, Vec<u16>)>;

    /// A namespace that binds on loopback and records every intercept set it
    /// is given, so an update can be driven without a network namespace.
    struct FakeNamespace {
        installed: Mutex<Vec<InterceptSet>>,
        refuse_binds: std::sync::atomic::AtomicBool,
    }

    impl FakeNamespace {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                installed: Mutex::new(Vec::new()),
                refuse_binds: std::sync::atomic::AtomicBool::new(false),
            })
        }

        fn installed(&self) -> Vec<InterceptSet> {
            self.installed.lock().unwrap().clone()
        }

        fn last_intercepts(&self) -> InterceptSet {
            self.installed
                .lock()
                .unwrap()
                .last()
                .cloned()
                .unwrap_or_default()
        }
    }

    impl EndpointNamespace for FakeNamespace {
        // The kernel picks whatever the policy declared: a real namespace is
        // one sandbox's own, so a declared port is not a claim on the
        // machine, and two of these tests running at once would collide here
        // if it were taken literally.
        fn listen(&self, _port: u16) -> Result<std::net::TcpListener> {
            if self.refuse_binds.load(Ordering::Relaxed) {
                anyhow::bail!("bind refused by the test");
            }
            let listener = std::net::TcpListener::bind(("127.0.0.1", 0))?;
            listener.set_nonblocking(true)?;
            Ok(listener)
        }

        fn install_intercepts(&self, intercepts: &[(u16, Vec<u16>)]) -> Result<()> {
            self.installed.lock().unwrap().push(intercepts.to_vec());
            Ok(())
        }
    }

    fn policy_with_brokers(brokers: Vec<BrokeredEndpoint>) -> SandboxNetworkPolicy {
        let mut policy = SandboxNetworkPolicy::new(
            BaseSandboxNetworkPolicy::Default,
            SandboxNetworkEgressPolicy::new(None, None).unwrap(),
        );
        policy.egress.brokers = brokers;
        policy
    }

    /// The endpoints a policy asks for, opened through the fake namespace.
    fn spawned(ns: &Arc<FakeNamespace>, policy: &SandboxNetworkPolicy) -> BrokeredEndpoints {
        BrokeredEndpoints::spawn(&test_runtime(), ns.as_ref(), test_identity(), policy)
            .expect("the fake namespace binds")
    }

    async fn update_with(
        endpoints: &mut BrokeredEndpoints,
        ns: &Arc<FakeNamespace>,
        policy: &SandboxNetworkPolicy,
    ) -> Result<()> {
        let identity = endpoints.active().identity.clone();
        endpoints
            .update(&test_runtime(), ns.as_ref(), identity, policy)
            .await
    }

    #[tokio::test]
    async fn turning_an_intercept_on_and_off_keeps_the_listener_and_moves_the_dnat() {
        let ns = FakeNamespace::new();
        let off = policy_with_brokers(vec![endpoint(15432, "postgres", None)]);
        let mut endpoints = spawned(&ns, &off);
        let port = endpoints.ports()[0];
        assert!(ns.last_intercepts().is_empty(), "nothing asked for a DNAT");

        let on = policy_with_brokers(vec![endpoint(15432, "postgres", Some(vec![15432]))]);
        update_with(&mut endpoints, &ns, &on).await.unwrap();
        assert_eq!(endpoints.ports(), vec![port], "the listener was rebound");
        assert_eq!(ns.last_intercepts(), vec![(port, vec![15432])]);

        update_with(&mut endpoints, &ns, &off).await.unwrap();
        assert_eq!(endpoints.ports(), vec![port], "the listener was rebound");
        assert!(
            ns.last_intercepts().is_empty(),
            "an intercept the policy turned off stayed installed: {:?}",
            ns.last_intercepts()
        );
    }

    #[tokio::test]
    async fn an_added_endpoint_is_bound_and_a_dropped_one_is_closed() {
        let ns = FakeNamespace::new();
        let one = policy_with_brokers(vec![endpoint(0, "http", Some(vec![443]))]);
        let mut endpoints = spawned(&ns, &one);
        let http_port = endpoints.ports()[0];

        let both = policy_with_brokers(vec![
            endpoint(0, "http", Some(vec![443])),
            endpoint(15432, "postgres", None),
        ]);
        update_with(&mut endpoints, &ns, &both).await.unwrap();
        assert_eq!(endpoints.live.len(), 2);
        assert_eq!(
            endpoints.ports()[0],
            http_port,
            "the listener that stayed was rebound"
        );
        assert_eq!(ns.last_intercepts(), vec![(http_port, vec![443])]);

        let only_postgres = policy_with_brokers(vec![endpoint(15432, "postgres", None)]);
        update_with(&mut endpoints, &ns, &only_postgres)
            .await
            .unwrap();
        assert_eq!(endpoints.live.len(), 1);
        assert_eq!(endpoints.live[0].declared_port, 15432);
        assert!(
            ns.last_intercepts().is_empty(),
            "the dropped listener's DNAT stayed installed"
        );
    }

    #[tokio::test]
    async fn a_dropped_listeners_dnat_comes_out_before_the_listener_does() {
        let ns = FakeNamespace::new();
        let both = policy_with_brokers(vec![
            endpoint(0, "http", Some(vec![443])),
            endpoint(15432, "postgres", Some(vec![15432])),
        ]);
        let mut endpoints = spawned(&ns, &both);
        let (http_port, postgres_port) = (endpoints.ports()[0], endpoints.ports()[1]);
        let surviving: InterceptSet = vec![(postgres_port, vec![15432])];

        let only_postgres =
            policy_with_brokers(vec![endpoint(15432, "postgres", Some(vec![15432]))]);
        update_with(&mut endpoints, &ns, &only_postgres)
            .await
            .unwrap();

        let installed = ns.installed();
        assert_eq!(
            installed[0],
            vec![(http_port, vec![443]), (postgres_port, vec![15432])],
            "spawn installed something else"
        );
        assert_eq!(
            &installed[1..],
            [surviving.clone(), surviving],
            "the DNAT of the listener this update closed outlived it"
        );
    }

    #[tokio::test]
    async fn a_bind_that_fails_publishes_nothing_and_keeps_every_listener() {
        let ns = FakeNamespace::new();
        let one = policy_with_brokers(vec![endpoint(0, "http", Some(vec![443]))]);
        let mut endpoints = spawned(&ns, &one);
        let http_port = endpoints.ports()[0];
        let before = endpoints.active();

        ns.refuse_binds.store(true, Ordering::Relaxed);
        let both = policy_with_brokers(vec![
            endpoint(0, "http", Some(vec![443])),
            endpoint(15432, "postgres", None),
        ]);
        let failed = update_with(&mut endpoints, &ns, &both).await;

        assert!(failed.is_err(), "a refused bind must fail the update");
        assert_eq!(
            endpoints.ports(),
            vec![http_port],
            "the listener this update never touched was closed"
        );
        assert!(
            Arc::ptr_eq(&before, &endpoints.active()),
            "a failed update published a policy nobody accepted"
        );
        assert_eq!(
            ns.last_intercepts(),
            vec![(http_port, vec![443])],
            "the intercept stopped describing the listeners that are there"
        );
    }

    #[tokio::test]
    async fn a_new_identity_keeps_every_listener() {
        let ns = FakeNamespace::new();
        let policy = policy_with_brokers(vec![endpoint(0, "http", Some(vec![443]))]);
        let mut endpoints = spawned(&ns, &policy);
        let before = endpoints.ports()[0];
        let identity = test_identity();

        endpoints
            .update(&test_runtime(), ns.as_ref(), identity.clone(), &policy)
            .await
            .unwrap();

        assert_eq!(
            endpoints.ports(),
            vec![before],
            "an execution id is stamped on the next header, not on the socket"
        );
        assert_eq!(endpoints.active().identity, identity);
        assert_eq!(ns.last_intercepts(), vec![(before, vec![443])]);
    }

    #[tokio::test]
    async fn a_kept_listener_stamps_the_handler_the_policy_names_now() {
        let ns = FakeNamespace::new();
        let transport = Arc::new(RecordingTransport::default());
        let runtime = test_runtime_over(Arc::clone(&transport) as Arc<dyn BrokerTransport>);
        let before = policy_with_brokers(vec![endpoint(15432, "postgres", None)]);
        let mut endpoints =
            BrokeredEndpoints::spawn(&runtime, ns.as_ref(), test_identity(), &before).unwrap();
        let port = endpoints.ports()[0];

        let after = policy_with_brokers(vec![endpoint(15432, "tcp", None)]);
        let identity = endpoints.active().identity.clone();
        endpoints
            .update(&runtime, ns.as_ref(), identity, &after)
            .await
            .unwrap();
        assert_eq!(endpoints.ports(), vec![port], "the listener was rebound");

        let _guest = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        assert_eq!(transport.first_handler().await.as_deref(), Some("tcp"));
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
    async fn only_a_declared_port_the_policy_dropped_costs_a_listener() {
        let ns = FakeNamespace::new();
        let before = policy_with_brokers(vec![endpoint(15432, "postgres", None)]);
        let mut endpoints = spawned(&ns, &before);
        let port = endpoints.ports()[0];

        // Handler, params and intercept all move at once, and the socket
        // does not: each of them is read per connection.
        let mut moved = endpoint(15432, "tcp", Some(vec![15432]));
        moved.params = serde_json::json!({"upstream": "db.example.com:5432"});
        update_with(
            &mut endpoints,
            &ns,
            &policy_with_brokers(vec![moved.clone()]),
        )
        .await
        .unwrap();
        assert_eq!(endpoints.ports(), vec![port]);
        assert_eq!(endpoints.active().endpoints[&15432], moved);

        // The port itself is the one thing that does cost it.
        update_with(
            &mut endpoints,
            &ns,
            &policy_with_brokers(vec![endpoint(15433, "postgres", None)]),
        )
        .await
        .unwrap();
        assert_ne!(endpoints.ports(), vec![port]);
        assert_eq!(endpoints.live[0].declared_port, 15433);
    }

    #[tokio::test]
    async fn the_intercept_set_names_the_bound_port_of_every_listener_that_asked_for_one() {
        let accept_mark = Arc::new(AtomicU64::new(0));
        let relays = Arc::new(Mutex::new(JoinSet::new()));
        let mut endpoints = endpoints_holding(&accept_mark, &relays);
        let asked = endpoint(0, "http", Some(vec![443]));
        let mut published = (*endpoints.active()).clone();
        published.endpoints.insert(asked.port, asked);
        endpoints.publish(published);
        endpoints.live.push(LiveEndpoint {
            declared_port: 0,
            // Port 0 asks the kernel for one; the intercept has to name what
            // it got, not what the policy wrote.
            port: 46821,
            accept: tokio::spawn(std::future::pending()),
        });

        assert_eq!(endpoints.intercepts(), vec![(46821, vec![443])]);
        assert_eq!(endpoints.ports(), vec![40443, 46821]);
    }

    #[test]
    fn the_embedded_broker_does_not_serve_the_handler_public_rules_name() {
        let policy = rules_policy();
        let handler = policy.egress.brokers[0].handler.as_str();

        assert!(!mode_serves_handler(EgressBrokerMode::Embedded, handler));
        assert!(mode_serves_handler(EgressBrokerMode::Local, handler));
        assert!(!mode_serves_handler(EgressBrokerMode::Disabled, handler));
        assert!(mode_serves_handler(
            EgressBrokerMode::Embedded,
            IdentityEchoHandler::NAME
        ));
        assert!(!mode_serves_handler(EgressBrokerMode::Embedded, "tcp"));
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
