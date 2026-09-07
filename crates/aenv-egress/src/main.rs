//! `aenv-egress`: the broker DaemonSet. Accepts the node's connections on a
//! Unix socket both share, serves the `http` handler (and, when asked, the
//! `tcp` and `postgres` ones) and resolves credentials one grant at a time
//! against the endpoint `[resolver].url` names.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use aenv_egress::audit::AuditLevel;
use aenv_egress::credential::{CachingSource, NoCredentials};
use aenv_egress::handlers::echo::IdentityEchoHandler;
use aenv_egress::handlers::http::HttpHandler;
use aenv_egress::handlers::postgres::PostgresHandler;
use aenv_egress::handlers::tcp::TcpRelayHandler;
use aenv_egress::resolver::ResolverSource;
use aenv_egress::runtime::{self, Options, Runtime};
use aenv_egress::tls::{generate_root, CaSigner, SignerOptions};
use aenv_egress::{BrokerDenyList, CredentialSource, Dispatcher, UpstreamGuard};
use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use confique::Config;
use tracing::info;

const CONFIG_PATH_ENV: &str = "AENV_EGRESS_CONFIG_PATH";

#[derive(Parser)]
#[command(name = "aenv-egress", about = "AgentENV sandbox egress broker")]
struct Args {
    /// TOML configuration; defaults to $AENV_EGRESS_CONFIG_PATH or /etc/aenv-egress/config.toml.
    #[arg(long)]
    config: Option<PathBuf>,
    /// Probe the configured socket and exit: 0 when a broker read the probe,
    /// 1 otherwise. The readiness probe of the DaemonSet runs this, because a
    /// listening port says the process is up and not that it is serving.
    #[arg(long)]
    ready: bool,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Mint a throwaway root into DIR, for a deployment with no `egress-ca`
    /// Secret to mount one from. Keeps a root already there.
    GenCa { dir: PathBuf },
}

#[derive(Config)]
struct EgressConfig {
    #[config(nested)]
    listen: ListenConfig,
    /// Prometheus scrape address; unset disables the exporter.
    #[config(env = "AENV_EGRESS_METRICS_LISTEN")]
    metrics_listen: Option<SocketAddr>,
    /// The deadline for the identity frame; nothing on the connection is
    /// admitted until it arrives.
    #[config(default = 10_000u64, env = "AENV_EGRESS_ADMISSION_TIMEOUT_MS")]
    admission_timeout_ms: u64,
    /// Connections held at once; the excess is closed, not queued.
    #[config(default = 4_096u32, env = "AENV_EGRESS_MAX_CONNECTIONS")]
    max_connections: u32,
    /// Connections one sandbox holds at once, inside the limit above.
    #[config(default = 256u32, env = "AENV_EGRESS_PER_SANDBOX_CONNECTIONS")]
    per_sandbox_connections: u32,
    /// How long a shutdown lets live sessions finish before it stops waiting.
    /// Keep it under the pod's termination grace period.
    #[config(default = 25u64, env = "AENV_EGRESS_SHUTDOWN_DRAIN_SECS")]
    shutdown_drain_secs: u64,
    #[config(nested)]
    ca: CaConfig,
    #[config(nested)]
    upstream: UpstreamConfig,
    #[config(nested)]
    resolver: ResolverSourceConfig,
    #[config(nested)]
    handlers: HandlersConfig,
    #[config(nested)]
    audit: AuditConfig,
}

/// What the audit trail records. Node-level, because the broker is.
#[derive(Config)]
struct AuditConfig {
    /// `metadata` (every request and event, values never) or `none`.
    #[config(default = "metadata", env = "AENV_EGRESS_AUDIT_LEVEL")]
    level: String,
}

/// The node-local socket the runtime on this machine connects to, and whose
/// uid the broker requires of every peer.
#[derive(Config)]
struct ListenConfig {
    #[config(
        default = "/run/aenv-egress/broker.sock",
        env = "AENV_EGRESS_SOCKET_PATH"
    )]
    socket_path: PathBuf,
    /// The uid `aenv-node` runs as. It closes a connection from any other
    /// non-root process on this machine; it is not a boundary against root.
    #[config(default = 0u32, env = "AENV_EGRESS_PEER_UID")]
    peer_uid: u32,
}

/// The root that signs leaf certificates for intercepted names: the same
/// pair on every broker in a deployment, and the certificate half of it is
/// what `[egress_broker].guest_ca_cert_path` puts in each guest's trust
/// store.
#[derive(Config)]
struct CaConfig {
    #[config(env = "AENV_EGRESS_CA_CERT_PATH")]
    cert_path: Option<PathBuf>,
    #[config(env = "AENV_EGRESS_CA_KEY_PATH")]
    key_path: Option<PathBuf>,
    #[config(default = 86_400u64)]
    leaf_ttl_secs: u64,
    #[config(default = 4096usize)]
    cache_capacity: usize,
    #[config(default = 60u32)]
    mints_per_sandbox_per_minute: u32,
}

/// Destinations no sandbox reaches through this broker, on top of the
/// built-in private ranges: the cluster's Service and Pod CIDRs.
#[derive(Config)]
struct UpstreamConfig {
    #[config(default = [])]
    denied_cidrs: Vec<String>,
}

/// Where the broker resolves credentials: the api half's own endpoint, or an
/// operator's service serving the same contract. Unset runs the broker
/// without any credential source and every marker answers 502.
#[derive(Config)]
struct ResolverSourceConfig {
    /// Base URL; call paths are joined onto it, so a path prefix is kept.
    #[config(env = "AENV_EGRESS_RESOLVER_URL")]
    url: Option<String>,
    /// Required whenever `url` is set: the endpoint checks a bearer on every
    /// call.
    #[config(env = "AENV_EGRESS_RESOLVER_TOKEN_FILE")]
    token_file: Option<PathBuf>,
    #[config(default = 5_000u64)]
    timeout_ms: u64,
    /// How long a resolved credential is reused before the resolver is asked
    /// again. It bounds how long a revocation takes to bite, and it is the
    /// only bound there is: the api half has no way to reach a broker, so a
    /// revoked grant is noticed on the next lookup and not before.
    #[config(default = 10u64)]
    cache_ttl_secs: u64,
    #[config(default = 4_096usize)]
    cache_capacity: usize,
}

#[derive(Config)]
struct HandlersConfig {
    /// The `echo` identity handler, for smoke tests; off in production.
    #[config(default = false)]
    echo: bool,
    #[config(nested)]
    tcp: RelayHandlerConfig,
    #[config(nested)]
    postgres: RelayHandlerConfig,
}

/// A handler whose upstream comes from the endpoint declaration or the
/// credential rather than from the guest's own connection, so the operator
/// names where it may go. An enabled handler with an empty allowlist reaches
/// nothing.
#[derive(Config)]
struct RelayHandlerConfig {
    #[config(default = false)]
    enabled: bool,
    #[config(default = [])]
    allowed_cidrs: Vec<String>,
}

fn read_secret_file(path: &PathBuf, what: &str) -> Result<Vec<u8>> {
    let bytes =
        std::fs::read(path).with_context(|| format!("read {what} at {}", path.display()))?;
    let trimmed: Vec<u8> = bytes
        .strip_suffix(b"\n")
        .map(<[u8]>::to_vec)
        .unwrap_or(bytes);
    if trimmed.is_empty() {
        bail!("{what} at {} is empty", path.display());
    }
    Ok(trimmed)
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .json()
        .init();
    let args = Args::parse();
    if let Some(Command::GenCa { dir }) = &args.command {
        return gen_ca(dir);
    }
    let config_path = args
        .config
        .or_else(|| std::env::var_os(CONFIG_PATH_ENV).map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from("/etc/aenv-egress/config.toml"));
    let config = EgressConfig::builder()
        .env()
        .file(&config_path)
        .load()
        .with_context(|| format!("load {}", config_path.display()))?;

    if args.ready {
        return probe_readiness(&config.listen.socket_path).await;
    }

    let audit_level = AuditLevel::parse(&config.audit.level).with_context(|| {
        format!(
            "audit.level {:?} is neither \"metadata\" nor \"none\"",
            config.audit.level
        )
    })?;
    aenv_egress::audit::set_level(audit_level);

    if let Some(metrics_listen) = config.metrics_listen {
        metrics_exporter_prometheus::PrometheusBuilder::new()
            .with_http_listener(metrics_listen)
            .install()
            .context("install the prometheus exporter")?;
    }

    let signer_options = SignerOptions {
        leaf_ttl: Duration::from_secs(config.ca.leaf_ttl_secs),
        cache_capacity: config.ca.cache_capacity,
        mints_per_sandbox_per_minute: config.ca.mints_per_sandbox_per_minute,
    };
    let (cert_path, key_path) = signing_pair(&config.ca)?;
    let ca_cert = std::fs::read(cert_path)
        .with_context(|| format!("read ca.cert_path {}", cert_path.display()))?;
    let ca_key = read_secret_file(key_path, "ca key")?;
    let signer = Arc::new(
        CaSigner::from_pem(&ca_cert, &ca_key, signer_options)
            .context("load the leaf signing CA")?,
    );
    info!(
        not_after = ?signer.not_after(),
        "signing leaf certificates with the root the guests trust"
    );

    let creds: Arc<dyn CredentialSource> = match config.resolver.url.as_deref() {
        Some(url) => {
            let Some(token_path) = config.resolver.token_file.as_ref() else {
                bail!(
                    "resolver.url is set but resolver.token_file is not: the resolve endpoint \
                     requires a bearer, and without one every lookup would be refused"
                );
            };
            // Read once here so an unreadable or empty file fails startup,
            // and then again by the source on every call: this may be a
            // projected token that kubelet rotates in place.
            read_secret_file(token_path, "resolver token")?;
            let source = ResolverSource::new(
                url,
                token_path.clone(),
                Duration::from_millis(config.resolver.timeout_ms),
            )
            .context("configure the credential resolver")?;
            Arc::new(
                CachingSource::new(source, Duration::from_secs(config.resolver.cache_ttl_secs))
                    .with_capacity(config.resolver.cache_capacity),
            )
        }
        None => {
            tracing::warn!("resolver.url is not set: every credential lookup will answer 502");
            Arc::new(NoCredentials)
        }
    };

    let mut guard = UpstreamGuard::new(
        BrokerDenyList::with_extra(&config.upstream.denied_cidrs)
            .context("upstream.denied_cidrs are not all cidrs")?,
    );
    if config.handlers.tcp.enabled {
        guard = guard
            .with_allowlist(TcpRelayHandler::NAME, &config.handlers.tcp.allowed_cidrs)
            .context("handlers.tcp.allowed_cidrs are not all cidrs")?;
    }
    if config.handlers.postgres.enabled {
        guard = guard
            .with_allowlist(
                PostgresHandler::NAME,
                &config.handlers.postgres.allowed_cidrs,
            )
            .context("handlers.postgres.allowed_cidrs are not all cidrs")?;
    }
    let mut dispatcher = Dispatcher::new(creds, Arc::new(guard)).with_handler(Arc::new(
        HttpHandler::new(Arc::clone(&signer)).context("build the http handler")?,
    ));
    if config.handlers.tcp.enabled {
        dispatcher = dispatcher.with_handler(Arc::new(TcpRelayHandler));
    }
    if config.handlers.postgres.enabled {
        dispatcher = dispatcher.with_handler(Arc::new(
            PostgresHandler::new().context("build the postgres handler")?,
        ));
    }
    if config.handlers.echo {
        dispatcher = dispatcher.with_handler(Arc::new(IdentityEchoHandler));
    }
    let runtime = Arc::new(Runtime::new(
        Options {
            admission_timeout: Duration::from_millis(config.admission_timeout_ms),
            max_connections: config.max_connections,
            per_sandbox_connections: config.per_sandbox_connections,
            shutdown_drain: Duration::from_secs(config.shutdown_drain_secs),
            expected_peer_uid: Some(config.listen.peer_uid),
        },
        Arc::new(dispatcher),
    ));

    let listener = bind_socket(&config.listen.socket_path)?;
    info!(
        socket_path = %config.listen.socket_path.display(),
        peer_uid = config.listen.peer_uid,
        audit = audit_level.as_str(),
        handlers = ?runtime.dispatcher().handler_names().collect::<Vec<_>>(),
        "aenv-egress listening"
    );
    let shutdown = async {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("install the SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
        info!("shutting down");
    };
    let result = runtime::run(runtime, listener, shutdown).await;
    let _ = std::fs::remove_file(&config.listen.socket_path);
    result
}

const ROOT_CN: &str = "AgentENV Local Egress Root";

fn present(path: &std::path::Path) -> bool {
    std::fs::metadata(path)
        .map(|m| m.len() > 0)
        .unwrap_or(false)
}

fn write_pem(path: &std::path::Path, bytes: &[u8], mode: u32) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    std::fs::write(path, bytes).with_context(|| format!("write {}", path.display()))?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
        .with_context(|| format!("set the mode of {}", path.display()))
}

/// Writes the root guests trust — `ca.crt` and the `ca.key` this broker
/// signs leaves with — into `dir`. A root already there is kept: replacing it
/// would leave every trust store already loaded from it trusting nothing this
/// broker signs. Renewing is "delete both and run this again", followed by a
/// restart of every sandbox holding the old root.
fn gen_ca(dir: &PathBuf) -> Result<()> {
    std::fs::create_dir_all(dir)
        .with_context(|| format!("create the CA directory {}", dir.display()))?;
    let (root_cert, root_key) = (dir.join("ca.crt"), dir.join("ca.key"));

    if present(&root_cert) && present(&root_key) {
        info!(dir = %dir.display(), "a root is already here");
        return Ok(());
    }

    let (cert_pem, key_pem) = generate_root(ROOT_CN).context("mint the root")?;
    write_pem(&root_cert, &cert_pem, 0o644)?;
    write_pem(&root_key, &key_pem, 0o600)?;
    info!(dir = %dir.display(), "minted a root");
    Ok(())
}

/// The pair the broker signs with. Both halves or neither: a broker that came
/// up without a root would answer every matched name with a certificate no
/// guest trusts, and that surfaces as a TLS failure inside the sandbox rather
/// than here.
fn signing_pair(ca: &CaConfig) -> Result<(&PathBuf, &PathBuf)> {
    match (ca.cert_path.as_ref(), ca.key_path.as_ref()) {
        (Some(cert), Some(key)) => Ok((cert, key)),
        _ => bail!(
            "the broker signs leaf certificates with the root the guests trust: set both \
             ca.cert_path and ca.key_path to the `egress-ca` Secret this node mounts"
        ),
    }
}

/// The readiness probe: one empty frame the serving process reads and refuses.
async fn probe_readiness(socket_path: &PathBuf) -> Result<()> {
    aenv_egress::transport::LocalTransport::new(socket_path)
        .with_connect_timeout(Duration::from_secs(2))
        .probe()
        .await
        .map_err(|err| anyhow::anyhow!("{socket_path:?} is not serving: {err}"))
}

/// Binds the node-local socket, replacing a stale one a previous run left
/// behind. The mode lets the node's uid connect and nobody else's.
fn bind_socket(socket_path: &PathBuf) -> Result<tokio::net::UnixListener> {
    if let Some(parent) = socket_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create the socket directory {}", parent.display()))?;
    }
    match std::fs::remove_file(socket_path) {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => {
            return Err(err)
                .with_context(|| format!("remove the stale socket at {}", socket_path.display()))
        }
    }
    let listener = tokio::net::UnixListener::bind(socket_path)
        .with_context(|| format!("bind {}", socket_path.display()))?;
    std::fs::set_permissions(
        socket_path,
        std::os::unix::fs::PermissionsExt::from_mode(0o660),
    )
    .with_context(|| format!("set the mode of {}", socket_path.display()))?;
    Ok(listener)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ca(cert_path: Option<&str>, key_path: Option<&str>) -> CaConfig {
        CaConfig {
            cert_path: cert_path.map(PathBuf::from),
            key_path: key_path.map(PathBuf::from),
            leaf_ttl_secs: 86_400,
            cache_capacity: 4096,
            mints_per_sandbox_per_minute: 60,
        }
    }

    #[test]
    fn a_broker_holding_half_a_signing_pair_refuses_to_start() {
        for half in [
            (None, None),
            (Some("/etc/aenv-egress/ca/ca.crt"), None),
            (None, Some("/etc/aenv-egress/ca/ca.key")),
        ] {
            let refused = signing_pair(&ca(half.0, half.1))
                .expect_err("a broker with no root would serve certificates nothing trusts");
            let said = format!("{refused:#}");
            assert!(
                said.contains("ca.cert_path") && said.contains("ca.key_path"),
                "{said}"
            );
        }
    }

    #[test]
    fn both_halves_are_the_pair_leaves_are_signed_with() {
        let config = ca(Some("/ca/ca.crt"), Some("/ca/ca.key"));

        let (cert, key) = signing_pair(&config).unwrap();

        assert_eq!(cert.as_path(), std::path::Path::new("/ca/ca.crt"));
        assert_eq!(key.as_path(), std::path::Path::new("/ca/ca.key"));
    }

    #[test]
    fn gen_ca_mints_one_root_and_nothing_under_it() {
        let dir = tempfile::tempdir().unwrap();
        let at = dir.path().to_path_buf();

        gen_ca(&at).unwrap();

        let mut written: Vec<String> = std::fs::read_dir(&at)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        written.sort();
        assert_eq!(written, ["ca.crt", "ca.key"]);
    }

    #[test]
    fn gen_ca_keeps_a_root_that_is_already_there() {
        let dir = tempfile::tempdir().unwrap();
        let at = dir.path().to_path_buf();
        gen_ca(&at).unwrap();
        let first = std::fs::read(at.join("ca.crt")).unwrap();

        gen_ca(&at).unwrap();

        assert_eq!(
            std::fs::read(at.join("ca.crt")).unwrap(),
            first,
            "a replaced root leaves every guest already holding the old one trusting nothing"
        );
    }
}
