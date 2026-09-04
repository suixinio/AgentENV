//! `aenv-egress`: the broker deployment. Terminates the runtime's TLS,
//! verifies identity headers, serves the `http` handler (and, when asked,
//! the `tcp` echo handler) and reads credentials from Vault under grants.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use aenv_egress::credential::{CachingSource, NoCredentials};
use aenv_egress::handlers::echo::IdentityEchoHandler;
use aenv_egress::handlers::http::HttpHandler;
use aenv_egress::handlers::tcp::TcpRelayHandler;
use aenv_egress::resolver::ResolverSource;
use aenv_egress::runtime::{self, Options, Runtime};
use aenv_egress::tls::{CaSigner, SignerOptions};
use aenv_egress::vault::VaultSource;
use aenv_egress::{BrokerDenyList, CredentialSource, Dispatcher, UpstreamGuard};
use anyhow::{bail, Context, Result};
use clap::Parser;
use confique::Config;
use tracing::info;

const CONFIG_PATH_ENV: &str = "AENV_EGRESS_CONFIG_PATH";

#[derive(Parser)]
#[command(name = "aenv-egress", about = "AgentENV sandbox egress broker")]
struct Args {
    /// TOML configuration; defaults to $AENV_EGRESS_CONFIG_PATH or /etc/aenv-egress/config.toml.
    #[arg(long)]
    config: Option<PathBuf>,
}

#[derive(Config)]
struct EgressConfig {
    /// Where the runtimes connect.
    #[config(default = "0.0.0.0:8443", env = "AENV_EGRESS_LISTEN")]
    listen: SocketAddr,
    /// Prometheus scrape address; unset disables the exporter.
    #[config(env = "AENV_EGRESS_METRICS_LISTEN")]
    metrics_listen: Option<SocketAddr>,
    #[config(default = 30_000u64, env = "AENV_EGRESS_MAX_SKEW_MS")]
    max_skew_ms: u64,
    #[config(default = 100_000usize, env = "AENV_EGRESS_REPLAY_CAPACITY")]
    replay_capacity: usize,
    /// One deadline for the TLS handshake and the identity frame behind it;
    /// nothing on the connection is authenticated until both are done.
    #[config(default = 10_000u64, env = "AENV_EGRESS_ADMISSION_TIMEOUT_MS")]
    admission_timeout_ms: u64,
    /// Connections held at once; the excess is closed, not queued.
    #[config(default = 4_096u32, env = "AENV_EGRESS_MAX_CONNECTIONS")]
    max_connections: u32,
    /// How long a shutdown lets live sessions finish before it stops waiting.
    /// Keep it under the pod's termination grace period.
    #[config(default = 25u64, env = "AENV_EGRESS_SHUTDOWN_DRAIN_SECS")]
    shutdown_drain_secs: u64,
    #[config(nested)]
    tls: TlsConfig,
    #[config(nested)]
    hmac: HmacConfig,
    #[config(nested)]
    ca: CaConfig,
    #[config(nested)]
    upstream: UpstreamConfig,
    #[config(nested)]
    vault: VaultSourceConfig,
    #[config(nested)]
    resolver: ResolverSourceConfig,
    #[config(nested)]
    handlers: HandlersConfig,
}

/// The server certificate the runtimes verify, signed by the cluster CA.
#[derive(Config)]
struct TlsConfig {
    #[config(env = "AENV_EGRESS_TLS_CERT_PATH")]
    cert_path: PathBuf,
    /// PKCS#8 PEM.
    #[config(env = "AENV_EGRESS_TLS_KEY_PATH")]
    key_path: PathBuf,
}

/// Files holding the shared secrets runtimes sign identity headers with.
/// Several files carry a rotation; any of them verifies.
#[derive(Config)]
struct HmacConfig {
    #[config(default = [])]
    key_files: Vec<PathBuf>,
}

/// The CA that signs leaf certificates for intercepted names.
#[derive(Config)]
struct CaConfig {
    #[config(env = "AENV_EGRESS_CA_CERT_PATH")]
    cert_path: PathBuf,
    #[config(env = "AENV_EGRESS_CA_KEY_PATH")]
    key_path: PathBuf,
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

#[derive(Config)]
struct VaultSourceConfig {
    /// Unset runs the broker without any credential source: every marker
    /// resolves to 502.
    #[config(env = "AENV_EGRESS_VAULT_ADDR")]
    addr: Option<String>,
    #[config(env = "AENV_EGRESS_VAULT_TOKEN_FILE")]
    token_file: Option<PathBuf>,
    #[config(default = "aenv", env = "AENV_EGRESS_VAULT_MOUNT")]
    mount: String,
    #[config(env = "AENV_EGRESS_VAULT_NAMESPACE")]
    namespace: Option<String>,
    #[config(default = 5_000u64)]
    timeout_ms: u64,
    /// How long a value is reused before Vault is asked again.
    #[config(default = 30u64)]
    cache_ttl_secs: u64,
    /// Values held at once; expired entries leave on every insert and the
    /// soonest to expire is dropped at capacity.
    #[config(default = 4_096usize)]
    cache_capacity: usize,
}

/// The operator-run resolver that owns the credentials themselves. Set at
/// most one of this and `[vault]`; with both, the resolver wins and the Vault
/// section is ignored.
#[derive(Config)]
struct ResolverSourceConfig {
    /// Base URL; call paths are joined onto it, so a path prefix is kept.
    #[config(env = "AENV_EGRESS_RESOLVER_URL")]
    url: Option<String>,
    #[config(env = "AENV_EGRESS_RESOLVER_TOKEN_FILE")]
    token_file: Option<PathBuf>,
    #[config(default = 5_000u64)]
    timeout_ms: u64,
    /// How long a resolved credential is reused before the resolver is asked
    /// again. It bounds how long a revocation takes to bite.
    #[config(default = 30u64)]
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
    http: PinnedHandlerConfig,
}

/// A handler whose upstream comes from the endpoint declaration, so the
/// operator names where it may go. An enabled handler with an empty
/// allowlist reaches nothing.
#[derive(Config)]
struct RelayHandlerConfig {
    #[config(default = false)]
    enabled: bool,
    #[config(default = [])]
    allowed_cidrs: Vec<String>,
}

/// A handler the sandbox's own policy already bounds. An empty allowlist
/// leaves it bounded by that policy alone; a non-empty one pins it further.
#[derive(Config)]
struct PinnedHandlerConfig {
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
    let config_path = args
        .config
        .or_else(|| std::env::var_os(CONFIG_PATH_ENV).map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from("/etc/aenv-egress/config.toml"));
    let config = EgressConfig::builder()
        .env()
        .file(&config_path)
        .load()
        .with_context(|| format!("load {}", config_path.display()))?;

    if let Some(metrics_listen) = config.metrics_listen {
        metrics_exporter_prometheus::PrometheusBuilder::new()
            .with_http_listener(metrics_listen)
            .install()
            .context("install the prometheus exporter")?;
    }

    let keys: Vec<Vec<u8>> = config
        .hmac
        .key_files
        .iter()
        .map(|path| read_secret_file(path, "hmac key"))
        .collect::<Result<_>>()?;
    if keys.is_empty() {
        bail!("hmac.key_files names no key; every identity header would be refused");
    }

    let ca_cert = std::fs::read(&config.ca.cert_path)
        .with_context(|| format!("read ca.cert_path {}", config.ca.cert_path.display()))?;
    let ca_key = read_secret_file(&config.ca.key_path, "ca key")?;
    let signer = Arc::new(
        CaSigner::from_pem(
            &ca_cert,
            &ca_key,
            SignerOptions {
                leaf_ttl: Duration::from_secs(config.ca.leaf_ttl_secs),
                cache_capacity: config.ca.cache_capacity,
                mints_per_sandbox_per_minute: config.ca.mints_per_sandbox_per_minute,
            },
        )
        .context("load the leaf signing CA")?,
    );

    let server_cert = std::fs::read(&config.tls.cert_path)
        .with_context(|| format!("read tls.cert_path {}", config.tls.cert_path.display()))?;
    let server_key = read_secret_file(&config.tls.key_path, "tls key")?;
    let identity = native_tls::Identity::from_pkcs8(&server_cert, &server_key)
        .context("tls.cert_path and tls.key_path do not form a PKCS#8 identity")?;
    let acceptor = tokio_native_tls::TlsAcceptor::from(
        native_tls::TlsAcceptor::builder(identity)
            .min_protocol_version(Some(native_tls::Protocol::Tlsv12))
            .build()
            .context("build the server tls acceptor")?,
    );

    let creds: Arc<dyn CredentialSource> = match (
        config.resolver.url.as_deref(),
        config.vault.addr.as_deref(),
    ) {
        (Some(url), vault) => {
            if vault.is_some() {
                tracing::warn!(
                    "both resolver.url and vault.addr are set; the resolver is used and vault is ignored"
                );
            }
            let token = match config.resolver.token_file.as_ref() {
                Some(path) => Some(String::from_utf8(read_secret_file(
                    path,
                    "resolver token",
                )?)?),
                None => None,
            };
            let source = ResolverSource::new(
                url,
                token.as_deref(),
                Duration::from_millis(config.resolver.timeout_ms),
            )
            .context("configure the external resolver credential source")?;
            Arc::new(
                CachingSource::new(source, Duration::from_secs(config.resolver.cache_ttl_secs))
                    .with_capacity(config.resolver.cache_capacity),
            )
        }
        (None, vault_addr) => match vault_addr {
            Some(addr) => {
                let token_file = config
                    .vault
                    .token_file
                    .as_ref()
                    .context("vault.addr is set but vault.token_file is not")?;
                let token = read_secret_file(token_file, "vault token")?;
                let source = VaultSource::new(
                    addr,
                    std::str::from_utf8(&token).context("vault token is not utf-8")?,
                    &config.vault.mount,
                    config.vault.namespace.clone(),
                    Duration::from_millis(config.vault.timeout_ms),
                )
                .context("configure the Vault credential source")?;
                Arc::new(
                    CachingSource::new(source, Duration::from_secs(config.vault.cache_ttl_secs))
                        .with_capacity(config.vault.cache_capacity),
                )
            }
            None => {
                tracing::warn!(
                "neither resolver.url nor vault.addr is set: every credential lookup will answer 502"
            );
                Arc::new(NoCredentials)
            }
        },
    };

    let mut guard = UpstreamGuard::new(
        BrokerDenyList::with_extra(&config.upstream.denied_cidrs)
            .context("upstream.denied_cidrs are not all cidrs")?,
    );
    if !config.handlers.http.allowed_cidrs.is_empty() {
        guard = guard
            .with_allowlist(HttpHandler::NAME, &config.handlers.http.allowed_cidrs)
            .context("handlers.http.allowed_cidrs are not all cidrs")?;
    }
    if config.handlers.tcp.enabled {
        guard = guard
            .with_allowlist(TcpRelayHandler::NAME, &config.handlers.tcp.allowed_cidrs)
            .context("handlers.tcp.allowed_cidrs are not all cidrs")?;
    }
    let mut dispatcher = Dispatcher::new(creds, Arc::new(guard)).with_handler(Arc::new(
        HttpHandler::new(Arc::clone(&signer)).context("build the http handler")?,
    ));
    if config.handlers.tcp.enabled {
        dispatcher = dispatcher.with_handler(Arc::new(TcpRelayHandler));
    }
    if config.handlers.echo {
        dispatcher = dispatcher.with_handler(Arc::new(IdentityEchoHandler));
    }
    let runtime = Arc::new(Runtime::new(
        Options {
            admission_timeout: Duration::from_millis(config.admission_timeout_ms),
            max_connections: config.max_connections,
            shutdown_drain: Duration::from_secs(config.shutdown_drain_secs),
            ..Options::new(
                keys,
                Duration::from_millis(config.max_skew_ms),
                config.replay_capacity,
            )
        },
        Arc::new(dispatcher),
    ));

    let listener = tokio::net::TcpListener::bind(config.listen)
        .await
        .with_context(|| format!("bind {}", config.listen))?;
    info!(
        listen = %config.listen,
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
    runtime::run(runtime, listener, acceptor, shutdown).await
}
