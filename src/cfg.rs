use std::path::{Path, PathBuf};
use std::sync::OnceLock;

pub mod egress_broker;
pub mod image;
pub mod network;
use anyhow::{anyhow, bail, Context, Result};
use confique::Config;
pub use egress_broker::{
    EgressBrokerConfig, EgressBrokerMode, SecretsBackendKind, SecretsConfig, SecretsPgConfig,
};
pub use image::{
    ImageCacheConfig, ImageConfig, ImageRemoteBlocksCacheConfig, ImageResolverConfig,
    ResolvedImageCacheConfig, ResolvedImageCacheGcConfig,
};
pub use network::{NetworkConfig, NetworkEgressConfig, NetworkInternalConfig};
use serde::Deserialize;
use tracing::warn;

use crate::virtualization::VirtualizationMode;

const ENV_CONFIG_PATH: &str = "AENV_CONFIG_PATH";

/// Additional TOML overlays, applied in order after [`ENV_CONFIG_PATH`].
const ENV_CONFIG_OVERLAY_PATH: &str = "AENV_CONFIG_OVERLAY_PATH";

/// Separates the entries of [`ENV_CONFIG_OVERLAY_PATH`], `PATH`-style.
const OVERLAY_PATH_SEPARATOR: char = ':';

#[cfg(test)]
const TEST_ACCESS_TOKEN_HASH_SEED: &str = "agentenv-unit-test-access-token-seed";

#[derive(Debug, Deserialize)]
struct SetupDependencyManifest {
    firecracker: ManifestVirtualizationDownloads,
    kernel: ManifestVirtualizationDownloads,
    tools: ManifestTools,
    overlaybd: ManifestDownload,
    #[serde(rename = "regclient")]
    regclient: ManifestDownload,
}

#[derive(Debug, Deserialize)]
struct ManifestDownload {
    version: String,
}

#[derive(Debug, Deserialize)]
struct ManifestVirtualizationDownloads {
    kvm: ManifestDownload,
    pvm: ManifestDownload,
}

impl ManifestVirtualizationDownloads {
    fn for_mode(&self, mode: VirtualizationMode) -> &ManifestDownload {
        match mode {
            VirtualizationMode::Kvm => &self.kvm,
            VirtualizationMode::Pvm => &self.pvm,
        }
    }
}

#[derive(Debug, Deserialize)]
struct ManifestTools {
    version: String,
}

impl SetupDependencyManifest {
    fn get() -> &'static Self {
        use std::sync::LazyLock;

        static MANIFEST: LazyLock<SetupDependencyManifest> = LazyLock::new(|| {
            toml::from_str(include_str!("../config/deps_manifest.toml"))
                .expect("bundled setup dependency manifest is valid")
        });
        &MANIFEST
    }
}

pub fn regctl_path(deps_path: &Path) -> PathBuf {
    deps_path
        .join("regctl")
        .join(&SetupDependencyManifest::get().regclient.version)
        .join("regctl")
}

/// Version of the Firecracker build the bundled manifest pins for `mode`.
pub fn manifest_firecracker_version(mode: VirtualizationMode) -> &'static str {
    &SetupDependencyManifest::get()
        .firecracker
        .for_mode(mode)
        .version
}

/// Version of the guest kernel the bundled manifest pins for `mode`.
pub fn manifest_kernel_version(mode: VirtualizationMode) -> &'static str {
    &SetupDependencyManifest::get().kernel.for_mode(mode).version
}

/// Version of the tools drive the bundled manifest pins.
pub fn manifest_tools_version() -> &'static str {
    &SetupDependencyManifest::get().tools.version
}

/// Version of the overlaybd toolchain the bundled manifest pins.
pub fn manifest_overlaybd_version() -> &'static str {
    &SetupDependencyManifest::get().overlaybd.version
}

#[derive(Debug, Clone, Config)]
pub struct AppConfig {
    /// Optional PostgreSQL settings loaded only from files.
    ///
    /// Keep this first so the source guard correctly detects unreachable env bindings.
    pub pg: Option<PgConfig>,
    #[config(
        env = "AENV_HOME_PATH",
        parse_env = parse_required_path,
        default = "/var/lib/aenv"
    )]
    pub home_path: PathBuf,
    #[config(
        env = "AENV_RUNTIME_PATH",
        parse_env = parse_required_path,
        default = "/run/aenv"
    )]
    pub runtime_path: PathBuf,
    #[config(
        env = "AENV_DEPS_PATH",
        parse_env = parse_required_path,
        default = "$AENV_HOME/deps"
    )]
    pub deps_path: PathBuf,
    #[config(default = "kvm", env = "AENV_VIRTUALIZATION_MODE")]
    pub virtualization_mode: VirtualizationMode,
    #[config(nested)]
    pub firecracker: FirecrackerConfig,
    #[config(nested)]
    pub kernel: KernelConfig,
    #[config(nested)]
    pub tools: ToolsConfig,
    pub overlaybd: Option<OverlaybdDependencyConfig>,
    #[config(nested)]
    pub machine: MachineConfig,
    #[config(nested)]
    pub backend: BackendConfig,
    #[config(nested)]
    pub envd: EnvdConfig,
    #[config(nested)]
    pub sandbox: SandboxConfig,
    #[config(nested)]
    pub orchestrator: OrchestratorConfig,
    #[config(nested)]
    pub snapshot: SnapshotConfig,
    #[config(nested)]
    pub ublk: UblkTomlConfig,
    #[config(nested)]
    pub observability: ObservabilityConfig,
    #[config(nested)]
    pub cluster: ClusterConfig,
    #[config(nested)]
    pub node_identity: NodeIdentityConfig,
    #[config(nested)]
    pub memory_snapshot: MemorySnapshotConfig,
    #[config(nested)]
    pub pool: PoolTomlConfig,
    #[config(nested)]
    pub p2p: P2pConfig,
    #[config(nested)]
    pub image: ImageConfig,
    #[config(nested)]
    pub sandbox_proxy: SandboxProxyConfig,
    #[config(nested)]
    pub network: NetworkConfig,
    #[config(nested)]
    pub custom_extension: CustomExtensionConfig,
    /// How this node reaches the egress broker for sandboxes with `rules`.
    #[config(nested)]
    pub egress_broker: EgressBrokerConfig,
    /// The api half's secrets store; a node leaves it disabled.
    #[config(nested)]
    pub secrets: SecretsConfig,
    #[config(nested)]
    pub api: ApiConfig,
    /// Routing/binding store configuration with an independent key space.
    #[config(nested)]
    pub binding_store: BindingStoreConfig,
}

/// The node's own HTTP API.
#[derive(Debug, Config, Clone)]
pub struct ApiConfig {
    /// Static gateway credentials accepted together during rotation.
    ///
    /// Empty together with the token file disables the gate.
    #[config(
        default = [],
        env = "AENV_API_CONTROL_PLANE_TOKEN",
        parse_env = confique::env::parse::list_by_comma
    )]
    pub control_plane_tokens: Vec<String>,
    /// Reloadable gateway credentials, one per line.
    ///
    /// The effective credential set is the union with static tokens.
    #[config(
        default = "",
        env = "AENV_API_CONTROL_PLANE_TOKEN_FILE",
        parse_env = parse_trimmed_string
    )]
    pub control_plane_token_file: String,
    /// The credential this half presents at another node's gate, when it is
    /// not one of the credentials it accepts itself.
    ///
    /// Empty falls back to the first line of `control_plane_token_file`, and
    /// to a static token only when there is no file: a rotation moves the
    /// file, while the static list keeps the retired credential so nodes
    /// still accept what is in flight.
    #[config(
        default = "",
        env = "AENV_API_NODE_CLIENT_TOKEN_FILE",
        parse_env = parse_trimmed_string
    )]
    pub node_client_token_file: String,
    #[config(nested)]
    pub proxy: ApiProxyConfig,
}

/// The data-plane reverse proxy, on the half that serves one.
#[derive(Debug, Config, Clone)]
pub struct ApiProxyConfig {
    /// Requests one sandbox may have in flight through the proxy at once.
    /// `0` does not limit: a sandbox's own service decides what it can take.
    #[config(default = 0u32, env = "AENV_API_PROXY_MAX_INCOMING_PER_SANDBOX")]
    pub max_incoming_per_sandbox: u32,
}

#[derive(Debug, Deserialize, Clone, Config)]
pub struct BackendConfig {
    pub posix_fs: Option<PosixFsBackendConfig>,
    pub oss: Option<OssBackendConfig>,
}

#[derive(Debug, Deserialize, Clone, Config)]
pub struct PosixFsBackendConfig {
    /// Root directory of the `posix_fs` snapshot repository.
    ///
    /// This optional nested config is file-only; use [`ENV_CONFIG_OVERLAY_PATH`].
    #[config(default = "$AENV_HOME/snapshot-store")]
    pub snapshot_store: PathBuf,
}

#[derive(Debug, Clone, Config)]
pub struct FirecrackerConfig {
    pub binary_path: Option<PathBuf>,
    pub boot_args: Option<String>,
    pub allowed_extra_boot_args_prefixes: Option<Vec<String>>,
    #[config(default = 3u64)]
    pub socket_timeout_secs: u64,
    #[config(default = 1u64)]
    pub socket_poll_ms: u64,
    pub version: Option<String>,
    pub url: Option<String>,
    /// Optional override for the per-sandbox Firecracker work root.
    /// Defaults to `$AENV_HOME/firecracker-work` after normalization.
    #[config(env = "AENV_FIRECRACKER_WORK_DIR", parse_env = parse_required_path)]
    pub work_dir: Option<PathBuf>,
    /// Optional override for the persistent Firecracker serial output directory.
    /// Defaults to `$AENV_HOME/logs/serial` after normalization.
    /// Files are grouped under `{serial_dir}/{sandbox_id}/`.
    #[config(env = "AENV_FIRECRACKER_SERIAL_DIR", parse_env = parse_required_path)]
    pub serial_dir: Option<PathBuf>,
    /// Optional Firecracker log level (e.g. "Error", "Warning", "Info", "Debug", "Trace").
    /// When set (non-empty), Firecracker logging is enabled and written to a
    /// `firecracker.log` file in the same directory as the Firecracker stdout log.
    pub log_level: Option<String>,
    /// Applies the cluster CPUID intersection to cold boots.
    ///
    /// Disable only on homogeneous hosts that reject the generated template.
    #[config(default = true, env = "AENV_FIRECRACKER_APPLY_CLUSTER_CPU_TEMPLATE")]
    pub apply_cluster_cpu_template: bool,
}

#[derive(Debug, Config, Clone)]
pub struct PoolTomlConfig {
    #[config(default = 2usize)]
    pub low_watermark: usize,
    #[config(default = 64usize)]
    pub high_watermark: usize,
    #[config(nested)]
    pub network: PoolComponentConfig,
    #[config(nested)]
    pub block: PoolComponentConfig,
    #[config(nested)]
    pub firecracker: FirecrackerProcessPoolConfig,
}

#[derive(Debug, Config, Clone)]
pub struct PoolComponentConfig {
    #[config(default = true)]
    pub enabled: bool,
    #[config(default = true)]
    pub maintenance_enabled: bool,
    #[config(default = true)]
    pub startup_prewarm: bool,
}

#[derive(Debug, Config, Clone)]
pub struct FirecrackerProcessPoolConfig {
    #[config(default = true)]
    pub enabled: bool,
    #[config(default = true)]
    pub maintenance_enabled: bool,
    #[config(default = true)]
    pub startup_prewarm: bool,
    #[config(default = 4usize)]
    pub fill_concurrency: usize,
}

#[derive(Debug, Clone, Config)]
pub struct KernelConfig {
    pub image_path: Option<PathBuf>,
    pub version: Option<String>,
    pub url: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct OverlaybdDependencyConfig {
    pub version: String,
    pub url: Option<String>,
    pub package_url: Option<String>,
}

#[derive(Debug, Clone, Config)]
pub struct ToolsConfig {
    /// Explicit source path for importing a local tools drive ext4 file.
    pub drive_path: Option<PathBuf>,
    /// Immutable tools drive release version (e.g. "0.1.0").
    pub version: Option<String>,
    /// Container registry URL template (e.g. "ghcr.io/org/agentenv-tools:{version}").
    pub url: Option<String>,
    /// Control plane port inside the VM (default: 49983).
    #[config(default = 49983u16)]
    pub control_plane_port: u16,
}

#[derive(Debug, Config, Clone)]
pub struct SandboxProxyConfig {
    #[config(
        default = [],
        env = "AENV_SANDBOX_PROXY_DOMAINS",
        parse_env = confique::env::parse::list_by_comma
    )]
    pub domains: Vec<String>,
}

#[derive(Debug, Config, Clone)]
pub struct EnvdConfig {
    #[config(default = "0.6.13")]
    pub version: String,
    #[config(default = 60u64)]
    pub init_timeout_secs: u64,
    #[config(default = 3u64)]
    pub poll_ms: u64,
}

#[derive(Clone, Config)]
pub struct SandboxConfig {
    #[config(
        env = "AENV_SANDBOX_ACCESS_TOKEN_HASH_SEED",
        parse_env = parse_trimmed_string
    )]
    pub access_token_hash_seed: Option<String>,
}

impl std::fmt::Debug for SandboxConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SandboxConfig")
            .field(
                "access_token_hash_seed",
                &self.access_token_hash_seed.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

#[derive(Debug, Config, Clone)]
pub struct MachineConfig {
    #[config(default = 2u32)]
    pub vcpu_count: u32,
    #[config(default = 1024u32)]
    pub mem_size_mib: u32,
    #[config(nested)]
    pub disk_rate_limit: DiskRateLimitConfig,
}

#[derive(Debug, Config, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct DiskRateLimitConfig {
    /// Enable per-sandbox disk I/O rate limiting via Firecracker's virtio-blk rate limiter.
    #[config(default = false)]
    pub enabled: bool,
    /// Sustained disk bandwidth limit in bytes per second (0 = unlimited).
    #[config(default = 0u64)]
    pub bandwidth_bytes_per_sec: u64,
    /// One-time bandwidth burst in bytes, granted once when the VM starts (maps
    /// to Firecracker's `one_time_burst`). It is a separate allowance consumed
    /// before the sustained bucket and is not replenished after use, so it only
    /// absorbs the initial I/O spike; it does not raise the steady-state rate.
    #[config(default = 0u64)]
    pub bandwidth_burst_bytes: u64,
    /// Sustained IOPS limit (0 = unlimited).
    #[config(default = 0u64)]
    pub iops: u64,
    /// One-time IOPS burst, granted once when the VM starts (maps to
    /// Firecracker's `one_time_burst`). Consumed before the sustained bucket and
    /// not replenished, so it only absorbs the initial spike, not steady state.
    #[config(default = 0u64)]
    pub iops_burst: u64,
}

#[derive(Debug, Config, Clone)]
pub struct SnapshotConfig {
    #[config(
        env = "AENV_SNAPSHOT_LOCAL_CACHE_PATH",
        parse_env = parse_required_path,
        default = "$AENV_HOME/snapshot-local-cache"
    )]
    pub local_cache_path: PathBuf,
    /// Snapshot repository backend selected independently of file overlays.
    ///
    /// Selecting `oss` still requires file-based `[backend.oss]` settings.
    #[config(default = "posix_fs", env = "AENV_SNAPSHOT_REPOSITORY_BACKEND")]
    pub repository_backend: SnapshotRepositoryBackendKind,
    /// When true, snapshot artifacts are published to and fetched from the P2P
    /// transport after each commit. Has no effect unless `[p2p].enabled` is also true.
    #[config(default = true)]
    pub p2p_enabled: bool,
    #[config(nested)]
    pub image_publish: SnapshotImagePublishConfig,
    #[config(nested)]
    pub catalog: SnapshotCatalogConfig,
}

/// Snapshot catalog tuning.
#[derive(Debug, Config, Clone)]
pub struct SnapshotCatalogConfig {
    /// Cluster-wide concurrent-build ceiling: zero uses the default, negative
    /// disables the ceiling.
    #[config(default = 20i32, env = "AENV_SNAPSHOT_CATALOG_MAX_CONCURRENT_BUILDS")]
    pub max_concurrent_builds: i32,
    /// Build heartbeat cadence; the reaper TTL is three times this interval.
    #[config(default = 100u64)]
    pub build_heartbeat_interval_secs: u64,
}

#[derive(Debug, Config, Clone)]
pub struct SnapshotImagePublishConfig {
    #[config(default = false)]
    pub enabled: bool,
}

/// File-only shared PostgreSQL settings.
///
/// Fields here must not declare unreachable environment bindings.
#[derive(Debug, Deserialize, Clone, Default)]
pub struct PgConfig {
    /// Libpq connection URL; absent or blank disables PostgreSQL.
    pub dsn: Option<String>,
    /// Per-replica pool cap. Defaults to 8 when unset — see
    /// `src/pg::pool::DEFAULT_MAX_CONNECTIONS` for why that number, and for
    /// the reminder that `aenv-api` runs more than one replica: the
    /// cluster-wide connection count this deployment produces is
    /// `replica_count * max_connections`, not this number alone, and has to
    /// stay under PostgreSQL's own `max_connections`.
    pub max_connections: Option<u32>,
    /// Bounds the pool's initial connection attempt and every later acquire.
    /// Defaults to 5 seconds when unset.
    pub connect_timeout_secs: Option<u64>,
}

impl PgConfig {
    /// Returns the trimmed DSN, treating blank as absent.
    pub fn dsn(&self) -> Option<&str> {
        self.dsn
            .as_deref()
            .map(str::trim)
            .filter(|dsn| !dsn.is_empty())
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct OssBackendConfig {
    pub endpoint: String,
    pub bucket: String,
    pub prefix: Option<String>,
    #[serde(alias = "credentialProcess", alias = "credential-process")]
    pub credential_process: Option<String>,
    pub access_key_id: Option<String>,
    pub access_key_secret: Option<String>,
    pub security_token: Option<String>,
    pub region: Option<String>,
    pub cache_max_size_gb: Option<u64>,
}

#[derive(Debug, Deserialize, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotImageStoragePolicy {
    #[default]
    ObjectStorage,
    SourceRegistry,
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotRepositoryBackendKind {
    PosixFs,
    Oss,
}

#[derive(Debug, Config, Clone)]
pub struct UblkTomlConfig {
    /// Path to the `uvm-ublk-daemon` binary.
    #[config(env = "AENV_UBLK_DAEMON_BINARY_PATH", parse_env = parse_required_path)]
    pub daemon_binary_path: Option<PathBuf>,
    /// Unix socket path for the ublk daemon.
    #[config(default = "$AENV_RUNTIME/ublk-daemon.sock")]
    pub daemon_socket_path: PathBuf,
    /// Optional override for the ublk daemon log file.
    /// Defaults to `$AENV_HOME/logs/ublk-daemon.log` after normalization.
    pub daemon_log_path: Option<PathBuf>,
    /// HTTP listen address for ublk daemon metrics. Empty string disables it.
    #[config(env = "AENV_UBLK_DAEMON_METRICS_LISTEN_ADDR", parse_env = parse_trimmed_string, default = "0.0.0.0:9103")]
    pub daemon_metrics_listen_addr: String,
    #[config(nested)]
    pub overlaybd: UblkOverlaybdTomlConfig,
}

/// Writable OverlayBD upper format, converted to the storage type at the boundary.
#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum RuntimeUpperMode {
    Sparse,
    LogStructured,
    HybridLogStructured,
}

#[derive(Debug, Config, Clone)]
pub struct UblkOverlaybdTomlConfig {
    #[config(default = "$AENV_HOME/overlaybd/overlaybd-global.json")]
    pub global_config_path: PathBuf,
    #[config(default = false)]
    pub read_only: bool,
    /// Runtime upper format for newly materialized writable OverlayBD images.
    /// Existing source uppers keep their own mode. Default: `hybridLogStructured`.
    #[config(default = "hybridLogStructured")]
    pub runtime_upper_mode: RuntimeUpperMode,
    /// Permit shrinking a fresh cold-sandbox rootfs. Default: `false`.
    #[config(default = false)]
    pub allow_shrink: bool,
    /// Timeout for the OverlayBD resize tool. Default: `120`.
    #[config(default = 120u64)]
    pub resize_timeout_secs: u64,
    /// Enable overlaybd layer-level background download. Default: `false`.
    #[config(default = false)]
    pub download_enable: bool,
    /// Timeout for overlaybd P2P provider lookup requests. Default: `300`.
    #[config(default = 300u64)]
    pub p2p_lookup_timeout_ms: u64,
    /// Timeout for overlaybd P2P foreground range fetches. Default: `2000`.
    #[config(default = 2000u64)]
    pub p2p_fetch_range_timeout_ms: u64,
}

#[derive(Debug, Deserialize, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MemorySnapshotCompressionAlgorithm {
    #[default]
    Lz4,
    Zstd,
}

#[derive(Debug, Config, Clone)]
pub struct MemorySnapshotConfig {
    #[config(default = "$AENV_HOME/overlaybd/mem-overlaybd-global.json")]
    pub overlaybd_global_config_path: PathBuf,
    /// Enable Firecracker KVM dirty-page tracking for memory snapshots.
    /// Default: false, preserving the mincore-based path.
    #[config(env = "AGENTENV_MEMORY_SNAPSHOT_TRACK_DIRTY_PAGES", default = false)]
    pub track_dirty_pages: bool,
    #[config(default = false)]
    pub compression_enabled: bool,
    #[config(default = "lz4")]
    pub compression_algorithm: MemorySnapshotCompressionAlgorithm,
    /// Number of blocking threads used to compress 4KiB blocks within a
    /// memory layer. 1 = sequential (identical output layout at any value).
    #[config(default = 1)]
    pub compression_workers: usize,
    #[config(nested)]
    pub background_download: MemorySnapshotBackgroundDownloadConfig,
}

#[derive(Debug, Config, Clone)]
pub struct MemorySnapshotBackgroundDownloadConfig {
    #[config(default = true)]
    pub enable: bool,
    #[config(default = 0i32)]
    pub delay: i32,
    #[config(default = 1i32)]
    pub delay_extra: i32,
    #[config(default = 5i32)]
    pub try_cnt: i32,
    #[config(default = 16_777_216u32)]
    pub block_size: u32,
    #[config(default = 4usize)]
    pub concurrency: usize,
    /// Cap on in-flight background download blocks enforced by the cache
    /// backend's download queue, shared by every layer download on the node
    /// (bounds total scratch memory). Fixed when the backend is created;
    /// per-image overrides never resize it.
    #[config(default = 16usize)]
    pub max_inflight_blocks: usize,
}

#[derive(Debug, Config, Clone)]
pub struct ObservabilityConfig {
    /// Enables the node/admin observability service exposed by the API layer.
    #[config(default = true)]
    pub enabled: bool,
    #[config(nested)]
    pub scheduler_report: ObservabilitySchedulerReportConfig,
}

#[derive(Debug, Config, Clone)]
pub struct ObservabilitySchedulerReportConfig {
    /// Enables scheduler heartbeat reporting. The scheduler endpoint is read
    /// from `[cluster].scheduler_endpoint`.
    #[config(default = false, env = "AENV_OBSERVABILITY_SCHEDULER_REPORT_ENABLED")]
    pub enabled: bool,
    #[config(default = 5u64, env = "AENV_OBSERVABILITY_REPORT_INTERVAL_SECS")]
    pub interval_secs: u64,
}

/// Backend for sharing heartbeat-derived node observations across API replicas.
///
/// This remains distinct from other shared-state subsystem backends.
#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NodeRegistryObservedBackendKind {
    /// Replica-local observations; rejected by multi-replica API assembly.
    InMemory,
    /// Shared Redis observations visible to every API replica.
    Redis,
}

/// Shared node-registry store configuration.
#[derive(Debug, Config, Clone)]
pub struct ClusterNodeRegistryStoreConfig {
    /// See [`NodeRegistryObservedBackendKind`].
    #[config(
        default = "in_memory",
        env = "AENV_CLUSTER_NODE_REGISTRY_STORE_BACKEND"
    )]
    pub backend: NodeRegistryObservedBackendKind,
    /// `redis://host:port[/db]`, read only when `backend = "redis"`.
    #[config(
        default = "redis://127.0.0.1:6379",
        env = "AENV_CLUSTER_NODE_REGISTRY_STORE_REDIS_URL",
        parse_env = parse_trimmed_string
    )]
    pub redis_url: String,
    /// Independent Redis namespace for node observations.
    #[config(
        default = "agentenv:node-registry",
        env = "AENV_CLUSTER_NODE_REGISTRY_STORE_REDIS_KEY_PREFIX",
        parse_env = parse_trimmed_string
    )]
    pub redis_key_prefix: String,
    /// Redis command response timeout.
    #[config(
        default = 2000u64,
        env = "AENV_CLUSTER_NODE_REGISTRY_STORE_REDIS_RESPONSE_TIMEOUT_MS"
    )]
    pub redis_response_timeout_ms: u64,
    /// How long a fresh TCP connection attempt (initial connect, or a
    /// reconnect after a `redis_response_timeout_ms`) may take.
    #[config(
        default = 3000u64,
        env = "AENV_CLUSTER_NODE_REGISTRY_STORE_REDIS_CONNECT_TIMEOUT_MS"
    )]
    pub redis_connect_timeout_ms: u64,
}

/// Node discovery strategy.
///
/// Kubernetes is the default because this process has no usable static fallback list.
#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum ClusterNodeDiscoveryMode {
    #[default]
    Kubernetes,
    Static,
}

/// Static discovery entry with the same id/endpoint shape as the scheduler.
#[derive(Debug, Deserialize, Clone, Default, PartialEq, Eq)]
pub struct ClusterStaticDiscoveryNode {
    pub id: String,
    pub endpoint: String,
}

#[derive(Debug, Config, Clone)]
pub struct ClusterConfig {
    /// Shared gRPC scheduler endpoint for cluster-level services.
    #[config(
        env = "AENV_OBSERVABILITY_SCHEDULER_ENDPOINT",
        parse_env = parse_trimmed_string
    )]
    pub scheduler_endpoint: Option<String>,
    /// Reloadable scheduler endpoint override.
    ///
    /// Before its first successful read, consumers fall back to the static endpoint.
    #[config(
        default = "",
        env = "AENV_CLUSTER_SCHEDULER_ENDPOINT_FILE",
        parse_env = parse_trimmed_string
    )]
    pub scheduler_endpoint_file: String,
    /// Address where nodes serve the API-driven sandbox gRPC service.
    #[config(default = "0.0.0.0:8001", env = "AENV_NODE_SERVICE_ADDR")]
    pub node_service_addr: String,
    /// Address where the API serves data-plane wake-up gRPC.
    #[config(default = "0.0.0.0:8002", env = "AENV_API_GRPC_ADDR")]
    pub api_grpc_addr: String,
    /// Port substituted into discovered node HTTP addresses for the node gRPC service.
    #[config(default = 8001u16, env = "AENV_NODE_SERVICE_PORT")]
    pub node_service_port: u16,
    /// Strategy used to seed the API process's node registry.
    #[config(default = "kubernetes", env = "AENV_CLUSTER_NODE_DISCOVERY_MODE")]
    pub node_discovery_mode: ClusterNodeDiscoveryMode,
    /// Kubernetes discovery settings used in Kubernetes mode.
    #[config(nested)]
    pub kubernetes_discovery: ClusterKubernetesDiscoveryConfig,
    /// File-only static node list used in static mode.
    ///
    /// Structured lists are supplied through TOML overlays, not an env binding.
    #[config(default = [])]
    pub static_discovery_nodes: Vec<ClusterStaticDiscoveryNode>,
    /// Native registry warm-up timeout, measured from gRPC listener readiness.
    ///
    /// Zero uses the registry's default timeout.
    #[config(default = 15u64, env = "AENV_CLUSTER_NATIVE_WARMUP_TIMEOUT_SECS")]
    pub native_warmup_timeout_secs: u64,
    /// Candidate count for metrics-only placement shadow scoring.
    ///
    /// Zero is invalid; this setting never changes real round-robin placement.
    #[config(default = 3u32, env = "AENV_CLUSTER_PLACEMENT_SHADOW_K")]
    pub placement_shadow_k: u32,
    /// The shared-roster fix: see [`ClusterNodeRegistryStoreConfig`].
    #[config(nested)]
    pub node_registry_store: ClusterNodeRegistryStoreConfig,
}

/// Kubernetes EndpointSlice and Pod discovery settings.
///
/// Namespace and service name are required when Kubernetes mode is selected.
#[derive(Debug, Config, Clone)]
pub struct ClusterKubernetesDiscoveryConfig {
    /// The namespace the watched `EndpointSlice`/`Pod` objects live in.
    #[config(default = "", env = "AENV_CLUSTER_KUBERNETES_DISCOVERY_NAMESPACE")]
    pub namespace: String,
    /// The `Service` name whose `EndpointSlice`s are watched — matches
    /// `kubernetes.io/service-name` on each slice.
    #[config(default = "", env = "AENV_CLUSTER_KUBERNETES_DISCOVERY_SERVICE_NAME")]
    pub service_name: String,
    /// Node HTTP port used to construct discovered endpoints.
    #[config(default = 8000u16, env = "AENV_CLUSTER_KUBERNETES_DISCOVERY_PORT")]
    pub port: u16,
    /// The scheme discovered node endpoints are built with (`"http"` or
    /// `"https"`).
    #[config(default = "http", env = "AENV_CLUSTER_KUBERNETES_DISCOVERY_SCHEME")]
    pub scheme: String,
    /// A label selector for pods to exclude from discovery entirely. Empty
    /// (the default) disables the filter.
    #[config(
        default = "",
        env = "AENV_CLUSTER_KUBERNETES_DISCOVERY_IGNORE_POD_SELECTOR"
    )]
    pub ignore_pod_selector: String,
    /// A label selector for pods to keep discovered but mark unschedulable.
    /// Empty (the default) disables the filter.
    #[config(
        default = "",
        env = "AENV_CLUSTER_KUBERNETES_DISCOVERY_NO_SCHEDULE_POD_SELECTOR"
    )]
    pub no_schedule_pod_selector: String,
    /// Consecutive empty syncs required before accepting an empty cluster.
    #[config(
        default = 3u32,
        env = "AENV_CLUSTER_KUBERNETES_DISCOVERY_EMPTY_SYNC_CONFIRMATIONS"
    )]
    pub empty_sync_confirmations: u32,
    /// Maximum window before an empty sync is confirmed by time alone.
    #[config(
        default = 60u64,
        env = "AENV_CLUSTER_KUBERNETES_DISCOVERY_EMPTY_SYNC_WINDOW_SECS"
    )]
    pub empty_sync_window_secs: u64,
}

#[derive(Debug, Config, Clone)]
pub struct NodeIdentityConfig {
    #[config(env = "AENV_NODE_ID")]
    pub node_id: Option<String>,
    #[config(env = "AENV_CLUSTER_ID")]
    pub cluster_id: Option<String>,
    #[config(env = "AENV_SERVICE_INSTANCE_ID")]
    pub service_instance_id: Option<String>,
}

#[derive(Debug, Config, Clone)]
pub struct OrchestratorConfig {
    #[config(default = 1000u64)]
    pub auto_evict_interval_ms: u64,
    #[config(default = 15u64)]
    pub default_sandbox_timeout_secs: u64,
    #[config(default = 300u64)]
    pub auto_resume_min_sandbox_timeout_secs: u64,
    /// Total running-time ceiling across resumes; zero disables it.
    ///
    /// Paused time does not consume this budget.
    #[config(default = 86400u64, env = "AENV_MAX_SANDBOX_LIFETIME_SECS")]
    pub max_sandbox_lifetime_secs: u64,
    /// Slack added to the routing projection's TTL so the record outlives the
    /// sandbox it points at rather than expiring just before it.
    #[config(default = 60u64, env = "AENV_PROJECTION_TTL_GRACE_SECS")]
    pub projection_ttl_grace_secs: u64,
    /// Scratch root for capture artifacts and startup reclaim; nothing under
    /// it outlives a pause.
    #[config(
        default = "$AENV_HOME/persisted-sandboxes",
        env = "AENV_PERSISTED_SANDBOX_STORE_PATH",
        parse_env = parse_required_path
    )]
    pub persisted_sandbox_store_path: PathBuf,
    /// Optional startup reclaim override; unset means enabled.
    #[config(env = "AENV_STARTUP_RECLAIM_ENABLED")]
    pub startup_reclaim_enabled: Option<bool>,
    /// Delay after node isolation before shutdown begins stopping sandboxes.
    #[config(default = 10u64, env = "AENV_SHUTDOWN_DRAIN_PROPAGATION_SECS")]
    pub shutdown_drain_propagation_secs: u64,
    #[config(nested)]
    pub store: OrchestratorStoreConfig,
}

/// Where a process's orchestrator keeps its active-state records.
#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MetadataStoreBackendKind {
    /// Process-local ledger, suitable only for machine-local orchestration.
    InMemory,
    /// The cluster's shared store, so every API replica reads and writes one
    /// ledger.
    Redis,
}

impl MetadataStoreBackendKind {
    /// Deployment spelling for this backend.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::InMemory => "in_memory",
            Self::Redis => "redis",
        }
    }
}

/// Binding-store configuration.
#[derive(Debug, Config, Clone)]
pub struct BindingStoreConfig {
    /// Redis endpoint; Redis is the only supported binding-store backend.
    #[config(
        default = "redis://127.0.0.1:6379",
        env = "AENV_BINDING_STORE_REDIS_URL",
        parse_env = parse_trimmed_string
    )]
    pub redis_url: String,
    /// Gateway-compatible Redis key prefix.
    #[config(
        default = "agentenv:scheduler:bindings",
        env = "AENV_BINDING_STORE_REDIS_KEY_PREFIX",
        parse_env = parse_trimmed_string
    )]
    pub redis_key_prefix: String,
    /// TTL for per-node reverse-index reconciliation data.
    #[config(
        default = 3600u64,
        env = "AENV_BINDING_STORE_REDIS_NODE_INDEX_TTL_SECS"
    )]
    pub redis_node_index_ttl_secs: u64,
    /// Default TTL when a writer supplies no positive projection budget.
    #[config(default = 30u64, env = "AENV_BINDING_STORE_BINDING_TTL_SECS")]
    pub binding_ttl_secs: u64,
    /// Whether same-incarnation heartbeat refreshes preserve projection TTL.
    #[config(default = false, env = "AENV_BINDING_STORE_PROJECTION_AUTHORITATIVE")]
    pub projection_authoritative: bool,
    /// Maximum node-supplied projection TTL; zero removes the ceiling.
    #[config(
        default = 90_000u64,
        env = "AENV_BINDING_STORE_MAX_PROJECTION_TTL_SECS"
    )]
    pub max_projection_ttl_secs: u64,
    /// Enables heartbeat-timeout binding sweeps.
    #[config(default = true, env = "AENV_BINDING_STORE_SWEEP_ENABLED")]
    pub sweep_enabled: bool,
    /// Binding sweep cadence.
    #[config(default = 30u64, env = "AENV_BINDING_STORE_SWEEP_INTERVAL_SECS")]
    pub sweep_interval_secs: u64,
    /// Node silence threshold before bindings become sweep candidates.
    #[config(default = 300u64, env = "AENV_BINDING_STORE_SWEEP_SILENCE_SECS")]
    pub sweep_silence_secs: u64,
    /// Capacity of the P2P artifact-to-node hint index.
    #[config(
        default = 1_000_000u64,
        env = "AENV_BINDING_STORE_ARTIFACT_INDEX_CAPACITY"
    )]
    pub artifact_index_capacity: u64,
    /// Redis command response timeout.
    #[config(
        default = 2000u64,
        env = "AENV_BINDING_STORE_REDIS_RESPONSE_TIMEOUT_MS"
    )]
    pub redis_response_timeout_ms: u64,
    /// Redis connection-attempt timeout.
    #[config(default = 3000u64, env = "AENV_BINDING_STORE_REDIS_CONNECT_TIMEOUT_MS")]
    pub redis_connect_timeout_ms: u64,
}

#[derive(Debug, Config, Clone)]
pub struct OrchestratorStoreConfig {
    /// Store backend selected independently of file overlays.
    ///
    /// API processes reject the process-local default.
    #[config(default = "in_memory", env = "AENV_ORCHESTRATOR_STORE_BACKEND")]
    pub backend: MetadataStoreBackendKind,
    /// `redis://host:port[/db]`, read only when `backend = "redis"`.
    #[config(
        default = "redis://127.0.0.1:6379",
        env = "AENV_ORCHESTRATOR_STORE_REDIS_URL",
        parse_env = parse_trimmed_string
    )]
    pub redis_url: String,
    /// Redis namespace, which must not overlap the binding projection.
    #[config(
        default = "agentenv:api",
        env = "AENV_ORCHESTRATOR_STORE_KEY_PREFIX",
        parse_env = parse_trimmed_string
    )]
    pub redis_key_prefix: String,
    /// Whether contended updates wait on a distributed lock or fail fast.
    #[config(
        default = true,
        env = "AENV_ORCHESTRATOR_STORE_DISTRIBUTED_LOCK_ENABLED"
    )]
    pub redis_distributed_lock_enabled: bool,
    /// Redis command response timeout.
    #[config(
        default = 1500u64,
        env = "AENV_ORCHESTRATOR_STORE_REDIS_RESPONSE_TIMEOUT_MS"
    )]
    pub redis_response_timeout_ms: u64,
    /// Redis connection-attempt timeout.
    #[config(
        default = 2500u64,
        env = "AENV_ORCHESTRATOR_STORE_REDIS_CONNECT_TIMEOUT_MS"
    )]
    pub redis_connect_timeout_ms: u64,
}

/// Custom extension service integration.
///
/// The custom extension is an external HTTP service that extends node
/// behavior. Its only current capability is sandbox lifecycle hooks
/// (`/sandbox-hook/*`), but more may be added in the future.
#[derive(Debug, Config, Clone)]
pub struct CustomExtensionConfig {
    /// Optional HTTP base URL of the custom extension service.
    /// When unset, the custom extension integration is fully disabled.
    #[config(env = "AENV_CUSTOM_EXTENSION_URL", parse_env = parse_trimmed_string)]
    pub url: Option<String>,
    /// Timeout for custom extension HTTP calls, in milliseconds.
    #[config(default = 5000u64)]
    pub timeout_ms: u64,
}

/// Configured P2P transport kind, mapped to an implementation by `aenv-node`'s
/// `p2p` module.
#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum P2pTransportKind {
    Disabled,
    Iroh,
}

#[derive(Debug, Config, Clone)]
pub struct P2pConfig {
    #[config(default = false)]
    pub enabled: bool,
    #[config(default = "iroh")]
    pub transport: P2pTransportKind,
    #[config(default = "$AENV_HOME/p2p/store")]
    pub store_dir: PathBuf,
    #[config(default = "0.0.0.0:0")]
    pub listen_addr: String,
    #[config(default = 5000u64)]
    pub lookup_timeout_ms: u64,
    #[config(default = 30000u64)]
    pub fetch_timeout_ms: u64,
    #[config(default = 5u64)]
    pub peer_discovery_refresh_interval_secs: u64,
}

macro_rules! impl_config_default {
    ($($ty:ty),+ $(,)?) => {
        $(
            impl Default for $ty {
                fn default() -> Self {
                    <$ty as confique::Config>::builder()
                        .load()
                        .expect(concat!(stringify!($ty), " defaults are complete"))
                }
            }
        )+
    };
}
pub(crate) use impl_config_default;

impl_config_default!(
    AppConfig,
    BackendConfig,
    PosixFsBackendConfig,
    FirecrackerConfig,
    PoolTomlConfig,
    PoolComponentConfig,
    FirecrackerProcessPoolConfig,
    KernelConfig,
    ToolsConfig,
    SandboxProxyConfig,
    EnvdConfig,
    SandboxConfig,
    MachineConfig,
    SnapshotConfig,
    SnapshotImagePublishConfig,
    UblkTomlConfig,
    UblkOverlaybdTomlConfig,
    MemorySnapshotConfig,
    MemorySnapshotBackgroundDownloadConfig,
    ObservabilityConfig,
    ObservabilitySchedulerReportConfig,
    ClusterConfig,
    ClusterKubernetesDiscoveryConfig,
    ClusterNodeRegistryStoreConfig,
    NodeIdentityConfig,
    OrchestratorConfig,
    P2pConfig,
    CustomExtensionConfig,
);

impl AppConfig {
    /// The tools drive version in effect: the configured one, else the version
    /// the bundled manifest pins.
    pub fn resolved_tools_version(&self) -> &str {
        self.tools
            .version
            .as_deref()
            .unwrap_or(manifest_tools_version())
    }

    fn normalize(&mut self, config_dir: &Path) -> Result<()> {
        // Resolve config-owned filesystem paths relative to the active config file.
        // This preserves existing mounted-config behavior while letting confique own
        // layered value selection.
        self.home_path = resolve_relative_to(config_dir, &self.home_path);
        self.runtime_path = resolve_path(&self.home_path, config_dir, &self.runtime_path);
        self.deps_path = resolve_path(&self.home_path, config_dir, &self.deps_path);

        self.normalize_dependency_override_paths(config_dir);
        resolve_optional_config_path_or(
            &mut self.firecracker.work_dir,
            &self.home_path,
            config_dir,
            "firecracker-work",
        );
        resolve_optional_config_path_or(
            &mut self.firecracker.serial_dir,
            &self.home_path,
            config_dir,
            "logs/serial",
        );

        resolve_optional_config_path_or(
            &mut self.ublk.daemon_binary_path,
            &self.home_path,
            config_dir,
            "ublk/uvm-ublk-daemon",
        );
        self.ublk.daemon_socket_path = resolve_runtime_path(
            &self.home_path,
            &self.runtime_path,
            config_dir,
            &self.ublk.daemon_socket_path,
        );
        resolve_optional_config_path_or(
            &mut self.ublk.daemon_log_path,
            &self.home_path,
            config_dir,
            "logs/ublk-daemon.log",
        );

        self.ublk.overlaybd.global_config_path = resolve_path(
            &self.home_path,
            config_dir,
            &self.ublk.overlaybd.global_config_path,
        );

        ImageConfig::normalize(&mut self.image, config_dir, &self.home_path);

        self.memory_snapshot.overlaybd_global_config_path = resolve_path(
            &self.home_path,
            config_dir,
            &self.memory_snapshot.overlaybd_global_config_path,
        );
        self.orchestrator.persisted_sandbox_store_path = resolve_path(
            &self.home_path,
            config_dir,
            &self.orchestrator.persisted_sandbox_store_path,
        );

        self.snapshot.local_cache_path =
            resolve_path(&self.home_path, config_dir, &self.snapshot.local_cache_path);

        if self.snapshot.repository_backend == SnapshotRepositoryBackendKind::PosixFs {
            let posix_fs = self
                .backend
                .posix_fs
                .get_or_insert_with(PosixFsBackendConfig::default);
            posix_fs.snapshot_store =
                resolve_path(&self.home_path, config_dir, &posix_fs.snapshot_store);
            // The default can silently select an empty local store; log the resolved path.
            warn!(
                path = %posix_fs.snapshot_store.display(),
                "snapshot repository backend resolved to posix_fs (this is also the default when \
                 nothing sets AENV_SNAPSHOT_REPOSITORY_BACKEND, so this line does not mean the \
                 choice was explicit); if the intent was `oss`, check \
                 AENV_SNAPSHOT_REPOSITORY_BACKEND and AENV_CONFIG_OVERLAY_PATH — an empty or \
                 unexpectedly small snapshot store at this path is that fallback, not a catalog \
                 inconsistency"
            );
        }

        self.p2p.store_dir = resolve_path(&self.home_path, config_dir, &self.p2p.store_dir);
        self.cluster.normalize();
        self.sandbox_proxy.normalize()?;

        Ok(())
    }

    fn normalize_dependency_override_paths(&mut self, config_dir: &Path) {
        let home_path = self.home_path.clone();

        if let Some(binary_path) = self.firecracker.binary_path.as_mut() {
            *binary_path = resolve_path(&home_path, config_dir, binary_path);
        }

        if let Some(image_path) = self.kernel.image_path.as_mut() {
            *image_path = resolve_path(&home_path, config_dir, image_path);
        }

        if let Some(drive_path) = self.tools.drive_path.as_mut() {
            *drive_path = resolve_path(&home_path, config_dir, drive_path);
        }
    }

    /// Checks over the sections both halves read. Each binary adds its own
    /// half's checks: the file carries both halves' sections.
    fn validate_shared(&self) -> Result<()> {
        self.image.cache.gc.validate()?;
        NetworkConfig::validate(&self.network)?;
        if self.snapshot.catalog.build_heartbeat_interval_secs == 0 {
            bail!(
                "snapshot.catalog.build_heartbeat_interval_secs must be > 0; a build that never \
                 says it is alive is ended by the catalog's reaper while it is still running"
            );
        }
        Ok(())
    }
}

/// The checks a binary adds on top of the shared set, over the sections only
/// its half reads.
pub type HalfChecks = fn(&AppConfig) -> Result<()>;

/// The checks over sections only `aenv-api` reads. The node half loads the same
/// file and must not run them.
pub fn validate_api_half(config: &AppConfig) -> Result<()> {
    config.secrets.validate()?;
    if config.cluster.placement_shadow_k == 0 {
        bail!(
            "cluster.placement_shadow_k must be > 0; a zero-width sample keeps every \
             placement shadow metric series alive while making all of them meaningless"
        );
    }
    Ok(())
}

/// For a process that is neither half: tools and tests that load the file
/// without either binary's checks.
pub fn no_half_checks(_config: &AppConfig) -> Result<()> {
    Ok(())
}

#[derive(Debug)]
pub struct ConfigManager {
    config: AppConfig,
    config_path: Option<PathBuf>,
}

static GLOBAL_CONFIG_MANAGER: OnceLock<ConfigManager> = OnceLock::new();

impl ConfigManager {
    pub fn global() -> &'static Self {
        if let Some(manager) = GLOBAL_CONFIG_MANAGER.get() {
            return manager;
        }

        // Sibling-crate tests build this crate as a dependency with `cfg(test)` off.
        #[cfg(any(test, feature = "test-support"))]
        {
            Self::init_global(no_half_checks).expect("test ConfigManager initialization failed")
        }

        #[cfg(not(any(test, feature = "test-support")))]
        {
            GLOBAL_CONFIG_MANAGER
                .get()
                .expect("ConfigManager must be initialized before use")
        }
    }

    /// Loads the configuration and validates it for the calling half.
    pub fn init_global(half_checks: HalfChecks) -> Result<&'static Self> {
        if let Some(manager) = GLOBAL_CONFIG_MANAGER.get() {
            return Ok(manager);
        }
        Self::set_global(Self::new(half_checks)?)
    }

    /// Loads the configuration at `path` and validates it for the calling half.
    pub fn init_global_from_path(path: &Path, half_checks: HalfChecks) -> Result<&'static Self> {
        if let Some(manager) = GLOBAL_CONFIG_MANAGER.get() {
            return Ok(manager);
        }
        Self::set_global(Self::new_from_path(path, half_checks)?)
    }

    fn set_global(manager: Self) -> Result<&'static Self> {
        #[cfg(test)]
        let manager = {
            let mut manager = manager;
            manager
                .config
                .sandbox
                .access_token_hash_seed
                .get_or_insert_with(|| TEST_ACCESS_TOKEN_HASH_SEED.to_string());
            manager
        };
        let _ = GLOBAL_CONFIG_MANAGER.set(manager);
        GLOBAL_CONFIG_MANAGER
            .get()
            .ok_or_else(|| anyhow!("failed to initialize global sandbox config manager"))
    }

    pub fn new(half_checks: HalfChecks) -> Result<Self> {
        let config_path = Self::env_path(ENV_CONFIG_PATH).unwrap_or_else(Self::default_config_path);
        let config = Self::load_config_file(&config_path, half_checks)?;
        Ok(Self {
            config,
            config_path: Some(config_path),
        })
    }

    pub fn new_from_path(path: &Path, half_checks: HalfChecks) -> Result<Self> {
        let config = Self::load_config_file(path, half_checks)?;
        Ok(Self {
            config,
            config_path: Some(path.to_path_buf()),
        })
    }

    pub fn config(&self) -> &AppConfig {
        &self.config
    }

    pub fn global_config() -> &'static AppConfig {
        Self::global().config()
    }

    /// Returns global configuration only if already initialized.
    pub fn try_global_config() -> Option<&'static AppConfig> {
        GLOBAL_CONFIG_MANAGER.get().map(Self::config)
    }

    pub fn config_path(&self) -> Option<&Path> {
        self.config_path.as_deref()
    }

    fn default_config_path() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("config")
            .join("default.toml")
    }

    fn load_config_file(path: &Path, half_checks: HalfChecks) -> Result<AppConfig> {
        Self::load_config_file_with_overlays(path, &Self::overlay_paths_from_env(), half_checks)
    }

    /// Returns non-empty overlay paths in left-to-right application order.
    fn overlay_paths_from_env() -> Vec<PathBuf> {
        std::env::var(ENV_CONFIG_OVERLAY_PATH)
            .ok()
            .into_iter()
            .flat_map(|raw| {
                raw.split(OVERLAY_PATH_SEPARATOR)
                    .map(|segment| segment.trim().to_string())
                    .filter(|segment| !segment.is_empty())
                    .map(PathBuf::from)
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    /// Loads with confique's ordinary path when no overlays are configured;
    /// otherwise deep-merges TOML before applying the environment layer.
    fn load_config_file_with_overlays(
        path: &Path,
        overlays: &[PathBuf],
        half_checks: HalfChecks,
    ) -> Result<AppConfig> {
        let config_dir = path.parent().unwrap_or_else(|| Path::new("."));
        let mut config = if overlays.is_empty() {
            AppConfig::builder()
                .env()
                .file(path)
                .load()
                .with_context(|| format!("load config {}", path.display()))?
        } else {
            let layer = overlaid_config_layer(path, overlays)?;
            AppConfig::builder()
                .env()
                .preloaded(layer)
                .load()
                .with_context(|| {
                    format!(
                        "load config {} with overlays [{}]",
                        path.display(),
                        overlays
                            .iter()
                            .map(|overlay| overlay.display().to_string())
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                })?
        };
        config.normalize(config_dir)?;
        config.validate_shared()?;
        half_checks(&config)?;

        Ok(config)
    }

    fn env_path(name: &str) -> Option<PathBuf> {
        std::env::var(name)
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
    }
}

/// Merges the main TOML file and overlays into one confique layer.
///
/// Tables merge recursively and later scalar/array values replace earlier ones.
/// Merging before conversion preserves partial optional nested sections such as
/// `[backend.oss]`; environment values remain the top confique layer.
fn overlaid_config_layer(
    path: &Path,
    overlays: &[PathBuf],
) -> Result<<AppConfig as Config>::Layer> {
    // The optional main file remains empty when absent; overlays are required.
    let mut merged = read_config_toml(path, false)?;
    for overlay in overlays {
        merge_toml_tables(&mut merged, read_config_toml(overlay, true)?);
    }

    merged.try_into().map_err(|err| {
        anyhow!(
            "merged config ({} + [{}]) is not a valid AgentENV configuration: {err}",
            path.display(),
            overlays
                .iter()
                .map(|overlay| overlay.display().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )
    })
}

/// Reads a TOML table, allowing a missing main file but requiring named overlays.
fn read_config_toml(path: &Path, required: bool) -> Result<toml::Table> {
    let raw = match std::fs::read_to_string(path) {
        Ok(raw) => raw,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound && !required => {
            return Ok(toml::Table::new())
        }
        Err(err) if required => {
            return Err(anyhow!(
                "read config overlay {}: {err}. {ENV_CONFIG_OVERLAY_PATH} names it, so it has \
                 to be there — a node that started without it would fall back to whatever this \
                 repository's own default.toml happens to say and report nothing",
                path.display(),
            ))
        }
        Err(err) => return Err(anyhow!("read config {}: {err}", path.display())),
    };
    toml::from_str(&raw).with_context(|| format!("parse config {}", path.display()))
}

/// Deep-merges `overlay` into `base`: tables recurse, everything else replaces.
fn merge_toml_tables(base: &mut toml::Table, overlay: toml::Table) {
    for (key, value) in overlay {
        match (base.get_mut(&key), value) {
            (Some(toml::Value::Table(existing)), toml::Value::Table(incoming)) => {
                merge_toml_tables(existing, incoming);
            }
            (_, value) => {
                base.insert(key, value);
            }
        }
    }
}

fn resolve_relative_to(base_dir: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        base_dir.join(path)
    }
}

const HOME_PATH_PLACEHOLDER: &str = "$AENV_HOME";
const RUNTIME_PATH_PLACEHOLDER: &str = "$AENV_RUNTIME";

fn resolve_path(home_path: &Path, config_dir: &Path, raw: &Path) -> PathBuf {
    let expanded = match raw.to_str() {
        Some(s) if s.contains(HOME_PATH_PLACEHOLDER) => {
            PathBuf::from(s.replace(HOME_PATH_PLACEHOLDER, &home_path.to_string_lossy()))
        }
        _ => raw.to_path_buf(),
    };
    resolve_relative_to(config_dir, &expanded)
}

fn resolve_runtime_path(
    home_path: &Path,
    runtime_path: &Path,
    config_dir: &Path,
    raw: &Path,
) -> PathBuf {
    let expanded = match raw.to_str() {
        Some(s) => PathBuf::from(
            s.replace(HOME_PATH_PLACEHOLDER, &home_path.to_string_lossy())
                .replace(RUNTIME_PATH_PLACEHOLDER, &runtime_path.to_string_lossy()),
        ),
        None => raw.to_path_buf(),
    };
    resolve_relative_to(config_dir, &expanded)
}

fn resolve_optional_config_path_or(
    path: &mut Option<PathBuf>,
    home_path: &Path,
    config_dir: &Path,
    default_relative_path: &str,
) {
    let resolved = match path.take() {
        Some(raw) => resolve_path(home_path, config_dir, &raw),
        None => home_path.join(default_relative_path),
    };
    *path = Some(resolved);
}

fn parse_required_path(raw: &str) -> std::result::Result<PathBuf, std::convert::Infallible> {
    Ok(PathBuf::from(raw.trim()))
}

fn parse_trimmed_string(raw: &str) -> std::result::Result<String, std::convert::Infallible> {
    Ok(raw.trim().to_string())
}

impl ClusterConfig {
    fn normalize(&mut self) {
        self.scheduler_endpoint = self
            .scheduler_endpoint
            .as_deref()
            .map(str::trim)
            .filter(|endpoint| !endpoint.is_empty())
            .map(ToOwned::to_owned);
    }
}

impl SandboxProxyConfig {
    fn normalize(&mut self) -> Result<()> {
        let domains = &self.domains;
        let mut normalized = Vec::with_capacity(domains.len());
        for domain in domains {
            let domain = domain.trim().trim_end_matches('.');
            if domain.is_empty() {
                continue;
            }
            let Some(domain) = network::normalize_dns_name(domain) else {
                bail!("sandbox_proxy.domains contains invalid domain {domain:?}");
            };
            if !normalized.iter().any(|seen| seen == &domain) {
                normalized.push(domain);
            }
        }
        // Keep user order: domains[0] is the advertised sandbox response domain.
        self.domains = normalized;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn bundled_default_config_loads() -> Result<()> {
        let _env = env_guard();
        let workspace = Path::new(env!("CARGO_MANIFEST_DIR"));
        ConfigManager::new_from_path(&workspace.join("config/default.toml"), validate_api_half)?;
        Ok(())
    }

    #[test]
    fn the_bundled_default_config_never_carries_a_pg_dsn() {
        let text = std::fs::read_to_string(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("config/default.toml"),
        )
        .expect("read the bundled config");
        let parsed: toml::Value = toml::from_str(&text)
            .map_err(|err| err.message().to_string())
            .expect("parse the bundled config");

        let Some(section) = parsed.get("pg").and_then(toml::Value::as_table) else {
            return;
        };
        let blank = section
            .get("dsn")
            .and_then(toml::Value::as_str)
            .map(str::trim)
            .unwrap_or("");
        assert!(
            blank.is_empty(),
            "config/default.toml commits a non-blank [pg].dsn -- a database credential must \
             come from a file named by AENV_CONFIG_OVERLAY_PATH, projected from a mounted \
             Secret, never from a tracked file"
        );
    }

    #[test]
    fn the_binding_store_redis_key_prefix_defaults_to_the_go_value() {
        let _env = env_guard();
        let workspace = Path::new(env!("CARGO_MANIFEST_DIR"));
        assert_eq!(
            ConfigManager::new_from_path(&workspace.join("config/default.toml"), no_half_checks)
                .expect("load without the override")
                .config()
                .binding_store
                .redis_key_prefix,
            "agentenv:scheduler:bindings",
            "this is gateway's existing read path, not a name this deployment may rename freely"
        );
    }

    #[test]
    fn the_snapshot_repository_backend_is_settable_from_the_environment() {
        let _env = env_guard();
        let workspace = Path::new(env!("CARGO_MANIFEST_DIR"));
        let bundled = workspace.join("config/default.toml");

        assert_eq!(
            ConfigManager::new_from_path(&bundled, no_half_checks)
                .expect("load without the override")
                .config()
                .snapshot
                .repository_backend,
            SnapshotRepositoryBackendKind::PosixFs,
            "the file's value must stand when the environment says nothing"
        );

        for (value, expected) in [
            ("oss", SnapshotRepositoryBackendKind::Oss),
            ("posix_fs", SnapshotRepositoryBackendKind::PosixFs),
        ] {
            std::env::set_var("AENV_SNAPSHOT_REPOSITORY_BACKEND", value);
            let overridden = ConfigManager::new_from_path(&bundled, no_half_checks);
            std::env::remove_var("AENV_SNAPSHOT_REPOSITORY_BACKEND");

            assert_eq!(
                overridden
                    .unwrap_or_else(|err| panic!("load with backend={value}: {err}"))
                    .config()
                    .snapshot
                    .repository_backend,
                expected,
                "AENV_SNAPSHOT_REPOSITORY_BACKEND={value} did not reach the config"
            );
        }

        for typo in ["OSS", "oss ", "s3", "object_storage", "posixfs"] {
            std::env::set_var("AENV_SNAPSHOT_REPOSITORY_BACKEND", typo);
            let loaded = ConfigManager::new_from_path(&bundled, no_half_checks);
            std::env::remove_var("AENV_SNAPSHOT_REPOSITORY_BACKEND");

            assert!(
                loaded.is_err(),
                "backend={typo:?} was accepted; a misspelled backend must not silently leave the \
                 node on `posix_fs` with a bucket full of snapshots it will never look at"
            );
        }

        // Environment values override file overlays.
        let dir = tempdir().expect("tempdir");
        let overlay = dir.path().join("overlay.toml");
        for (env_value, overlay_value, overlay_expected, expected) in [
            (
                "oss",
                "posix_fs",
                SnapshotRepositoryBackendKind::PosixFs,
                SnapshotRepositoryBackendKind::Oss,
            ),
            (
                "posix_fs",
                "oss",
                SnapshotRepositoryBackendKind::Oss,
                SnapshotRepositoryBackendKind::PosixFs,
            ),
        ] {
            std::fs::write(
                &overlay,
                format!(
                    "[snapshot]\nrepository_backend = \"{overlay_value}\"\n\n\
                     [backend.oss]\nendpoint = \"http://rustfs:9000\"\nbucket = \"b\"\n"
                ),
            )
            .expect("write overlay");

            let from_overlay = ConfigManager::load_config_file_with_overlays(
                &bundled,
                std::slice::from_ref(&overlay),
                no_half_checks,
            )
            .expect("load with the overlay alone");
            assert_eq!(
                from_overlay.snapshot.repository_backend, overlay_expected,
                "the overlay must decide when the environment says nothing"
            );

            std::env::set_var("AENV_SNAPSHOT_REPOSITORY_BACKEND", env_value);
            let overridden = ConfigManager::load_config_file_with_overlays(
                &bundled,
                std::slice::from_ref(&overlay),
                no_half_checks,
            );
            std::env::remove_var("AENV_SNAPSHOT_REPOSITORY_BACKEND");

            assert_eq!(
                overridden
                    .expect("load with both the overlay and the environment")
                    .snapshot
                    .repository_backend,
                expected,
                "the overlay said {overlay_value} and the environment said {env_value}; the \
                 environment has to win or `kubectl set env` stops being a rollback"
            );
        }
    }

    #[test]
    fn the_image_cache_budgets_are_settable_from_the_environment() {
        let _env = env_guard();
        let workspace = Path::new(env!("CARGO_MANIFEST_DIR"));
        let bundled = workspace.join("config/default.toml");

        let shipped = ConfigManager::new_from_path(&bundled, no_half_checks)
            .expect("load without the override");
        let shipped_capacity = shipped.config().image.cache.capacity_gb;
        let shipped_remote = shipped.config().image.cache.remote_blocks.max_size_gb;

        assert_ne!(
            shipped_capacity,
            Some(24),
            "the shipped capacity is already the value this test overrides to; the override arm \
             would prove nothing"
        );
        assert_ne!(
            shipped_remote, 12,
            "the shipped remote-block budget is already the value this test overrides to"
        );

        std::env::set_var("AENV_IMAGE_CACHE_CAPACITY_GB", "24");
        std::env::set_var("AENV_IMAGE_CACHE_REMOTE_BLOCKS_MAX_SIZE_GB", "12");
        let overridden = ConfigManager::new_from_path(&bundled, no_half_checks);
        std::env::remove_var("AENV_IMAGE_CACHE_CAPACITY_GB");
        std::env::remove_var("AENV_IMAGE_CACHE_REMOTE_BLOCKS_MAX_SIZE_GB");

        let overridden = overridden.expect("load with the budget overrides");
        assert_eq!(
            overridden.config().image.cache.capacity_gb,
            Some(24),
            "AENV_IMAGE_CACHE_CAPACITY_GB did not reach the config"
        );
        assert_eq!(
            overridden.config().image.cache.remote_blocks.max_size_gb,
            12,
            "AENV_IMAGE_CACHE_REMOTE_BLOCKS_MAX_SIZE_GB did not reach the config"
        );

        let after = ConfigManager::new_from_path(&bundled, no_half_checks)
            .expect("load after the override");
        assert_eq!(after.config().image.cache.capacity_gb, shipped_capacity);
        assert_eq!(
            after.config().image.cache.remote_blocks.max_size_gb,
            shipped_remote
        );
    }

    /// Serializes tests that read or modify process-global config environment variables.
    fn env_guard() -> std::sync::MutexGuard<'static, ()> {
        static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        ENV_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    #[test]
    fn merging_config_documents_is_deep_and_the_later_file_wins() {
        let mut merged: toml::Table = toml::from_str(
            r#"
scalar = "from base"
array = [1, 2]
[backend.oss]
endpoint = "http://base:9000"
bucket = "base-bucket"
[keep]
untouched = "base"
"#,
        )
        .expect("parse base");

        merge_toml_tables(
            &mut merged,
            toml::from_str(
                r#"
[backend.oss]
access_key_id = "from-first-overlay"
endpoint = "http://first:9000"
"#,
            )
            .expect("parse first overlay"),
        );
        merge_toml_tables(
            &mut merged,
            toml::from_str(
                r#"
scalar = "from second"
array = [9]
[backend.oss]
endpoint = "http://second:9000"
"#,
            )
            .expect("parse second overlay"),
        );

        let oss = merged["backend"]["oss"].as_table().expect("[backend.oss]");

        // Base-only keys survive partial table overlays.
        assert_eq!(
            oss["bucket"].as_str(),
            Some("base-bucket"),
            "an overlay that mentions [backend.oss] replaced the section instead of merging \
             into it; the endpoint and the bucket would then have to live in the same file as \
             the credentials"
        );
        assert_eq!(
            oss["access_key_id"].as_str(),
            Some("from-first-overlay"),
            "the second overlay dropped what the first one contributed"
        );
        assert_eq!(oss["endpoint"].as_str(), Some("http://second:9000"));
        assert_eq!(merged["scalar"].as_str(), Some("from second"));
        assert_eq!(
            merged["keep"]["untouched"].as_str(),
            Some("base"),
            "a table no overlay mentioned was disturbed"
        );

        // Arrays replace rather than concatenate.
        assert_eq!(
            merged["array"].as_array().map(Vec::len),
            Some(1),
            "arrays must replace whole"
        );
        assert_eq!(merged["array"][0].as_integer(), Some(9));

        let mut swapped: toml::Table = toml::from_str("value = 1\n[table]\nx = 1\n").expect("base");
        merge_toml_tables(
            &mut swapped,
            toml::from_str("table = 2\n[value]\nx = 1\n").expect("overlay"),
        );
        assert_eq!(swapped["table"].as_integer(), Some(2));
        assert!(swapped["value"].is_table());
    }

    #[test]
    fn no_overlay_is_byte_for_byte_the_old_load() {
        let _env = env_guard();
        let workspace = Path::new(env!("CARGO_MANIFEST_DIR"));
        let bundled = workspace.join("config/default.toml");
        let config_dir = bundled.parent().expect("config dir");

        let mut old = AppConfig::builder()
            .env()
            .file(&bundled)
            .load()
            .expect("the pre-overlay load");
        old.normalize(config_dir).expect("normalize");
        old.validate_shared().expect("validate");

        let new = ConfigManager::load_config_file_with_overlays(&bundled, &[], no_half_checks)
            .expect("the current load with no overlay");

        let old_dump = format!("{old:#?}");
        let new_dump = format!("{new:#?}");
        assert_eq!(
            old_dump, new_dump,
            "loading config/default.toml with no overlay no longer produces what the old \
             .env().file(path) produced"
        );

        let mut planted = old;
        planted.home_path = PathBuf::from("/planted-home-path");
        assert_ne!(
            format!("{planted:#?}"),
            new_dump,
            "the dump comparison cannot see a field that was changed by hand, so its verdict \
             above means nothing"
        );
        assert!(
            old_dump.contains("/var/lib/aenv") && old_dump.lines().count() > 200,
            "the dump is not the whole config; it has {} lines",
            old_dump.lines().count()
        );
    }

    #[test]
    fn an_empty_overlay_leaves_the_main_file_alone() {
        let _env = env_guard();
        let bundled = Path::new(env!("CARGO_MANIFEST_DIR")).join("config/default.toml");
        let dir = tempdir().expect("tempdir");

        let empty = dir.path().join("empty.toml");
        std::fs::write(&empty, "# nothing but a comment\n").expect("write empty overlay");

        let without = ConfigManager::load_config_file_with_overlays(&bundled, &[], no_half_checks)
            .expect("load with no overlay");
        let with_empty = ConfigManager::load_config_file_with_overlays(
            &bundled,
            std::slice::from_ref(&empty),
            no_half_checks,
        )
        .expect("load with an empty overlay");
        assert_eq!(
            format!("{without:#?}"),
            format!("{with_empty:#?}"),
            "routing config/default.toml through the overlay merge changed how it parses"
        );

        let changed = dir.path().join("changed.toml");
        std::fs::write(&changed, "[firecracker]\nsocket_poll_ms = 7\n").expect("write overlay");
        let with_change = ConfigManager::load_config_file_with_overlays(
            &bundled,
            std::slice::from_ref(&changed),
            no_half_checks,
        )
        .expect("load with a real overlay");
        assert_eq!(with_change.firecracker.socket_poll_ms, 7);
        assert_ne!(
            format!("{without:#?}"),
            format!("{with_change:#?}"),
            "an overlay that changes a value produced an identical dump; the comparison above \
             is measuring nothing"
        );
    }

    #[test]
    fn two_overlays_assemble_the_backend_section_no_environment_can_reach() {
        let _env = env_guard();
        let bundled = Path::new(env!("CARGO_MANIFEST_DIR")).join("config/default.toml");
        let dir = tempdir().expect("tempdir");

        let public = dir.path().join("oss.toml");
        std::fs::write(
            &public,
            "[snapshot]\nrepository_backend = \"oss\"\n\n\
             [backend.oss]\nendpoint = \"http://rustfs:9000\"\nbucket = \"agentenv-snapshots\"\n\
             region = \"us-east-1\"\nprefix = \"snapshots/\"\ncache_max_size_gb = 8\n",
        )
        .expect("write the public overlay");

        let secret = dir.path().join("oss-credentials.toml");
        std::fs::write(
            &secret,
            "[backend.oss]\naccess_key_id = \"planted-key-id\"\n\
             access_key_secret = \"planted-key-secret\"\n",
        )
        .expect("write the secret overlay");

        let config = ConfigManager::load_config_file_with_overlays(
            &bundled,
            &[public.clone(), secret.clone()],
            no_half_checks,
        )
        .expect("load with both halves");
        let oss = config
            .backend
            .oss
            .as_ref()
            .expect("[backend.oss] must arrive from the overlays");
        assert_eq!(
            config.snapshot.repository_backend,
            SnapshotRepositoryBackendKind::Oss
        );
        assert_eq!(oss.endpoint, "http://rustfs:9000");
        assert_eq!(oss.bucket, "agentenv-snapshots");
        assert_eq!(oss.region.as_deref(), Some("us-east-1"));
        assert_eq!(oss.prefix.as_deref(), Some("snapshots/"));
        assert_eq!(oss.cache_max_size_gb, Some(8));
        assert_eq!(oss.access_key_id.as_deref(), Some("planted-key-id"));
        assert_eq!(oss.access_key_secret.as_deref(), Some("planted-key-secret"));

        let alone = ConfigManager::load_config_file_with_overlays(&bundled, &[], no_half_checks)
            .expect("load the main file alone");
        assert!(alone.backend.oss.is_none());
        assert_eq!(
            alone.snapshot.repository_backend,
            SnapshotRepositoryBackendKind::PosixFs
        );

        let public_only = ConfigManager::load_config_file_with_overlays(
            &bundled,
            std::slice::from_ref(&public),
            no_half_checks,
        )
        .expect("the public half alone loads");
        assert!(public_only
            .backend
            .oss
            .expect("public half supplies the section")
            .access_key_id
            .is_none());
        let secret_only = ConfigManager::load_config_file_with_overlays(
            &bundled,
            std::slice::from_ref(&secret),
            no_half_checks,
        );
        assert!(
            secret_only.is_err(),
            "a [backend.oss] with credentials but no endpoint or bucket must be refused, not \
             completed from somewhere"
        );

        let shadow = dir.path().join("shadow.toml");
        std::fs::write(&shadow, "[backend.oss]\naccess_key_id = \"shadowed\"\n")
            .expect("write shadow");
        let secret_last = ConfigManager::load_config_file_with_overlays(
            &bundled,
            &[public.clone(), shadow.clone(), secret.clone()],
            no_half_checks,
        )
        .expect("load");
        let shadow_last = ConfigManager::load_config_file_with_overlays(
            &bundled,
            &[public, secret, shadow],
            no_half_checks,
        )
        .expect("load");
        assert_eq!(
            secret_last
                .backend
                .oss
                .expect("section")
                .access_key_id
                .as_deref(),
            Some("planted-key-id")
        );
        assert_eq!(
            shadow_last
                .backend
                .oss
                .expect("section")
                .access_key_id
                .as_deref(),
            Some("shadowed")
        );
    }

    #[test]
    fn a_named_overlay_that_is_not_on_disk_stops_the_load() {
        let _env = env_guard();
        let bundled = Path::new(env!("CARGO_MANIFEST_DIR")).join("config/default.toml");
        let dir = tempdir().expect("tempdir");

        let present = dir.path().join("present.toml");
        std::fs::write(&present, "[firecracker]\nsocket_poll_ms = 7\n").expect("write");
        assert_eq!(
            ConfigManager::load_config_file_with_overlays(
                &bundled,
                std::slice::from_ref(&present),
                no_half_checks
            )
            .expect("an overlay that exists loads")
            .firecracker
            .socket_poll_ms,
            7
        );

        let missing = dir.path().join("not-mounted.toml");
        let err = ConfigManager::load_config_file_with_overlays(
            &bundled,
            &[present, missing.clone()],
            no_half_checks,
        )
        .expect_err("a missing overlay must not be treated as an empty layer");
        let text = format!("{err:#}");
        assert!(
            text.contains(&missing.display().to_string()),
            "the error does not name the file that is missing: {text}"
        );
        assert!(
            text.contains(ENV_CONFIG_OVERLAY_PATH),
            "the error does not name the variable that asked for it: {text}"
        );
    }

    #[test]
    fn the_overlay_variable_is_read_and_only_separators_means_unset() {
        let _env = env_guard();

        let _ = ConfigManager::global();

        let bundled = Path::new(env!("CARGO_MANIFEST_DIR")).join("config/default.toml");
        let dir = tempdir().expect("tempdir");
        let overlay = dir.path().join("overlay.toml");
        std::fs::write(&overlay, "[firecracker]\nsocket_poll_ms = 7\n").expect("write overlay");

        std::env::remove_var(ENV_CONFIG_OVERLAY_PATH);
        assert!(ConfigManager::overlay_paths_from_env().is_empty());

        for quiet in ["", "   ", ":", " : ", "::"] {
            std::env::set_var(ENV_CONFIG_OVERLAY_PATH, quiet);
            let paths = ConfigManager::overlay_paths_from_env();
            std::env::remove_var(ENV_CONFIG_OVERLAY_PATH);
            assert!(
                paths.is_empty(),
                "{quiet:?} should mean no overlay, got {paths:?}"
            );
        }

        for (value, expected) in [
            ("a.toml:b.toml", vec!["a.toml", "b.toml"]),
            (":a.toml::b.toml:", vec!["a.toml", "b.toml"]),
            (" a.toml : b.toml ", vec!["a.toml", "b.toml"]),
            ("only.toml", vec!["only.toml"]),
        ] {
            std::env::set_var(ENV_CONFIG_OVERLAY_PATH, value);
            let paths = ConfigManager::overlay_paths_from_env();
            std::env::remove_var(ENV_CONFIG_OVERLAY_PATH);
            assert_eq!(
                paths,
                expected.iter().map(PathBuf::from).collect::<Vec<_>>(),
                "{value:?} was not read as the list it names"
            );
        }

        let quiet = ConfigManager::new_from_path(&bundled, no_half_checks)
            .expect("load with the variable unset");
        assert_eq!(
            quiet.config().firecracker.socket_poll_ms,
            1,
            "the file's own value must stand when the variable says nothing"
        );

        std::env::set_var(ENV_CONFIG_OVERLAY_PATH, overlay.display().to_string());
        let mounted = ConfigManager::new_from_path(&bundled, no_half_checks);
        std::env::remove_var(ENV_CONFIG_OVERLAY_PATH);
        assert_eq!(
            mounted
                .expect("load with the variable set")
                .config()
                .firecracker
                .socket_poll_ms,
            7,
            "AENV_CONFIG_OVERLAY_PATH did not reach the loader"
        );
    }

    fn struct_bodies(source: &str) -> Vec<(String, String)> {
        let mut out = Vec::new();
        let mut rest = source;
        while let Some(at) = rest.find("\npub struct ") {
            let after = &rest[at + "\npub struct ".len()..];
            let name: String = after
                .chars()
                .take_while(|ch| ch.is_alphanumeric() || *ch == '_')
                .collect();
            // Rustfmt closes top-level item bodies at column zero.
            let body_start = match after.find('{') {
                Some(brace) => brace,
                None => break,
            };
            let body = match after[body_start..].find("\n}") {
                Some(end) => &after[body_start..body_start + end],
                None => &after[body_start..],
            };
            out.push((name, body.to_string()));
            rest = after;
        }
        out
    }

    fn unreachable_config_structs(bodies: &[(String, String)]) -> Vec<String> {
        let known: std::collections::HashSet<&str> =
            bodies.iter().map(|(name, _)| name.as_str()).collect();

        let mut unreachable: Vec<String> = Vec::new();
        for (_, body) in bodies {
            let lines: Vec<&str> = body.lines().collect();
            for (idx, line) in lines.iter().enumerate() {
                let trimmed = line.trim();
                let inner = match trimmed
                    .split_once(": Option<")
                    .and_then(|(_, rest)| rest.split_once('>'))
                {
                    Some((inner, _)) => inner,
                    None => continue,
                };
                if !known.contains(inner) {
                    continue;
                }
                // Walk backward across this field's attributes.
                let mut nested = false;
                for prev in lines[..idx].iter().rev() {
                    let prev = prev.trim();
                    if prev.is_empty() || prev.starts_with("///") {
                        continue;
                    }
                    if prev.starts_with('#') || prev.starts_with(')') || prev.ends_with(',') {
                        if prev.contains("nested") {
                            nested = true;
                        }
                        if prev.starts_with("#[config(") || prev.starts_with('#') {
                            continue;
                        }
                        continue;
                    }
                    break;
                }
                if !nested {
                    unreachable.push(inner.to_string());
                }
            }
        }
        unreachable.sort();
        unreachable.dedup();
        unreachable
    }

    fn dead_env_bindings(source: &str) -> Vec<String> {
        let bodies = struct_bodies(source);
        let unreachable = unreachable_config_structs(&bodies);

        let mut dead: Vec<String> = Vec::new();
        for (name, body) in &bodies {
            if !unreachable.contains(name) {
                continue;
            }
            let code_only: String = body
                .lines()
                .filter(|line| !line.trim_start().starts_with("///"))
                .collect::<Vec<_>>()
                .join("\n");
            if code_only.contains("env =") {
                dead.push(name.clone());
            }
        }
        dead.sort();
        dead.dedup();
        dead
    }

    fn live_env_bindings(source: &str) -> Vec<String> {
        let bodies = struct_bodies(source);
        let unreachable = unreachable_config_structs(&bodies);

        let mut live: Vec<String> = Vec::new();
        for (name, body) in &bodies {
            if unreachable.contains(name) {
                continue;
            }
            for line in body.lines() {
                if line.trim_start().starts_with("///") {
                    continue;
                }
                let mut rest = line;
                while let Some(at) = rest.find("env = \"") {
                    rest = &rest[at + "env = \"".len()..];
                    match rest.split_once('"') {
                        Some((value, tail)) => {
                            live.push(value.to_string());
                            rest = tail;
                        }
                        None => break,
                    }
                }
            }
        }
        live.sort();
        live.dedup();
        live
    }

    fn documented_server_env_vars(doc: &str) -> Vec<String> {
        let section = match doc.split_once("\n## Server\n") {
            Some((_, rest)) => rest.split("\n## ").next().unwrap_or(rest),
            None => return Vec::new(),
        };

        let mut out: Vec<String> = Vec::new();
        for line in section.lines() {
            let trimmed = line.trim();
            let Some(first_cell) = trimmed
                .strip_prefix('|')
                .and_then(|rest| rest.split('|').next())
            else {
                continue;
            };
            let first_cell = first_cell.trim();
            if first_cell.starts_with("~~") {
                continue;
            }
            let Some(name) = first_cell
                .strip_prefix('`')
                .and_then(|rest| rest.split('`').next())
            else {
                continue;
            };
            if name.starts_with("AENV_") || name.starts_with("API_") {
                out.push(name.to_string());
            }
        }
        out.sort();
        out.dedup();
        out
    }

    #[test]
    fn every_documented_server_variable_is_one_the_process_reads() {
        let workspace = Path::new(env!("CARGO_MANIFEST_DIR"));
        let sources = [
            "src/cfg.rs",
            "src/cfg/egress_broker.rs",
            "src/cfg/image.rs",
            "src/cfg/network.rs",
        ]
        .iter()
        .map(|relative| {
            std::fs::read_to_string(workspace.join(relative))
                .unwrap_or_else(|err| panic!("read {relative}: {err}"))
        })
        .collect::<Vec<_>>()
        .join("\n");
        let doc = std::fs::read_to_string(workspace.join("docs/src/configuration/env-vars.md"))
            .expect("read env-vars.md");

        let bound = live_env_bindings(&sources);
        let documented = documented_server_env_vars(&doc);

        assert!(
            bound.contains(&"AENV_HOME_PATH".to_string())
                && bound.contains(&"AENV_SNAPSHOT_REPOSITORY_BACKEND".to_string())
                && bound.contains(&"AENV_IMAGE_CACHE_CAPACITY_GB".to_string()),
            "the attribute scan did not find bindings that are certainly there (the last one \
             lives in src/cfg/image.rs, so this also proves every source file was read); \
             found {} names",
            bound.len()
        );
        assert!(
            !bound.contains(&"AENV_SNAPSHOT_STORE".to_string()),
            "the attribute scan counts a binding on a struct confique cannot reach as live"
        );
        assert!(
            documented.len() > 15 && documented.contains(&"AENV_HOME_PATH".to_string()),
            "the documentation scan did not read the ## Server table; found {documented:?}"
        );
        assert!(
            !documented.contains(&"AENV_SNAPSHOT_STORE".to_string()),
            "AENV_SNAPSHOT_STORE is still offered as a supported override. It has never been \
             read; the row must stay struck through"
        );
        assert!(
            !documented.contains(&"E2B_API_URL".to_string()),
            "the documentation scan ran past the ## Server table into the SDK's own variables"
        );

        let read_elsewhere = [
            ("AENV_CONFIG_PATH", "ENV_CONFIG_PATH, ConfigManager::new"),
            (
                "AENV_CONFIG_OVERLAY_PATH",
                "ENV_CONFIG_OVERLAY_PATH, ConfigManager::overlay_paths_from_env",
            ),
            ("AENV_LOG_FORMAT", "src/logging.rs"),
            ("AENV_LOG_SPAN_EVENTS", "src/logging.rs"),
            ("AENV_FORCE_SYSCTL_TUNING", "src/setup/network_capacity.rs"),
            ("API_ADDR", "src/server_main.rs"),
        ];

        let unkept: Vec<&String> = documented
            .iter()
            .filter(|name| !bound.contains(name))
            .filter(|name| {
                !read_elsewhere
                    .iter()
                    .any(|(known, _)| *known == name.as_str())
            })
            .collect();
        assert!(
            unkept.is_empty(),
            "docs/src/configuration/env-vars.md offers variables nothing reads: {unkept:?}. \
             Either bind them on a field confique can reach, add them to `read_elsewhere` with \
             the file that reads them, or strike the row through the way ~~AENV_SNAPSHOT_STORE~~ \
             is. A documented variable that does nothing is how AENV_SNAPSHOT_STORE survived \
             for years."
        );

        let planted_doc = "\n## Server\n\n| Variable | Default | Description |\n\
                           |---|---|---|\n\
                           | `AENV_PLANTED_DEAD` | — | offered |\n\
                           | ~~`AENV_PLANTED_GONE`~~ | — | struck through |\n\
                           \n## Something Else\n| `AENV_PLANTED_ELSEWHERE` | — | other table |\n";
        assert_eq!(
            documented_server_env_vars(planted_doc),
            vec!["AENV_PLANTED_DEAD".to_string()],
            "the documentation scan cannot tell an offered row from a struck-through one, or \
             it ran past the end of the table"
        );
    }

    #[test]
    fn no_new_env_binding_is_declared_where_confique_cannot_read_it() {
        let planted = r#"
#[derive(Debug, Deserialize, Clone, Config)]
@@ITEM@@ PlantedOuterConfig {
    pub reachable_only_by_serde: Option<PlantedLeafConfig>,
    #[config(nested)]
    pub reachable_by_env: PlantedNestedConfig,
}

#[derive(Debug, Deserialize, Clone, Config)]
@@ITEM@@ PlantedLeafConfig {
    /// A doc comment that names env = "AENV_PLANTED_IN_A_COMMENT" and must not
    /// count, or nothing could ever explain this rule in prose.
    #[config(default = "x", env = "AENV_PLANTED_DEAD")]
    pub value: String,
}

#[derive(Debug, Deserialize, Clone, Config)]
@@ITEM@@ PlantedNestedConfig {
    #[config(default = "x", env = "AENV_PLANTED_LIVE")]
    pub value: String,
}
"#
        .replace("@@ITEM@@", concat!("pub ", "struct"));
        let planted = planted.as_str();
        let planted_bodies = struct_bodies(planted);
        assert_eq!(
            unreachable_config_structs(&planted_bodies),
            vec!["PlantedLeafConfig".to_string()],
            "the scan cannot tell a bare Option<_> field from a #[config(nested)] one; \
             its verdict on the real tree means nothing"
        );
        assert_eq!(
            dead_env_bindings(planted),
            vec!["PlantedLeafConfig".to_string()],
            "the scan does not report a dead binding that is deliberately there — either it \
             cannot see the attribute, or it counted the one inside the doc comment as well"
        );

        let source =
            std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("src/cfg.rs"))
                .expect("read src/cfg.rs");
        let bodies = struct_bodies(&source);
        let unreachable = unreachable_config_structs(&bodies);

        assert!(
            unreachable.contains(&"OssBackendConfig".to_string())
                && unreachable.contains(&"PosixFsBackendConfig".to_string())
                && unreachable.contains(&"PgConfig".to_string()),
            "the scan lost sight of the [backend] structs and PgConfig it exists to guard; \
             found {unreachable:?}"
        );

        assert_eq!(
            dead_env_bindings(&source),
            Vec::<String>::new(),
            "a config struct declares an environment binding confique cannot read. Most likely \
             an `env` attribute added to `OssBackendConfig` in the belief that a secretKeyRef \
             would reach it. It will not: `[backend.oss]` is deserialized from a file only. Put \
             the value in a file named by AENV_CONFIG_OVERLAY_PATH instead — that is what it is \
             for, and it is what the deployment already mounts."
        );
    }

    #[test]
    fn sandbox_access_token_seed_is_redacted() {
        let config = SandboxConfig {
            access_token_hash_seed: Some("cluster-secret".to_string()),
        };
        assert!(!format!("{config:?}").contains("cluster-secret"));
    }

    #[test]
    fn resolve_path_joins_relative_paths_against_config_dir() -> Result<()> {
        let temp = tempdir()?;
        let config_dir = temp.path();
        let home_path = temp.path().join("home");

        assert_eq!(
            resolve_path(&home_path, config_dir, Path::new("bin/firecracker")),
            config_dir.join("bin/firecracker")
        );
        assert_eq!(
            resolve_path(&home_path, config_dir, Path::new("$AENV_HOME/image-cache")),
            home_path.join("image-cache")
        );

        Ok(())
    }

    #[test]
    fn normalize_paths_relative_to_config_dir() -> Result<()> {
        let temp = tempdir()?;
        let config_dir = temp.path().join("configs");
        let mut config = AppConfig {
            deps_path: "./env".into(),
            ..Default::default()
        };
        config.firecracker.binary_path = Some("./bin/firecracker".into());
        config.firecracker.work_dir = Some("./firecracker-work".into());
        config.firecracker.serial_dir = Some("./firecracker-serial".into());
        config.kernel.image_path = Some("./kernel/vmlinux.bin".into());
        config.tools.drive_path = Some("./tools.ext4".into());
        config.image.cache.root_dir = "./custom-image-cache".into();
        config.snapshot.local_cache_path = "./snapshot-local-cache".into();
        config.backend.posix_fs = Some(PosixFsBackendConfig {
            snapshot_store: "./snapshot-store".into(),
        });
        config.p2p.store_dir = "./p2p-store".into();
        config.ublk.daemon_binary_path = Some("./bin/uvm-ublk-daemon".into());
        config.ublk.daemon_socket_path = "./run/uvm-ublk-daemon.sock".into();
        config.ublk.daemon_log_path = Some("./logs/uvm-ublk-daemon.log".into());
        config.ublk.overlaybd.global_config_path = "./overlaybd-global.json".into();
        config.memory_snapshot.overlaybd_global_config_path = "./mem-overlaybd-global.json".into();
        config.orchestrator.persisted_sandbox_store_path = "./persisted-sandboxes".into();
        config.normalize(&config_dir)?;

        assert_eq!(config.deps_path, config_dir.join("env"));
        assert_eq!(
            config.firecracker.binary_path,
            Some(config_dir.join("bin/firecracker"))
        );
        assert_eq!(
            config.firecracker.work_dir,
            Some(config_dir.join("firecracker-work"))
        );
        assert_eq!(
            config.firecracker.serial_dir,
            Some(config_dir.join("firecracker-serial"))
        );
        assert_eq!(
            config.kernel.image_path,
            Some(config_dir.join("kernel/vmlinux.bin"))
        );
        assert_eq!(config.tools.drive_path, Some(config_dir.join("tools.ext4")));
        assert_eq!(
            config.image.cache.root_dir,
            config_dir.join("custom-image-cache")
        );
        assert_eq!(
            config.snapshot.local_cache_path,
            config_dir.join("snapshot-local-cache")
        );
        assert_eq!(config.p2p.store_dir, config_dir.join("p2p-store"));
        assert_eq!(
            config.backend.posix_fs.as_ref().unwrap().snapshot_store,
            config_dir.join("snapshot-store")
        );
        assert_eq!(
            config.ublk.daemon_binary_path,
            Some(config_dir.join("bin/uvm-ublk-daemon"))
        );
        assert_eq!(
            config.ublk.daemon_socket_path,
            config_dir.join("run/uvm-ublk-daemon.sock")
        );
        assert_eq!(
            config.ublk.daemon_log_path,
            Some(config_dir.join("logs/uvm-ublk-daemon.log"))
        );
        assert_eq!(
            config.ublk.overlaybd.global_config_path,
            config_dir.join("overlaybd-global.json")
        );
        assert_eq!(
            config.memory_snapshot.overlaybd_global_config_path,
            config_dir.join("mem-overlaybd-global.json")
        );
        assert_eq!(
            config.orchestrator.persisted_sandbox_store_path,
            config_dir.join("persisted-sandboxes")
        );

        Ok(())
    }

    #[test]
    fn managed_dependency_paths_remain_implicit() -> Result<()> {
        let temp = tempdir()?;
        let config_dir = temp.path().join("configs");
        let mut config = AppConfig {
            deps_path: "./deps".into(),
            ..Default::default()
        };
        config.firecracker.version = Some("fc-test".to_string());
        config.kernel.version = Some("kernel-test".to_string());
        config.tools.version = Some("1.2.3-custom.1".to_string());

        config.normalize(&config_dir)?;

        assert_eq!(config.deps_path, config_dir.join("deps"));
        assert_eq!(config.firecracker.binary_path, None);
        assert_eq!(config.kernel.image_path, None);
        assert_eq!(config.tools.drive_path, None);

        Ok(())
    }

    #[test]
    fn home_relative_toml_values_derive_from_home_path() -> Result<()> {
        let temp = tempdir()?;
        let config_dir = temp.path().join("configs");
        let mut config = AppConfig {
            home_path: "./env".into(),
            runtime_path: "$AENV_HOME/run".into(),
            deps_path: "$AENV_HOME/deps".into(),
            ..Default::default()
        };
        config.ublk.daemon_socket_path = "$AENV_RUNTIME/ublk-daemon.sock".into();
        config.firecracker.serial_dir = Some("$AENV_HOME/logs/serial".into());
        config.image.cache.root_dir = "$AENV_HOME/image-cache".into();
        config.snapshot.local_cache_path = "$AENV_HOME/snapshot-local-cache".into();
        config.p2p.store_dir = "$AENV_HOME/p2p/store".into();
        config.ublk.overlaybd.global_config_path =
            "$AENV_HOME/overlaybd/overlaybd-global.json".into();
        config.memory_snapshot.overlaybd_global_config_path =
            "$AENV_HOME/overlaybd/mem-overlaybd-global.json".into();
        config.normalize(&config_dir)?;

        let home_path = config_dir.join("env");
        assert_eq!(config.runtime_path, home_path.join("run"));
        assert_eq!(
            config.ublk.daemon_socket_path,
            home_path.join("run/ublk-daemon.sock")
        );
        assert_eq!(
            config.firecracker.serial_dir,
            Some(home_path.join("logs/serial"))
        );
        assert_eq!(config.deps_path, home_path.join("deps"));
        assert_eq!(config.image.cache.root_dir, home_path.join("image-cache"));
        assert_eq!(
            config.snapshot.local_cache_path,
            home_path.join("snapshot-local-cache")
        );
        assert_eq!(config.p2p.store_dir, home_path.join("p2p").join("store"));
        assert_eq!(
            config.ublk.overlaybd.global_config_path,
            home_path.join("overlaybd").join("overlaybd-global.json")
        );
        assert_eq!(
            config.memory_snapshot.overlaybd_global_config_path,
            home_path
                .join("overlaybd")
                .join("mem-overlaybd-global.json")
        );

        Ok(())
    }

    #[test]
    fn compile_time_defaults_derive_from_home_path() -> Result<()> {
        let temp = tempdir()?;
        let config_dir = temp.path().join("configs");
        let mut config = AppConfig {
            home_path: "./env".into(),
            ..Default::default()
        };
        config.normalize(&config_dir)?;

        let home_path = config_dir.join("env");
        assert_eq!(config.runtime_path, PathBuf::from("/run/aenv"));
        assert_eq!(config.deps_path, home_path.join("deps"));
        assert_eq!(
            config.firecracker.work_dir,
            Some(home_path.join("firecracker-work"))
        );
        assert_eq!(
            config.firecracker.serial_dir,
            Some(home_path.join("logs/serial"))
        );
        assert_eq!(config.image.cache.root_dir, home_path.join("image-cache"));
        assert_eq!(
            config.snapshot.local_cache_path,
            home_path.join("snapshot-local-cache")
        );
        assert_eq!(config.p2p.store_dir, home_path.join("p2p/store"));
        assert_eq!(
            config.ublk.overlaybd.global_config_path,
            home_path.join("overlaybd/overlaybd-global.json")
        );
        assert_eq!(
            config.memory_snapshot.overlaybd_global_config_path,
            home_path.join("overlaybd/mem-overlaybd-global.json")
        );
        assert_eq!(
            config.orchestrator.persisted_sandbox_store_path,
            home_path.join("persisted-sandboxes")
        );
        assert_eq!(
            config.backend.posix_fs.as_ref().unwrap().snapshot_store,
            home_path.join("snapshot-store")
        );
        assert_eq!(
            config.ublk.daemon_binary_path,
            Some(home_path.join("ublk/uvm-ublk-daemon"))
        );
        assert_eq!(
            config.ublk.daemon_socket_path,
            PathBuf::from("/run/aenv/ublk-daemon.sock")
        );
        assert_eq!(
            config.ublk.daemon_log_path,
            Some(home_path.join("logs/ublk-daemon.log"))
        );

        Ok(())
    }

    #[test]
    fn sandbox_proxy_domains_are_normalized() -> Result<()> {
        let mut config = SandboxProxyConfig {
            domains: vec![
                " Sandbox.Example.Invalid. ".to_string(),
                "sandbox.example.invalid".to_string(),
                "".to_string(),
                "old.example.invalid".to_string(),
            ],
        };
        config.normalize()?;

        assert_eq!(
            config.domains,
            vec!["sandbox.example.invalid", "old.example.invalid"]
        );

        Ok(())
    }

    #[test]
    fn sandbox_proxy_domains_reject_invalid_domain() {
        let mut config = SandboxProxyConfig {
            domains: vec!["not a domain".to_string()],
        };
        let err = config.normalize().unwrap_err();
        assert!(
            err.to_string()
                .contains("sandbox_proxy.domains contains invalid domain"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn cluster_normalize_trims_and_drops_blank_scheduler_endpoint() {
        let mut config = ClusterConfig {
            scheduler_endpoint: Some("  ".to_string()),
            scheduler_endpoint_file: String::new(),
            node_service_addr: "0.0.0.0:8001".to_string(),
            api_grpc_addr: "0.0.0.0:8002".to_string(),
            node_service_port: 8001,
            node_discovery_mode: Default::default(),
            kubernetes_discovery: Default::default(),
            static_discovery_nodes: Vec::new(),
            native_warmup_timeout_secs: 15,
            placement_shadow_k: crate::node_registry::placement::DEFAULT_PLACEMENT_SHADOW_K,
            node_registry_store: Default::default(),
        };
        config.normalize();
        assert_eq!(config.scheduler_endpoint, None);

        let mut config = ClusterConfig {
            scheduler_endpoint: Some("  http://scheduler:9090  ".to_string()),
            scheduler_endpoint_file: String::new(),
            node_service_addr: "0.0.0.0:8001".to_string(),
            api_grpc_addr: "0.0.0.0:8002".to_string(),
            node_service_port: 8001,
            node_discovery_mode: Default::default(),
            kubernetes_discovery: Default::default(),
            static_discovery_nodes: Vec::new(),
            native_warmup_timeout_secs: 15,
            placement_shadow_k: crate::node_registry::placement::DEFAULT_PLACEMENT_SHADOW_K,
            node_registry_store: Default::default(),
        };
        config.normalize();
        assert_eq!(
            config.scheduler_endpoint.as_deref(),
            Some("http://scheduler:9090")
        );
    }

    #[test]
    fn resolve_config_path_keeps_absolute_paths_unchanged() {
        let absolute = PathBuf::from("/tmp/agentenv-absolute");
        assert_eq!(
            resolve_path(Path::new("/ignored-home"), Path::new("/ignored"), &absolute),
            absolute
        );
    }

    #[test]
    fn placement_shadow_k_defaults_to_three_and_binds_its_documented_env_var() {
        use confique::meta::{Expr, FieldKind, Integer, LeafKind};

        let field = ClusterConfig::META
            .fields
            .iter()
            .find(|field| field.name == "placement_shadow_k")
            .expect("[cluster].placement_shadow_k exists");
        let FieldKind::Leaf { env, kind } = field.kind else {
            panic!("placement_shadow_k is a leaf, not a nested section");
        };
        assert_eq!(env, Some("AENV_CLUSTER_PLACEMENT_SHADOW_K"));
        assert_eq!(
            kind,
            LeafKind::Required {
                default: Some(Expr::Integer(Integer::U32(3))),
            },
        );

        assert_eq!(AppConfig::default().cluster.placement_shadow_k, 3);
    }

    #[test]
    fn the_api_half_accepts_what_only_the_node_half_reads() {
        let mut config = AppConfig::default();
        config.ublk.overlaybd.resize_timeout_secs = 0;
        config.memory_snapshot.background_download.concurrency = 0;
        config.pool.low_watermark = 64;
        config.pool.high_watermark = 32;

        config
            .validate_shared()
            .expect("none of these sections is shared");
        validate_api_half(&config).expect(
            "the api half reads none of these sections, so a file that carries them must \
             still start it",
        );
    }

    #[test]
    fn validate_rejects_a_zero_placement_shadow_k() {
        let mut config = AppConfig::default();
        config.cluster.placement_shadow_k = 0;
        let err = validate_api_half(&config).unwrap_err();
        assert!(
            err.to_string()
                .contains("cluster.placement_shadow_k must be > 0"),
            "unexpected error: {err}"
        );

        validate_api_half(&AppConfig::default()).expect("the shipped default must load");
    }

    /// Returns whether a manifest sets `var`, excluding comment-only mentions.
    fn manifest_sets(contents: &str, var: &str) -> bool {
        contents
            .lines()
            .any(|line| line.contains(var) && !line.trim_start().starts_with('#'))
    }

    #[test]
    fn no_manifest_sets_a_node_service_gate_nothing_reads() {
        const VAR: &str = "AENV_NODE_SERVICE_ENABLED";

        for (form, shape) in [
            (
                format!("            - name: {VAR}\n              value: \"true\""),
                "a container env: entry",
            ),
            (format!("  {VAR}: \"true\""), "a ConfigMap data: key"),
            (format!("      - {VAR}=true"), "a kustomize literal"),
        ] {
            assert!(
                manifest_sets(&form, VAR),
                "the scan cannot see {VAR} written as {shape}, so the gate could be reintroduced \
                 in that form and this test would still pass"
            );
        }
        assert!(!manifest_sets(
            &format!("            # {VAR} is deliberately absent, and here is why"),
            VAR
        ));
        assert!(!manifest_sets(
            "            - name: AENV_NODE_SERVICE_ADDR",
            VAR
        ));

        let cfg = include_str!("cfg.rs");
        assert!(
            cfg.contains("env = \"AENV_NODE_SERVICE_ADDR\""),
            "`env = \"...\"` is no longer how this file binds an environment \
             variable, so the check below is looking for the wrong shape"
        );
        assert!(
            !cfg.contains(&format!("env = \"{VAR}\"")),
            "{VAR} is now read by the config. It was removed from the manifests \
             precisely because nothing read it; if it has become a real setting, \
             this test is the wrong shape and the manifests need it back"
        );

        let deploy = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("deploy");
        let mut checked = 0;
        // Keep one real manifest as an end-to-end scanner control.
        let mut sample: Option<(std::path::PathBuf, String)> = None;
        let mut stack = vec![deploy.clone()];
        while let Some(dir) = stack.pop() {
            let Ok(entries) = std::fs::read_dir(&dir) else {
                continue;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    stack.push(path);
                    continue;
                }
                let Ok(contents) = std::fs::read_to_string(&path) else {
                    continue;
                };
                checked += 1;
                assert!(
                    !manifest_sets(&contents, VAR),
                    "{} sets {VAR}. No AgentENV process reads it, so this buys a serial \
                     DaemonSet roll across the fleet and changes nothing. Serving the node \
                     sandbox service is what `aenv-node` is for",
                    path.display()
                );
                if sample.is_none() && contents.contains('\n') {
                    sample = Some((path.clone(), contents));
                }
            }
        }

        let (sampled_path, sampled) =
            sample.expect("the walk read no file with more than one line");
        assert!(
            manifest_sets(&format!("{sampled}\n  {VAR}: \"true\"\n"), VAR),
            "adding a real setting to {} did not make the scan notice it",
            sampled_path.display()
        );
        assert!(
            checked > 10,
            "only {checked} files under {} were read; a scan that reads nothing passes \
             everything",
            deploy.display()
        );
    }
}
