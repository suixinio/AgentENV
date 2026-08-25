use std::path::{Path, PathBuf};
use std::sync::OnceLock;

pub(crate) mod image;
pub(crate) mod network;
use anyhow::{anyhow, bail, Context, Result};
use confique::Config;
pub use image::{
    ImageCacheConfig, ImageConfig, ImageRemoteBlocksCacheConfig, ImageResolverConfig,
    ResolvedImageCacheConfig, ResolvedImageCacheGcConfig,
};
pub use network::{NetworkConfig, NetworkEgressConfig, NetworkInternalConfig};
use overlaybd::config::UpperMode;
use serde::Deserialize;
use tracing::warn;

use crate::virtualization::VirtualizationMode;

const ENV_CONFIG_PATH: &str = "AENV_CONFIG_PATH";

/// Extra TOML files layered on top of [`ENV_CONFIG_PATH`], separated by `:`.
///
/// 🔴 This exists because of one file that cannot be in this repository and
/// cannot be reached from the environment either.
///
/// `deploy/k8s/run.sh` copies `config/default.toml` over the cluster's
/// `agentenv-k8s-config` ConfigMap on every apply (run.sh:30), so anything a
/// cluster says only in that ConfigMap is lost on the next `make k8s-apply`.
/// For scalars the answer is an `env =` attribute, which an apply cannot
/// reach — `AENV_SNAPSHOT_REPOSITORY_BACKEND` and the two image-cache budgets
/// went that way. `[backend.oss]` cannot: confique descends into a struct only
/// through `#[config(nested)]`, `nested` may not be `Option<_>`, and
/// `backend.oss` is `Option<OssBackendConfig>` — so the endpoint, the bucket
/// and the credentials are deserialized from a file and from nothing else.
/// `no_new_env_binding_is_declared_where_confique_cannot_read_it` pins that.
///
/// A second file is the only way in, and it wants to be a *mounted* one: the
/// credentials belong in a Secret, and kubelet refreshes volumes.
///
/// Unset — or set to nothing but separators — is the whole of the old
/// behaviour: [`ConfigManager::load_config_file`] then runs confique's
/// `.env().file(path)` untouched, and
/// `no_overlay_is_byte_for_byte_the_old_load` compares a full dump of both.
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

pub(crate) fn regctl_path(deps_path: &Path) -> PathBuf {
    deps_path
        .join("regctl")
        .join(&SetupDependencyManifest::get().regclient.version)
        .join("regctl")
}

#[derive(Debug, Clone, Config)]
pub struct AppConfig {
    /// Shared PostgreSQL connection settings for the control plane
    /// (`--role api` / `--role all`). See `src/pg/mod.rs`.
    ///
    /// `Option<PgConfig>`, not `#[config(nested)]`, on purpose and for the
    /// same reason as `[backend.oss]`: confique only descends into a struct
    /// through `#[config(nested)]`, which may not be `Option<_>`, so this is
    /// deserialized from a file and from nothing else — no `env =` binding on
    /// it or anything inside it can ever be read. The DSN belongs in a file
    /// named by `AENV_CONFIG_OVERLAY_PATH`, mounted from a Secret, never in
    /// `config/default.toml`.
    ///
    /// 🔴 Kept first in this struct, deliberately: the source scan behind
    /// `no_new_env_binding_is_declared_where_confique_cannot_read_it` walks
    /// backward from an `Option<_>` field over any preceding sibling field
    /// that ends in a comma, including ones with unrelated `#[config(nested)]`
    /// attributes, and only stops at whatever comes before the field it is
    /// checking — so a bare `Option<_>` field placed *after* one of this
    /// struct's many `#[config(nested)]` fields would read as falsely
    /// reachable and the scan would stop watching it for a dead `env =`
    /// binding. `[backend.oss]`/`[backend.posix_fs]` avoid the same trap by
    /// living inside `BackendConfig`, a struct with no `nested` fields in it
    /// at all; `[pg]` has no natural wrapper of its own to borrow that from,
    /// so first-with-nothing-before-it is what keeps the scan honest here
    /// instead.
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
    #[config(nested)]
    pub api: ApiConfig,
}

/// The node's own HTTP API.
#[derive(Debug, Config, Clone)]
pub struct ApiConfig {
    /// Credentials that prove a control-plane call came through the gateway.
    ///
    /// A list rather than a single value so a rotation can accept the old and
    /// the new one at once. Empty — together with an empty
    /// [`control_plane_token_file`](Self::control_plane_token_file) — means the
    /// gate is off and the node behaves exactly as it did before it existed,
    /// which is what makes turning it off a configuration change rather than a
    /// code change.
    ///
    /// 🔴 Read once, at startup. Changing it means restarting the process, and
    /// this process pauses every sandbox on the node on its way out. Use the
    /// file below to turn the gate on and off.
    #[config(
        default = [],
        env = "AENV_API_CONTROL_PLANE_TOKEN",
        parse_env = confique::env::parse::list_by_comma
    )]
    pub control_plane_tokens: Vec<String>,
    /// A file holding the same thing, one credential per line, re-read while
    /// the process runs.
    ///
    /// 🔴 This is what makes enabling and disabling the gate free. The node is
    /// a DaemonSet whose graceful shutdown pauses every sandbox it holds, so a
    /// restart is never cheap and a rollback that needs one is a rollback
    /// nobody will reach for. Point this at a mounted Secret — mounted, not
    /// `secretKeyRef`, because kubelet refreshes volumes and does not refresh
    /// environment variables.
    ///
    /// The effective set is the union of both. Empty or unset contributes
    /// nothing.
    #[config(
        default = "",
        env = "AENV_API_CONTROL_PLANE_TOKEN_FILE",
        parse_env = parse_trimmed_string
    )]
    pub control_plane_token_file: String,
}

#[derive(Debug, Deserialize, Clone, Config)]
pub struct BackendConfig {
    pub posix_fs: Option<PosixFsBackendConfig>,
    pub oss: Option<OssBackendConfig>,
}

#[derive(Debug, Deserialize, Clone, Config)]
pub struct PosixFsBackendConfig {
    /// Root directory of the posix_fs snapshot repository.
    ///
    /// 🔴 There is no environment binding here, and one must not be added
    /// back. This field carried an `env` attribute naming AENV_SNAPSHOT_STORE
    /// from the day it was written; `docs/src/configuration/env-vars.md`
    /// promised that variable to operators for just as long, and confique
    /// never once read it. `[backend.posix_fs]`
    /// is reached as `Option<PosixFsBackendConfig>`, confique descends into a
    /// struct only through `#[config(nested)]`, and `nested` may not be
    /// `Option<_>` (`confique-macro/src/parse.rs:127`) — so this struct is
    /// deserialized by serde from a file and by nothing else. An environment
    /// binding on it is not an override, it is a promise the loader will not
    /// keep, and a
    /// promise is worse than an absence: somebody sets the variable, nothing
    /// happens, and nothing says so.
    ///
    /// Making it real would mean giving `[backend]` two non-`Option` nested
    /// fields, which changes what a config with `repository_backend = "oss"`
    /// resolves to — `backend.posix_fs` would stop being `None` there, and
    /// `sandbox/firecracker/overlaybd_snapshot.rs` reads exactly that. Not a
    /// change to make on the way past.
    ///
    /// The supported way to set this from outside the file is
    /// [`ENV_CONFIG_OVERLAY_PATH`], which is also the only way to reach
    /// `[backend.oss]`. `no_new_env_binding_is_declared_where_confique_cannot_read_it`
    /// fails if any environment binding reappears on this side of the seam.
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
    /// Whether the cluster-wide CPUID intersection the scheduler computes is
    /// applied to a cold-booting microVM via `PUT /cpu-config`.
    ///
    /// 🔴 Default on, because turning it off is a real loss: the template is
    /// what makes two machines with different CPUs present the same CPUID to a
    /// sandbox, and a snapshot that cold-boots on one host and is expected to
    /// behave the same on another depends on it.
    ///
    /// It is a setting at all because a host can refuse the template outright.
    /// On Intel Granite Rapids (Xeon 6975P-C) the helper dumps CPUID leaf 0x1f
    /// subleaf 1, which KVM will not let a VMM write, and Firecracker answers
    /// the pre-boot call with
    ///   `Template changes a CPUID entry not supported by KVM: Leaf: 1f, Subleaf: 1`
    /// — every cold boot fails, which means every template build and every new
    /// sandbox fails, with nothing in the message pointing at a setting.
    ///
    /// Turning it off is safe exactly when every node in the cluster has the
    /// same CPU, which is the only shape the intersection was protecting.
    /// Resume is unaffected either way: a snapshot carries the full CPU state
    /// in `vm_state.bin` and Firecracker rejects re-applying a template on top
    /// of it, so this path is cold boot only.
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

#[derive(Debug, Clone)]
pub struct ResolvedFirecrackerPoolConfig {
    pub pool: warm_pool::PoolConfig,
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
    #[config(default = "0.5.15")]
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
    /// 🔴 Settable from the environment for the reason `paused_registry.backend`
    /// and `catalog.write`/`catalog.read` are: `deploy/k8s/run.sh` copies
    /// `config/default.toml` over the cluster's `agentenv-k8s-config` ConfigMap
    /// on every apply (run.sh:30), so a cluster that said `oss` only in that
    /// ConfigMap loses it on the next `make k8s-apply`.
    ///
    /// And it loses it in the quiet direction. `posix_fs` is a backend that
    /// starts: the node comes up serving an empty local filesystem while the
    /// snapshots and templates it used to answer for sit untouched in a bucket
    /// it no longer looks at. Nothing fails, nothing is logged at `error`, and
    /// the catalog rows still point at artifacts the process can no longer
    /// fetch. That is the failure this override exists to remove.
    ///
    /// 🔴 Setting this to `oss` is only half of it. `[backend.oss]` — the
    /// endpoint, bucket and credentials — is *not* reachable from the
    /// environment (see `no_env_binding_is_declared_where_confique_cannot_read_it`),
    /// so a cluster that sets this variable and lets the apply take its
    /// `[backend.oss]` section away does not start at all: both
    /// `write_generated_overlaybd_global_config` and the repository builder
    /// stop with "backend.oss config is required when repository_backend =
    /// oss". Loud, and therefore survivable — but it is not a working cluster,
    /// and the section still has to reach the node some other way.
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

/// Which store holds the snapshot catalog, while it is moving between two.
///
/// 🔴 Two knobs, not one, and they roll separately. Adding the second copy and
/// starting to trust it are different decisions with different ways back — one
/// is "stop writing there", the other is "stop reading there" — and the
/// combination that is legal at any moment is the one the matrix in
/// [`AppConfig::validate_snapshot_catalog`] allows.
#[derive(Debug, Config, Clone)]
pub struct SnapshotCatalogConfig {
    /// 🔴 Settable from the environment for the reason
    /// `paused_registry.backend` is: `deploy/k8s/run.sh` copies
    /// `config/default.toml` over the cluster's ConfigMap on every apply, so a
    /// cluster that expressed this by editing the ConfigMap would lose it
    /// silently, and lose it in the quiet direction — back to writing one store
    /// while believing it writes two.
    #[config(default = "object_store", env = "AENV_SNAPSHOT_CATALOG_WRITE")]
    pub write: SnapshotCatalogWrite,
    #[config(default = "object_store", env = "AENV_SNAPSHOT_CATALOG_READ")]
    pub read: SnapshotCatalogRead,
    /// How often owed object-store writes are replayed.
    #[config(default = 30u64)]
    pub mirror_compensator_interval_secs: u64,
    /// How often a running build tells the catalog it is still alive.
    ///
    /// 🔴 Must stay comfortably below the scheduler's
    /// `scheduler.catalog.build_heartbeat_ttl` (5 minutes by default), because
    /// the reaper ends a build that has gone unheard from for that long and
    /// hands its template to whoever asks next. A third of the TTL is the usual
    /// margin: two renewals may be lost — to a scheduler rollout, a slow
    /// network — before a build that is running perfectly well is taken away
    /// from it.
    ///
    /// 🔴 The scheduler cannot see this number — nothing on the wire carries
    /// it — so it is declared to the scheduler a second time, as
    /// `scheduler.catalog.node_build_heartbeat_interval`, which is what its
    /// TTL floor is computed from. Changing this without changing that leaves
    /// the scheduler enforcing a floor for a cadence this cluster no longer
    /// uses; the two must move together.
    #[config(default = 100u64)]
    pub build_heartbeat_interval_secs: u64,
    /// Where the owed writes are kept.
    ///
    /// Node-local and durable: what it holds is the difference between "the two
    /// catalogs agree" and "they do not", and a process that crashed holding
    /// the answer must not come back believing they agreed.
    #[config(
        default = "$AENV_HOME/snapshot-catalog-mirror",
        env = "AENV_SNAPSHOT_CATALOG_MIRROR_PATH",
        parse_env = parse_required_path
    )]
    pub mirror_backlog_path: PathBuf,
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotCatalogWrite {
    /// Today: object storage is the catalog.
    ObjectStore,
    /// Both, object storage still answering reads. The central catalog gets a
    /// second copy that can be checked against the first.
    Both,
    /// The central catalog alone. Only after the read side has been served from
    /// it for an observation period — until then, dropping the object-store
    /// copy removes the way back.
    Postgres,
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotCatalogRead {
    ObjectStore,
    Postgres,
}

#[derive(Debug, Config, Clone)]
pub struct SnapshotImagePublishConfig {
    #[config(default = false)]
    pub enabled: bool,
}

/// `[pg]`: shared PostgreSQL connection settings, consumed by
/// `src/pg::PgPoolSettings::from_config`.
///
/// Reached only as `Option<PgConfig>` (see the field doc on
/// [`AppConfig::pg`]), so — like [`OssBackendConfig`] — every field here is
/// deserialized by serde from a file and none may ever carry an `env =`
/// attribute; `no_new_env_binding_is_declared_where_confique_cannot_read_it`
/// enforces that by scanning this file's source text.
#[derive(Debug, Deserialize, Clone, Default)]
pub struct PgConfig {
    /// A libpq-style connection URL
    /// (`postgres://user:password@host:port/dbname`). Absent or blank means
    /// PostgreSQL is not configured for this process.
    ///
    /// 🔴 Never a real value in `config/default.toml` or any other tracked
    /// file — `the_bundled_default_config_never_carries_a_pg_dsn` fails the
    /// build if it ever is. `deploy/k8s` must supply it the same way it
    /// supplies `[backend.oss]`'s credentials: a tracked, credential-free
    /// overlay file for everything else in this struct, and a second overlay
    /// projected from a mounted Secret, listed after it in
    /// `AENV_CONFIG_OVERLAY_PATH`, carrying this field alone.
    pub dsn: Option<String>,
    /// Per-replica pool cap. Defaults to 8 when unset — see
    /// `src/pg::pool::DEFAULT_MAX_CONNECTIONS` for why that number, and for
    /// the reminder that `--role api` runs more than one replica: the
    /// cluster-wide connection count this deployment produces is
    /// `replica_count * max_connections`, not this number alone, and has to
    /// stay under PostgreSQL's own `max_connections`.
    pub max_connections: Option<u32>,
    /// Bounds the pool's initial connection attempt and every later acquire.
    /// Defaults to 5 seconds when unset.
    pub connect_timeout_secs: Option<u64>,
}

impl PgConfig {
    /// [`Self::dsn`] with surrounding whitespace trimmed and blank treated as
    /// absent — the same "blank is the same as absent" rule
    /// `PausedRegistryConfig`'s `scheduler_endpoint` uses, for the same
    /// reason: a ConfigMap that carries the key with an empty value is not
    /// naming a database.
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

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PausedRegistryBackendKind {
    /// Node-local only. A paused sandbox is resumable on the node that paused
    /// it and invisible to the rest of the cluster.
    Local,
    /// Removed. Kept as a name so a deployment still carrying it is told what
    /// to do instead of being told the word is unknown.
    ///
    /// 🔴 It fails startup rather than being treated as `central`. The two are
    /// not interchangeable at the moment of the switch: `central` needs a
    /// scheduler endpoint this node may not have been given, and a node that
    /// silently reinterpreted the value would either start with no registry at
    /// all or start against an endpoint nobody meant it to use. Neither
    /// reports anything until a node is lost.
    Postgres,
    /// Same registry, reached over gRPC through whoever owns the database
    /// instead of by connecting to it. Identical semantics to `postgres` — the
    /// difference is that the database credentials, the connection budget and
    /// the schema stop being every node's business.
    ///
    /// Requires `[cluster].scheduler_endpoint`.
    Central,
}

impl PausedRegistryBackendKind {
    /// The name a deployment writes into `AENV_PAUSED_REGISTRY_BACKEND`.
    ///
    /// Used by the registry's assembly log, so what an operator reads back is
    /// the same word they set — a log that named the backends differently
    /// would be one more thing to translate at the moment somebody is checking
    /// whether the switch they just made took effect.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Postgres => "postgres",
            Self::Central => "central",
        }
    }
}

#[derive(Debug, Config, Clone)]
pub struct PausedRegistryConfig {
    /// 🔴 Settable from the environment on purpose. `deploy/k8s/run.sh` copies
    /// `config/default.toml` over the cluster's ConfigMap on every apply, so a
    /// cluster that expressed its choice of backend by editing that ConfigMap
    /// would silently lose it — and lose it in the quiet direction, falling
    /// back to node-local pauses that report no error at all. A deployment
    /// selects the backend here instead, where the file cannot overwrite it.
    #[config(default = "local", env = "AENV_PAUSED_REGISTRY_BACKEND")]
    pub backend: PausedRegistryBackendKind,
    /// How often to renew this node's registry leases and re-check its local
    /// paused records against the registry.
    ///
    /// This is how a node finds out that a sandbox it still holds as paused was
    /// resumed somewhere else — nothing tells it, so the only bound on how long
    /// it keeps advertising a sandbox it no longer owns is this interval. It is
    /// also the renewal cadence for `lease_ttl_secs`. Ignored with the `local`
    /// backend, where there is nothing to reconcile against.
    #[config(default = 30u64)]
    pub reconcile_interval_secs: u64,
    /// How long a node's hold on a *parked* sandbox stays valid without
    /// renewal.
    ///
    /// A sandbox that was paused but whose snapshot never reached the
    /// repository can only be brought back by the node holding its local
    /// artifacts. Once that node stops renewing, the cluster gives up waiting
    /// and rebuilds the sandbox elsewhere from the previous snapshot instead —
    /// losing the last pause's work, which is why it waits at all. This is that
    /// wait.
    ///
    /// 🔴 A live sandbox is never handed to another node on this alone. A lapsed
    /// lease only proves the holder cannot reach the database, and a
    /// partitioned node goes on running every sandbox it has; rebuilding one of
    /// those elsewhere would produce two live copies. Live rows are released by
    /// the next process to start on the holder's own machine — the only party
    /// that can prove the previous one is gone — or, for a machine that never
    /// comes back, reclaimed once the sandbox has *also* outlived its own
    /// deadline.
    ///
    /// So this value is a floor on how long the cluster waits before either of
    /// those, never the thing that decides them. Lowering it does not bring a
    /// dead node's sandboxes back sooner than their own timeouts allow.
    #[config(default = 90u64)]
    pub lease_ttl_secs: u64,
}

impl PausedRegistryConfig {
    /// Reconciliation cadence, floored at one second.
    ///
    /// Zero is not merely useless here, it is fatal: a zero-period
    /// `tokio::time::interval` panics, so an operator could take the node down
    /// at startup with a config value.
    pub fn reconcile_interval(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.reconcile_interval_secs.max(1))
    }

    /// Lease length, held to at least three renewal intervals.
    ///
    /// A lease shorter than the cadence that renews it expires on a healthy
    /// node, so a sandbox parked on a node that is doing fine would be rebuilt
    /// elsewhere from an older snapshot for no reason. Three intervals leaves
    /// room for two missed renewals before the cluster concludes a node is
    /// gone.
    pub fn lease_ttl_secs(&self) -> u64 {
        self.lease_ttl_secs
            .max(self.reconcile_interval_secs.max(1).saturating_mul(3))
    }
}

#[cfg(test)]
mod paused_registry_config_tests {
    use super::*;

    fn config(reconcile_interval_secs: u64, lease_ttl_secs: u64) -> PausedRegistryConfig {
        PausedRegistryConfig {
            backend: PausedRegistryBackendKind::Central,
            reconcile_interval_secs,
            lease_ttl_secs,
        }
    }

    /// `tokio::time::interval` panics on a zero period, so an unclamped value
    /// here is a config field that takes the node down at startup.
    #[test]
    fn a_zero_interval_never_reaches_the_timer() {
        assert_eq!(
            config(0, 90).reconcile_interval(),
            std::time::Duration::from_secs(1)
        );
    }

    /// A lease shorter than the cadence renewing it expires on a perfectly
    /// healthy node — which is an invitation to take over a live sandbox, the
    /// exact thing the lease exists to prevent.
    #[test]
    fn a_lease_can_never_be_shorter_than_the_renewal_cadence() {
        assert_eq!(config(60, 10).lease_ttl_secs(), 180);
        assert_eq!(config(0, 0).lease_ttl_secs(), 3);
    }

    /// A lease longer than the floor is the operator's call: it only trades
    /// slower recovery for more tolerance of an unresponsive node.
    #[test]
    fn a_generous_lease_is_left_alone() {
        assert_eq!(config(30, 600).lease_ttl_secs(), 600);
    }

    /// The shipped defaults have to satisfy the same rule, or every deployment
    /// that touches nothing starts out broken.
    #[test]
    fn the_defaults_leave_room_for_two_missed_renewals() {
        let defaults = config(30, 90);

        assert_eq!(defaults.lease_ttl_secs(), 90);
        assert!(defaults.lease_ttl_secs() >= defaults.reconcile_interval().as_secs() * 3);
    }
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

#[derive(Debug, Config, Clone)]
pub struct UblkOverlaybdTomlConfig {
    #[config(default = "$AENV_HOME/overlaybd/overlaybd-global.json")]
    pub global_config_path: PathBuf,
    #[config(default = false)]
    pub read_only: bool,
    /// Runtime upper format for newly materialized writable OverlayBD images.
    /// Existing source uppers keep their own mode. Default: `hybridLogStructured`.
    #[config(default = "hybridLogStructured")]
    pub runtime_upper_mode: UpperMode,
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

impl MemorySnapshotBackgroundDownloadConfig {
    pub(crate) fn to_overlaybd_download_config(&self) -> overlaybd::config::DownloadConfig {
        overlaybd::config::DownloadConfig {
            enable: self.enable,
            delay: self.delay,
            delay_extra: self.delay_extra,
            // Memory-snapshot background download is intentionally unthrottled;
            // OSS/registry image configs may still carry maxMBps and it keeps
            // working there via the shared throttle.
            max_mbps: 0,
            try_cnt: self.try_cnt,
            block_size: self.block_size,
            concurrency: self.concurrency,
            max_inflight_blocks: self.max_inflight_blocks,
            // Not a memory-snapshot knob: the cache scheduler default applies.
            max_concurrent_files: overlaybd::config::DownloadConfig::default().max_concurrent_files,
        }
    }
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
    /// A file holding the same endpoint as `[cluster].scheduler_endpoint`,
    /// re-read once per heartbeat tick while the process runs.
    ///
    /// 🔴 This is what makes changing the heartbeat target free of a
    /// DaemonSet roll. `[cluster].scheduler_endpoint` is read once at process
    /// startup and baked into one gRPC channel that lives for the rest of the
    /// process; today, changing it means editing the DaemonSet's
    /// `AENV_OBSERVABILITY_SCHEDULER_ENDPOINT` and rolling every node — the
    /// serial, hour-long-grace-period roll that
    /// `docs/proposals/2026-08-20-service-decomposition.md`'s phase four
    /// section warns a rollback must not depend on. Point this at a file
    /// mounted from a ConfigMap **without** `subPath` — kubelet only
    /// refreshes non-`subPath` volumes, so a `subPath` mount would silently
    /// never update — and the reporter notices an edit within one heartbeat
    /// interval, with no pod restart.
    ///
    /// Only this field's own consumer, [`crate::observability::reporter`],
    /// reads it. It is not a second way to reach the scheduler for
    /// `[cluster].scheduler_endpoint`'s other consumers (P2P, the paused
    /// sandbox registry's `central` backend, resume placement) — those still
    /// read the static value and still require a restart to change.
    ///
    /// 🔴 Not a union with the static value, unlike
    /// `ApiConfig::control_plane_token_file`'s relationship to
    /// `control_plane_tokens`: a heartbeat can only go to one place, so when
    /// this is set and has been read successfully at least once, it
    /// *overrides* `[cluster].scheduler_endpoint` outright rather than adding
    /// to it. Unset — or set but never yet read successfully (not mounted
    /// yet, briefly unreadable) — falls back to the static value, which is
    /// today's behavior, byte-for-byte, for every deployment that has not
    /// opted into this.
    #[config(
        default = "",
        env = "AENV_OBSERVABILITY_SCHEDULER_ENDPOINT_FILE",
        parse_env = parse_trimmed_string
    )]
    pub scheduler_endpoint_file: String,
}

#[derive(Debug, Config, Clone)]
pub struct ClusterConfig {
    /// Shared gRPC scheduler endpoint for cluster-level services.
    #[config(
        env = "AENV_OBSERVABILITY_SCHEDULER_ENDPOINT",
        parse_env = parse_trimmed_string
    )]
    pub scheduler_endpoint: Option<String>,
    /// Where `--role node` serves the node sandbox service — the gRPC surface
    /// the API half drives a machine through (`crate::node_server`).
    ///
    /// 🔴 A second listener rather than a route on the HTTP port, because the
    /// two have different audiences: this one is spoken to only by the API
    /// half, and a deployment has to be able to expose them differently.
    /// `--role api` and `--role all` never bind it — see `ServerRole` for why
    /// `all` in particular must not.
    #[config(default = "0.0.0.0:8001", env = "AENV_NODE_SERVICE_ADDR")]
    pub node_service_addr: String,
    /// Where `--role api` serves the data plane's wake-up surface
    /// (`crate::api::grpc`).
    ///
    /// Separate from the HTTP port for the same reason as above: the gateway's
    /// cold path is the only caller.
    #[config(default = "0.0.0.0:8002", env = "AENV_API_GRPC_ADDR")]
    pub api_grpc_addr: String,
    /// The port the API half reaches a node's [`node_service_addr`] on.
    ///
    /// 🔴 A port and not an address, because the *host* is not this process's
    /// to choose: it comes from the scheduler, which names a node as
    /// `http://<addr>:<http-port>` (`kubernetes_discovery.go`, and the static
    /// discovery list). That is the node's user-facing HTTP address, and the
    /// node service is on a different port of the same machine — so the API
    /// half substitutes this port into the answer rather than being configured
    /// with a second endpoint list that would have to be kept in step with the
    /// first.
    ///
    /// [`node_service_addr`]: ClusterConfig::node_service_addr
    #[config(default = 8001u16, env = "AENV_NODE_SERVICE_PORT")]
    pub node_service_port: u16,
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
    /// Hard ceiling on how long a sandbox may **run** in total, summed across
    /// every resume. A request asking for more is clamped, not refused, and a
    /// later SetTimeout can never push the deadline past it.
    ///
    /// 🔴 Running time, not wall-clock time since creation: a sandbox spends
    /// this budget only while it is not paused. A sandbox paused for a week
    /// comes back with the budget it went away with, which is what makes a
    /// resume after the ceiling has elapsed a working sandbox instead of one
    /// the eviction loop tears down within the second.
    ///
    /// 🔴 `0` means "no ceiling", and with it no derivable routing-projection
    /// TTL: the node then reports `projection_ttl_secs = 0` and the scheduler
    /// falls back to its own `binding_ttl`, which is exactly the behaviour that
    /// shipped before this knob existed.
    #[config(default = 86400u64, env = "AENV_MAX_SANDBOX_LIFETIME_SECS")]
    pub max_sandbox_lifetime_secs: u64,
    /// Slack added to the routing projection's TTL so the record outlives the
    /// sandbox it points at rather than expiring just before it.
    #[config(default = 60u64, env = "AENV_PROJECTION_TTL_GRACE_SECS")]
    pub projection_ttl_grace_secs: u64,
    #[config(
        default = "$AENV_HOME/persisted-sandboxes",
        env = "AENV_PERSISTED_SANDBOX_STORE_PATH",
        parse_env = parse_required_path
    )]
    pub persisted_sandbox_store_path: PathBuf,
    /// Whether this process sweeps the host at startup for what a previous
    /// process on this machine left behind: leftover Firecracker VMMs and the
    /// work directories they were running in. See `crate::node_reclaim`.
    ///
    /// 🔴 Three states, and the unset one is not "off". Unset means the role
    /// decides — `--role node` sweeps, `--role all` does not — because the
    /// sweep is only sound while "the previous process on this machine is
    /// gone" holds, and that is a property of the deployment rather than of
    /// the code. A DaemonSet with `maxSurge: 0` guarantees it; a developer's
    /// laptop running a second server alongside the first does not, and a
    /// sweep there would kill the other one's VMs.
    #[config(env = "AENV_STARTUP_RECLAIM_ENABLED")]
    pub startup_reclaim_enabled: Option<bool>,
    /// How long shutdown waits, after isolating the node, before it starts
    /// tearing sandboxes down.
    ///
    /// The pause exists so the scheduler learns this node is out of rotation
    /// while the node can still serve — otherwise a sandbox placed in the last
    /// moments before shutdown is created only to be paused again. Two
    /// heartbeat intervals is enough for the report to land and be applied.
    /// Zero disables the wait, which is what tests and local runs want.
    #[config(default = 10u64, env = "AENV_SHUTDOWN_DRAIN_PROPAGATION_SECS")]
    pub shutdown_drain_propagation_secs: u64,
    #[config(nested)]
    pub paused_registry: PausedRegistryConfig,
    #[config(nested)]
    pub store: OrchestratorStoreConfig,
}

/// Where a process's orchestrator keeps its active-state records.
#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MetadataStoreBackendKind {
    /// This process's own ledger, lost when it exits.
    ///
    /// 🔴 Correct for a machine-local role and *only* for one. A node's records
    /// describe the sandboxes on that machine, and a machine that is gone has
    /// no sandboxes; an API replica's records describe sandboxes on other
    /// machines, and a replica that used this would hold an opinion about them
    /// that no other replica shared.
    InMemory,
    /// The cluster's shared store, so every API replica reads and writes one
    /// ledger.
    Redis,
}

impl MetadataStoreBackendKind {
    /// The name a deployment writes into `AENV_ORCHESTRATOR_STORE_BACKEND`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::InMemory => "in_memory",
            Self::Redis => "redis",
        }
    }
}

#[derive(Debug, Config, Clone)]
pub struct OrchestratorStoreConfig {
    /// 🔴 Settable from the environment for the same reason
    /// [`PausedRegistryConfig::backend`] is: `deploy/k8s/run.sh` overwrites the
    /// deployed `config/default.toml` on every apply, so a store selected by
    /// editing that ConfigMap would silently revert — and revert to the
    /// per-process ledger, which reports nothing and simply forgets other
    /// replicas' sandboxes.
    ///
    /// The default is what `--role all` and `--role node` want. `--role api`
    /// refuses to start with it rather than starting a replica whose ledger
    /// nobody else can see.
    #[config(default = "in_memory", env = "AENV_ORCHESTRATOR_STORE_BACKEND")]
    pub backend: MetadataStoreBackendKind,
    /// `redis://host:port[/db]`, read only when `backend = "redis"`.
    #[config(
        default = "redis://127.0.0.1:6379",
        env = "AENV_ORCHESTRATOR_STORE_REDIS_URL",
        parse_env = parse_trimmed_string
    )]
    pub redis_url: String,
    /// Prefix for every key the Redis store owns.
    ///
    /// 🔴 Must not overlap `agentenv:scheduler:bindings:*`, which is the
    /// routing projection and belongs to a different subsystem with a
    /// different lifetime. `RedisStoreConfig::validate` refuses an overlapping
    /// value at startup rather than letting two owners share a keyspace.
    #[config(
        default = "agentenv:api",
        env = "AENV_ORCHESTRATOR_STORE_KEY_PREFIX",
        parse_env = parse_trimmed_string
    )]
    pub redis_key_prefix: String,
    /// Whether contended record updates queue behind a distributed lock or
    /// fail fast.
    ///
    /// 🔴 A throughput switch, not a correctness switch: with it off, a
    /// contended `update_if_state` answers `ConcurrentUpdate` instead of
    /// waiting, and nothing is corrupted either way. It is exposed because it
    /// is the one knob whose right value depends on the deployment's replica
    /// count rather than on the code.
    #[config(
        default = true,
        env = "AENV_ORCHESTRATOR_STORE_DISTRIBUTED_LOCK_ENABLED"
    )]
    pub redis_distributed_lock_enabled: bool,
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

#[derive(Debug, Config, Clone)]
pub struct P2pConfig {
    #[config(default = false)]
    pub enabled: bool,
    #[config(default = "iroh")]
    pub transport: crate::p2p::P2pTransportKind,
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
    NodeIdentityConfig,
    OrchestratorConfig,
    P2pConfig,
    CustomExtensionConfig,
);

impl AppConfig {
    fn manifest_firecracker(&self) -> &ManifestDownload {
        SetupDependencyManifest::get()
            .firecracker
            .for_mode(self.virtualization_mode)
    }

    fn manifest_kernel(&self) -> &ManifestDownload {
        SetupDependencyManifest::get()
            .kernel
            .for_mode(self.virtualization_mode)
    }

    pub fn resolved_firecracker_binary_path(&self) -> PathBuf {
        self.firecracker.binary_path.clone().unwrap_or_else(|| {
            let version = self
                .firecracker
                .version
                .as_deref()
                .unwrap_or(&self.manifest_firecracker().version);
            self.deps_path
                .join("firecracker")
                .join(version)
                .join("firecracker")
        })
    }

    pub fn resolved_kernel_image_path(&self) -> PathBuf {
        self.kernel.image_path.clone().unwrap_or_else(|| {
            let version = self
                .kernel
                .version
                .as_deref()
                .unwrap_or(&self.manifest_kernel().version);
            self.deps_path
                .join("kernel")
                .join(version)
                .join("vmlinux.bin")
        })
    }

    pub fn resolved_tools_version(&self) -> &str {
        self.tools
            .version
            .as_deref()
            .unwrap_or(&SetupDependencyManifest::get().tools.version)
    }

    pub fn resolved_tools_drive_path_for_version(&self, version: &str) -> Result<PathBuf> {
        let version = semver::Version::parse(version).with_context(|| {
            format!(
                "invalid tools drive version '{version}': expected SemVer without build metadata"
            )
        })?;
        if !version.build.is_empty() {
            bail!("invalid tools drive version '{version}': build metadata is not supported");
        }
        Ok(self
            .deps_path
            .join("tools")
            .join(version.to_string())
            .join("tools.ext4"))
    }

    pub fn resolved_tools_drive_path(&self) -> Result<PathBuf> {
        self.resolved_tools_drive_path_for_version(self.resolved_tools_version())
    }

    pub(crate) fn resolved_overlaybd_oci_converter_id(&self) -> String {
        let version = self
            .overlaybd
            .as_ref()
            .map(|overlaybd| overlaybd.version.as_str())
            .unwrap_or(&SetupDependencyManifest::get().overlaybd.version);
        format!("overlaybd-oci:{version}:agentenv-cache-v1")
    }

    /// Path of the generated overlaybd global config dedicated to the offline
    /// C++ conversion tools (`overlaybd-apply`). It mirrors the runtime global
    /// config but points at an isolated cacheDir: the C++ file cache manages
    /// cacheDir as flat files and evicts (truncate+unlink) whatever it finds,
    /// which would destroy the Rust runtime cache's per-entry directories if
    /// both shared `remote-blocks`.
    pub(crate) fn resolved_overlaybd_convert_global_config_path(&self) -> PathBuf {
        self.ublk
            .overlaybd
            .global_config_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join("convert-overlaybd-global.json")
    }

    /// Path of the generated overlaybd global config dedicated to the offline
    /// C++ resize tool (`overlaybd-resize`). It mirrors the runtime global
    /// config but points at an isolated cacheDir: the C++ file cache manages
    /// cacheDir as flat files and evicts (truncate+unlink) whatever it finds,
    /// which would destroy the Rust runtime cache's per-entry directories if
    /// both shared `remote-blocks`.
    pub(crate) fn resolved_overlaybd_resize_global_config_path(&self) -> PathBuf {
        self.ublk
            .overlaybd
            .global_config_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join("resize-overlaybd-global.json")
    }

    /// Resolve the cpu-template-helper binary path derived from deps_path + version.
    /// Returns `None` if the binary does not exist on disk.
    pub fn resolved_cpu_template_helper(&self) -> Option<PathBuf> {
        let version = self
            .firecracker
            .version
            .as_deref()
            .unwrap_or(&self.manifest_firecracker().version);
        let path = self
            .deps_path
            .join("firecracker")
            .join(version)
            .join("cpu-template-helper");
        path.exists().then_some(path)
    }

    pub fn resolved_regctl_binary(&self) -> PathBuf {
        regctl_path(&self.deps_path)
    }

    pub(crate) fn image_cache_layout(&self) -> ResolvedImageCacheConfig {
        self.image.cache.layout()
    }

    pub fn network_pool_config(&self) -> warm_pool::PoolConfig {
        let pool = &self.pool.network;

        warm_pool::PoolConfig {
            low_watermark: self.pool.low_watermark,
            high_watermark: self.pool.high_watermark,
            maintenance_enabled: pool.enabled && pool.maintenance_enabled,
            startup_prewarm: pool.startup_prewarm,
        }
    }

    pub fn block_pool_config(&self) -> Option<warm_pool::PoolConfig> {
        let pool = &self.pool.block;

        if !pool.enabled {
            return None;
        }

        Some(warm_pool::PoolConfig {
            low_watermark: self.pool.low_watermark,
            high_watermark: self.pool.high_watermark,
            // The ublk daemon uses async request-time refill because the
            // reusable device shape is image/size dependent.
            maintenance_enabled: false,
            startup_prewarm: pool.startup_prewarm,
        })
    }

    pub fn firecracker_pool_config(&self) -> Option<ResolvedFirecrackerPoolConfig> {
        let pool = &self.pool.firecracker;

        if !pool.enabled {
            return None;
        }

        Some(ResolvedFirecrackerPoolConfig {
            pool: warm_pool::PoolConfig {
                low_watermark: self.pool.low_watermark,
                high_watermark: self.pool.high_watermark,
                maintenance_enabled: pool.maintenance_enabled,
                startup_prewarm: pool.startup_prewarm,
            },
            fill_concurrency: pool.fill_concurrency,
        })
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
        self.snapshot.catalog.mirror_backlog_path = resolve_path(
            &self.home_path,
            config_dir,
            &self.snapshot.catalog.mirror_backlog_path,
        );

        if self.snapshot.repository_backend == SnapshotRepositoryBackendKind::PosixFs {
            let posix_fs = self
                .backend
                .posix_fs
                .get_or_insert_with(PosixFsBackendConfig::default);
            posix_fs.snapshot_store =
                resolve_path(&self.home_path, config_dir, &posix_fs.snapshot_store);
            // 🔴 `posix_fs` is also `repository_backend`'s own `#[config(default
            // = "posix_fs", ...)]`, so this branch cannot tell "posix_fs was
            // chosen" from "nothing chose anything" — confique has already
            // collapsed that distinction by the time `normalize` runs, and
            // recovering it would mean this field stops being a plain enum with
            // a default. What is still true either way, and worth saying either
            // way, is *where this process is about to look for snapshots*: on a
            // machine where `$AENV_HOME` is a real, persistent directory this is
            // a normal, working default; in a container where it is an emptyDir
            // — every Kubernetes Pod this binary runs in — it is silently a
            // brand-new, empty store on every restart, and the only visible
            // effect is downstream reads answering "0 rows" or "no such
            // snapshot" in a way that reads exactly like a genuine catalog
            // inconsistency rather than like a missing `oss` config. That
            // confusion has already cost real debugging time on a real cluster
            // once. Logged here, once, at the one place both paths (default and
            // explicit) are guaranteed to pass through.
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

    fn validate(&self) -> Result<()> {
        self.validate_pool_config()?;
        self.image.cache.gc.validate()?;
        NetworkConfig::validate(&self.network)?;
        if self.ublk.overlaybd.resize_timeout_secs == 0 {
            bail!("invalid ublk.overlaybd config: resize_timeout_secs must be > 0");
        }
        self.validate_memory_snapshot_options()?;
        self.validate_memory_snapshot_background_download()?;
        self.validate_overlaybd_global_config_paths()?;
        self.validate_disk_rate_limit()?;
        self.validate_snapshot_catalog()?;
        if self.snapshot.catalog.build_heartbeat_interval_secs == 0 {
            bail!(
                "snapshot.catalog.build_heartbeat_interval_secs must be > 0; a build that never \
                 says it is alive is ended by the catalog's reaper while it is still running"
            );
        }
        Ok(())
    }

    /// The legal (write, read) pairs, and why the rest are not.
    ///
    /// 🔴 An illegal pair fails startup rather than being corrected. Both
    /// mistakes it rules out are quiet ones: reading a store nobody writes
    /// answers "no such snapshot" for everything written since the switch, and
    /// reading an empty table answers it for everything, full stop. Neither
    /// reports an error — they report absence, and callers delete artifacts and
    /// refuse resumes on absence.
    fn validate_snapshot_catalog(&self) -> Result<()> {
        let catalog = &self.snapshot.catalog;
        match (catalog.write, catalog.read) {
            (SnapshotCatalogWrite::ObjectStore, SnapshotCatalogRead::ObjectStore) => Ok(()),
            (SnapshotCatalogWrite::Both, SnapshotCatalogRead::ObjectStore) => Ok(()),
            (SnapshotCatalogWrite::ObjectStore, SnapshotCatalogRead::Postgres) => bail!(
                "snapshot.catalog: write = \"object_store\" with read = \"postgres\" reads a table \
                 nothing writes. Set write = \"both\" first and let the mirror catch up."
            ),
            (SnapshotCatalogWrite::Postgres, SnapshotCatalogRead::ObjectStore) => bail!(
                "snapshot.catalog: write = \"postgres\" with read = \"object_store\" reads a store \
                 nothing writes any more, so every snapshot published since the switch reads as \
                 absent. Set read = \"postgres\" in the same change."
            ),
            // 🔴 The read switch, and passing here is not the switch being
            // allowed — it is only this layer having nothing left to say about
            // it. Four more refusals sit below, and every one of them is about
            // whether PostgreSQL actually holds what object storage does:
            //
            //   1. Owed writes — `mirror_lag{direction="central"}`.
            //   2. Disagreements no replay can settle —
            //      `mirror_diverged{direction="central"}`. Until build
            //      admission is wired that is every template, because
            //      `try_start_build` writes object storage alone. A cluster
            //      holding templates reads a mirror lag of zero and would
            //      otherwise be waved through into making all of them vanish
            //      from the API.
            //   3. The snapshots that predate the double write. Neither number
            //      says anything about those — both count writes the mirror
            //      *saw* — so on the cluster this was measured on, the instant
            //      `write = "both"` was switched on read `lag = 0, diverged =
            //      0` over thirty-two snapshots PostgreSQL had never heard of.
            //      `MirrorBacklog::queue_history_toward_central` puts them into
            //      the queue at startup and `guard_read_side` refuses the
            //      switch until it has run.
            //   4. 🔴 And because all three of those describe the *queue*
            //      rather than the two catalogs, a direct comparison of what
            //      each one holds runs beside them —
            //      `compare_catalog_populations`. It is the only one of the
            //      four that needs no marker and no gauge to be right.
            (SnapshotCatalogWrite::Both, SnapshotCatalogRead::Postgres) => Ok(()),
            (SnapshotCatalogWrite::Postgres, SnapshotCatalogRead::Postgres) => bail!(
                "snapshot.catalog: write = \"postgres\" drops the object-store copy, which is the \
                 only way back from the central catalog. It is allowed once the read side has \
                 been served from PostgreSQL for an observation period and the mirror lag has \
                 been 0 throughout; it is not allowed in this build. Set write = \"both\"."
            ),
        }
    }

    /// Reject internally inconsistent or out-of-range disk rate limit configs so
    /// operator mistakes fail at load time. Disabled sections are skipped: both
    /// the fresh-boot and snapshot-resume paths ignore all configured values when
    /// disabled, so dormant/pre-staged values must not block startup.
    ///
    /// When enabled: a one-time burst is meaningless without a nonzero sustained
    /// limit (`build_disk_rate_limiter` only creates a bucket when the sustained
    /// value is > 0, so a burst paired with a zero sustained limit is silently
    /// ignored), and every value must fit Firecracker's signed `i64` token-bucket
    /// fields (the consumer converts with `i64::try_from`, so an out-of-range
    /// value would otherwise only fail later at sandbox start).
    fn validate_disk_rate_limit(&self) -> Result<()> {
        let cfg = &self.machine.disk_rate_limit;
        if !cfg.enabled {
            return Ok(());
        }
        if cfg.bandwidth_burst_bytes > 0 && cfg.bandwidth_bytes_per_sec == 0 {
            bail!(
                "machine.disk_rate_limit: bandwidth_burst_bytes is set but \
                 bandwidth_bytes_per_sec is 0; a burst requires a nonzero sustained limit"
            );
        }
        if cfg.iops_burst > 0 && cfg.iops == 0 {
            bail!(
                "machine.disk_rate_limit: iops_burst is set but iops is 0; \
                 a burst requires a nonzero sustained limit"
            );
        }
        for (name, value) in [
            ("bandwidth_bytes_per_sec", cfg.bandwidth_bytes_per_sec),
            ("bandwidth_burst_bytes", cfg.bandwidth_burst_bytes),
            ("iops", cfg.iops),
            ("iops_burst", cfg.iops_burst),
        ] {
            if value > i64::MAX as u64 {
                bail!(
                    "machine.disk_rate_limit.{name} ({value}) exceeds the maximum \
                     supported value {}",
                    i64::MAX
                );
            }
        }
        Ok(())
    }

    fn validate_memory_snapshot_options(&self) -> Result<()> {
        let memory = &self.memory_snapshot;
        if !memory.track_dirty_pages {
            return Ok(());
        }
        if self.virtualization_mode == VirtualizationMode::Pvm {
            bail!(
                "memory_snapshot.track_dirty_pages=true is disabled in PVM mode because this combination has not been tested"
            );
        }
        Ok(())
    }

    /// Sanity-bound the memory-snapshot background download knobs so a legal
    /// config cannot allocate unbounded scratch or fan out unbounded requests.
    /// Peak scratch per active layer download is `block_size × concurrency`
    /// (the download chunk is `block_size` cache blocks fetched per request),
    /// and `max_inflight_blocks × block_size` node-wide.
    fn validate_memory_snapshot_background_download(&self) -> Result<()> {
        const MAX_BLOCK_SIZE: u64 = 64 * 1024 * 1024;
        const MAX_CONCURRENCY: usize = 16;
        const MAX_SCRATCH_BYTES: u64 = 256 * 1024 * 1024;
        let cfg = &self.memory_snapshot.background_download;
        if cfg.concurrency == 0 {
            bail!("memory_snapshot.background_download.concurrency must be > 0");
        }
        if cfg.concurrency > MAX_CONCURRENCY {
            bail!("memory_snapshot.background_download.concurrency must be <= {MAX_CONCURRENCY}");
        }
        if u64::from(cfg.block_size) > MAX_BLOCK_SIZE {
            bail!("memory_snapshot.background_download.block_size must be <= {MAX_BLOCK_SIZE}");
        }
        if u64::from(cfg.block_size) * cfg.concurrency as u64 > MAX_SCRATCH_BYTES {
            bail!(
                "memory_snapshot.background_download block_size * concurrency must be <= \
                 {MAX_SCRATCH_BYTES} bytes"
            );
        }
        const MAX_INFLIGHT_BLOCKS: usize = 128;
        if cfg.max_inflight_blocks == 0 {
            bail!("memory_snapshot.background_download.max_inflight_blocks must be > 0");
        }
        if cfg.max_inflight_blocks > MAX_INFLIGHT_BLOCKS {
            bail!(
                "memory_snapshot.background_download.max_inflight_blocks must be <= \
                 {MAX_INFLIGHT_BLOCKS}"
            );
        }
        // Backend-wide scratch budget: every in-flight chunk may allocate one
        // block_size buffer, so the global product must stay bounded too.
        const MAX_GLOBAL_SCRATCH_BYTES: u64 = 2 * 1024 * 1024 * 1024;
        let global_scratch = u64::from(cfg.block_size)
            .checked_mul(cfg.max_inflight_blocks as u64)
            .ok_or_else(|| {
                anyhow::anyhow!("memory_snapshot.background_download scratch overflow")
            })?;
        if global_scratch > MAX_GLOBAL_SCRATCH_BYTES {
            bail!(
                "memory_snapshot.background_download block_size * max_inflight_blocks must be <= \
                 {MAX_GLOBAL_SCRATCH_BYTES} bytes"
            );
        }
        if cfg.delay < 0 {
            bail!("memory_snapshot.background_download.delay must be >= 0");
        }
        if cfg.try_cnt < 1 {
            bail!("memory_snapshot.background_download.try_cnt must be >= 1");
        }
        Ok(())
    }

    pub(crate) fn validate_overlaybd_global_config_paths(&self) -> Result<()> {
        let paths = [
            (
                "ublk.overlaybd.global_config_path",
                &self.ublk.overlaybd.global_config_path,
            ),
            (
                "memory_snapshot.overlaybd_global_config_path",
                &self.memory_snapshot.overlaybd_global_config_path,
            ),
            (
                "derived convert overlaybd global config path",
                &self.resolved_overlaybd_convert_global_config_path(),
            ),
            (
                "derived resize overlaybd global config path",
                &self.resolved_overlaybd_resize_global_config_path(),
            ),
        ];
        let normalized =
            paths.map(|(name, path)| (name, overlaybd::config::lexically_normalize_path(path)));
        let canonical = normalized
            .clone()
            .map(|(name, path)| (name, std::fs::canonicalize(&path).ok()));

        for left in 0..normalized.len() {
            for right in (left + 1)..normalized.len() {
                let lexical_alias = normalized[left].1 == normalized[right].1;
                let canonical_alias =
                    canonical[left].1.is_some() && canonical[left].1 == canonical[right].1;
                if lexical_alias || canonical_alias {
                    bail!(
                        "{} ({:?}) and {} ({:?}) must be different",
                        normalized[left].0,
                        normalized[left].1,
                        normalized[right].0,
                        normalized[right].1,
                    );
                }
            }
        }
        Ok(())
    }

    fn validate_pool_config(&self) -> Result<()> {
        let network = self.network_pool_config();
        if network.maintenance_enabled {
            PoolTomlConfig::validate("network", &network)?;
        }
        if let Some(block) = self.block_pool_config() {
            PoolTomlConfig::validate("block", &block)?;
        }
        if let Some(firecracker) = self.firecracker_pool_config() {
            PoolTomlConfig::validate("firecracker", &firecracker.pool)?;
            if firecracker.fill_concurrency == 0 {
                bail!("invalid firecracker pool config: fill_concurrency must be > 0");
            }
        }
        Ok(())
    }
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

        #[cfg(test)]
        {
            Self::init_global().expect("test ConfigManager initialization failed")
        }

        #[cfg(not(test))]
        {
            GLOBAL_CONFIG_MANAGER
                .get()
                .expect("ConfigManager must be initialized before use")
        }
    }

    pub fn init_global() -> Result<&'static Self> {
        if let Some(manager) = GLOBAL_CONFIG_MANAGER.get() {
            return Ok(manager);
        }
        Self::set_global(Self::new()?)
    }

    pub fn init_global_from_path(path: &Path) -> Result<&'static Self> {
        if let Some(manager) = GLOBAL_CONFIG_MANAGER.get() {
            return Ok(manager);
        }
        Self::set_global(Self::new_from_path(path)?)
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

    pub fn new() -> Result<Self> {
        let config_path = Self::env_path(ENV_CONFIG_PATH).unwrap_or_else(Self::default_config_path);
        let config = Self::load_config_file(&config_path)?;
        Ok(Self {
            config,
            config_path: Some(config_path),
        })
    }

    pub fn new_from_path(path: &Path) -> Result<Self> {
        let config = Self::load_config_file(path)?;
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

    /// The global config if one has been loaded, and `None` otherwise.
    ///
    /// 🔴 For the handful of callers that must not take a process down by
    /// asking. [`Self::global`] panics outside the crate's own tests when
    /// nothing initialised it, which is right for the server's own paths — a
    /// node running on defaults nobody chose is worse than one that will not
    /// start — but wrong for a value that has a perfectly good fallback of its
    /// own, such as this machine's name.
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

    fn load_config_file(path: &Path) -> Result<AppConfig> {
        Self::load_config_file_with_overlays(path, &Self::overlay_paths_from_env())
    }

    /// The overlay files named by [`ENV_CONFIG_OVERLAY_PATH`], in the order
    /// they are applied — left to right, each one layered over what came
    /// before it.
    ///
    /// 🔴 Empty segments are dropped rather than refused, and that is what
    /// makes the switch flippable from a manifest. A Deployment writes
    /// `$(A):$(B)` and turns one half off by clearing the ConfigMap key behind
    /// it; if an empty segment were an error, or were read as the current
    /// directory, turning half of it off would take a manifest edit instead of
    /// a `kubectl set env`. A value that is nothing but separators is
    /// therefore the same as unset, which is the same rule
    /// [`Self::env_path`] applies to `AENV_CONFIG_PATH` itself.
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

    /// 🔴 With no overlay this is confique's `.env().file(path)` and nothing
    /// else — the same two lines it has always been. That branch is the
    /// promise every existing deployment is owed: a tree that grew this
    /// mechanism must parse identically on a cluster that never sets the
    /// variable, and `no_overlay_is_byte_for_byte_the_old_load` compares a
    /// full `{:#?}` of the loaded config to prove it rather than asserting it.
    ///
    /// With overlays the main file stops being a confique source and becomes
    /// the bottom of a TOML document that is merged in this process first —
    /// see [`overlaid_config_layer`] for why the merge cannot be left to
    /// confique's own layering.
    fn load_config_file_with_overlays(path: &Path, overlays: &[PathBuf]) -> Result<AppConfig> {
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
        config.validate()?;

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

/// The main config file and its overlays, merged into one confique layer.
///
/// # Merge semantics
///
/// One deep, key-by-key merge of TOML tables: the main file first, then each
/// overlay in `AENV_CONFIG_OVERLAY_PATH` order. Where both sides hold a table
/// the two are merged; anywhere else — a scalar, a string, an array — the
/// later file replaces the earlier value whole. There is no way to *remove* a
/// key, only to give it another value.
///
/// 🔴 The merge is done here rather than by handing confique a second `.file()`
/// source, and the difference is the entire reason this function exists.
/// confique merges *layers*, and a layer mirrors the config struct: it descends
/// into a section only where the field is `#[config(nested)]`. `[backend.oss]`
/// is `Option<OssBackendConfig>` — one serde value — so under confique's own
/// layering a second file that mentioned `[backend.oss]` at all would replace
/// the section entire, and the endpoint and the bucket would have to be in the
/// same file as the credentials. They must not be: the credentials come from a
/// Secret and the endpoint and bucket are ordinary deployment facts that belong
/// in this repository where they can be read and reviewed. Merging the
/// documents before either becomes a layer is what lets one `[backend.oss]`
/// section be assembled out of a tracked ConfigMap file and a mounted Secret.
///
/// A consequence worth stating: the main file is merged the same way, so
/// `AENV_CONFIG_PATH` reaches confique through
/// `toml::Table` -> `Layer` here instead of through confique's own file source.
/// `overlay_merge_leaves_the_main_file_alone` loads the bundled config both
/// ways and compares the whole of the result.
///
/// # What is *not* merged here
///
/// The environment. It stays confique's top layer, above everything this
/// function produces, exactly as it was above `.file(path)` before. An operator
/// who reaches for `kubectl set env` in an incident still wins over every file
/// on the node.
fn overlaid_config_layer(
    path: &Path,
    overlays: &[PathBuf],
) -> Result<<AppConfig as Config>::Layer> {
    // Missing is empty, matching what confique's optional file source does with
    // `AENV_CONFIG_PATH`. Adding an overlay must not also change what happens
    // when the *main* file is absent.
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

/// Reads one TOML document as a table.
///
/// 🔴 `required` is the difference between the main config file and an
/// overlay, and a named overlay that is not on disk stops the process.
///
/// That is the opposite of what confique's own file source does, and it is
/// deliberate. confique treats a missing file as an empty layer, which is
/// right for "the operator may or may not have written a config" and wrong for
/// every reason [`ENV_CONFIG_OVERLAY_PATH`] is ever set: the file is a mounted
/// Secret carrying the object-storage endpoint and credentials, and a node
/// that quietly started without it falls back to `posix_fs` — a backend that
/// *works*. It starts, serves an empty local snapshot store, logs nothing
/// above `info`, and the catalog goes on naming artifacts the process can no
/// longer fetch. Naming a file is a statement that it is there.
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

impl PoolTomlConfig {
    fn validate(name: &str, pool: &warm_pool::PoolConfig) -> Result<()> {
        if pool.low_watermark > pool.high_watermark {
            bail!(
                "invalid {name} pool config: low_watermark ({}) must be <= high_watermark ({})",
                pool.low_watermark,
                pool.high_watermark
            );
        }

        Ok(())
    }
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
        ConfigManager::new_from_path(&workspace.join("config/default.toml"))?;
        Ok(())
    }

    /// 🔴 The section has to be in the file, not merely in the code defaults.
    /// `deploy/k8s/run.sh` copies this file over the cluster ConfigMap on every
    /// apply, so a cluster whose registry settings live only in that ConfigMap
    /// loses them on the next `make k8s-apply` — falling back to node-local
    /// pauses without reporting anything.
    #[test]
    fn the_bundled_default_config_documents_the_paused_registry() {
        let text = std::fs::read_to_string(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("config/default.toml"),
        )
        .expect("read the bundled config");
        let parsed: toml::Value = toml::from_str(&text)
            .map_err(|err| err.message().to_string())
            .expect("parse the bundled config");

        let section = parsed
            .get("orchestrator")
            .and_then(|orchestrator| orchestrator.get("paused_registry"))
            .and_then(toml::Value::as_table)
            .expect("config/default.toml must carry an [orchestrator.paused_registry] section");

        for key in ["backend", "reconcile_interval_secs", "lease_ttl_secs"] {
            assert!(
                section.contains_key(key),
                "[orchestrator.paused_registry] is missing {key}"
            );
        }

        // 🔴 Neither key may come back. The node does not connect to the
        // registry database at all any more, and a config that still offers a
        // DSN and a connection budget is a config that reads as though it
        // could — on the machines that run user code, which is the whole
        // reason the connection moved.
        for key in ["dsn", "max_connections"] {
            assert!(
                !section.contains_key(key),
                "[orchestrator.paused_registry] still carries {key}: the node holds no database \
                 connection of its own"
            );
        }
    }

    /// `[pg].dsn` is a credential and `config/default.toml` is copied
    /// verbatim over the cluster ConfigMap on every apply
    /// (`deploy/k8s/run.sh`), so a real DSN committed here would ship a
    /// database password into a checkout and into every apply of it.
    ///
    /// An absent `[pg]` table is fine (`dsn` reads back as `None` either
    /// way); what this refuses is a `dsn` key with a non-blank value.
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

    /// The backend a deployment actually runs has to be settable from outside
    /// the file, for the same reason: the file is overwritten on every apply.
    /// And what is settable has to be exactly what is accepted — a value that
    /// is neither accepted nor refused is the silent fallback this override
    /// exists to remove.
    ///
    /// Touches a process-global environment variable, which nothing else in
    /// this crate reads or writes. Both halves live in one test so that stays
    /// true: two tests setting it would race each other under the default
    /// parallel runner.
    #[test]
    fn the_paused_registry_backend_is_settable_from_the_environment() {
        let _env = env_guard();
        let workspace = Path::new(env!("CARGO_MANIFEST_DIR"));

        // Every backend has to be reachable this way, or the one that is not
        // can only be selected by editing a file that the next apply
        // overwrites — which is the failure this override exists to remove.
        for (value, expected) in [
            ("postgres", PausedRegistryBackendKind::Postgres),
            ("central", PausedRegistryBackendKind::Central),
            ("local", PausedRegistryBackendKind::Local),
        ] {
            std::env::set_var("AENV_PAUSED_REGISTRY_BACKEND", value);
            let overridden = ConfigManager::new_from_path(&workspace.join("config/default.toml"));
            std::env::remove_var("AENV_PAUSED_REGISTRY_BACKEND");

            assert_eq!(
                overridden
                    .unwrap_or_else(|err| panic!("load with backend={value}: {err}"))
                    .config()
                    .orchestrator
                    .paused_registry
                    .backend,
                expected
            );

            // The assembly log names the backend with `as_str`, and it is read
            // by whoever just set this variable and wants to know whether it
            // took. A name the log prints but the environment will not accept
            // is a name that cannot be checked against anything.
            assert_eq!(
                expected.as_str(),
                value,
                "the logged name and the accepted value must be the same word"
            );
        }

        // 🔴 A value nothing recognises stops the node instead of leaving it on
        // `local`. This override exists because the backend cannot be chosen in
        // the ConfigMap the next apply overwrites — and that is worth nothing
        // if a typo in the replacement is answered by node-local pauses and no
        // error at all. Startup is where the mistake is still cheap; past it,
        // it surfaces when a node is lost and its sandboxes turn out to have
        // gone with it.
        for typo in ["postgress", "Central", "postgres ", "node-local"] {
            std::env::set_var("AENV_PAUSED_REGISTRY_BACKEND", typo);
            let loaded = ConfigManager::new_from_path(&workspace.join("config/default.toml"));
            std::env::remove_var("AENV_PAUSED_REGISTRY_BACKEND");

            assert!(
                loaded.is_err(),
                "backend={typo:?} was accepted; a misspelled backend must not \
                 silently leave the node on `local`"
            );
        }

        assert_eq!(
            ConfigManager::new_from_path(&workspace.join("config/default.toml"))
                .expect("load without the override")
                .config()
                .orchestrator
                .paused_registry
                .backend,
            PausedRegistryBackendKind::Local,
            "the file's value must stand when the environment says nothing"
        );
    }

    /// The repository backend a deployment actually runs has to be settable
    /// from outside the file, for the same reason the paused registry's is: the
    /// file is overwritten on every apply, and losing this one is silent.
    ///
    /// 🔴 The two halves are each other's control and live in one test on
    /// purpose. "The environment set it to `oss`" proves nothing on its own —
    /// a loader that ignored the variable and a config that already said `oss`
    /// are the same observation. What makes it evidence is that the *same*
    /// file, read in the same test, answers `posix_fs` when the variable is
    /// unset. Two tests could not say that: the environment variable is
    /// process-global and nothing else in this crate touches it, so a second
    /// test setting it would race this one under the default parallel runner.
    #[test]
    fn the_snapshot_repository_backend_is_settable_from_the_environment() {
        let _env = env_guard();
        let workspace = Path::new(env!("CARGO_MANIFEST_DIR"));
        let bundled = workspace.join("config/default.toml");

        // The control. This is the value the repository ships and the value
        // every `make k8s-apply` puts back into the cluster ConfigMap.
        assert_eq!(
            ConfigManager::new_from_path(&bundled)
                .expect("load without the override")
                .config()
                .snapshot
                .repository_backend,
            SnapshotRepositoryBackendKind::PosixFs,
            "the file's value must stand when the environment says nothing"
        );

        // Every backend has to be reachable this way, or the one that is not
        // can only be selected by editing the file the next apply overwrites.
        for (value, expected) in [
            ("oss", SnapshotRepositoryBackendKind::Oss),
            ("posix_fs", SnapshotRepositoryBackendKind::PosixFs),
        ] {
            std::env::set_var("AENV_SNAPSHOT_REPOSITORY_BACKEND", value);
            let overridden = ConfigManager::new_from_path(&bundled);
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

        // 🔴 A value nothing recognises stops the node instead of leaving it on
        // `posix_fs`. The override exists because the backend cannot be chosen
        // in the ConfigMap the next apply overwrites, and that is worth nothing
        // if a typo in the replacement is answered by a node that starts
        // serving an empty local filesystem and says so nowhere.
        for typo in ["OSS", "oss ", "s3", "object_storage", "posixfs"] {
            std::env::set_var("AENV_SNAPSHOT_REPOSITORY_BACKEND", typo);
            let loaded = ConfigManager::new_from_path(&bundled);
            std::env::remove_var("AENV_SNAPSHOT_REPOSITORY_BACKEND");

            assert!(
                loaded.is_err(),
                "backend={typo:?} was accepted; a misspelled backend must not silently leave the \
                 node on `posix_fs` with a bucket full of snapshots it will never look at"
            );
        }

        // 🔴 And it still wins over an overlay file that says the opposite.
        //
        // This is the priority order, pinned where the variable already has an
        // owner: file, then AENV_CONFIG_OVERLAY_PATH, then the environment.
        // The environment on top is not a preference, it is the rollback: the
        // overlay is a mounted Secret an operator may not be able to rewrite
        // in the middle of an incident, and `kubectl set env` has to be able to
        // overrule it. Both directions are checked in the same loop, so
        // "the environment won" cannot be an overlay that happened to agree.
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

            // The control, in the same iteration: with the environment quiet
            // the overlay is what decides.
            let from_overlay = ConfigManager::load_config_file_with_overlays(
                &bundled,
                std::slice::from_ref(&overlay),
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

    /// The three local-disk budgets are per-machine numbers, and the file they
    /// would otherwise be set in is one file for the whole fleet — and is
    /// overwritten by every apply. Two of the three are reachable from the
    /// environment; this pins that they are, and that they are *not* already
    /// the values being set.
    #[test]
    fn the_image_cache_budgets_are_settable_from_the_environment() {
        let _env = env_guard();
        let workspace = Path::new(env!("CARGO_MANIFEST_DIR"));
        let bundled = workspace.join("config/default.toml");

        // The control, read from the same file in the same test. The bundled
        // config's own numbers, which are what an apply restores.
        let shipped = ConfigManager::new_from_path(&bundled).expect("load without the override");
        let shipped_capacity = shipped.config().image.cache.capacity_gb;
        let shipped_remote = shipped.config().image.cache.remote_blocks.max_size_gb;

        // 🔴 The overrides below have to be values the file does not already
        // carry, or the assertion passes against a loader that ignores the
        // environment entirely.
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
        let overridden = ConfigManager::new_from_path(&bundled);
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

        // And back to the file's own numbers once the environment is quiet, in
        // the same test, so "the override worked" cannot be a leaked value.
        let after = ConfigManager::new_from_path(&bundled).expect("load after the override");
        assert_eq!(after.config().image.cache.capacity_gb, shipped_capacity);
        assert_eq!(
            after.config().image.cache.remote_blocks.max_size_gb,
            shipped_remote
        );
    }

    /// 🔴 Serializes every test in this module that either sets one of the
    /// `AENV_*` variables the loader reads or compares two loads against each
    /// other.
    ///
    /// The variables are process-global and `cargo test` runs this module's
    /// tests in parallel threads of one process. Without this, a test that
    /// sets `AENV_SNAPSHOT_REPOSITORY_BACKEND` for a few microseconds can land
    /// between the two loads another test is comparing, and the failure it
    /// produces names neither test. The convention up to now has been one
    /// owner per variable, which keeps the *setters* from fighting each other
    /// but does nothing for a reader.
    fn env_guard() -> std::sync::MutexGuard<'static, ()> {
        static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        ENV_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Deep merge, key by key, with the later document winning.
    ///
    /// 🔴 Every assertion here has its opposite in the same call: the merged
    /// table is checked for what the *earlier* file said as well as for what
    /// the later one changed. "The overlay won" and "the overlay replaced the
    /// whole table" are the same observation if only the overridden key is
    /// looked at, and they are the difference between being able to keep
    /// `[backend.oss]`'s endpoint in this repository and having to put it in a
    /// Secret alongside the credentials.
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

        // The point of the whole mechanism: a key only the base has survives a
        // file that writes into the same table.
        assert_eq!(
            oss["bucket"].as_str(),
            Some("base-bucket"),
            "an overlay that mentions [backend.oss] replaced the section instead of merging \
             into it; the endpoint and the bucket would then have to live in the same file as \
             the credentials"
        );
        // ...and so does a key only an intermediate overlay has.
        assert_eq!(
            oss["access_key_id"].as_str(),
            Some("from-first-overlay"),
            "the second overlay dropped what the first one contributed"
        );
        // The counter-face: where they collide, the last file wins — twice
        // over, so "later wins" is not "first wins" read the wrong way round.
        assert_eq!(oss["endpoint"].as_str(), Some("http://second:9000"));
        assert_eq!(merged["scalar"].as_str(), Some("from second"));
        assert_eq!(
            merged["keep"]["untouched"].as_str(),
            Some("base"),
            "a table no overlay mentioned was disturbed"
        );

        // 🔴 Arrays replace, they do not concatenate. Stated as a test because
        // the alternative is defensible and somebody will assume it: a list of
        // proxy domains or of allowed boot-arg prefixes that grew by one entry
        // per layer would be a surprise nothing reports.
        assert_eq!(
            merged["array"].as_array().map(Vec::len),
            Some(1),
            "arrays must replace whole"
        );
        assert_eq!(merged["array"][0].as_integer(), Some(9));

        // And a scalar giving way to a table (or the reverse) is a plain
        // replacement rather than a panic or a silent keep.
        let mut swapped: toml::Table = toml::from_str("value = 1\n[table]\nx = 1\n").expect("base");
        merge_toml_tables(
            &mut swapped,
            toml::from_str("table = 2\n[value]\nx = 1\n").expect("overlay"),
        );
        assert_eq!(swapped["table"].as_integer(), Some(2));
        assert!(swapped["value"].is_table());
    }

    /// 🔴 The promise every cluster that never sets the new variable is owed.
    ///
    /// With no overlay, `load_config_file` must be the two lines it always
    /// was — confique's `.env().file(path)` — and produce a configuration that
    /// is identical field for field. This compares a full `{:#?}` of the
    /// result against the old expression written out by hand, over
    /// `config/default.toml`, which is the file `deploy/k8s/run.sh` copies into
    /// every cluster.
    ///
    /// 🔴 "The two dumps are equal" is also what a test comparing a value with
    /// itself reports, so the comparison is shown to fail before it is
    /// believed: one field is changed by hand and the same comparison has to
    /// notice.
    #[test]
    fn no_overlay_is_byte_for_byte_the_old_load() {
        let _env = env_guard();
        let workspace = Path::new(env!("CARGO_MANIFEST_DIR"));
        let bundled = workspace.join("config/default.toml");
        let config_dir = bundled.parent().expect("config dir");

        // The load as it was written before AENV_CONFIG_OVERLAY_PATH existed.
        let mut old = AppConfig::builder()
            .env()
            .file(&bundled)
            .load()
            .expect("the pre-overlay load");
        old.normalize(config_dir).expect("normalize");
        old.validate().expect("validate");

        let new = ConfigManager::load_config_file_with_overlays(&bundled, &[])
            .expect("the current load with no overlay");

        let old_dump = format!("{old:#?}");
        let new_dump = format!("{new:#?}");
        assert_eq!(
            old_dump, new_dump,
            "loading config/default.toml with no overlay no longer produces what the old \
             .env().file(path) produced"
        );

        // 🔴 The self-check. A dump comparison that cannot fail proves nothing,
        // and `{:#?}` skipping a field — `SandboxConfig` already has a Debug
        // that redacts one — is exactly how it would silently stop being able
        // to.
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

    /// The main config file is merged the same way an overlay is, so with
    /// overlays in play it reaches confique as a `toml::Table` rather than
    /// through confique's own file source. That is a second code path over the
    /// file every cluster runs, and this pins that it is not a second answer.
    ///
    /// The control is in the same test: an overlay that changes one value has
    /// to produce a different dump, or "identical" would only mean the overlay
    /// was never read.
    #[test]
    fn an_empty_overlay_leaves_the_main_file_alone() {
        let _env = env_guard();
        let bundled = Path::new(env!("CARGO_MANIFEST_DIR")).join("config/default.toml");
        let dir = tempdir().expect("tempdir");

        let empty = dir.path().join("empty.toml");
        std::fs::write(&empty, "# nothing but a comment\n").expect("write empty overlay");

        let without = ConfigManager::load_config_file_with_overlays(&bundled, &[])
            .expect("load with no overlay");
        let with_empty =
            ConfigManager::load_config_file_with_overlays(&bundled, std::slice::from_ref(&empty))
                .expect("load with an empty overlay");
        assert_eq!(
            format!("{without:#?}"),
            format!("{with_empty:#?}"),
            "routing config/default.toml through the overlay merge changed how it parses"
        );

        let changed = dir.path().join("changed.toml");
        std::fs::write(&changed, "[firecracker]\nsocket_poll_ms = 7\n").expect("write overlay");
        let with_change =
            ConfigManager::load_config_file_with_overlays(&bundled, std::slice::from_ref(&changed))
                .expect("load with a real overlay");
        assert_eq!(with_change.firecracker.socket_poll_ms, 7);
        assert_ne!(
            format!("{without:#?}"),
            format!("{with_change:#?}"),
            "an overlay that changes a value produced an identical dump; the comparison above \
             is measuring nothing"
        );
    }

    /// 🔴 The reason this mechanism exists, in the shape the cluster runs it.
    ///
    /// `[backend.oss]` cannot be reached from the environment at all, and the
    /// file it would otherwise live in is overwritten by every
    /// `make k8s-apply`. Here the section arrives from two overlay files that
    /// neither the main config nor each other could supply alone: the
    /// endpoint, bucket, region and cache budget from a file this repository
    /// tracks, and the credentials from a mounted Secret.
    #[test]
    fn two_overlays_assemble_the_backend_section_no_environment_can_reach() {
        let _env = env_guard();
        let bundled = Path::new(env!("CARGO_MANIFEST_DIR")).join("config/default.toml");
        let dir = tempdir().expect("tempdir");

        // The half that is safe to commit: everything except the credentials.
        let public = dir.path().join("oss.toml");
        std::fs::write(
            &public,
            "[snapshot]\nrepository_backend = \"oss\"\n\n\
             [backend.oss]\nendpoint = \"http://rustfs:9000\"\nbucket = \"agentenv-snapshots\"\n\
             region = \"us-east-1\"\nprefix = \"snapshots/\"\ncache_max_size_gb = 8\n",
        )
        .expect("write the public overlay");

        // The half that only ever comes from a Secret.
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

        // 🔴 Control 1: the same main file, alone, has neither. This is what a
        // cluster gets from an apply, and it is why the section has to come
        // from somewhere the apply cannot reach.
        let alone = ConfigManager::load_config_file_with_overlays(&bundled, &[])
            .expect("load the main file alone");
        assert!(alone.backend.oss.is_none());
        assert_eq!(
            alone.snapshot.repository_backend,
            SnapshotRepositoryBackendKind::PosixFs
        );

        // 🔴 Control 2: neither half is sufficient, and they fail in opposite
        // directions. The public half loads and leaves the credentials unset —
        // which is the state a cluster whose Secret failed to mount would be
        // in, if the mount were allowed to fail quietly. The credential half
        // alone is not a valid section at all.
        let public_only =
            ConfigManager::load_config_file_with_overlays(&bundled, std::slice::from_ref(&public))
                .expect("the public half alone loads");
        assert!(public_only
            .backend
            .oss
            .expect("public half supplies the section")
            .access_key_id
            .is_none());
        let secret_only =
            ConfigManager::load_config_file_with_overlays(&bundled, std::slice::from_ref(&secret));
        assert!(
            secret_only.is_err(),
            "a [backend.oss] with credentials but no endpoint or bucket must be refused, not \
             completed from somewhere"
        );

        // 🔴 Control 3: order decides where they collide, so the file listed
        // last is the one that can overrule a mounted Secret — worth knowing
        // before writing the list into a manifest.
        let shadow = dir.path().join("shadow.toml");
        std::fs::write(&shadow, "[backend.oss]\naccess_key_id = \"shadowed\"\n")
            .expect("write shadow");
        let secret_last = ConfigManager::load_config_file_with_overlays(
            &bundled,
            &[public.clone(), shadow.clone(), secret.clone()],
        )
        .expect("load");
        let shadow_last =
            ConfigManager::load_config_file_with_overlays(&bundled, &[public, secret, shadow])
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

    /// 🔴 A named overlay that is not on disk stops the load.
    ///
    /// confique's own file source treats a missing file as an empty layer, and
    /// inheriting that here would make the one failure this mechanism exists to
    /// prevent silent: a Secret that failed to mount, a node that starts on
    /// `posix_fs`, an empty local snapshot store, and a catalog still naming
    /// artifacts nothing can fetch. The error has to name the file and the
    /// variable, because the operator reading it did not necessarily write the
    /// manifest.
    #[test]
    fn a_named_overlay_that_is_not_on_disk_stops_the_load() {
        let _env = env_guard();
        let bundled = Path::new(env!("CARGO_MANIFEST_DIR")).join("config/default.toml");
        let dir = tempdir().expect("tempdir");

        // The control: the same call, with the file there, loads.
        let present = dir.path().join("present.toml");
        std::fs::write(&present, "[firecracker]\nsocket_poll_ms = 7\n").expect("write");
        assert_eq!(
            ConfigManager::load_config_file_with_overlays(&bundled, std::slice::from_ref(&present))
                .expect("an overlay that exists loads")
                .firecracker
                .socket_poll_ms,
            7
        );

        let missing = dir.path().join("not-mounted.toml");
        let err =
            ConfigManager::load_config_file_with_overlays(&bundled, &[present, missing.clone()])
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

    /// 🔴 The one test that touches `AENV_CONFIG_OVERLAY_PATH`, because it is
    /// process-global; everything else about overlays goes through
    /// `load_config_file_with_overlays` directly.
    ///
    /// The overlay it mounts changes `firecracker.socket_poll_ms` and nothing
    /// else, deliberately: while this test holds the variable set, any other
    /// test in the process that loads a config sees the overlay too, and that
    /// value is asserted on nowhere.
    #[test]
    fn the_overlay_variable_is_read_and_only_separators_means_unset() {
        let _env = env_guard();

        // The global is initialised here rather than left to whichever test
        // gets there first, so it cannot be built while the variable below is
        // set and carry a tempdir that is about to be deleted.
        let _ = ConfigManager::global();

        let bundled = Path::new(env!("CARGO_MANIFEST_DIR")).join("config/default.toml");
        let dir = tempdir().expect("tempdir");
        let overlay = dir.path().join("overlay.toml");
        std::fs::write(&overlay, "[firecracker]\nsocket_poll_ms = 7\n").expect("write overlay");

        std::env::remove_var(ENV_CONFIG_OVERLAY_PATH);
        assert!(ConfigManager::overlay_paths_from_env().is_empty());

        // Unset, empty, and nothing-but-separators are the same thing. That is
        // what lets a Deployment write "$(A):$(B)" and turn one half off by
        // clearing a ConfigMap key instead of editing the manifest.
        for quiet in ["", "   ", ":", " : ", "::"] {
            std::env::set_var(ENV_CONFIG_OVERLAY_PATH, quiet);
            let paths = ConfigManager::overlay_paths_from_env();
            std::env::remove_var(ENV_CONFIG_OVERLAY_PATH);
            assert!(
                paths.is_empty(),
                "{quiet:?} should mean no overlay, got {paths:?}"
            );
        }

        // The counter-face: real entries are read, in order, and empty
        // segments between them are skipped rather than becoming paths.
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

        // End to end, through the entry point the server actually calls.
        let quiet = ConfigManager::new_from_path(&bundled).expect("load with the variable unset");
        assert_eq!(
            quiet.config().firecracker.socket_poll_ms,
            1,
            "the file's own value must stand when the variable says nothing"
        );

        std::env::set_var(ENV_CONFIG_OVERLAY_PATH, overlay.display().to_string());
        let mounted = ConfigManager::new_from_path(&bundled);
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

    /// Every struct named in this file, by name, with its body.
    fn struct_bodies(source: &str) -> Vec<(String, String)> {
        let mut out = Vec::new();
        let mut rest = source;
        while let Some(at) = rest.find("\npub struct ") {
            let after = &rest[at + "\npub struct ".len()..];
            let name: String = after
                .chars()
                .take_while(|ch| ch.is_alphanumeric() || *ch == '_')
                .collect();
            // The body runs to the first line that is exactly a closing brace,
            // which is what rustfmt guarantees for an item at column 0.
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

    /// The names of config structs that are reached as a bare `Option<_>` —
    /// the ones confique deserializes with serde and never visits from the
    /// environment.
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
                // Walk back over this field's attribute lines.
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

    /// The unreachable structs that nevertheless declare an environment
    /// binding — the dead promises.
    ///
    /// 🔴 Doc comments are stripped first. Explaining *why* a struct must not
    /// carry an `env` attribute requires writing the attribute down, and a
    /// scanner that counted the explanation would make the explanation
    /// impossible to write.
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

    /// The environment bindings confique actually reads: an `env` attribute on
    /// a field of a struct it can reach.
    ///
    /// Doc comments are stripped for the reason [`dead_env_bindings`] strips
    /// them — the prose has to be able to name an attribute without becoming
    /// one.
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

    /// The `AENV_*` / `API_*` variables the `## Server` table of
    /// `docs/src/configuration/env-vars.md` offers an operator.
    ///
    /// Rows whose name is struck through (`~~NAME~~`) are skipped: that is how
    /// this file records a variable that has been removed, and a removal notice
    /// is the opposite of a promise.
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

    /// 🔴 Every variable the documentation offers is one the process reads.
    ///
    /// This is the half of the `AENV_SNAPSHOT_STORE` problem that lived outside
    /// the code. The attribute was dead, but what made it *cost* something was
    /// `docs/src/configuration/env-vars.md` listing it in the table an operator
    /// reads when a cluster's snapshot store has to move — a supported override
    /// that has never had any effect, with nothing anywhere to say so.
    ///
    /// The rule: a name in the `## Server` table is either bound on a field
    /// confique can reach, or is in the list below, which says where it *is*
    /// read. A name that is neither is a promise nothing keeps.
    #[test]
    fn every_documented_server_variable_is_one_the_process_reads() {
        let workspace = Path::new(env!("CARGO_MANIFEST_DIR"));
        let sources = ["src/cfg.rs", "src/cfg/image.rs", "src/cfg/network.rs"]
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

        // 🔴 Both scanners are shown to work before their agreement is
        // believed. Two empty lists agree perfectly.
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

        // Read somewhere other than a confique attribute. Each one names where,
        // so this list cannot quietly become a place to put a dead promise.
        let read_elsewhere = [
            ("AENV_CONFIG_PATH", "ENV_CONFIG_PATH, ConfigManager::new"),
            (
                "AENV_CONFIG_OVERLAY_PATH",
                "ENV_CONFIG_OVERLAY_PATH, ConfigManager::overlay_paths_from_env",
            ),
            ("AENV_LOG_FORMAT", "src/logging.rs"),
            ("AENV_LOG_SPAN_EVENTS", "src/logging.rs"),
            ("AENV_FORCE_SYSCTL_TUNING", "src/setup/network_capacity.rs"),
            ("API_ADDR", "src/bin/server.rs"),
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

        // 🔴 The counter-face for both scanners, on planted input: a binding
        // that is only reachable by serde must not count as live, and a row
        // that is not struck through must be read as a promise.
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

    /// 🔴 An `env` attribute on a field confique never reads is worse than no
    /// attribute at all: it is a promise, and `docs/src/configuration/` repeats
    /// it to operators as a supported variable.
    ///
    /// confique walks into a struct's fields only through a field marked
    /// `#[config(nested)]`, and `nested` may not be `Option<_>` — the derive
    /// rejects it outright (`confique-macro/src/parse.rs:127`). So a config
    /// struct reached as a bare `Option<Something>` is deserialized by serde
    /// from the file and nothing else: its environment bindings are dead,
    /// silently, and the file always wins.
    ///
    /// `[backend]` is where this bites. `BackendConfig` is `nested`, but both
    /// of its fields are `Option<_>`, so nothing under `[backend.posix_fs]` or
    /// `[backend.oss]` can be set from the environment. That is why the OSS
    /// endpoint, bucket and credentials this cluster runs on cannot simply be
    /// given environment bindings and a `secretKeyRef`, and why
    /// [`ENV_CONFIG_OVERLAY_PATH`] exists.
    ///
    /// 🔴 The expected set is now *empty*, and that is a change from when this
    /// test was written. `AENV_SNAPSHOT_STORE` used to be here: declared on
    /// `PosixFsBackendConfig::snapshot_store`, documented in
    /// `docs/src/configuration/env-vars.md`, and never once read. It has been
    /// removed rather than made to work — making it work means giving
    /// `[backend]` non-`Option` nested fields, which changes what
    /// `backend.posix_fs` resolves to under `repository_backend = "oss"`. The
    /// overlay file is the supported replacement, and the documentation now
    /// says so.
    ///
    /// An empty expected set is exactly the result a broken scanner reports, so
    /// the scanner is run against a planted source first.
    #[test]
    fn no_new_env_binding_is_declared_where_confique_cannot_read_it() {
        // 🔴 The non-empty half. A parser that found no structs, no fields, or
        // no attributes reports the same clean result as a tree with no dead
        // bindings — so make it find one that is deliberately there.
        //
        // 🔴 Written with a placeholder for the item keyword, because this
        // scanner reads *this file*. Spelled out, the planted structs would be
        // found by the real scan below and the test would fail on its own
        // fixture.
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
    fn validate_memory_snapshot_options_enforces_dirty_page_requirements() {
        let cases = [
            (VirtualizationMode::Kvm, false, None),
            (VirtualizationMode::Kvm, true, None),
            (
                VirtualizationMode::Pvm,
                true,
                Some("is disabled in PVM mode"),
            ),
        ];

        for (virtualization_mode, track_dirty_pages, expected_error) in cases {
            let config = AppConfig {
                virtualization_mode,
                memory_snapshot: MemorySnapshotConfig {
                    track_dirty_pages,
                    ..Default::default()
                },
                ..Default::default()
            };

            let result = config.validate_memory_snapshot_options();
            match expected_error {
                Some(expected_error) => {
                    let error = result.unwrap_err();
                    assert!(
                        error.to_string().contains(expected_error),
                        "expected error containing {expected_error:?}, got: {error}"
                    );
                }
                None => result.expect("supported memory snapshot options should be valid"),
            }
        }
    }

    /// 🔴 The legal pairs, and the four illegal ones by name.
    ///
    /// Every rejected combination fails the same quiet way if it is allowed
    /// through — a read that answers "no such snapshot" rather than an error —
    /// and absence is what callers delete artifacts and refuse resumes on.
    #[test]
    fn the_snapshot_catalog_matrix_allows_only_the_pairs_that_are_served() {
        let cases: [(SnapshotCatalogWrite, SnapshotCatalogRead, Option<&str>); 6] = [
            (
                SnapshotCatalogWrite::ObjectStore,
                SnapshotCatalogRead::ObjectStore,
                None,
            ),
            (
                SnapshotCatalogWrite::Both,
                SnapshotCatalogRead::ObjectStore,
                None,
            ),
            (
                SnapshotCatalogWrite::ObjectStore,
                SnapshotCatalogRead::Postgres,
                Some("reads a table nothing writes"),
            ),
            (
                SnapshotCatalogWrite::Postgres,
                SnapshotCatalogRead::ObjectStore,
                Some("reads a store nothing writes any more"),
            ),
            // 🔴 Legal as of the read switch, and this layer having nothing to
            // say about it is not the switch being safe: `guard_read_side` and
            // the population comparison run below it, on a store this function
            // cannot see.
            (
                SnapshotCatalogWrite::Both,
                SnapshotCatalogRead::Postgres,
                None,
            ),
            (
                SnapshotCatalogWrite::Postgres,
                SnapshotCatalogRead::Postgres,
                Some("drops the object-store copy"),
            ),
        ];

        for (write, read, expected) in cases {
            let mut config = AppConfig::default();
            config.snapshot.catalog.write = write;
            config.snapshot.catalog.read = read;

            match expected {
                None => config
                    .validate_snapshot_catalog()
                    .unwrap_or_else(|error| panic!("{write:?}/{read:?} should be legal: {error}")),
                Some(expected) => {
                    let error = config
                        .validate_snapshot_catalog()
                        .expect_err(&format!("{write:?}/{read:?} should be refused"));
                    assert!(
                        error.to_string().contains(expected),
                        "{write:?}/{read:?}: expected {expected:?}, got: {error}"
                    );
                }
            }
        }
    }

    /// 🔴 This layer no longer answers for the one below it.
    ///
    /// `read = "postgres"` used to be refused here, one layer *above*
    /// `MirrorBacklog::guard_read_side` — so everything that guard refuses on
    /// had never executed anywhere but a unit test of the guard itself. Opening
    /// the arm is not the same as those refusals working, and this test is the
    /// seam: the configuration that now passes here goes straight on to meet
    /// them, and is refused by them.
    ///
    /// The control is the second half: with the same configuration and two
    /// catalogs that agree, the same call is allowed.
    #[tokio::test]
    async fn a_legal_postgres_read_still_has_to_get_past_the_mirror() {
        use crate::snapshot::repository::mirror::{
            admit_read_side, CatalogCensus, CatalogReadSide, MirrorBacklog, MirrorDirection,
            MirrorTargets,
        };
        use crate::snapshot::repository::RepositoryResult;
        use crate::snapshot::SnapshotId;

        struct Census(Vec<SnapshotId>);

        // Targets with no central half, so the replay the admission now runs
        // before it refuses can land nothing. What this test is a seam for is
        // the refusals, and a refusal proven against a target that could have
        // repaired proves less than it looks.
        fn no_repair_possible() -> MirrorTargets {
            MirrorTargets::object_store(std::sync::Arc::new(
                crate::snapshot::repository::mirror::test_doubles::ScriptedCatalog::default(),
            )
                as std::sync::Arc<dyn crate::snapshot::repository::interfaces::SnapshotCatalog>)
        }

        #[async_trait::async_trait]
        impl CatalogCensus for Census {
            async fn every_snapshot_id(&self) -> RepositoryResult<Vec<SnapshotId>> {
                Ok(self.0.clone())
            }
        }

        let mut config = AppConfig::default();
        config.snapshot.catalog.write = SnapshotCatalogWrite::Both;
        config.snapshot.catalog.read = SnapshotCatalogRead::Postgres;
        config
            .validate_snapshot_catalog()
            .expect("the read switch is a legal configuration now");

        let dir = tempfile::TempDir::new().expect("tempdir");
        let backlog = MirrorBacklog::open(dir.path().join("mirror"))
            .await
            .expect("the backlog should open");

        // 1. The history the double write never saw. Refused before either
        //    catalog is even asked.
        let error = admit_read_side(
            CatalogReadSide::Postgres,
            &backlog,
            &no_repair_possible(),
            &Census(Vec::new()),
            &Census(Vec::new()),
        )
        .await
        .expect_err("a mirror that never enumerated the object store's history refuses");
        assert!(error.to_string().contains("never been queued"), "{error}");

        backlog
            .queue_history_toward_central(
                &crate::snapshot::repository::mirror::test_doubles::ScriptedCatalog::default(),
            )
            .await
            .expect("an empty object store has no history to queue");
        backlog
            .record_read_side(CatalogReadSide::ObjectStore)
            .await
            .expect("the node starts out reading object storage");

        // 2. The two catalogs holding different snapshots — the refusal no
        //    gauge can express, and the one this batch added.
        let held: Vec<SnapshotId> = (0..3).map(|_| SnapshotId::generate()).collect();
        let error = admit_read_side(
            CatalogReadSide::Postgres,
            &backlog,
            &no_repair_possible(),
            &Census(held.clone()),
            &Census(Vec::new()),
        )
        .await
        .expect_err("object storage holds snapshots the central catalog has never heard of");
        assert!(error.to_string().contains("3 row(s)"), "{error}");
        assert_eq!(backlog.lag_toward(MirrorDirection::Central), 0);
        assert_eq!(backlog.diverged_toward(MirrorDirection::Central), 0);

        // The control.
        admit_read_side(
            CatalogReadSide::Postgres,
            &backlog,
            &no_repair_possible(),
            &Census(held.clone()),
            &Census(held),
        )
        .await
        .expect("two catalogs holding the same snapshots may switch");
    }

    /// The default is the arrangement that exists today: one catalog, in
    /// object storage. Anything else has to be asked for.
    #[test]
    fn the_snapshot_catalog_defaults_to_the_single_store_it_has_always_had() {
        let config = AppConfig::default();
        assert_eq!(
            config.snapshot.catalog.write,
            SnapshotCatalogWrite::ObjectStore
        );
        assert_eq!(
            config.snapshot.catalog.read,
            SnapshotCatalogRead::ObjectStore
        );
        config
            .validate()
            .expect("the default configuration must be valid");
    }

    #[test]
    fn validate_rejects_zero_memory_snapshot_download_concurrency() {
        let mut config = AppConfig::default();
        config.memory_snapshot.background_download.concurrency = 0;

        let err = config.validate().unwrap_err();
        assert!(
            err.to_string()
                .contains("memory_snapshot.background_download.concurrency must be > 0"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn validate_rejects_invalid_disk_rate_limit() {
        let cases = [
            (
                DiskRateLimitConfig {
                    enabled: true,
                    bandwidth_burst_bytes: 1024,
                    ..Default::default()
                },
                "bandwidth_burst_bytes is set but",
            ),
            (
                DiskRateLimitConfig {
                    enabled: true,
                    iops_burst: 500,
                    ..Default::default()
                },
                "iops_burst is set but",
            ),
            (
                DiskRateLimitConfig {
                    enabled: true,
                    bandwidth_bytes_per_sec: i64::MAX as u64 + 1,
                    ..Default::default()
                },
                "machine.disk_rate_limit.bandwidth_bytes_per_sec",
            ),
        ];

        for (disk_rate_limit, expected_error) in cases {
            let mut config = AppConfig::default();
            config.machine.disk_rate_limit = disk_rate_limit;

            let err = config.validate().unwrap_err();
            assert!(
                err.to_string().contains(expected_error),
                "expected error containing {expected_error:?}, got: {err}"
            );
        }
    }

    #[test]
    fn validate_skips_disabled_disk_rate_limit() {
        // A disabled section is ignored at runtime, so even internally
        // inconsistent or out-of-range values must not block startup.
        let mut config = AppConfig::default();
        config.machine.disk_rate_limit.enabled = false;
        config.machine.disk_rate_limit.bandwidth_bytes_per_sec = 0;
        config.machine.disk_rate_limit.bandwidth_burst_bytes = 1024;
        config.machine.disk_rate_limit.iops = u64::MAX;
        config
            .validate()
            .expect("disabled disk rate limit config is not validated");
    }

    #[test]
    fn validate_accepts_consistent_disk_rate_limit() {
        let mut config = AppConfig::default();
        config.machine.disk_rate_limit.enabled = true;
        config.machine.disk_rate_limit.bandwidth_bytes_per_sec = 104_857_600;
        config.machine.disk_rate_limit.bandwidth_burst_bytes = 10_485_760;
        config.machine.disk_rate_limit.iops = 3000;
        config.machine.disk_rate_limit.iops_burst = 500;
        config
            .validate()
            .expect("consistent disk rate limit config passes");
    }

    #[test]
    fn validate_rejects_colliding_overlaybd_global_config_paths() {
        let cases = [
            (
                "/tmp/shared-overlaybd-global.json",
                "/tmp/shared-overlaybd-global.json",
                "ublk.overlaybd.global_config_path",
                "memory_snapshot.overlaybd_global_config_path",
            ),
            (
                "/tmp/aenv/x/../global.json",
                "/tmp/aenv/global.json",
                "ublk.overlaybd.global_config_path",
                "memory_snapshot.overlaybd_global_config_path",
            ),
            (
                "/tmp/overlaybd/resize-overlaybd-global.json",
                "/var/lib/aenv/overlaybd/mem-overlaybd-global.json",
                "ublk.overlaybd.global_config_path",
                "derived resize",
            ),
            (
                "/tmp/overlaybd/overlaybd-global.json",
                "/tmp/overlaybd/convert-overlaybd-global.json",
                "memory_snapshot.overlaybd_global_config_path",
                "derived convert",
            ),
        ];

        for (runtime_path, memory_path, left_name, right_name) in cases {
            let mut config = AppConfig::default();
            config.ublk.overlaybd.global_config_path = runtime_path.into();
            config.memory_snapshot.overlaybd_global_config_path = memory_path.into();

            let err = config.validate().unwrap_err();
            let message = err.to_string();
            assert!(message.contains(left_name), "unexpected error: {err}");
            assert!(message.contains(right_name), "unexpected error: {err}");
            assert!(
                message.contains("must be different"),
                "unexpected error: {err}"
            );
        }
    }

    #[test]
    fn validate_bounds_memory_snapshot_background_download() {
        let mut config = AppConfig::default();
        config.memory_snapshot.background_download.concurrency = 17;
        assert!(config.validate().is_err());

        let mut config = AppConfig::default();
        config.memory_snapshot.background_download.block_size = 65 * 1024 * 1024;
        config.memory_snapshot.background_download.concurrency = 1;
        assert!(config.validate().is_err());

        let mut config = AppConfig::default();
        config.memory_snapshot.background_download.block_size = 64 * 1024 * 1024;
        config.memory_snapshot.background_download.concurrency = 8;
        assert!(config.validate().is_err());

        let mut config = AppConfig::default();
        config.memory_snapshot.background_download.block_size = 32 * 1024 * 1024;
        config.memory_snapshot.background_download.concurrency = 8;
        config.validate().expect("defaults-shaped config passes");

        let mut config = AppConfig::default();
        config.memory_snapshot.background_download.delay = -1;
        assert!(config.validate().is_err());

        let mut config = AppConfig::default();
        config.memory_snapshot.background_download.try_cnt = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn overlaybd_converter_cache_id_includes_configured_tool_version() {
        let config = AppConfig {
            overlaybd: Some(OverlaybdDependencyConfig {
                version: "test-version".to_string(),
                url: None,
                package_url: None,
            }),
            ..Default::default()
        };
        assert_eq!(
            config.resolved_overlaybd_oci_converter_id(),
            "overlaybd-oci:test-version:agentenv-cache-v1"
        );
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
    fn managed_dependency_paths_remain_implicit_and_resolve_from_deps_path() -> Result<()> {
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

        let deps_path = config_dir.join("deps");
        assert_eq!(config.deps_path, deps_path);
        assert_eq!(config.firecracker.binary_path, None);
        assert_eq!(config.kernel.image_path, None);
        assert_eq!(config.tools.drive_path, None);
        assert_eq!(
            config.resolved_firecracker_binary_path(),
            deps_path.join("firecracker/fc-test/firecracker")
        );
        assert_eq!(
            config.resolved_kernel_image_path(),
            deps_path.join("kernel/kernel-test/vmlinux.bin")
        );
        assert_eq!(
            config.resolved_tools_drive_path()?,
            deps_path.join("tools/1.2.3-custom.1/tools.ext4")
        );
        assert!(config
            .resolved_tools_drive_path_for_version("../escape")
            .is_err());
        assert!(config
            .resolved_tools_drive_path_for_version("1.2.3+rebuilt")
            .is_err());

        Ok(())
    }

    #[test]
    fn managed_dependency_paths_select_the_active_mode_versions() {
        let manifest = SetupDependencyManifest::get();
        let mut config = AppConfig {
            deps_path: PathBuf::from("/deps"),
            virtualization_mode: VirtualizationMode::Kvm,
            ..Default::default()
        };
        assert_eq!(
            config.resolved_firecracker_binary_path(),
            PathBuf::from("/deps/firecracker")
                .join(&manifest.firecracker.kvm.version)
                .join("firecracker")
        );
        assert_eq!(
            config.resolved_kernel_image_path(),
            PathBuf::from("/deps/kernel")
                .join(&manifest.kernel.kvm.version)
                .join("vmlinux.bin")
        );

        config.virtualization_mode = VirtualizationMode::Pvm;
        assert_eq!(
            config.resolved_firecracker_binary_path(),
            PathBuf::from("/deps/firecracker")
                .join(&manifest.firecracker.pvm.version)
                .join("firecracker")
        );
        assert_eq!(
            config.resolved_kernel_image_path(),
            PathBuf::from("/deps/kernel")
                .join(&manifest.kernel.pvm.version)
                .join("vmlinux.bin")
        );
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
            node_service_addr: "0.0.0.0:8001".to_string(),
            api_grpc_addr: "0.0.0.0:8002".to_string(),
            node_service_port: 8001,
        };
        config.normalize();
        assert_eq!(config.scheduler_endpoint, None);

        let mut config = ClusterConfig {
            scheduler_endpoint: Some("  http://scheduler:9090  ".to_string()),
            node_service_addr: "0.0.0.0:8001".to_string(),
            api_grpc_addr: "0.0.0.0:8002".to_string(),
            node_service_port: 8001,
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
    fn validate_pool_config_rejects_invalid_values() {
        let config = AppConfig {
            pool: PoolTomlConfig {
                low_watermark: 64,
                high_watermark: 32,
                ..PoolTomlConfig::default()
            },
            ..AppConfig::default()
        };
        let err = config.validate_pool_config().unwrap_err();
        assert!(
            err.to_string().contains("low_watermark"),
            "unexpected error: {err}"
        );

        let config = AppConfig {
            pool: PoolTomlConfig {
                low_watermark: 64,
                high_watermark: 32,
                network: PoolComponentConfig {
                    maintenance_enabled: false,
                    ..PoolComponentConfig::default()
                },
                block: PoolComponentConfig {
                    enabled: false,
                    ..PoolComponentConfig::default()
                },
                firecracker: FirecrackerProcessPoolConfig {
                    enabled: false,
                    ..FirecrackerProcessPoolConfig::default()
                },
            },
            ..AppConfig::default()
        };
        assert!(config.validate_pool_config().is_ok());
    }

    #[test]
    fn firecracker_pool_fill_concurrency_rejects_zero() {
        let config = AppConfig {
            pool: PoolTomlConfig {
                firecracker: FirecrackerProcessPoolConfig {
                    enabled: true,
                    fill_concurrency: 0,
                    ..FirecrackerProcessPoolConfig::default()
                },
                ..PoolTomlConfig::default()
            },
            ..AppConfig::default()
        };

        let err = config.validate_pool_config().unwrap_err();
        assert!(
            err.to_string().contains("fill_concurrency"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn validate_rejects_zero_overlaybd_resize_timeout() {
        let mut config = AppConfig::default();
        config.ublk.overlaybd.resize_timeout_secs = 0;
        let err = config.validate().unwrap_err();
        assert!(
            err.to_string().contains("resize_timeout_secs must be > 0"),
            "unexpected error: {err}"
        );
    }

    /// Whether a manifest *sets* `var`, as against mentioning it.
    ///
    /// 🔴 Comment lines are not a loophole, and here that is the whole point:
    /// both manifests that used to carry this key now name it in order to say
    /// it is deliberately absent. A predicate that could not tell a comment
    /// from a setting would push those explanations out of the files — and the
    /// explanation is the only thing standing between the next operator and a
    /// runbook that still tells them to flip it.
    ///
    /// What is still caught is the variable on every line a deployment tool
    /// reads: a `- name:` entry in a container's `env:`, a `KEY: value` under a
    /// ConfigMap's `data:`, and a `KEY=value` kustomize literal.
    fn manifest_sets(contents: &str, var: &str) -> bool {
        contents
            .lines()
            .any(|line| line.contains(var) && !line.trim_start().starts_with('#'))
    }

    /// 🔴 No deployment manifest declares a node-service gate nothing reads.
    ///
    /// `AENV_NODE_SERVICE_ENABLED` was a seam for a design this tree decided
    /// against: letting `--role all` serve the node sandbox service. Nothing in
    /// the Rust tree ever read it, and `only_the_split_roles_bind_a_second_listener`
    /// in `src/bin/server.rs` fails if `assemble_all` ever grows the listener —
    /// so setting the variable could not change behaviour even in principle.
    ///
    /// What it did cost was real. Being read by every node in the fleet, two
    /// runbooks costed flipping it at a serial DaemonSet roll with a drain per
    /// machine, and billed it as the *first* step of the cutover. That is a
    /// fleet-wide roll bought for a no-op, and a dead switch in a manifest is
    /// exactly the thing that gets copied forward by someone who assumes the
    /// manifest knows something they do not.
    ///
    /// 🔴 The scan's whole result is an absence, so the proof that it *would*
    /// find a setting lives in this same test rather than in a sibling one. A
    /// separate control can be filtered out of a run or deleted on its own, and
    /// what is left then passes identically against a scanner that reads
    /// nothing at all.
    #[test]
    fn no_manifest_sets_a_node_service_gate_nothing_reads() {
        const VAR: &str = "AENV_NODE_SERVICE_ENABLED";

        // 🔴 The non-empty half, ahead of the scan rather than beside it.
        // These are the three forms a manifest in this tree can express the
        // setting in; the predicate has to catch all three before the absence
        // the scan reports means anything.
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
        // And the direction the narrowing exists for, plus a line that merely
        // resembles one: neither is a setting.
        assert!(!manifest_sets(
            &format!("            # {VAR} is deliberately absent, and here is why"),
            VAR
        ));
        assert!(!manifest_sets(
            "            - name: AENV_NODE_SERVICE_ADDR",
            VAR
        ));

        // 🔴 Resolution, inverted from the usual direction. Elsewhere a scan
        // like this proves the name it looks for is the one the config reads.
        // Here the claim is that *nothing* reads it, so this asserts the
        // absence of a reader — with the live neighbour as the control that
        // `env = "..."` is the shape a reader takes in this file. Without that
        // control the assertion would also pass in a file that had never used
        // the attribute at all.
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
        // One real file from the walk, kept so the predicate can be shown to
        // have teeth against an actual manifest and not only against the
        // fragments above — a whole file has comments, blank lines, block
        // scalars and indentation that a three-line literal does not.
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
                     sandbox service is what `--role node` is for",
                    path.display()
                );
                if sample.is_none() && contents.contains('\n') {
                    sample = Some((path.clone(), contents));
                }
            }
        }

        // 🔴 The same assertion the walk just made, on the same file, with one
        // setting line added — so "no manifest sets it" is a fact about the
        // tree rather than about the scan. Whichever file this is, it passed
        // above and must fail here.
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
