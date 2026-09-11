//! The shared configuration plus the node's own view of it: the derived paths,
//! pool shapes and checks over sections only `aenv-node` reads.

use std::path::{Path, PathBuf};

use aenv_core::virtualization::VirtualizationMode;
use anyhow::{bail, Context, Result};

pub use aenv_core::cfg::*;

/// The firecracker warm pool's watermarks together with its fill concurrency.
#[derive(Debug, Clone)]
pub struct ResolvedFirecrackerPoolConfig {
    pub pool: warm_pool::PoolConfig,
    pub fill_concurrency: usize,
}

/// Paths and pool shapes the node derives from configuration. The api half
/// resolves none of them: every one of them names a runtime asset or a warm
/// pool that exists only where sandboxes run.
pub trait NodeConfigExt {
    fn resolved_firecracker_binary_path(&self) -> PathBuf;

    fn resolved_kernel_image_path(&self) -> PathBuf;

    fn resolved_tools_drive_path_for_version(&self, version: &str) -> Result<PathBuf>;

    fn resolved_tools_drive_path(&self) -> Result<PathBuf>;

    fn resolved_overlaybd_oci_converter_id(&self) -> String;

    /// Path of the generated overlaybd global config dedicated to the offline
    /// C++ conversion tools (`overlaybd-apply`). It mirrors the runtime global
    /// config but points at an isolated cacheDir: the C++ file cache manages
    /// cacheDir as flat files and evicts (truncate+unlink) whatever it finds,
    /// which would destroy the Rust runtime cache's per-entry directories if
    /// both shared `remote-blocks`.
    fn resolved_overlaybd_convert_global_config_path(&self) -> PathBuf;

    /// Path of the generated overlaybd global config dedicated to the offline
    /// C++ resize tool (`overlaybd-resize`). It mirrors the runtime global
    /// config but points at an isolated cacheDir: the C++ file cache manages
    /// cacheDir as flat files and evicts (truncate+unlink) whatever it finds,
    /// which would destroy the Rust runtime cache's per-entry directories if
    /// both shared `remote-blocks`.
    fn resolved_overlaybd_resize_global_config_path(&self) -> PathBuf;

    /// Resolve the cpu-template-helper binary path derived from deps_path + version.
    /// Returns `None` if the binary does not exist on disk.
    fn resolved_cpu_template_helper(&self) -> Option<PathBuf>;

    fn image_cache_layout(&self) -> ResolvedImageCacheConfig;

    fn network_pool_config(&self) -> warm_pool::PoolConfig;

    fn block_pool_config(&self) -> Option<warm_pool::PoolConfig>;

    fn firecracker_pool_config(&self) -> Option<ResolvedFirecrackerPoolConfig>;
}

impl NodeConfigExt for AppConfig {
    fn resolved_firecracker_binary_path(&self) -> PathBuf {
        self.firecracker.binary_path.clone().unwrap_or_else(|| {
            let version = self
                .firecracker
                .version
                .as_deref()
                .unwrap_or(manifest_firecracker_version(self.virtualization_mode));
            self.deps_path
                .join("firecracker")
                .join(version)
                .join("firecracker")
        })
    }

    fn resolved_kernel_image_path(&self) -> PathBuf {
        self.kernel.image_path.clone().unwrap_or_else(|| {
            let version = self
                .kernel
                .version
                .as_deref()
                .unwrap_or(manifest_kernel_version(self.virtualization_mode));
            self.deps_path
                .join("kernel")
                .join(version)
                .join("vmlinux.bin")
        })
    }

    fn resolved_tools_drive_path_for_version(&self, version: &str) -> Result<PathBuf> {
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

    fn resolved_tools_drive_path(&self) -> Result<PathBuf> {
        self.resolved_tools_drive_path_for_version(self.resolved_tools_version())
    }

    fn resolved_overlaybd_oci_converter_id(&self) -> String {
        let version = self
            .overlaybd
            .as_ref()
            .map(|overlaybd| overlaybd.version.as_str())
            .unwrap_or(manifest_overlaybd_version());
        format!("overlaybd-oci:{version}:agentenv-cache-v1")
    }

    fn resolved_overlaybd_convert_global_config_path(&self) -> PathBuf {
        self.ublk
            .overlaybd
            .global_config_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join("convert-overlaybd-global.json")
    }

    fn resolved_overlaybd_resize_global_config_path(&self) -> PathBuf {
        self.ublk
            .overlaybd
            .global_config_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join("resize-overlaybd-global.json")
    }

    fn resolved_cpu_template_helper(&self) -> Option<PathBuf> {
        let version = self
            .firecracker
            .version
            .as_deref()
            .unwrap_or(manifest_firecracker_version(self.virtualization_mode));
        let path = self
            .deps_path
            .join("firecracker")
            .join(version)
            .join("cpu-template-helper");
        path.exists().then_some(path)
    }

    fn image_cache_layout(&self) -> ResolvedImageCacheConfig {
        self.image.cache.layout()
    }

    fn network_pool_config(&self) -> warm_pool::PoolConfig {
        let pool = &self.pool.network;

        warm_pool::PoolConfig {
            low_watermark: self.pool.low_watermark,
            high_watermark: self.pool.high_watermark,
            maintenance_enabled: pool.enabled && pool.maintenance_enabled,
            startup_prewarm: pool.startup_prewarm,
        }
    }

    fn block_pool_config(&self) -> Option<warm_pool::PoolConfig> {
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

    fn firecracker_pool_config(&self) -> Option<ResolvedFirecrackerPoolConfig> {
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
}

/// The checks over sections only `aenv-node` reads. The api half loads the same
/// file and must not run them: it has no ublk, no memory snapshots and no pools.
pub fn validate_node_half(config: &AppConfig) -> Result<()> {
    validate_pool_config(config)?;
    config.egress_broker.validate(&config.cluster)?;
    if config.ublk.overlaybd.resize_timeout_secs == 0 {
        bail!("invalid ublk.overlaybd config: resize_timeout_secs must be > 0");
    }
    if config.ublk.transport == BlockTransport::Nbd {
        if config.ublk.nbd.connections == 0 {
            bail!("invalid ublk.nbd config: connections must be > 0");
        }
        if config.ublk.nbd.io_timeout_secs == 0 {
            bail!("invalid ublk.nbd config: io_timeout_secs must be > 0");
        }
    }
    validate_memory_snapshot_options(config)?;
    validate_memory_snapshot_background_download(config)?;
    validate_overlaybd_global_config_paths(config)?;
    validate_disk_rate_limit(config)?;
    Ok(())
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
fn validate_disk_rate_limit(config: &AppConfig) -> Result<()> {
    let cfg = &config.machine.disk_rate_limit;
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

fn validate_memory_snapshot_options(config: &AppConfig) -> Result<()> {
    let memory = &config.memory_snapshot;
    if memory.backend == MemorySnapshotBackend::Uffd {
        if memory.uffd.max_inflight == 0 {
            bail!("invalid memory_snapshot.uffd config: max_inflight must be > 0");
        }
        if memory.uffd.read_retry_secs == 0 {
            bail!("invalid memory_snapshot.uffd config: read_retry_secs must be > 0");
        }
        if memory.uffd.handshake_timeout_secs == 0 {
            bail!("invalid memory_snapshot.uffd config: handshake_timeout_secs must be > 0");
        }
        // Write protection is what marks a written page under this backend:
        // every page userfaultfd installs is anonymous, so without the uffd-wp
        // bit the only readout left is Firecracker's mincore overapproximation,
        // which copies the whole faulted working set into each pause layer.
        if !memory.track_dirty_pages && !memory.uffd.write_protect {
            if config.virtualization_mode == VirtualizationMode::Pvm {
                bail!(
                    "memory_snapshot.backend=\"uffd\" needs memory_snapshot.track_dirty_pages=true, \
                     which is disabled in PVM mode, so this backend is KVM-only"
                );
            }
            bail!(
                "memory_snapshot.backend=\"uffd\" requires memory_snapshot.track_dirty_pages=true \
                 because every page userfaultfd installs is an anonymous page, so the mincore-based \
                 dirty range readout would copy the whole faulted working set into each pause layer"
            );
        }
    }
    if memory.huge_pages && memory.backend != MemorySnapshotBackend::Uffd {
        bail!(
            "memory_snapshot.huge_pages=true requires memory_snapshot.backend=\"uffd\": Firecracker \
             restores a hugepage-backed snapshot only through a userfaultfd memory backend"
        );
    }
    if !memory.track_dirty_pages {
        return Ok(());
    }
    if config.virtualization_mode == VirtualizationMode::Pvm {
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
fn validate_memory_snapshot_background_download(config: &AppConfig) -> Result<()> {
    const MAX_BLOCK_SIZE: u64 = 64 * 1024 * 1024;
    const MAX_CONCURRENCY: usize = 16;
    const MAX_SCRATCH_BYTES: u64 = 256 * 1024 * 1024;
    let cfg = &config.memory_snapshot.background_download;
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
        .ok_or_else(|| anyhow::anyhow!("memory_snapshot.background_download scratch overflow"))?;
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

fn validate_overlaybd_global_config_paths(config: &AppConfig) -> Result<()> {
    let paths = [
        (
            "ublk.overlaybd.global_config_path",
            &config.ublk.overlaybd.global_config_path,
        ),
        (
            "memory_snapshot.overlaybd_global_config_path",
            &config.memory_snapshot.overlaybd_global_config_path,
        ),
        (
            "derived convert overlaybd global config path",
            &config.resolved_overlaybd_convert_global_config_path(),
        ),
        (
            "derived resize overlaybd global config path",
            &config.resolved_overlaybd_resize_global_config_path(),
        ),
    ];
    let normalized = paths.map(|(name, path)| (name, shell_util::lexically_normalize_path(path)));
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

fn validate_pool_config(config: &AppConfig) -> Result<()> {
    let network = config.network_pool_config();
    if network.maintenance_enabled {
        validate_pool_watermarks("network", &network)?;
    }
    if let Some(block) = config.block_pool_config() {
        validate_pool_watermarks("block", &block)?;
    }
    if let Some(firecracker) = config.firecracker_pool_config() {
        validate_pool_watermarks("firecracker", &firecracker.pool)?;
        if firecracker.fill_concurrency == 0 {
            bail!("invalid firecracker pool config: fill_concurrency must be > 0");
        }
    }
    Ok(())
}

fn validate_pool_watermarks(name: &str, pool: &warm_pool::PoolConfig) -> Result<()> {
    if pool.low_watermark > pool.high_watermark {
        bail!(
            "invalid {name} pool config: low_watermark ({}) must be <= high_watermark ({})",
            pool.low_watermark,
            pool.high_watermark
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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

            let result = validate_memory_snapshot_options(&config);
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

    fn uffd_config(virtualization_mode: VirtualizationMode, track_dirty_pages: bool) -> AppConfig {
        AppConfig {
            virtualization_mode,
            memory_snapshot: MemorySnapshotConfig {
                backend: MemorySnapshotBackend::Uffd,
                track_dirty_pages,
                uffd: MemorySnapshotUffdConfig {
                    write_protect: false,
                    ..Default::default()
                },
                ..Default::default()
            },
            ..Default::default()
        }
    }

    #[test]
    fn uffd_with_write_protection_needs_no_kvm_dirty_log_on_either_mode() {
        for mode in [VirtualizationMode::Kvm, VirtualizationMode::Pvm] {
            let mut config = uffd_config(mode, false);
            config.memory_snapshot.uffd.write_protect = true;
            validate_memory_snapshot_options(&config).unwrap_or_else(|err| panic!("{mode}: {err}"));
        }
    }

    #[test]
    fn validate_memory_snapshot_options_accepts_uffd_with_dirty_page_tracking() {
        validate_memory_snapshot_options(&uffd_config(VirtualizationMode::Kvm, true))
            .expect("uffd with KVM dirty page tracking should be valid");
    }

    #[test]
    fn validate_memory_snapshot_options_rejects_uffd_without_dirty_page_tracking() {
        let error = validate_memory_snapshot_options(&uffd_config(VirtualizationMode::Kvm, false))
            .unwrap_err();
        let message = error.to_string();
        assert!(
            message.contains("requires memory_snapshot.track_dirty_pages=true"),
            "unexpected error: {message}"
        );
        assert!(
            message.contains("anonymous page"),
            "unexpected error: {message}"
        );
    }

    #[test]
    fn validate_memory_snapshot_options_reports_uffd_as_kvm_only_under_pvm() {
        let error = validate_memory_snapshot_options(&uffd_config(VirtualizationMode::Pvm, false))
            .unwrap_err();
        assert!(
            error.to_string().contains("KVM-only"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn validate_memory_snapshot_options_requires_uffd_for_huge_pages() {
        let mut config = AppConfig::default();
        config.memory_snapshot.huge_pages = true;
        let error = validate_memory_snapshot_options(&config).unwrap_err();
        assert!(
            error.to_string().contains("huge_pages=true requires"),
            "unexpected error: {error}"
        );

        let mut config = uffd_config(VirtualizationMode::Kvm, true);
        config.memory_snapshot.huge_pages = true;
        validate_memory_snapshot_options(&config).expect("huge pages over uffd are valid");
    }

    #[test]
    fn validate_memory_snapshot_options_rejects_zero_uffd_bounds() {
        for (max_inflight, read_retry_secs, handshake_timeout_secs, expected) in [
            (0, 60, 60, "max_inflight must be > 0"),
            (64, 0, 60, "read_retry_secs must be > 0"),
            (64, 60, 0, "handshake_timeout_secs must be > 0"),
        ] {
            let mut config = uffd_config(VirtualizationMode::Kvm, true);
            config.memory_snapshot.uffd.max_inflight = max_inflight;
            config.memory_snapshot.uffd.read_retry_secs = read_retry_secs;
            config.memory_snapshot.uffd.handshake_timeout_secs = handshake_timeout_secs;

            let error = validate_memory_snapshot_options(&config).unwrap_err();
            assert!(
                error.to_string().contains(expected),
                "expected error containing {expected:?}, got: {error}"
            );
        }
    }

    #[test]
    fn validate_memory_snapshot_options_ignores_uffd_bounds_on_the_block_backend() {
        let mut config = AppConfig::default();
        config.memory_snapshot.uffd.max_inflight = 0;
        config.memory_snapshot.uffd.read_retry_secs = 0;
        config.memory_snapshot.uffd.handshake_timeout_secs = 0;

        validate_memory_snapshot_options(&config)
            .expect("the block backend reads no uffd settings");
    }

    #[test]
    fn validate_rejects_zero_memory_snapshot_download_concurrency() {
        let mut config = AppConfig::default();
        config.memory_snapshot.background_download.concurrency = 0;

        let err = validate_node_half(&config).unwrap_err();
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

            let err = validate_node_half(&config).unwrap_err();
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
        validate_node_half(&config).expect("disabled disk rate limit config is not validated");
    }

    #[test]
    fn validate_accepts_consistent_disk_rate_limit() {
        let mut config = AppConfig::default();
        config.machine.disk_rate_limit.enabled = true;
        config.machine.disk_rate_limit.bandwidth_bytes_per_sec = 104_857_600;
        config.machine.disk_rate_limit.bandwidth_burst_bytes = 10_485_760;
        config.machine.disk_rate_limit.iops = 3000;
        config.machine.disk_rate_limit.iops_burst = 500;
        validate_node_half(&config).expect("consistent disk rate limit config passes");
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

            let err = validate_node_half(&config).unwrap_err();
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
        assert!(validate_node_half(&config).is_err());

        let mut config = AppConfig::default();
        config.memory_snapshot.background_download.block_size = 65 * 1024 * 1024;
        config.memory_snapshot.background_download.concurrency = 1;
        assert!(validate_node_half(&config).is_err());

        let mut config = AppConfig::default();
        config.memory_snapshot.background_download.block_size = 64 * 1024 * 1024;
        config.memory_snapshot.background_download.concurrency = 8;
        assert!(validate_node_half(&config).is_err());

        let mut config = AppConfig::default();
        config.memory_snapshot.background_download.block_size = 32 * 1024 * 1024;
        config.memory_snapshot.background_download.concurrency = 8;
        validate_node_half(&config).expect("defaults-shaped config passes");

        let mut config = AppConfig::default();
        config.memory_snapshot.background_download.delay = -1;
        assert!(validate_node_half(&config).is_err());

        let mut config = AppConfig::default();
        config.memory_snapshot.background_download.try_cnt = 0;
        assert!(validate_node_half(&config).is_err());
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
    fn managed_dependency_paths_select_the_active_mode_versions() {
        let mut config = AppConfig {
            deps_path: PathBuf::from("/deps"),
            virtualization_mode: VirtualizationMode::Kvm,
            ..Default::default()
        };
        assert_eq!(
            config.resolved_firecracker_binary_path(),
            PathBuf::from("/deps/firecracker")
                .join(manifest_firecracker_version(VirtualizationMode::Kvm))
                .join("firecracker")
        );
        assert_eq!(
            config.resolved_kernel_image_path(),
            PathBuf::from("/deps/kernel")
                .join(manifest_kernel_version(VirtualizationMode::Kvm))
                .join("vmlinux.bin")
        );

        config.virtualization_mode = VirtualizationMode::Pvm;
        assert_eq!(
            config.resolved_firecracker_binary_path(),
            PathBuf::from("/deps/firecracker")
                .join(manifest_firecracker_version(VirtualizationMode::Pvm))
                .join("firecracker")
        );
        assert_eq!(
            config.resolved_kernel_image_path(),
            PathBuf::from("/deps/kernel")
                .join(manifest_kernel_version(VirtualizationMode::Pvm))
                .join("vmlinux.bin")
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
        let err = validate_pool_config(&config).unwrap_err();
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
        assert!(validate_pool_config(&config).is_ok());
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

        let err = validate_pool_config(&config).unwrap_err();
        assert!(
            err.to_string().contains("fill_concurrency"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn validate_rejects_zero_overlaybd_resize_timeout() {
        let mut config = AppConfig::default();
        config.ublk.overlaybd.resize_timeout_secs = 0;
        let err = validate_node_half(&config).unwrap_err();
        assert!(
            err.to_string().contains("resize_timeout_secs must be > 0"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn managed_dependency_paths_resolve_from_deps_path() -> Result<()> {
        let deps_path = PathBuf::from("/deps");
        let mut config = AppConfig {
            deps_path: deps_path.clone(),
            ..Default::default()
        };
        config.firecracker.version = Some("fc-test".to_string());
        config.kernel.version = Some("kernel-test".to_string());
        config.tools.version = Some("1.2.3-custom.1".to_string());

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
    fn the_node_half_accepts_what_only_the_api_half_reads() {
        let mut config = AppConfig::default();
        config.cluster.placement_shadow_k = 0;
        config.secrets.backend = SecretsBackendKind::Postgres;
        config.secrets.pg.key_file = None;

        validate_node_half(&config).expect(
            "the node half reads none of these sections, so a file that carries them must \
             still start it",
        );
    }

    #[test]
    fn the_shipped_default_config_passes_the_node_half_checks() {
        let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        ConfigManager::new_from_path(&workspace.join("config/default.toml"), validate_node_half)
            .expect("the shipped default must load on the node half");
    }
}
