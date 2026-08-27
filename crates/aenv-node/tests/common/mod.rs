use std::path::{Path, PathBuf};
use std::sync::Arc;

use aenv_node::cfg::ConfigManager;
use aenv_node::image::ImageResolver;
use aenv_node::sandbox::{FirecrackerSandboxConfig, OverlaybdConfig, UblkDeviceManager};
use aenv_node::snapshot::repository::backends::storage::{PosixFsBackend, PosixFsBackendConfig};
use aenv_node::snapshot::repository::SnapshotRepository;
use aenv_node::snapshot::SnapshotManager;
use aenv_node::template::{TemplateBuildSpec, TemplateBuilder};
use anyhow::Result;
use tokio::sync::OnceCell;

const TEST_ROOTFS_IMAGE: &str = "ghcr.io/linuxserver/baseimage-ubuntu:noble";
static DEFAULT_ROOTFS_IMAGE_CONFIG: OnceCell<PathBuf> = OnceCell::const_new();

/// Standard setup for integration tests.
///
/// Initializes logging, resolves the default template rootfs image, and
/// initializes the global ublk device manager (spawning the daemon if ublk is
/// enabled in the config). Safe to call multiple times within the same process.
pub async fn setup() {
    let config = setup_runtime_only().await;
    DEFAULT_ROOTFS_IMAGE_CONFIG
        .get_or_init(|| async {
            let resolved = ImageResolver::new(config)
                .resolve(TEST_ROOTFS_IMAGE)
                .await
                .expect("failed to resolve default template rootfs image");
            resolved.overlaybd_config_path
        })
        .await;
}

/// Initialize logging, config, and the global ublk manager without resolving a
/// registry image.
pub async fn setup_runtime_only() -> &'static aenv_node::cfg::AppConfig {
    aenv_node::logging::init_for_tests();
    std::env::set_var(
        "AENV_SANDBOX_ACCESS_TOKEN_HASH_SEED",
        "integration-test-seed",
    );
    let config = ConfigManager::init_global()
        .expect("config manager")
        .config();
    UblkDeviceManager::init_global_from_config(config)
        .await
        .expect("failed to initialize ublk device manager");
    config
}

#[allow(dead_code)]
pub fn default_rootfs_template_build_spec() -> TemplateBuildSpec {
    let image_config_path = DEFAULT_ROOTFS_IMAGE_CONFIG
        .get()
        .expect("common::setup() must run before using the default rootfs image config")
        .clone();
    TemplateBuildSpec::new().from_overlaybd_config(image_config_path)
}

#[allow(dead_code)]
pub fn default_rootfs_template_build_spec_with_image_config() -> (TemplateBuildSpec, PathBuf) {
    let image_config_path = DEFAULT_ROOTFS_IMAGE_CONFIG
        .get()
        .expect("common::setup() must run before using the default rootfs image config")
        .clone();
    (
        TemplateBuildSpec::new().from_overlaybd_config(image_config_path.clone()),
        image_config_path,
    )
}

#[allow(dead_code)]
pub fn default_sandbox_config() -> Result<FirecrackerSandboxConfig> {
    let image_config_path = DEFAULT_ROOTFS_IMAGE_CONFIG
        .get()
        .expect("common::setup() must run before using the default rootfs image config")
        .clone();
    FirecrackerSandboxConfig::from_global_config_with_user_image(OverlaybdConfig {
        image_config_path,
        read_only: false,
        runtime_upper_mode: overlaybd::config::UpperMode::LogStructured,
    })
}

#[allow(dead_code)]
pub fn snapshot_test_parts(
    root: &Path,
) -> (TemplateBuilder, SnapshotManager, Arc<SnapshotRepository>) {
    let backend = PosixFsBackend::new(PosixFsBackendConfig {
        root: root.join("repository"),
        cache_root: Some(root.join("runtime-cache")),
        runtime_cache_root: Some(root.join("runtime-cache").join("runtime")),
    })
    .expect("posix backend");
    let repository = backend.repository();
    let runtime_resolver = backend.runtime_resolver();
    let snapshot_manager = Arc::new(SnapshotManager::from_parts(
        Arc::clone(&repository),
        Some(Arc::clone(&runtime_resolver)),
        None,
    ));
    (
        TemplateBuilder::new(),
        snapshot_manager.as_ref().clone(),
        repository,
    )
}
