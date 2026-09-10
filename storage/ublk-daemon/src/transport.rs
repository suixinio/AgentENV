//! The kernel block transport the daemon exposes overlaybd images through.
//!
//! One `BlockDevice` per exposed image; everything above this module names
//! devices by `dev_id` and path and never sees which driver is underneath.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use overlaybd::image_file::ImageFile;
use serde::Deserialize;
use storage_util::io_ring::IoRingHandle;
use uvm_ublk::{
    delete_dev, wait_for_ublk_dev, UVMUblkCtrlBuilder, UVMUblkDev, UVMUblkDevBuilder, UVMUblkTarget,
};

const QUIESCE_TIMEOUT: Duration = Duration::from_secs(5);

/// The protocol counts capacity in 512-byte sectors, matching `num_lbas()`.
const SECTOR_SIZE: u64 = 512;

/// Which kernel driver backs the daemon's devices. Chosen once per process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Transport {
    #[default]
    Ublk,
    Nbd,
}

impl FromStr for Transport {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "ublk" => Ok(Self::Ublk),
            "nbd" => Ok(Self::Nbd),
            other => bail!("unknown block transport `{other}`, expected `ublk` or `nbd`"),
        }
    }
}

impl std::fmt::Display for Transport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Ublk => "ublk",
            Self::Nbd => "nbd",
        })
    }
}

/// What a transport needs to create and destroy devices: the ublk control ring,
/// or the options every nbd device is brought up with.
#[derive(Clone)]
pub enum TransportHandle {
    Ublk(IoRingHandle<io_uring::squeue::Entry128>),
    Nbd(uvm_nbd::NbdOptions),
}

impl TransportHandle {
    pub fn transport(&self) -> Transport {
        match self {
            Self::Ublk(_) => Transport::Ublk,
            Self::Nbd(_) => Transport::Nbd,
        }
    }
}

impl std::fmt::Debug for TransportHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ublk(_) => formatter.write_str("TransportHandle::Ublk"),
            Self::Nbd(options) => formatter
                .debug_tuple("TransportHandle::Nbd")
                .field(options)
                .finish(),
        }
    }
}

/// One live block device serving an overlaybd image.
pub(crate) enum BlockDevice {
    // Boxed: a ublk device is two orders of magnitude larger than the two Arcs
    // the nbd variant holds.
    Ublk(Box<UVMUblkDev<uvm_ublk::OverlaybdTarget>>),
    Nbd {
        // Owned through an Arc so a resize can be detached from the caller's
        // map guard: awaiting a resize while holding a DashMap shard lock
        // deadlocks against a concurrent release on the same shard.
        device: Arc<uvm_nbd::NbdDevice>,
        target: Arc<uvm_nbd::OverlaybdTarget>,
    },
}

impl BlockDevice {
    pub(crate) fn dev_id(&self) -> u32 {
        match self {
            Self::Ublk(dev) => dev.dev_id(),
            Self::Nbd { device, .. } => device.index(),
        }
    }

    pub(crate) fn device_path(&self) -> PathBuf {
        match self {
            Self::Ublk(dev) => dev.device_path(),
            Self::Nbd { device, .. } => device.device_path().to_path_buf(),
        }
    }

    /// Rebind the device to another image. The device must be idle.
    pub(crate) fn swap_state(
        &self,
        image_config: PathBuf,
        image: Arc<ImageFile>,
        writable: bool,
    ) -> Result<()> {
        match self {
            Self::Ublk(dev) => dev.target().swap_state(image_config, image, writable),
            Self::Nbd { target, .. } => target.swap_state(image_config, image, writable),
        }
    }

    /// A resize that borrows nothing, so a caller holding a map guard can drop
    /// it before awaiting. Under nbd the target's own bound grows before the
    /// kernel's, or the first read past the old end is answered EINVAL.
    pub(crate) fn update_size(
        &self,
        new_sectors: u64,
    ) -> impl Future<Output = Result<()>> + Send + 'static {
        enum Resize {
            Ublk(std::pin::Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>>),
            Nbd(Arc<uvm_nbd::NbdDevice>, Arc<uvm_nbd::OverlaybdTarget>),
        }

        let resize = match self {
            Self::Ublk(dev) => Resize::Ublk(Box::pin(dev.ctrl.update_size(new_sectors))),
            Self::Nbd { device, target } => Resize::Nbd(Arc::clone(device), Arc::clone(target)),
        };

        async move {
            match resize {
                Resize::Ublk(future) => future.await,
                Resize::Nbd(device, target) => {
                    target
                        .set_dev_sectors(new_sectors)
                        .context("grow the nbd target before the kernel device")?;
                    device.update_size(new_sectors * SECTOR_SIZE).await
                }
            }
        }
    }

    /// Tear the device down: quiesce its workers, then remove it from the kernel.
    pub(crate) async fn stop(self, handle: &TransportHandle) {
        match (self, handle) {
            (Self::Ublk(mut dev), TransportHandle::Ublk(ctrl_ring)) => {
                let dev_id = dev.dev_id();
                quiesce_ublk_device(&mut dev).await;
                // The device holds an open fd to the ublk char dev; DEL_DEV
                // blocks until it is closed.
                drop(dev);
                if let Err(err) = delete_dev(ctrl_ring.clone(), dev_id).await {
                    tracing::warn!(dev_id, ?err, "failed to delete ublk device");
                }
            }
            (Self::Nbd { device, target }, _) => {
                drop(target);
                let index = device.index();
                match Arc::try_unwrap(device) {
                    Ok(device) => {
                        if let Err(err) = device.stop().await {
                            tracing::warn!(index, ?err, "failed to disconnect nbd device");
                        }
                    }
                    Err(device) => {
                        tracing::error!(
                            index,
                            "nbd device still has a detached resize in flight at stop; \
                             disconnecting without joining its workers"
                        );
                        drop(device);
                    }
                }
            }
            (Self::Ublk(dev), TransportHandle::Nbd(_)) => {
                tracing::error!(
                    dev_id = dev.dev_id(),
                    "a ublk device reached an nbd transport handle; leaking it rather than \
                     deleting the wrong device"
                );
            }
        }
    }
}

/// Create and start a device serving `image`.
pub(crate) async fn create_device(
    handle: &TransportHandle,
    image_config: &Path,
    image: &Arc<ImageFile>,
) -> Result<BlockDevice> {
    let writable = !image.is_read_only().await;
    match handle {
        TransportHandle::Ublk(ctrl_ring) => {
            let target = uvm_ublk::OverlaybdTarget::from_opened_image(
                image_config.to_path_buf(),
                Arc::clone(image),
                writable,
            )
            .context("create overlaybd target")?;

            let ctrl = UVMUblkCtrlBuilder::new()
                .name("overlaybd-blk")
                .build(ctrl_ring.clone())
                .context("build ublk ctrl")?;

            let mut dev = UVMUblkDevBuilder::new(ctrl)
                .set_target(target)
                .build()
                .await
                .context("build ublk dev")?;

            let dev_id = dev.dev_id();
            if let Err(err) = dev.start().await.context("start ublk dev") {
                cleanup_failed_ublk_start(ctrl_ring.clone(), dev).await;
                return Err(err);
            }
            if let Err(err) = wait_for_ublk_dev(dev_id).context("wait for ublk device") {
                cleanup_failed_ublk_start(ctrl_ring.clone(), dev).await;
                return Err(err);
            }

            tracing::debug!(dev_id, path = %dev.device_path().display(), "created new ublk device");
            Ok(BlockDevice::Ublk(Box::new(dev)))
        }
        TransportHandle::Nbd(options) => {
            let target = Arc::new(
                uvm_nbd::OverlaybdTarget::from_opened_image(
                    image_config.to_path_buf(),
                    Arc::clone(image),
                    writable,
                )
                .context("create overlaybd target")?,
            );
            let device = uvm_nbd::NbdDevice::start(Arc::clone(&target), options.clone())
                .await
                .context("start nbd device")?;
            tracing::debug!(
                dev_id = device.index(),
                path = %device.device_path().display(),
                "created new nbd device"
            );
            Ok(BlockDevice::Nbd {
                device: Arc::new(device),
                target,
            })
        }
    }
}

async fn quiesce_ublk_device<T: UVMUblkTarget>(dev: &mut UVMUblkDev<T>) {
    let dev_id = dev.dev_id();

    if let Err(err) = dev.ctrl.stop_dev().await {
        tracing::warn!(dev_id, ?err, "failed to stop ublk device before delete");
        return;
    }

    if let Err(err) = tokio::time::timeout(QUIESCE_TIMEOUT, dev.wait_for_bg_tasks()).await {
        tracing::warn!(
            dev_id,
            ?err,
            "timed out waiting for ublk queue workers to exit after stop_dev"
        );
    }
}

async fn cleanup_failed_ublk_start<T: UVMUblkTarget>(
    ctrl_ring: IoRingHandle<io_uring::squeue::Entry128>,
    mut dev: UVMUblkDev<T>,
) {
    let dev_id = dev.dev_id();

    match dev.ctrl.stop_dev().await {
        Ok(()) => {}
        Err(err) if is_enodev(&err) => {
            tracing::info!(dev_id, "ublk device disappeared before startup cleanup");
            drop(dev);
            return;
        }
        Err(err) => {
            tracing::warn!(
                dev_id,
                ?err,
                "failed to stop ublk device after startup failure"
            );
        }
    }

    if let Err(err) = tokio::time::timeout(QUIESCE_TIMEOUT, dev.wait_for_bg_tasks()).await {
        tracing::warn!(
            dev_id,
            ?err,
            "timed out waiting for ublk queue workers after startup failure"
        );
    }

    drop(dev);

    let mut ctrl = match UVMUblkCtrlBuilder::new().dev_id(dev_id).build(ctrl_ring) {
        Ok(ctrl) => ctrl,
        Err(err) => {
            tracing::warn!(
                dev_id,
                ?err,
                "failed to build ublk ctrl for startup cleanup; kernel device may remain active"
            );
            return;
        }
    };

    match ctrl.del_dev().await {
        Ok(()) => {
            tracing::info!(dev_id, "deleted ublk device after startup failure");
        }
        Err(err) if is_enodev(&err) => {
            tracing::info!(dev_id, "ublk device already deleted after startup failure");
        }
        Err(err) => {
            tracing::warn!(
                dev_id,
                ?err,
                "failed to delete ublk device after startup failure; kernel device may remain active"
            );
        }
    }
}

/// Whether this process could bring an nbd device up here: the module is
/// loaded and either a device node is reachable or none exists yet, in which
/// case the kernel creates one on connect.
pub fn nbd_transport_usable() -> bool {
    if !Path::new("/sys/module/nbd").exists() {
        return false;
    }
    let Ok(entries) = std::fs::read_dir("/dev") else {
        return false;
    };
    let mut nodes = 0usize;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if !name.starts_with("nbd") || !name[3..].chars().all(|c| c.is_ascii_digit()) {
            continue;
        }
        nodes += 1;
        let Ok(path) = std::ffi::CString::new(entry.path().as_os_str().as_encoded_bytes()) else {
            continue;
        };
        // SAFETY: `path` is a valid NUL-terminated string for the call.
        if unsafe { libc::access(path.as_ptr(), libc::R_OK | libc::W_OK) } == 0 {
            return true;
        }
    }
    nodes == 0
}

fn is_enodev(err: &anyhow::Error) -> bool {
    matches!(
        err.root_cause()
            .downcast_ref::<std::io::Error>()
            .and_then(|err| err.raw_os_error()),
        Some(libc::ENODEV)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use overlaybd::config::UpperMode;
    use overlaybd::helper::prepare_runtime_upper;
    use overlaybd::image_service::ImageService;
    use tempfile::TempDir;
    use uvm_nbd::NbdTarget;

    #[test]
    fn a_transport_name_round_trips_through_parse_and_display() {
        assert_eq!(Transport::from_str("ublk").unwrap(), Transport::Ublk);
        assert_eq!(Transport::from_str("nbd").unwrap(), Transport::Nbd);
        assert_eq!(Transport::from_str(" NBD ").unwrap(), Transport::Nbd);
        assert_eq!(Transport::Ublk.to_string(), "ublk");
        assert_eq!(Transport::Nbd.to_string(), "nbd");
        assert_eq!(Transport::default(), Transport::Ublk);
    }

    #[test]
    fn an_unknown_transport_name_is_refused_by_name() {
        let err = Transport::from_str("virtio").unwrap_err().to_string();
        assert!(err.contains("virtio"), "{err}");
        assert!(err.contains("ublk"), "{err}");
        assert!(err.contains("nbd"), "{err}");
    }

    #[test]
    fn a_transport_deserializes_from_a_lowercase_toml_value() {
        #[derive(Deserialize)]
        struct Holder {
            transport: Transport,
        }
        let holder: Holder = toml::from_str(r#"transport = "nbd""#).unwrap();
        assert_eq!(holder.transport, Transport::Nbd);
        assert!(toml::from_str::<Holder>(r#"transport = "Nbd""#).is_err());
    }

    #[test]
    fn a_handle_names_the_transport_it_carries() {
        let handle = TransportHandle::Nbd(uvm_nbd::NbdOptions::default());
        assert_eq!(handle.transport(), Transport::Nbd);
        assert!(format!("{handle:?}").contains("Nbd"));
    }

    /// Live devices belong to the nbd transport suite, so `make test-unit`
    /// (which never selects it) creates none.
    fn nbd_available(test: &str) -> bool {
        let selected = std::env::var("AENV_DAEMON_TEST_TRANSPORT").unwrap_or_default();
        let reason = if Transport::from_str(&selected).unwrap_or_default() != Transport::Nbd {
            "AENV_DAEMON_TEST_TRANSPORT does not select nbd"
        } else if !nbd_transport_usable() {
            "the nbd transport is not reachable here"
        } else {
            return true;
        };
        if std::env::var("AENV_NBD_TEST_REQUIRED").as_deref() == Ok("1")
            && reason.starts_with("the nbd")
        {
            panic!("AENV_NBD_TEST_REQUIRED=1 but {test} cannot run: {reason}");
        }
        eprintln!("SKIPPED[nbd]: {test} ({reason})");
        false
    }

    async fn sparse_image(dir: &TempDir, virtual_size: u64) -> (PathBuf, Arc<ImageFile>) {
        let cache_dir = dir.path().join("cache");
        std::fs::create_dir_all(&cache_dir).unwrap();
        let global_config = dir.path().join("global.json");
        std::fs::write(
            &global_config,
            serde_json::to_vec(&serde_json::json!({
                "registryFsVersion": "v2",
                "nrIoRings": 1,
                "cacheConfig": {
                    "cacheType": "file",
                    "cacheDir": cache_dir,
                    "cacheSizeGB": 1,
                    "refillSize": 262144,
                    "blockSize": 65536
                },
                "download": { "enable": false }
            }))
            .unwrap(),
        )
        .unwrap();

        let upper_data = dir.path().join("upper.data");
        prepare_runtime_upper(&upper_data, None, virtual_size, UpperMode::Sparse).unwrap();
        let image_config = dir.path().join("image.json");
        std::fs::write(
            &image_config,
            serde_json::to_vec(&serde_json::json!({
                "lowers": [],
                "upper": { "mode": "sparse", "data": upper_data },
                "resultFile": dir.path().join("result.txt")
            }))
            .unwrap(),
        )
        .unwrap();

        let service = ImageService::from_config_path(&global_config)
            .await
            .unwrap();
        let image = Arc::new(service.create_image_file(&image_config).await.unwrap());
        (image_config, image)
    }

    #[tokio::test]
    async fn an_nbd_resize_grows_the_target_bound_before_the_kernel_device() {
        let name = "an_nbd_resize_grows_the_target_bound_before_the_kernel_device";
        if !nbd_available(name) {
            return;
        }
        let dir = TempDir::new().unwrap();
        let (image_config, image) = sparse_image(&dir, 64 * 1024 * 1024).await;

        let target = Arc::new(
            uvm_nbd::OverlaybdTarget::from_opened_image(image_config, Arc::clone(&image), true)
                .unwrap(),
        );
        let half_sectors = image.num_lbas() / 2;
        target.set_dev_sectors(half_sectors).unwrap();
        let device = uvm_nbd::NbdDevice::start(
            Arc::clone(&target),
            uvm_nbd::NbdOptions {
                connections: 2,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let index = device.index();
        let block = BlockDevice::Nbd {
            device: Arc::new(device),
            target: Arc::clone(&target),
        };

        assert_eq!(
            uvm_nbd::device_size_bytes(index).unwrap(),
            half_sectors * 512
        );

        let full_sectors = image.num_lbas();
        block.update_size(full_sectors).await.unwrap();

        assert_eq!(
            target.geometry().size_bytes,
            full_sectors * 512,
            "the target bound must grow, or every read past the old end answers EINVAL"
        );
        assert_eq!(
            uvm_nbd::device_size_bytes(index).unwrap(),
            full_sectors * 512
        );

        let past_old_end = half_sectors * 512 + 4096;
        let mut buf = vec![0u8; 4096];
        let file = std::fs::File::open(block.device_path()).unwrap();
        std::os::unix::fs::FileExt::read_exact_at(&file, &mut buf, past_old_end)
            .expect("a read past the pre-resize bound must be served");
        drop(file);

        // A capacity the target refuses must leave the kernel device alone,
        // which only holds while the target is updated first.
        let before = uvm_nbd::device_size_bytes(index).unwrap();
        assert!(block.update_size(0).await.is_err());
        assert_eq!(
            uvm_nbd::device_size_bytes(index).unwrap(),
            before,
            "the kernel device was resized before the target accepted the new bound"
        );

        block
            .stop(&TransportHandle::Nbd(uvm_nbd::NbdOptions::default()))
            .await;
    }
}
