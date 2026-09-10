use anyhow::{bail, Context, Result};
use arc_swap::ArcSwap;
use async_trait::async_trait;
use overlaybd::image_file::ImageFile;
use overlaybd::image_service::ImageService;
use overlaybd::virtual_file::{IoCtx, VirtualFile};
use std::fmt;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use storage_util::io_ring::AsyncIoRing;

use crate::target::{Geometry, NbdTarget};

#[derive(Clone)]
struct TargetState {
    image_config_path: PathBuf,
    image: Arc<ImageFile>,
    dev_sectors: u64,
    block_size: u32,
    writable: bool,
}

impl TargetState {
    fn dev_bytes(&self) -> u64 {
        self.dev_sectors * u64::from(self.block_size)
    }
}

/// An overlaybd image exposed as an nbd target. The backing image is swapped
/// under a live device by [`OverlaybdTarget::swap_state`].
pub struct OverlaybdTarget {
    state: ArcSwap<TargetState>,
}

impl fmt::Debug for OverlaybdTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.state.load();
        f.debug_struct("OverlaybdTarget")
            .field("image_config_path", &state.image_config_path)
            .field("dev_sectors", &state.dev_sectors)
            .field("block_size", &state.block_size)
            .field("writable", &state.writable)
            .finish_non_exhaustive()
    }
}

impl OverlaybdTarget {
    pub async fn open(
        global_config_path: impl AsRef<Path>,
        image_config_path: impl AsRef<Path>,
    ) -> Result<Self> {
        let global_config_path = global_config_path.as_ref().to_path_buf();
        let image_config_path = image_config_path.as_ref().to_path_buf();
        let image_service = ImageService::from_config_path(&global_config_path)
            .await
            .with_context(|| {
                format!(
                    "open overlaybd image service failed: {}",
                    global_config_path.display()
                )
            })?;
        let image = Arc::new(
            image_service
                .create_image_file(&image_config_path)
                .await
                .with_context(|| {
                    format!(
                        "open overlaybd image file failed: {}",
                        image_config_path.display()
                    )
                })?,
        );
        let writable = !image.is_read_only().await;
        Self::from_opened_image(image_config_path, image, writable)
    }

    pub fn from_opened_image(
        image_config_path: PathBuf,
        image: Arc<ImageFile>,
        writable: bool,
    ) -> Result<Self> {
        let state = build_state(image_config_path, image, writable)?;
        Ok(Self {
            state: ArcSwap::new(Arc::new(state)),
        })
    }

    /// Swaps the backing image and capacity atomically; the device must be idle,
    /// and a request that already loaded the previous state keeps using it.
    pub fn swap_state(
        &self,
        image_config_path: PathBuf,
        image: Arc<ImageFile>,
        writable: bool,
    ) -> Result<()> {
        let state = build_state(image_config_path, image, writable)?;
        self.state.store(Arc::new(state));
        Ok(())
    }

    /// Refuse writes from here on without changing the backing image.
    pub fn set_read_only(&self) {
        let mut state = TargetState::clone(&self.state.load());
        state.writable = false;
        self.state.store(Arc::new(state));
    }

    /// Re-advertise the capacity without changing the backing image, for a
    /// device whose image grew under it.
    pub fn set_dev_sectors(&self, dev_sectors: u64) -> Result<()> {
        if dev_sectors == 0 {
            bail!("overlaybd nbd target capacity must be non-zero");
        }
        let mut state = TargetState::clone(&self.state.load());
        state.dev_sectors = dev_sectors;
        self.state.store(Arc::new(state));
        Ok(())
    }

    fn checked_range(state: &TargetState, offset: u64, len: u64) -> std::result::Result<(), i32> {
        let end = offset.checked_add(len).ok_or(-libc::EINVAL)?;
        if end > state.dev_bytes() {
            return Err(-libc::EINVAL);
        }
        Ok(())
    }
}

fn build_state(
    image_config_path: PathBuf,
    image: Arc<ImageFile>,
    writable: bool,
) -> Result<TargetState> {
    let block_size = image.block_size;
    if block_size == 0 || !block_size.is_power_of_two() {
        bail!("overlaybd block size must be power-of-two, got {block_size}");
    }
    let dev_sectors = image.num_lbas();
    if dev_sectors == 0 {
        bail!(
            "overlaybd image has zero capacity: {}",
            image_config_path.display()
        );
    }
    Ok(TargetState {
        image_config_path,
        image,
        dev_sectors,
        block_size,
        writable,
    })
}

fn anyhow_to_errno(err: &anyhow::Error) -> i32 {
    if let Some(io_err) = err.downcast_ref::<std::io::Error>() {
        if let Some(errno) = io_err.raw_os_error() {
            return -errno;
        }
        let errno = match io_err.kind() {
            ErrorKind::PermissionDenied => libc::EROFS,
            ErrorKind::Unsupported => libc::EOPNOTSUPP,
            ErrorKind::InvalidInput | ErrorKind::InvalidData => libc::EINVAL,
            ErrorKind::WouldBlock => libc::EAGAIN,
            ErrorKind::TimedOut => libc::ETIMEDOUT,
            ErrorKind::Interrupted => libc::EINTR,
            ErrorKind::UnexpectedEof | ErrorKind::WriteZero => libc::EIO,
            _ => libc::EIO,
        };
        return -errno;
    }
    -libc::EIO
}

#[async_trait(?Send)]
impl NbdTarget for OverlaybdTarget {
    const DEV_NAME: &str = "overlaybd";

    fn geometry(&self) -> Geometry {
        let state = self.state.load();
        Geometry {
            size_bytes: state.dev_bytes(),
            block_size: state.block_size,
            read_only: !state.writable,
            supports_discard: state.writable,
        }
    }

    async fn init(&self, _conn: u16, _io_ring: &AsyncIoRing) -> Result<()> {
        Ok(())
    }

    async fn read(self: &Arc<Self>, io_ring: &AsyncIoRing, offset: u64, buf: &mut [u8]) -> i32 {
        if buf.is_empty() {
            return 0;
        }
        let state = self.state.load_full();
        if let Err(errno) = Self::checked_range(&state, offset, buf.len() as u64) {
            return errno;
        }
        let len = buf.len();
        let ctx = IoCtx::new(io_ring);
        let result = state
            .image
            .read_at_into_with_ctx(ctx, offset, buf)
            .await
            .and_then(|read| {
                if read == len {
                    Ok(())
                } else {
                    bail!("overlaybd short read at {offset}: expect {len}, got {read}")
                }
            });
        report(result.map(|()| 0), "read", offset, len as u64)
    }

    async fn write(
        self: &Arc<Self>,
        io_ring: &AsyncIoRing,
        offset: u64,
        data: &[u8],
        fua: bool,
    ) -> i32 {
        if data.is_empty() {
            return 0;
        }
        let state = self.state.load_full();
        if !state.writable {
            return -libc::EPERM;
        }
        if let Err(errno) = Self::checked_range(&state, offset, data.len() as u64) {
            return errno;
        }
        let len = data.len();
        let ctx = IoCtx::new(io_ring);
        let result = match state.image.write_at_with_ctx(ctx, offset, data).await {
            Ok(written) if written == len => {
                if fua {
                    state.image.sync().await.map(|()| 0)
                } else {
                    Ok(0)
                }
            }
            Ok(written) => Err(anyhow::anyhow!(
                "overlaybd short write at {offset}: expect {len}, wrote {written}"
            )),
            Err(err) => Err(err),
        };
        report(result, "write", offset, len as u64)
    }

    async fn flush(self: &Arc<Self>, _io_ring: &AsyncIoRing) -> i32 {
        let state = self.state.load_full();
        report(state.image.sync().await.map(|()| 0), "flush", 0, 0)
    }

    async fn discard(self: &Arc<Self>, _io_ring: &AsyncIoRing, offset: u64, len: u64) -> i32 {
        if len == 0 {
            return 0;
        }
        let state = self.state.load_full();
        if !state.writable {
            return -libc::EPERM;
        }
        if let Err(errno) = Self::checked_range(&state, offset, len) {
            return errno;
        }
        report(
            state.image.discard(offset, len).await.map(|()| 0),
            "discard",
            offset,
            len,
        )
    }
}

fn report(result: Result<i32>, op: &'static str, offset: u64, len: u64) -> i32 {
    match result {
        Ok(value) => value,
        Err(err) => {
            let errno = anyhow_to_errno(&err);
            tracing::error!(op, offset, len, ?err, errno, "overlaybd nbd request failed");
            errno
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use overlaybd::config::UpperMode;
    use overlaybd::helper::prepare_runtime_upper;
    use std::io::Error;
    use storage_util::io_ring::AsyncIoRingBuilder;
    use tempfile::TempDir;

    #[test]
    fn an_io_error_keeps_its_errno_through_the_anyhow_wrapper() {
        let to_errno = |err: Error| anyhow_to_errno(&anyhow::Error::from(err));
        assert_eq!(
            to_errno(Error::new(ErrorKind::PermissionDenied, "ro")),
            -libc::EROFS
        );
        assert_eq!(
            to_errno(Error::new(ErrorKind::Unsupported, "unsupported")),
            -libc::EOPNOTSUPP
        );
        assert_eq!(
            to_errno(Error::from_raw_os_error(libc::ENOENT)),
            -libc::ENOENT
        );
        assert_eq!(anyhow_to_errno(&anyhow::anyhow!("unknown")), -libc::EIO);
    }

    fn write_global_config(tmp: &TempDir) -> Result<PathBuf> {
        let path = tmp.path().join("overlaybd-global.json");
        std::fs::write(
            &path,
            serde_json::to_vec_pretty(&serde_json::json!({
                "registryFsVersion": "v2",
                "nrIoRings": 1,
                "cacheConfig": {
                    "cacheType": "file",
                    "cacheDir": tmp.path().join("cache"),
                    "cacheSizeGB": 1,
                    "refillSize": 262144,
                    "blockSize": 65536
                },
                "download": { "enable": false }
            }))?,
        )?;
        Ok(path)
    }

    fn write_sparse_image_config(tmp: &TempDir, upper_data: &Path) -> Result<PathBuf> {
        let path = tmp.path().join("overlaybd-image.json");
        std::fs::write(
            &path,
            serde_json::to_vec_pretty(&serde_json::json!({
                "lowers": [],
                "upper": { "mode": "sparse", "data": upper_data },
                "resultFile": tmp.path().join("result.txt")
            }))?,
        )?;
        Ok(path)
    }

    async fn open_target(tmp: &TempDir, size_bytes: u64) -> Arc<OverlaybdTarget> {
        let global_config = write_global_config(tmp).expect("write global config");
        let upper_data = tmp.path().join("upper.data");
        prepare_runtime_upper(&upper_data, None, size_bytes, UpperMode::Sparse)
            .expect("prepare sparse upper");
        let image_config = write_sparse_image_config(tmp, &upper_data).expect("write image config");
        Arc::new(
            OverlaybdTarget::open(&global_config, &image_config)
                .await
                .expect("open overlaybd nbd target"),
        )
    }

    #[tokio::test(flavor = "current_thread")]
    async fn the_geometry_follows_the_image_capacity_and_block_size() {
        let tmp = TempDir::new().expect("tempdir");
        let target = open_target(&tmp, 1 << 20).await;
        let geometry = target.geometry();
        assert_eq!(geometry.block_size, 512);
        assert_eq!(geometry.size_bytes, 1 << 20);
        assert!(!geometry.read_only);
        assert!(geometry.supports_discard);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_request_past_the_image_capacity_is_refused_as_einval() {
        let tmp = TempDir::new().expect("tempdir");
        let target = open_target(&tmp, 1 << 20).await;
        let ring = AsyncIoRingBuilder::new()
            .nr_sparse_buffer(2)
            .nr_sparse_file(4)
            .sqe_entries(8)
            .cqe_entries(16)
            .build()
            .expect("build async io ring");
        let size = target.geometry().size_bytes;
        let mut buf = vec![0u8; 512];
        assert_eq!(target.read(&ring, size, &mut buf).await, -libc::EINVAL);
        assert_eq!(
            target.write(&ring, size, &[0u8; 512], false).await,
            -libc::EINVAL
        );
        assert_eq!(target.discard(&ring, size, 512).await, -libc::EINVAL);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_swapped_state_answers_from_the_new_image() {
        let first = TempDir::new().expect("tempdir");
        let target = open_target(&first, 1 << 20).await;
        let ring = AsyncIoRingBuilder::new()
            .nr_sparse_buffer(2)
            .nr_sparse_file(4)
            .sqe_entries(8)
            .cqe_entries(16)
            .build()
            .expect("build async io ring");
        assert_eq!(target.write(&ring, 0, &[0x11u8; 512], false).await, 0);

        let second = TempDir::new().expect("tempdir");
        let global_config = write_global_config(&second).expect("write global config");
        let upper_data = second.path().join("upper.data");
        prepare_runtime_upper(&upper_data, None, 1 << 19, UpperMode::Sparse)
            .expect("prepare sparse upper");
        let image_config =
            write_sparse_image_config(&second, &upper_data).expect("write image config");
        let service = ImageService::from_config_path(&global_config)
            .await
            .expect("open image service");
        let image = Arc::new(
            service
                .create_image_file(&image_config)
                .await
                .expect("open image"),
        );
        target
            .swap_state(image_config, image, true)
            .expect("swap state");

        assert_eq!(target.geometry().size_bytes, 1 << 19);
        let mut buf = vec![0u8; 512];
        assert_eq!(target.read(&ring, 0, &mut buf).await, 0);
        assert!(
            buf.iter().all(|&byte| byte == 0),
            "the swapped-in image must not answer with the previous image's bytes"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_target_made_read_only_stops_advertising_discard() {
        let tmp = TempDir::new().expect("tempdir");
        let target = open_target(&tmp, 1 << 20).await;
        assert!(!target.geometry().read_only);
        assert!(target.geometry().supports_discard);
        target.set_read_only();
        let geometry = target.geometry();
        assert!(geometry.read_only);
        assert!(!geometry.supports_discard);
        assert_eq!(geometry.size_bytes, 1 << 20);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_new_capacity_widens_the_range_the_target_answers() {
        let tmp = TempDir::new().expect("tempdir");
        let target = open_target(&tmp, 1 << 20).await;
        let ring = AsyncIoRingBuilder::new()
            .nr_sparse_buffer(2)
            .nr_sparse_file(4)
            .sqe_entries(8)
            .cqe_entries(16)
            .build()
            .expect("build async io ring");
        target.set_dev_sectors(1024).expect("shrink");
        assert_eq!(target.geometry().size_bytes, 1024 * 512);
        let mut buf = vec![0u8; 512];
        assert_eq!(
            target.read(&ring, 1024 * 512, &mut buf).await,
            -libc::EINVAL
        );

        target.set_dev_sectors(2048).expect("grow");
        assert_eq!(target.geometry().size_bytes, 2048 * 512);
        assert_eq!(target.read(&ring, 1024 * 512, &mut buf).await, 0);
        assert!(target.set_dev_sectors(0).is_err());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_read_only_state_refuses_a_write_and_a_discard_with_eperm() {
        let tmp = TempDir::new().expect("tempdir");
        let target = open_target(&tmp, 8192).await;
        let state = target.state.load_full();
        target
            .swap_state(state.image_config_path.clone(), state.image.clone(), false)
            .expect("swap to read only");
        let ring = AsyncIoRingBuilder::new()
            .nr_sparse_buffer(2)
            .nr_sparse_file(4)
            .sqe_entries(8)
            .cqe_entries(16)
            .build()
            .expect("build async io ring");
        assert_eq!(
            target.write(&ring, 0, &[0u8; 512], false).await,
            -libc::EPERM
        );
        assert_eq!(target.discard(&ring, 0, 512).await, -libc::EPERM);
        assert!(target.geometry().read_only);
    }
}
