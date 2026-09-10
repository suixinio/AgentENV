use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use overlaybd::image_file::ImageFile;
use overlaybd::image_service::ImageService;
use overlaybd::virtual_file::{IoCtx, VirtualFile};
use storage_util::io_ring::AsyncIoRing;

use crate::source::{LocalBoxFuture, PageSource};

/// The stacked overlaybd memory layers as a page source. Reads go straight
/// to the image on the handler's ring; no block device is involved.
pub struct OverlaybdSource {
    image_config_path: PathBuf,
    image: Arc<ImageFile>,
}

impl OverlaybdSource {
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
        Self::from_opened_image(image_config_path, image)
    }

    pub fn from_opened_image(image_config_path: PathBuf, image: Arc<ImageFile>) -> Result<Self> {
        if image.size_bytes() == 0 {
            bail!(
                "overlaybd memory image {} is empty",
                image_config_path.display()
            );
        }
        Ok(Self {
            image_config_path,
            image,
        })
    }

    pub fn image_config_path(&self) -> &Path {
        &self.image_config_path
    }

    pub fn image(&self) -> &Arc<ImageFile> {
        &self.image
    }
}

impl PageSource for OverlaybdSource {
    fn size(&self) -> u64 {
        self.image.size_bytes()
    }

    fn read_page<'a>(
        &'a self,
        ring: &'a AsyncIoRing,
        offset: u64,
        dst: &'a mut [u8],
    ) -> LocalBoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let ctx = IoCtx::new(ring);
            let n = self
                .image
                .read_at_into_with_ctx(ctx, offset, dst)
                .await
                .with_context(|| {
                    format!(
                        "read {} bytes at {offset} from {}",
                        dst.len(),
                        self.image_config_path.display()
                    )
                })?;
            if n != dst.len() {
                bail!(
                    "short read at {offset} from {}: {n} of {} bytes",
                    self.image_config_path.display(),
                    dst.len()
                );
            }
            Ok(())
        })
    }
}
