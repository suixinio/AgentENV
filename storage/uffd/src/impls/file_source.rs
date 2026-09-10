use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::Path;

use anyhow::{Context, Result};
use storage_util::io_ring::AsyncIoRing;

use crate::source::{LocalBoxFuture, PageSource};

/// A plain file (a raw Firecracker memory file, say) read with `pread`. The
/// read blocks the handler thread; this source is a tool, not the production
/// path.
pub struct FileSource {
    file: File,
    size: u64,
}

impl FileSource {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
        let size = file
            .metadata()
            .with_context(|| format!("stat {}", path.display()))?
            .len();
        Ok(Self { file, size })
    }
}

impl PageSource for FileSource {
    fn size(&self) -> u64 {
        self.size
    }

    fn read_page<'a>(
        &'a self,
        _ring: &'a AsyncIoRing,
        offset: u64,
        dst: &'a mut [u8],
    ) -> LocalBoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let mut done = 0;
            while done < dst.len() {
                let n = self
                    .file
                    .read_at(&mut dst[done..], offset + done as u64)
                    .with_context(|| format!("pread at {offset}"))?;
                if n == 0 {
                    dst[done..].fill(0);
                    break;
                }
                done += n;
            }
            Ok(())
        })
    }
}
