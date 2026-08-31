use crate::compression::zfile::{is_zfile, zfile_open_ro_vfile};
use crate::io::virtual_file::VirtualFile;
use anyhow::{Context, Result};
use std::sync::Arc;

async fn try_open_zfile(
    file: Arc<dyn VirtualFile>,
    verify: bool,
    file_path: Option<&str>,
) -> Result<Arc<dyn VirtualFile>> {
    let detected = match file_path {
        Some(path) => is_zfile(file.clone())
            .await
            .context(format!("check file type `{path}` failed"))?,
        None => is_zfile(file.clone()).await?,
    };
    if detected == 1 {
        return match file_path {
            Some(path) => zfile_open_ro_vfile(file, verify)
                .await
                .context(format!("open zfile `{path}` failed")),
            None => zfile_open_ro_vfile(file, verify).await,
        };
    }
    Ok(file)
}

/// Open `source` as a lower layer, transparently unwrapping a zfile header
/// (`verify` is enabled for remote sources and disabled for local ones).
pub async fn new_switch_file(
    source: Arc<dyn VirtualFile>,
    local: bool,
    file_path: Option<&str>,
) -> Result<Arc<dyn VirtualFile>> {
    let mut retry = 1u8;
    loop {
        match try_open_zfile(source.clone(), !local, file_path).await {
            Ok(file) => return Ok(file),
            Err(_e) if retry > 0 => retry -= 1,
            Err(e) => {
                return Err(match file_path {
                    Some(path) => e.context(format!("open source file as zfile `{path}` failed")),
                    None => e,
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::local::LocalFile;
    use super::super::tar::{new_tar_file_adaptor, new_tar_file_create};
    use super::*;
    use crate::compression::zfile::{zfile_compress, CompressArgs, CompressOptions};
    use crate::test_utils::test_io_ring;
    use anyhow::bail;
    use async_trait::async_trait;
    use bytes::Bytes;
    use std::path::Path;
    use tempfile::tempdir;
    use tokio::sync::Mutex;

    #[derive(Debug, Default)]
    struct MemoryFile {
        data: Mutex<Vec<u8>>,
    }

    impl MemoryFile {
        fn new(data: Vec<u8>) -> Self {
            Self {
                data: Mutex::new(data),
            }
        }
    }

    #[async_trait]
    impl VirtualFile for MemoryFile {
        async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes> {
            let data = self.data.lock().await;
            if len == 0 || offset >= data.len() as u64 {
                return Ok(Bytes::new());
            }
            let end = offset.saturating_add(len as u64).min(data.len() as u64) as usize;
            Ok(Bytes::copy_from_slice(&data[offset as usize..end]))
        }

        async fn write_at(&self, offset: u64, buf: &[u8]) -> Result<usize> {
            let mut data = self.data.lock().await;
            let end = usize::try_from(offset.saturating_add(buf.len() as u64))
                .context("write range overflow")?;
            if end > data.len() {
                data.resize(end, 0);
            }
            data[offset as usize..end].copy_from_slice(buf);
            Ok(buf.len())
        }

        async fn size(&self) -> Result<u64> {
            Ok(self.data.lock().await.len() as u64)
        }

        async fn truncate(&self, size: u64) -> Result<()> {
            let mut data = self.data.lock().await;
            let size = usize::try_from(size).context("truncate size overflow")?;
            data.resize(size, 0);
            Ok(())
        }

        async fn sync(&self) -> Result<()> {
            Ok(())
        }
    }

    async fn read_all(file: &(dyn VirtualFile + Send + Sync)) -> Result<Vec<u8>> {
        let size = file.size().await? as usize;
        let mut offset = 0u64;
        let mut out = Vec::with_capacity(size);
        while out.len() < size {
            let chunk = file.read_at(offset, size - out.len()).await?;
            if chunk.is_empty() {
                bail!("short read while collecting file contents");
            }
            offset = offset
                .checked_add(chunk.len() as u64)
                .context("offset overflow")?;
            out.extend_from_slice(&chunk);
        }
        Ok(out)
    }

    async fn create_zfile_tar_file(path: &Path, data: &[u8]) -> Result<()> {
        let src: Arc<dyn VirtualFile> = Arc::new(MemoryFile::new(data.to_vec()));
        let dst: Arc<dyn VirtualFile> = Arc::new(LocalFile::new(path, test_io_ring()).await?);
        let tar = new_tar_file_create(dst).await?;
        let args = CompressArgs {
            opt: CompressOptions::new(CompressOptions::LZ4, 4096, 1),
            overwrite_header: false,
            workers: 1,
        };
        zfile_compress(src, tar.clone(), &args).await?;
        tar.close().await
    }

    fn sample_bytes(seed: u8, len: usize) -> Vec<u8> {
        (0..len)
            .map(|idx| seed.wrapping_add((idx % 251) as u8))
            .collect()
    }

    #[tokio::test]
    async fn test_new_switch_file_passthrough_source() {
        let source_data = sample_bytes(0x11, 4096);
        let source: Arc<dyn VirtualFile> = Arc::new(MemoryFile::new(source_data.clone()));

        let opened = new_switch_file(source.clone(), false, Some("http://registry/blob"))
            .await
            .expect("open switch file");

        assert_eq!(
            read_all(opened.as_ref()).await.expect("read switch source"),
            source_data
        );
        assert!(Arc::ptr_eq(&source, &opened));
    }

    #[tokio::test]
    async fn test_new_switch_file_local_mode_opens_zfile() {
        let dir = tempdir().expect("create tempdir");
        let local_path = dir.path().join("local-zfile.commit");
        let local_data = sample_bytes(0x58, 12 * 1024 + 97);
        create_zfile_tar_file(&local_path, &local_data)
            .await
            .expect("create tar zfile");

        let local: Arc<dyn VirtualFile> = Arc::new(
            LocalFile::open_ro(&local_path, test_io_ring())
                .await
                .expect("open local"),
        );
        let local = new_tar_file_adaptor(local).await.expect("adapt local tar");
        let switch = new_switch_file(local, true, Some(local_path.to_string_lossy().as_ref()))
            .await
            .expect("open local switch file");

        assert_eq!(switch.size().await.expect("size"), local_data.len() as u64);
        assert_eq!(
            read_all(switch.as_ref()).await.expect("read local mode"),
            local_data
        );
    }
}
