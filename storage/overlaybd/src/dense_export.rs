use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use storage_util::CompactWriter;

use crate::backend::local::LocalFile;
use crate::io::transient_io_ring::shared_transient_io_ring;
use crate::io::virtual_file::VirtualFile;
use crate::layer::layer_metadata::read_overlaybd_layer_is_sparse_rw;
use crate::lsmt::file::{CommitArgs, LSMTReadOnlyFile};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DenseLayerDescriptor {
    pub digest: String,
    pub size: u64,
}

pub fn should_dense_export_layer(path: &Path) -> bool {
    match read_overlaybd_layer_is_sparse_rw(path) {
        Ok(is_sparse) => is_sparse,
        Err(error) => {
            tracing::warn!(
                path = %path.display(),
                error = %format_args!("{error:#}"),
                "failed to inspect overlaybd sparse header; falling back to raw layer upload"
            );
            false
        }
    }
}

pub async fn write_dense_layer_to(path: &Path, writer: Arc<dyn CompactWriter>) -> Result<()> {
    let io_ring = shared_transient_io_ring();
    let source: Arc<dyn VirtualFile> = Arc::new(
        LocalFile::open_ro(path, io_ring)
            .await
            .with_context(|| format!("open sparse overlaybd layer '{}'", path.display()))?,
    );
    let layer = LSMTReadOnlyFile::open(source)
        .await
        .with_context(|| format!("open sealed sparse overlaybd layer '{}'", path.display()))?;
    // `LSMTReadOnlyFile::open` above constructs a single-file sealed layer from
    // this path. `commit_preserving_metadata` intentionally rejects stacked
    // read-only views because their trailer metadata is not a single source of
    // truth for a newly exported dense object.
    let mut args = CommitArgs::from_writer(writer);
    // Dense export writers hash and upload a single byte stream, so compact
    // output must stay sequential even if the default commit concurrency changes.
    args.concurrency = 1;
    layer
        .commit_preserving_metadata(args)
        .await
        .with_context(|| format!("dense-export sparse overlaybd layer '{}'", path.display()))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::backend::local::LocalFile;
    use crate::io::transient_io_ring::shared_transient_io_ring;
    use crate::io::virtual_file::VirtualFile;
    use crate::lsmt::file::{CommitArgs, LSMTFile, LSMTReadOnlyFile};
    use sha2::{Digest, Sha256};

    use super::*;

    #[tokio::test]
    async fn dense_export_rewrites_sparse_offsets_without_changing_logical_data() {
        let temp = tempfile::tempdir().unwrap();
        let sparse_path = temp.path().join("snapshot.commit");
        let dense_path = temp.path().join("dense.commit");
        let virtual_size = 64 * 1024 * 1024;

        let sparse_file: Arc<dyn VirtualFile> = Arc::new(
            LocalFile::new(&sparse_path, shared_transient_io_ring())
                .await
                .unwrap(),
        );
        let sparse = LSMTFile::create(sparse_file, None, virtual_size, true)
            .await
            .unwrap();
        let first = vec![0xAB; 4096];
        let second = vec![0xCD; 4096];
        sparse.write_at(0, &first).await.unwrap();
        sparse.write_at(32 * 1024 * 1024, &second).await.unwrap();
        sparse.close_seal().await.unwrap();
        drop(sparse);

        assert!(should_dense_export_layer(&sparse_path));

        let dense_file: Arc<dyn VirtualFile> = Arc::new(
            LocalFile::new(&dense_path, shared_transient_io_ring())
                .await
                .unwrap(),
        );
        write_dense_layer_to(&sparse_path, CommitArgs::new(dense_file.clone()).writer)
            .await
            .unwrap();

        let dense_size = dense_file.size().await.unwrap();
        let dense_bytes = std::fs::read(&dense_path).unwrap();
        let dense_digest = format!("sha256:{:x}", Sha256::digest(&dense_bytes));
        assert_eq!(dense_bytes.len() as u64, dense_size);
        assert!(
            dense_size < 1024 * 1024,
            "dense output should skip the large sparse hole, got {dense_size}"
        );

        let repeat_path = temp.path().join("dense-repeat.commit");
        let repeat_file: Arc<dyn VirtualFile> = Arc::new(
            LocalFile::new(&repeat_path, shared_transient_io_ring())
                .await
                .unwrap(),
        );
        write_dense_layer_to(&sparse_path, CommitArgs::new(repeat_file.clone()).writer)
            .await
            .unwrap();
        assert_eq!(repeat_file.size().await.unwrap(), dense_size);
        assert_eq!(
            format!(
                "sha256:{:x}",
                Sha256::digest(std::fs::read(&repeat_path).unwrap())
            ),
            dense_digest
        );

        let dense = LSMTReadOnlyFile::open(dense_file).await.unwrap();
        assert_eq!(dense.read_at(0, 4096).await.unwrap().as_ref(), &first[..]);
        assert_eq!(
            dense
                .read_at(32 * 1024 * 1024, 4096)
                .await
                .unwrap()
                .as_ref(),
            &second[..]
        );
        assert!(dense
            .read_at(4096, 4096)
            .await
            .unwrap()
            .iter()
            .all(|b| *b == 0));
    }
}
