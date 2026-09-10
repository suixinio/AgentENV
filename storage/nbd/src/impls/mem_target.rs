use anyhow::{bail, Result};
use async_trait::async_trait;
use parking_lot::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use storage_util::io_ring::AsyncIoRing;

use crate::target::{Geometry, NbdTarget};

/// An in-memory block target: the reference implementation of [`NbdTarget`]
/// and the one the device suite exposes when no image is involved.
#[derive(Debug)]
pub struct MemTarget {
    data: Mutex<Vec<u8>>,
    block_size: u32,
    read_only: bool,
    supports_discard: bool,
    largest_request: AtomicUsize,
    flushes: AtomicUsize,
}

impl MemTarget {
    pub fn new(size_bytes: usize, block_size: u32) -> Self {
        Self {
            data: Mutex::new(vec![0u8; size_bytes]),
            block_size,
            read_only: false,
            supports_discard: true,
            largest_request: AtomicUsize::new(0),
            flushes: AtomicUsize::new(0),
        }
    }

    pub fn read_only(mut self, read_only: bool) -> Self {
        self.read_only = read_only;
        self
    }

    pub fn supports_discard(mut self, supports_discard: bool) -> Self {
        self.supports_discard = supports_discard;
        self
    }

    /// A copy of the backing bytes, for asserting what reached the target.
    pub fn snapshot(&self, offset: u64, len: usize) -> Vec<u8> {
        let data = self.data.lock();
        let start = offset as usize;
        data[start..start + len].to_vec()
    }

    /// Grows or shrinks the target; new bytes read back as zero.
    pub fn resize(&self, size_bytes: usize) {
        self.data.lock().resize(size_bytes, 0);
    }

    pub fn fill(&self, offset: u64, bytes: &[u8]) {
        let mut data = self.data.lock();
        let start = offset as usize;
        data[start..start + bytes.len()].copy_from_slice(bytes);
    }

    /// The longest single read or write the kernel has issued so far.
    pub fn largest_request(&self) -> usize {
        self.largest_request.load(Ordering::Relaxed)
    }

    pub fn flushes(&self) -> usize {
        self.flushes.load(Ordering::Relaxed)
    }

    fn observe(&self, len: usize) {
        self.largest_request.fetch_max(len, Ordering::Relaxed);
    }

    fn range(&self, offset: u64, len: u64) -> Result<(usize, usize)> {
        let end = offset
            .checked_add(len)
            .ok_or_else(|| anyhow::anyhow!("range {offset}+{len} overflows"))?;
        let size = self.data.lock().len() as u64;
        if end > size {
            bail!("range {offset}+{len} runs past the {size} byte target");
        }
        Ok((offset as usize, len as usize))
    }
}

#[async_trait(?Send)]
impl NbdTarget for MemTarget {
    const DEV_NAME: &str = "mem";

    fn geometry(&self) -> Geometry {
        Geometry {
            size_bytes: self.data.lock().len() as u64,
            block_size: self.block_size,
            read_only: self.read_only,
            supports_discard: self.supports_discard,
        }
    }

    async fn init(&self, _conn: u16, _io_ring: &AsyncIoRing) -> Result<()> {
        Ok(())
    }

    async fn read(self: &Arc<Self>, _io_ring: &AsyncIoRing, offset: u64, buf: &mut [u8]) -> i32 {
        let Ok((start, len)) = self.range(offset, buf.len() as u64) else {
            return -libc::EINVAL;
        };
        self.observe(len);
        buf.copy_from_slice(&self.data.lock()[start..start + len]);
        0
    }

    async fn write(
        self: &Arc<Self>,
        _io_ring: &AsyncIoRing,
        offset: u64,
        data: &[u8],
        _fua: bool,
    ) -> i32 {
        if self.read_only {
            return -libc::EPERM;
        }
        let Ok((start, len)) = self.range(offset, data.len() as u64) else {
            return -libc::EINVAL;
        };
        self.observe(len);
        self.data.lock()[start..start + len].copy_from_slice(data);
        0
    }

    async fn flush(self: &Arc<Self>, _io_ring: &AsyncIoRing) -> i32 {
        self.flushes.fetch_add(1, Ordering::Relaxed);
        0
    }

    async fn discard(self: &Arc<Self>, _io_ring: &AsyncIoRing, offset: u64, len: u64) -> i32 {
        if self.read_only {
            return -libc::EPERM;
        }
        if !self.supports_discard {
            return -libc::EOPNOTSUPP;
        }
        let Ok((start, len)) = self.range(offset, len) else {
            return -libc::EINVAL;
        };
        self.data.lock()[start..start + len].fill(0);
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use storage_util::io_ring::AsyncIoRingBuilder;

    fn ring() -> AsyncIoRing {
        AsyncIoRingBuilder::new()
            .nr_sparse_buffer(2)
            .nr_sparse_file(2)
            .sqe_entries(8)
            .cqe_entries(16)
            .build()
            .expect("build async io ring")
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_write_is_visible_to_a_later_read_at_the_same_offset() {
        let target = Arc::new(MemTarget::new(8192, 4096));
        let ring = ring();
        assert_eq!(target.write(&ring, 4096, &[0x5A; 4096], false).await, 0);
        let mut buf = [0u8; 4096];
        assert_eq!(target.read(&ring, 4096, &mut buf).await, 0);
        assert!(buf.iter().all(|&byte| byte == 0x5A));
        assert_eq!(target.snapshot(0, 4096), vec![0u8; 4096]);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_request_past_the_end_of_the_target_is_refused_as_einval() {
        let target = Arc::new(MemTarget::new(8192, 4096));
        let ring = ring();
        let mut buf = [0u8; 4096];
        assert_eq!(target.read(&ring, 8192, &mut buf).await, -libc::EINVAL);
        assert_eq!(
            target.write(&ring, 8192, &[0u8; 4096], false).await,
            -libc::EINVAL
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_read_only_target_refuses_a_write_and_a_discard_with_eperm() {
        let target = Arc::new(MemTarget::new(8192, 4096).read_only(true));
        let ring = ring();
        assert_eq!(
            target.write(&ring, 0, &[0u8; 4096], false).await,
            -libc::EPERM
        );
        assert_eq!(target.discard(&ring, 0, 4096).await, -libc::EPERM);
        assert!(target.geometry().read_only);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_discard_zeroes_only_the_named_range() {
        let target = Arc::new(MemTarget::new(8192, 4096));
        let ring = ring();
        target.fill(0, &[0xFF; 8192]);
        assert_eq!(target.discard(&ring, 4096, 4096).await, 0);
        assert!(target.snapshot(0, 4096).iter().all(|&byte| byte == 0xFF));
        assert!(target.snapshot(4096, 4096).iter().all(|&byte| byte == 0));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn the_largest_request_tracks_the_longest_transfer_seen() {
        let target = Arc::new(MemTarget::new(8192, 4096));
        let ring = ring();
        let mut small = [0u8; 512];
        assert_eq!(target.read(&ring, 0, &mut small).await, 0);
        let mut large = vec![0u8; 8192];
        assert_eq!(target.read(&ring, 0, &mut large).await, 0);
        assert_eq!(target.largest_request(), 8192);
    }
}
