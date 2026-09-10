use anyhow::Result;
use storage_util::io_ring::AsyncIoRing;

use crate::source::{LocalBoxFuture, PageSource};

/// An in-memory image; tests and the self-test use it.
pub struct MemSource {
    data: Vec<u8>,
}

impl MemSource {
    pub fn new(data: Vec<u8>) -> Self {
        Self { data }
    }

    /// `size` bytes where byte `i` of page `p` is `(p * 31 + i) as u8`, and
    /// pages whose index is a multiple of `zero_every` (when non-zero) are
    /// all zeros.
    pub fn patterned(size: usize, page_size: usize, zero_every: usize) -> Self {
        let mut data = vec![0u8; size];
        for (p, page) in data.chunks_mut(page_size).enumerate() {
            if zero_every != 0 && p % zero_every == 0 {
                continue;
            }
            for (i, b) in page.iter_mut().enumerate() {
                *b = (p.wrapping_mul(31).wrapping_add(i)) as u8;
            }
        }
        Self { data }
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }
}

impl PageSource for MemSource {
    fn size(&self) -> u64 {
        self.data.len() as u64
    }

    fn read_page<'a>(
        &'a self,
        _ring: &'a AsyncIoRing,
        offset: u64,
        dst: &'a mut [u8],
    ) -> LocalBoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let start = (offset as usize).min(self.data.len());
            let end = (start + dst.len()).min(self.data.len());
            let n = end - start;
            dst[..n].copy_from_slice(&self.data[start..end]);
            dst[n..].fill(0);
            Ok(())
        })
    }
}
