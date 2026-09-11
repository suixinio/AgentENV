use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use anyhow::Result;
use storage_util::io_ring::AsyncIoRing;
use tokio::sync::Notify;

use crate::source::{LocalBoxFuture, PageSource};

/// Counters for one cache, read when its server stops.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BlockCacheStats {
    /// Faults served from a block already held.
    pub hits: u64,
    /// Faults that had to read a block.
    pub misses: u64,
    /// Faults that waited for a block another fault was already reading.
    pub waits: u64,
    /// Bytes read from the inner source, which is the read amplification a
    /// scattered fault pattern pays for the block size.
    pub bytes_read: u64,
    /// Blocks dropped to stay inside the capacity.
    pub evictions: u64,
}

/// Reads whole blocks from the inner source and serves pages out of them.
///
/// A guest walking its restored memory then pays one source read per block
/// instead of one per page; a scattered one pays the whole block for a single
/// page, so the block size is the knob that trades amplification against round
/// trips. The cache belongs to one served VM and is dropped with it.
pub struct BlockCacheSource<S> {
    inner: S,
    block_size: u64,
    capacity_blocks: usize,
    state: Mutex<Blocks>,
    /// A fault that finds its block already being read waits here instead of
    /// issuing the same read again.
    progress: Notify,
    hits: AtomicU64,
    misses: AtomicU64,
    waits: AtomicU64,
    bytes_read: AtomicU64,
    evictions: AtomicU64,
}

#[derive(Default)]
struct Blocks {
    cached: HashMap<u64, Vec<u8>>,
    /// Least recently used first.
    order: VecDeque<u64>,
    loading: HashSet<u64>,
}

impl Blocks {
    fn get(&mut self, block: u64) -> Option<&[u8]> {
        if !self.cached.contains_key(&block) {
            return None;
        }
        if let Some(at) = self.order.iter().position(|held| *held == block) {
            self.order.remove(at);
        }
        self.order.push_back(block);
        self.cached.get(&block).map(Vec::as_slice)
    }

    fn insert(&mut self, block: u64, bytes: Vec<u8>, capacity: usize) -> u64 {
        let mut evicted = 0;
        while self.order.len() >= capacity {
            match self.order.pop_front() {
                Some(oldest) => {
                    self.cached.remove(&oldest);
                    evicted += 1;
                }
                None => break,
            }
        }
        if self.cached.insert(block, bytes).is_none() {
            self.order.push_back(block);
        }
        evicted
    }
}

impl<S: PageSource> BlockCacheSource<S> {
    /// `block_size` bytes per read, `capacity_bytes` of them held at once.
    /// Either below one page leaves `inner` uncached, which is also how a
    /// deployment turns the cache off.
    pub fn new(inner: S, block_size: u64, capacity_bytes: u64) -> Self {
        let capacity_blocks = capacity_bytes.checked_div(block_size).unwrap_or(0) as usize;
        Self {
            inner,
            block_size,
            capacity_blocks,
            state: Mutex::new(Blocks::default()),
            progress: Notify::new(),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            waits: AtomicU64::new(0),
            bytes_read: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
        }
    }

    pub fn stats(&self) -> BlockCacheStats {
        BlockCacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            waits: self.waits.load(Ordering::Relaxed),
            bytes_read: self.bytes_read.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
        }
    }

    /// True when reads go through blocks rather than straight to the source.
    pub fn is_caching(&self) -> bool {
        self.capacity_blocks > 0
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Blocks> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl<S: PageSource> PageSource for BlockCacheSource<S> {
    fn size(&self) -> u64 {
        self.inner.size()
    }

    fn init<'a>(&'a self, ring: &'a AsyncIoRing) -> LocalBoxFuture<'a, Result<()>> {
        self.inner.init(ring)
    }

    fn read_page<'a>(
        &'a self,
        ring: &'a AsyncIoRing,
        offset: u64,
        dst: &'a mut [u8],
    ) -> LocalBoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let len = dst.len();
            let block = offset / self.block_size.max(1);
            let start = block * self.block_size;
            let within = (offset - start) as usize;
            // A read that does not fit inside one block goes straight through:
            // stitching two blocks would only ever run under a block size
            // smaller than the page size the handler serves.
            if !self.is_caching() || within as u64 + len as u64 > self.block_size {
                return self.inner.read_page(ring, offset, dst).await;
            }

            loop {
                let waiting = self.progress.notified();
                tokio::pin!(waiting);
                // Register before releasing the lock, or a block that finishes
                // loading in between would leave this fault waiting forever.
                waiting.as_mut().enable();
                {
                    let mut state = self.lock();
                    if let Some(bytes) = state.get(block) {
                        dst.copy_from_slice(&bytes[within..within + len]);
                        self.hits.fetch_add(1, Ordering::Relaxed);
                        return Ok(());
                    }
                    if state.loading.insert(block) {
                        break;
                    }
                }
                self.waits.fetch_add(1, Ordering::Relaxed);
                waiting.await;
            }

            self.misses.fetch_add(1, Ordering::Relaxed);
            let mut buf = vec![0u8; self.block_size as usize];
            let read = self.inner.read_page(ring, start, &mut buf).await;
            let mut state = self.lock();
            state.loading.remove(&block);
            self.progress.notify_waiters();
            read?;
            self.bytes_read
                .fetch_add(self.block_size, Ordering::Relaxed);
            dst.copy_from_slice(&buf[within..within + len]);
            let evicted = state.insert(block, buf, self.capacity_blocks);
            drop(state);
            if evicted > 0 {
                self.evictions.fetch_add(evicted, Ordering::Relaxed);
            }
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use storage_util::io_ring::{AsyncIoRing, AsyncIoRingBuilder};

    use super::*;
    use crate::impls::MemSource;

    struct CountingSource {
        inner: MemSource,
        reads: AtomicU64,
        bytes: AtomicU64,
    }

    impl CountingSource {
        fn new(data: Vec<u8>) -> Self {
            Self {
                inner: MemSource::new(data),
                reads: AtomicU64::new(0),
                bytes: AtomicU64::new(0),
            }
        }
    }

    impl PageSource for CountingSource {
        fn size(&self) -> u64 {
            self.inner.size()
        }

        fn read_page<'a>(
            &'a self,
            ring: &'a AsyncIoRing,
            offset: u64,
            dst: &'a mut [u8],
        ) -> LocalBoxFuture<'a, Result<()>> {
            Box::pin(async move {
                self.reads.fetch_add(1, Ordering::Relaxed);
                self.bytes.fetch_add(dst.len() as u64, Ordering::Relaxed);
                // Hold the read open long enough for another fault to reach
                // the cache; without this no two reads ever overlap and the
                // concurrency test cannot tell single-flight from its absence.
                tokio::task::yield_now().await;
                self.inner.read_page(ring, offset, dst).await
            })
        }
    }

    fn image(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i % 251) as u8).collect()
    }

    async fn read(
        source: &impl PageSource,
        ring: &AsyncIoRing,
        offset: u64,
        len: usize,
    ) -> Vec<u8> {
        let mut page = vec![0u8; len];
        source
            .read_page(ring, offset, &mut page)
            .await
            .expect("read the page");
        page
    }

    fn run<F: std::future::Future<Output = ()>>(body: impl FnOnce(AsyncIoRing) -> F) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let local = tokio::task::LocalSet::new();
        local.block_on(&runtime, async move {
            let ring = AsyncIoRingBuilder::new()
                .nr_sparse_buffer(16)
                .nr_sparse_file(4)
                .sqe_entries(16)
                .cqe_entries(32)
                .build()
                .expect("ring");
            body(ring).await;
        });
    }

    #[test]
    fn a_walk_through_one_block_reads_the_source_once() {
        run(|ring| async move {
            let data = image(64 * 1024);
            let source =
                BlockCacheSource::new(CountingSource::new(data.clone()), 16 * 1024, 64 * 1024);
            for page in 0..4u64 {
                let got = read(&source, &ring, page * 4096, 4096).await;
                assert_eq!(got, data[(page as usize * 4096)..][..4096]);
            }
            assert_eq!(source.inner.reads.load(Ordering::Relaxed), 1);
            let stats = source.stats();
            assert_eq!((stats.hits, stats.misses), (3, 1));
        });
    }

    #[test]
    fn a_page_from_each_block_reads_each_block_once() {
        run(|ring| async move {
            let data = image(64 * 1024);
            let source =
                BlockCacheSource::new(CountingSource::new(data.clone()), 16 * 1024, 64 * 1024);
            for block in 0..4u64 {
                let got = read(&source, &ring, block * 16 * 1024, 4096).await;
                assert_eq!(got, data[(block as usize * 16 * 1024)..][..4096]);
            }
            assert_eq!(source.inner.reads.load(Ordering::Relaxed), 4);
            // Every byte of the image was read to serve a quarter of it: the
            // amplification a scattered pattern pays.
            assert_eq!(source.stats().bytes_read, 64 * 1024);
        });
    }

    #[test]
    fn the_capacity_bounds_what_is_held_and_a_dropped_block_is_read_again() {
        run(|ring| async move {
            let data = image(64 * 1024);
            let source =
                BlockCacheSource::new(CountingSource::new(data.clone()), 16 * 1024, 32 * 1024);
            for block in 0..3u64 {
                read(&source, &ring, block * 16 * 1024, 4096).await;
            }
            // Block 0 was evicted by block 2, so touching it reads again.
            read(&source, &ring, 0, 4096).await;
            assert_eq!(source.inner.reads.load(Ordering::Relaxed), 4);
            assert!(source.stats().evictions >= 1);
        });
    }

    #[test]
    fn a_zero_capacity_passes_every_read_through() {
        run(|ring| async move {
            let data = image(16 * 1024);
            let source = BlockCacheSource::new(CountingSource::new(data.clone()), 16 * 1024, 0);
            assert!(!source.is_caching());
            for page in 0..4u64 {
                let got = read(&source, &ring, page * 4096, 4096).await;
                assert_eq!(got, data[(page as usize * 4096)..][..4096]);
            }
            assert_eq!(source.inner.reads.load(Ordering::Relaxed), 4);
            assert_eq!(source.stats().bytes_read, 0);
        });
    }

    #[test]
    fn a_page_past_the_image_end_serves_zeros_for_the_tail() {
        run(|ring| async move {
            // The image ends mid-page, and the block covering it is short.
            let data = image(18 * 1024);
            let source =
                BlockCacheSource::new(CountingSource::new(data.clone()), 16 * 1024, 64 * 1024);
            let tail = read(&source, &ring, 16 * 1024, 4096).await;
            assert_eq!(&tail[..2048], &data[16 * 1024..]);
            assert!(tail[2048..].iter().all(|byte| *byte == 0));
        });
    }

    #[test]
    fn concurrent_faults_in_one_block_read_it_once() {
        run(|ring| async move {
            let data = image(64 * 1024);
            let source = std::rc::Rc::new(BlockCacheSource::new(
                CountingSource::new(data.clone()),
                64 * 1024,
                64 * 1024,
            ));
            let mut tasks = Vec::new();
            for page in 0..8u64 {
                let source = std::rc::Rc::clone(&source);
                let ring = ring.clone();
                tasks.push(tokio::task::spawn_local(async move {
                    let mut buf = vec![0u8; 4096];
                    source
                        .read_page(&ring, page * 4096, &mut buf)
                        .await
                        .expect("read");
                    buf
                }));
            }
            for (page, task) in tasks.into_iter().enumerate() {
                let got = task.await.expect("join");
                assert_eq!(got, data[(page * 4096)..][..4096]);
            }
            assert_eq!(source.inner.reads.load(Ordering::Relaxed), 1);
        });
    }
}
