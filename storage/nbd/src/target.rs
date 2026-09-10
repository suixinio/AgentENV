//! The transport-agnostic block target contract the nbd server dispatches to.

use anyhow::Result;
use async_trait::async_trait;
use std::sync::Arc;
use storage_util::io_ring::AsyncIoRing;

/// What the device advertises to the kernel at CONNECT time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Geometry {
    pub size_bytes: u64,
    pub block_size: u32,
    pub read_only: bool,
    pub supports_discard: bool,
}

/// A userspace block target. The futures are `?Send` because overlaybd's I/O
/// path holds an `AsyncIoRing`, which is `!Send`; every connection worker runs
/// its own current-thread runtime and its own ring.
#[async_trait(?Send)]
pub trait NbdTarget: Send + Sync + 'static {
    const DEV_NAME: &str;

    /// Read once per request; a target that resizes itself is seen through it.
    fn geometry(&self) -> Geometry;

    /// Called once per connection before the device is connected, on that
    /// connection's own thread and ring.
    async fn init(&self, conn: u16, io_ring: &AsyncIoRing) -> Result<()>;

    /// Return 0 on success or a negative errno.
    async fn read(self: &Arc<Self>, io_ring: &AsyncIoRing, offset: u64, buf: &mut [u8]) -> i32;

    /// Return 0 on success or a negative errno.
    async fn write(
        self: &Arc<Self>,
        io_ring: &AsyncIoRing,
        offset: u64,
        data: &[u8],
        fua: bool,
    ) -> i32;

    /// Return 0 on success or a negative errno.
    async fn flush(self: &Arc<Self>, io_ring: &AsyncIoRing) -> i32;

    /// Return 0 on success or a negative errno.
    async fn discard(self: &Arc<Self>, io_ring: &AsyncIoRing, offset: u64, len: u64) -> i32;
}
