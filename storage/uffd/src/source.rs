//! Where page contents come from.

use std::future::Future;
use std::pin::Pin;

use anyhow::Result;
use storage_util::io_ring::AsyncIoRing;

pub type LocalBoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

/// A memory image the handler reads pages from. Every method runs on the
/// handler's own thread with its own ring, so the futures need not be `Send`.
pub trait PageSource: Send + Sync + 'static {
    /// Total image size in bytes; faults beyond it are served as zeros.
    fn size(&self) -> u64;

    /// Called once on the handler thread before the first fault.
    fn init<'a>(&'a self, _ring: &'a AsyncIoRing) -> LocalBoxFuture<'a, Result<()>> {
        Box::pin(async { Ok(()) })
    }

    /// Fills `dst` (one page) with the image bytes at `offset`.
    fn read_page<'a>(
        &'a self,
        ring: &'a AsyncIoRing,
        offset: u64,
        dst: &'a mut [u8],
    ) -> LocalBoxFuture<'a, Result<()>>;
}
