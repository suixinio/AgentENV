//! The fault-serving thread: one per restored VM, with its own tokio
//! current-thread runtime, `LocalSet` and io_uring, the same layout as an nbd
//! connection worker.
//!
//! The loop reads events off the userfaultfd, dedupes faults on the same page,
//! and hands each page to a task bounded by `max_inflight`: read it from the
//! `PageSource`, then `UFFDIO_COPY` it in (or `UFFDIO_ZEROPAGE` when the page
//! is all zeros, was discarded by a `REMOVE` event, or lies past the image).
//! A source read that keeps failing past `read_retry_budget` is fatal: the
//! handler exits, its descriptor closes, and the guest is no longer backed.

use std::cell::{Cell, RefCell};
use std::collections::HashSet;
use std::os::fd::OwnedFd;
use std::os::unix::net::UnixListener as StdUnixListener;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use storage_util::io_ring::{AsyncIoRing, AsyncIoRingBuilder};
use tokio::io::unix::AsyncFd;
use tokio::net::UnixListener;
use tokio::sync::{oneshot, watch, Notify, Semaphore};

use crate::handshake::{recv_handshake, GuestRegionUffdMapping};
use crate::proto::{Event, Uffd, UffdMsg};
use crate::source::PageSource;

const EVENT_BATCH: usize = 64;
const COPY_RETRY_LIMIT: u32 = 20_000;
const READ_RETRY_BASE: Duration = Duration::from_millis(50);
const READ_RETRY_MAX: Duration = Duration::from_secs(2);

#[derive(Debug, Clone)]
pub struct HandlerOptions {
    /// Pages being read and installed concurrently.
    pub max_inflight: usize,
    /// How long a page read may keep failing before the handler gives up.
    pub read_retry_budget: Duration,
    /// How long `serve_socket` waits for Firecracker to connect and send the
    /// handshake.
    pub handshake_timeout: Duration,
    /// How long `stop` waits for in-flight pages before cancelling them.
    pub drain_timeout: Duration,
    /// Thread name and log tag.
    pub name: String,
}

impl Default for HandlerOptions {
    fn default() -> Self {
        Self {
            max_inflight: 64,
            read_retry_budget: Duration::from_secs(60),
            handshake_timeout: Duration::from_secs(60),
            drain_timeout: Duration::from_secs(2),
            name: "uffd".to_string(),
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct StatsSnapshot {
    /// Page-fault events read from the descriptor.
    pub faults: u64,
    /// Pages installed with `UFFDIO_COPY`.
    pub pages_copied: u64,
    /// Pages installed as zero pages.
    pub pages_zeroed: u64,
    /// Installs the kernel refused with `EEXIST` because another install won.
    pub already_present: u64,
    /// Faults dropped because the same page was already being served.
    pub duplicates: u64,
    pub bytes_read: u64,
    pub read_retries: u64,
    pub copy_retries: u64,
    /// `UFFD_EVENT_REMOVE` events.
    pub removes: u64,
}

#[derive(Default)]
struct Stats {
    faults: AtomicU64,
    pages_copied: AtomicU64,
    pages_zeroed: AtomicU64,
    already_present: AtomicU64,
    duplicates: AtomicU64,
    bytes_read: AtomicU64,
    read_retries: AtomicU64,
    copy_retries: AtomicU64,
    removes: AtomicU64,
}

impl Stats {
    fn snapshot(&self) -> StatsSnapshot {
        let load = |a: &AtomicU64| a.load(Ordering::Relaxed);
        StatsSnapshot {
            faults: load(&self.faults),
            pages_copied: load(&self.pages_copied),
            pages_zeroed: load(&self.pages_zeroed),
            already_present: load(&self.already_present),
            duplicates: load(&self.duplicates),
            bytes_read: load(&self.bytes_read),
            read_retries: load(&self.read_retries),
            copy_retries: load(&self.copy_retries),
            removes: load(&self.removes),
        }
    }
}

fn bump(a: &AtomicU64) {
    a.fetch_add(1, Ordering::Relaxed);
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandlerState {
    /// Waiting for the handshake (socket mode) or for the thread to come up.
    Starting,
    /// Faults are being served.
    Serving,
    /// The thread is gone; `Some` carries the error that ended it.
    Exited(Option<String>),
}

/// A running fault handler. Dropping it asks the thread to stop without
/// waiting; `stop` waits.
pub struct UffdHandler {
    stop: Option<oneshot::Sender<()>>,
    thread: Option<thread::JoinHandle<()>>,
    state: watch::Receiver<HandlerState>,
    stats: Arc<Stats>,
}

impl std::fmt::Debug for UffdHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UffdHandler")
            .field("state", &*self.state.borrow())
            .finish_non_exhaustive()
    }
}

enum Entry {
    Socket(StdUnixListener),
    Fd(OwnedFd, Vec<GuestRegionUffdMapping>),
}

impl UffdHandler {
    /// Serves the VM that connects to `listener` and sends the handshake.
    pub fn serve_socket<S: PageSource>(
        listener: StdUnixListener,
        source: Arc<S>,
        opts: HandlerOptions,
    ) -> Result<Self> {
        Self::spawn(Entry::Socket(listener), source, opts)
    }

    /// Serves an already registered descriptor; tests and the self-test play
    /// the VMM side themselves.
    pub fn serve_fd<S: PageSource>(
        uffd: OwnedFd,
        mappings: Vec<GuestRegionUffdMapping>,
        source: Arc<S>,
        opts: HandlerOptions,
    ) -> Result<Self> {
        Self::spawn(Entry::Fd(uffd, mappings), source, opts)
    }

    fn spawn<S: PageSource>(entry: Entry, source: Arc<S>, opts: HandlerOptions) -> Result<Self> {
        if opts.max_inflight == 0 {
            bail!("max_inflight must be at least 1");
        }
        let (stop_tx, stop_rx) = oneshot::channel();
        let (state_tx, state_rx) = watch::channel(HandlerState::Starting);
        let stats = Arc::new(Stats::default());
        let thread_stats = Arc::clone(&stats);
        let name = opts.name.clone();
        let thread = thread::Builder::new()
            .name(format!("uffd-{name}"))
            .spawn(move || {
                let result = thread_main(entry, source, opts, thread_stats, stop_rx, &state_tx);
                let error = result.err().map(|err| format!("{err:#}"));
                if let Some(error) = &error {
                    tracing::error!(name, error, "uffd handler exited with an error");
                } else {
                    tracing::debug!(name, "uffd handler exited");
                }
                let _ = state_tx.send(HandlerState::Exited(error));
            })
            .context("spawn the uffd handler thread")?;
        Ok(Self {
            stop: Some(stop_tx),
            thread: Some(thread),
            state: state_rx,
            stats,
        })
    }

    pub fn state(&self) -> HandlerState {
        self.state.borrow().clone()
    }

    pub fn stats(&self) -> StatsSnapshot {
        self.stats.snapshot()
    }

    pub fn is_running(&self) -> bool {
        !matches!(*self.state.borrow(), HandlerState::Exited(_))
    }

    /// Resolves once faults are being served, or with the error that stopped
    /// the handler first.
    pub async fn wait_serving(&self) -> Result<()> {
        let mut rx = self.state.clone();
        loop {
            let state = rx.borrow_and_update().clone();
            match state {
                HandlerState::Starting => {}
                HandlerState::Serving => return Ok(()),
                HandlerState::Exited(Some(err)) => bail!("uffd handler exited: {err}"),
                HandlerState::Exited(None) => bail!("uffd handler exited before serving"),
            }
            if rx.changed().await.is_err() {
                bail!("uffd handler thread is gone");
            }
        }
    }

    /// Resolves when the thread has exited, with its error if it had one.
    pub async fn wait_exit(&self) -> Option<String> {
        let mut rx = self.state.clone();
        loop {
            let state = rx.borrow_and_update().clone();
            if let HandlerState::Exited(err) = state {
                return err;
            }
            if rx.changed().await.is_err() {
                return Some("uffd handler thread is gone".to_string());
            }
        }
    }

    /// Asks the thread to stop and waits for it. Blocks; call it from a
    /// blocking context. Returns the error the thread ended with, if any.
    pub fn stop(mut self) -> Result<()> {
        self.signal_stop();
        self.join()
    }

    fn signal_stop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }

    fn join(&mut self) -> Result<()> {
        if let Some(thread) = self.thread.take() {
            thread
                .join()
                .map_err(|_| anyhow!("the uffd handler thread panicked"))?;
        }
        match self.state.borrow().clone() {
            HandlerState::Exited(Some(err)) => bail!("{err}"),
            _ => Ok(()),
        }
    }
}

impl Drop for UffdHandler {
    fn drop(&mut self) {
        self.signal_stop();
    }
}

thread_local! {
    static HANDLER_URING: std::cell::OnceCell<AsyncIoRing> = const { std::cell::OnceCell::new() };
}

fn thread_main<S: PageSource>(
    entry: Entry,
    source: Arc<S>,
    opts: HandlerOptions,
    stats: Arc<Stats>,
    mut stop: oneshot::Receiver<()>,
    state: &watch::Sender<HandlerState>,
) -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .on_thread_park(|| {
            HANDLER_URING.with(|uring| {
                if let Some(uring) = uring.get() {
                    if let Err(err) = uring.borrow().submit() {
                        tracing::error!(?err, "uffd handler uring submit on park failed");
                    }
                }
            })
        })
        .build()
        .context("build the tokio runtime for the uffd handler")?;
    let local_set = tokio::task::LocalSet::new();
    local_set.block_on(&runtime, async move {
        let (uffd, mappings) = match entry {
            Entry::Fd(fd, mappings) => (Uffd::from(fd), mappings),
            Entry::Socket(listener) => match accept_handshake(listener, &opts, &mut stop).await? {
                Some(pair) => pair,
                None => return Ok(()),
            },
        };
        run(uffd, mappings, source, opts, stats, &mut stop, state).await
    })
}

async fn accept_handshake(
    listener: StdUnixListener,
    opts: &HandlerOptions,
    stop: &mut oneshot::Receiver<()>,
) -> Result<Option<(Uffd, Vec<GuestRegionUffdMapping>)>> {
    listener
        .set_nonblocking(true)
        .context("put the uffd handshake listener in non-blocking mode")?;
    let listener = UnixListener::from_std(listener).context("adopt the uffd handshake listener")?;
    let accepted = tokio::select! {
        accepted = listener.accept() => accepted.context("accept the uffd handshake connection")?,
        _ = &mut *stop => return Ok(None),
        _ = tokio::time::sleep(opts.handshake_timeout) => {
            bail!("no uffd handshake connection within {:?}", opts.handshake_timeout)
        }
    };
    let stream = accepted
        .0
        .into_std()
        .context("detach the uffd handshake stream")?;
    stream
        .set_nonblocking(false)
        .context("put the uffd handshake stream in blocking mode")?;
    stream
        .set_read_timeout(Some(opts.handshake_timeout))
        .context("set the uffd handshake read timeout")?;
    let handshake = recv_handshake(&stream)?;
    let mut fds = handshake.fds.into_iter();
    let uffd = Uffd::from(fds.next().expect("recv_handshake yields at least one fd"));
    let extra = fds.count();
    if extra > 0 {
        tracing::debug!(extra, "uffd handshake carried extra descriptors; closed");
    }
    Ok(Some((uffd, handshake.mappings)))
}

struct Ctx<S> {
    uffd: AsyncFd<Uffd>,
    mappings: Vec<GuestRegionUffdMapping>,
    page_size: u64,
    source: Arc<S>,
    ring: AsyncIoRing,
    stats: Arc<Stats>,
    opts: HandlerOptions,
    inflight: RefCell<HashSet<u64>>,
    removed: RefCell<Vec<u64>>,
    pool: RefCell<Vec<Vec<u8>>>,
    zero_page: Vec<u8>,
    fatal: RefCell<Option<anyhow::Error>>,
    fatal_notify: Notify,
    active: Cell<usize>,
    drained: Notify,
}

impl<S: PageSource> Ctx<S> {
    fn mapping_for(&self, host_addr: u64) -> Option<&GuestRegionUffdMapping> {
        self.mappings.iter().find(|m| m.contains(host_addr))
    }

    fn set_fatal(&self, err: anyhow::Error) {
        let mut slot = self.fatal.borrow_mut();
        if slot.is_none() {
            *slot = Some(err);
            self.fatal_notify.notify_one();
        }
    }

    fn take_buf(&self) -> Vec<u8> {
        self.pool
            .borrow_mut()
            .pop()
            .unwrap_or_else(|| vec![0u8; self.page_size as usize])
    }

    fn give_buf(&self, buf: Vec<u8>) {
        let mut pool = self.pool.borrow_mut();
        if pool.len() < self.opts.max_inflight {
            pool.push(buf);
        }
    }

    fn is_removed(&self, page_idx: u64) -> bool {
        let removed = self.removed.borrow();
        let word = (page_idx / 64) as usize;
        word < removed.len() && removed[word] & (1u64 << (page_idx % 64)) != 0
    }

    fn mark_removed(&self, start: u64, end: u64) {
        let mut removed = self.removed.borrow_mut();
        for m in &self.mappings {
            let lo = start.max(m.base_host_virt_addr);
            let hi = end.min(m.base_host_virt_addr + m.size);
            if lo >= hi {
                continue;
            }
            let first = m.image_offset(lo) / self.page_size;
            let last = m.image_offset(hi - 1) / self.page_size;
            for idx in first..=last {
                let word = (idx / 64) as usize;
                if word < removed.len() {
                    removed[word] |= 1u64 << (idx % 64);
                }
            }
        }
    }
}

async fn run<S: PageSource>(
    uffd: Uffd,
    mappings: Vec<GuestRegionUffdMapping>,
    source: Arc<S>,
    opts: HandlerOptions,
    stats: Arc<Stats>,
    stop: &mut oneshot::Receiver<()>,
    state: &watch::Sender<HandlerState>,
) -> Result<()> {
    let page_size = mappings
        .first()
        .map(|m| m.page_size())
        .ok_or_else(|| anyhow!("no guest memory regions to serve"))?;
    if mappings.iter().any(|m| m.page_size() != page_size) {
        bail!("guest memory regions disagree on the page size");
    }
    if !page_size.is_power_of_two() || page_size < 4096 {
        bail!("unsupported page size {page_size}");
    }
    let total_pages = mappings
        .iter()
        .map(|m| m.end_offset())
        .max()
        .unwrap_or(0)
        .div_ceil(page_size);

    let ring = AsyncIoRingBuilder::new()
        .nr_sparse_buffer(opts.max_inflight * 2)
        .nr_sparse_file(16)
        .sqe_entries(opts.max_inflight * 2)
        .cqe_entries(opts.max_inflight * 4)
        .build()
        .context("create the io uring for the uffd handler")?;
    tokio::task::spawn_local({
        let ring = ring.clone();
        async move {
            if let Err(err) = ring.handle_completion().await {
                panic!("uffd handler uring handle_completion exited: {err:?}");
            }
        }
    });
    if HANDLER_URING.with(|uring| uring.set(ring.clone())).is_err() {
        bail!("the uffd handler uring was already initialized on this thread");
    }
    source.init(&ring).await.context("init the page source")?;
    uffd.set_nonblocking(true)
        .context("put the userfaultfd in non-blocking mode")?;
    let uffd = AsyncFd::new(uffd).context("register the userfaultfd with the reactor")?;

    let ctx = Rc::new(Ctx {
        uffd,
        mappings,
        page_size,
        source,
        ring,
        stats,
        inflight: RefCell::new(HashSet::new()),
        removed: RefCell::new(vec![0u64; total_pages.div_ceil(64) as usize]),
        pool: RefCell::new(Vec::new()),
        zero_page: vec![0u8; page_size as usize],
        fatal: RefCell::new(None),
        fatal_notify: Notify::new(),
        active: Cell::new(0),
        drained: Notify::new(),
        opts,
    });
    let permits = Arc::new(Semaphore::new(ctx.opts.max_inflight));
    let _ = state.send(HandlerState::Serving);
    tracing::info!(
        name = ctx.opts.name,
        regions = ctx.mappings.len(),
        page_size,
        total_pages,
        "uffd handler serving"
    );

    let mut msgs = vec![UffdMsg::default(); EVENT_BATCH];
    let outcome = loop {
        tokio::select! {
            _ = &mut *stop => break Ok(()),
            _ = ctx.fatal_notify.notified() => break Err(()),
            guard = ctx.uffd.readable() => {
                let mut guard = guard.context("poll the userfaultfd")?;
                let n = match guard.try_io(|fd| fd.get_ref().read_events(&mut msgs)) {
                    Ok(read) => read.context("read userfaultfd events")?,
                    Err(_would_block) => continue,
                };
                if n == 0 {
                    // EOF: the VMM closed its end.
                    break Ok(());
                }
                for msg in &msgs[..n] {
                    match msg.decode() {
                        Event::Pagefault { address, .. } => dispatch(&ctx, address, &permits).await?,
                        Event::Remove { start, end } => {
                            bump(&ctx.stats.removes);
                            ctx.mark_removed(start, end);
                        }
                        Event::Other(kind) => {
                            tracing::debug!(kind, "ignoring userfaultfd event");
                        }
                    }
                }
            }
        }
    };

    // Let in-flight installs land before the descriptor goes away; whatever
    // is left is cancelled with the LocalSet.
    let deadline = Instant::now() + ctx.opts.drain_timeout;
    while ctx.active.get() > 0 {
        let now = Instant::now();
        if now >= deadline {
            tracing::warn!(
                pending = ctx.active.get(),
                "uffd handler stopping with pages still in flight"
            );
            break;
        }
        tokio::select! {
            _ = ctx.drained.notified() => {}
            _ = tokio::time::sleep(deadline - now) => {}
        }
    }
    match outcome {
        Ok(()) => Ok(()),
        Err(()) => Err(ctx
            .fatal
            .borrow_mut()
            .take()
            .unwrap_or_else(|| anyhow!("uffd handler failed"))),
    }
}

async fn dispatch<S: PageSource>(
    ctx: &Rc<Ctx<S>>,
    address: u64,
    permits: &Arc<Semaphore>,
) -> Result<()> {
    bump(&ctx.stats.faults);
    let Some(mapping) = ctx.mapping_for(address) else {
        bail!("page fault at {address:#x} outside every registered region");
    };
    let aligned = address & !(ctx.page_size - 1);
    let offset = mapping.image_offset(aligned);
    if !ctx.inflight.borrow_mut().insert(aligned) {
        bump(&ctx.stats.duplicates);
        return Ok(());
    }
    let permit = Arc::clone(permits)
        .acquire_owned()
        .await
        .expect("the inflight semaphore is never closed");
    ctx.active.set(ctx.active.get() + 1);
    let ctx = Rc::clone(ctx);
    tokio::task::spawn_local(async move {
        let result = serve_page(&ctx, offset, aligned).await;
        drop(permit);
        ctx.inflight.borrow_mut().remove(&aligned);
        let active = ctx.active.get() - 1;
        ctx.active.set(active);
        if active == 0 {
            ctx.drained.notify_one();
        }
        if let Err(err) = result {
            ctx.set_fatal(err);
        }
    });
    Ok(())
}

async fn serve_page<S: PageSource>(ctx: &Ctx<S>, offset: u64, host_addr: u64) -> Result<()> {
    let page_size = ctx.page_size;
    if ctx.is_removed(offset / page_size) || offset >= ctx.source.size() {
        return install_zero(ctx, host_addr).await;
    }
    let mut buf = ctx.take_buf();
    let read = read_with_retry(ctx, offset, &mut buf).await;
    let result = match read {
        Ok(()) => {
            ctx.stats.bytes_read.fetch_add(page_size, Ordering::Relaxed);
            if is_all_zero(&buf) {
                install_zero(ctx, host_addr).await
            } else {
                install_copy(ctx, host_addr, &buf).await
            }
        }
        Err(err) => Err(err),
    };
    ctx.give_buf(buf);
    result
}

async fn read_with_retry<S: PageSource>(ctx: &Ctx<S>, offset: u64, buf: &mut [u8]) -> Result<()> {
    let started = Instant::now();
    let mut attempt = 0u32;
    loop {
        match ctx.source.read_page(&ctx.ring, offset, buf).await {
            Ok(()) => return Ok(()),
            Err(err) => {
                let elapsed = started.elapsed();
                if elapsed >= ctx.opts.read_retry_budget {
                    return Err(err.context(format!(
                        "page read at offset {offset} failed for {elapsed:?} ({attempt} retries)"
                    )));
                }
                bump(&ctx.stats.read_retries);
                let backoff = READ_RETRY_BASE
                    .saturating_mul(1u32 << attempt.min(6))
                    .min(READ_RETRY_MAX)
                    .min(ctx.opts.read_retry_budget.saturating_sub(elapsed));
                tracing::warn!(
                    name = ctx.opts.name,
                    offset,
                    attempt,
                    ?backoff,
                    error = format!("{err:#}"),
                    "uffd page read failed; retrying"
                );
                attempt += 1;
                tokio::time::sleep(backoff).await;
            }
        }
    }
}

async fn install_copy<S: PageSource>(ctx: &Ctx<S>, host_addr: u64, buf: &[u8]) -> Result<()> {
    let uffd = ctx.uffd.get_ref();
    let len = ctx.page_size;
    let mut attempt = 0u32;
    loop {
        match uffd.copy(host_addr, buf.as_ptr(), len, 0) {
            Ok(()) => {
                bump(&ctx.stats.pages_copied);
                return Ok(());
            }
            Err(err) => match retry_install(ctx, host_addr, err, &mut attempt).await? {
                InstallOutcome::Done => return Ok(()),
                InstallOutcome::Retry => {}
            },
        }
    }
}

async fn install_zero<S: PageSource>(ctx: &Ctx<S>, host_addr: u64) -> Result<()> {
    let uffd = ctx.uffd.get_ref();
    let len = ctx.page_size;
    let mut attempt = 0u32;
    loop {
        // The shared zero page exists for 4 KiB pages only; a huge page is
        // copied from a zero buffer.
        let result = if len == 4096 {
            uffd.zeropage(host_addr, len, 0)
        } else {
            uffd.copy(host_addr, ctx.zero_page.as_ptr(), len, 0)
        };
        match result {
            Ok(()) => {
                bump(&ctx.stats.pages_zeroed);
                return Ok(());
            }
            Err(err) => match retry_install(ctx, host_addr, err, &mut attempt).await? {
                InstallOutcome::Done => return Ok(()),
                InstallOutcome::Retry => {}
            },
        }
    }
}

enum InstallOutcome {
    Done,
    Retry,
}

async fn retry_install<S: PageSource>(
    ctx: &Ctx<S>,
    host_addr: u64,
    err: std::io::Error,
    attempt: &mut u32,
) -> Result<InstallOutcome> {
    match err.raw_os_error() {
        Some(libc::EEXIST) => {
            // Another install won (a racing fault on the same page whose
            // event arrived after this one left the inflight set). Its wake
            // covered every waiter in range; this one is belt and braces.
            bump(&ctx.stats.already_present);
            let _ = ctx.uffd.get_ref().wake(host_addr, ctx.page_size);
            Ok(InstallOutcome::Done)
        }
        Some(libc::EAGAIN) => {
            // The address space is changing (madvise, mremap); the kernel
            // does not redeliver, so keep trying here.
            bump(&ctx.stats.copy_retries);
            *attempt += 1;
            if *attempt > COPY_RETRY_LIMIT {
                bail!("install at {host_addr:#x} kept answering EAGAIN");
            }
            tokio::time::sleep(Duration::from_micros(50)).await;
            Ok(InstallOutcome::Retry)
        }
        Some(libc::ESRCH) => bail!("the VMM process is gone (install at {host_addr:#x}: {err})"),
        _ => Err(anyhow!(err).context(format!("install page at {host_addr:#x}"))),
    }
}

fn is_all_zero(buf: &[u8]) -> bool {
    let (chunks, rest) = buf.as_chunks::<8>();
    chunks.iter().all(|c| u64::from_ne_bytes(*c) == 0) && rest.iter().all(|b| *b == 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_detection_sees_every_byte() {
        let mut page = vec![0u8; 4096];
        assert!(is_all_zero(&page));
        page[4095] = 1;
        assert!(!is_all_zero(&page));
        page[4095] = 0;
        page[7] = 1;
        assert!(!is_all_zero(&page));
        assert!(is_all_zero(&[0u8; 13]));
        assert!(!is_all_zero(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]));
    }
}
