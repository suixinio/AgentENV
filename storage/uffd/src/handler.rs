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
//!
//! The event loop never waits on a page: a fault that finds every install
//! slot taken queues until one frees, so `REMOVE` events, `stop` and a fatal
//! error are seen while the source is slow or hung.

use std::cell::{Cell, RefCell};
use std::collections::{HashSet, VecDeque};
use std::os::fd::OwnedFd;
use std::os::unix::net::UnixListener as StdUnixListener;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use storage_util::io_ring::{AsyncIoRing, AsyncIoRingBuilder};
use tokio::io::unix::AsyncFd;
use tokio::net::UnixListener;
use tokio::sync::{mpsc, oneshot, watch, Notify, OwnedSemaphorePermit, Semaphore};

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
    /// How long one read attempt may take before it counts as failed and is
    /// retried; a hung source is otherwise indistinguishable from a slow one.
    pub read_timeout: Duration,
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
            read_timeout: Duration::from_secs(10),
            handshake_timeout: Duration::from_secs(60),
            drain_timeout: Duration::from_secs(2),
            name: "uffd".to_string(),
        }
    }
}

impl HandlerOptions {
    /// The longest `stop` waits for the thread after asking it to stop: the
    /// drain, one read attempt that may be mid-cancel, and slack.
    fn stop_deadline(&self) -> Duration {
        self.drain_timeout + self.read_timeout + Duration::from_secs(5)
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
    /// Pages installed ahead of the guest by `prefault`.
    pub prefaulted: u64,
    /// Faults at addresses no handshake region covers, answered with a zero
    /// page.
    pub unmapped: u64,
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
    prefaulted: AtomicU64,
    unmapped: AtomicU64,
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
            prefaulted: load(&self.prefaulted),
            unmapped: load(&self.unmapped),
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
    stop: Mutex<Option<oneshot::Sender<()>>>,
    thread: Option<thread::JoinHandle<()>>,
    opts: HandlerOptions,
    state: watch::Receiver<HandlerState>,
    stats: Arc<Stats>,
    prefault: mpsc::UnboundedSender<Vec<u64>>,
    /// One bit per image page, set once the page is installed in the guest.
    served: Arc<Mutex<Vec<u64>>>,
    /// The regions' page size, 0 until the handshake.
    page_size: Arc<AtomicU64>,
}

/// What the handler thread runs with.
struct ThreadInputs {
    entry: Entry,
    opts: HandlerOptions,
    stats: Arc<Stats>,
    served: Arc<Mutex<Vec<u64>>>,
    page_size: Arc<AtomicU64>,
    prefault_rx: mpsc::UnboundedReceiver<Vec<u64>>,
    stop: oneshot::Receiver<()>,
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
        let (prefault_tx, prefault_rx) = mpsc::unbounded_channel();
        let stats = Arc::new(Stats::default());
        let served = Arc::new(Mutex::new(Vec::new()));
        let page_size = Arc::new(AtomicU64::new(0));
        let inputs = ThreadInputs {
            entry,
            opts: opts.clone(),
            stats: Arc::clone(&stats),
            served: Arc::clone(&served),
            page_size: Arc::clone(&page_size),
            prefault_rx,
            stop: stop_rx,
        };
        let name = opts.name.clone();
        let thread = thread::Builder::new()
            .name(format!("uffd-{name}"))
            .spawn(move || {
                // A panic anywhere on the thread still publishes `Exited`, so
                // a query never reports a dead handler as serving.
                let result =
                    match catch_unwind(AssertUnwindSafe(|| thread_main(inputs, source, &state_tx)))
                    {
                        Ok(result) => result,
                        Err(payload) => Err(anyhow!(
                            "uffd handler thread panicked: {}",
                            panic_message(payload.as_ref())
                        )),
                    };
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
            stop: Mutex::new(Some(stop_tx)),
            thread: Some(thread),
            opts,
            state: state_rx,
            stats,
            prefault: prefault_tx,
            served,
            page_size,
        })
    }

    /// The page size of the served regions; `None` before the handshake.
    pub fn page_size(&self) -> Option<u64> {
        match self.page_size.load(Ordering::Acquire) {
            0 => None,
            size => Some(size),
        }
    }

    /// Page indices (image offset over page size) installed in the guest so
    /// far, ascending: the working set to prefault on the next resume of the
    /// same image.
    pub fn faulted_pages(&self) -> Vec<u64> {
        let served = self
            .served
            .lock()
            .expect("the served bitmap is never poisoned");
        let mut pages = Vec::new();
        for (word_idx, word) in served.iter().enumerate() {
            let mut bits = *word;
            while bits != 0 {
                let bit = bits.trailing_zeros() as u64;
                pages.push(word_idx as u64 * 64 + bit);
                bits &= bits - 1;
            }
        }
        pages
    }

    /// Installs `pages` (page indices) in the background ahead of the guest,
    /// on a fraction of the fault budget; pages already present, discarded or
    /// outside the mapped regions are skipped. Fails once the thread is gone.
    pub fn prefault(&self, pages: Vec<u64>) -> Result<()> {
        self.prefault
            .send(pages)
            .map_err(|_| anyhow!("uffd handler is not running"))
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
    /// A thread that does not stop within the options' deadline is left
    /// running and reported as an error rather than blocking the caller.
    pub fn stop(mut self) -> Result<()> {
        self.request_stop();
        self.join()
    }

    /// Asks the thread to stop without waiting; `wait_exit` or `stop`
    /// observe the outcome.
    pub fn request_stop(&self) {
        let sender = self
            .stop
            .lock()
            .expect("the stop sender is never poisoned")
            .take();
        if let Some(stop) = sender {
            let _ = stop.send(());
        }
    }

    fn join(&mut self) -> Result<()> {
        if let Some(thread) = self.thread.take() {
            let deadline = Instant::now() + self.opts.stop_deadline();
            while !thread.is_finished() {
                if Instant::now() >= deadline {
                    // Leave it running: joining would block on whatever the
                    // source is stuck in, and the descriptor is already
                    // unusable for the guest once the thread's loop stopped.
                    bail!(
                        "the uffd handler thread did not stop within {:?}",
                        self.opts.stop_deadline()
                    );
                }
                thread::sleep(Duration::from_millis(5));
            }
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
        self.request_stop();
    }
}

fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "non-string panic payload".to_string()
    }
}

thread_local! {
    static HANDLER_URING: std::cell::OnceCell<AsyncIoRing> = const { std::cell::OnceCell::new() };
}

fn thread_main<S: PageSource>(
    inputs: ThreadInputs,
    source: Arc<S>,
    state: &watch::Sender<HandlerState>,
) -> Result<()> {
    let ThreadInputs {
        entry,
        opts,
        stats,
        served,
        page_size,
        prefault_rx,
        mut stop,
    } = inputs;
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
        let inputs = RunInputs {
            opts,
            stats,
            served,
            page_size,
            prefault_rx,
        };
        run(uffd, mappings, source, inputs, &mut stop, state).await
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
    let uffd = Uffd::from(
        fds.next()
            .ok_or_else(|| anyhow!("uffd handshake carried no descriptor"))?,
    );
    let extra = fds.count();
    if extra > 0 {
        tracing::debug!(extra, "uffd handshake carried extra descriptors; closed");
    }
    Ok(Some((uffd, handshake.mappings)))
}

struct RunInputs {
    opts: HandlerOptions,
    stats: Arc<Stats>,
    served: Arc<Mutex<Vec<u64>>>,
    page_size: Arc<AtomicU64>,
    prefault_rx: mpsc::UnboundedReceiver<Vec<u64>>,
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
    served: Arc<Mutex<Vec<u64>>>,
    /// Install slots for guest faults; a fault that finds none waits in
    /// `pending` rather than blocking the event loop.
    permits: Arc<Semaphore>,
    pending: RefCell<VecDeque<(u64, u64)>>,
    prefault_permits: Arc<Semaphore>,
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

    fn host_addr_for_offset(&self, offset: u64) -> Option<u64> {
        self.mappings
            .iter()
            .find(|m| offset >= m.offset && offset < m.end_offset())
            .map(|m| m.host_addr(offset))
    }

    fn is_served(&self, page_idx: u64) -> bool {
        let served = self
            .served
            .lock()
            .expect("the served bitmap is never poisoned");
        let word = (page_idx / 64) as usize;
        word < served.len() && served[word] & (1u64 << (page_idx % 64)) != 0
    }

    fn mark_served(&self, page_idx: u64) {
        let mut served = self
            .served
            .lock()
            .expect("the served bitmap is never poisoned");
        let word = (page_idx / 64) as usize;
        if word < served.len() {
            served[word] |= 1u64 << (page_idx % 64);
        }
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
    inputs: RunInputs,
    stop: &mut oneshot::Receiver<()>,
    state: &watch::Sender<HandlerState>,
) -> Result<()> {
    let RunInputs {
        opts,
        stats,
        served,
        page_size: page_size_out,
        mut prefault_rx,
    } = inputs;
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
    if HANDLER_URING.with(|uring| uring.set(ring.clone())).is_err() {
        bail!("the uffd handler uring was already initialized on this thread");
    }
    uffd.set_nonblocking(true)
        .context("put the userfaultfd in non-blocking mode")?;
    let uffd = AsyncFd::new(uffd).context("register the userfaultfd with the reactor")?;
    let bitmap_words = total_pages.div_ceil(64) as usize;
    *served.lock().expect("the served bitmap is never poisoned") = vec![0u64; bitmap_words];

    let ctx = Rc::new(Ctx {
        uffd,
        mappings,
        page_size,
        source,
        ring,
        stats,
        inflight: RefCell::new(HashSet::new()),
        removed: RefCell::new(vec![0u64; bitmap_words]),
        served,
        permits: Arc::new(Semaphore::new(opts.max_inflight)),
        pending: RefCell::new(VecDeque::new()),
        prefault_permits: Arc::new(Semaphore::new((opts.max_inflight / 4).max(1))),
        pool: RefCell::new(Vec::new()),
        zero_page: vec![0u8; page_size as usize],
        fatal: RefCell::new(None),
        fatal_notify: Notify::new(),
        active: Cell::new(0),
        drained: Notify::new(),
        opts,
    });
    // The completion task ending is the ring being unusable: every read in
    // flight would wait forever, so it ends the handler instead.
    tokio::task::spawn_local({
        let ctx = Rc::clone(&ctx);
        async move {
            let outcome = ctx.ring.handle_completion().await;
            ctx.set_fatal(match outcome {
                Ok(()) => anyhow!("the uffd handler uring completion loop ended"),
                Err(err) => anyhow!(err).context("the uffd handler uring completion loop failed"),
            });
        }
    });
    ctx.source
        .init(&ctx.ring)
        .await
        .context("init the page source")?;
    page_size_out.store(page_size, Ordering::Release);
    let _ = state.send(HandlerState::Serving);
    tracing::info!(
        name = ctx.opts.name,
        regions = ctx.mappings.len(),
        page_size,
        total_pages,
        "uffd handler serving"
    );

    let mut msgs = vec![UffdMsg::default(); EVENT_BATCH];
    let mut prefault_open = true;
    let outcome = loop {
        tokio::select! {
            _ = &mut *stop => break Ok(()),
            _ = ctx.fatal_notify.notified() => break Err(()),
            pages = prefault_rx.recv(), if prefault_open => match pages {
                Some(pages) => {
                    tokio::task::spawn_local(prefault_all(Rc::clone(&ctx), pages));
                }
                None => prefault_open = false,
            },
            guard = ctx.uffd.readable() => {
                let mut guard = guard.context("poll the userfaultfd")?;
                let n = match guard.try_io(|fd| fd.get_ref().read_events(&mut msgs)) {
                    Ok(Ok(n)) => n,
                    Ok(Err(err)) if err.raw_os_error() == Some(libc::EINTR) => continue,
                    Ok(Err(err)) => return Err(err).context("read userfaultfd events"),
                    Err(_would_block) => continue,
                };
                if n == 0 {
                    // EOF: the VMM closed its end.
                    break Ok(());
                }
                for msg in &msgs[..n] {
                    match msg.decode() {
                        Event::Pagefault { address, .. } => dispatch(&ctx, address),
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

/// Routes one fault: a page already in flight is a duplicate, a page no
/// region covers gets a zero page, everything else takes an install slot or
/// queues for one. Never waits, so the event loop keeps reading.
fn dispatch<S: PageSource>(ctx: &Rc<Ctx<S>>, address: u64) {
    bump(&ctx.stats.faults);
    let aligned = address & !(ctx.page_size - 1);
    let Some(mapping) = ctx.mapping_for(address) else {
        // Registered with the kernel but absent from the handshake: nothing
        // in the image backs it. A wake alone would refault forever.
        bump(&ctx.stats.unmapped);
        tracing::warn!(
            name = ctx.opts.name,
            address = format!("{address:#x}"),
            "page fault outside every handshake region; installing a zero page"
        );
        let ctx = Rc::clone(ctx);
        tokio::task::spawn_local(async move {
            if let Err(err) = install_zero(&ctx, aligned).await {
                ctx.set_fatal(err);
            }
        });
        return;
    };
    let offset = mapping.image_offset(aligned);
    if !ctx.inflight.borrow_mut().insert(aligned) {
        bump(&ctx.stats.duplicates);
        return;
    }
    match Arc::clone(&ctx.permits).try_acquire_owned() {
        Ok(permit) => spawn_serve(ctx, offset, aligned, permit, false),
        Err(_) => ctx.pending.borrow_mut().push_back((offset, aligned)),
    }
}

/// Starts queued faults on the slots that just freed.
fn pump_pending<S: PageSource>(ctx: &Rc<Ctx<S>>) {
    loop {
        let Some((offset, host_addr)) = ctx.pending.borrow_mut().pop_front() else {
            return;
        };
        match Arc::clone(&ctx.permits).try_acquire_owned() {
            Ok(permit) => spawn_serve(ctx, offset, host_addr, permit, false),
            Err(_) => {
                ctx.pending.borrow_mut().push_front((offset, host_addr));
                return;
            }
        }
    }
}

/// Reads and installs one page on its own task. `host_addr` is already in
/// the inflight set; the task takes it out again.
fn spawn_serve<S: PageSource>(
    ctx: &Rc<Ctx<S>>,
    offset: u64,
    host_addr: u64,
    permit: OwnedSemaphorePermit,
    prefault: bool,
) {
    ctx.active.set(ctx.active.get() + 1);
    let ctx = Rc::clone(ctx);
    tokio::task::spawn_local(async move {
        let result = serve_page(&ctx, offset, host_addr).await;
        drop(permit);
        ctx.inflight.borrow_mut().remove(&host_addr);
        pump_pending(&ctx);
        let active = ctx.active.get() - 1;
        ctx.active.set(active);
        if active == 0 {
            ctx.drained.notify_one();
        }
        match result {
            Ok(()) => {
                ctx.mark_served(offset / ctx.page_size);
                if prefault {
                    bump(&ctx.stats.prefaulted);
                }
            }
            Err(err) => ctx.set_fatal(err),
        }
    });
}

async fn prefault_all<S: PageSource>(ctx: Rc<Ctx<S>>, pages: Vec<u64>) {
    for idx in pages {
        if ctx.is_served(idx) || ctx.is_removed(idx) {
            continue;
        }
        let offset = idx * ctx.page_size;
        let Some(host_addr) = ctx.host_addr_for_offset(offset) else {
            continue;
        };
        if ctx.inflight.borrow().contains(&host_addr) {
            continue;
        }
        let Ok(permit) = Arc::clone(&ctx.prefault_permits).acquire_owned().await else {
            return;
        };
        // A fault may have served the page while this waited for a permit.
        if ctx.is_served(idx) || !ctx.inflight.borrow_mut().insert(host_addr) {
            continue;
        }
        spawn_serve(&ctx, offset, host_addr, permit, true);
    }
}

async fn serve_page<S: PageSource>(ctx: &Ctx<S>, offset: u64, host_addr: u64) -> Result<()> {
    let page_size = ctx.page_size;
    let page_idx = offset / page_size;
    if ctx.is_removed(page_idx) || offset >= ctx.source.size() {
        return install_zero(ctx, host_addr).await;
    }
    let (read, buf) = read_with_retry(ctx, offset).await;
    let result = match read {
        Ok(()) => {
            ctx.stats.bytes_read.fetch_add(page_size, Ordering::Relaxed);
            let content = if is_all_zero(&buf) {
                None
            } else {
                Some(&buf[..])
            };
            install(ctx, host_addr, page_idx, content).await
        }
        Err(err) => Err(err),
    };
    ctx.give_buf(buf);
    result
}

/// Reads the page at `offset` into a pool buffer, retrying failures with
/// backoff until `read_retry_budget` runs out. An attempt that outlives
/// `read_timeout` is abandoned: its buffer may still be written by the ring,
/// so it is forgotten rather than reused, and the retry takes a fresh one.
async fn read_with_retry<S: PageSource>(ctx: &Ctx<S>, offset: u64) -> (Result<()>, Vec<u8>) {
    let started = Instant::now();
    let mut attempt = 0u32;
    let mut buf = ctx.take_buf();
    loop {
        let attempt_result = tokio::time::timeout(
            ctx.opts.read_timeout,
            ctx.source.read_page(&ctx.ring, offset, &mut buf),
        )
        .await;
        let err = match attempt_result {
            Ok(Ok(())) => return (Ok(()), buf),
            Ok(Err(err)) => err,
            Err(_elapsed) => {
                std::mem::forget(std::mem::replace(&mut buf, ctx.take_buf()));
                anyhow!("page read timed out after {:?}", ctx.opts.read_timeout)
            }
        };
        let elapsed = started.elapsed();
        if elapsed >= ctx.opts.read_retry_budget {
            let err = err.context(format!(
                "page read at offset {offset} failed for {elapsed:?} ({attempt} retries)"
            ));
            return (Err(err), buf);
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

/// Installs `content` at `host_addr`, or the zero page when `content` is
/// `None`. A `REMOVE` that arrived while the page was being read wins: the
/// check sits right before each ioctl with no await in between, and the
/// event loop runs on this thread, so what it sees is current.
async fn install<S: PageSource>(
    ctx: &Ctx<S>,
    host_addr: u64,
    page_idx: u64,
    content: Option<&[u8]>,
) -> Result<()> {
    let uffd = ctx.uffd.get_ref();
    let len = ctx.page_size;
    let mut attempt = 0u32;
    loop {
        let content = if ctx.is_removed(page_idx) {
            None
        } else {
            content
        };
        let result = match content {
            Some(buf) => uffd.copy(host_addr, buf.as_ptr(), len, 0),
            // The shared zero page exists for 4 KiB pages only; a huge page
            // is copied from a zero buffer.
            None if len == 4096 => uffd.zeropage(host_addr, len, 0),
            None => uffd.copy(host_addr, ctx.zero_page.as_ptr(), len, 0),
        };
        match result {
            Ok(()) => {
                bump(if content.is_some() {
                    &ctx.stats.pages_copied
                } else {
                    &ctx.stats.pages_zeroed
                });
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
    install(ctx, host_addr, u64::MAX, None).await
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
        Some(libc::ENOENT) => {
            // The range is no longer registered (unmapped or unregistered
            // under the fault): nothing to install and nobody left waiting.
            bump(&ctx.stats.already_present);
            tracing::debug!(
                name = ctx.opts.name,
                host_addr = format!("{host_addr:#x}"),
                "install on a range that is no longer registered"
            );
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
