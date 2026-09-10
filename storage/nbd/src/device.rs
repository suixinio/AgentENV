//! One connected `/dev/nbdN` device: its connection workers, the netlink
//! socket that configured it, and the request loop each worker runs.

use anyhow::{bail, Context, Result};
use std::cell::OnceCell;
use std::io::ErrorKind;
use std::os::fd::{AsRawFd, OwnedFd};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use storage_util::io_ring::{AsyncIoRing, AsyncIoRingBuilder};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::unix::OwnedWriteHalf;
use tokio::sync::{mpsc, oneshot, watch, Mutex, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;

use crate::netlink::{ConnectSpec, NbdNetlink, ReconfigureSpec};
use crate::proto::{
    reply_error, NbdCommand, NbdReply, NbdRequest, NBD_CFLAG_DESTROY_ON_DISCONNECT,
    NBD_FLAG_CAN_MULTI_CONN, NBD_FLAG_HAS_FLAGS, NBD_FLAG_READ_ONLY, NBD_FLAG_SEND_FLUSH,
    NBD_FLAG_SEND_FUA, NBD_FLAG_SEND_TRIM, REQUEST_LEN,
};
use crate::target::{Geometry, NbdTarget};
use crate::{device_size_bytes, wait_for_nbd_dev};

// The kernel takes a replacement socket only into a connection slot it has
// already marked dead, and answers the message as a success even when it had
// fewer dead slots than the sockets offered. A reattach issued before the
// kernel has noticed every socket die therefore reports success while leaving
// slots dead, and the device answers EIO from then on.
const REATTACH_SETTLE: Duration = Duration::from_millis(500);

// What a replacement connection waits before each attempt. Five attempts over
// roughly ten seconds, settle included, so a replacement lands well inside the
// default dead-connection window.
const REPLACEMENT_BACKOFF: [Duration; 5] = [
    Duration::from_millis(0),
    Duration::from_secs(1),
    Duration::from_secs(2),
    Duration::from_secs(3),
    Duration::from_secs(3),
];

// The kernel's max_hw_sectors for nbd is 65536 sectors. A longer length in a
// request header means the stream desynchronized, not a large transfer.
const MAX_REQUEST_LEN: u32 = 32 * 1024 * 1024;

thread_local! {
    static NBD_CONN_URING: OnceCell<AsyncIoRing> = const { OnceCell::new() };
}

/// How a device is brought up. `queue_depth` bounds the requests one
/// connection dispatches concurrently.
#[derive(Debug, Clone)]
pub struct NbdOptions {
    pub connections: u16,
    /// How long the kernel waits for a request to be answered. After it the
    /// request fails EIO and the kernel takes that connection down, which the
    /// supervisor then replaces.
    pub io_timeout: Duration,
    /// How long a request with no live connection waits for a replacement
    /// before failing EIO. `None` fails it at once and shuts the device down,
    /// which leaves the supervisor nothing to save.
    pub dead_conn_timeout: Option<Duration>,
    pub queue_depth: usize,
    pub backend_identifier: Option<String>,
    /// `NBD_CFLAG_DESTROY_ON_DISCONNECT`. Set, the kernel removes the whole
    /// gendisk on disconnect, consuming one of the `nbds_max` devices the
    /// module preallocated and never giving it back. Clear, the node survives
    /// at capacity zero and keeps its index for the next connect.
    pub destroy_on_disconnect: bool,
}

impl Default for NbdOptions {
    fn default() -> Self {
        Self {
            connections: 4,
            io_timeout: Duration::from_secs(90),
            dead_conn_timeout: Some(Duration::from_secs(30)),
            queue_depth: 64,
            backend_identifier: None,
            destroy_on_disconnect: false,
        }
    }
}

/// Why a connection worker stopped serving.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConnectionExit {
    /// The device asked for it, so nothing is wrong.
    ShutdownRequested,
    /// The kernel sent NBD_CMD_DISC: the whole device is going away.
    Disconnected,
    /// The socket died under the worker. The kernel closes it after an io
    /// timeout, so this is the shape a stalled request leaves behind.
    Lost,
}

struct ConnectionHandle {
    thread: thread::JoinHandle<()>,
    shutdown: Option<oneshot::Sender<()>>,
}

impl ConnectionHandle {
    /// Signals the worker and waits for its thread, which has already exited
    /// when the caller got here through its exit report.
    fn join(mut self) {
        self.shutdown.take();
        if self.thread.join().is_err() {
            tracing::error!("an nbd connection worker thread panicked");
        }
    }
}

/// One connection's userspace end, before the kernel has been given the other.
struct SpawnedConnection {
    kernel_end: OwnedFd,
    handle: ConnectionHandle,
}

/// A worker's exit, tagged so a report from a connection the supervisor has
/// already replaced cannot be mistaken for its successor dying.
type ExitReport = (u16, u64, ConnectionExit);

/// Builds one replacement connection over the device's own target, which the
/// supervisor cannot name because [`NbdDevice`] is not generic over it.
type ConnectionFactory = Arc<
    dyn Fn(
            u16,
            u64,
            mpsc::UnboundedSender<ExitReport>,
        )
            -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<SpawnedConnection>> + Send>>
        + Send
        + Sync,
>;

fn connection_factory<T: NbdTarget>(target: Arc<T>, queue_depth: usize) -> ConnectionFactory {
    Arc::new(move |ordinal, generation, exits| {
        let target = Arc::clone(&target);
        Box::pin(
            async move { spawn_connection(target, ordinal, generation, queue_depth, exits).await },
        )
    })
}

struct SupervisorHandle {
    /// Dropping it is the stand-down signal.
    _stand_down: watch::Sender<bool>,
    commands: mpsc::UnboundedSender<SupervisorCommand>,
    finished: oneshot::Receiver<()>,
}

enum SupervisorCommand {
    DropConnection(u16),
}

/// A live `/dev/nbdN` served by this process.
pub struct NbdDevice {
    netlink: NbdNetlink,
    index: u32,
    device_path: PathBuf,
    backend_identifier: Option<String>,
    supervisor: Option<SupervisorHandle>,
    stopped: bool,
}

impl NbdDevice {
    /// Brings the target up as a new `/dev/nbdN` and returns once the device
    /// node is open-able and reports the target's size.
    pub async fn start<T: NbdTarget>(target: Arc<T>, opts: NbdOptions) -> Result<Self> {
        let geometry = target.geometry();
        validate_geometry(&geometry)?;
        let connections = opts.connections.max(1);

        let netlink = NbdNetlink::open()?;
        let factory = connection_factory(target, opts.queue_depth.max(1));
        let (exits_tx, exits_rx) = mpsc::unbounded_channel();
        let (kernel_ends, handles) = spawn_all(&factory, connections, &exits_tx).await?;

        let spec = ConnectSpec {
            index: None,
            size_bytes: geometry.size_bytes,
            block_size: geometry.block_size,
            timeout: Some(opts.io_timeout),
            dead_conn_timeout: opts.dead_conn_timeout,
            server_flags: server_flags(&geometry),
            client_flags: if opts.destroy_on_disconnect {
                NBD_CFLAG_DESTROY_ON_DISCONNECT
            } else {
                0
            },
            sockets: kernel_ends.iter().map(|fd| fd.as_raw_fd()).collect(),
            backend_identifier: opts.backend_identifier.clone(),
        };
        let index = match netlink.connect(&spec) {
            Ok(index) => index,
            Err(err) => {
                drop(kernel_ends);
                join_all(handles);
                return Err(err);
            }
        };
        // The kernel holds its own reference to each socket now.
        drop(kernel_ends);

        let device = Self {
            netlink,
            index,
            device_path: PathBuf::from(format!("/dev/nbd{index}")),
            backend_identifier: opts.backend_identifier.clone(),
            supervisor: Some(spawn_supervisor(
                index, factory, handles, exits_tx, exits_rx, &opts,
            )?),
            stopped: false,
        };
        if let Err(err) = wait_for_nbd_dev(index, geometry.size_bytes) {
            device.stop().await.ok();
            return Err(err);
        }
        tracing::info!(
            index,
            target = T::DEV_NAME,
            connections,
            size_bytes = geometry.size_bytes,
            block_size = geometry.block_size,
            "nbd device connected"
        );
        Ok(device)
    }

    /// Serve an existing `/dev/nbd<index>` from fresh connections, replacing
    /// those of a server that went away.
    ///
    /// `opts` must name the connection count the abandoned server had: the
    /// kernel fills one dead slot per socket offered and drops the surplus
    /// without saying so.
    pub async fn reattach<T: NbdTarget>(
        index: u32,
        target: Arc<T>,
        opts: NbdOptions,
    ) -> Result<Self> {
        let geometry = target.geometry();
        validate_geometry(&geometry)?;
        tokio::time::sleep(REATTACH_SETTLE).await;
        let netlink = NbdNetlink::open()?;
        let connections = opts.connections.max(1);
        let factory = connection_factory(target, opts.queue_depth.max(1));
        let (exits_tx, exits_rx) = mpsc::unbounded_channel();
        let (kernel_ends, handles) = spawn_all(&factory, connections, &exits_tx).await?;

        let spec = ReconfigureSpec {
            timeout: Some(opts.io_timeout),
            dead_conn_timeout: opts.dead_conn_timeout,
            sockets: kernel_ends.iter().map(|fd| fd.as_raw_fd()).collect(),
            backend_identifier: opts.backend_identifier.clone(),
            ..Default::default()
        };
        if let Err(err) = netlink.reconfigure(index, &spec) {
            drop(kernel_ends);
            join_all(handles);
            return Err(err);
        }
        drop(kernel_ends);

        tracing::info!(
            index,
            target = T::DEV_NAME,
            connections,
            "nbd device reattached"
        );
        Ok(Self {
            netlink,
            index,
            device_path: PathBuf::from(format!("/dev/nbd{index}")),
            backend_identifier: opts.backend_identifier.clone(),
            supervisor: Some(spawn_supervisor(
                index, factory, handles, exits_tx, exits_rx, &opts,
            )?),
            stopped: false,
        })
    }

    /// Close the connections without telling the kernel, the way a server that
    /// crashed would. The device keeps its index with every slot marked dead,
    /// ready for [`NbdDevice::reattach`].
    pub async fn abandon(mut self) -> u32 {
        self.stopped = true;
        let index = self.index;
        self.stand_down().await;
        tracing::info!(index, "nbd device abandoned without a disconnect");
        index
    }

    /// Stops the supervisor and, with it, every connection worker. Nothing
    /// replaces a connection from here on.
    async fn stand_down(&mut self) {
        let Some(supervisor) = self.supervisor.take() else {
            return;
        };
        drop(supervisor._stand_down);
        let _ = supervisor.finished.await;
    }

    /// Closes one connection and lets the supervisor rebuild it, the way a
    /// worker that died is rebuilt. Returns once the supervisor has taken the
    /// request, not once the replacement has landed.
    pub fn drop_connection(&self, ordinal: u16) -> Result<()> {
        let supervisor = self
            .supervisor
            .as_ref()
            .context("this nbd device has no supervisor to rebuild a connection")?;
        supervisor
            .commands
            .send(SupervisorCommand::DropConnection(ordinal))
            .map_err(|_| anyhow::anyhow!("the nbd connection supervisor has stopped"))
    }

    pub fn index(&self) -> u32 {
        self.index
    }

    pub fn device_path(&self) -> &Path {
        &self.device_path
    }

    /// Sets a new capacity through RECONFIGURE and reads it back, because the
    /// kernel answers the message whether or not it acted on the attribute.
    pub async fn update_size(&self, new_size_bytes: u64) -> Result<()> {
        let spec = ReconfigureSpec {
            size_bytes: Some(new_size_bytes),
            backend_identifier: self.backend_identifier.clone(),
            ..Default::default()
        };
        self.netlink.reconfigure(self.index, &spec)?;
        let actual = device_size_bytes(self.index)?;
        if actual != new_size_bytes {
            bail!(
                "nbd{} still reports {actual} bytes after RECONFIGURE asked for \
                 {new_size_bytes}: this kernel's NBD_CMD_RECONFIGURE did not act on \
                 NBD_ATTR_SIZE_BYTES",
                self.index
            );
        }
        Ok(())
    }

    /// Disconnects the device and waits for every connection worker to finish.
    pub async fn stop(mut self) -> Result<()> {
        self.stopped = true;
        let index = self.index;
        // The supervisor stands down first: a disconnect closes the sockets,
        // and a supervisor still watching would read that as a connection to
        // replace.
        self.stand_down().await;
        if let Err(err) = self.netlink.disconnect(index) {
            match err.root_cause().downcast_ref::<std::io::Error>() {
                Some(io_err)
                    if matches!(io_err.raw_os_error(), Some(libc::ENOENT | libc::EINVAL)) =>
                {
                    tracing::info!(index, "nbd device was already gone at disconnect");
                }
                _ => return Err(err),
            }
        }
        self.wait_for_teardown().await;
        tracing::info!(index, "nbd device disconnected");
        Ok(())
    }

    // The kernel finishes a disconnect after the last socket closes, so the
    // capacity a caller sees would still be the old one without this.
    async fn wait_for_teardown(&self) {
        for _ in 0..200 {
            match self.netlink.status(self.index) {
                Ok(false) => return,
                Ok(true) => {}
                Err(err) => {
                    tracing::debug!(index = self.index, ?err, "nbd status during teardown");
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        tracing::warn!(
            index = self.index,
            "nbd device is still listed as connected after disconnect"
        );
    }
}

impl Drop for NbdDevice {
    fn drop(&mut self) {
        if self.stopped {
            return;
        }
        tracing::warn!(
            index = self.index,
            "nbd device dropped without stop; disconnecting"
        );
        if let Err(err) = self.netlink.disconnect(self.index) {
            tracing::error!(index = self.index, ?err, "nbd disconnect on drop failed");
        }
    }
}

fn validate_geometry(geometry: &Geometry) -> Result<()> {
    let block_size = geometry.block_size;
    if !(512..=4096).contains(&block_size) || !block_size.is_power_of_two() {
        bail!("nbd block size must be a power of two in [512, 4096], got {block_size}");
    }
    if geometry.size_bytes == 0 || !geometry.size_bytes.is_multiple_of(u64::from(block_size)) {
        bail!(
            "nbd device size {} must be a non-zero multiple of the {block_size} byte block",
            geometry.size_bytes
        );
    }
    Ok(())
}

fn server_flags(geometry: &Geometry) -> u64 {
    let mut flags =
        NBD_FLAG_HAS_FLAGS | NBD_FLAG_SEND_FLUSH | NBD_FLAG_SEND_FUA | NBD_FLAG_CAN_MULTI_CONN;
    if geometry.read_only {
        flags |= NBD_FLAG_READ_ONLY;
    }
    if geometry.supports_discard {
        flags |= NBD_FLAG_SEND_TRIM;
    }
    flags
}

fn socket_pair() -> Result<(std::os::unix::net::UnixStream, OwnedFd)> {
    use nix::sys::socket::{socketpair, AddressFamily, SockFlag, SockType};
    let (ours, theirs) = socketpair(
        AddressFamily::Unix,
        SockType::Stream,
        None,
        SockFlag::SOCK_CLOEXEC,
    )?;
    Ok((std::os::unix::net::UnixStream::from(ours), theirs))
}

/// Spawn every connection worker and wait until each has initialized its
/// target; the returned kernel ends are what the netlink message hands over.
async fn spawn_all(
    factory: &ConnectionFactory,
    connections: u16,
    exits: &mpsc::UnboundedSender<ExitReport>,
) -> Result<(Vec<OwnedFd>, Vec<ConnectionHandle>)> {
    let mut kernel_ends = Vec::with_capacity(connections as usize);
    let mut handles = Vec::with_capacity(connections as usize);
    for ordinal in 0..connections {
        match factory(ordinal, 0, exits.clone()).await {
            Ok(spawned) => {
                kernel_ends.push(spawned.kernel_end);
                handles.push(spawned.handle);
            }
            Err(err) => {
                drop(kernel_ends);
                join_all(handles);
                return Err(err);
            }
        }
    }
    Ok((kernel_ends, handles))
}

fn join_all(handles: Vec<ConnectionHandle>) {
    // Every worker is signalled before any is waited on, so they wind down in
    // parallel rather than one timeout at a time.
    let mut handles = handles;
    for handle in &mut handles {
        handle.shutdown.take();
    }
    for handle in handles {
        handle.join();
    }
}

/// Bring up one connection: a socketpair, a worker thread over the target, and
/// the wait until that worker has initialized it.
async fn spawn_connection<T: NbdTarget>(
    target: Arc<T>,
    ordinal: u16,
    generation: u64,
    queue_depth: usize,
    exits: mpsc::UnboundedSender<ExitReport>,
) -> Result<SpawnedConnection> {
    let (ours, theirs) = socket_pair().context("create the nbd connection socketpair")?;
    let (ready_tx, ready_rx) = oneshot::channel();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let thread = thread::Builder::new()
        .name(format!("nbd-c{ordinal}"))
        .spawn(move || {
            let exit = connection_work(target, ours, ordinal, queue_depth, ready_tx, shutdown_rx);
            let _ = exits.send((ordinal, generation, exit));
        })
        .context("spawn an nbd connection worker thread")?;
    let handle = ConnectionHandle {
        thread,
        shutdown: Some(shutdown_tx),
    };

    match ready_rx.await {
        Ok(Ok(())) => Ok(SpawnedConnection {
            kernel_end: theirs,
            handle,
        }),
        Ok(Err(err)) => {
            handle.join();
            Err(err)
        }
        Err(_) => {
            handle.join();
            bail!("nbd connection {ordinal} worker exited before it was ready")
        }
    }
}

/// Replaces a connection the kernel took down, so a single timed-out request
/// does not leave the device answering EIO for good.
///
/// What a guest sees: a request the kernel gave up on fails with EIO, a
/// request submitted while the connection is being replaced waits (bounded by
/// `dead_conn_timeout`), a request the kernel can still retry is held until the
/// target answers, and everything issued after the replacement is served.
struct Supervisor {
    index: u32,
    factory: ConnectionFactory,
    slots: Vec<Slot>,
    exits_tx: mpsc::UnboundedSender<ExitReport>,
    exits: mpsc::UnboundedReceiver<ExitReport>,
    commands: mpsc::UnboundedReceiver<SupervisorCommand>,
    netlink: NbdNetlink,
    backend_identifier: Option<String>,
    io_timeout: Duration,
    dead_conn_timeout: Option<Duration>,
}

struct Slot {
    generation: u64,
    handle: Option<ConnectionHandle>,
}

fn spawn_supervisor(
    index: u32,
    factory: ConnectionFactory,
    handles: Vec<ConnectionHandle>,
    exits_tx: mpsc::UnboundedSender<ExitReport>,
    exits: mpsc::UnboundedReceiver<ExitReport>,
    opts: &NbdOptions,
) -> Result<SupervisorHandle> {
    let (commands_tx, commands_rx) = mpsc::unbounded_channel();
    let supervisor = Supervisor {
        index,
        factory,
        slots: handles
            .into_iter()
            .map(|handle| Slot {
                generation: 0,
                handle: Some(handle),
            })
            .collect(),
        exits_tx,
        exits,
        commands: commands_rx,
        netlink: NbdNetlink::open().context("open the supervisor's netlink socket")?,
        backend_identifier: opts.backend_identifier.clone(),
        io_timeout: opts.io_timeout,
        dead_conn_timeout: opts.dead_conn_timeout,
    };
    let (stand_down_tx, stand_down_rx) = watch::channel(false);
    let (finished_tx, finished_rx) = oneshot::channel();
    tokio::spawn(async move {
        supervisor.run(stand_down_rx).await;
        let _ = finished_tx.send(());
    });
    Ok(SupervisorHandle {
        _stand_down: stand_down_tx,
        commands: commands_tx,
        finished: finished_rx,
    })
}

impl Supervisor {
    async fn run(mut self, mut stand_down: watch::Receiver<bool>) {
        loop {
            tokio::select! {
                _ = stand_down.changed() => break,
                command = self.commands.recv() => {
                    let Some(SupervisorCommand::DropConnection(ordinal)) = command else {
                        break;
                    };
                    self.drop_connection(ordinal, &mut stand_down).await;
                }
                report = self.exits.recv() => {
                    let Some((ordinal, generation, exit)) = report else {
                        break;
                    };
                    if self.observe(ordinal, generation, exit, &mut stand_down).await {
                        break;
                    }
                }
            }
        }
        let handles = self
            .slots
            .iter_mut()
            .filter_map(|slot| slot.handle.take())
            .collect();
        join_all(handles);
    }

    /// Returns whether the supervisor should stop watching.
    async fn observe(
        &mut self,
        ordinal: u16,
        generation: u64,
        exit: ConnectionExit,
        stand_down: &mut watch::Receiver<bool>,
    ) -> bool {
        let index = self.index;
        let Some(slot) = self.slots.get_mut(ordinal as usize) else {
            return false;
        };
        if slot.generation != generation {
            // A connection this supervisor already gave up on; its successor is
            // the one in the slot.
            return false;
        }
        if let Some(handle) = slot.handle.take() {
            handle.join();
        }
        match exit {
            ConnectionExit::ShutdownRequested => false,
            ConnectionExit::Disconnected => {
                tracing::info!(index, ordinal, "nbd connection closed by the kernel");
                true
            }
            ConnectionExit::Lost => {
                tracing::warn!(
                    index,
                    ordinal,
                    "nbd connection died; replacing it so the device keeps serving"
                );
                self.replace(ordinal, stand_down).await;
                false
            }
        }
    }

    /// Closes one connection and rebuilds it, the way a worker that died is
    /// rebuilt. The old worker's own exit carries the superseded generation, so
    /// it is ignored when it arrives.
    async fn drop_connection(&mut self, ordinal: u16, stand_down: &mut watch::Receiver<bool>) {
        let Some(slot) = self.slots.get_mut(ordinal as usize) else {
            return;
        };
        if let Some(handle) = slot.handle.take() {
            handle.join();
        }
        tracing::warn!(
            index = self.index,
            ordinal,
            "nbd connection dropped on request; replacing it"
        );
        self.replace(ordinal, stand_down).await;
    }

    async fn replace(&mut self, ordinal: u16, stand_down: &mut watch::Receiver<bool>) {
        let index = self.index;
        let generation = self.slots[ordinal as usize].generation + 1;
        self.slots[ordinal as usize].generation = generation;

        for (attempt, backoff) in REPLACEMENT_BACKOFF.iter().enumerate() {
            if sleep_or_stand_down(*backoff, stand_down).await {
                return;
            }
            let spawned = match (self.factory)(ordinal, generation, self.exits_tx.clone()).await {
                Ok(spawned) => spawned,
                Err(err) => {
                    tracing::warn!(
                        index,
                        ordinal,
                        attempt,
                        err = format_args!("{err:#}"),
                        "could not build a replacement nbd connection"
                    );
                    continue;
                }
            };
            // The kernel takes a socket only into a slot it has already marked
            // dead, and drops the surplus without saying so.
            if sleep_or_stand_down(REATTACH_SETTLE, stand_down).await {
                join_all(vec![spawned.handle]);
                return;
            }
            let spec = ReconfigureSpec {
                timeout: Some(self.io_timeout),
                dead_conn_timeout: self.dead_conn_timeout,
                sockets: vec![spawned.kernel_end.as_raw_fd()],
                backend_identifier: self.backend_identifier.clone(),
                ..Default::default()
            };
            match self.netlink.reconfigure(index, &spec) {
                Ok(()) => {
                    drop(spawned.kernel_end);
                    self.slots[ordinal as usize].handle = Some(spawned.handle);
                    tracing::info!(index, ordinal, attempt, "nbd connection replaced");
                    return;
                }
                Err(err) => {
                    tracing::warn!(
                        index,
                        ordinal,
                        attempt,
                        err = format_args!("{err:#}"),
                        "the kernel refused a replacement nbd connection"
                    );
                    drop(spawned.kernel_end);
                    join_all(vec![spawned.handle]);
                }
            }
        }
        tracing::error!(
            index,
            ordinal,
            attempts = REPLACEMENT_BACKOFF.len(),
            "gave up replacing an nbd connection; the device answers EIO on this slot until \
             it is reattached"
        );
    }
}

/// Returns whether the wait ended because the supervisor was told to stop.
async fn sleep_or_stand_down(wait: Duration, stand_down: &mut watch::Receiver<bool>) -> bool {
    if wait.is_zero() {
        return false;
    }
    tokio::select! {
        _ = stand_down.changed() => true,
        _ = tokio::time::sleep(wait) => false,
    }
}

fn connection_work<T: NbdTarget>(
    target: Arc<T>,
    sock: std::os::unix::net::UnixStream,
    conn: u16,
    queue_depth: usize,
    ready: oneshot::Sender<Result<()>>,
    shutdown: oneshot::Receiver<()>,
) -> ConnectionExit {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .on_thread_park(move || {
            NBD_CONN_URING.with(|uring| {
                if let Some(uring) = uring.get() {
                    let _ = uring
                        .borrow()
                        .submit()
                        .with_context(|| format!("nbd connection {conn} submit for io uring"))
                        .inspect_err(|err| tracing::error!(?err, "on_thread_park callback failed"));
                }
            })
        })
        .build()
        .context("build the tokio runtime for an nbd connection worker");
    let runtime = match runtime {
        Ok(runtime) => runtime,
        Err(err) => {
            let _ = ready.send(Err(err));
            return ConnectionExit::ShutdownRequested;
        }
    };

    let local_set = tokio::task::LocalSet::new();
    local_set.block_on(&runtime, async move {
        let prepared = async {
            let ring = AsyncIoRingBuilder::new()
                .nr_sparse_buffer(queue_depth * 2)
                .nr_sparse_file(16)
                .sqe_entries(queue_depth * 2)
                .cqe_entries(queue_depth * 4)
                .build()
                .with_context(|| format!("create io uring for nbd connection {conn}"))?;
            tokio::task::spawn_local({
                let ring = ring.clone();
                async move {
                    if let Err(err) = ring.handle_completion().await {
                        panic!("nbd connection uring handle_completion exited: {err:?}");
                    }
                }
            });
            if NBD_CONN_URING
                .with(|uring| uring.set(ring.clone()))
                .is_err()
            {
                bail!("the uring for nbd connection {conn} was already initialized");
            }
            sock.set_nonblocking(true)
                .context("put the nbd connection socket in non-blocking mode")?;
            let sock = tokio::net::UnixStream::from_std(sock)
                .context("adopt the nbd connection socket")?;
            target
                .init(conn, &ring)
                .await
                .with_context(|| format!("init the target for nbd connection {conn}"))?;
            Ok((ring, sock))
        }
        .await;

        let (ring, sock) = match prepared {
            Ok(prepared) => prepared,
            Err(err) => {
                let _ = ready.send(Err(err));
                // Never served, so there is no connection to replace.
                return ConnectionExit::ShutdownRequested;
            }
        };
        let _ = ready.send(Ok(()));
        serve(target, ring, sock, conn, queue_depth, shutdown).await
    })
}

async fn serve<T: NbdTarget>(
    target: Arc<T>,
    ring: AsyncIoRing,
    sock: tokio::net::UnixStream,
    conn: u16,
    queue_depth: usize,
    mut shutdown: oneshot::Receiver<()>,
) -> ConnectionExit {
    let (mut reader, writer) = sock.into_split();
    let writer = Rc::new(Mutex::new(writer));
    let permits = Arc::new(Semaphore::new(queue_depth));
    let mut inflight = JoinSet::new();
    let mut header = [0u8; REQUEST_LEN];

    let exit = loop {
        while inflight.try_join_next().is_some() {}
        // Cancelling a partially consumed header is safe: the loop only ever
        // leaves through a teardown.
        let read = tokio::select! {
            read = reader.read_exact(&mut header) => read,
            _ = &mut shutdown => {
                tracing::debug!(conn, "nbd connection worker asked to stop");
                break ConnectionExit::ShutdownRequested;
            }
        };
        match read {
            Ok(_) => {}
            Err(err) if err.kind() == ErrorKind::UnexpectedEof => break ConnectionExit::Lost,
            Err(err) => {
                tracing::error!(conn, ?err, "nbd connection read failed");
                break ConnectionExit::Lost;
            }
        }
        let request = match NbdRequest::decode(&header) {
            Ok(request) => request,
            Err(err) => {
                tracing::error!(conn, err = format_args!("{err:#}"), "nbd request refused");
                break ConnectionExit::Lost;
            }
        };
        if request.kind() == Some(NbdCommand::Disc) {
            tracing::debug!(conn, "nbd connection received a disconnect");
            break ConnectionExit::Disconnected;
        }
        if request.len > MAX_REQUEST_LEN {
            tracing::error!(
                conn,
                len = request.len,
                "nbd request length exceeds the kernel's maximum; closing the connection"
            );
            break ConnectionExit::Lost;
        }
        let payload = if request.kind() == Some(NbdCommand::Write) {
            let mut payload = vec![0u8; request.len as usize];
            if let Err(err) = reader.read_exact(&mut payload).await {
                tracing::error!(conn, ?err, "nbd write payload read failed");
                break ConnectionExit::Lost;
            }
            payload
        } else {
            Vec::new()
        };
        let Ok(permit) = permits.clone().acquire_owned().await else {
            break ConnectionExit::Lost;
        };
        inflight.spawn_local(handle_request(
            target.clone(),
            ring.clone(),
            writer.clone(),
            request,
            payload,
            conn,
            permit,
        ));
    };

    // The connection is going away, so a reply has nowhere to land: abort the
    // in-flight requests rather than wait for a target that may never answer.
    inflight.abort_all();
    while inflight.join_next().await.is_some() {}
    tracing::debug!(conn, ?exit, "nbd connection worker finished");
    exit
}

async fn handle_request<T: NbdTarget>(
    target: Arc<T>,
    ring: AsyncIoRing,
    writer: Rc<Mutex<OwnedWriteHalf>>,
    request: NbdRequest,
    payload: Vec<u8>,
    conn: u16,
    _permit: OwnedSemaphorePermit,
) {
    let (error, data) = dispatch(&target, &ring, &request, &payload).await;
    let reply = NbdReply {
        error,
        handle: request.handle,
    }
    .encode();
    let mut writer = writer.lock().await;
    if let Err(err) = writer.write_all(&reply).await {
        tracing::error!(conn, ?err, "nbd reply header write failed");
        return;
    }
    if error == 0 && !data.is_empty() {
        if let Err(err) = writer.write_all(&data).await {
            tracing::error!(conn, ?err, "nbd reply payload write failed");
        }
    }
}

async fn dispatch<T: NbdTarget>(
    target: &Arc<T>,
    ring: &AsyncIoRing,
    request: &NbdRequest,
    payload: &[u8],
) -> (u32, Vec<u8>) {
    let Some(command) = request.kind() else {
        tracing::error!(command = request.command, "unknown nbd command");
        return (libc::EINVAL as u32, Vec::new());
    };
    let geometry = target.geometry();
    let len = u64::from(request.len);
    if matches!(
        command,
        NbdCommand::Read | NbdCommand::Write | NbdCommand::Trim
    ) {
        let end = request.from.checked_add(len);
        if end.is_none_or(|end| end > geometry.size_bytes) {
            tracing::error!(
                offset = request.from,
                len,
                size_bytes = geometry.size_bytes,
                "nbd request runs past the end of the device"
            );
            return (libc::EINVAL as u32, Vec::new());
        }
    }
    if geometry.read_only && matches!(command, NbdCommand::Write | NbdCommand::Trim) {
        return (libc::EPERM as u32, Vec::new());
    }

    match command {
        NbdCommand::Read => {
            let mut buf = vec![0u8; request.len as usize];
            let result = target.read(ring, request.from, &mut buf).await;
            if result == 0 {
                (0, buf)
            } else {
                (reply_error(result), Vec::new())
            }
        }
        NbdCommand::Write => {
            let result = target
                .write(ring, request.from, payload, request.fua())
                .await;
            (reply_error(result), Vec::new())
        }
        NbdCommand::Flush => (reply_error(target.flush(ring).await), Vec::new()),
        NbdCommand::Trim => (
            reply_error(target.discard(ring, request.from, len).await),
            Vec::new(),
        ),
        NbdCommand::Disc => (0, Vec::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn geometry(size_bytes: u64, block_size: u32) -> Geometry {
        Geometry {
            size_bytes,
            block_size,
            read_only: false,
            supports_discard: true,
        }
    }

    #[test]
    fn a_writable_discardable_device_advertises_flush_fua_trim_and_multi_conn() {
        let flags = server_flags(&geometry(1 << 20, 4096));
        assert_eq!(
            flags,
            NBD_FLAG_HAS_FLAGS
                | NBD_FLAG_SEND_FLUSH
                | NBD_FLAG_SEND_FUA
                | NBD_FLAG_CAN_MULTI_CONN
                | NBD_FLAG_SEND_TRIM
        );
    }

    #[test]
    fn a_read_only_target_adds_the_read_only_flag_and_drops_trim() {
        let mut geometry = geometry(1 << 20, 4096);
        geometry.read_only = true;
        geometry.supports_discard = false;
        let flags = server_flags(&geometry);
        assert_eq!(flags & NBD_FLAG_READ_ONLY, NBD_FLAG_READ_ONLY);
        assert_eq!(flags & NBD_FLAG_SEND_TRIM, 0);
    }

    #[test]
    fn a_block_size_the_kernel_refuses_is_rejected_before_connect() {
        assert!(validate_geometry(&geometry(1 << 20, 4096)).is_ok());
        assert!(validate_geometry(&geometry(1 << 20, 512)).is_ok());
        assert!(validate_geometry(&geometry(1 << 20, 256)).is_err());
        assert!(validate_geometry(&geometry(1 << 20, 8192)).is_err());
        assert!(validate_geometry(&geometry(1 << 20, 768)).is_err());
    }

    #[test]
    fn a_size_that_is_not_a_whole_number_of_blocks_is_rejected_before_connect() {
        assert!(validate_geometry(&geometry(4096 + 512, 4096)).is_err());
        assert!(validate_geometry(&geometry(0, 4096)).is_err());
    }

    #[test]
    fn a_replacement_fits_inside_the_default_reconnect_window() {
        let attempts: Duration = REPLACEMENT_BACKOFF.iter().sum::<Duration>()
            + REATTACH_SETTLE * REPLACEMENT_BACKOFF.len() as u32;
        let window = NbdOptions::default()
            .dead_conn_timeout
            .expect("a default reconnect window");
        assert!(
            attempts < window,
            "every replacement attempt together takes {attempts:?}, which does not fit the \
             {window:?} a request waits before it fails"
        );
    }

    #[test]
    fn the_default_options_match_the_kernel_defaults_this_crate_assumes() {
        let options = NbdOptions::default();
        assert_eq!(options.connections, 4);
        assert_eq!(options.io_timeout, Duration::from_secs(90));
        assert_eq!(options.queue_depth, 64);
        assert_eq!(
            options.dead_conn_timeout,
            Some(Duration::from_secs(30)),
            "without a reconnect window there is nothing for the supervisor to replace into"
        );
        assert!(
            !options.destroy_on_disconnect,
            "destroying the gendisk drains the module's preallocated devices"
        );
    }
}
