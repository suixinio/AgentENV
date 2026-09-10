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
use tokio::sync::{oneshot, Mutex, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;

use crate::netlink::{ConnectSpec, NbdNetlink, ReconfigureSpec};
use crate::proto::{
    reply_error, NbdCommand, NbdReply, NbdRequest, NBD_CFLAG_DESTROY_ON_DISCONNECT,
    NBD_FLAG_CAN_MULTI_CONN, NBD_FLAG_HAS_FLAGS, NBD_FLAG_READ_ONLY, NBD_FLAG_SEND_FLUSH,
    NBD_FLAG_SEND_FUA, NBD_FLAG_SEND_TRIM, REQUEST_LEN,
};
use crate::target::{Geometry, NbdTarget};
use crate::{device_size_bytes, wait_for_nbd_dev};

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
    pub io_timeout: Duration,
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
            dead_conn_timeout: None,
            queue_depth: 64,
            backend_identifier: None,
            destroy_on_disconnect: false,
        }
    }
}

struct ConnectionHandle {
    thread: thread::JoinHandle<()>,
    finished: oneshot::Receiver<()>,
}

/// A live `/dev/nbdN` served by this process.
pub struct NbdDevice {
    netlink: NbdNetlink,
    index: u32,
    device_path: PathBuf,
    backend_identifier: Option<String>,
    connections: Vec<ConnectionHandle>,
    stopped: bool,
}

impl NbdDevice {
    /// Brings the target up as a new `/dev/nbdN` and returns once the device
    /// node is open-able and reports the target's size.
    pub async fn start<T: NbdTarget>(target: Arc<T>, opts: NbdOptions) -> Result<Self> {
        let geometry = target.geometry();
        validate_geometry(&geometry)?;
        let connections = opts.connections.max(1);
        let queue_depth = opts.queue_depth.max(1);

        let netlink = NbdNetlink::open()?;
        let mut kernel_ends: Vec<OwnedFd> = Vec::with_capacity(connections as usize);
        let mut handles: Vec<ConnectionHandle> = Vec::with_capacity(connections as usize);
        let mut ready = Vec::with_capacity(connections as usize);
        for conn in 0..connections {
            let (ours, theirs) = socket_pair().context("create the nbd connection socketpair")?;
            let (ready_tx, ready_rx) = oneshot::channel();
            let (finished_tx, finished_rx) = oneshot::channel();
            let spawned = thread::Builder::new()
                .name(format!("nbd-c{conn}"))
                .spawn({
                    let target = target.clone();
                    move || {
                        connection_work(target, ours, conn, queue_depth, ready_tx);
                        let _ = finished_tx.send(());
                    }
                })
                .context("spawn an nbd connection worker thread");
            match spawned {
                Ok(thread) => {
                    kernel_ends.push(theirs);
                    handles.push(ConnectionHandle {
                        thread,
                        finished: finished_rx,
                    });
                    ready.push(ready_rx);
                }
                Err(err) => {
                    drop(kernel_ends);
                    join_connections(handles).await;
                    return Err(err);
                }
            }
        }

        for (conn, rx) in ready.into_iter().enumerate() {
            let outcome = rx
                .await
                .with_context(|| format!("nbd connection {conn} worker exited before it was ready"))
                .and_then(|result| result);
            if let Err(err) = outcome {
                drop(kernel_ends);
                join_connections(handles).await;
                return Err(err);
            }
        }

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
                join_connections(handles).await;
                return Err(err);
            }
        };
        // The kernel holds its own reference to each socket now.
        drop(kernel_ends);

        let device = Self {
            netlink,
            index,
            device_path: PathBuf::from(format!("/dev/nbd{index}")),
            backend_identifier: opts.backend_identifier,
            connections: handles,
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
        if let Err(err) = self.netlink.disconnect(index) {
            match err.root_cause().downcast_ref::<std::io::Error>() {
                Some(io_err)
                    if matches!(io_err.raw_os_error(), Some(libc::ENOENT | libc::EINVAL)) =>
                {
                    tracing::info!(index, "nbd device was already gone at disconnect");
                }
                _ => {
                    let connections = std::mem::take(&mut self.connections);
                    join_connections(connections).await;
                    return Err(err);
                }
            }
        }
        let connections = std::mem::take(&mut self.connections);
        join_connections(connections).await;
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

async fn join_connections(connections: Vec<ConnectionHandle>) {
    for connection in connections {
        let _ = connection.finished.await;
        if connection.thread.join().is_err() {
            tracing::error!("an nbd connection worker thread panicked");
        }
    }
}

fn connection_work<T: NbdTarget>(
    target: Arc<T>,
    sock: std::os::unix::net::UnixStream,
    conn: u16,
    queue_depth: usize,
    ready: oneshot::Sender<Result<()>>,
) {
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
            return;
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
                return;
            }
        };
        let _ = ready.send(Ok(()));
        serve(target, ring, sock, conn, queue_depth).await;
    });
}

async fn serve<T: NbdTarget>(
    target: Arc<T>,
    ring: AsyncIoRing,
    sock: tokio::net::UnixStream,
    conn: u16,
    queue_depth: usize,
) {
    let (mut reader, writer) = sock.into_split();
    let writer = Rc::new(Mutex::new(writer));
    let permits = Arc::new(Semaphore::new(queue_depth));
    let mut inflight = JoinSet::new();
    let mut header = [0u8; REQUEST_LEN];

    loop {
        while inflight.try_join_next().is_some() {}
        match reader.read_exact(&mut header).await {
            Ok(_) => {}
            Err(err) if err.kind() == ErrorKind::UnexpectedEof => break,
            Err(err) => {
                tracing::error!(conn, ?err, "nbd connection read failed");
                break;
            }
        }
        let request = match NbdRequest::decode(&header) {
            Ok(request) => request,
            Err(err) => {
                tracing::error!(conn, err = format_args!("{err:#}"), "nbd request refused");
                break;
            }
        };
        if request.kind() == Some(NbdCommand::Disc) {
            tracing::debug!(conn, "nbd connection received a disconnect");
            break;
        }
        if request.len > MAX_REQUEST_LEN {
            tracing::error!(
                conn,
                len = request.len,
                "nbd request length exceeds the kernel's maximum; closing the connection"
            );
            break;
        }
        let payload = if request.kind() == Some(NbdCommand::Write) {
            let mut payload = vec![0u8; request.len as usize];
            if let Err(err) = reader.read_exact(&mut payload).await {
                tracing::error!(conn, ?err, "nbd write payload read failed");
                break;
            }
            payload
        } else {
            Vec::new()
        };
        let Ok(permit) = permits.clone().acquire_owned().await else {
            break;
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
    }

    while inflight.join_next().await.is_some() {}
    tracing::debug!(conn, "nbd connection worker finished");
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
    fn the_default_options_match_the_kernel_defaults_this_crate_assumes() {
        let options = NbdOptions::default();
        assert_eq!(options.connections, 4);
        assert_eq!(options.io_timeout, Duration::from_secs(90));
        assert_eq!(options.queue_depth, 64);
        assert!(options.dead_conn_timeout.is_none());
        assert!(
            !options.destroy_on_disconnect,
            "destroying the gendisk drains the module's preallocated devices"
        );
    }
}
