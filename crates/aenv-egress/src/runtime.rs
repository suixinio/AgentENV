use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::dispatch::{Dispatcher, Reject};
use crate::framing::{self, FramingError};
use crate::handler::HandlerError;
use crate::header::{Ack, IdentityHeader};
use crate::transport::AsyncStream;

/// The deadline for the identity frame: nothing on the connection is admitted
/// before it arrives.
pub const DEFAULT_ADMISSION_TIMEOUT: Duration = Duration::from_secs(10);
/// Connections the listener holds at once; the excess is closed, not queued.
pub const DEFAULT_MAX_CONNECTIONS: u32 = 4096;
/// Connections one sandbox holds at once, inside the node-wide limit.
pub const DEFAULT_PER_SANDBOX_CONNECTIONS: u32 = 256;
/// How long a shutdown lets live sessions finish, inside a 30s pod grace.
pub const DEFAULT_SHUTDOWN_DRAIN: Duration = Duration::from_secs(25);

/// How much unauthenticated work the broker admits, and whose connections it
/// admits at all: `expected_peer_uid` is the uid of the node process on this
/// machine. `None` accepts any peer, which is only right in tests.
pub struct Options {
    pub admission_timeout: Duration,
    pub max_connections: u32,
    pub per_sandbox_connections: u32,
    pub shutdown_drain: Duration,
    pub expected_peer_uid: Option<u32>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            admission_timeout: DEFAULT_ADMISSION_TIMEOUT,
            max_connections: DEFAULT_MAX_CONNECTIONS,
            per_sandbox_connections: DEFAULT_PER_SANDBOX_CONNECTIONS,
            shutdown_drain: DEFAULT_SHUTDOWN_DRAIN,
            expected_peer_uid: None,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error(transparent)]
    Framing(#[from] FramingError),
    /// The header was refused and the peer told why.
    #[error("identity header refused: {0}")]
    Refused(&'static str),
    #[error(transparent)]
    Handler(#[from] HandlerError),
}

impl RuntimeError {
    /// Stable label for metrics; the exact peer wire reason when refused.
    pub fn outcome(&self) -> &'static str {
        match self {
            Self::Framing(_) => "framing_error",
            Self::Refused(reason) => reason,
            Self::Handler(_) => "handler_error",
        }
    }
}

/// Wire reason for a header whose version this broker does not speak.
pub const UNSUPPORTED_VERSION: &str = crate::header::UNSUPPORTED_VERSION_REASON;
/// Wire reason for a sandbox that already holds its share of connections.
pub const PER_SANDBOX_FULL: &str = "per_sandbox_full";

/// The broker side of one transport: reads the identity, applies the
/// per-sandbox limit, acknowledges and dispatches every incoming stream.
pub struct Runtime {
    options: Options,
    dispatcher: Arc<Dispatcher>,
    per_sandbox: Arc<Mutex<HashMap<String, u32>>>,
}

/// Holds one sandbox's connection slot for the life of its session.
struct SandboxSlot {
    counts: Arc<Mutex<HashMap<String, u32>>>,
    sandbox_id: String,
}

impl Drop for SandboxSlot {
    fn drop(&mut self) {
        let mut counts = self
            .counts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(count) = counts.get_mut(&self.sandbox_id) {
            *count -= 1;
            if *count == 0 {
                counts.remove(&self.sandbox_id);
            }
        }
    }
}

impl Runtime {
    pub fn new(options: Options, dispatcher: Arc<Dispatcher>) -> Self {
        Self {
            options,
            dispatcher,
            per_sandbox: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn dispatcher(&self) -> &Arc<Dispatcher> {
        &self.dispatcher
    }

    /// Reads the header frame, admits it, writes the [`Ack`] and, when
    /// accepted, runs the handler to completion on the rest of the stream.
    pub async fn dispatch(&self, stream: Box<dyn AsyncStream>) -> Result<(), RuntimeError> {
        let admitted = self.admit_stream(stream).await?;
        self.serve(admitted).await
    }

    async fn admit_stream(
        &self,
        mut stream: Box<dyn AsyncStream>,
    ) -> Result<Admitted, RuntimeError> {
        let header: IdentityHeader = framing::read_json(&mut stream).await?;
        let (handler, slot) = match self.admit(&header) {
            Ok(admitted) => admitted,
            Err(reason) => {
                framing::write_json(&mut stream, &Ack::rejected(reason)).await?;
                return Err(RuntimeError::Refused(reason));
            }
        };
        framing::write_json(&mut stream, &Ack::accepted()).await?;
        Ok(Admitted {
            handler,
            header,
            stream,
            _slot: slot,
        })
    }

    async fn serve(&self, admitted: Admitted) -> Result<(), RuntimeError> {
        self.dispatcher
            .serve(admitted.handler, &admitted.header, admitted.stream)
            .await?;
        Ok(())
    }

    fn admit(
        &self,
        header: &IdentityHeader,
    ) -> Result<(Arc<dyn crate::handler::Handler>, SandboxSlot), &'static str> {
        if header.v != crate::header::IDENTITY_HEADER_VERSION {
            return Err(UNSUPPORTED_VERSION);
        }
        let handler = self
            .dispatcher
            .accept(header)
            .map_err(|reject: Reject| reject.reason())?;
        let slot = self.claim_slot(&header.sandbox_id)?;
        Ok((handler, slot))
    }

    fn claim_slot(&self, sandbox_id: &str) -> Result<SandboxSlot, &'static str> {
        let mut counts = self
            .per_sandbox
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let count = counts.entry(sandbox_id.to_string()).or_insert(0);
        if *count >= self.options.per_sandbox_connections {
            if *count == 0 {
                counts.remove(sandbox_id);
            }
            return Err(PER_SANDBOX_FULL);
        }
        *count += 1;
        Ok(SandboxSlot {
            counts: Arc::clone(&self.per_sandbox),
            sandbox_id: sandbox_id.to_string(),
        })
    }
}

/// A stream past admission, with the handler that will serve it.
struct Admitted {
    handler: Arc<dyn crate::handler::Handler>,
    header: IdentityHeader,
    stream: Box<dyn AsyncStream>,
    _slot: SandboxSlot,
}

/// Accepts connections on the node-local Unix socket and dispatches each on
/// its own task until `shutdown` resolves, then stops accepting and lets the
/// live sessions finish. Every connection's outcome is counted.
#[cfg(feature = "local")]
pub async fn run(
    runtime: Arc<Runtime>,
    listener: tokio::net::UnixListener,
    shutdown: impl std::future::Future<Output = ()>,
) -> anyhow::Result<()> {
    let max_connections = runtime.options.max_connections;
    let admission = runtime.options.admission_timeout;
    let expected_peer_uid = runtime.options.expected_peer_uid;
    let permits = Arc::new(tokio::sync::Semaphore::new(max_connections as usize));
    let mut shutdown = std::pin::pin!(shutdown);
    loop {
        let stream = tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => stream,
                Err(err) => {
                    tracing::warn!(error = %err, "accept failed");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
            },
            _ = &mut shutdown => break,
        };
        if let Some(expected) = expected_peer_uid {
            if !peer_uid_matches(&stream, expected) {
                metrics::counter!("egress_peer_rejected_total").increment(1);
                drop(stream);
                continue;
            }
        }
        let Ok(permit) = Arc::clone(&permits).try_acquire_owned() else {
            tracing::debug!(max_connections, "closing: no connection slot is free");
            metrics::counter!("egress_conns_total", "handler" => "none", "outcome" => "admission_full").increment(1);
            drop(stream);
            continue;
        };
        let runtime = Arc::clone(&runtime);
        tokio::spawn(async move {
            let _permit = permit;
            metrics::gauge!("egress_active_conns").increment(1.0);
            let (handler, outcome) = admit_and_serve(&runtime, stream, admission).await;
            metrics::counter!("egress_conns_total", "handler" => handler, "outcome" => outcome)
                .increment(1);
            metrics::gauge!("egress_active_conns").decrement(1.0);
        });
    }
    drop(listener);
    drain(&permits, max_connections, runtime.options.shutdown_drain).await;
    Ok(())
}

/// Whether the connecting process runs as the uid this broker serves. A peer
/// whose credentials cannot be read is not that process.
#[cfg(feature = "local")]
fn peer_uid_matches(stream: &tokio::net::UnixStream, expected: u32) -> bool {
    match stream.peer_cred() {
        Ok(cred) if cred.uid() == expected => true,
        Ok(cred) => {
            tracing::warn!(
                uid = cred.uid(),
                expected,
                "closing: the peer is not the node process"
            );
            false
        }
        Err(err) => {
            tracing::warn!(error = %err, "closing: the peer's credentials are unreadable");
            false
        }
    }
}

/// The metric labels the connection ends with: the handler side, then the
/// outcome.
#[cfg(feature = "local")]
async fn admit_and_serve(
    runtime: &Runtime,
    stream: tokio::net::UnixStream,
    admission: Duration,
) -> (&'static str, &'static str) {
    let deadline = tokio::time::Instant::now() + admission;
    let admitted =
        match tokio::time::timeout_at(deadline, runtime.admit_stream(Box::new(stream))).await {
            Ok(Ok(admitted)) => admitted,
            Ok(Err(err)) => {
                let outcome = err.outcome();
                tracing::debug!(error = %err, "connection refused before any handler");
                return ("runtime", outcome);
            }
            Err(_) => {
                tracing::debug!("the identity header did not arrive inside the deadline");
                return ("none", "admission_timeout");
            }
        };
    match runtime.serve(admitted).await {
        Ok(()) => ("runtime", "ok"),
        Err(err) => {
            tracing::debug!(error = %err, "connection ended with an error");
            ("runtime", err.outcome())
        }
    }
}

/// Every permit back in hand means every session has ended.
#[cfg(feature = "local")]
async fn drain(permits: &tokio::sync::Semaphore, max_connections: u32, deadline: Duration) {
    if tokio::time::timeout(deadline, permits.acquire_many(max_connections))
        .await
        .is_err()
    {
        tracing::warn!(
            open = max_connections as usize - permits.available_permits(),
            "the drain deadline passed with sessions still open"
        );
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

    use super::*;
    use crate::credential::NoCredentials;
    use crate::handlers::echo::{IdentityBanner, IdentityEchoHandler};
    use crate::header::test_support::sample_header;
    use crate::policy::{BrokerDenyList, UpstreamGuard};

    fn dispatcher() -> Arc<Dispatcher> {
        Arc::new(
            Dispatcher::new(
                Arc::new(NoCredentials),
                Arc::new(UpstreamGuard::new(BrokerDenyList::default())),
            )
            .with_handler(Arc::new(IdentityEchoHandler)),
        )
    }

    fn runtime() -> Arc<Runtime> {
        Arc::new(Runtime::new(Options::default(), dispatcher()))
    }

    fn current_header() -> IdentityHeader {
        IdentityHeader {
            issued_at_unix_ms: IdentityHeader::now_unix_ms(),
            ..sample_header()
        }
    }

    async fn open(
        runtime: &Arc<Runtime>,
        header: &IdentityHeader,
    ) -> (tokio::io::DuplexStream, Ack) {
        let (mut client, server) = tokio::io::duplex(64 * 1024);
        let rt = runtime.clone();
        tokio::spawn(async move {
            let _ = rt.dispatch(Box::new(server)).await;
        });
        framing::write_json(&mut client, header).await.unwrap();
        let ack: Ack = framing::read_json(&mut client).await.unwrap();
        (client, ack)
    }

    #[tokio::test]
    async fn a_valid_header_is_acknowledged_and_reaches_the_handler() {
        let runtime = runtime();
        let header = current_header();
        let (client, ack) = open(&runtime, &header).await;
        assert_eq!(ack, Ack::accepted());

        let mut client = BufReader::new(client);
        let mut line = String::new();
        client.read_line(&mut line).await.unwrap();
        let banner: IdentityBanner = serde_json::from_str(&line).unwrap();
        assert_eq!(banner.sandbox_id, header.sandbox_id);
        assert_eq!(banner.original_dst, header.original_dst);

        client.get_mut().write_all(b"hello").await.unwrap();
        client.get_mut().shutdown().await.unwrap();
        let mut echoed = Vec::new();
        client.read_to_end(&mut echoed).await.unwrap();
        assert_eq!(echoed, b"hello");
    }

    #[tokio::test]
    async fn a_header_of_another_version_is_refused_before_any_handler() {
        let runtime = runtime();
        for version in [1, 3, 0] {
            let header = IdentityHeader {
                v: version,
                ..current_header()
            };
            let (mut client, ack) = open(&runtime, &header).await;
            assert_eq!(ack, Ack::rejected(UNSUPPORTED_VERSION));
            let mut rest = Vec::new();
            client.read_to_end(&mut rest).await.unwrap();
            assert!(rest.is_empty());
        }
    }

    #[tokio::test]
    async fn an_unknown_handler_is_refused() {
        let runtime = runtime();
        let header = IdentityHeader {
            handler: "postgres".into(),
            ..current_header()
        };
        let (_client, ack) = open(&runtime, &header).await;
        assert_eq!(ack, Ack::rejected("unknown_handler"));
    }

    #[tokio::test]
    async fn one_sandbox_cannot_hold_more_than_its_share_of_connections() {
        let runtime = Arc::new(Runtime::new(
            Options {
                per_sandbox_connections: 2,
                ..Options::default()
            },
            dispatcher(),
        ));
        let header = current_header();

        let (first, ack) = open(&runtime, &header).await;
        assert_eq!(ack, Ack::accepted());
        let (second, ack) = open(&runtime, &header).await;
        assert_eq!(ack, Ack::accepted());
        let (_third, ack) = open(&runtime, &header).await;
        assert_eq!(ack, Ack::rejected(PER_SANDBOX_FULL));

        // Another sandbox keeps its own budget.
        let other = IdentityHeader {
            sandbox_id: "sbx-2".into(),
            ..current_header()
        };
        let (_other, ack) = open(&runtime, &other).await;
        assert_eq!(ack, Ack::accepted());

        drop(first);
        drop(second);
        // The slot returns when the session ends, not when the ack is written.
        for _ in 0..100 {
            let (_client, ack) = open(&runtime, &header).await;
            if ack == Ack::accepted() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("a closed session never returned its connection slot");
    }

    #[cfg(feature = "local")]
    mod listener {
        use super::*;
        use crate::transport::{BrokerTransport, LocalTransport};

        struct Broker {
            socket_path: std::path::PathBuf,
            _dir: tempfile::TempDir,
            shutdown: tokio::sync::oneshot::Sender<()>,
            joined: tokio::task::JoinHandle<anyhow::Result<()>>,
        }

        fn options() -> Options {
            Options {
                admission_timeout: Duration::from_secs(5),
                shutdown_drain: Duration::from_millis(500),
                ..Options::default()
            }
        }

        async fn start(options: Options) -> Broker {
            let dir = tempfile::tempdir().unwrap();
            let socket_path = dir.path().join("broker.sock");
            let listener = tokio::net::UnixListener::bind(&socket_path).unwrap();
            let (shutdown, rx) = tokio::sync::oneshot::channel();
            let joined = tokio::spawn(run(
                Arc::new(Runtime::new(options, dispatcher())),
                listener,
                async move {
                    let _ = rx.await;
                },
            ));
            Broker {
                socket_path,
                _dir: dir,
                shutdown,
                joined,
            }
        }

        /// This process's uid, which is what a peer on its own socket carries.
        fn own_uid() -> u32 {
            use std::os::unix::fs::MetadataExt as _;
            std::fs::metadata("/proc/self")
                .expect("procfs names this process")
                .uid()
        }

        fn transport(broker: &Broker) -> LocalTransport {
            LocalTransport::new(&broker.socket_path)
        }

        async fn expect_closed(stream: &mut tokio::net::UnixStream, what: &str) {
            let mut buf = [0u8; 1];
            let read = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf))
                .await
                .unwrap_or_else(|_| panic!("{what}"));
            match read {
                Ok(0) | Err(_) => {}
                Ok(n) => panic!("the listener answered {n} bytes instead of closing"),
            }
        }

        #[tokio::test]
        async fn a_peer_of_another_uid_is_closed_without_reading_its_header() {
            let broker = start(Options {
                expected_peer_uid: Some(own_uid() + 1),
                ..options()
            })
            .await;

            let mut refused = tokio::net::UnixStream::connect(&broker.socket_path)
                .await
                .unwrap();
            expect_closed(&mut refused, "the listener admitted a foreign peer").await;
        }

        #[tokio::test]
        async fn the_probe_answers_only_while_the_broker_reads() {
            let broker = start(options()).await;
            let transport = transport(&broker);

            transport
                .probe()
                .await
                .expect("a live broker reads the probe");

            broker.shutdown.send(()).unwrap();
            let _ = tokio::time::timeout(Duration::from_secs(10), broker.joined).await;
            transport
                .probe()
                .await
                .expect_err("a stopped broker binds nothing");
        }

        #[tokio::test]
        async fn the_node_uid_is_admitted() {
            let broker = start(Options {
                expected_peer_uid: Some(own_uid()),
                ..options()
            })
            .await;

            let _held = transport(&broker)
                .open(current_header())
                .await
                .expect("the broker admits its own node");
        }

        #[tokio::test]
        async fn a_connection_past_the_limit_is_closed_at_the_door() {
            let broker = start(Options {
                max_connections: 1,
                ..options()
            })
            .await;
            let _held = transport(&broker).open(current_header()).await.unwrap();

            let mut refused = tokio::net::UnixStream::connect(&broker.socket_path)
                .await
                .unwrap();
            expect_closed(
                &mut refused,
                "the listener admitted a connection past its limit",
            )
            .await;
        }

        #[tokio::test]
        async fn a_connection_that_sends_nothing_is_closed_when_the_deadline_passes() {
            let broker = start(Options {
                admission_timeout: Duration::from_millis(150),
                ..options()
            })
            .await;
            let mut idle = tokio::net::UnixStream::connect(&broker.socket_path)
                .await
                .unwrap();
            expect_closed(&mut idle, "an unadmitted connection was held open").await;
        }

        #[tokio::test]
        async fn a_live_session_holds_the_shutdown_until_it_ends() {
            let broker = start(Options {
                shutdown_drain: Duration::from_secs(10),
                ..options()
            })
            .await;
            let held = transport(&broker).open(current_header()).await.unwrap();
            broker.shutdown.send(()).unwrap();

            tokio::time::sleep(Duration::from_millis(250)).await;
            assert!(
                !broker.joined.is_finished(),
                "the shutdown dropped a live session"
            );

            drop(held);
            tokio::time::timeout(Duration::from_secs(10), broker.joined)
                .await
                .expect("the drain never finished")
                .unwrap()
                .unwrap();
        }

        #[tokio::test]
        async fn the_drain_stops_waiting_at_its_deadline() {
            let broker = start(Options {
                shutdown_drain: Duration::from_millis(200),
                ..options()
            })
            .await;
            let _held = transport(&broker).open(current_header()).await.unwrap();
            broker.shutdown.send(()).unwrap();

            tokio::time::timeout(Duration::from_secs(10), broker.joined)
                .await
                .expect("the drain ignored its deadline")
                .unwrap()
                .unwrap();
        }
    }

    #[tokio::test]
    async fn an_oversized_first_frame_is_a_framing_error() {
        let runtime = runtime();
        let (mut client, server) = tokio::io::duplex(1024);
        let task = tokio::spawn(async move { runtime.dispatch(Box::new(server)).await });
        client
            .write_all(&(framing::MAX_FRAME_LEN as u32 + 1).to_le_bytes())
            .await
            .unwrap();
        let err = task.await.unwrap().err().unwrap();
        assert!(matches!(
            err,
            RuntimeError::Framing(FramingError::TooLarge(_))
        ));
        assert_eq!(err.outcome(), "framing_error");
    }
}
