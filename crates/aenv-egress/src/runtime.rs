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
/// Wire reason, and metric outcome, for a session that arrived with no
/// connection slot free. The same string on both sides on purpose: the label
/// existed before the reason did, and a renamed label is a new series.
pub const ADMISSION_FULL: &str = "admission_full";
/// Metric outcome for a connection that was only a readiness probe.
pub const PROBE: &str = "probe";
/// Metric outcome for a connection whose peer is not the node. Never written
/// to the wire: such a peer is closed unanswered.
#[cfg(feature = "local")]
const PEER_REJECTED: &str = "peer_rejected";
/// What a readiness probe reads back. Its value carries nothing; that it
/// arrives at all is the whole answer.
pub const PROBE_ACK: &[u8] = b"\0";

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

    /// Reads the first frame and answers whichever of the two things it is:
    /// an empty frame is a readiness probe, anything else an identity header
    /// whose session runs to completion on the rest of the stream.
    pub async fn dispatch(&self, mut stream: Box<dyn AsyncStream>) -> Result<(), RuntimeError> {
        let frame = framing::read_frame(&mut stream).await?;
        if frame.is_empty() {
            answer_probe(&mut stream).await?;
            return Err(RuntimeError::Refused(PROBE));
        }
        let admitted = self.admit_frame(&frame, stream).await?;
        self.serve(admitted).await
    }

    /// Parses an identity frame, applies the version, handler and per-sandbox
    /// checks and acknowledges. Past this the stream is a session.
    async fn admit_frame(
        &self,
        frame: &[u8],
        mut stream: Box<dyn AsyncStream>,
    ) -> Result<Admitted, RuntimeError> {
        let header: IdentityHeader = serde_json::from_slice(frame).map_err(FramingError::from)?;
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

/// One byte back, so the prober learns that something read its frame rather
/// than that something was listening. It takes no permit and opens no
/// session, which is why it is answered before anything else is asked of the
/// connection.
async fn answer_probe<S>(stream: &mut S) -> Result<(), FramingError>
where
    S: tokio::io::AsyncWrite + Unpin + ?Sized,
{
    tokio::io::AsyncWriteExt::write_all(stream, PROBE_ACK).await?;
    tokio::io::AsyncWriteExt::flush(stream).await?;
    Ok(())
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
        let runtime = Arc::clone(&runtime);
        let permits = Arc::clone(&permits);
        tokio::spawn(async move {
            metrics::gauge!("egress_active_conns").increment(1.0);
            let (handler, outcome, sandbox_id) =
                admit_and_serve(&runtime, stream, expected_peer_uid, &permits, admission).await;
            // A probe holds nothing and a peer that is not the node was never
            // a connection to this broker; neither belongs on the session
            // series, and each has a counter of its own.
            if outcome != PROBE && outcome != PEER_REJECTED {
                metrics::counter!("egress_conns_total", "handler" => handler, "outcome" => outcome)
                    .increment(1);
            }
            // One series per sandbox, and only while debug is on: a node
            // runs thousands of sandboxes a day and each one would leave a
            // series behind it forever.
            if let Some(sandbox_id) =
                sandbox_id.filter(|_| tracing::enabled!(tracing::Level::DEBUG))
            {
                metrics::counter!("egress_sandbox_conns_total", "sandbox" => sandbox_id)
                    .increment(1);
            }
            metrics::gauge!("egress_active_conns").decrement(1.0);
        });
    }
    drop(listener);
    drain(&permits, max_connections, runtime.options.shutdown_drain).await;
    Ok(())
}

/// Whether this peer may open a session. `None` accepts any peer, which is
/// only right in tests.
///
/// Nothing here gates a readiness probe: a probe is answered before this is
/// asked, because it carries no identity, opens no session and learns
/// nothing that the socket's own directory mode does not already grant.
#[cfg(feature = "local")]
fn peer_is_the_node(stream: &tokio::net::UnixStream, expected: Option<u32>) -> bool {
    let Some(expected) = expected else {
        return true;
    };
    match stream.peer_cred() {
        Ok(cred) if cred.uid() == expected => true,
        Ok(cred) => {
            tracing::debug!(
                uid = cred.uid(),
                expected,
                "closing: the peer is not the node process"
            );
            false
        }
        Err(err) => {
            tracing::debug!(error = %err, "closing: the peer's credentials are unreadable");
            false
        }
    }
}

/// The metric labels the connection ends with: the handler side, the outcome,
/// and the sandbox it belonged to once one is known.
///
/// The first frame is read before anything is spent on the connection. A
/// readiness probe takes no permit, is counted on no session series and is
/// answered whatever uid sent it; only a frame that claims to be a session is
/// measured against the peer's uid and the connection budget. A full broker
/// therefore still answers its probe, which is what keeps back-pressure from
/// reading as an unreachable node.
#[cfg(feature = "local")]
async fn admit_and_serve(
    runtime: &Runtime,
    mut stream: tokio::net::UnixStream,
    expected_peer_uid: Option<u32>,
    permits: &Arc<tokio::sync::Semaphore>,
    admission: Duration,
) -> (&'static str, &'static str, Option<String>) {
    let deadline = tokio::time::Instant::now() + admission;
    let frame = match tokio::time::timeout_at(deadline, framing::read_frame(&mut stream)).await {
        Ok(Ok(frame)) => frame,
        Ok(Err(err)) => {
            let err = RuntimeError::from(err);
            tracing::debug!(error = %err, "connection refused before any handler");
            return (refusal_handler(err.outcome()), err.outcome(), None);
        }
        Err(_) => {
            tracing::debug!("the first frame did not arrive inside the deadline");
            return ("none", "admission_timeout", None);
        }
    };
    if frame.is_empty() {
        if let Err(err) = answer_probe(&mut stream).await {
            tracing::debug!(error = %err, "the readiness probe went unanswered");
        }
        return ("none", PROBE, None);
    }
    if !peer_is_the_node(&stream, expected_peer_uid) {
        metrics::counter!("egress_peer_rejected_total").increment(1);
        return ("none", PEER_REJECTED, None);
    }
    let Ok(_permit) = Arc::clone(permits).try_acquire_owned() else {
        // Answering a refusal needs no slot, and the caller learns why
        // instead of waiting out its own timeout.
        tracing::debug!("refusing a session: no connection slot is free");
        let _ = framing::write_json(&mut stream, &Ack::rejected(ADMISSION_FULL)).await;
        return (refusal_handler(ADMISSION_FULL), ADMISSION_FULL, None);
    };
    let admitted = match tokio::time::timeout_at(
        deadline,
        runtime.admit_frame(&frame, Box::new(stream)),
    )
    .await
    {
        Ok(Ok(admitted)) => admitted,
        Ok(Err(err)) => {
            tracing::debug!(error = %err, "connection refused before any handler");
            return (refusal_handler(err.outcome()), err.outcome(), None);
        }
        Err(_) => {
            tracing::debug!("the acknowledgement did not get out inside the deadline");
            return ("none", "admission_timeout", None);
        }
    };
    let sandbox_id = Some(admitted.header.sandbox_id.clone());
    match runtime.serve(admitted).await {
        Ok(()) => ("runtime", "ok", sandbox_id),
        Err(err) => {
            tracing::debug!(error = %err, "connection ended with an error");
            ("runtime", err.outcome(), sandbox_id)
        }
    }
}

/// The `handler` label a refusal before any handler is counted under.
///
/// `admission_full` keeps the `handler="none"` it was counted under while the
/// refusal happened at the door: it is the same event seen one frame later,
/// and a label that moves is a different series.
#[cfg(feature = "local")]
fn refusal_handler(outcome: &str) -> &'static str {
    if outcome == ADMISSION_FULL {
        "none"
    } else {
        "runtime"
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
            // SAFETY: `getuid` reads process state and cannot fail.
            unsafe { libc::getuid() }
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

        /// A broker whose `expected_peer_uid` is somebody else's, so this
        /// test process is any uid but the node's.
        async fn start_expecting_another_uid() -> Broker {
            start(Options {
                expected_peer_uid: Some(own_uid() + 1),
                ..options()
            })
            .await
        }

        #[tokio::test]
        async fn a_probe_is_answered_whatever_uid_sent_it() {
            let broker = start_expecting_another_uid().await;

            transport(&broker)
                .probe()
                .await
                .expect("a probe carries no identity and opens no session");
        }

        #[test]
        fn a_full_broker_counts_its_refusals_on_the_series_it_always_used() {
            assert_eq!(refusal_handler(ADMISSION_FULL), "none");
            assert_eq!(refusal_handler(PER_SANDBOX_FULL), "runtime");
        }

        #[tokio::test]
        async fn a_full_broker_answers_the_probe_and_refuses_the_session() {
            // Not one connection slot, so no session can be admitted at all —
            // and the probe still is. A broker that could not answer its
            // readiness probe under load would be marked NotReady and its
            // node `local_unreachable`, which turns back-pressure into an
            // outage.
            let broker = start(Options {
                max_connections: 0,
                expected_peer_uid: Some(own_uid()),
                ..options()
            })
            .await;
            let transport = transport(&broker);

            transport
                .probe()
                .await
                .expect("a probe takes no connection slot");

            // The same connection shape carrying an identity frame is what
            // gets refused instead, and with a reason rather than a close.
            match transport.open(current_header()).await {
                Err(crate::transport::TransportError::Rejected { reason }) => {
                    assert_eq!(reason, ADMISSION_FULL)
                }
                Err(other) => panic!("expected a refusal with a reason, got {other:?}"),
                Ok(_) => panic!("a session was admitted with no connection slot free"),
            }
        }

        #[tokio::test]
        async fn an_identity_frame_from_another_uid_is_closed_unanswered() {
            let broker = start_expecting_another_uid().await;

            match transport(&broker).open(current_header()).await {
                Err(crate::transport::TransportError::Unavailable(_)) => {}
                Err(other) => panic!("an identity header stays the node's alone, got {other:?}"),
                Ok(_) => panic!("an identity header stays the node's alone"),
            }
        }

        #[tokio::test]
        async fn the_node_uid_may_open_a_session_and_probe() {
            let broker = start(Options {
                expected_peer_uid: Some(own_uid()),
                ..options()
            })
            .await;

            transport(&broker)
                .probe()
                .await
                .expect("the node may probe");
            let _held = transport(&broker)
                .open(current_header())
                .await
                .expect("the node opens sessions");
        }

        #[tokio::test]
        async fn only_the_node_uid_may_open_a_session() {
            let broker = start_expecting_another_uid().await;
            // The listener cannot be reached from another uid inside a test,
            // so the uid the broker expects is varied instead.
            let stream = tokio::net::UnixStream::connect(&broker.socket_path)
                .await
                .unwrap();

            assert!(peer_is_the_node(&stream, Some(own_uid())));
            assert!(!peer_is_the_node(&stream, Some(own_uid() + 1)));
            assert!(
                peer_is_the_node(&stream, None),
                "an unset expectation admits any peer, which is only right in tests"
            );
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
        async fn a_session_past_the_limit_is_refused_with_a_reason() {
            // Refused after its frame rather than at the door: the reason
            // reaches the node, and its own probe on the same socket is
            // answered rather than swept up in the refusal.
            let broker = start(Options {
                max_connections: 1,
                expected_peer_uid: Some(own_uid()),
                ..options()
            })
            .await;
            let _held = transport(&broker).open(current_header()).await.unwrap();

            match transport(&broker).open(current_header()).await {
                Err(crate::transport::TransportError::Rejected { reason }) => {
                    assert_eq!(reason, ADMISSION_FULL)
                }
                Err(other) => panic!("expected a refusal with a reason, got {other:?}"),
                Ok(_) => panic!("the listener admitted a session past its limit"),
            }
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
