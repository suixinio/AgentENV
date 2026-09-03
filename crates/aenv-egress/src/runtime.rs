use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use zeroize::Zeroizing;

use crate::dispatch::{Dispatcher, Reject};
use crate::framing::{self, FramingError};
use crate::handler::HandlerError;
use crate::header::{Ack, IdentityHeader, ReplayCache, ReplayVerdict};
use crate::transport::AsyncStream;

/// One deadline covers the TLS handshake and the identity frame behind it:
/// nothing is authenticated before both are done.
pub const DEFAULT_ADMISSION_TIMEOUT: Duration = Duration::from_secs(10);
/// Connections the listener holds at once; the excess is closed, not queued.
pub const DEFAULT_MAX_CONNECTIONS: u32 = 4096;
/// How long a shutdown lets live sessions finish, inside a 30s pod grace.
pub const DEFAULT_SHUTDOWN_DRAIN: Duration = Duration::from_secs(25);

/// How the broker side authenticates headers and how much unauthenticated
/// work it admits. `keys` holds the current and, during rotation, the
/// previous shared secret.
pub struct Options {
    pub keys: Vec<Zeroizing<Vec<u8>>>,
    pub max_skew: Duration,
    pub replay_capacity: usize,
    pub admission_timeout: Duration,
    pub max_connections: u32,
    pub shutdown_drain: Duration,
}

impl Options {
    pub fn new(keys: Vec<Vec<u8>>, max_skew: Duration, replay_capacity: usize) -> Self {
        Self {
            keys: keys.into_iter().map(Zeroizing::new).collect(),
            max_skew,
            replay_capacity,
            admission_timeout: DEFAULT_ADMISSION_TIMEOUT,
            max_connections: DEFAULT_MAX_CONNECTIONS,
            shutdown_drain: DEFAULT_SHUTDOWN_DRAIN,
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

/// The broker side of one transport: verifies, deduplicates, acknowledges
/// and dispatches every incoming stream.
pub struct Runtime {
    options: Options,
    dispatcher: Arc<Dispatcher>,
    replay: Mutex<ReplayCache>,
}

impl Runtime {
    pub fn new(options: Options, dispatcher: Arc<Dispatcher>) -> Self {
        let replay = Mutex::new(ReplayCache::new(options.max_skew, options.replay_capacity));
        Self {
            options,
            dispatcher,
            replay,
        }
    }

    pub fn dispatcher(&self) -> &Arc<Dispatcher> {
        &self.dispatcher
    }

    /// Reads the header frame, verifies it, writes the [`Ack`] and, when
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
        let handler = match self.admit(&header) {
            Ok(handler) => handler,
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
    ) -> Result<Arc<dyn crate::handler::Handler>, &'static str> {
        let now = SystemTime::now();
        header
            .verify(&self.options.keys, self.options.max_skew, now)
            .map_err(|err| err.reason())?;
        let verdict = self
            .replay
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .check_and_insert(
                header.nonce,
                header.issued_at_unix_ms,
                IdentityHeader::now_unix_ms(),
            );
        if let Some(reason) = verdict.reason() {
            return Err(reason);
        }
        debug_assert_eq!(verdict, ReplayVerdict::Fresh);
        self.dispatcher
            .accept(header)
            .map_err(|reject: Reject| reject.reason())
    }
}

/// A stream past authentication, with the handler that will serve it.
struct Admitted {
    handler: Arc<dyn crate::handler::Handler>,
    header: IdentityHeader,
    stream: Box<dyn AsyncStream>,
}

/// Accepts TLS connections on `listener` and dispatches each on its own task
/// until `shutdown` resolves, then stops accepting and lets the live sessions
/// finish. Every connection's outcome is counted.
#[cfg(feature = "tls")]
pub async fn run(
    runtime: Arc<Runtime>,
    listener: tokio::net::TcpListener,
    acceptor: tokio_native_tls::TlsAcceptor,
    shutdown: impl std::future::Future<Output = ()>,
) -> anyhow::Result<()> {
    let max_connections = runtime.options.max_connections;
    let admission = runtime.options.admission_timeout;
    let permits = Arc::new(tokio::sync::Semaphore::new(max_connections as usize));
    let mut shutdown = std::pin::pin!(shutdown);
    loop {
        let (tcp, peer) = tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok(accepted) => accepted,
                Err(err) => {
                    tracing::warn!(error = %err, "accept failed");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
            },
            _ = &mut shutdown => break,
        };
        let Ok(permit) = Arc::clone(&permits).try_acquire_owned() else {
            tracing::debug!(%peer, max_connections, "closing: no connection slot is free");
            metrics::counter!("egress_conns_total", "handler" => "none", "outcome" => "admission_full").increment(1);
            drop(tcp);
            continue;
        };
        let runtime = Arc::clone(&runtime);
        let acceptor = acceptor.clone();
        tokio::spawn(async move {
            let _permit = permit;
            metrics::gauge!("egress_active_conns").increment(1.0);
            let (handler, outcome) =
                admit_and_serve(&runtime, &acceptor, tcp, peer, admission).await;
            metrics::counter!("egress_conns_total", "handler" => handler, "outcome" => outcome)
                .increment(1);
            metrics::gauge!("egress_active_conns").decrement(1.0);
        });
    }
    drop(listener);
    drain(&permits, max_connections, runtime.options.shutdown_drain).await;
    Ok(())
}

/// The metric labels the connection ends with: the handler side, then the
/// outcome.
#[cfg(feature = "tls")]
async fn admit_and_serve(
    runtime: &Runtime,
    acceptor: &tokio_native_tls::TlsAcceptor,
    tcp: tokio::net::TcpStream,
    peer: std::net::SocketAddr,
    admission: Duration,
) -> (&'static str, &'static str) {
    let deadline = tokio::time::Instant::now() + admission;
    let tls = match tokio::time::timeout_at(deadline, acceptor.accept(tcp)).await {
        Ok(Ok(tls)) => tls,
        Ok(Err(err)) => {
            tracing::debug!(%peer, error = %err, "tls accept failed");
            return ("none", "tls_failed");
        }
        Err(_) => {
            tracing::debug!(%peer, "the tls handshake did not finish inside the deadline");
            return ("none", "admission_timeout");
        }
    };
    let admitted =
        match tokio::time::timeout_at(deadline, runtime.admit_stream(Box::new(tls))).await {
            Ok(Ok(admitted)) => admitted,
            Ok(Err(err)) => {
                let outcome = err.outcome();
                if outcome == "replayed_nonce" {
                    metrics::counter!("egress_replay_rejected_total").increment(1);
                }
                tracing::debug!(%peer, error = %err, "connection refused before any handler");
                return ("runtime", outcome);
            }
            Err(_) => {
                tracing::debug!(%peer, "the identity header did not arrive inside the deadline");
                return ("none", "admission_timeout");
            }
        };
    match runtime.serve(admitted).await {
        Ok(()) => ("runtime", "ok"),
        Err(err) => {
            tracing::debug!(%peer, error = %err, "connection ended with an error");
            ("runtime", err.outcome())
        }
    }
}

/// Every permit back in hand means every session has ended.
#[cfg(feature = "tls")]
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
    use crate::handlers::tcp::{IdentityBanner, TcpEchoHandler};
    use crate::header::test_support::sample_header;
    use crate::policy::{BrokerDenyList, UpstreamGuard};

    const KEY: &[u8] = b"shared";

    fn runtime() -> Arc<Runtime> {
        let dispatcher = Arc::new(
            Dispatcher::new(
                Arc::new(NoCredentials),
                Arc::new(UpstreamGuard::new(BrokerDenyList::default())),
            )
            .with_handler(Arc::new(TcpEchoHandler)),
        );
        Arc::new(Runtime::new(
            Options::new(vec![KEY.to_vec()], Duration::from_secs(30), 1024),
            dispatcher,
        ))
    }

    fn current_header() -> IdentityHeader {
        let mut header = sample_header();
        header.issued_at_unix_ms = IdentityHeader::now_unix_ms();
        header.nonce = IdentityHeader::fresh_nonce();
        header.sign(KEY);
        header
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
    async fn a_replayed_header_is_refused_after_the_first_use() {
        let runtime = runtime();
        let header = current_header();
        let (_first, ack) = open(&runtime, &header).await;
        assert_eq!(ack, Ack::accepted());
        let (_second, ack) = open(&runtime, &header).await;
        assert_eq!(ack, Ack::rejected("replayed_nonce"));
    }

    #[tokio::test]
    async fn a_tampered_header_is_refused_with_the_mac_reason() {
        let runtime = runtime();
        let mut header = current_header();
        header.sandbox_id = "someone-else".into();
        let (mut client, ack) = open(&runtime, &header).await;
        assert_eq!(ack, Ack::rejected("bad_mac"));
        let mut rest = Vec::new();
        client.read_to_end(&mut rest).await.unwrap();
        assert!(rest.is_empty());
    }

    #[tokio::test]
    async fn a_stale_header_is_refused_before_its_nonce_is_remembered() {
        let runtime = runtime();
        let mut header = sample_header();
        header.issued_at_unix_ms = IdentityHeader::now_unix_ms() - 60_000;
        header.sign(KEY);
        let (_client, ack) = open(&runtime, &header).await;
        assert_eq!(ack, Ack::rejected("skew_exceeded"));
        assert!(runtime.replay.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn an_unknown_handler_is_refused_after_authentication() {
        let runtime = runtime();
        let mut header = sample_header();
        header.issued_at_unix_ms = IdentityHeader::now_unix_ms();
        header.handler = "postgres".into();
        header.sign(KEY);
        let (_client, ack) = open(&runtime, &header).await;
        assert_eq!(ack, Ack::rejected("unknown_handler"));
    }

    #[cfg(all(feature = "tls", feature = "remote"))]
    mod listener {
        use tokio::net::{TcpListener, TcpStream};

        use super::*;
        use crate::tls::{generate_test_ca, CaSigner, SignerOptions};
        use crate::transport::{BrokerTransport, RemoteTransport};

        struct Broker {
            addr: std::net::SocketAddr,
            ca_pem: Vec<u8>,
            shutdown: tokio::sync::oneshot::Sender<()>,
            joined: tokio::task::JoinHandle<anyhow::Result<()>>,
        }

        fn options() -> Options {
            Options {
                admission_timeout: Duration::from_secs(5),
                shutdown_drain: Duration::from_millis(500),
                ..Options::new(vec![KEY.to_vec()], Duration::from_secs(30), 1024)
            }
        }

        async fn start(options: Options) -> Broker {
            let (ca_pem, ca_key) = generate_test_ca("runtime listener ca").unwrap();
            let signer = CaSigner::from_pem(&ca_pem, &ca_key, SignerOptions::default()).unwrap();
            let acceptor = signer
                .leaf_for("localhost", "broker")
                .unwrap()
                .acceptor
                .clone();
            let dispatcher = Arc::new(
                Dispatcher::new(
                    Arc::new(NoCredentials),
                    Arc::new(UpstreamGuard::new(BrokerDenyList::default())),
                )
                .with_handler(Arc::new(TcpEchoHandler)),
            );
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let (shutdown, rx) = tokio::sync::oneshot::channel();
            let joined = tokio::spawn(run(
                Arc::new(Runtime::new(options, dispatcher)),
                listener,
                acceptor,
                async move {
                    let _ = rx.await;
                },
            ));
            Broker {
                addr,
                ca_pem,
                shutdown,
                joined,
            }
        }

        fn transport(broker: &Broker) -> RemoteTransport {
            RemoteTransport::new(
                &broker.addr.to_string(),
                Some("localhost"),
                &broker.ca_pem,
                KEY,
            )
            .unwrap()
        }

        async fn expect_closed(stream: &mut TcpStream, what: &str) {
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
        async fn a_connection_past_the_limit_is_closed_at_the_door() {
            let broker = start(Options {
                max_connections: 1,
                ..options()
            })
            .await;
            let transport = transport(&broker);
            let _held = transport.open(current_header()).await.unwrap();

            let mut refused = TcpStream::connect(broker.addr).await.unwrap();
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
            let mut idle = TcpStream::connect(broker.addr).await.unwrap();
            expect_closed(&mut idle, "an unauthenticated connection was held open").await;
        }

        #[tokio::test]
        async fn a_live_session_holds_the_shutdown_until_it_ends() {
            let broker = start(Options {
                shutdown_drain: Duration::from_secs(10),
                ..options()
            })
            .await;
            let transport = transport(&broker);
            let held = transport.open(current_header()).await.unwrap();
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
            let transport = transport(&broker);
            let _held = transport.open(current_header()).await.unwrap();
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
