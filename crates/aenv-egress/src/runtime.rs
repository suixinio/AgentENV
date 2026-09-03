use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use zeroize::Zeroizing;

use crate::dispatch::{Dispatcher, Reject};
use crate::framing::{self, FramingError};
use crate::handler::HandlerError;
use crate::header::{Ack, IdentityHeader, ReplayCache, ReplayVerdict};
use crate::transport::AsyncStream;

/// How the broker side authenticates headers. `keys` holds the current and,
/// during rotation, the previous shared secret.
pub struct Options {
    pub keys: Vec<Zeroizing<Vec<u8>>>,
    pub max_skew: Duration,
    pub replay_capacity: usize,
}

impl Options {
    pub fn new(keys: Vec<Vec<u8>>, max_skew: Duration, replay_capacity: usize) -> Self {
        Self {
            keys: keys.into_iter().map(Zeroizing::new).collect(),
            max_skew,
            replay_capacity,
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
    pub async fn dispatch(&self, mut stream: Box<dyn AsyncStream>) -> Result<(), RuntimeError> {
        let header: IdentityHeader = framing::read_json(&mut stream).await?;
        let handler = match self.admit(&header) {
            Ok(handler) => handler,
            Err(reason) => {
                framing::write_json(&mut stream, &Ack::rejected(reason)).await?;
                return Err(RuntimeError::Refused(reason));
            }
        };
        framing::write_json(&mut stream, &Ack::accepted()).await?;
        self.dispatcher.serve(handler, &header, stream).await?;
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
