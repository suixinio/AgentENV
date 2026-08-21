//! The wake-up channel.
//!
//! One Redis pub/sub connection per replica, one channel, and the routing key
//! in the payload. That is enough because a subscriber filters locally; a
//! channel per sandbox would mean a `SUBSCRIBE`/`UNSUBSCRIBE` round trip on
//! every wait.
//!
//! 🔴 This is not the lifecycle-event channel, and the two must not be merged.
//! What travels here is "something you were waiting on moved" — a routing key,
//! a shape that will not change. Lifecycle events carry orchestration payloads
//! that change with the orchestration logic. Hanging the observability reporter
//! off this channel would tie the heartbeat's wire format to the state machine.
//!
//! Everything here is an optimisation. Every wait in this module also has a
//! poll fallback, so a dropped notification costs latency and never
//! correctness — which is what makes it safe for the subscriber task to
//! reconnect silently.

use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use futures::StreamExt;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tracing::{debug, warn};

/// A modest per-key buffer. Waiters treat a lagged channel exactly as they
/// treat a received notification — go and read the truth — so overflow is not
/// a lost wake-up.
const WAKE_BUFFER: usize = 8;

pub struct Notifier {
    channel: String,
    waiters: Arc<DashMap<String, broadcast::Sender<()>>>,
    publisher: redis::aio::ConnectionManager,
    task: Option<JoinHandle<()>>,
}

impl Notifier {
    pub fn start(
        client: redis::Client,
        publisher: redis::aio::ConnectionManager,
        channel: String,
    ) -> Self {
        let waiters: Arc<DashMap<String, broadcast::Sender<()>>> = Arc::new(DashMap::new());
        let task = tokio::spawn(subscriber_loop(
            client,
            channel.clone(),
            Arc::clone(&waiters),
        ));
        Self {
            channel,
            waiters,
            publisher,
            task: Some(task),
        }
    }

    /// Registers interest in a routing key.
    ///
    /// 🔴 Callers must subscribe **before** they read the value they are
    /// waiting on. The other order has a window in which the value changes
    /// after the read and before the subscription, and the waiter then sleeps
    /// until the poll fallback fires.
    pub fn subscribe(&self, routing_key: &str) -> broadcast::Receiver<()> {
        self.waiters
            .entry(routing_key.to_string())
            .or_insert_with(|| broadcast::channel(WAKE_BUFFER).0)
            .subscribe()
    }

    /// Best-effort wake-up. A failure here is logged and dropped: the poll
    /// fallback covers it, and failing an otherwise successful write because
    /// its notification did not go out would be worse than the delay.
    pub async fn publish(&self, routing_key: &str) {
        let mut connection = self.publisher.clone();
        let published: redis::RedisResult<()> = redis::cmd("PUBLISH")
            .arg(&self.channel)
            .arg(routing_key)
            .query_async(&mut connection)
            .await;
        if let Err(error) = published {
            debug!(%routing_key, %error, "wake-up notification not published");
        }
    }
}

impl Drop for Notifier {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

async fn subscriber_loop(
    client: redis::Client,
    channel: String,
    waiters: Arc<DashMap<String, broadcast::Sender<()>>>,
) {
    let mut backoff = Duration::from_millis(100);
    loop {
        match client.get_async_pubsub().await {
            Ok(mut pubsub) => {
                if let Err(error) = pubsub.subscribe(&channel).await {
                    warn!(%channel, %error, "failed to subscribe to the store wake-up channel");
                } else {
                    backoff = Duration::from_millis(100);
                    let mut stream = pubsub.on_message();
                    while let Some(message) = stream.next().await {
                        let Ok(routing_key) = message.get_payload::<String>() else {
                            continue;
                        };
                        wake(&waiters, &routing_key);
                    }
                    debug!(%channel, "store wake-up subscription ended; reconnecting");
                }
            }
            Err(error) => {
                warn!(%channel, %error, "store wake-up connection failed; reconnecting");
            }
        }

        // 🔴 Wake everyone on the way back round. A subscription that dropped
        // may have swallowed notifications, and a waiter that re-reads the
        // truth for no reason costs one round trip, where a waiter that never
        // wakes costs the caller its whole patience.
        wake_all(&waiters);

        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(5));
    }
}

fn wake(waiters: &DashMap<String, broadcast::Sender<()>>, routing_key: &str) {
    let empty = match waiters.get(routing_key) {
        Some(sender) => sender.send(()).is_err(),
        None => return,
    };
    if empty {
        // Nobody is listening any more; do not keep the entry for ever.
        waiters.remove_if(routing_key, |_, sender| sender.receiver_count() == 0);
    }
}

fn wake_all(waiters: &DashMap<String, broadcast::Sender<()>>) {
    waiters.retain(|_, sender| sender.send(()).is_ok());
}
