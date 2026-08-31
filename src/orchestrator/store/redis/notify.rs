//! Best-effort Redis wake-up channel with polling fallback.
//! One channel carries local routing keys; missed notifications cost latency only.

use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use futures::StreamExt;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tracing::{debug, warn};

// Overflow is harmless because waiters always re-read authoritative state.
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

    /// Subscribes before callers read to close the lost-wake window.
    pub fn subscribe(&self, routing_key: &str) -> broadcast::Receiver<()> {
        self.waiters
            .entry(routing_key.to_string())
            .or_insert_with(|| broadcast::channel(WAKE_BUFFER).0)
            .subscribe()
    }

    /// Publishes a best-effort wake-up covered by polling fallback.
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

        // Wake all waiters after reconnect because notifications may have been missed.
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
        // Drop routing entries with no receivers.
        waiters.remove_if(routing_key, |_, sender| sender.receiver_count() == 0);
    }
}

fn wake_all(waiters: &DashMap<String, broadcast::Sender<()>>) {
    waiters.retain(|_, sender| sender.send(()).is_ok());
}
