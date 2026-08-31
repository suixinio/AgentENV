//! Shutdown handle for a leader-elected background task.
//!
//! Shutdown must release the leader's advisory lock; aborting the task would
//! strand it until the connection pool is torn down.

use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::warn;

/// A leader-elected background loop and its shutdown controls.
///
/// Shutdown does not preempt an iteration already in flight.
pub struct LeaderTaskHandle {
    shutdown_tx: watch::Sender<bool>,
    join: JoinHandle<()>,
}

impl LeaderTaskHandle {
    pub fn new(shutdown_tx: watch::Sender<bool>, join: JoinHandle<()>) -> Self {
        Self { shutdown_tx, join }
    }

    /// Requests the loop stop, releases whatever the loop holds while it is
    /// leader, and waits for the background task to exit.
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(true);
        if let Err(err) = self.join.await {
            warn!(error = %err, "leader-elected task join failed");
        }
    }
}
