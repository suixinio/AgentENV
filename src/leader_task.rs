//! A handle onto a background task only one replica in the cluster runs.
//!
//! # 🔴 The handle, not the election
//!
//! The election is PostgreSQL's — `pg_try_advisory_lock`, in
//! [`crate::pg::election`] — and needs a connection pool. Waiting for the
//! resulting loop to stop needs neither: it is a `watch` channel and a
//! `JoinHandle`. The process assembly that owns the shutdown order carries a
//! `Vec` of these and never asks how the leader was chosen, so the type it
//! carries lives here rather than beside the election that mints it.
//!
//! Kept apart from the plain `JoinHandle`s the same assembly aborts, and
//! deliberately: [`LeaderTaskHandle::shutdown`] is what releases this
//! replica's advisory lock if it is currently leader, and an `.abort()` would
//! skip that release entirely and strand the lock until the pool is torn down.

use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::warn;

/// A leader-elected background loop, and the two things that stop it.
///
/// 🔴 `shutdown` does not preempt an iteration already running. The loop
/// observes the signal at the top of each iteration; a body already in flight
/// runs to completion first. Callers on a shutdown budget should bound the
/// body's own runtime rather than assume this can cut it off.
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
