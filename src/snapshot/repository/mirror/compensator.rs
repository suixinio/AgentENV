//! The one thing that makes the second copy verifiable rather than asserted.
//!
//! A double write that reported both halves and repaired neither would leave
//! every failed mirror write as a permanent, invisible disagreement. This is
//! the loop that replays them, and the gauge it maintains is what the read-side
//! switch is allowed to depend on.

use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::snapshot::repository::interfaces::SnapshotCatalog;

use super::backlog::MirrorBacklog;

/// How often the backlog is replayed.
pub const DEFAULT_COMPENSATOR_INTERVAL: Duration = Duration::from_secs(30);

/// Replays owed object-store writes for as long as it is alive.
pub struct MirrorCompensator {
    task: JoinHandle<()>,
}

impl MirrorCompensator {
    /// Starts the loop. It stops when this value is dropped.
    pub fn spawn(
        backlog: Arc<MirrorBacklog>,
        object_store: Arc<dyn SnapshotCatalog>,
        interval: Duration,
    ) -> Self {
        // A zero period panics `tokio::time::interval`, so an operator could
        // otherwise take the node down with a configuration value.
        let period = interval.max(Duration::from_secs(1));

        let task = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(period);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                if backlog.lag() == 0 {
                    continue;
                }
                match backlog.drain_once(object_store.as_ref()).await {
                    Ok(pass) => {
                        if pass.repaired > 0 || pass.diverged > 0 {
                            info!(
                                repaired = pass.repaired,
                                retry = pass.retry,
                                diverged = pass.diverged,
                                skipped = pass.skipped,
                                remaining = pass.remaining,
                                "replayed owed snapshot catalog mirror writes"
                            );
                        } else {
                            debug!(
                                retry = pass.retry,
                                remaining = pass.remaining,
                                "snapshot catalog mirror backlog did not clear this pass"
                            );
                        }
                    }
                    Err(error) => warn!(
                        %error,
                        "a snapshot catalog mirror repair pass could not read its own backlog"
                    ),
                }
            }
        });

        Self { task }
    }
}

impl Drop for MirrorCompensator {
    fn drop(&mut self) {
        self.task.abort();
    }
}
