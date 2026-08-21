//! The one thing that makes the second copy verifiable rather than asserted.
//!
//! A double write that reported both halves and repaired neither would leave
//! every failed mirror write as a permanent, invisible disagreement. This is
//! the loop that replays them, and the gauges it maintains are what the
//! read-side switch is allowed to depend on.

use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use super::backlog::{MirrorBacklog, MirrorTargets};

/// How often the backlog is replayed.
pub const DEFAULT_COMPENSATOR_INTERVAL: Duration = Duration::from_secs(30);

/// Replays owed catalog writes for as long as it is alive.
pub struct MirrorCompensator {
    task: JoinHandle<()>,
}

impl MirrorCompensator {
    /// Starts the loop. It stops when this value is dropped.
    pub fn spawn(backlog: Arc<MirrorBacklog>, targets: MirrorTargets, interval: Duration) -> Self {
        // A zero period panics `tokio::time::interval`, so an operator could
        // otherwise take the node down with a configuration value.
        //
        // 🔴 Built here rather than inside the task. A panic inside a spawned
        // task goes into its join handle and nowhere else, so a clamp applied
        // in there could be removed without anything outside noticing — which
        // is the same as not having one.
        let mut ticker = tokio::time::interval(interval.max(Duration::from_secs(1)));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        let task = tokio::spawn(async move {
            loop {
                ticker.tick().await;
                // 🔴 The lag, not the divergences. A recorded divergence is not
                // debt: no replay changes the answer, so a pass over one would
                // spend a request against the store the acceptance number is
                // measured on and learn nothing. They are cleared by the
                // snapshot being deleted, or by the batch that wires build
                // admission — never by this loop.
                if backlog.lag() == 0 {
                    continue;
                }
                match backlog.drain_once(&targets).await {
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
                                skipped = pass.skipped,
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use async_trait::async_trait;

    use super::*;
    use crate::snapshot::repository::interfaces::{
        SnapshotCatalog, SnapshotCommit, SnapshotListFilter,
    };
    use crate::snapshot::repository::mirror::backlog::{MirrorDirection, MirrorOp};
    use crate::snapshot::repository::{RepositoryError, RepositoryResult};
    use crate::snapshot::types::{SnapshotId, SnapshotRecord, TemplateBuildErrorReason};
    use crate::types::SandboxResources;

    /// A catalog that counts what it was asked and can be told to refuse.
    #[derive(Default)]
    struct CountingCatalog {
        creates: AtomicUsize,
        broken: AtomicBool,
    }

    impl CountingCatalog {
        fn break_it(&self) {
            self.broken.store(true, Ordering::SeqCst);
        }

        fn fix_it(&self) {
            self.broken.store(false, Ordering::SeqCst);
        }

        fn creates(&self) -> usize {
            self.creates.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl SnapshotCatalog for CountingCatalog {
        async fn create(&self, record: SnapshotRecord) -> RepositoryResult<SnapshotRecord> {
            self.creates.fetch_add(1, Ordering::SeqCst);
            if self.broken.load(Ordering::SeqCst) {
                return Err(RepositoryError::Backend {
                    message: "unreachable".to_string(),
                    source: None,
                });
            }
            Ok(record)
        }

        async fn publish_commit(
            &self,
            _commit: SnapshotCommit,
        ) -> RepositoryResult<SnapshotRecord> {
            unreachable!("this test only replays creates")
        }

        async fn get(&self, _id_or_alias: &str) -> RepositoryResult<Option<SnapshotRecord>> {
            Ok(None)
        }

        async fn list(&self, _filter: SnapshotListFilter) -> RepositoryResult<Vec<SnapshotRecord>> {
            Ok(Vec::new())
        }

        async fn delete_record(&self, _record: &SnapshotRecord) -> RepositoryResult<()> {
            Ok(())
        }

        async fn resolve_alias(&self, _alias: &str) -> RepositoryResult<Option<SnapshotId>> {
            Ok(None)
        }

        async fn try_start_build(&self, _id: &SnapshotId) -> RepositoryResult<SnapshotRecord> {
            unreachable!("this test only replays creates")
        }

        async fn mark_build_error(
            &self,
            _id: &SnapshotId,
            _reason: TemplateBuildErrorReason,
        ) -> RepositoryResult<()> {
            Ok(())
        }
    }

    fn record() -> SnapshotRecord {
        SnapshotRecord::template_waiting(SnapshotId::generate(), None, SandboxResources::default())
    }

    async fn backlog(dir: &tempfile::TempDir) -> Arc<MirrorBacklog> {
        MirrorBacklog::open(dir.path().join("mirror"))
            .await
            .expect("the backlog should open")
    }

    /// Drives the paused clock until `settled` holds, or gives up.
    ///
    /// The loop's own work reaches RocksDB through `spawn_blocking`, so one
    /// `advance` is not enough to see a tick through to its end: the runtime has
    /// to be driven again once the blocking half comes back.
    async fn advance_until(what: &str, mut settled: impl FnMut() -> bool) {
        for _ in 0..50 {
            if settled() {
                return;
            }
            tokio::time::advance(Duration::from_millis(1_100)).await;
            for _ in 0..20 {
                tokio::task::yield_now().await;
            }
        }
        panic!("the compensator never {what}");
    }

    /// Lets the loop run for a while without expecting anything of it.
    async fn advance_a_while() {
        for _ in 0..5 {
            tokio::time::advance(Duration::from_millis(1_100)).await;
            for _ in 0..20 {
                tokio::task::yield_now().await;
            }
        }
    }

    /// 🔴 The loop, not the pass. `drain_once` has been covered since it was
    /// written; what had not been is that anything ever calls it. A compensator
    /// that never ticked would leave every failed mirror write outstanding
    /// forever, and every unit test of the pass would still be green.
    #[tokio::test(start_paused = true)]
    async fn the_loop_replays_what_the_backlog_holds() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let backlog = backlog(&dir).await;
        let catalog = Arc::new(CountingCatalog::default());

        backlog
            .record(
                MirrorDirection::ObjectStore,
                MirrorOp::Create { record: record() },
            )
            .await;
        assert_eq!(backlog.lag(), 1);

        let _compensator = MirrorCompensator::spawn(
            Arc::clone(&backlog),
            MirrorTargets::object_store(Arc::clone(&catalog) as Arc<dyn SnapshotCatalog>),
            Duration::from_secs(1),
        );

        advance_until("paid the debt off", || backlog.lag() == 0).await;
        assert_eq!(catalog.creates(), 1);
    }

    /// 🔴 The loop must survive a pass that could not clear. A compensator that
    /// stopped on the first failure would leave the mirror behind for as long
    /// as the process lived, which is exactly the state the lag exists to make
    /// temporary.
    #[tokio::test(start_paused = true)]
    async fn a_pass_that_fails_does_not_stop_the_loop() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let backlog = backlog(&dir).await;
        let catalog = Arc::new(CountingCatalog::default());
        catalog.break_it();

        backlog
            .record(
                MirrorDirection::ObjectStore,
                MirrorOp::Create { record: record() },
            )
            .await;

        let _compensator = MirrorCompensator::spawn(
            Arc::clone(&backlog),
            MirrorTargets::object_store(Arc::clone(&catalog) as Arc<dyn SnapshotCatalog>),
            Duration::from_secs(1),
        );

        advance_until("tried the broken store", || catalog.creates() >= 1).await;
        assert_eq!(backlog.lag(), 1, "a broken store keeps the debt");

        catalog.fix_it();
        advance_until("recovered when the store came back", || backlog.lag() == 0).await;
    }

    /// 🔴 An operator cannot take the node down with a configuration value.
    /// `tokio::time::interval` panics on a zero period, and the interval comes
    /// straight out of the config file.
    #[tokio::test(start_paused = true)]
    async fn a_zero_interval_does_not_panic() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let backlog = backlog(&dir).await;
        let catalog = Arc::new(CountingCatalog::default());

        let _compensator = MirrorCompensator::spawn(
            backlog,
            MirrorTargets::object_store(catalog as Arc<dyn SnapshotCatalog>),
            Duration::ZERO,
        );

        advance_a_while().await;
    }

    /// Dropping it stops the replay. It is held by the assembled backend for
    /// exactly as long as the manager is, and a task that outlived its owner
    /// would keep writing a catalog nothing else is using.
    #[tokio::test(start_paused = true)]
    async fn dropping_the_compensator_stops_the_loop() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let backlog = backlog(&dir).await;
        let catalog = Arc::new(CountingCatalog::default());
        catalog.break_it();

        backlog
            .record(
                MirrorDirection::ObjectStore,
                MirrorOp::Create { record: record() },
            )
            .await;

        let compensator = MirrorCompensator::spawn(
            Arc::clone(&backlog),
            MirrorTargets::object_store(Arc::clone(&catalog) as Arc<dyn SnapshotCatalog>),
            Duration::from_secs(1),
        );
        advance_until("tried at all", || catalog.creates() >= 1).await;
        let before = catalog.creates();

        drop(compensator);
        advance_a_while().await;
        assert_eq!(
            catalog.creates(),
            before,
            "a dropped compensator must stop replaying"
        );
    }
}
