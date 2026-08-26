use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use rocksdb::{Direction, IteratorMode, Options, WriteBatch, WriteOptions, DB};

/// Default bound for [`LocalKvStore::close`], reused by every node-local
/// RocksDB store's shutdown path so an operator reading shutdown logs across
/// all of them only has one number to remember.
pub const DEFAULT_CLOSE_TIMEOUT: Duration = Duration::from_secs(15);

/// Outcome of [`LocalKvStore::close`]: whether RocksDB's background
/// compaction/flush work actually stopped within the timeout, or is still
/// running when the call gave up waiting on it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocalKvCloseOutcome {
    /// Background work stopped before the timeout elapsed. Whenever the last
    /// `Arc<DB>` reference for this store eventually drops — right after this
    /// call, or much later if something else still holds a clone — RocksDB's
    /// own close (`rocksdb_close`, invoked from `DBWithThreadModeInner`'s
    /// `Drop`) has nothing left to wait for and returns immediately.
    Closed,
    /// The timeout elapsed before background work stopped. The cancellation
    /// request has already been made — RocksDB is still winding down on its
    /// own, in the background — but nothing is waiting for it any more, so a
    /// later, unbounded `Drop` of the last reference could still block for as
    /// long as that work takes.
    ///
    /// 🔴 In this process that later `Drop` is **not** bounded by
    /// `shutdown_timeout` in `src/bin/aenv-node.rs`, whatever a stale version of
    /// this comment used to claim. `main` there is `let result =
    /// runtime.block_on(async_main()); runtime.shutdown_timeout(...);` —
    /// `shutdown_timeout` only starts once `block_on` has already *returned*.
    /// But the last `Arc<DB>` reference for a store like this one is dropped
    /// from inside `async_main` itself: either directly, when a local holding
    /// a clone (the API app's `Arc<SnapshotManager>`, say) goes out of scope,
    /// or via `shutdown_cleanup.await?` joining a spawned task that drops its
    /// own clone (`NodeRuntime::shutdown` consuming `self`). Either way that
    /// drop — and, if it is the last reference, RocksDB's blocking close
    /// inside it — has to finish before `async_main` can return, which is
    /// before `block_on` can return, which is before `shutdown_timeout` is
    /// even called. So a `TimedOut` here is not "logged and then bounded
    /// later": if this store's last reference drops during this process's own
    /// shutdown sequence, rather than being kept alive by something that
    /// outlives it, that drop can hang `block_on` — and with it the whole
    /// process — for as long as the outstanding compaction/flush work takes,
    /// with nothing left in this file or `server.rs` to cut it short.
    TimedOut,
}

/// Durability policy for writes made through [`LocalKvStore`].
///
/// The levels intentionally expose only the knobs AgentENV needs instead of
/// leaking RocksDB's full `WriteOptions` surface to callers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocalStoreDurability {
    /// Skip the write-ahead log.
    ///
    /// This is useful for tests and regenerable local indexes where process or
    /// machine crashes may lose the latest writes.
    Memory,
    /// Use RocksDB's write-ahead log without fsyncing each write.
    ///
    /// This is a good default for local catalogs that should survive normal
    /// process restarts but do not need power-loss guarantees.
    Wal,
    /// Use the write-ahead log and sync each write before it is acknowledged.
    ///
    /// This is intended for small, critical metadata records where recovery is
    /// more important than write throughput.
    Sync,
}

impl LocalStoreDurability {
    /// Build the RocksDB write options for this durability policy.
    pub fn write_options(self) -> WriteOptions {
        let mut options = WriteOptions::new();
        options.disable_wal(matches!(self, Self::Memory));
        options.set_sync(matches!(self, Self::Sync));
        options
    }
}

/// Small async-friendly wrapper around a single RocksDB key-value database.
///
/// RocksDB's Rust API is synchronous. All potentially blocking database calls
/// are run on Tokio's blocking thread pool so callers can use this helper from
/// async orchestrator and P2P paths without parking a runtime worker thread.
#[derive(Clone)]
pub struct LocalKvStore {
    db: Arc<DB>,
    durability: LocalStoreDurability,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LocalKvBatchOp {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

impl LocalKvBatchOp {
    pub fn put(key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
        Self::Put {
            key: key.into(),
            value: value.into(),
        }
    }

    pub fn delete(key: impl Into<Vec<u8>>) -> Self {
        Self::Delete { key: key.into() }
    }
}

impl std::fmt::Debug for LocalKvStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalKvStore")
            .field("durability", &self.durability)
            .finish_non_exhaustive()
    }
}

impl LocalKvStore {
    /// Open or create a RocksDB database at `path`.
    ///
    /// The parent directory is created automatically. The database itself uses
    /// RocksDB's default column family and stores opaque byte keys and values.
    pub async fn open(
        path: impl Into<PathBuf>,
        durability: LocalStoreDurability,
    ) -> anyhow::Result<Self> {
        let path = path.into();
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("create RocksDB parent dir {}", parent.display()))?;
        }
        let db = tokio::task::spawn_blocking(move || {
            let mut options = Options::default();
            options.create_if_missing(true);
            DB::open(&options, &path).with_context(|| format!("open RocksDB {}", path.display()))
        })
        .await
        .context("join RocksDB open task")??;

        Ok(Self {
            db: Arc::new(db),
            durability,
        })
    }

    /// Boundedly stops this store's background compaction/flush work ahead of
    /// its eventual `Drop`.
    ///
    /// 🔴 RocksDB's own close (`rocksdb_close`, called from
    /// `DBWithThreadModeInner`'s `Drop`) waits, *unboundedly*, for any
    /// in-progress background compaction/flush to finish before it returns —
    /// which is what turns an ordinary `drop(store)` deep inside a
    /// `spawn_blocking` closure into a wait `tokio::runtime::Runtime::drop`
    /// can sit through forever (its `BlockingPool::drop` calls
    /// `shutdown(None)`, and `None` means "no timeout"). RocksDB exposes
    /// `cancel_all_background_work(wait: bool)` for exactly this: an `&self`,
    /// non-consuming call that requests the same stop and, with `wait: true`,
    /// blocks until it has actually happened. Running that call here, wrapped
    /// in our own `tokio::time::timeout`, moves the wait to a call site that
    /// bounds it and reports whether it finished — so the store's actual
    /// `Drop`, whenever it runs and regardless of how many other
    /// `LocalKvStore` clones still hold the same `Arc<DB>`, finds nothing left
    /// to wait for.
    ///
    /// Because this never consumes or drops the underlying `Arc<DB>`, calling
    /// it more than once (or on a store other code still uses) is harmless:
    /// a later call just re-confirms background work is already stopped.
    pub async fn close(&self, timeout: Duration) -> LocalKvCloseOutcome {
        let db = Arc::clone(&self.db);
        run_blocking_bounded(timeout, move || db.cancel_all_background_work(true)).await
    }

    /// Read a value by key.
    ///
    /// Returns `Ok(None)` when the key is absent.
    pub async fn get(&self, key: impl Into<Vec<u8>>) -> anyhow::Result<Option<Vec<u8>>> {
        let db = Arc::clone(&self.db);
        let key = key.into();
        tokio::task::spawn_blocking(move || db.get(key).context("get RocksDB value"))
            .await
            .context("join RocksDB get task")?
    }

    /// Insert or replace a key-value pair using the store's durability policy.
    pub async fn put(
        &self,
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
    ) -> anyhow::Result<()> {
        let db = Arc::clone(&self.db);
        let key = key.into();
        let value = value.into();
        let write_options = self.durability.write_options();
        tokio::task::spawn_blocking(move || {
            db.put_opt(key, value, &write_options)
                .context("put RocksDB value")
        })
        .await
        .context("join RocksDB put task")?
    }

    /// Remove a key using the store's durability policy.
    ///
    /// Deleting a missing key is treated as success by RocksDB.
    pub async fn delete(&self, key: impl Into<Vec<u8>>) -> anyhow::Result<()> {
        let db = Arc::clone(&self.db);
        let key = key.into();
        let write_options = self.durability.write_options();
        tokio::task::spawn_blocking(move || {
            db.delete_opt(key, &write_options)
                .context("delete RocksDB value")
        })
        .await
        .context("join RocksDB delete task")?
    }

    /// Apply multiple key mutations atomically using the store's durability policy.
    pub async fn write_batch(
        &self,
        ops: impl IntoIterator<Item = LocalKvBatchOp>,
    ) -> anyhow::Result<()> {
        let ops = ops.into_iter().collect::<Vec<_>>();
        if ops.is_empty() {
            return Ok(());
        }

        let db = Arc::clone(&self.db);
        let write_options = self.durability.write_options();
        tokio::task::spawn_blocking(move || {
            let mut batch = WriteBatch::default();
            for op in ops {
                match op {
                    LocalKvBatchOp::Put { key, value } => batch.put(key, value),
                    LocalKvBatchOp::Delete { key } => batch.delete(key),
                }
            }
            db.write_opt(batch, &write_options)
                .context("write RocksDB batch")
        })
        .await
        .context("join RocksDB batch task")?
    }

    /// Iterate over all entries in key order and accumulate a result.
    ///
    /// The callback runs on a blocking thread and must remain synchronous. Use
    /// this when entries can be decoded or accumulated without awaiting. If the
    /// caller needs async cleanup or side effects per entry, use [`Self::entries`]
    /// and process the returned vector in async code.
    pub async fn fold<T, F>(&self, mut init: T, mut visit: F) -> anyhow::Result<T>
    where
        T: Send + 'static,
        F: FnMut(&mut T, Vec<u8>, Vec<u8>) -> anyhow::Result<()> + Send + 'static,
    {
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || {
            for item in db.iterator(IteratorMode::Start) {
                let (key, value) = item.context("iterate RocksDB values")?;
                visit(&mut init, key.into_vec(), value.into_vec())?;
            }
            Ok(init)
        })
        .await
        .context("join RocksDB iterator task")?
    }

    /// Load all key-value pairs into memory.
    ///
    /// This is intentionally explicit for call sites that need to leave the
    /// blocking iterator before doing async work.
    pub async fn entries(&self) -> anyhow::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.fold(Vec::new(), |entries, key, value| {
            entries.push((key, value));
            Ok(())
        })
        .await
    }

    /// Load all entries whose key starts with `prefix`, in key order.
    ///
    /// Seeks to `prefix` and stops at the first key that no longer matches, so
    /// prefix-scoped lookups don't load the whole database the way
    /// [`Self::entries`] does.
    pub async fn scan_prefix(
        &self,
        prefix: impl Into<Vec<u8>>,
    ) -> anyhow::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let db = Arc::clone(&self.db);
        let prefix = prefix.into();
        tokio::task::spawn_blocking(move || {
            let mut entries = Vec::new();
            for item in db.iterator(IteratorMode::From(&prefix, Direction::Forward)) {
                let (key, value) = item.context("iterate RocksDB prefix")?;
                if !key.starts_with(&prefix) {
                    break;
                }
                entries.push((key.into_vec(), value.into_vec()));
            }
            Ok(entries)
        })
        .await
        .context("join RocksDB prefix scan task")?
    }
}

/// Runs `f` on the blocking pool, bounded by `timeout`. The mechanism behind
/// [`LocalKvStore::close`], factored out so it can be exercised with a
/// deliberately stuck closure without needing real RocksDB internals to
/// actually stall (which is slow to arrange and not reliably deterministic).
async fn run_blocking_bounded<F>(timeout: Duration, f: F) -> LocalKvCloseOutcome
where
    F: FnOnce() + Send + 'static,
{
    match tokio::time::timeout(timeout, tokio::task::spawn_blocking(f)).await {
        Ok(Ok(())) => LocalKvCloseOutcome::Closed,
        // `f` cannot itself return an error; a `JoinError` here only happens
        // if the blocking task panicked or was cancelled, neither of which
        // this call does. Treat it the same as "did not finish" rather than
        // propagating a panic from a shutdown path whose whole job is to not
        // make things worse.
        Ok(Err(_)) => LocalKvCloseOutcome::TimedOut,
        Err(_) => LocalKvCloseOutcome::TimedOut,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 🔴 The test load-bearing for the entire point of `close`: without the
    /// `tokio::time::timeout` wrapper in `run_blocking_bounded`, a slow (or,
    /// in the production case this guards against, RocksDB-background-work-is-
    /// still-running) blocking closure would make the caller wait for however
    /// long the closure takes — which, unbounded, is the exact failure mode
    /// that left `server --role node` running past `terminationGracePeriodSeconds`
    /// on every node that had actually run a VM.
    ///
    /// The closure sleeps well past the timeout rather than blocking forever:
    /// a closure that never returns would still make *this* assertion pass,
    /// but would then hang the test's own runtime teardown, which joins
    /// exactly this kind of still-running blocking-pool thread unboundedly —
    /// the same bug this whole file exists to close off, just relocated into
    /// the test suite instead of fixed.
    #[tokio::test]
    async fn close_returns_promptly_when_the_blocking_closure_is_still_running() {
        const TIMEOUT: Duration = Duration::from_millis(100);
        const CLOSURE_DURATION: Duration = Duration::from_secs(2);

        let started = std::time::Instant::now();
        let outcome = run_blocking_bounded(TIMEOUT, || std::thread::sleep(CLOSURE_DURATION)).await;
        let elapsed = started.elapsed();

        assert_eq!(
            outcome,
            LocalKvCloseOutcome::TimedOut,
            "the closure was still running when the timeout elapsed"
        );
        assert!(
            elapsed < CLOSURE_DURATION / 2,
            "close waited for the stuck closure instead of returning at the timeout: {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn close_reports_closed_when_the_blocking_closure_finishes_in_time() {
        let outcome = run_blocking_bounded(Duration::from_secs(5), || {}).await;
        assert_eq!(outcome, LocalKvCloseOutcome::Closed);
    }
}
