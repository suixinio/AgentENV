use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use rocksdb::{Direction, IteratorMode, Options, WriteBatch, WriteOptions, DB};

/// Default timeout for node-local RocksDB shutdown.
pub const DEFAULT_CLOSE_TIMEOUT: Duration = Duration::from_secs(15);

/// Whether RocksDB background work stopped before [`LocalKvStore::close`] timed out.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocalKvCloseOutcome {
    /// Background work stopped before the timeout.
    Closed,
    /// Background work is still winding down after cancellation was requested.
    ///
    /// Dropping the last `Arc<DB>` may still block until that work finishes.
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

    /// Requests cancellation of RocksDB background work and waits up to `timeout`.
    ///
    /// This does not consume the underlying `Arc<DB>` and is safe to call repeatedly.
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

/// Runs `f` on the blocking pool for at most `timeout`.
async fn run_blocking_bounded<F>(timeout: Duration, f: F) -> LocalKvCloseOutcome
where
    F: FnOnce() + Send + 'static,
{
    match tokio::time::timeout(timeout, tokio::task::spawn_blocking(f)).await {
        Ok(Ok(())) => LocalKvCloseOutcome::Closed,
        // A panic or cancellation also means the work did not finish cleanly.
        Ok(Err(_)) => LocalKvCloseOutcome::TimedOut,
        Err(_) => LocalKvCloseOutcome::TimedOut,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
