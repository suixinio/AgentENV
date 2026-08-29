//! Test support: a real Redis, and a policy about what happens when there
//! isn't one.
//!
//! # 🔴 A skipped test that prints `ok`
//!
//! The Go half of this repository shipped a make target that silently skipped
//! 152 tests and reported green, and two of the tests inside it were the only
//! ones that would have caught a `KEEPTTL` being dropped — the same defect this
//! module's tests exist to catch. So:
//!
//! * `AENV_REDIS_TEST_REQUIRED=1` turns "no Redis" into a **failure**, not a
//!   skip. `make test-with-redis` sets it.
//! * Without it, a skip prints a line beginning `SKIPPED[redis]` to stderr, and
//!   the make target greps for that line and fails if it finds one. A skip that
//!   nobody can see is a skip that becomes permanent.
//!
//! A real server, never a fake. A fake would agree with whatever this code
//! believes about `KEEPTTL`, `ZADD XX`, `cjson`'s number handling and script
//! atomicity, which is precisely the set of things worth checking.
//!
//! # What is shared with the other Redis harnesses, and what is not
//!
//! Shared, via [`crate::redis_test_server`]: the mechanical process
//! bootstrap only — free port, `redis-server` spawn under `PR_SET_PDEATHSIG`,
//! the readiness probe, `redis://…/<db>` URL construction, the
//! `AENV_REDIS_TEST_REQUIRED` predicate, and the bounds check on a database
//! counter. Those were byte-identical in three places, so a fix to one copy
//! missed the other two.
//!
//! 🔴 **Not** shared, deliberately: the [`OnceLock`] below and therefore this
//! suite's own `redis-server` process; `NEXT`, its own counter over its own
//! [`DATABASES`] logical databases; and [`store_for`] with the config it
//! builds, its `flush_namespace()` reset, the background-warmup skip,
//! [`raw`] and [`sibling`]. One shared server would
//! let this suite's namespace flush reach into a database another suite is
//! mid-test on; one shared counter would hand two suites the same database
//! number. `crate::redis_test_server::tests` fails by name if either
//! separation is undone. `crates/aenv-api/src/pg/harness.rs`'s doc comment
//! covers why the per-subsystem half stays per-subsystem rather than becoming
//! one generic harness.

use std::sync::atomic::AtomicU32;
use std::sync::OnceLock;
use std::time::Duration;

use crate::redis_test_server::{self, RedisTestServer};

use super::{RedisMetadataStore, RedisStoreConfig};

/// How many logical databases the spawned server offers, one per test.
///
/// 🔴 This suite's own space, not a shared one — see the module doc.
const DATABASES: u32 = 512;

/// 🔴 This suite's own `redis-server`, started once for this test binary.
/// Deliberately not shared with `binding_store` or `node_registry`.
pub(crate) fn server() -> Option<&'static RedisTestServer> {
    static SERVER: OnceLock<Option<RedisTestServer>> = OnceLock::new();
    SERVER
        .get_or_init(|| redis_test_server::start("redis-test-server", "the store tests", DATABASES))
        .as_ref()
}

/// 🔴 This suite's own logical-database counter. Handed to
/// [`redis_test_server::next_db`] by reference precisely so that it stays this
/// suite's; see `crate::redis_test_server::tests`.
pub(crate) fn db_counter() -> &'static AtomicU32 {
    static NEXT: AtomicU32 = AtomicU32::new(1);
    &NEXT
}

fn next_db() -> u32 {
    redis_test_server::next_db(db_counter(), DATABASES, "the store tests")
}

/// A store on its own database, or `None` when this machine has no Redis and
/// the run has not demanded one.
pub async fn store_for(
    test: &str,
    tweak: impl FnOnce(&mut RedisStoreConfig),
) -> Option<RedisMetadataStore> {
    let Some(server) = server() else {
        if redis_test_server::redis_required() {
            panic!(
                "AENV_REDIS_TEST_REQUIRED=1 but no redis-server could be started for {test}. \
                 Install redis-server, or point REDIS_SERVER_BIN at one."
            );
        }
        // 🔴 Loud, greppable and on stderr, so `make test-with-redis` can fail
        // on it and a human reading a log can see it.
        eprintln!("SKIPPED[redis]: {test} (no redis-server available)");
        return None;
    };

    let mut config = RedisStoreConfig {
        url: server.url(next_db()),
        // Short waits: these tests are about behaviour, not patience.
        poll_interval: Duration::from_millis(20),
        lock_wait: Duration::from_millis(500),
        ..Default::default()
    };
    tweak(&mut config);

    let store = RedisMetadataStore::connect(config)
        .await
        .expect("failed to connect the store to the test redis");
    store
        .flush_namespace()
        .await
        .expect("failed to clear the test key namespace");
    // Background rounds are exercised directly in these tests, so the
    // process-level warm-up is stepped over rather than waited out.
    store.inner().skip_background_warmup();
    Some(store)
}

/// Binds a store, or returns from the test having said so out loud.
macro_rules! store_or_skip {
    ($test:literal) => {
        store_or_skip!($test, |_config| {})
    };
    ($test:literal, $tweak:expr) => {
        match crate::orchestrator::store::redis::harness::store_for($test, $tweak).await {
            Some(store) => store,
            None => return,
        }
    };
}

pub(crate) use store_or_skip;

/// A raw connection to the same database, for assertions the store's own API
/// deliberately cannot make — `PTTL`, `ZSCORE`, key existence.
pub fn raw(store: &RedisMetadataStore) -> redis::aio::ConnectionManager {
    store.inner().connection()
}

/// A second store sharing the same Redis and the same key namespace.
///
/// 🔴 This is the only way to demonstrate anything about several replicas. A
/// single store instance cannot show that two of them stay out of each other's
/// way, however many tasks are run against it.
pub async fn sibling(store: &RedisMetadataStore) -> RedisMetadataStore {
    let sibling = RedisMetadataStore::connect(store.inner().config().clone())
        .await
        .expect("a second store should connect to the same redis");
    sibling.inner().skip_background_warmup();
    sibling
}
