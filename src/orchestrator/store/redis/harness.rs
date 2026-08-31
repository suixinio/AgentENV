//! Real-Redis test harness with visible optional skips.
//! This suite owns a separate server and database counter so sibling namespace
//! cleanup cannot interfere with its tests.

use std::sync::atomic::AtomicU32;
use std::sync::OnceLock;
use std::time::Duration;

use crate::redis_test_server::{self, RedisTestServer};

use super::{RedisMetadataStore, RedisStoreConfig};

// Dedicated logical database per test.
const DATABASES: u32 = 512;

/// This suite's dedicated Redis server.
pub(crate) fn server() -> Option<&'static RedisTestServer> {
    static SERVER: OnceLock<Option<RedisTestServer>> = OnceLock::new();
    SERVER
        .get_or_init(|| redis_test_server::start("redis-test-server", "the store tests", DATABASES))
        .as_ref()
}

/// This suite's dedicated logical-database counter.
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
        // Visible skip consumed by `make test-with-redis`.
        eprintln!("SKIPPED[redis]: {test} (no redis-server available)");
        return None;
    };

    let mut config = RedisStoreConfig {
        url: server.url(next_db()),
        // Short waits keep behavioral tests fast.
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
    // Tests invoke background rounds directly.
    store.inner().skip_background_warmup();
    Some(store)
}

/// Binds a store or returns after reporting a visible skip.
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

/// Raw connection for assertions outside the store API.
pub fn raw(store: &RedisMetadataStore) -> redis::aio::ConnectionManager {
    store.inner().connection()
}

/// Second store sharing this Redis namespace.
pub async fn sibling(store: &RedisMetadataStore) -> RedisMetadataStore {
    let sibling = RedisMetadataStore::connect(store.inner().config().clone())
        .await
        .expect("a second store should connect to the same redis");
    sibling.inner().skip_background_warmup();
    sibling
}
