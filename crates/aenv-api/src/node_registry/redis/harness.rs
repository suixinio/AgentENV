//! Real-Redis test harness with one server and logical-database counter for this suite.
//!
//! `AENV_REDIS_TEST_REQUIRED=1` makes unavailable Redis fail; otherwise tests emit
//! `SKIPPED[redis]`. Other Redis-backed suites must not share this server or counter.

use std::sync::atomic::AtomicU32;
use std::sync::OnceLock;

use aenv_core::redis_test_server::{self, RedisTestServer};

use super::{SharedObservedStore, SharedObservedStoreConfig, DEFAULT_KEY_PREFIX};

/// Logical databases reserved for this suite.
const DATABASES: u32 = 512;

/// Starts this suite's Redis server once per test binary.
pub(crate) fn server() -> Option<&'static RedisTestServer> {
    static SERVER: OnceLock<Option<RedisTestServer>> = OnceLock::new();
    SERVER
        .get_or_init(|| {
            redis_test_server::start(
                "redis-node-registry-test-server",
                "the node registry tests",
                DATABASES,
            )
        })
        .as_ref()
}

/// Returns this suite's logical-database counter.
pub(crate) fn db_counter() -> &'static AtomicU32 {
    static NEXT: AtomicU32 = AtomicU32::new(1);
    &NEXT
}

fn next_db() -> u32 {
    redis_test_server::next_db(db_counter(), DATABASES, "the node registry redis tests")
}

/// Creates an isolated store or skips when optional Redis is unavailable.
pub async fn store_for(test: &str) -> Option<SharedObservedStore> {
    let Some(server) = server() else {
        if redis_test_server::redis_required() {
            panic!(
                "AENV_REDIS_TEST_REQUIRED=1 but no redis-server could be started for {test}. \
                 Install redis-server, or point REDIS_SERVER_BIN at one."
            );
        }
        eprintln!("SKIPPED[redis]: {test} (no redis-server available)");
        return None;
    };

    let config = SharedObservedStoreConfig {
        url: server.url(next_db()),
        key_prefix: DEFAULT_KEY_PREFIX.to_string(),
        ..Default::default()
    };
    let store = SharedObservedStore::connect(config)
        .await
        .expect("failed to connect the shared observed store to the test redis");
    Some(store)
}

/// Returns raw access to the hot hash for storage-level assertions.
pub fn raw(store: &SharedObservedStore) -> (redis::aio::ConnectionManager, String) {
    (store.connection.clone(), store.hash_key.clone())
}

/// Returns raw access to the machine-info side hash.
pub fn raw_machine(store: &SharedObservedStore) -> (redis::aio::ConnectionManager, String) {
    (store.connection.clone(), store.machine_hash_key.clone())
}

macro_rules! store_or_skip {
    ($test:literal) => {
        match crate::node_registry::redis::harness::store_for($test).await {
            Some(store) => store,
            None => return,
        }
    };
}

pub(crate) use store_or_skip;
