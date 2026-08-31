//! Real-Redis test harness with visible optional skips.
//! This suite owns a separate server and database counter so sibling namespace
//! cleanup cannot interfere with its tests.

use std::sync::atomic::AtomicU32;
use std::sync::OnceLock;

use crate::redis_test_server::{self, RedisTestServer};

use super::{RedisBindingStore, RedisBindingStoreConfig};
use crate::binding_store::BindingStoreSettings;

const DATABASES: u32 = 512;

/// This suite's dedicated Redis server.
pub(crate) fn server() -> Option<&'static RedisTestServer> {
    static SERVER: OnceLock<Option<RedisTestServer>> = OnceLock::new();
    SERVER
        .get_or_init(|| {
            redis_test_server::start(
                "binding-store-redis-test-server",
                "the binding store tests",
                DATABASES,
            )
        })
        .as_ref()
}

/// This suite's dedicated logical-database counter.
pub(crate) fn db_counter() -> &'static AtomicU32 {
    static NEXT: AtomicU32 = AtomicU32::new(1);
    &NEXT
}

fn next_db() -> u32 {
    redis_test_server::next_db(db_counter(), DATABASES, "the binding store tests")
}

/// A store on its own database, or `None` when this machine has no Redis
/// and the run has not demanded one.
pub async fn store_for(
    test: &str,
    settings: BindingStoreSettings,
    tweak: impl FnOnce(&mut RedisBindingStoreConfig),
) -> Option<RedisBindingStore> {
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

    let mut config = RedisBindingStoreConfig {
        url: server.url(next_db()),
        ..Default::default()
    };
    tweak(&mut config);

    let store = RedisBindingStore::connect(config, settings)
        .await
        .expect("failed to connect the binding store to the test redis");
    Some(store)
}

macro_rules! store_or_skip {
    ($test:literal, $settings:expr) => {
        store_or_skip!($test, $settings, |_config| {})
    };
    ($test:literal, $settings:expr, $tweak:expr) => {
        match crate::binding_store::redis::harness::store_for($test, $settings, $tweak).await {
            Some(store) => store,
            None => return,
        }
    };
}

pub(crate) use store_or_skip;
