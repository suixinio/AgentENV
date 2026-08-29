//! Test support: a real Redis, and a policy about what happens when there
//! isn't one. Deliberately mirrors `src/orchestrator/store/redis/harness.rs`
//! (and `crates/aenv-api/src/pg/harness.rs`, which mirrors that same one) down
//! to the skip/required policy — see `crates/aenv-api/src/pg/harness.rs`'s own
//! doc comment for why duplicating this pattern per Redis-backed subsystem,
//! rather than sharing one generic module across all of them, is this
//! codebase's established choice: `AENV_REDIS_TEST_REQUIRED=1` turns "no
//! Redis" into a failure, not a skip; without it, `SKIPPED[redis]: <test>`
//! prints to stderr and `make test-with-redis` greps for that line and fails
//! on it.
//!
//! # What is shared with the other Redis harnesses, and what is not
//!
//! Shared, via [`crate::redis_test_server`]: the mechanical process
//! bootstrap only — free port, `redis-server` spawn under `PR_SET_PDEATHSIG`,
//! the readiness probe, `redis://…/<db>` URL construction, the
//! `AENV_REDIS_TEST_REQUIRED` predicate, and the bounds check on a database
//! counter.
//!
//! 🔴 **Not** shared, deliberately: the [`OnceLock`] below and therefore this
//! suite's own `redis-server` process; `NEXT`, its own counter over its own
//! [`DATABASES`] logical databases; and [`store_for`], which takes a
//! [`BindingStoreSettings`] and a [`RedisBindingStoreConfig`] tweak no other
//! harness has. One shared server would let another suite's namespace flush
//! reach into a database this one is mid-test on; one shared counter would
//! hand two suites the same database number.
//! `crate::redis_test_server::tests` fails by name if either separation is
//! undone.

use std::sync::atomic::AtomicU32;
use std::sync::OnceLock;

use crate::redis_test_server::{self, RedisTestServer};

use super::{RedisBindingStore, RedisBindingStoreConfig};
use crate::binding_store::BindingStoreSettings;

/// 🔴 This suite's own space, not a shared one — see the module doc.
const DATABASES: u32 = 512;

/// 🔴 This suite's own `redis-server`, started once for this test binary.
/// Deliberately not shared with `orchestrator::store` or `node_registry`.
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

/// 🔴 This suite's own logical-database counter. Handed to
/// [`redis_test_server::next_db`] by reference precisely so that it stays this
/// suite's; see `crate::redis_test_server::tests`.
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
