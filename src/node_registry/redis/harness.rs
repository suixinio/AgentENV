//! Test support: a real Redis, and a policy about what happens when there
//! isn't one.
//!
//! Deliberately mirrors `src/orchestrator/store/redis/harness.rs` (and
//! `src/binding_store/redis/harness.rs`, which mirrors that same one) down
//! to the skip/required policy — see `crates/aenv-api/src/pg/harness.rs`'s own
//! doc comment for why duplicating this pattern per Redis-backed subsystem,
//! rather than sharing one generic module across all of them, is this
//! codebase's established choice.
//!
//! * `AENV_REDIS_TEST_REQUIRED=1` turns "no Redis" into a **failure**, not a
//!   skip. `make test-with-redis` sets it.
//! * Without it, a skip prints a line beginning `SKIPPED[redis]` to stderr,
//!   and the make target greps for that line and fails if it finds one.
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
//! [`DATABASES`] logical databases; and [`store_for`], [`raw`] and
//! [`raw_machine`], which are shaped around
//! [`SharedObservedStore`]'s two hashes and nothing else's. One shared server
//! would let another suite's namespace flush reach into a database this one is
//! mid-test on; one shared counter would hand two suites the same database
//! number. `crate::redis_test_server::tests` fails by name if either
//! separation is undone.

use std::sync::atomic::AtomicU32;
use std::sync::OnceLock;

use crate::redis_test_server::{self, RedisTestServer};

use super::{SharedObservedStore, SharedObservedStoreConfig, DEFAULT_KEY_PREFIX};

/// How many logical databases the spawned server offers, one per test.
///
/// 🔴 This suite's own space, not a shared one — see the module doc.
const DATABASES: u32 = 512;

/// 🔴 This suite's own `redis-server`, started once for this test binary.
/// Deliberately not shared with `orchestrator::store` or `binding_store`.
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

/// 🔴 This suite's own logical-database counter. Handed to
/// [`redis_test_server::next_db`] by reference precisely so that it stays this
/// suite's; see `crate::redis_test_server::tests`.
pub(crate) fn db_counter() -> &'static AtomicU32 {
    static NEXT: AtomicU32 = AtomicU32::new(1);
    &NEXT
}

fn next_db() -> u32 {
    redis_test_server::next_db(db_counter(), DATABASES, "the node registry redis tests")
}

/// A store on its own database, or `None` when this machine has no Redis and
/// the run has not demanded one.
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

/// A raw connection to the same database, plus the hash key it uses, for
/// assertions `SharedObservedStore`'s own API deliberately cannot make
/// (`HLEN`, a direct `HGET`, "this field is truly gone").
pub fn raw(store: &SharedObservedStore) -> (redis::aio::ConnectionManager, String) {
    (store.connection.clone(), store.hash_key.clone())
}

/// Same as [`raw`], but for the machine-info side hash — see
/// `super::machine_hash_key`'s own doc.
pub fn raw_machine(store: &SharedObservedStore) -> (redis::aio::ConnectionManager, String) {
    (store.connection.clone(), store.machine_hash_key.clone())
}

/// Binds a store, or returns from the test having said so out loud.
macro_rules! store_or_skip {
    ($test:literal) => {
        match crate::node_registry::redis::harness::store_for($test).await {
            Some(store) => store,
            None => return,
        }
    };
}

pub(crate) use store_or_skip;
