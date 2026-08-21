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

use std::io;
use std::net::TcpListener;
use std::os::unix::process::CommandExt;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use super::{RedisMetadataStore, RedisStoreConfig};

/// How many logical databases the spawned server offers, one per test.
const DATABASES: u32 = 512;

pub(crate) struct RedisServer {
    port: u16,
}

impl RedisServer {
    /// `redis://127.0.0.1:<port>/<db>`.
    ///
    /// 🔴 A database, not a key prefix. Prefixing would change the `{global}`
    /// hash tag's position in every key, and the hash tag is one of the things
    /// under test.
    fn url(&self, db: u32) -> String {
        format!("redis://127.0.0.1:{}/{db}", self.port)
    }
}

fn spawn_server() -> io::Result<(Child, u16)> {
    let binary = std::env::var("REDIS_SERVER_BIN").unwrap_or_else(|_| "redis-server".to_string());

    let mut last_error = io::Error::other("no attempt made");
    for _ in 0..5 {
        let port = free_port()?;
        let mut command = Command::new(&binary);
        command
            .arg("--port")
            .arg(port.to_string())
            .arg("--bind")
            .arg("127.0.0.1")
            .arg("--save")
            .arg("")
            .arg("--appendonly")
            .arg("no")
            .arg("--databases")
            .arg(DATABASES.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::null());

        // 🔴 Die with the test process. A test binary that panics or is killed
        // must not leave a Redis behind; over a few dozen runs that is a
        // machine full of orphaned servers.
        //
        // 🔴 And note *which* thread does the spawning. `PR_SET_PDEATHSIG`
        // fires when the creating **thread** exits, not when the process does.
        // Spawning from whichever test thread happened to be first killed the
        // server the moment that thread finished its test — 27 tests passed and
        // the remaining 57 all failed with "connection refused". The owner is
        // therefore a thread that never returns; see `server`.
        unsafe {
            command.pre_exec(|| {
                if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) != 0 {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            });
        }

        let mut child = match command.spawn() {
            Ok(child) => child,
            Err(error) => {
                last_error = error;
                continue;
            }
        };
        match wait_until_ready(&mut child, port) {
            Ok(()) => return Ok((child, port)),
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                last_error = error;
            }
        }
    }
    Err(last_error)
}

fn free_port() -> io::Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}

fn wait_until_ready(child: &mut Child, port: u16) -> io::Result<()> {
    let client = redis::Client::open(format!("redis://127.0.0.1:{port}"))
        .map_err(|error| io::Error::other(error.to_string()))?;
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut last = io::Error::other("server never became ready");
    while Instant::now() < deadline {
        // A binary that is not a Redis exits at once; waiting ten seconds for
        // it turns a clear failure into a slow one.
        if let Ok(Some(status)) = child.try_wait() {
            return Err(io::Error::other(format!(
                "the server process exited immediately with {status}"
            )));
        }
        match client.get_connection() {
            Ok(mut connection) => {
                let pong: redis::RedisResult<String> = redis::cmd("PING").query(&mut connection);
                if pong.is_ok() {
                    return Ok(());
                }
                last = io::Error::other("PING failed");
            }
            Err(error) => last = io::Error::other(error.to_string()),
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    Err(last)
}

fn server() -> Option<&'static RedisServer> {
    static SERVER: OnceLock<Option<RedisServer>> = OnceLock::new();
    SERVER
        .get_or_init(|| {
            let (ready_tx, ready_rx) = std::sync::mpsc::channel();
            // 🔴 A thread that owns the child and never returns. It is what
            // makes `PR_SET_PDEATHSIG` mean "when this process ends" rather
            // than "when whichever test ran first ends", and it is also what
            // keeps the `Child` handle from being dropped.
            let owner = std::thread::Builder::new()
                .name("redis-test-server".to_string())
                .spawn(move || match spawn_server() {
                    Ok((child, port)) => {
                        let _ = ready_tx.send(Ok(port));
                        let _child = child;
                        loop {
                            std::thread::park();
                        }
                    }
                    Err(error) => {
                        let _ = ready_tx.send(Err(error.to_string()));
                    }
                });
            if let Err(error) = owner {
                eprintln!("could not start the redis owner thread: {error}");
                return None;
            }
            match ready_rx.recv() {
                Ok(Ok(port)) => Some(RedisServer { port }),
                Ok(Err(error)) => {
                    eprintln!("could not start a redis-server for the store tests: {error}");
                    None
                }
                Err(error) => {
                    eprintln!("the redis owner thread died before reporting: {error}");
                    None
                }
            }
        })
        .as_ref()
}

/// Whether a missing Redis is a failure rather than a skip.
fn redis_required() -> bool {
    std::env::var("AENV_REDIS_TEST_REQUIRED")
        .map(|value| value == "1" || value.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

fn next_db() -> u32 {
    static NEXT: AtomicU32 = AtomicU32::new(1);
    let db = NEXT.fetch_add(1, Ordering::Relaxed);
    assert!(
        db < DATABASES,
        "ran out of logical databases for the store tests; raise DATABASES"
    );
    db
}

/// A store on its own database, or `None` when this machine has no Redis and
/// the run has not demanded one.
pub(crate) async fn store_for(
    test: &str,
    tweak: impl FnOnce(&mut RedisStoreConfig),
) -> Option<RedisMetadataStore> {
    let Some(server) = server() else {
        if redis_required() {
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
pub(crate) fn raw(store: &RedisMetadataStore) -> redis::aio::ConnectionManager {
    store.inner().connection()
}

/// A second store sharing the same Redis and the same key namespace.
///
/// 🔴 This is the only way to demonstrate anything about several replicas. A
/// single store instance cannot show that two of them stay out of each other's
/// way, however many tasks are run against it.
pub(crate) async fn sibling(store: &RedisMetadataStore) -> RedisMetadataStore {
    let sibling = RedisMetadataStore::connect(store.inner().config().clone())
        .await
        .expect("a second store should connect to the same redis");
    sibling.inner().skip_background_warmup();
    sibling
}
