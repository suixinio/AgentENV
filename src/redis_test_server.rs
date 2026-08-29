//! Test support: starting a real `redis-server` for a test binary, and the
//! policy about what happens when there isn't one.
//!
//! # What this module is
//!
//! The *process bootstrap* every Redis-backed subsystem's harness needs, and
//! nothing else: pick a free port, spawn `redis-server` under
//! `PR_SET_PDEATHSIG`, wait for it to answer `PING`, hand back a
//! [`RedisTestServer`] that can build `redis://127.0.0.1:<port>/<db>` URLs —
//! plus [`redis_required`], the `AENV_REDIS_TEST_REQUIRED` predicate, and
//! [`next_db`], the bounds check on a caller-owned database counter.
//!
//! Every one of those was byte-identical in three harnesses, which meant a fix
//! to one copy — a readiness probe that hangs ten seconds on a binary that is
//! not a Redis, say — silently missed the other two.
//!
//! # 🔴 What this module deliberately is not
//!
//! It is **not** a shared Redis, and **not** a shared database allocator.
//! Each subsystem's harness still owns:
//!
//! * its own `OnceLock`, and therefore its own `redis-server` **process**.
//!   Three server processes during a test run is the intended behaviour. One
//!   shared server would put three suites' keyspaces in one process, where
//!   `orchestrator::store::redis::harness`'s `flush_namespace()` would be
//!   reaching into databases the other two suites are mid-test on;
//! * its own owner thread, which holds the `Child` and never returns. That is
//!   what makes `PR_SET_PDEATHSIG` mean "when this *process* ends" rather than
//!   "when whichever test ran first ends" — see [`start`];
//! * its own `static NEXT: AtomicU32` and its own `const DATABASES: u32 = 512`,
//!   so three independent 512-database spaces. One shared allocator would let
//!   two suites hand out the same logical database number, which surfaces as
//!   cross-suite flakiness that is very hard to attribute to its cause.
//!
//! [`tests`] guards both of those properties by name, so that a future
//! "simplification" into one server or one counter turns red instead of
//! turning into intermittent, unattributable failures. That guard is the
//! reason this module can exist at all.
//!
//! # The skip policy
//!
//! The Go half of this repository shipped a make target that silently skipped
//! 152 tests and reported green. So:
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
use std::time::{Duration, Instant};

/// A `redis-server` started for one test binary, owned by one subsystem's
/// harness.
pub struct RedisTestServer {
    port: u16,
}

impl RedisTestServer {
    /// `redis://127.0.0.1:<port>/<db>`.
    ///
    /// 🔴 A database, not a key prefix. Prefixing would change the `{global}`
    /// hash tag's position in every key, and the hash tag is one of the things
    /// under test.
    pub fn url(&self, db: u32) -> String {
        format!("redis://127.0.0.1:{}/{db}", self.port)
    }

    /// The port this server is listening on. Two harnesses reporting the same
    /// port would mean they had stopped owning separate processes; see
    /// [`tests::the_three_redis_harnesses_own_distinct_redis_server_instances`].
    pub fn port(&self) -> u16 {
        self.port
    }
}

/// Starts a `redis-server` owned by a thread that never returns, or reports
/// why it could not on stderr and returns `None`.
///
/// 🔴 Call this exactly once per subsystem, from that subsystem's own
/// `OnceLock`. It is not idempotent and it is not shared state: every call
/// spawns another server and another owner thread.
///
/// `thread_name` names the owner thread and `subsystem` names the suite in the
/// failure line, so a machine with a broken `redis-server` says which harness
/// could not start one.
pub fn start(thread_name: &str, subsystem: &str, databases: u32) -> Option<RedisTestServer> {
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    // 🔴 A thread that owns the child and never returns. It is what makes
    // `PR_SET_PDEATHSIG` mean "when this process ends" rather than "when
    // whichever test ran first ends", and it is also what keeps the `Child`
    // handle from being dropped.
    let owner = std::thread::Builder::new()
        .name(thread_name.to_string())
        .spawn(move || match spawn_server(databases) {
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
        Ok(Ok(port)) => Some(RedisTestServer { port }),
        Ok(Err(error)) => {
            eprintln!("could not start a redis-server for {subsystem}: {error}");
            None
        }
        Err(error) => {
            eprintln!("the redis owner thread died before reporting: {error}");
            None
        }
    }
}

fn spawn_server(databases: u32) -> io::Result<(Child, u16)> {
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
            .arg(databases.to_string())
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
        // therefore a thread that never returns; see `start`.
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

/// Whether a missing Redis is a failure rather than a skip.
pub fn redis_required() -> bool {
    std::env::var("AENV_REDIS_TEST_REQUIRED")
        .map(|value| value == "1" || value.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

/// Takes the next logical database out of **the caller's own** counter.
///
/// 🔴 `counter` is borrowed, not owned by this module, and that is the whole
/// point: each harness passes its own `static NEXT`, so the three subsystems
/// never hand out the same database number for concurrent tests. Passing a
/// counter that another harness also passes reintroduces exactly the collision
/// this shape exists to prevent — [`tests`] fails when that happens.
///
/// `tests` names the suite in the panic, which is what tells whoever hits the
/// bound which `DATABASES` to raise.
pub fn next_db(counter: &AtomicU32, databases: u32, tests: &str) -> u32 {
    let db = counter.fetch_add(1, Ordering::Relaxed);
    assert!(
        db < databases,
        "ran out of logical databases for {tests}; raise DATABASES"
    );
    db
}

/// 🔴 The guard on the separation this module documents.
///
/// Extracting the process bootstrap makes "and now share the server too, and
/// the counter" look like the obvious next step. It is not: see this module's
/// own doc. These two tests are what makes taking that step fail loudly rather
/// than produce cross-suite flakiness nobody can attribute.
#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};

    use super::{next_db, redis_required, RedisTestServer};

    /// Every Redis-backed subsystem's harness, by module path, with the two
    /// things that must not be shared between them.
    ///
    /// A new Redis-backed subsystem belongs in this list; that is the only
    /// maintenance these guards need.
    fn harnesses() -> [(
        &'static str,
        &'static AtomicU32,
        Option<&'static RedisTestServer>,
    ); 3] {
        [
            (
                "orchestrator::store::redis",
                crate::orchestrator::store::redis::harness::db_counter(),
                crate::orchestrator::store::redis::harness::server(),
            ),
            (
                "binding_store::redis",
                crate::binding_store::redis::harness::db_counter(),
                crate::binding_store::redis::harness::server(),
            ),
            (
                "node_registry::redis",
                crate::node_registry::redis::harness::db_counter(),
                crate::node_registry::redis::harness::server(),
            ),
        ]
    }

    /// 🔴 Each harness allocates logical databases out of its own counter.
    ///
    /// Stated as pointer identity rather than as "bump one, watch the other
    /// stay put", because the other suites in this binary are calling
    /// `next_db` on all three counters concurrently while this test runs — a
    /// value-based assertion here would be flaky in exactly the direction that
    /// teaches people to delete it. Two harnesses passing one `static NEXT` is
    /// the defect, and pointer identity is that defect stated exactly.
    #[test]
    fn the_three_redis_harnesses_allocate_logical_databases_from_independent_counters() {
        let harnesses = harnesses();
        for (index, (name, counter, _)) in harnesses.iter().enumerate() {
            for (other_name, other_counter, _) in harnesses.iter().skip(index + 1) {
                assert!(
                    !std::ptr::eq(*counter, *other_counter),
                    "{name} and {other_name} allocate logical databases from one shared \
                     counter. They must not: three independent 512-database spaces is what \
                     stops two suites' concurrent tests landing on the same logical database, \
                     which surfaces as cross-suite flakiness nobody can attribute. Give each \
                     harness back its own `static NEXT: AtomicU32`."
                );
            }
        }
    }

    /// And the counters really are independent once they are distinct: this is
    /// the value-based half of the property above, run on counters this test
    /// owns so that nothing else in the binary can touch them.
    #[test]
    fn allocating_from_one_counter_does_not_advance_another() {
        const DATABASES: u32 = 512;
        let mine = AtomicU32::new(1);
        let yours = AtomicU32::new(1);

        assert_eq!(next_db(&mine, DATABASES, "mine"), 1);
        assert_eq!(next_db(&mine, DATABASES, "mine"), 2);
        assert_eq!(
            yours.load(Ordering::Relaxed),
            1,
            "allocating twice from one counter advanced another"
        );
        assert_eq!(next_db(&yours, DATABASES, "yours"), 1);
    }

    /// 🔴 Each harness owns its own `redis-server` process.
    ///
    /// Three processes during a test run is intended. Sharing one would put
    /// three suites' keyspaces in one server, where the orchestrator store's
    /// `flush_namespace()` reaches into databases the other two are mid-test
    /// on.
    #[test]
    fn the_three_redis_harnesses_own_distinct_redis_server_instances() {
        let harnesses = harnesses();
        let mut started = Vec::new();
        for (name, _, server) in harnesses {
            let Some(server) = server else {
                if redis_required() {
                    panic!(
                        "AENV_REDIS_TEST_REQUIRED=1 but no redis-server could be started for \
                         {name}'s harness. Install redis-server, or point REDIS_SERVER_BIN at \
                         one."
                    );
                }
                eprintln!(
                    "SKIPPED[redis]: the_three_redis_harnesses_own_distinct_redis_server_instances \
                     (no redis-server available)"
                );
                return;
            };
            started.push((name, server));
        }

        for (index, (name, server)) in started.iter().enumerate() {
            for (other_name, other_server) in started.iter().skip(index + 1) {
                assert_ne!(
                    server.port(),
                    other_server.port(),
                    "{name} and {other_name} are talking to one shared redis-server. They must \
                     not: each harness owns its own process, so that one suite flushing its key \
                     namespace cannot reach a database another suite is mid-test on."
                );
                assert_ne!(server.url(7), other_server.url(7));
            }
        }
    }
}
