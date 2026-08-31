//! Real `redis-server` bootstrap for Redis-backed test harnesses.
//!
//! Each subsystem owns a separate server process, database counter, and owner
//! thread. Missing Redis is visible as `SKIPPED[redis]`, or fatal when
//! `AENV_REDIS_TEST_REQUIRED=1`.

use std::io;
use std::net::TcpListener;
use std::os::unix::process::CommandExt;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

/// Redis server owned by one subsystem's test harness.
pub struct RedisTestServer {
    port: u16,
}

impl RedisTestServer {
    /// Builds a URL selecting one logical Redis database.
    pub fn url(&self, db: u32) -> String {
        format!("redis://127.0.0.1:{}/{db}", self.port)
    }

    /// Returns the distinct server port for this harness.
    pub fn port(&self) -> u16 {
        self.port
    }
}

/// Starts one Redis server and permanent owner thread for a subsystem harness.
pub fn start(thread_name: &str, subsystem: &str, databases: u32) -> Option<RedisTestServer> {
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    // The permanent thread retains the child handle until the test process exits.
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

        // `PR_SET_PDEATHSIG` follows the spawning thread, so use the owner thread.
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
        // Fail promptly when the configured binary exits immediately.
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

/// Allocates the next logical database from the caller-owned counter.
pub fn next_db(counter: &AtomicU32, databases: u32, tests: &str) -> u32 {
    let db = counter.fetch_add(1, Ordering::Relaxed);
    assert!(
        db < databases,
        "ran out of logical databases for {tests}; raise DATABASES"
    );
    db
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};

    use super::{next_db, redis_required, RedisTestServer};

    /// Redis subsystem harnesses and their independently owned resources.
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
