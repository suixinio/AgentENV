//! Test support: a real, ephemeral PostgreSQL server, and a policy about what
//! happens when one cannot be started.
//!
//! Mirrors `src/orchestrator/store/redis/harness.rs` deliberately, down to
//! the skip/required policy: a real server, spawned once per test binary and
//! shared by every test in it, never a fake — the whole point of testing
//! [`crate::pg::election`] is the actual concurrency behaviour of
//! `pg_try_advisory_lock`/`pg_advisory_unlock` across real sessions, which a
//! fake would only agree with this code's own beliefs about.
//!
//! * `AENV_PG_TEST_REQUIRED=1` turns "no usable `initdb`/`postgres`" into a
//!   **failure**, not a skip.
//! * Without it, a skip prints a line beginning `SKIPPED[postgres]` to
//!   stderr — greppable, the same convention the Redis harness uses, so a
//!   skip that silently becomes permanent is something a make target can
//!   still catch.

use std::io;
use std::net::TcpListener;
use std::os::unix::process::CommandExt;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use tempfile::TempDir;

struct PgTestServer {
    port: u16,
}

impl PgTestServer {
    /// `postgres://postgres@127.0.0.1:<port>/postgres`. Trust auth, no
    /// password — this cluster exists only for the lifetime of this test
    /// binary, on a random localhost port, with unix sockets disabled.
    fn url(&self) -> String {
        format!("postgres://postgres@127.0.0.1:{}/postgres", self.port)
    }
}

/// Finds a PostgreSQL server-side binary (`initdb`, `postgres`): an explicit
/// override environment variable first, then `PATH`, then Debian/Ubuntu's
/// versioned `/usr/lib/postgresql/<version>/bin/` layout — `apt install
/// postgresql` puts binaries there and nowhere on `PATH` except the
/// `pg_wrapper`-based client tools.
fn find_bin(name: &str, env_override: &str) -> Option<PathBuf> {
    if let Ok(path) = std::env::var(env_override) {
        let path = PathBuf::from(path);
        if path.is_file() {
            return Some(path);
        }
    }
    if let Some(path) = on_path(name) {
        return Some(path);
    }
    let mut versions: Vec<PathBuf> = std::fs::read_dir("/usr/lib/postgresql")
        .into_iter()
        .flatten()
        .flatten()
        .map(|entry| entry.path())
        .collect();
    // Best-effort newest-first; any version with both binaries works equally
    // well for these tests.
    versions.sort();
    versions.reverse();
    versions
        .into_iter()
        .map(|dir| dir.join("bin").join(name))
        .find(|candidate| candidate.is_file())
}

fn on_path(name: &str) -> Option<PathBuf> {
    let path_var = std::env::var_os("PATH")?;
    std::env::split_paths(&path_var)
        .map(|dir| dir.join(name))
        .find(|candidate| candidate.is_file())
}

fn free_port() -> io::Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}

fn spawn_server() -> io::Result<(Child, u16, TempDir)> {
    let initdb = find_bin("initdb", "INITDB_BIN").ok_or_else(|| {
        io::Error::other(
            "initdb not found on PATH, at $INITDB_BIN, or under /usr/lib/postgresql/*/bin; \
             install postgresql",
        )
    })?;
    let postgres_bin = find_bin("postgres", "POSTGRES_BIN").ok_or_else(|| {
        io::Error::other(
            "the postgres server binary was not found on PATH, at $POSTGRES_BIN, or under \
             /usr/lib/postgresql/*/bin; install postgresql",
        )
    })?;

    let data_dir = tempfile::tempdir()?;
    let status = Command::new(&initdb)
        .arg("-D")
        .arg(data_dir.path())
        .arg("-U")
        .arg("postgres")
        .arg("--auth")
        .arg("trust")
        .arg("--no-sync")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()?;
    if !status.success() {
        return Err(io::Error::other(format!("initdb exited with {status}")));
    }

    let mut last_error = io::Error::other("no attempt made");
    for _ in 0..5 {
        let port = free_port()?;
        let mut command = Command::new(&postgres_bin);
        command
            .arg("-D")
            .arg(data_dir.path())
            .arg("-p")
            .arg(port.to_string())
            .arg("-c")
            .arg("listen_addresses=127.0.0.1")
            // Disables the unix socket entirely rather than pointing it at a
            // tempdir, whose path can exceed the kernel's ~100-byte socket
            // path limit; every test connects over TCP anyway.
            .arg("-c")
            .arg("unix_socket_directories=")
            .arg("-c")
            .arg("fsync=off")
            .arg("-c")
            .arg("full_page_writes=off")
            .arg("-c")
            .arg("synchronous_commit=off")
            .stdout(Stdio::null())
            .stderr(Stdio::null());

        // 🔴 Die with the test process, the same reasoning and the same
        // pitfall as the Redis harness: PR_SET_PDEATHSIG fires when the
        // spawning *thread* exits, not the process, so the spawn has to
        // happen from a thread that outlives every test — see `server`.
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
            Ok(()) => return Ok((child, port, data_dir)),
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                last_error = error;
            }
        }
    }
    Err(last_error)
}

fn wait_until_ready(child: &mut Child, port: u16) -> io::Result<()> {
    let pg_isready = on_path("pg_isready").unwrap_or_else(|| PathBuf::from("pg_isready"));
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut last = io::Error::other("server never became ready");
    while Instant::now() < deadline {
        if let Ok(Some(status)) = child.try_wait() {
            return Err(io::Error::other(format!(
                "the server process exited immediately with {status}"
            )));
        }
        match Command::new(&pg_isready)
            .arg("-h")
            .arg("127.0.0.1")
            .arg("-p")
            .arg(port.to_string())
            .arg("-U")
            .arg("postgres")
            .arg("-d")
            .arg("postgres")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
        {
            Ok(status) if status.success() => return Ok(()),
            Ok(status) => last = io::Error::other(format!("pg_isready reported {status}")),
            Err(error) => last = io::Error::other(error.to_string()),
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    Err(last)
}

fn server() -> Option<&'static PgTestServer> {
    static SERVER: OnceLock<Option<PgTestServer>> = OnceLock::new();
    SERVER
        .get_or_init(|| {
            let (ready_tx, ready_rx) = std::sync::mpsc::channel();
            // 🔴 A thread that owns the child (and the data directory) and
            // never returns — see the Redis harness's identical note on why
            // this has to be a thread that outlives every test.
            let owner = std::thread::Builder::new()
                .name("pg-test-server".to_string())
                .spawn(move || match spawn_server() {
                    Ok((child, port, data_dir)) => {
                        let _ = ready_tx.send(Ok(port));
                        let _child = child;
                        let _data_dir = data_dir;
                        loop {
                            std::thread::park();
                        }
                    }
                    Err(error) => {
                        let _ = ready_tx.send(Err(error.to_string()));
                    }
                });
            if let Err(error) = owner {
                eprintln!("could not start the postgres owner thread: {error}");
                return None;
            }
            match ready_rx.recv() {
                Ok(Ok(port)) => Some(PgTestServer { port }),
                Ok(Err(error)) => {
                    eprintln!("could not start a postgres server for the pg tests: {error}");
                    None
                }
                Err(error) => {
                    eprintln!("the postgres owner thread died before reporting: {error}");
                    None
                }
            }
        })
        .as_ref()
}

/// Whether a missing PostgreSQL is a failure rather than a skip.
fn pg_required() -> bool {
    std::env::var("AENV_PG_TEST_REQUIRED")
        .map(|value| value == "1" || value.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

/// A fresh advisory lock key for one test. Starts well above every real
/// [`super::AdvisoryLockKey`] variant and every digit of the Go
/// `schemaLockKey`'s magnitude, so a test can never collide with a real key
/// or with another test racing it on the same shared server.
pub(crate) fn next_test_lock_key() -> i64 {
    static NEXT: AtomicI64 = AtomicI64::new(1_000_000);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

/// The shared ephemeral test server's connection URL, or `None` when this
/// machine has no usable `initdb`/`postgres` and the run has not demanded
/// one.
pub(crate) fn dsn_for(test: &str) -> Option<String> {
    let Some(server) = server() else {
        if pg_required() {
            panic!(
                "AENV_PG_TEST_REQUIRED=1 but no postgres server could be started for {test}. \
                 Install postgresql (initdb/postgres), or point INITDB_BIN/POSTGRES_BIN at them."
            );
        }
        // 🔴 Loud, greppable and on stderr — mirrors SKIPPED[redis].
        eprintln!("SKIPPED[postgres]: {test} (no postgres server available)");
        return None;
    };
    Some(server.url())
}

/// A fresh pool over the shared ephemeral test server, or `None` under the
/// same conditions as [`dsn_for`]. Always a brand-new `PgPool`, never a
/// shared/cloned one — two calls simulate two independent replicas each
/// dialing the same database, which is what election tests need.
pub(crate) async fn pool_for(test: &str) -> Option<sqlx::PgPool> {
    let dsn = dsn_for(test)?;
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(5)
        .connect(&dsn)
        .await
        .unwrap_or_else(|error| panic!("failed to connect the pool to the test postgres: {error}"));
    Some(pool)
}

/// Binds a pool, or returns from the test having said so out loud.
macro_rules! pool_or_skip {
    ($test:literal) => {
        match crate::pg::harness::pool_for($test).await {
            Some(pool) => pool,
            None => return,
        }
    };
}

pub(crate) use pool_or_skip;

/// A pool scoped to a fresh, uniquely-named PostgreSQL schema on the shared
/// ephemeral test server, or `None` under the same conditions as
/// [`dsn_for`].
///
/// 🔴 Exists because [`pool_for`] is not enough for anything that runs real
/// DDL. The ephemeral server is one Postgres instance shared by every test in
/// this binary (see this module's own doc comment), all connecting to the
/// same literal `postgres` database — [`pool_for`] gives election tests their
/// own *pool*, but every pool still points at the same physical tables. Two
/// `#[tokio::test]` functions run concurrently by default, and a migration
/// test that creates `snapshots`/`aliases`/etc. races every other migration
/// test doing the same in the same schema — including one that drops them
/// (`the_documented_rollback_command_actually_rolls_back`), which is a
/// `relation "..." does not exist` away from failing a sibling test that
/// merely happened to run at the wrong moment. A private schema per test,
/// selected via `search_path` on every connection the pool hands out, gives
/// each test its own copy of every table name with no coordination between
/// tests required.
///
/// Every connection this pool ever opens carries the schema via
/// `after_connect`, not a per-transaction `SET LOCAL` — the schema has to
/// survive for the whole test, across however many connections the pool
/// borrows out over that time, not just one transaction.
pub(crate) async fn isolated_schema_pool(test: &str) -> Option<sqlx::PgPool> {
    let dsn = dsn_for(test)?;

    static NEXT: AtomicI64 = AtomicI64::new(0);
    let schema = format!(
        "aenv_test_{}_{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    );

    // A throwaway single-connection pool just to create the schema — the
    // real pool below assumes it already exists by the time its first
    // `after_connect` hook runs.
    let bootstrap = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&dsn)
        .await
        .unwrap_or_else(|error| panic!("failed to connect the bootstrap pool for {test}: {error}"));
    sqlx::query(&format!("CREATE SCHEMA \"{schema}\""))
        .execute(&bootstrap)
        .await
        .unwrap_or_else(|error| {
            panic!("failed to create test schema {schema} for {test}: {error}")
        });
    bootstrap.close().await;

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(5)
        .after_connect(move |conn, _meta| {
            let schema = schema.clone();
            Box::pin(async move {
                sqlx::query(&format!("SET search_path TO \"{schema}\""))
                    .execute(&mut *conn)
                    .await?;
                Ok(())
            })
        })
        .connect(&dsn)
        .await
        .unwrap_or_else(|error| {
            panic!("failed to connect the schema-scoped pool for {test}: {error}")
        });
    Some(pool)
}

/// Binds a schema-scoped pool, or returns from the test having said so out
/// loud. See [`isolated_schema_pool`].
macro_rules! isolated_schema_pool_or_skip {
    ($test:literal) => {
        match crate::pg::harness::isolated_schema_pool($test).await {
            Some(pool) => pool,
            None => return,
        }
    };
}

pub(crate) use isolated_schema_pool_or_skip;
