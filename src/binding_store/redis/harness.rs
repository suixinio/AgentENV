//! Test support: a real Redis, and a policy about what happens when there
//! isn't one. Deliberately mirrors `src/orchestrator/store/redis/harness.rs`
//! (and `src/pg/harness.rs`, which mirrors that same one) down to the
//! skip/required policy — see `src/pg/harness.rs`'s own doc comment for why
//! duplicating this pattern per Redis-backed subsystem, rather than sharing
//! one generic module across all of them, is this codebase's established
//! choice: `AENV_REDIS_TEST_REQUIRED=1` turns "no Redis" into a failure, not
//! a skip; without it, `SKIPPED[redis]: <test>` prints to stderr and `make
//! test-with-redis` greps for that line and fails on it.

use std::io;
use std::net::TcpListener;
use std::os::unix::process::CommandExt;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use super::{RedisBindingStore, RedisBindingStoreConfig};
use crate::binding_store::BindingStoreSettings;

const DATABASES: u32 = 512;

pub(crate) struct RedisServer {
    port: u16,
}

impl RedisServer {
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
            let owner = std::thread::Builder::new()
                .name("binding-store-redis-test-server".to_string())
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
                    eprintln!(
                        "could not start a redis-server for the binding store tests: {error}"
                    );
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
        "ran out of logical databases for the binding store tests; raise DATABASES"
    );
    db
}

/// A store on its own database, or `None` when this machine has no Redis
/// and the run has not demanded one.
pub(crate) async fn store_for(
    test: &str,
    settings: BindingStoreSettings,
    tweak: impl FnOnce(&mut RedisBindingStoreConfig),
) -> Option<RedisBindingStore> {
    let Some(server) = server() else {
        if redis_required() {
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
