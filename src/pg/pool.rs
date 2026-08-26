//! A per-process PostgreSQL connection pool for the control plane
//! (`--role api` / `--role all`).
//!
//! `--role node` must never build one of these — see
//! [`crate::role::ServerRole::check_pg_dsn`], enforced in `src/bin/aenv-node.rs`
//! before any role-specific assembly runs.

use std::time::Duration;

use anyhow::{Context, Result};
use sqlx::postgres::{PgPool, PgPoolOptions};
use tracing::info;

use crate::cfg::PgConfig;

/// Per-replica pool cap used when `[pg].max_connections` is unset.
///
/// 🔴 `--role api` runs more than one replica, and every replica builds its
/// own pool independently — there is no cluster-wide coordination over how
/// many connections exist, only over how many *this process* opens. The
/// cluster-wide total this deployment produces is therefore
/// `replica_count * max_connections`, not this number alone, and it has to
/// stay comfortably under PostgreSQL's own `max_connections` (default 100)
/// with room for every replica plus whatever else already connects — the
/// Go scheduler's registry pool, migrations, `psql`, and so on. 8 mirrors
/// `services/scheduler/internal/registry/store_postgres.go`'s
/// `defaultStoreMaxConnections`, chosen there for a single-instance service;
/// it is more conservative here on purpose because this process is not
/// single-instance.
pub const DEFAULT_MAX_CONNECTIONS: u32 = 8;

/// How long [`connect`] waits for the first connection, and how long every
/// later `pool.acquire()` waits under saturation, when `[pg]` does not set
/// `connect_timeout_secs`.
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Resolved, ready-to-connect pool settings for one process's `[pg]` pool.
///
/// Built from [`crate::cfg::PgConfig`] by [`PgPoolSettings::from_config`]
/// rather than used directly, so every caller applies the same defaults —
/// `[pg]` is deserialized straight from TOML and leaves every field `None`
/// where the deployment did not set one.
#[derive(Debug, Clone)]
pub struct PgPoolSettings {
    /// A libpq-style connection URL. Never logged or included in an error
    /// message verbatim — see [`redact_dsn`].
    pub dsn: String,
    pub max_connections: u32,
    pub connect_timeout: Duration,
}

impl PgPoolSettings {
    /// `Ok(None)` when PostgreSQL is not configured for this process (no
    /// `[pg]` section, or a `dsn` that is absent or blank) — a legitimate,
    /// common state today, since nothing yet consumes this pool.
    pub fn from_config(pg: Option<&PgConfig>) -> Result<Option<Self>> {
        let Some(pg) = pg else {
            return Ok(None);
        };
        let Some(dsn) = pg.dsn() else {
            return Ok(None);
        };
        Ok(Some(Self {
            dsn: dsn.to_string(),
            max_connections: pg.max_connections.unwrap_or(DEFAULT_MAX_CONNECTIONS),
            connect_timeout: pg
                .connect_timeout_secs
                .map(Duration::from_secs)
                .unwrap_or(DEFAULT_CONNECT_TIMEOUT),
        }))
    }
}

/// Builds a pool and validates it can actually reach PostgreSQL before
/// returning, so a misconfigured or unreachable database is a startup
/// failure with an actionable message rather than a surprise on the first
/// request that needs one.
///
/// Uses `sqlx::PgPoolOptions::connect` (not `connect_lazy`), which opens and
/// tests at least one real connection as part of this call.
pub async fn connect(settings: &PgPoolSettings) -> Result<PgPool> {
    let redacted = redact_dsn(&settings.dsn);
    let pool = PgPoolOptions::new()
        .max_connections(settings.max_connections)
        .acquire_timeout(settings.connect_timeout)
        .connect(&settings.dsn)
        .await
        .with_context(|| {
            format!(
                "could not connect to PostgreSQL at {redacted} (pool max_connections=\
                 {max_connections}, connect_timeout={connect_timeout:?}). Check that the host \
                 is reachable from this replica, that the credentials in the [pg].dsn overlay \
                 file are current, that the named database exists, and that PostgreSQL's own \
                 max_connections comfortably exceeds replica_count * max_connections for every \
                 process connecting to it",
                max_connections = settings.max_connections,
                connect_timeout = settings.connect_timeout,
            )
        })?;
    info!(
        target: "agentenv",
        target_addr = %redacted,
        max_connections = settings.max_connections,
        "connected PostgreSQL pool"
    );
    Ok(pool)
}

/// `host:port/dbname` with the userinfo (username and password) stripped, for
/// safe use in logs and error messages. Falls back to a fixed placeholder
/// when `dsn` does not parse as a URL, rather than risking a credential
/// leaking through an un-parsed fallback.
pub fn redact_dsn(dsn: &str) -> String {
    match url::Url::parse(dsn) {
        Ok(url) => {
            let host = url.host_str().unwrap_or("<unknown-host>");
            let port = url
                .port()
                .map(|port| format!(":{port}"))
                .unwrap_or_default();
            let path = url.path().trim_start_matches('/');
            if path.is_empty() {
                format!("{host}{port}")
            } else {
                format!("{host}{port}/{path}")
            }
        }
        Err(_) => "<unparsable dsn>".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cfg::PgConfig;

    #[test]
    fn absent_pg_config_yields_no_settings() {
        assert!(PgPoolSettings::from_config(None).unwrap().is_none());
    }

    #[test]
    fn an_empty_or_blank_dsn_is_the_same_as_absent() {
        for dsn in [None, Some(""), Some("   ")] {
            let config = PgConfig {
                dsn: dsn.map(str::to_string),
                max_connections: None,
                connect_timeout_secs: None,
            };
            assert!(
                PgPoolSettings::from_config(Some(&config))
                    .unwrap()
                    .is_none(),
                "{dsn:?} should read as PostgreSQL not configured"
            );
        }
    }

    #[test]
    fn defaults_apply_when_unset() {
        let config = PgConfig {
            dsn: Some(" postgres://user:pw@db.internal:5432/agentenv ".to_string()),
            max_connections: None,
            connect_timeout_secs: None,
        };
        let settings = PgPoolSettings::from_config(Some(&config))
            .unwrap()
            .expect("dsn is present");
        assert_eq!(settings.dsn, "postgres://user:pw@db.internal:5432/agentenv");
        assert_eq!(settings.max_connections, DEFAULT_MAX_CONNECTIONS);
        assert_eq!(settings.connect_timeout, DEFAULT_CONNECT_TIMEOUT);
    }

    #[test]
    fn configured_values_override_defaults() {
        let config = PgConfig {
            dsn: Some("postgres://user:pw@db.internal:5432/agentenv".to_string()),
            max_connections: Some(3),
            connect_timeout_secs: Some(1),
        };
        let settings = PgPoolSettings::from_config(Some(&config))
            .unwrap()
            .expect("dsn is present");
        assert_eq!(settings.max_connections, 3);
        assert_eq!(settings.connect_timeout, Duration::from_secs(1));
    }

    /// The one property that actually matters for this helper: whatever goes
    /// in, the password never comes out.
    #[test]
    fn redact_dsn_never_reproduces_the_password() {
        let redacted = redact_dsn("postgres://api_user:hunter2@db.internal:5432/agentenv");
        assert!(!redacted.contains("hunter2"));
        assert!(!redacted.contains("api_user"));
        assert_eq!(redacted, "db.internal:5432/agentenv");
    }

    #[test]
    fn redact_dsn_falls_back_on_garbage_input() {
        assert_eq!(redact_dsn("not a url at all"), "<unparsable dsn>");
    }

    /// Against a real server: `connect` succeeds and the returned pool
    /// actually works.
    #[tokio::test]
    async fn connect_reaches_a_real_server() {
        let Some(dsn) = crate::pg::harness::dsn_for("connect_reaches_a_real_server") else {
            return;
        };
        let settings = PgPoolSettings {
            dsn,
            max_connections: 3,
            connect_timeout: Duration::from_secs(5),
        };
        let pool = connect(&settings).await.expect("connect should succeed");
        let answer: i32 = sqlx::query_scalar("SELECT 1")
            .fetch_one(&pool)
            .await
            .expect("a connected pool should be able to run a query");
        assert_eq!(answer, 1);
    }

    /// The startup failure this whole helper exists for: a DSN that cannot
    /// be reached has to fail fast (bounded by `connect_timeout`, not
    /// sqlx's own 30-second default) and say something a human can act on.
    #[tokio::test]
    async fn connect_to_an_unreachable_host_fails_fast_and_actionably() {
        // 203.0.113.0/24 is TEST-NET-3 (RFC 5737): reserved for documentation,
        // guaranteed to route nowhere, so this fails on a timeout rather than
        // an immediate "connection refused" that would race the timeout logic
        // this test means to cover.
        let settings = PgPoolSettings {
            dsn: "postgres://user:pw@203.0.113.1:5432/agentenv".to_string(),
            max_connections: 2,
            connect_timeout: Duration::from_millis(500),
        };
        let started = std::time::Instant::now();
        let error = connect(&settings)
            .await
            .expect_err("an unreachable host must not silently succeed");
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "connect should fail within roughly connect_timeout, took {:?}",
            started.elapsed()
        );
        let message = format!("{error:#}");
        assert!(
            message.contains("203.0.113.1"),
            "the error should name the unreachable host: {message}"
        );
        assert!(
            !message.contains("user:pw") && !message.contains("pw@"),
            "the error must never reproduce the DSN's credentials: {message}"
        );
    }
}
