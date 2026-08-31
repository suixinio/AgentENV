//! Per-process PostgreSQL pool for control-plane services only.

use std::time::Duration;

use anyhow::{Context, Result};
use sqlx::postgres::{PgPool, PgPoolOptions};
use tracing::info;

use crate::cfg::PgConfig;

/// Default per-replica pool cap.
///
/// Deployment capacity is `replicas * max_connections`.
pub const DEFAULT_MAX_CONNECTIONS: u32 = 8;

/// Default connection and acquire timeout.
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Resolved settings for one process's PostgreSQL pool.
#[derive(Debug, Clone)]
pub struct PgPoolSettings {
    /// Connection URL; never log it without [`redact_dsn`].
    pub dsn: String,
    pub max_connections: u32,
    pub connect_timeout: Duration,
}

impl PgPoolSettings {
    /// Returns `None` when the `[pg]` section or DSN is absent or blank.
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

/// Connects eagerly and fails startup when PostgreSQL is unreachable.
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

/// Removes userinfo from a DSN for safe diagnostics.
///
/// Unparseable input returns a fixed placeholder rather than leaking text.
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

    #[tokio::test]
    async fn connect_to_an_unreachable_host_fails_fast_and_actionably() {
        // TEST-NET-3 forces a timeout rather than connection refusal.
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
