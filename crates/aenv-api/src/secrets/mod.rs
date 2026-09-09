//! The api half's secrets: names and versions in PostgreSQL, values in the
//! same database, encrypted under a key it reads from a file.

pub mod envelope;
pub mod pg;
pub mod service;

pub use service::*;

use std::sync::Arc;

use aenv_core::cfg::{AppConfig, SecretsBackendKind};
use anyhow::{Context, Result};
use sqlx::PgPool;

pub use envelope::Envelope;
pub use pg::values::PgSecretValues;
pub use pg::PgSecretRefStore;

/// What `[secrets]` assembled: the `/secrets` service and the values half the
/// resolve endpoint reads through.
pub struct SecretsAssembly {
    pub service: Arc<SecretsService>,
    pub values: Arc<PgSecretValues>,
}

/// Builds what `[secrets]` describes, or `None` when the backend is disabled
/// and `/secrets` must answer 503.
pub fn build_secrets_service(config: &AppConfig, pool: &PgPool) -> Result<Option<SecretsAssembly>> {
    match config.secrets.backend {
        SecretsBackendKind::Disabled => Ok(None),
        SecretsBackendKind::Postgres => {
            let pg = &config.secrets.pg;
            let key_file = pg
                .key_file
                .as_deref()
                .context("secrets.pg.key_file is required")?;
            let envelope = Envelope::from_key_file(key_file)
                .context("configure the PostgreSQL secrets backend")?;
            let values = Arc::new(PgSecretValues::new(pool.clone(), envelope));
            Ok(Some(SecretsAssembly {
                service: Arc::new(SecretsService::new(
                    Arc::new(PgSecretRefStore::new(pool.clone())),
                    Arc::clone(&values) as Arc<dyn SecretsBackend>,
                )),
                values,
            }))
        }
    }
}
