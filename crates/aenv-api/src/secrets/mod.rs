//! The api half's secrets: names and versions in PostgreSQL, values in the
//! same database, encrypted under a key it reads from a file.

pub mod envelope;
pub mod pg;

use std::sync::Arc;

use aenv_core::cfg::{AppConfig, SecretsBackendKind};
use aenv_core::secrets::SecretsService;
use anyhow::{Context, Result};
use sqlx::PgPool;
use zeroize::Zeroizing;

pub use envelope::Envelope;
pub use pg::values::PgSecretValues;
pub use pg::PgSecretRefStore;

/// What `[secrets]` assembled: the `/secrets` service, the values half the
/// resolve endpoint reads through, and the bearer that endpoint checks.
pub struct SecretsAssembly {
    pub service: Arc<SecretsService>,
    pub values: Arc<PgSecretValues>,
    pub resolver_token: Zeroizing<String>,
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
            // Read at assembly so a missing or unreadable token fails the
            // process rather than every resolve once it is serving.
            let token_file = pg
                .resolver_token_file
                .as_deref()
                .context("secrets.pg.resolver_token_file is required")?;
            let token = Zeroizing::new(
                std::fs::read_to_string(token_file)
                    .with_context(|| format!("read secrets.pg.resolver_token_file {token_file:?}"))?
                    .trim()
                    .to_string(),
            );
            if token.is_empty() {
                anyhow::bail!("secrets.pg.resolver_token_file {token_file:?} is empty");
            }
            let values = Arc::new(PgSecretValues::new(pool.clone(), envelope));
            Ok(Some(SecretsAssembly {
                service: Arc::new(SecretsService::new(
                    Arc::new(PgSecretRefStore::new(pool.clone())),
                    Arc::clone(&values) as Arc<dyn aenv_core::secrets::SecretsBackend>,
                )),
                values,
                resolver_token: token,
            }))
        }
    }
}
