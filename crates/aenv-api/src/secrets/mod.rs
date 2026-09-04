//! The api half's secrets: names and versions in PostgreSQL, values and
//! grants in Vault or in an operator-run resolver. Nothing in this crate
//! reads a value back.

pub mod pg;
pub mod resolver;
pub mod vault;

use std::sync::Arc;

use aenv_core::cfg::{AppConfig, SecretsBackendKind};
use aenv_core::secrets::SecretsService;
use anyhow::{Context, Result};
use sqlx::PgPool;

pub use pg::PgSecretRefStore;
pub use resolver::ExternalResolverBackend;
pub use vault::VaultKv2Backend;

/// Builds the service `[secrets]` describes, or `None` when the backend is
/// disabled and `/secrets` must answer 503.
pub fn build_secrets_service(
    config: &AppConfig,
    pool: &PgPool,
) -> Result<Option<Arc<SecretsService>>> {
    match config.secrets.backend {
        SecretsBackendKind::Disabled => Ok(None),
        SecretsBackendKind::Vault => {
            let vault = VaultKv2Backend::from_config(&config.secrets.vault)
                .context("configure the Vault secrets backend")?;
            Ok(Some(Arc::new(SecretsService::new(
                Arc::new(PgSecretRefStore::new(pool.clone())),
                Arc::new(vault),
            ))))
        }
        SecretsBackendKind::ExternalResolver => {
            let resolver = ExternalResolverBackend::from_config(&config.secrets.resolver)
                .context("configure the external resolver secrets backend")?;
            Ok(Some(Arc::new(SecretsService::new(
                Arc::new(PgSecretRefStore::new(pool.clone())),
                Arc::new(resolver),
            ))))
        }
    }
}
