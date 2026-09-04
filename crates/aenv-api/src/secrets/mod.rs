//! The api half's secrets: names and versions in PostgreSQL, values in this
//! process's own database, in Vault or in an operator-run resolver.

pub mod envelope;
pub mod pg;
pub mod resolver;
pub mod vault;

use std::sync::Arc;

use aenv_core::cfg::{AppConfig, SecretsBackendKind};
use aenv_core::secrets::SecretsService;
use anyhow::{Context, Result};
use sqlx::PgPool;
use zeroize::Zeroizing;

pub use envelope::Envelope;
pub use pg::values::PgSecretValues;
pub use pg::PgSecretRefStore;
pub use resolver::ExternalResolverBackend;
pub use vault::VaultKv2Backend;

/// What `[secrets]` assembled. `values` is present only for a backend whose
/// values this process can open, which is also the only backend that serves
/// the broker's resolve endpoint from here.
pub struct SecretsAssembly {
    pub service: Arc<SecretsService>,
    pub values: Option<Arc<PgSecretValues>>,
    pub resolver_token: Option<Zeroizing<String>>,
}

/// Builds what `[secrets]` describes, or `None` when the backend is disabled
/// and `/secrets` must answer 503.
pub fn build_secrets_service(config: &AppConfig, pool: &PgPool) -> Result<Option<SecretsAssembly>> {
    let refs = || Arc::new(PgSecretRefStore::new(pool.clone()));
    match config.secrets.backend {
        SecretsBackendKind::Disabled => Ok(None),
        SecretsBackendKind::Vault => {
            let vault = VaultKv2Backend::from_config(&config.secrets.vault)
                .context("configure the Vault secrets backend")?;
            Ok(Some(SecretsAssembly {
                service: Arc::new(SecretsService::new(refs(), Arc::new(vault))),
                values: None,
                resolver_token: None,
            }))
        }
        SecretsBackendKind::ExternalResolver => {
            let resolver = ExternalResolverBackend::from_config(&config.secrets.resolver)
                .context("configure the external resolver secrets backend")?;
            Ok(Some(SecretsAssembly {
                service: Arc::new(SecretsService::new(refs(), Arc::new(resolver))),
                values: None,
                resolver_token: None,
            }))
        }
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
                    refs(),
                    Arc::clone(&values) as Arc<dyn aenv_core::secrets::SecretsBackend>,
                )),
                values: Some(values),
                resolver_token: Some(token),
            }))
        }
    }
}
