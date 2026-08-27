//! Undoing a source-registry publication.
//!
//! 🔴 Split out of `publisher` so the half that owns catalog rows can remove a
//! snapshot's external publications without linking the half that creates them.
//! Creating one reads overlaybd layers off local disk; removing one is a
//! registry `DELETE` against a digest already written down in the row.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tracing::warn;

use super::client::{AcrClient, AcrClientError};
use super::reference::SourceRegistryRepository;
use crate::snapshot::repository::{RepositoryError, RepositoryResult};
use crate::snapshot::PersistedDiskImagePublication;

pub type AcrClientBuilder =
    dyn Fn(&str) -> Result<AcrClient, AcrClientError> + Send + Sync + 'static;

pub struct AcrPublicationRollback {
    // This mutex only protects the in-memory client map. It is never held across
    // an await point; client construction happens outside the lock as well.
    clients: Mutex<HashMap<String, AcrClient>>,
    client_builder: Arc<AcrClientBuilder>,
}

impl Default for AcrPublicationRollback {
    fn default() -> Self {
        Self::new()
    }
}

impl AcrPublicationRollback {
    pub fn new() -> Self {
        Self {
            clients: Mutex::new(HashMap::new()),
            client_builder: Arc::new(AcrClient::from_docker_config),
        }
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn new_with_client_builder(client_builder: Arc<AcrClientBuilder>) -> Self {
        Self {
            clients: Mutex::new(HashMap::new()),
            client_builder,
        }
    }

    pub async fn rollback_publication(
        &self,
        publication: &PersistedDiskImagePublication,
    ) -> RepositoryResult<()> {
        let repository_ref = match SourceRegistryRepository::parse(&publication.repo_blob_url) {
            Ok(repository_ref) => repository_ref,
            Err(error) => {
                warn!(
                    repo_blob_url = %publication.repo_blob_url,
                    error = %error,
                    "cannot roll back ACR publication with invalid repoBlobUrl"
                );
                return Ok(());
            }
        };
        let client = self.client_for_registry(&repository_ref.registry).await?;
        client
            .delete_manifest_by_digest(
                &repository_ref.registry,
                &repository_ref.repository,
                &publication.manifest_digest,
            )
            .await
            .map_err(RepositoryError::from)
    }

    pub async fn client_for_registry(&self, registry: &str) -> Result<AcrClient, AcrClientError> {
        {
            let clients = self.clients.lock().map_err(|_| AcrClientError::Registry {
                message: "ACR client cache lock poisoned".to_string(),
            })?;
            if let Some(client) = clients.get(registry) {
                return Ok(client.clone());
            }
        }

        let registry = registry.to_string();
        let registry_for_builder = registry.clone();
        let client_builder = Arc::clone(&self.client_builder);
        let client = tokio::task::spawn_blocking(move || (client_builder)(&registry_for_builder))
            .await
            .map_err(|e| AcrClientError::Registry {
                message: format!("join ACR client builder task: {e}"),
            })??;
        let mut clients = self.clients.lock().map_err(|_| AcrClientError::Registry {
            message: "ACR client cache lock poisoned".to_string(),
        })?;
        // Another task may have populated the cache while this task was loading
        // Docker credentials in spawn_blocking. Prefer the cached client and
        // discard the duplicate one.
        if let Some(existing) = clients.get(&registry) {
            return Ok(existing.clone());
        }
        clients.insert(registry, client.clone());
        Ok(client)
    }
}
