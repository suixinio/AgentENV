use std::collections::HashMap;
use std::sync::Arc;

use iroh::endpoint::Connection;
use iroh::protocol::{AcceptError, ProtocolHandler};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::p2p::types::{P2pArtifactDescriptor, P2pArtifactKey, P2pEndpoint, P2pPeer};

pub const CATALOG_ALPN: &[u8] = b"/agentenv/artifact-catalog/v1";
pub const MAX_CATALOG_RESPONSE_BYTES: usize = 8 * 1024 * 1024;
const MAX_CATALOG_REQUEST_BYTES: usize = 1024 * 1024;
/// Time to wait for the client to close the connection after we finish
/// sending. Prevents a slow or misbehaving peer from holding a handler task open.
const CONNECTION_CLOSE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

#[derive(Debug, Serialize, Deserialize)]
pub struct CatalogRequest {
    pub key: P2pArtifactKey,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CatalogResponse {
    pub descriptor: Option<P2pArtifactDescriptor>,
}

/// What this process can currently serve to peers, for the lifetime of the
/// process.
///
/// The catalog is derived state: an entry exists only while this process holds
/// the descriptor that produced it, and a peer that asks about anything else
/// is told this node has nothing.
#[derive(Debug, Clone)]
pub struct PublishedArtifactCatalog {
    inner: Arc<RwLock<HashMap<P2pArtifactKey, P2pArtifactDescriptor>>>,
    local_provider: P2pPeer,
}

impl PublishedArtifactCatalog {
    pub fn new(node_id: &str, local_endpoint: &P2pEndpoint) -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
            local_provider: P2pPeer {
                node_id: node_id.to_string(),
                endpoint: local_endpoint.clone(),
            },
        }
    }

    pub async fn descriptor_for(&self, key: &P2pArtifactKey) -> Option<P2pArtifactDescriptor> {
        self.inner.read().await.get(key).cloned()
    }

    pub async fn upsert(&self, descriptor: P2pArtifactDescriptor) {
        let mut catalog = self.inner.write().await;
        catalog.insert(descriptor.key.clone(), descriptor);
    }

    pub async fn remove(&self, key: &P2pArtifactKey) -> Option<P2pArtifactDescriptor> {
        self.inner.write().await.remove(key)
    }
}

#[derive(Debug, Clone)]
pub struct CatalogProtocol {
    published_catalog: PublishedArtifactCatalog,
}

impl CatalogProtocol {
    pub fn new(published_catalog: PublishedArtifactCatalog) -> Self {
        Self { published_catalog }
    }

    async fn descriptor_for_response(&self, key: &P2pArtifactKey) -> Option<P2pArtifactDescriptor> {
        self.published_catalog
            .descriptor_for(key)
            .await
            .map(|mut descriptor| {
                descriptor.providers = vec![self.published_catalog.local_provider.clone().into()];
                descriptor
            })
    }
}

impl ProtocolHandler for CatalogProtocol {
    async fn accept(&self, connection: Connection) -> std::result::Result<(), AcceptError> {
        let (mut send, mut recv) = connection.accept_bi().await?;
        let request_bytes = recv
            .read_to_end(MAX_CATALOG_REQUEST_BYTES)
            .await
            .map_err(AcceptError::from_err)?;
        let request: CatalogRequest =
            serde_json::from_slice(&request_bytes).map_err(AcceptError::from_err)?;
        let descriptor = self.descriptor_for_response(&request.key).await;
        let found = descriptor.is_some();
        let response = CatalogResponse { descriptor };
        let response_bytes = serde_json::to_vec(&response).map_err(AcceptError::from_err)?;
        send.write_all(&response_bytes)
            .await
            .map_err(AcceptError::from_err)?;
        send.finish()?;
        tracing::trace!(key = %request.key, found, "served P2P catalog request");
        let _ = tokio::time::timeout(CONNECTION_CLOSE_TIMEOUT, connection.closed()).await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::p2p::types::P2pArtifactProvider;

    fn endpoint(address: &str) -> P2pEndpoint {
        P2pEndpoint {
            backend: "iroh".to_string(),
            address: address.to_string(),
        }
    }

    fn provider(node_id: &str, address: &str) -> P2pArtifactProvider {
        P2pArtifactProvider::from(P2pPeer {
            node_id: node_id.to_string(),
            endpoint: endpoint(address),
        })
    }

    #[tokio::test]
    async fn response_descriptor_uses_current_local_provider() {
        let local_endpoint = endpoint("fresh-local-endpoint");
        let catalog = PublishedArtifactCatalog::new("current-node", &local_endpoint);
        let key = "test/p2p/catalog/accept-provider".to_string();
        catalog
            .upsert(P2pArtifactDescriptor {
                key: key.clone(),
                providers: vec![
                    P2pArtifactProvider::Local,
                    provider("stale-node", "stale-endpoint"),
                ],
                backend_locator: Some("blob-hash".to_string()),
                metadata: serde_json::json!({ "kind": "catalog-accept-test" }),
            })
            .await;
        let protocol = CatalogProtocol::new(catalog);

        let descriptor = protocol
            .descriptor_for_response(&key)
            .await
            .expect("descriptor should be present");

        assert_eq!(descriptor.backend_locator, Some("blob-hash".to_string()));
        assert_eq!(
            descriptor.metadata,
            serde_json::json!({ "kind": "catalog-accept-test" })
        );
        assert_eq!(
            descriptor.providers,
            vec![P2pArtifactProvider::from(P2pPeer {
                node_id: "current-node".to_string(),
                endpoint: local_endpoint,
            })]
        );
    }

    #[tokio::test]
    async fn a_fresh_catalog_serves_nothing() {
        let catalog = PublishedArtifactCatalog::new("current-node", &endpoint("local-endpoint"));
        let key = "test/p2p/catalog/never-published".to_string();

        assert!(catalog.descriptor_for(&key).await.is_none());
        assert!(catalog.remove(&key).await.is_none());
    }
}
