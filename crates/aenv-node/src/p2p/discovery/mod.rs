mod scheduler;

use std::collections::{HashMap, HashSet};

use async_trait::async_trait;

use super::error::Result;
use super::types::{P2pArtifactKey, P2pArtifactProviderHint, P2pEndpoint, P2pPeer};

pub use scheduler::SchedulerPeerDiscovery;

#[async_trait]
/// Source of candidate peers for artifact lookup.
pub trait P2pPeerDiscovery: Send + Sync {
    /// Return currently known peers that may serve artifact catalog requests.
    async fn peers(&self) -> Result<Vec<P2pPeer>>;

    /// Return candidate peers with caller-provided hints prioritized ahead of discovery results.
    async fn peers_with_hints(&self, hints: &[P2pArtifactProviderHint]) -> Result<Vec<P2pPeer>> {
        let discovered = self.peers().await?;
        Ok(merge_hints_with_discovered_peers(hints, discovered))
    }

    /// Return peers scheduler has indexed for a specific artifact key.
    async fn peers_for_key(&self, _key: &P2pArtifactKey) -> Result<Vec<P2pPeer>>;

    /// Best-effort notification that this node can serve `key`.
    async fn record_key(&self, _key: &P2pArtifactKey) -> Result<()>;

    /// Best-effort notification that this node no longer serves `key`.
    async fn forget_key(&self, _key: &P2pArtifactKey) -> Result<()>;
}

#[derive(Debug, Default)]
pub struct NoopP2pPeerDiscovery;

#[async_trait]
impl P2pPeerDiscovery for NoopP2pPeerDiscovery {
    async fn peers(&self) -> Result<Vec<P2pPeer>> {
        Ok(Vec::new())
    }

    async fn peers_for_key(&self, _key: &P2pArtifactKey) -> Result<Vec<P2pPeer>> {
        Ok(Vec::new())
    }

    async fn record_key(&self, _key: &P2pArtifactKey) -> Result<()> {
        Ok(())
    }

    async fn forget_key(&self, _key: &P2pArtifactKey) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
pub struct StaticP2pPeerDiscovery {
    peers: Vec<P2pPeer>,
}

impl StaticP2pPeerDiscovery {
    pub fn new(peers: Vec<P2pPeer>) -> Self {
        Self { peers }
    }
}

#[async_trait]
impl P2pPeerDiscovery for StaticP2pPeerDiscovery {
    async fn peers(&self) -> Result<Vec<P2pPeer>> {
        Ok(self.peers.clone())
    }

    async fn peers_for_key(&self, _key: &P2pArtifactKey) -> Result<Vec<P2pPeer>> {
        Ok(self.peers.clone())
    }

    async fn record_key(&self, _key: &P2pArtifactKey) -> Result<()> {
        Ok(())
    }

    async fn forget_key(&self, _key: &P2pArtifactKey) -> Result<()> {
        Ok(())
    }
}

fn merge_hints_with_discovered_peers(
    hints: &[P2pArtifactProviderHint],
    discovered: Vec<P2pPeer>,
) -> Vec<P2pPeer> {
    let mut discovered_by_node_id = HashMap::with_capacity(discovered.len());
    for peer in &discovered {
        discovered_by_node_id
            .entry(peer.node_id.as_str())
            .or_insert(peer);
    }
    let mut peers = Vec::new();
    let mut seen_node_ids = HashSet::new();
    let mut seen_endpoints = HashSet::new();

    for hint in hints {
        if let Some(peer) = peer_from_hint(hint, &discovered_by_node_id) {
            push_unique_peer(&mut peers, &mut seen_node_ids, &mut seen_endpoints, peer);
        }
    }

    for peer in discovered {
        push_unique_peer(&mut peers, &mut seen_node_ids, &mut seen_endpoints, peer);
    }

    peers
}

fn peer_from_hint(
    hint: &P2pArtifactProviderHint,
    discovered_by_node_id: &HashMap<&str, &P2pPeer>,
) -> Option<P2pPeer> {
    if let Some(endpoint) = hint.endpoint.clone() {
        let node_id = hint
            .node_id
            .clone()
            .unwrap_or_else(|| endpoint.address.clone());
        return Some(P2pPeer { node_id, endpoint });
    }

    let node_id = hint.node_id.as_deref()?;
    discovered_by_node_id
        .get(node_id)
        .map(|peer| (*peer).clone())
}

fn push_unique_peer(
    peers: &mut Vec<P2pPeer>,
    seen_node_ids: &mut HashSet<String>,
    seen_endpoints: &mut HashSet<P2pEndpoint>,
    peer: P2pPeer,
) {
    if seen_node_ids.contains(&peer.node_id) || seen_endpoints.contains(&peer.endpoint) {
        return;
    }

    seen_node_ids.insert(peer.node_id.clone());
    seen_endpoints.insert(peer.endpoint.clone());
    peers.push(peer);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::p2p::P2pEndpoint;

    fn peer(node_id: &str, address: &str) -> P2pPeer {
        P2pPeer {
            node_id: node_id.to_string(),
            endpoint: P2pEndpoint {
                backend: "backend".to_string(),
                address: address.to_string(),
            },
        }
    }

    #[tokio::test]
    async fn static_discovery_returns_configured_peers() {
        let discovery = StaticP2pPeerDiscovery::new(vec![peer("node-a", "addr-a")]);

        assert_eq!(discovery.peers().await.expect("peers").len(), 1);
        assert_eq!(discovery.peers().await.expect("peers")[0].node_id, "node-a");
    }

    #[tokio::test]
    async fn peers_with_hints_prioritizes_hints_and_deduplicates() {
        let discovery = StaticP2pPeerDiscovery::new(vec![
            peer("node-a", "addr-a-from-discovery"),
            peer("node-b", "addr-b"),
        ]);
        let hinted_a = P2pArtifactProviderHint {
            node_id: Some("node-a".to_string()),
            endpoint: Some(P2pEndpoint {
                backend: "backend".to_string(),
                address: "addr-a-from-hint".to_string(),
            }),
        };
        let hinted_b_by_node = P2pArtifactProviderHint {
            node_id: Some("node-b".to_string()),
            endpoint: None,
        };

        let peers = discovery
            .peers_with_hints(&[hinted_a, hinted_b_by_node])
            .await
            .expect("peers");

        assert_eq!(peers.len(), 2);
        assert_eq!(peers[0].node_id, "node-a");
        assert_eq!(peers[0].endpoint.address, "addr-a-from-hint");
        assert_eq!(peers[1].node_id, "node-b");
    }

    #[tokio::test]
    async fn peers_with_hints_uses_endpoint_address_as_fallback_node_id() {
        let discovery = StaticP2pPeerDiscovery::new(Vec::new());
        let hint = P2pArtifactProviderHint {
            node_id: None,
            endpoint: Some(P2pEndpoint {
                backend: "backend".to_string(),
                address: "addr-from-hint".to_string(),
            }),
        };

        let peers = discovery.peers_with_hints(&[hint]).await.expect("peers");

        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].node_id, "addr-from-hint");
        assert_eq!(peers[0].endpoint.address, "addr-from-hint");
    }

    #[tokio::test]
    async fn peers_with_hints_keeps_discovery_order_without_hints() {
        let discovery = StaticP2pPeerDiscovery::new(vec![
            peer("node-a", "addr-a"),
            peer("node-c", "addr-c"),
            peer("node-b", "addr-b"),
        ]);

        let peers = discovery.peers_with_hints(&[]).await.expect("peers");

        assert_eq!(
            peers,
            vec![
                peer("node-a", "addr-a"),
                peer("node-c", "addr-c"),
                peer("node-b", "addr-b"),
            ]
        );
    }

    #[tokio::test]
    async fn peers_with_hints_preserves_hint_order_across_hint_kinds() {
        let discovery = StaticP2pPeerDiscovery::new(vec![
            peer("node-a", "addr-a"),
            peer("node-b", "addr-b-from-discovery"),
            peer("node-c", "addr-c"),
        ]);
        let hinted_a_by_node = P2pArtifactProviderHint {
            node_id: Some("node-a".to_string()),
            endpoint: None,
        };
        let hinted_b = P2pArtifactProviderHint {
            node_id: Some("node-b".to_string()),
            endpoint: Some(P2pEndpoint {
                backend: "backend".to_string(),
                address: "addr-b-from-hint".to_string(),
            }),
        };

        let peers = discovery
            .peers_with_hints(&[hinted_a_by_node, hinted_b])
            .await
            .expect("peers");

        assert_eq!(
            peers,
            vec![
                peer("node-a", "addr-a"),
                peer("node-b", "addr-b-from-hint"),
                peer("node-c", "addr-c"),
            ]
        );
    }

    #[tokio::test]
    async fn peers_with_hints_deduplicates_by_endpoint() {
        let discovery = StaticP2pPeerDiscovery::new(vec![
            peer("node-from-discovery", "shared-addr"),
            peer("node-b", "addr-b"),
        ]);
        let hint = P2pArtifactProviderHint {
            node_id: Some("node-from-hint".to_string()),
            endpoint: Some(P2pEndpoint {
                backend: "backend".to_string(),
                address: "shared-addr".to_string(),
            }),
        };

        let peers = discovery.peers_with_hints(&[hint]).await.expect("peers");

        assert_eq!(peers.len(), 2);
        assert_eq!(peers[0].node_id, "node-from-hint");
        assert_eq!(peers[0].endpoint.address, "shared-addr");
        assert_eq!(peers[1].node_id, "node-b");
    }

    #[tokio::test]
    async fn peers_with_hints_deduplicates_by_node_id() {
        let discovery = StaticP2pPeerDiscovery::new(vec![
            peer("node-a", "addr-from-discovery"),
            peer("node-b", "addr-b"),
        ]);
        let hint = P2pArtifactProviderHint {
            node_id: Some("node-a".to_string()),
            endpoint: Some(P2pEndpoint {
                backend: "backend".to_string(),
                address: "addr-from-hint".to_string(),
            }),
        };

        let peers = discovery.peers_with_hints(&[hint]).await.expect("peers");

        assert_eq!(peers.len(), 2);
        assert_eq!(peers[0].node_id, "node-a");
        assert_eq!(peers[0].endpoint.address, "addr-from-hint");
        assert_eq!(peers[1].node_id, "node-b");
    }

    #[tokio::test]
    async fn peers_with_hints_ignores_node_only_hint_missing_from_discovery() {
        let discovery = StaticP2pPeerDiscovery::new(vec![peer("node-a", "addr-a")]);
        let missing_hint = P2pArtifactProviderHint {
            node_id: Some("missing-node".to_string()),
            endpoint: None,
        };

        let peers = discovery
            .peers_with_hints(&[missing_hint])
            .await
            .expect("peers");

        assert_eq!(peers, vec![peer("node-a", "addr-a")]);
    }
}
