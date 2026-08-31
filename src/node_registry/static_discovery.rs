//! Converts and validates one-shot static node discovery configuration.

use crate::cfg::ClusterStaticDiscoveryNode;

use super::types::Node;

/// Converts static entries to registry nodes, preserving configured endpoints verbatim.
pub fn nodes_from_static_config(nodes: &[ClusterStaticDiscoveryNode]) -> Vec<Node> {
    nodes
        .iter()
        .map(|n| Node {
            id: n.id.clone(),
            endpoint: n.endpoint.clone(),
            pod_name: String::new(),
        })
        .collect()
}

/// Requires at least one node with nonblank identity and endpoint.
pub fn validate_static_discovery_nodes(nodes: &[ClusterStaticDiscoveryNode]) -> anyhow::Result<()> {
    if nodes.is_empty() {
        anyhow::bail!("[cluster].static_discovery_nodes must not be empty");
    }
    for n in nodes {
        if n.id.trim().is_empty() || n.endpoint.trim().is_empty() {
            anyhow::bail!("[cluster].static_discovery_nodes entries require id and endpoint");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: &str, endpoint: &str) -> ClusterStaticDiscoveryNode {
        ClusterStaticDiscoveryNode {
            id: id.to_string(),
            endpoint: endpoint.to_string(),
        }
    }

    #[test]
    fn parses_the_compose_static_discovery_overlay_into_the_expected_nodes() {
        #[derive(serde::Deserialize)]
        struct Overlay {
            cluster: OverlayCluster,
        }
        #[derive(serde::Deserialize)]
        struct OverlayCluster {
            static_discovery_nodes: Vec<ClusterStaticDiscoveryNode>,
        }

        let raw = include_str!("../../deploy/docker/config/cluster-static-discovery-overlay.toml");
        let overlay: Overlay =
            toml::from_str(raw).expect("cluster-static-discovery-overlay.toml is valid TOML");
        let configured = overlay.cluster.static_discovery_nodes;

        assert!(
            !configured.is_empty(),
            "the fixture this test exists to pin down must actually carry nodes"
        );

        validate_static_discovery_nodes(&configured)
            .expect("the compose overlay's node list must be valid");

        let got = nodes_from_static_config(&configured);

        // Literal expectations independently pin the tracked compose overlay.
        let expected = vec![
            Node {
                id: "node-a".to_string(),
                endpoint: "http://agentenv-a:8000".to_string(),
                pod_name: String::new(),
            },
            Node {
                id: "node-b".to_string(),
                endpoint: "http://agentenv-b:8000".to_string(),
                pod_name: String::new(),
            },
        ];

        assert_eq!(
            got, expected,
            "nodes_from_static_config must turn \
             cluster-static-discovery-overlay.toml's entries into these Nodes \
             field-for-field, including pod_name staying empty and endpoint \
             being carried through without rewriting"
        );
    }

    #[test]
    fn empty_list_is_refused_like_go() {
        let err = validate_static_discovery_nodes(&[]).unwrap_err();
        assert!(err.to_string().contains("must not be empty"), "{err}");
    }

    #[test]
    fn blank_id_or_endpoint_is_refused_like_go() {
        let err = validate_static_discovery_nodes(&[node("", "http://a:8000")]).unwrap_err();
        assert!(err.to_string().contains("require id and endpoint"), "{err}");

        let err = validate_static_discovery_nodes(&[node("node-a", "")]).unwrap_err();
        assert!(err.to_string().contains("require id and endpoint"), "{err}");

        let err = validate_static_discovery_nodes(&[node("   ", "http://a:8000")]).unwrap_err();
        assert!(err.to_string().contains("require id and endpoint"), "{err}");
    }

    #[test]
    fn endpoint_is_carried_through_verbatim_not_rebuilt() {
        let nodes = vec![node("node-a", "https://custom.example:9999/weird-path")];
        let got = nodes_from_static_config(&nodes);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].endpoint, "https://custom.example:9999/weird-path");
        assert_eq!(got[0].pod_name, "", "static nodes never carry a pod alias");
    }

    #[test]
    fn order_is_preserved() {
        let nodes = vec![
            node("z-node", "http://z:8000"),
            node("a-node", "http://a:8000"),
        ];
        let got = nodes_from_static_config(&nodes);
        assert_eq!(
            got.iter().map(|n| n.id.as_str()).collect::<Vec<_>>(),
            ["z-node", "a-node"]
        );
    }
}
