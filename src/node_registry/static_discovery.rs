//! Port of `services/scheduler/cmd/main.go`'s static discovery branch
//! (the `default:` arm of its `switch strings.ToLower(strings.TrimSpace(
//! cfg.Scheduler.Discovery.Mode))`) and `services/shared/config/config.go`'s
//! `Config.validate`'s `case "static":` block.
//!
//! Go's static branch is short: build a `[]scheduler.Node{ID, Endpoint}`
//! straight off the configured list and call `registry.Set(nodes, nil)`
//! **once** — no watcher, no background task, unlike the Kubernetes branch
//! (`go runKubernetesDiscoveryWithRetry(...)`). This module mirrors that
//! shape: [`nodes_from_static_config`] is the pure `Node` list conversion
//! (the same role [`super::kubernetes_discovery::nodes_from_endpoint_slices`]
//! plays for the Kubernetes branch), and [`validate_static_discovery_nodes`]
//! is the pure validation Go's `validate()` runs before the registry is ever
//! touched. The call site (`start_native_node_registry`,
//! `crates/aenv-api/src/bin/aenv-api.rs`) does the one-shot `registry.set(...)`
//! call itself — there is no live glue here to test, unlike
//! [`super::kubernetes_discovery::KubernetesDiscovery`].

use crate::cfg::ClusterStaticDiscoveryNode;

use super::types::Node;

/// Converts a configured static node list into the `Node`s
/// [`super::registry::AtomicNodeRegistry::set`] consumes. Mirrors
/// `services/scheduler/cmd/main.go`'s static branch exactly:
///
/// ```go
/// nodes := make([]scheduler.Node, 0, len(cfg.Scheduler.Nodes))
/// for _, n := range cfg.Scheduler.Nodes {
///     nodes = append(nodes, scheduler.Node{ID: n.ID, Endpoint: n.Endpoint})
/// }
/// ```
///
/// `pod_name` is always empty: Go's `scheduler.Node` is an alias for
/// `routing.Node{ID, Endpoint, PodName}` (`services/shared/routing/record.go`),
/// and the static branch never sets `PodName`, leaving it at its zero value
/// — the same as [`Node::pod_name`]'s `String::new()` here. `endpoint` is
/// carried through byte-for-byte, unlike
/// [`super::kubernetes_discovery::nodes_from_endpoint_slices`]'s
/// `scheme://host:port` construction: a static entry's `endpoint` is
/// already a complete URL in the config (see
/// [`ClusterStaticDiscoveryNode::endpoint`]'s doc comment), the same way
/// Go's own `n.Endpoint` is.
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

/// Mirrors `services/shared/config/config.go`'s `validate()`, `case
/// "static":` block:
///
/// ```go
/// case "static":
///     if len(c.Scheduler.Nodes) == 0 {
///         return errors.New("scheduler.nodes must not be empty")
///     }
///     for _, n := range c.Scheduler.Nodes {
///         if n.ID == "" || n.Endpoint == "" {
///             return errors.New("scheduler.nodes require id and endpoint")
///         }
///     }
/// ```
///
/// 🔴 One deliberate widening: Go compares `n.ID == ""`/`n.Endpoint == ""`
/// literally (no trimming), so a whitespace-only entry passes Go's check and
/// is only caught later, if at all, by whatever tries to dial it. This port
/// trims first — the same discipline [`super::kubernetes_discovery`]'s own
/// `validate_optional_pod_selector` already applies to its config strings —
/// so a whitespace-only `id`/`endpoint` is refused here at startup instead
/// of becoming an unreachable node with a name nobody can look up.
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

    /// 🔴 The golden-test contract this module's own module doc promises:
    /// parses the exact `scheduler.nodes` array Go's own compose config
    /// carries (`deploy/docker/config/default.json`, the JSON
    /// `services/scheduler`/`services/gateway` read), through the identical
    /// `{"id": ..., "endpoint": ...}` wire shape
    /// [`ClusterStaticDiscoveryNode`] deserializes from TOML, and asserts
    /// [`nodes_from_static_config`]'s output against the byte-for-byte set
    /// Go's own static branch would build from the same file:
    /// `scheduler.Node{ID: n.ID, Endpoint: n.Endpoint}` for each entry, in
    /// order, with `PodName` left empty.
    ///
    /// A single point of contact with the real file, not a copy-pasted
    /// literal: if `deploy/docker/config/default.json`'s `scheduler.nodes`
    /// ever drifts from what this test expects, this test is what notices.
    #[test]
    fn matches_the_go_compose_config_nodes_array() {
        let raw = include_str!("../../deploy/docker/config/default.json");
        let parsed: serde_json::Value =
            serde_json::from_str(raw).expect("deploy/docker/config/default.json is valid JSON");
        let go_nodes = parsed
            .get("scheduler")
            .and_then(|s| s.get("nodes"))
            .and_then(|n| n.as_array())
            .expect("deploy/docker/config/default.json carries scheduler.nodes");
        assert!(
            !go_nodes.is_empty(),
            "the fixture this test exists to pin down must actually carry nodes"
        );

        let configured: Vec<ClusterStaticDiscoveryNode> = go_nodes
            .iter()
            .map(|entry| ClusterStaticDiscoveryNode {
                id: entry["id"].as_str().expect("id").to_string(),
                endpoint: entry["endpoint"].as_str().expect("endpoint").to_string(),
            })
            .collect();

        validate_static_discovery_nodes(&configured)
            .expect("the compose fixture's node list must be valid");

        let got = nodes_from_static_config(&configured);

        // Go's own `defaultConfig`/compose fixture, spelled out by hand as
        // the independent expectation — this is what makes the mutation
        // check below meaningful: change either side and only one of the
        // two literal sources moves.
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
            "nodes_from_static_config must reproduce Go's static branch \
             (scheduler.Node{{ID: n.ID, Endpoint: n.Endpoint}}) field-for-field, \
             including pod_name staying empty and endpoint being carried through \
             without rewriting"
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

        // Widened past Go on purpose (see this function's own doc comment):
        // whitespace-only is refused here even though Go's literal `== ""`
        // comparison would accept it.
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
