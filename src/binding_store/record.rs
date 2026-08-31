//! Cross-language Redis binding record consumed by the gateway.
//! Field names and key prefixes are wire contracts.

use serde::{Deserialize, Serialize};

use crate::node_registry::types::Node;

/// Gateway-compatible node JSON.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireNode {
    pub node_id: String,
    pub endpoint: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub pod_name: String,
}

impl From<&Node> for WireNode {
    fn from(node: &Node) -> Self {
        Self {
            node_id: node.id.clone(),
            endpoint: node.endpoint.clone(),
            pod_name: node.pod_name.clone(),
        }
    }
}

impl From<WireNode> for Node {
    fn from(wire: WireNode) -> Self {
        Node {
            id: wire.node_id,
            endpoint: wire.endpoint,
            pod_name: wire.pod_name,
        }
    }
}

/// JSON value stored at `{prefix}:sandbox:{sandbox_id}`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Record {
    pub node: WireNode,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub execution_id: String,
}

/// Serializes a gateway-compatible routing record.
pub fn marshal_record(node: &Node, execution_id: &str) -> String {
    let record = Record {
        node: node.into(),
        execution_id: execution_id.to_string(),
    };
    // This string-only record shape is infallible to serialize.
    serde_json::to_string(&record).expect("Record serialization is infallible for this shape")
}

/// Parses a routing record, rejecting malformed records and missing node coordinates.
pub fn parse_record(raw: &[u8]) -> Option<Record> {
    let mut record: Record = serde_json::from_slice(raw).ok()?;
    record.node.node_id = record.node.node_id.trim().to_string();
    record.node.endpoint = record.node.endpoint.trim().to_string();
    record.node.pod_name = record.node.pod_name.trim().to_string();
    if record.node.node_id.is_empty() || record.node.endpoint.is_empty() {
        return None;
    }
    Some(record)
}

/// Builds a sandbox binding key.
pub fn binding_key(prefix: &str, sandbox_id: &str) -> String {
    format!("{prefix}:sandbox:{sandbox_id}")
}

/// Builds a node reverse-index key.
pub fn node_index_key(prefix: &str, node_id: &str) -> String {
    format!("{prefix}:node:{node_id}")
}

/// Gateway-compatible routing namespace.
pub const DEFAULT_KEY_PREFIX: &str = "agentenv:scheduler:bindings";

/// Normalizes execution ids for lexical UUIDv7 comparison.
pub fn normalize_execution_id(raw: &str) -> String {
    raw.trim().to_lowercase()
}

/// Normalizes and validates an execution id without recording roster metrics.
pub fn normalize_execution_id_reason(raw: &str) -> (String, Option<&'static str>) {
    crate::node_registry::registry::normalize_execution_id_reason(raw)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn marshal_matches_gos_field_names_and_order() {
        let node = Node {
            id: "node-a".to_string(),
            endpoint: "http://10.0.0.1:8000".to_string(),
            pod_name: String::new(),
        };
        let json = marshal_record(&node, "0198f5c0-1234-7abc-8def-000000000001");
        assert_eq!(
            json,
            r#"{"node":{"node_id":"node-a","endpoint":"http://10.0.0.1:8000"},"execution_id":"0198f5c0-1234-7abc-8def-000000000001"}"#
        );
    }

    #[test]
    fn marshal_includes_pod_name_when_present() {
        let node = Node {
            id: "node-a".to_string(),
            endpoint: "http://10.0.0.1:8000".to_string(),
            pod_name: "agentenv-node-xk29f".to_string(),
        };
        let json = marshal_record(&node, "");
        assert_eq!(
            json,
            r#"{"node":{"node_id":"node-a","endpoint":"http://10.0.0.1:8000","pod_name":"agentenv-node-xk29f"}}"#
        );
    }

    #[test]
    fn go_and_rust_agree_on_the_stored_record_bytes() {
        const STORED_RECORD_WITH_POD_NAME: &str = r#"{"node":{"node_id":"node-a","endpoint":"http://node-a","pod_name":"agentenv-node-7f4c2"},"execution_id":"0198b7cc-1111-7000-8000-000000000001"}"#;
        const STORED_RECORD_WITHOUT_POD_NAME: &str = r#"{"node":{"node_id":"node-a","endpoint":"http://node-a"},"execution_id":"0198b7cc-1111-7000-8000-000000000001"}"#;
        const EXECUTION_ID: &str = "0198b7cc-1111-7000-8000-000000000001";

        let with_pod_name = Node {
            id: "node-a".to_string(),
            endpoint: "http://node-a".to_string(),
            pod_name: "agentenv-node-7f4c2".to_string(),
        };
        assert_eq!(
            marshal_record(&with_pod_name, EXECUTION_ID),
            STORED_RECORD_WITH_POD_NAME
        );

        let without_pod_name = Node {
            id: "node-a".to_string(),
            endpoint: "http://node-a".to_string(),
            pod_name: String::new(),
        };
        assert_eq!(
            marshal_record(&without_pod_name, EXECUTION_ID),
            STORED_RECORD_WITHOUT_POD_NAME
        );
    }

    #[test]
    fn parse_round_trips_marshal() {
        let node = Node {
            id: "node-a".to_string(),
            endpoint: "http://10.0.0.1:8000".to_string(),
            pod_name: String::new(),
        };
        let json = marshal_record(&node, "exec-1");
        let record = parse_record(json.as_bytes()).expect("parses");
        assert_eq!(record.node.node_id, "node-a");
        assert_eq!(record.execution_id, "exec-1");
    }

    #[test]
    fn parse_a_missing_execution_id_is_not_a_decode_failure() {
        let record = parse_record(br#"{"node":{"node_id":"a","endpoint":"http://a"}}"#)
            .expect("backward-compatible with an older writer");
        assert_eq!(record.execution_id, "");
    }

    #[test]
    fn parse_rejects_a_record_naming_nowhere() {
        assert!(parse_record(br#"{"node":{"node_id":"","endpoint":""}}"#).is_none());
        assert!(parse_record(br#"{"node":{"node_id":"a","endpoint":""}}"#).is_none());
        assert!(parse_record(b"not json").is_none());
    }

    #[test]
    fn parse_trims_whitespace_the_same_way_go_does() {
        let record =
            parse_record(br#"{"node":{"node_id":" a ","endpoint":" http://a "}}"#).expect("parses");
        assert_eq!(record.node.node_id, "a");
        assert_eq!(record.node.endpoint, "http://a");
    }

    #[test]
    fn keys_match_gos_format() {
        assert_eq!(
            binding_key("agentenv:scheduler:bindings", "sbx-1"),
            "agentenv:scheduler:bindings:sandbox:sbx-1"
        );
        assert_eq!(
            node_index_key("agentenv:scheduler:bindings", "node-a"),
            "agentenv:scheduler:bindings:node:node-a"
        );
    }

    #[test]
    fn normalize_lowercases_and_trims() {
        assert_eq!(normalize_execution_id("  ABC-123  "), "abc-123");
    }
}
