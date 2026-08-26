//! Task's own "D3": the wire-compatible binding record, ported from
//! `services/shared/routing/record.go`. Byte-compatible with what gateway
//! already reads out of Redis (`GATEWAY_ROUTING_PROJECTION_READ=on`,
//! `services/shared/routing.Reader`) — this is a cross-language contract,
//! not an internal type this build is free to reshape.
//!
//! `Record`'s field names (`node`/`execution_id`, and `node.node_id`/
//! `node.endpoint`/`node.pod_name`) are load-bearing serde output, not
//! naming taste: gateway's `routing.Record`/`routing.Node` decode exactly
//! this JSON shape and nothing else.

use serde::{Deserialize, Serialize};

use crate::node_registry::types::Node;

/// Mirrors `routing.Node`'s JSON tags exactly (`record.go`): `node_id`,
/// `endpoint`, `pod_name` (omitted when empty).
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

/// Mirrors `routing.Record` (`record.go`): the JSON value written at
/// `{prefix}:sandbox:{sandbox_id}`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Record {
    pub node: WireNode,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub execution_id: String,
}

/// Ports `routing.MarshalRecord`.
pub fn marshal_record(node: &Node, execution_id: &str) -> String {
    let record = Record {
        node: node.into(),
        execution_id: execution_id.to_string(),
    };
    // A `Node`/`Record` composed of plain `String` fields cannot fail to
    // serialize as JSON — `serde_json::to_string` only errors on map keys
    // that are not strings or on a `Serialize` impl that itself returns an
    // error, neither of which applies here.
    serde_json::to_string(&record).expect("Record serialization is infallible for this shape")
}

/// Ports `routing.ParseRecord`: `(Record{}, false)` on anything that does
/// not decode, or decodes to a record naming nowhere (`node_id` or
/// `endpoint` empty after trimming) — never a decode failure for a missing
/// `execution_id` (backward compatible with an older writer).
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

/// Ports `routing.BindingKey`.
pub fn binding_key(prefix: &str, sandbox_id: &str) -> String {
    format!("{prefix}:sandbox:{sandbox_id}")
}

/// Ports `routing.NodeIndexKey`.
pub fn node_index_key(prefix: &str, node_id: &str) -> String {
    format!("{prefix}:node:{node_id}")
}

/// Ports `routing.DefaultKeyPrefix`. 🔴 Deliberately the exact Go value,
/// not a Rust-flavored rename: this is the key namespace gateway already
/// reads (`GATEWAY_ROUTING_PROJECTION_READ=on`), and the whole point of
/// this port is that gateway's read path does not need to change.
pub const DEFAULT_KEY_PREFIX: &str = "agentenv:scheduler:bindings";

/// Ports `routing.NormalizeExecutionID`: trim, then lower-case. Lexicographic
/// comparison over UUIDv7 text only sorts in mint order when normalized this
/// way (`'0'-'9' < 'A'-'F' < 'a'-'f'` in ASCII would reverse it otherwise).
pub fn normalize_execution_id(raw: &str) -> String {
    raw.trim().to_lowercase()
}

/// Ports `services/scheduler/internal/node_registry.go`'s
/// `normalizeExecutionIDReason`, reusing the same shape check
/// [`crate::node_registry::registry`] already implements for the roster
/// path — trims, lower-cases, and checks the canonical-UUID shape, but
/// (unlike that module's own `normalize_execution_id`) never records a
/// metric for the drop: this is the event/assignment path, and Go's own
/// split (`applyProjectionDelete` vs `normalizeExecutionID`, the roster
/// path) reserves roster-drop counting for the roster path alone —
/// counting it again here would double-count the same bad value when it
/// arrives on both.
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
