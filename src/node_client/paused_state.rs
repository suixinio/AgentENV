//! A paused sandbox as the deciding half holds it: not a handle, a reference.

use anyhow::Result;
use serde_json::{json, Value};

use crate::sandbox::{PausedSandboxState, RuntimeArtifactSet};
use crate::types::ExecutionId;

/// Serializable reference to a capture stored on another node.
///
/// Origin node and paused incarnation jointly pin the machine and exact capture to reopen.
#[derive(Clone, Debug)]
pub struct RemotePausedState {
    origin_node_id: String,
    artifact_root: String,
    /// Incarnation captured, not the incarnation a resume will start.
    paused_execution_id: ExecutionId,
    state: Value,
}

impl RemotePausedState {
    pub fn new(
        origin_node_id: String,
        artifact_root: String,
        paused_execution_id: ExecutionId,
        state: Value,
    ) -> Self {
        Self {
            origin_node_id,
            artifact_root,
            paused_execution_id,
            state,
        }
    }

    pub fn origin_node_id(&self) -> &str {
        &self.origin_node_id
    }

    pub fn artifact_root(&self) -> &str {
        &self.artifact_root
    }

    pub fn paused_execution_id(&self) -> ExecutionId {
        self.paused_execution_id
    }
}

impl PausedSandboxState for RemotePausedState {
    /// Encodes every field persistence must retain for remote resume.
    fn encode(&self) -> Result<Value> {
        Ok(json!({
            "origin_node_id": self.origin_node_id,
            "artifact_root": self.artifact_root,
            "execution_id": self.paused_execution_id.to_string(),
            "state": self.state,
        }))
    }

    /// Returns no artifacts because the deciding process owns no local layers.
    fn runtime_artifacts(&self) -> RuntimeArtifactSet {
        RuntimeArtifactSet::empty()
    }

    /// Returns the origin node holding the remote artifacts.
    fn holding_node_id(&self) -> Option<&str> {
        Some(&self.origin_node_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct CapturedHere;

    impl PausedSandboxState for CapturedHere {
        fn encode(&self) -> Result<Value> {
            Ok(Value::Null)
        }

        fn runtime_artifacts(&self) -> RuntimeArtifactSet {
            RuntimeArtifactSet::empty()
        }
    }

    #[test]
    fn only_a_capture_taken_elsewhere_names_a_machine() {
        let elsewhere: &dyn PausedSandboxState = &RemotePausedState::new(
            "node-a".to_string(),
            "/var/lib/agentenv/paused/abc".to_string(),
            ExecutionId::new(),
            json!({}),
        );
        let here: &dyn PausedSandboxState = &CapturedHere;

        assert_eq!(
            elsewhere.holding_node_id(),
            Some("node-a"),
            "a capture taken on another machine has to say which"
        );
        assert_eq!(
            here.holding_node_id(),
            None,
            "a capture taken here names no other machine"
        );
    }

    #[test]
    fn the_encoding_carries_the_machine_the_bytes_are_on() {
        let paused_execution_id = ExecutionId::new();
        let state = RemotePausedState::new(
            "node-a".to_string(),
            "/var/lib/agentenv/paused/abc".to_string(),
            paused_execution_id,
            json!({"memory": "mem.json"}),
        );

        let encoded = state.encode().expect("encode");
        assert_eq!(encoded["origin_node_id"], "node-a");
        assert_eq!(encoded["artifact_root"], "/var/lib/agentenv/paused/abc");
        assert_eq!(encoded["execution_id"], paused_execution_id.to_string());
        assert_eq!(encoded["state"]["memory"], "mem.json");
    }

    #[test]
    fn the_encoding_says_which_run_it_captured() {
        let one = ExecutionId::new();
        let other = ExecutionId::new();
        assert_ne!(one, other, "two mints produced one incarnation");

        let encode_with = |execution_id| {
            RemotePausedState::new("node-a".into(), "/tmp/x".into(), execution_id, json!({}))
                .encode()
                .expect("encode")
        };

        assert_eq!(encode_with(one)["execution_id"], one.to_string());
        assert_eq!(encode_with(other)["execution_id"], other.to_string());
        assert_ne!(encode_with(one), encode_with(other));
    }

    #[test]
    fn it_pins_no_local_artifacts() {
        let state = RemotePausedState::new(
            "node-a".into(),
            "/tmp/x".into(),
            ExecutionId::new(),
            json!({}),
        );
        assert!(state.runtime_artifacts().is_empty());
    }
}
