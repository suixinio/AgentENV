//! A paused sandbox as the deciding half holds it: not a handle, a reference.

use anyhow::Result;
use serde_json::{json, Value};

use crate::sandbox::{PausedSandboxState, RuntimeArtifactSet};

/// The paused state of a sandbox that was paused on another machine.
///
/// # 🔴 What this is not
///
/// The local implementation of [`PausedSandboxState`] is a live object holding
/// the things a resume reopens. This one holds a *path on somebody else's disk*
/// and the backend's own encoding of its state. It cannot reopen anything, and
/// it is not supposed to: it exists so the deciding half can carry a paused
/// sandbox's state between the node that produced it and the node that will
/// consume it, without ever being able to mistake itself for either.
///
/// # 🔴 Why the node id is part of it
///
/// `artifact_root` is a path, and a path means nothing without the machine it
/// is on. A record that carried the path and not the machine would be a resume
/// request that could be sent to the wrong node and would fail there in a way
/// that looks like corruption rather than like misrouting.
#[derive(Clone, Debug)]
pub struct RemotePausedState {
    origin_node_id: String,
    artifact_root: String,
    state: Value,
}

impl RemotePausedState {
    pub fn new(origin_node_id: String, artifact_root: String, state: Value) -> Self {
        Self {
            origin_node_id,
            artifact_root,
            state,
        }
    }

    /// The machine whose disk holds this sandbox's paused artifacts.
    pub fn origin_node_id(&self) -> &str {
        &self.origin_node_id
    }

    pub fn artifact_root(&self) -> &str {
        &self.artifact_root
    }

    /// The backend's own encoding, to be handed back to that backend's factory
    /// on the origin node.
    pub fn backend_state(&self) -> &Value {
        &self.state
    }
}

impl PausedSandboxState for RemotePausedState {
    /// Re-emits everything a resume needs, including where it has to happen.
    ///
    /// 🔴 The origin node travels inside the encoding rather than beside it.
    /// This value is stored by whatever the deciding half persists records in,
    /// and a field that lived outside `encode` would be the field that gets
    /// dropped by the persistence layer that did not know to carry it.
    fn encode(&self) -> Result<Value> {
        Ok(json!({
            "origin_node_id": self.origin_node_id,
            "artifact_root": self.artifact_root,
            "state": self.state,
        }))
    }

    /// None.
    ///
    /// 🔴 A fact, not a shortcut. This is what the image-liveness layer uses to
    /// keep local overlaybd layers from being reclaimed while a paused sandbox
    /// might still reopen them — and the deciding half has no local layers.
    /// The layers that must be kept are on the origin node, and they are kept
    /// by the origin node's own copy of this state.
    fn runtime_artifacts(&self) -> RuntimeArtifactSet {
        RuntimeArtifactSet::empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_encoding_carries_the_machine_the_bytes_are_on() {
        let state = RemotePausedState::new(
            "node-a".to_string(),
            "/var/lib/agentenv/paused/abc".to_string(),
            json!({"memory": "mem.json"}),
        );

        let encoded = state.encode().expect("encode");
        assert_eq!(encoded["origin_node_id"], "node-a");
        assert_eq!(encoded["artifact_root"], "/var/lib/agentenv/paused/abc");
        assert_eq!(encoded["state"]["memory"], "mem.json");
    }

    #[test]
    fn it_pins_no_local_artifacts() {
        let state = RemotePausedState::new("node-a".into(), "/tmp/x".into(), json!({}));
        assert!(state.runtime_artifacts().is_empty());
    }
}
