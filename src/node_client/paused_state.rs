//! A paused sandbox as the deciding half holds it: not a handle, a reference.

use anyhow::Result;
use serde_json::{json, Value};

use crate::sandbox::{PausedSandboxState, RuntimeArtifactSet};
use crate::types::ExecutionId;

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
///
/// # 🔴 Why the incarnation is part of it
///
/// The origin node says *which machine* to ask; this says *which capture* to
/// ask it for. A node keeps its paused record until something tells it the
/// cluster has moved on, so a sandbox paused here, resumed elsewhere and paused
/// there leaves a stale record behind — and a resume that named only the
/// sandbox would reopen the run the user abandoned two runs ago while their
/// newer work sat on another disk. It travels as the fence on the resume call,
/// which is how every other command on that service says which run it means.
#[derive(Clone, Debug)]
pub struct RemotePausedState {
    origin_node_id: String,
    artifact_root: String,
    /// The run the capture is *of*, not the run a resume will start.
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

    /// The machine whose disk holds this sandbox's paused artifacts.
    pub fn origin_node_id(&self) -> &str {
        &self.origin_node_id
    }

    pub fn artifact_root(&self) -> &str {
        &self.artifact_root
    }

    /// The run this capture was taken from.
    pub fn paused_execution_id(&self) -> ExecutionId {
        self.paused_execution_id
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
            "execution_id": self.paused_execution_id.to_string(),
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

    /// The origin node, which is the whole reason this type exists.
    ///
    /// 🔴 The one implementation that answers this at all. Everything else
    /// captures on the machine it runs on and says `None`; this state is the
    /// one that was produced somewhere else, and a caller deciding what a pause
    /// left behind has to be able to tell those apart without knowing which
    /// backend produced the value.
    fn holding_node_id(&self) -> Option<&str> {
        Some(&self.origin_node_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A capture taken on the machine holding it: what every backend but this
    /// one produces, standing in for them so both answers come from the same
    /// trait in the same run.
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

    /// 🔴 Both halves in one run, through one trait. The caller deciding what a
    /// pause left behind reads this through `dyn PausedSandboxState` and cannot
    /// see which backend answered, so "names a machine" is only a fact if
    /// something in the same run answers `None` — otherwise a default that
    /// started returning some node would pass unnoticed and every local pause
    /// would be reported as parked on another machine.
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

    /// 🔴 The encoding says which run it captured, and it is the run that was
    /// paused rather than any other one in scope.
    ///
    /// The control face is a second state that differs from the first in that
    /// one value and in nothing else: if `encode` ever wrote a constant, an
    /// empty string, or the wrong field, the two encodings would agree here.
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
