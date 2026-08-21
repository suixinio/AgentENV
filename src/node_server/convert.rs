//! Turning wire messages into the orchestrator's own types, and back.
//!
//! 🔴 Every conversion in here is fallible in the direction that matters. A
//! protobuf message has no required fields: an id that was never set arrives as
//! an empty string, an enum that was never set arrives as its zero value, and a
//! nested message that was never set arrives as `None`. Accepting those as
//! defaults is how a request that meant nothing gets acted on, so each one is
//! named and refused here rather than allowed to become a plausible value
//! further in.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tonic::Status;

use crate::orchestrator::{ControlPlaneConfig, LiveSandbox, SandboxTimeoutAction};
use crate::proto::node as pb;
use crate::types::{ExecutionId, SandboxId};

use crate::proto::node::SERIALIZED_VALUE_VERSION;

pub(super) fn sandbox_id(raw: &str) -> Result<SandboxId, Status> {
    if raw.is_empty() {
        return Err(Status::invalid_argument("sandbox_id is required"));
    }
    SandboxId::parse_str(raw)
        .map_err(|err| Status::invalid_argument(format!("sandbox_id {raw:?}: {err}")))
}

pub(super) fn execution_id(raw: &str) -> Result<ExecutionId, Status> {
    if raw.is_empty() {
        return Err(Status::invalid_argument("execution_id is required"));
    }
    ExecutionId::parse_str(raw)
        .map_err(|err| Status::invalid_argument(format!("execution_id {raw:?}: {err}")))
}

/// An incarnation the caller minted, if it minted one.
///
/// 🔴 Empty means "you choose", and it is the only reading available: the wire
/// has no null string. What must not happen is an empty value being parsed into
/// something — a caller that sent nothing gets a fresh incarnation and is told
/// which one, and a caller that sent a malformed one is refused rather than
/// given a different sandbox than it asked for.
pub(super) fn optional_execution_id(raw: &str) -> Result<Option<ExecutionId>, Status> {
    if raw.is_empty() {
        return Ok(None);
    }
    execution_id(raw).map(Some)
}

/// The ownership marker a request carried, if it carried one.
///
/// 🔴 Zero bytes is `None`, not `Some(empty)`. Protobuf `bytes` cannot tell an
/// unset field from an empty one, and the two possible readings are "the
/// control plane does not own this" and "the control plane owns this but said
/// nothing about it" — the second of which is not a thing. Collapsing them at
/// the boundary is what keeps `Option` two-valued everywhere behind it.
pub(super) fn control_plane_config(raw: &[u8]) -> Option<ControlPlaneConfig> {
    ControlPlaneConfig::from_bytes(raw.to_vec())
}

pub(super) fn timeout_action(raw: i32) -> Result<SandboxTimeoutAction, Status> {
    match pb::TimeoutAction::try_from(raw) {
        Ok(pb::TimeoutAction::Pause) => Ok(SandboxTimeoutAction::Pause),
        Ok(pb::TimeoutAction::Delete) => Ok(SandboxTimeoutAction::Delete),
        // 🔴 Not defaulted to `Pause`. proto3 gives every enum a zero value
        // whether the sender meant it or not, and the two actions differ by
        // whether a user's sandbox is kept or destroyed when its timeout
        // elapses. A caller that did not say must be told it did not say.
        Ok(pb::TimeoutAction::Unspecified) => Err(Status::invalid_argument(
            "timeout_action is required: TIMEOUT_ACTION_UNSPECIFIED means the sender set nothing, \
             and pause and delete are not interchangeable",
        )),
        Err(_) => Err(Status::invalid_argument(format!(
            "timeout_action {raw} is not a value this build knows"
        ))),
    }
}

/// Decodes a serde value carried as a versioned blob.
///
/// `None` for an absent message, which is how "the caller supplied nothing" is
/// spelled for every optional document on this service.
pub(super) fn serialized<T>(
    value: Option<&pb::SerializedValue>,
    what: &str,
) -> Result<Option<T>, Status>
where
    T: serde::de::DeserializeOwned,
{
    let Some(value) = value else {
        return Ok(None);
    };
    if value.schema_version != SERIALIZED_VALUE_VERSION {
        return Err(Status::invalid_argument(format!(
            "{what} was encoded with schema version {}, and this build only reads version {}",
            value.schema_version, SERIALIZED_VALUE_VERSION
        )));
    }
    if value.json.is_empty() {
        return Ok(None);
    }
    serde_json::from_slice(&value.json)
        .map(Some)
        .map_err(|err| Status::invalid_argument(format!("{what}: {err}")))
}

/// 0 means "the node's default", and never "no timeout".
pub(super) fn timeout(millis: u64) -> Option<Duration> {
    (millis > 0).then(|| Duration::from_millis(millis))
}

pub(super) fn unix_millis(at: Option<SystemTime>) -> i64 {
    at.and_then(|at| at.duration_since(UNIX_EPOCH).ok())
        .and_then(|since| i64::try_from(since.as_millis()).ok())
        .unwrap_or(0)
}

/// Renders one live sandbox for the wire.
///
/// 🔴 Takes the marker separately from the sandbox, and by reference to a value
/// that exists. The caller has already established that this sandbox is the
/// control plane's; passing the marker in rather than re-reading it from the
/// `Option` is what stops a future edit from quietly emitting an entry for a
/// sandbox that has none.
pub(super) fn node_sandbox(
    sandbox: &LiveSandbox,
    control_plane_config: &ControlPlaneConfig,
    node_id: &str,
) -> pb::NodeSandbox {
    pb::NodeSandbox {
        sandbox_id: sandbox.sandbox_id.to_string(),
        execution_id: sandbox
            .execution_id
            .map(|execution_id| execution_id.to_string())
            .unwrap_or_default(),
        node_id: node_id.to_string(),
        started_at_ms: unix_millis(sandbox.created_at),
        expires_at_ms: unix_millis(sandbox.expires_at),
        vcpu: sandbox
            .resources
            .map(|resources| resources.cpu_count)
            .unwrap_or_default(),
        ram_mb: sandbox
            .resources
            .map(|resources| u64::from(resources.memory_mib))
            .unwrap_or_default(),
        host_interaction_ip: sandbox
            .host_interaction_ip
            .map(|ip| ip.to_string())
            .unwrap_or_default(),
        rootfs_virtual_size: sandbox.rootfs_virtual_size.unwrap_or_default(),
        control_plane_config: control_plane_config.as_bytes().to_vec(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::SandboxNetworkPolicy;
    use crate::types::SandboxResources;

    #[test]
    fn an_unset_id_is_refused_rather_than_defaulted() {
        for err in [
            sandbox_id("").unwrap_err(),
            sandbox_id("not-a-uuid").unwrap_err(),
        ] {
            assert_eq!(err.code(), tonic::Code::InvalidArgument, "{err}");
        }
        for err in [
            execution_id("").unwrap_err(),
            execution_id("not-a-uuid").unwrap_err(),
        ] {
            assert_eq!(err.code(), tonic::Code::InvalidArgument, "{err}");
        }
    }

    #[test]
    fn an_unset_timeout_action_is_refused() {
        // 🔴 The control probe: the two real values do convert, so this is not
        // a function that refuses everything.
        assert!(matches!(
            timeout_action(pb::TimeoutAction::Pause as i32),
            Ok(SandboxTimeoutAction::Pause)
        ));
        assert!(matches!(
            timeout_action(pb::TimeoutAction::Delete as i32),
            Ok(SandboxTimeoutAction::Delete)
        ));

        let err = timeout_action(pb::TimeoutAction::Unspecified as i32).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        let err = timeout_action(41).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn a_blob_from_another_schema_version_is_refused() {
        let policy = SandboxNetworkPolicy::default();
        let encoded = crate::proto::node::encode_value(&policy).expect("encode");
        let decoded: Option<SandboxNetworkPolicy> =
            serialized(Some(&encoded), "network policy").expect("decode");
        assert_eq!(decoded, Some(policy));

        let stale = pb::SerializedValue {
            schema_version: SERIALIZED_VALUE_VERSION + 1,
            ..encoded
        };
        let err = serialized::<SandboxNetworkPolicy>(Some(&stale), "network policy").unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("schema version"), "{err}");
    }

    #[test]
    fn a_zero_timeout_means_the_nodes_default_and_not_forever() {
        assert_eq!(timeout(0), None);
        assert_eq!(timeout(1_500), Some(Duration::from_millis(1_500)));
    }

    #[test]
    fn a_sandbox_with_no_record_still_renders_what_is_known() {
        // The fields a record would have supplied come back as the wire's own
        // zero values, which is what "unset" is on this transport. What must
        // not happen is the entry disappearing.
        let marker = ControlPlaneConfig::from_bytes(b"owned".to_vec()).expect("non-empty");
        let sandbox = LiveSandbox {
            sandbox_id: SandboxId::new(),
            execution_id: None,
            facts_from_handle: false,
            host_interaction_ip: None,
            rootfs_virtual_size: None,
            created_at: None,
            expires_at: None,
            resources: None,
            control_plane_config: Some(marker.clone()),
        };

        let rendered = node_sandbox(&sandbox, &marker, "node-a");
        assert_eq!(rendered.sandbox_id, sandbox.sandbox_id.to_string());
        assert_eq!(rendered.node_id, "node-a");
        assert!(rendered.execution_id.is_empty());
        assert_eq!(rendered.expires_at_ms, 0);
        assert_eq!(rendered.control_plane_config, b"owned");
    }

    #[test]
    fn the_facts_a_record_supplies_reach_the_wire() {
        let marker = ControlPlaneConfig::from_bytes(b"owned".to_vec()).expect("non-empty");
        let execution_id = ExecutionId::new();
        let sandbox = LiveSandbox {
            sandbox_id: SandboxId::new(),
            execution_id: Some(execution_id),
            facts_from_handle: true,
            host_interaction_ip: Some(std::net::Ipv4Addr::new(10, 0, 0, 7)),
            rootfs_virtual_size: Some(4096),
            created_at: Some(UNIX_EPOCH + Duration::from_millis(1_700_000_000_123)),
            expires_at: Some(UNIX_EPOCH + Duration::from_millis(1_700_000_060_000)),
            resources: Some(SandboxResources {
                cpu_count: 4,
                memory_mib: 2048,
                disk_size_mib: 10240,
            }),
            control_plane_config: Some(marker.clone()),
        };

        let rendered = node_sandbox(&sandbox, &marker, "node-b");
        assert_eq!(rendered.execution_id, execution_id.to_string());
        assert_eq!(rendered.started_at_ms, 1_700_000_000_123);
        assert_eq!(rendered.expires_at_ms, 1_700_000_060_000);
        assert_eq!(rendered.vcpu, 4);
        assert_eq!(rendered.ram_mb, 2048);
        assert_eq!(rendered.host_interaction_ip, "10.0.0.7");
        assert_eq!(rendered.rootfs_virtual_size, 4096);
    }
}
