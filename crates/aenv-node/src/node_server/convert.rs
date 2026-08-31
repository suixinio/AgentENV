//! Fail-closed conversions between protobuf messages and orchestrator types.
//! Missing required protobuf values are rejected rather than defaulted.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tonic::Status;

use crate::orchestrator::{ControlPlaneConfig, LiveSandbox, SandboxExpiry, SandboxTimeoutAction};
use crate::proto::node as pb;
use crate::types::{ExecutionId, SandboxId};

use crate::proto::node::SERIALIZED_VALUE_VERSION;

pub fn sandbox_id(raw: &str) -> Result<SandboxId, Status> {
    if raw.is_empty() {
        return Err(Status::invalid_argument("sandbox_id is required"));
    }
    SandboxId::parse_str(raw)
        .map_err(|err| Status::invalid_argument(format!("sandbox_id {raw:?}: {err}")))
}

pub fn execution_id(raw: &str) -> Result<ExecutionId, Status> {
    if raw.is_empty() {
        return Err(Status::invalid_argument("execution_id is required"));
    }
    ExecutionId::parse_str(raw)
        .map_err(|err| Status::invalid_argument(format!("execution_id {raw:?}: {err}")))
}

/// Parses an optional caller-supplied execution ID; empty means absent.
pub fn optional_execution_id(raw: &str) -> Result<Option<ExecutionId>, Status> {
    if raw.is_empty() {
        return Ok(None);
    }
    execution_id(raw).map(Some)
}

/// Converts empty protobuf bytes to no ownership marker.
pub fn control_plane_config(raw: &[u8]) -> Option<ControlPlaneConfig> {
    ControlPlaneConfig::from_bytes(raw.to_vec())
}

pub fn timeout_action(raw: i32) -> Result<SandboxTimeoutAction, Status> {
    match pb::TimeoutAction::try_from(raw) {
        Ok(pb::TimeoutAction::Pause) => Ok(SandboxTimeoutAction::Pause),
        Ok(pb::TimeoutAction::Delete) => Ok(SandboxTimeoutAction::Delete),
        // The protobuf zero value does not select pause or delete.
        Ok(pb::TimeoutAction::Unspecified) => Err(Status::invalid_argument(
            "timeout_action is required: TIMEOUT_ACTION_UNSPECIFIED means the sender set nothing, \
             and pause and delete are not interchangeable",
        )),
        Err(_) => Err(Status::invalid_argument(format!(
            "timeout_action {raw} is not a value this build knows"
        ))),
    }
}

/// Decodes an optional versioned JSON blob.
pub fn serialized<T>(value: Option<&pb::SerializedValue>, what: &str) -> Result<Option<T>, Status>
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

/// Converts a nonzero resume/fork timeout; zero keeps the existing deadline.
pub fn optional_timeout(millis: u64) -> Option<Duration> {
    (millis > 0).then(|| Duration::from_millis(millis))
}

/// Converts the create-time deadline owner; an unset oneof is invalid.
pub fn create_expiry(
    expiry: Option<pb::sandbox_create_request::Expiry>,
) -> Result<SandboxExpiry, Status> {
    match expiry {
        Some(pb::sandbox_create_request::Expiry::NodeKeptTimeoutMs(0)) => {
            // A zero node-kept timeout is neither a default nor caller-kept deadline.
            Err(Status::invalid_argument(
                "node_kept_timeout_ms must be greater than zero: a node-kept deadline of zero \
                 expires the sandbox as it starts. Send node_kept_default for this node's \
                 configured default, or caller_kept to keep the deadline yourself",
            ))
        }
        Some(pb::sandbox_create_request::Expiry::NodeKeptTimeoutMs(millis)) => {
            Ok(SandboxExpiry::After(Duration::from_millis(millis)))
        }
        Some(pb::sandbox_create_request::Expiry::NodeKeptDefault(_)) => {
            Ok(SandboxExpiry::AfterConfiguredDefault)
        }
        Some(pb::sandbox_create_request::Expiry::CallerKept(_)) => Ok(SandboxExpiry::NotKeptHere),
        None => Err(Status::invalid_argument(
            "expiry is required: a create that says nothing about who keeps the sandbox's \
             deadline used to be read as \"use the node's default\", which silently gave a \
             caller that keeps its own deadline a second one on this node. Send \
             node_kept_timeout_ms, node_kept_default or caller_kept",
        )),
    }
}

pub fn unix_millis(at: Option<SystemTime>) -> i64 {
    at.and_then(|at| at.duration_since(UNIX_EPOCH).ok())
        .and_then(|since| i64::try_from(since.as_millis()).ok())
        .unwrap_or(0)
}

/// Renders a verified control-plane-owned sandbox for the wire.
pub fn node_sandbox(
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
    fn a_zero_timeout_on_a_resume_or_a_fork_names_no_new_deadline() {
        assert_eq!(optional_timeout(0), None);
        assert_eq!(optional_timeout(1_500), Some(Duration::from_millis(1_500)));
    }

    #[test]
    fn a_create_says_which_of_the_three_answers_about_its_deadline_it_means() {
        use pb::sandbox_create_request::Expiry;

        assert_eq!(
            create_expiry(Some(Expiry::NodeKeptTimeoutMs(600_000))).expect("a named deadline"),
            SandboxExpiry::After(Duration::from_secs(600))
        );
        assert_eq!(
            create_expiry(Some(Expiry::NodeKeptDefault(pb::NodeDefaultExpiry {})))
                .expect("the node's default"),
            SandboxExpiry::AfterConfiguredDefault
        );
        assert_eq!(
            create_expiry(Some(Expiry::CallerKept(pb::CallerKeptExpiry {})))
                .expect("the caller keeps it"),
            SandboxExpiry::NotKeptHere
        );

        let unset = create_expiry(None).unwrap_err();
        assert_eq!(unset.code(), tonic::Code::InvalidArgument, "{unset}");
        assert!(unset.message().contains("expiry is required"), "{unset}");

        let zero = create_expiry(Some(Expiry::NodeKeptTimeoutMs(0))).unwrap_err();
        assert_eq!(zero.code(), tonic::Code::InvalidArgument, "{zero}");
        assert!(zero.message().contains("greater than zero"), "{zero}");
    }

    #[test]
    fn the_wire_tells_the_three_expiry_answers_apart_and_from_saying_nothing() {
        use pb::sandbox_create_request::Expiry;
        use prost::Message;

        let encode = |expiry: Option<Expiry>| {
            pb::SandboxCreateRequest {
                expiry,
                ..Default::default()
            }
            .encode_to_vec()
        };

        let named = encode(Some(Expiry::NodeKeptTimeoutMs(600_000)));
        let zero = encode(Some(Expiry::NodeKeptTimeoutMs(0)));
        let default = encode(Some(Expiry::NodeKeptDefault(pb::NodeDefaultExpiry {})));
        let caller = encode(Some(Expiry::CallerKept(pb::CallerKeptExpiry {})));
        let silent = encode(None);

        assert!(silent.is_empty(), "an unset oneof is the empty message");
        for (what, bytes) in [
            ("a named deadline", &named),
            ("a node-kept zero", &zero),
            ("the node's default", &default),
            ("the caller's own", &caller),
        ] {
            assert!(
                !bytes.is_empty(),
                "{what} encoded to nothing, which is how it would be read as silence"
            );
        }
        assert_ne!(default, caller, "the two answers that used to be one value");
        assert_ne!(zero, default);
        assert_ne!(zero, caller);
        assert_ne!(named, default);

        for expiry in [
            Expiry::NodeKeptTimeoutMs(600_000),
            Expiry::NodeKeptTimeoutMs(0),
            Expiry::NodeKeptDefault(pb::NodeDefaultExpiry {}),
            Expiry::CallerKept(pb::CallerKeptExpiry {}),
        ] {
            let bytes = encode(Some(expiry));
            let decoded = pb::SandboxCreateRequest::decode(bytes.as_slice()).expect("decode");
            assert_eq!(decoded.expiry, Some(expiry));
        }
    }

    #[test]
    fn a_sandbox_with_no_record_still_renders_what_is_known() {
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
