pub mod scheduler {
    // Prost generates the oneof shape, including its unavoidable size spread.
    #![allow(clippy::large_enum_variant)]

    tonic::include_proto!("scheduler.v1");

    // Hand-written port of Go's `NodeStatus.CanAcceptNewRequests`.
    impl NodeStatus {
        /// Whether the node may receive work that starts a new VM.
        pub fn can_accept_new_requests(self) -> bool {
            matches!(self, NodeStatus::Ready)
        }
    }
}

pub mod node {
    //! Rust-only node-service protocol generated from `node.proto`.
    tonic::include_proto!("agentenv.node.v1");

    /// Schema version written and accepted for serialized values.
    pub const SERIALIZED_VALUE_VERSION: u32 = 2;

    /// Encodes a serde value with this build's schema version.
    pub fn encode_value<T: serde::Serialize>(
        value: &T,
    ) -> Result<SerializedValue, serde_json::Error> {
        Ok(SerializedValue {
            schema_version: SERIALIZED_VALUE_VERSION,
            json: serde_json::to_vec(value)?,
        })
    }

    /// Encodes capture terminality as structured status details.
    pub fn capture_failure_status(
        code: tonic::Code,
        message: impl Into<String>,
        terminal: bool,
        reason: impl Into<String>,
    ) -> tonic::Status {
        use prost::Message as _;

        let failure = SandboxCaptureFailure {
            terminal,
            reason: reason.into(),
        };
        tonic::Status::with_details(code, message, failure.encode_to_vec().into())
    }

    /// Encodes the failed template-build step as structured status details.
    pub fn build_failure_status(
        code: tonic::Code,
        message: impl Into<String>,
        step: Option<&str>,
    ) -> tonic::Status {
        use prost::Message as _;

        let detail = TemplateBuildFailureDetail {
            step: step.unwrap_or_default().to_string(),
        };
        tonic::Status::with_details(code, message, detail.encode_to_vec().into())
    }
}

#[cfg(test)]
mod node_wire_tests {
    use super::node as pb;
    use prost::Message as _;

    #[test]
    fn the_staging_fields_keep_their_wire_numbers() {
        // Tag 2, wire type 2: 2 << 3 | 2 == 0x12.
        let staged = pb::SandboxPauseResponse {
            staged: Some(pb::StagedSnapshot {
                value: Some(pb::SerializedValue {
                    schema_version: pb::SERIALIZED_VALUE_VERSION,
                    json: b"{}".to_vec(),
                }),
            }),
        };
        let bytes = staged.encode_to_vec();
        assert_eq!(
            bytes.first().copied(),
            Some(0x12),
            "SandboxPauseResponse.staged moved off field 2: {bytes:02x?}"
        );

        // Zero-valued messages encode to nothing, proving the prefixes are field-driven.
        assert!(pb::SandboxPauseResponse::default()
            .encode_to_vec()
            .is_empty());
        assert!(pb::SandboxPauseRequest::default()
            .encode_to_vec()
            .is_empty());

        let checkpoint = pb::SandboxCheckpointResponse {
            staged: Some(pb::StagedSnapshot { value: None }),
        };
        assert_eq!(
            checkpoint.encode_to_vec().first().copied(),
            Some(0x0a),
            "SandboxCheckpointResponse.staged moved off field 1"
        );
        assert!(pb::SandboxCheckpointResponse::default()
            .encode_to_vec()
            .is_empty());
    }

    #[test]
    fn the_unresolved_image_create_fields_keep_their_wire_numbers() {
        // Tag 5, wire type 2 (length-delimited string): 5 << 3 | 2 == 0x2a.
        let sub_path = pb::AttachedDrive {
            image_ref: String::new(),
            mount_path: String::new(),
            drive_id: String::new(),
            read_only: false,
            sub_path: "workspace/data".to_string(),
            virtual_size_bytes: 0,
        };
        assert_eq!(
            sub_path.encode_to_vec().first().copied(),
            Some(0x2a),
            "AttachedDrive.sub_path moved off field 5"
        );

        // Tag 6, wire type 0 (varint): 6 << 3 | 0 == 0x30.
        let virtual_size = pb::AttachedDrive {
            image_ref: String::new(),
            mount_path: String::new(),
            drive_id: String::new(),
            read_only: false,
            sub_path: String::new(),
            virtual_size_bytes: 2 * 1024 * 1024 * 1024,
        };
        assert_eq!(
            virtual_size.encode_to_vec().first().copied(),
            Some(0x30),
            "AttachedDrive.virtual_size_bytes moved off field 6"
        );

        assert!(pb::AttachedDrive::default().encode_to_vec().is_empty());

        // Tag 8, wire type 2 (length-delimited message): 8 << 3 | 2 == 0x42.
        let context = pb::SandboxCreateResponse {
            context: Some(pb::SerializedValue::default()),
            ..Default::default()
        };
        assert_eq!(
            context.encode_to_vec().first().copied(),
            Some(0x42),
            "SandboxCreateResponse.context moved off field 8"
        );

        // Tag 9, wire type 2 (length-delimited message): 9 << 3 | 2 == 0x4a.
        let image_configs = pb::SandboxCreateResponse {
            image_configs: Some(pb::SerializedValue::default()),
            ..Default::default()
        };
        assert_eq!(
            image_configs.encode_to_vec().first().copied(),
            Some(0x4a),
            "SandboxCreateResponse.image_configs moved off field 9"
        );

        assert!(pb::SandboxCreateResponse::default()
            .encode_to_vec()
            .is_empty());
    }
}

pub mod apiproxy {
    //! Resume RPC shared with the Go gateway.
    tonic::include_proto!("agentenv.apiproxy.v1");

    /// Trailer carrying the structured `FailedPrecondition` reason.
    pub const REFUSAL_REASON_TRAILER: &str = "x-agentenv-resume-refusal";

    /// Trailer carrying the pinned origin node for diagnostics.
    pub const REFUSAL_ORIGIN_TRAILER: &str = "x-agentenv-resume-origin-node";

    /// Metadata carrying the addressed data-plane port.
    pub const TARGET_PORT_METADATA: &str = "x-agentenv-target-port";

    /// Metadata carrying the envd credential without logging it in the message body.
    pub const ACCESS_TOKEN_METADATA: &str = "x-access-token";
}

#[cfg(test)]
mod serialized_value_golden {
    use serde_json::json;

    const BUMP: &str = "this type's serde encoding changed. Both halves of the split decode each \
                        other's blobs by this shape, and they are separate images now, so a \
                        change here without a matching SERIALIZED_VALUE_VERSION bump is a \
                        rolling upgrade in which one half silently misreads the other. If the \
                        change is intended: update this golden AND bump \
                        crate::proto::node::SERIALIZED_VALUE_VERSION";

    #[test]
    fn a_sandbox_network_policy_still_encodes_the_way_the_other_half_reads_it() {
        use std::collections::BTreeMap;

        use crate::sandbox::network::policy::{DomainRule, HeaderTransform};
        use crate::sandbox::{
            BaseSandboxNetworkPolicy, SandboxNetworkEgressPolicy, SandboxNetworkPolicy,
        };

        let rules = BTreeMap::from([(
            "api.openai.com".to_string(),
            vec![DomainRule {
                transform: HeaderTransform {
                    headers: BTreeMap::from([(
                        "authorization".to_string(),
                        "Bearer ${aenv.secrets.openai}".to_string(),
                    )]),
                },
            }],
        )]);
        // Derive `brokers` the way the api half does, so the golden pins the
        // shape the node actually receives.
        let egress = SandboxNetworkEgressPolicy::with_rules(
            Some(vec![
                "10.0.0.0/8".to_string(),
                "example.invalid".to_string(),
            ]),
            Some(vec!["192.168.0.0/16".to_string()]),
            Some(rules),
        )
        .expect("the golden policy is valid");
        let policy = SandboxNetworkPolicy {
            base_policy: BaseSandboxNetworkPolicy::Deny,
            egress,
        };

        let transform = json!({
            "transform": {"headers": {"authorization": "Bearer ${aenv.secrets.openai}"}}
        });
        assert_eq!(
            serde_json::to_value(&policy).expect("a policy serialises"),
            json!({
                "base_policy": "Deny",
                "egress": {
                    "allowed_cidrs": ["10.0.0.0/8"],
                    "allowed_domains": ["example.invalid"],
                    "denied_cidrs": ["192.168.0.0/16"],
                    "rules": {"api.openai.com": [transform]},
                    "brokers": [{
                        "port": 0,
                        "handler": "http",
                        "params": {"rules": {"api.openai.com": [transform]}},
                        "intercept": {"dports": [443]},
                    }],
                }
            }),
            "{BUMP}"
        );
    }

    #[test]
    fn a_policy_without_rules_still_omits_the_brokered_fields() {
        use crate::sandbox::{
            BaseSandboxNetworkPolicy, SandboxNetworkEgressPolicy, SandboxNetworkPolicy,
        };

        let policy = SandboxNetworkPolicy {
            base_policy: BaseSandboxNetworkPolicy::Default,
            egress: SandboxNetworkEgressPolicy::new(None, None).expect("an empty policy is valid"),
        };

        assert_eq!(
            serde_json::to_value(&policy).expect("a policy serialises"),
            json!({
                "base_policy": "Default",
                "egress": {
                    "allowed_cidrs": [],
                    "allowed_domains": [],
                    "denied_cidrs": [],
                }
            }),
            "{BUMP}"
        );
    }

    #[test]
    fn custom_extension_params_still_encode_the_way_the_other_half_reads_them() {
        use crate::types::CustomExtensionParams;

        let mut params = CustomExtensionParams::new();
        params.insert("opaque".to_string(), json!({"to": ["this", "layer"]}));

        assert_eq!(
            serde_json::to_value(&params).expect("params serialise"),
            json!({"opaque": {"to": ["this", "layer"]}}),
            "{BUMP}"
        );
    }

    #[test]
    fn a_command_context_still_encodes_the_way_the_other_half_reads_it() {
        use crate::snapshot::CommandContext;

        let context = CommandContext {
            env_vars: [("KEY".to_string(), "value".to_string())]
                .into_iter()
                .collect(),
            workdir: "/work".to_string(),
            user: Some("1000:1000".to_string()),
            exposed_ports: vec!["8080/tcp".to_string()],
            entrypoint: Some(vec!["/bin/sh".to_string()]),
            cmd: Some(vec!["-c".to_string(), "true".to_string()]),
            volumes: vec!["/data".to_string()],
            labels: [("owner".to_string(), "aenv".to_string())]
                .into_iter()
                .collect(),
        };

        assert_eq!(
            serde_json::to_value(&context).expect("a context serialises"),
            json!({
                "env_vars": {"KEY": "value"},
                "workdir": "/work",
                "user": "1000:1000",
                "exposed_ports": ["8080/tcp"],
                "entrypoint": ["/bin/sh"],
                "cmd": ["-c", "true"],
                "volumes": ["/data"],
                "labels": {"owner": "aenv"},
            }),
            "{BUMP}"
        );

        // Pin both default omission and explicitly renamed ImageConfigs fields.
        assert_eq!(
            serde_json::to_value(CommandContext::default()).expect("a default context serialises"),
            json!({"env_vars": {}, "workdir": "/"}),
            "{BUMP}"
        );
    }

    #[test]
    fn image_configs_still_encode_the_way_the_other_half_reads_them() {
        use crate::types::ImageConfigs;

        let mut configs = ImageConfigs::new();
        configs.add(None::<String>, "/", json!({"lowers": []}));
        configs.add(
            Some("data"),
            "/mnt/data",
            json!({"lowers": [{"dir": "/x"}]}),
        );

        assert_eq!(
            serde_json::to_value(&configs).expect("configs serialise"),
            json!([
                {"mountPath": "/", "config": {"lowers": []}},
                {
                    "driveId": "data",
                    "mountPath": "/mnt/data",
                    "config": {"lowers": [{"dir": "/x"}]}
                }
            ]),
            "{BUMP}"
        );
    }

    #[test]
    fn template_build_steps_still_encode_the_way_the_other_half_reads_them() {
        use crate::template::TemplateBuildStep;

        // Externally tagged variants make every variant name part of the wire shape.
        let steps = vec![
            TemplateBuildStep::run("echo hi"),
            TemplateBuildStep::env("KEY", "value"),
            TemplateBuildStep::workdir("/work"),
            TemplateBuildStep::user("1000"),
            TemplateBuildStep::exposed_port("8080/tcp"),
            TemplateBuildStep::volume("/data"),
            TemplateBuildStep::label("owner", "aenv"),
        ];

        assert_eq!(
            serde_json::to_value(&steps).expect("steps serialise"),
            json!([
                {"kind": {"Run": {"cmd": "echo hi"}}},
                {"kind": {"Env": {"key": "KEY", "value": "value"}}},
                {"kind": {"Workdir": {"path": "/work"}}},
                {"kind": {"User": {"value": "1000"}}},
                {"kind": {"ExposedPort": {"port": "8080/tcp"}}},
                {"kind": {"Volume": {"path": "/data"}}},
                {"kind": {"Label": {"key": "owner", "value": "aenv"}}},
            ]),
            "{BUMP}"
        );
    }
}

#[cfg(test)]
mod scheduler_wire_tests {
    use prost::Message as _;

    use super::scheduler as pb;

    /// Legacy hint shape used to exercise rolling-upgrade decoding.
    #[derive(Clone, PartialEq, prost::Message)]
    struct LegacyNewSandboxHint {
        #[prost(map = "string, string", tag = "1")]
        metadata: std::collections::HashMap<String, String>,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct LegacyScheduleRequestHint {
        #[prost(oneof = "LegacyKind", tags = "1, 2")]
        kind: Option<LegacyKind>,
    }

    #[derive(Clone, PartialEq, prost::Oneof)]
    enum LegacyKind {
        #[prost(message, tag = "1")]
        NewColdSandbox(LegacyNewColdSandboxHint),
        #[prost(message, tag = "2")]
        NewSandbox(LegacyNewSandboxHint),
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct LegacyNewColdSandboxHint {
        #[prost(uint32, tag = "1")]
        cpu_count: u32,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct LegacyScheduleRequest {
        #[prost(message, optional, tag = "2")]
        hint: Option<LegacyScheduleRequestHint>,
    }

    fn new_sandbox_request(cpu_count: Option<u32>, memory_mib: Option<u64>) -> pb::ScheduleRequest {
        pb::ScheduleRequest {
            hint: Some(pb::ScheduleRequestHint {
                kind: Some(pb::schedule_request_hint::Kind::NewSandbox(
                    pb::NewSandboxHint {
                        requires_egress_broker: false,
                        metadata: Default::default(),
                        cpu_count,
                        memory_mib,
                        preferred_node_id: String::new(),
                        excluded_node_ids: Vec::new(),
                    },
                )),
            }),
        }
    }

    #[test]
    fn new_sandbox_hint_resource_fields_keep_their_wire_shape() {
        const ABSENT: &[u8] = &[0x12, 0x02, 0x12, 0x00];
        const STATED: &[u8] = &[0x12, 0x07, 0x12, 0x05, 0x10, 0x02, 0x18, 0x80, 0x04];
        const EXPLICIT_ZEROES: &[u8] = &[0x12, 0x06, 0x12, 0x04, 0x10, 0x00, 0x18, 0x00];

        assert_eq!(new_sandbox_request(None, None).encode_to_vec(), ABSENT);
        assert_eq!(
            new_sandbox_request(Some(2), Some(512)).encode_to_vec(),
            STATED
        );
        assert_eq!(
            new_sandbox_request(Some(0), Some(0)).encode_to_vec(),
            EXPLICIT_ZEROES
        );

        // Absence and explicit zero must remain distinct on decode.
        let decoded = pb::ScheduleRequest::decode(ABSENT).expect("decodes");
        let pb::schedule_request_hint::Kind::NewSandbox(hint) = decoded
            .hint
            .expect("the hint survives")
            .kind
            .expect("the oneof survives")
        else {
            panic!("the absent case must still decode as new_sandbox");
        };
        assert_eq!(hint.cpu_count, None);
        assert_eq!(hint.memory_mib, None);

        let decoded = pb::ScheduleRequest::decode(EXPLICIT_ZEROES).expect("decodes");
        let pb::schedule_request_hint::Kind::NewSandbox(hint) = decoded
            .hint
            .expect("the hint survives")
            .kind
            .expect("the oneof survives")
        else {
            panic!("the explicit-zero case must still decode as new_sandbox");
        };
        assert_eq!(hint.cpu_count, Some(0));
        assert_eq!(hint.memory_mib, Some(0));
    }

    #[test]
    fn a_pre_resource_peer_still_reads_a_hint_that_carries_them() {
        const STATED: &[u8] = &[0x12, 0x07, 0x12, 0x05, 0x10, 0x02, 0x18, 0x80, 0x04];

        let legacy = LegacyScheduleRequest::decode(STATED)
            .expect("an older build must not fail on the new fields");
        let Some(LegacyKind::NewSandbox(hint)) = legacy.hint.expect("the hint survives").kind
        else {
            panic!("the older build must still see a new_sandbox hint");
        };
        assert!(
            hint.metadata.is_empty(),
            "and must not have mistaken a resource field for metadata"
        );
    }

    #[test]
    fn an_unknown_oneof_tag_decodes_to_a_present_hint_with_no_kind() {
        let decoded = pb::ScheduleRequest::decode(&[0x12u8, 0x02, 0x1a, 0x00][..])
            .expect("an unknown oneof tag must not fail the decode");
        let hint = decoded.hint.expect("the outer hint is present");
        assert!(
            hint.kind.is_none(),
            "and its kind is empty, which is a shape the mapping has to answer for"
        );
    }
}
