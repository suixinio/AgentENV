pub mod scheduler {
    // Generated code. prost expands a `oneof` into an enum whose variants are
    // whatever the messages behind them weigh, so their size spread is decided
    // by the .proto and not by anything reachable from here: `AcquireSandbox`'s
    // outcome carries a whole registry row in one arm and a node id in the
    // others, and adding a field to the row widens the gap. Boxing it is not
    // ours to do — the file is regenerated on every build.
    #![allow(clippy::large_enum_variant)]

    tonic::include_proto!("scheduler.v1");

    // Hand-written, mirroring `services/api/proto/node_status.go`: a method
    // can only be attached to `NodeStatus` from the module that declares it,
    // and codegen would clobber anything placed in the generated file itself.
    //
    // Port of `NodeStatus.CanAcceptNewRequests()` — see the Go doc comment on
    // that method for the full rationale (`node_status.go`). Kept in lockstep
    // with `src/node_registry/filter.rs`'s `FilterUnschedulable` port, the
    // one caller in this codebase so far.
    impl NodeStatus {
        /// Reports whether a node in this status may be given new work — a
        /// fresh sandbox, a fork, or a resume that would start a VM there.
        ///
        /// Answers "may I send this node something new", not "is this node
        /// still working": a draining node keeps serving what it already
        /// holds, so paths that act on existing sandboxes must not gate on
        /// this.
        ///
        /// `Unspecified` deliberately answers `false` — it is the zero value,
        /// so a caller with no snapshot at all must decide for itself whether
        /// a node that has never reported is a candidate, rather than getting
        /// an accidental "yes" from a missing field.
        pub fn can_accept_new_requests(self) -> bool {
            matches!(self, NodeStatus::Ready)
        }
    }
}

pub mod node {
    //! The node service: what one node accepts from the API half.
    //!
    //! Generated from `services/api/proto/node.proto`. Unlike the scheduler
    //! proto next to it, this one has no Go consumer — see the note at the top
    //! of the file for why that is deliberate.
    tonic::include_proto!("agentenv.node.v1");

    /// The schema version written into every [`SerializedValue`] on this
    /// service, and the only one this build accepts.
    ///
    /// 🔴 Checked rather than ignored on the way in. serde decodes a document
    /// that has lost a field it has a default for without complaint, so a
    /// version mismatch that was not refused would surface as a sandbox
    /// running under the wrong network policy rather than as an error.
    pub const SERIALIZED_VALUE_VERSION: u32 = 1;

    /// Encodes a serde value for the wire, stamped with the schema version
    /// this build writes.
    pub fn encode_value<T: serde::Serialize>(
        value: &T,
    ) -> Result<SerializedValue, serde_json::Error> {
        Ok(SerializedValue {
            schema_version: SERIALIZED_VALUE_VERSION,
            json: serde_json::to_vec(value)?,
        })
    }

    /// Builds the status a node returns for a failed capture.
    ///
    /// 🔴 The classification travels in the details and never in the code:
    /// `Internal` is produced by both kinds of capture failure and by the
    /// transport itself, so a caller reading the code alone would be guessing
    /// about the one thing it must not guess about — whether the live runtime
    /// was mutated past safe resume.
    ///
    /// Lives beside the decoder's schema on purpose: a classification written
    /// in one file and read in another is a classification that drifts.
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

    /// Builds the status a node returns for a failed `BuildTemplate` call.
    ///
    /// 🔴 The step travels in the details and never folded into `message`:
    /// `TemplateBuildErrorReason.step` (`src/snapshot/types/snapshot.rs`) is
    /// read back structurally by a template's build-status API
    /// (`models::BuildStatusReason.step`), and a caller that had to re-parse
    /// `message` to recover it would be one string-format change away from
    /// losing it silently.
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

    /// The staging fields keep the numbers they were assigned.
    ///
    /// # 🔴 Why a test and not a comment
    ///
    /// `cargo mutants` does not reach a `.proto`: nothing it can change in the
    /// Rust tree alters a field number, so the whole class of "someone
    /// renumbered a field" is invisible to every other check in this
    /// repository. It is also the class with the worst failure mode. Both ends
    /// of this service are Rust and are built from the same file, so a
    /// renumbering compiles, passes every type check, and only misbehaves
    /// against a *peer that was built before it* — a rolling upgrade, which is
    /// how this control plane is deployed.
    ///
    /// 🔴 Encoded and decoded rather than compared field-by-field, because the
    /// number is the only thing on the wire. A struct-level assertion would
    /// still pass after a renumbering; a byte-level one is what a peer sees.
    #[test]
    fn the_staging_fields_keep_their_wire_numbers() {
        // Tag 3, wire type 2 (length-delimited): 3 << 3 | 2 == 0x1a.
        let paused = pb::SandboxPauseResponse {
            paused_state: None,
            staged: None,
            staging_error: "no room on the device".to_string(),
        };
        let bytes = paused.encode_to_vec();
        assert_eq!(
            bytes.first().copied(),
            Some(0x1a),
            "SandboxPauseResponse.staging_error moved off field 3: {bytes:02x?}"
        );

        // Tag 2, wire type 2: 2 << 3 | 2 == 0x12.
        let staged = pb::SandboxPauseResponse {
            paused_state: None,
            staged: Some(pb::StagedSnapshot {
                value: Some(pb::SerializedValue {
                    schema_version: pb::SERIALIZED_VALUE_VERSION,
                    json: b"{}".to_vec(),
                }),
            }),
            staging_error: String::new(),
        };
        let bytes = staged.encode_to_vec();
        assert_eq!(
            bytes.first().copied(),
            Some(0x12),
            "SandboxPauseResponse.staged moved off field 2: {bytes:02x?}"
        );

        // Tag 1, wire type 0 (varint): 1 << 3 | 0 == 0x08.
        let asked = pb::SandboxPauseRequest {
            sandbox_id: String::new(),
            execution_id: String::new(),
            publish: true,
        };
        let bytes = asked.encode_to_vec();
        assert_eq!(
            bytes,
            vec![0x18, 0x01],
            "SandboxPauseRequest.publish moved off field 3, or stopped being a bool: {bytes:02x?}"
        );

        // 🔴 The contrast that makes the three above mean something: the same
        // encoder, on the same messages, with the fields at their zero values,
        // writes nothing at all. Without it every assertion here is satisfiable
        // by a build that emits a fixed prefix regardless of content.
        assert!(pb::SandboxPauseResponse::default()
            .encode_to_vec()
            .is_empty());
        assert!(pb::SandboxPauseRequest::default()
            .encode_to_vec()
            .is_empty());

        // And a checkpoint's whole product is field 1.
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

    /// The fields added for an unresolved-image `Create` — `AttachedDrive`'s
    /// `sub_path`/`virtual_size_bytes` and `SandboxCreateResponse`'s
    /// `context`/`image_configs` — keep the numbers they were assigned.
    ///
    /// 🔴 Same rationale as `the_staging_fields_keep_their_wire_numbers`
    /// above: nothing that touches the Rust tree alone can catch a
    /// renumbering here, both ends of this service are built from the same
    /// file, and a renumbering only misbehaves against a peer built before
    /// it — a rolling upgrade, which is how this control plane deploys.
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

    /// A peer built before `staging_error` existed still parses a reply that
    /// carries one, and reads it as the pause it is.
    ///
    /// 🔴 The reason the field was added rather than the failure being folded
    /// into the call's status: a node and an API replica are upgraded
    /// separately, so for the length of every rollout one end is older than the
    /// other. An older reader must not choke on the field, and must not mistake
    /// its presence for a row.
    #[test]
    fn an_older_reader_still_parses_a_reply_carrying_a_staging_error() {
        let bytes = pb::SandboxPauseResponse {
            paused_state: Some(pb::PausedState {
                artifact_root: "/var/lib/agentenv/paused/7".to_string(),
                state: None,
            }),
            staged: None,
            staging_error: "no room on the device".to_string(),
        }
        .encode_to_vec();

        // The shape an older build compiles: the same message without field 3.
        #[derive(prost::Message)]
        struct OlderPauseResponse {
            #[prost(message, optional, tag = "1")]
            paused_state: Option<pb::PausedState>,
            #[prost(message, optional, tag = "2")]
            staged: Option<pb::StagedSnapshot>,
        }

        let older = OlderPauseResponse::decode(bytes.as_slice())
            .expect("an older reader must not choke on a field it has never heard of");
        assert_eq!(
            older
                .paused_state
                .expect("the pause it can read is still there")
                .artifact_root,
            "/var/lib/agentenv/paused/7"
        );
        assert!(
            older.staged.is_none(),
            "an older reader read a staging failure as a row"
        );
    }
}

pub mod apiproxy {
    //! The one RPC the data plane asks the control plane for.
    //!
    //! Generated from `services/api/proto/apiproxy/apiproxy.proto`. Unlike
    //! `node` above, this one *does* have a Go consumer — the gateway is the
    //! only caller — so the same file is compiled twice, here and by
    //! `services/Makefile`.
    tonic::include_proto!("agentenv.apiproxy.v1");

    /// Trailer carrying why a `FailedPrecondition` refusal happened.
    ///
    /// 🔴 A trailer rather than a status detail, and the reason is interop, not
    /// taste: `Status::with_details` writes raw bytes into
    /// `grpc-status-details-bin`, while Go's `status.Details()` reads that
    /// trailer as a marshalled `google.rpc.Status`. The two do not meet, so a
    /// detail would arrive at the gateway as an undecodable blob. `node.proto`
    /// can use details because both of its ends are Rust.
    pub const REFUSAL_REASON_TRAILER: &str = "x-agentenv-resume-refusal";

    /// Trailer carrying the node a pinned refusal named, for the operator
    /// looking at the gateway's log rather than at the API half's.
    pub const REFUSAL_ORIGIN_TRAILER: &str = "x-agentenv-resume-origin-node";

    /// Metadata key carrying the port the data-plane request was addressed to.
    ///
    /// Spelled the same as the HTTP header the local reverse proxy reads, on
    /// purpose: it is the same fact travelling one hop further.
    pub const TARGET_PORT_METADATA: &str = "x-agentenv-target-port";

    /// Metadata key carrying the caller's envd access token.
    ///
    /// 🔴 Metadata and not a proto field, copying e2b
    /// (`paused_sandbox_resumer_grpc.go`). A credential in a message body ends
    /// up in every request log that prints the request.
    pub const ACCESS_TOKEN_METADATA: &str = "x-access-token";
}

/// 🔴 The wire encoding of every Rust type `SerializedValue` carries, pinned.
///
/// # What this exists to catch, and what it does not
///
/// `SerializedValue.schema_version` refuses a blob written by a *different*
/// version. It cannot notice a blob written by a different *schema* under the
/// same version — which is what a renamed field, a changed `#[serde(rename)]`,
/// a variant reordering on an externally-tagged enum, or a newly-`skip`ped
/// field all produce. Both halves would say "version 1" and mean two things.
///
/// That was survivable while one binary served both roles: the two sides were
/// compiled from one commit, so they could not disagree. Since the crate split
/// they are two binaries in two images, and a schema change that nobody thought
/// to pair with a version bump is a real rolling-upgrade hazard —
/// `node.proto`'s own doc on [`node::SerializedValue`] states the deployment
/// rule that follows from it.
///
/// So each carried type's encoding is written down here. Changing one of these
/// types fails this test, and the failure says the one thing a reader needs:
/// **if the change is intended, bump
/// [`SERIALIZED_VALUE_VERSION`][node::SERIALIZED_VALUE_VERSION] with it**.
///
/// 🔴 Coverage is partial and stated rather than implied. Pinned here:
/// `SandboxNetworkPolicy`, `CustomExtensionParams`, `CommandContext`,
/// `ImageConfigs` and `Vec<TemplateBuildStep>` — the five carried types whose
/// shape is entirely their own. Not pinned: `SnapshotRecord` and
/// `StagedSnapshot` (their encodings are the snapshot catalog's, already
/// exercised end to end by the repository round-trip tests), and the paused
/// state, which is deliberately opaque to this layer — it is the sandbox
/// backend's own document, and pinning it here would be this module asserting
/// something it is not allowed to know.
#[cfg(test)]
mod serialized_value_golden {
    use serde_json::json;

    /// The one sentence every failure in this module should be read with.
    const BUMP: &str = "this type's serde encoding changed. Both halves of the split decode each \
                        other's blobs by this shape, and they are separate images now, so a \
                        change here without a matching SERIALIZED_VALUE_VERSION bump is a \
                        rolling upgrade in which one half silently misreads the other. If the \
                        change is intended: update this golden AND bump \
                        crate::proto::node::SERIALIZED_VALUE_VERSION";

    #[test]
    fn a_sandbox_network_policy_still_encodes_the_way_the_other_half_reads_it() {
        use crate::sandbox::{
            BaseSandboxNetworkPolicy, SandboxNetworkEgressPolicy, SandboxNetworkPolicy,
        };

        let policy = SandboxNetworkPolicy {
            base_policy: BaseSandboxNetworkPolicy::Deny,
            egress: SandboxNetworkEgressPolicy {
                allowed_cidrs: vec!["10.0.0.0/8".to_string()],
                allowed_domains: vec!["example.invalid".to_string()],
                denied_cidrs: vec!["192.168.0.0/16".to_string()],
            },
        };

        assert_eq!(
            serde_json::to_value(&policy).expect("a policy serialises"),
            json!({
                "base_policy": "Deny",
                "egress": {
                    "allowed_cidrs": ["10.0.0.0/8"],
                    "allowed_domains": ["example.invalid"],
                    "denied_cidrs": ["192.168.0.0/16"],
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

        // 🔴 And the default shape too: every optional field here is
        // `skip_serializing_if`, so what the other half receives for a default
        // context is a *shorter document*, not one full of nulls. A field
        // losing or gaining that attribute is exactly the kind of change the
        // version counter cannot see.
        //
        // 🔴 `ImageConfigs` below is the counter-example in the same family:
        // its fields are `#[serde(rename)]`d to camelCase one at a time, so its
        // wire names do not match its Rust names at all. Two carried types, two
        // conventions — which is precisely why each is written down rather than
        // assumed.
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

        // 🔴 Every variant, not a representative one. `TemplateBuildStepKind`
        // is externally tagged, so each variant's tag *is* its Rust variant
        // name: renaming one is a wire change that compiles everywhere.
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

    /// The `NewSandboxHint` shape as it was **before** `cpu_count` and
    /// `memory_mib` existed: metadata on field 1, and nothing else.
    ///
    /// 🔴 Declaring it here, by hand, is the whole point. Encoding with the
    /// new type and decoding with the new type proves only that prost is
    /// self-consistent — it cannot fail. What a rolling upgrade actually
    /// does is hand new bytes to a peer built from the old `.proto`, and
    /// the only way to test that in one process is to keep the old shape
    /// alive as a separate type.
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

    /// Only what the oneof needs to be well-formed; the cold hint is not
    /// what this test is about.
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
                        metadata: Default::default(),
                        cpu_count,
                        memory_mib,
                    },
                )),
            }),
        }
    }

    /// `NewSandboxHint`'s two resource fields, byte by byte, in both
    /// directions.
    ///
    /// # 🔴 Why bytes and not a round trip
    ///
    /// This control plane deploys as a rolling upgrade of two halves built
    /// from one `.proto`, so a field-number or wire-type mistake compiles,
    /// type-checks, and passes every same-build test — it only misbehaves
    /// against the peer that has not restarted yet. The three sequences
    /// below are what a peer sees.
    ///
    /// - `12 02 12 00` — neither field set. The hint is still present and
    ///   still `new_sandbox`; the two optional fields write nothing.
    /// - `12 07 12 05 10 02 18 80 04` — `cpu_count = 2` (tag 2, varint) and
    ///   `memory_mib = 512` (tag 3, varint `80 04`).
    /// - `12 06 12 04 10 00 18 00` — both set to an explicit zero. 🔴 This
    ///   is the sequence that proves the fields carry presence: without
    ///   `optional`, proto3 elides zero-valued scalars and these bytes
    ///   would be identical to the first case.
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

        // And back: absence and explicit zero must not collapse into each
        // other on the way in either.
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

    /// The rolling-upgrade direction that matters: a peer built before
    /// these fields existed reads a message that carries them.
    ///
    /// It must still recognise `new_sandbox` — that is, tags 2 and 3 must
    /// be skipped as unknown fields inside the hint, not mistaken for a
    /// different `oneof` arm and not fatal to the decode.
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

    /// `Some(hint { kind: None })` is reachable, and `request_from_hint`'s
    /// table has a row for it because of this: a peer that assigns a third
    /// `oneof` tag sends a hint this build cannot name.
    ///
    /// `12 02 1a 00` is `ScheduleRequest.hint` wrapping a single unknown
    /// length-delimited field 3.
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
