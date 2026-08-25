pub(crate) mod scheduler {
    // Generated code. prost expands a `oneof` into an enum whose variants are
    // whatever the messages behind them weigh, so their size spread is decided
    // by the .proto and not by anything reachable from here: `AcquireSandbox`'s
    // outcome carries a whole registry row in one arm and a node id in the
    // others, and adding a field to the row widens the gap. Boxing it is not
    // ours to do — the file is regenerated on every build.
    #![allow(clippy::large_enum_variant)]

    tonic::include_proto!("scheduler.v1");
}

pub(crate) mod node {
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
    pub(crate) const SERIALIZED_VALUE_VERSION: u32 = 1;

    /// Encodes a serde value for the wire, stamped with the schema version
    /// this build writes.
    pub(crate) fn encode_value<T: serde::Serialize>(
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
    pub(crate) fn capture_failure_status(
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
    pub(crate) fn build_failure_status(
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

pub(crate) mod apiproxy {
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
    pub(crate) const REFUSAL_REASON_TRAILER: &str = "x-agentenv-resume-refusal";

    /// Trailer carrying the node a pinned refusal named, for the operator
    /// looking at the gateway's log rather than at the API half's.
    pub(crate) const REFUSAL_ORIGIN_TRAILER: &str = "x-agentenv-resume-origin-node";

    /// Metadata key carrying the port the data-plane request was addressed to.
    ///
    /// Spelled the same as the HTTP header the local reverse proxy reads, on
    /// purpose: it is the same fact travelling one hop further.
    pub(crate) const TARGET_PORT_METADATA: &str = "x-agentenv-target-port";

    /// Metadata key carrying the caller's envd access token.
    ///
    /// 🔴 Metadata and not a proto field, copying e2b
    /// (`paused_sandbox_resumer_grpc.go`). A credential in a message body ends
    /// up in every request log that prints the request.
    pub(crate) const ACCESS_TOKEN_METADATA: &str = "x-access-token";
}
