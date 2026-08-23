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
