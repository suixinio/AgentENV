//! Reading what a node said, and refusing what it did not.

use std::net::Ipv4Addr;

use anyhow::{anyhow, Context, Result};
use prost::Message;
use tonic::Status;

use crate::proto::node as pb;
use crate::proto::node::SERIALIZED_VALUE_VERSION;
use crate::sandbox::SandboxCaptureError;
use crate::types::SandboxId;

/// Turns a gRPC failure into an ordinary error.
///
/// 🔴 Every code, including the ones that sound like answers. `NotFound` from a
/// node means "not on that node", which is not the same as "not anywhere", and
/// the only caller allowed to read it as an answer is the one that knows what
/// it asked — see `RemoteSandboxStub::stop`.
pub(super) fn into_error(status: Status) -> anyhow::Error {
    anyhow!("{}: {}", status.code(), status.message())
}

/// Turns a gRPC failure on a capture into the classification the caller acts
/// on.
///
/// # 🔴 A missing classification is terminal
///
/// The two outcomes are not symmetric. "Recoverable" tells the caller the
/// runtime was put back and may go on serving; "terminal" tells it the runtime
/// was mutated past safe resume and must be torn down. Reading an unclassified
/// failure as recoverable would keep serving a VM that may have been snapshotted
/// out from under itself; reading it as terminal costs one sandbox. So an error
/// that arrived without a classification, or with one this build cannot decode,
/// is terminal.
///
/// The classification travels in the status details and never in the code:
/// `Internal` is produced by both kinds and by the transport itself, so a
/// caller reading the code alone would be guessing.
pub(super) fn into_capture_error(status: Status) -> SandboxCaptureError {
    let message = format!("{}: {}", status.code(), status.message());
    let unclassified = || {
        SandboxCaptureError::terminal(anyhow!(
            "{message} (no capture classification, so the sandbox is assumed unsafe to resume)"
        ))
    };

    // 🔴 Empty details are checked *before* the decode, and this is not
    // defensive tidiness. prost decodes an empty buffer into a message with
    // every field at its default, so `SandboxCaptureFailure::decode(b"")`
    // succeeds and yields `terminal: false` — which would silently turn every
    // ordinary transport failure into "recoverable, the sandbox is fine".
    if status.details().is_empty() {
        return unclassified();
    }
    match pb::SandboxCaptureFailure::decode(status.details()) {
        Ok(failure) if !failure.terminal => {
            SandboxCaptureError::recoverable(anyhow!("{message} ({})", failure.reason))
        }
        Ok(failure) => SandboxCaptureError::terminal(anyhow!("{message} ({})", failure.reason)),
        Err(_) => unclassified(),
    }
}

/// Why a paused sandbox was not reopened on the node that holds its capture.
///
/// # 🔴 Three answers, because two of them end the same way and mean opposite
/// things
///
/// A resume that did not happen leaves the caller with a decision to take, and
/// the decision differs by *why*:
///
/// - [`CaptureAbsent`][Self::CaptureAbsent] — the node looked and is not
///   holding this sandbox's capture. The only copy is not there, so the caller
///   must rebuild the sandbox from a published snapshot or give it up; asking
///   again will not change the answer.
/// - [`NodeUnreachable`][Self::NodeUnreachable] — nobody answered, or the node
///   answered that it is not taking work. The capture is presumed intact and
///   the right move is to ask again later.
/// - [`Refused`][Self::Refused] — the node answered and said no for a reason of
///   its own: the fence named a run it is not holding, the request was
///   malformed, something failed inside.
///
/// 🔴 The whole point of the type is that these cannot be collapsed. Reading an
/// unreachable node as an absent capture discards a sandbox whose bytes are
/// sitting intact on a disk that is merely offline — which is the same mistake,
/// one layer up, as reading a failed store read as "the record is not there".
/// Nothing above this layer acts on the difference *yet*, because the wake-up
/// surface that will is still being assembled; what it must never do is arrive
/// to find the difference already thrown away in a string.
#[derive(Debug, thiserror::Error)]
pub(super) enum RemoteResumeFailure {
    #[error("node {node_id} is not holding a paused capture for sandbox {sandbox_id}: {detail}")]
    CaptureAbsent {
        node_id: String,
        sandbox_id: SandboxId,
        detail: String,
    },
    #[error("node {node_id} could not be reached about sandbox {sandbox_id}: {detail}")]
    NodeUnreachable {
        node_id: String,
        sandbox_id: SandboxId,
        detail: String,
    },
    #[error("node {node_id} refused to reopen sandbox {sandbox_id}'s capture: {detail}")]
    Refused {
        node_id: String,
        sandbox_id: SandboxId,
        detail: String,
    },
}

impl RemoteResumeFailure {
    /// Classifies what a node said about a resume.
    ///
    /// 🔴 `Unavailable` covers both a transport that never delivered the call
    /// and a node that delivered it and said it is not taking work, and they
    /// are one class on purpose: both mean *the capture is still there, ask
    /// again*, which is the only thing the caller does differently.
    pub(super) fn from_status(node_id: &str, sandbox_id: SandboxId, status: Status) -> Self {
        let node_id = node_id.to_string();
        let detail = format!("{}: {}", status.code(), status.message());
        match status.code() {
            tonic::Code::NotFound => Self::CaptureAbsent {
                node_id,
                sandbox_id,
                detail,
            },
            tonic::Code::Unavailable | tonic::Code::DeadlineExceeded => Self::NodeUnreachable {
                node_id,
                sandbox_id,
                detail,
            },
            _ => Self::Refused {
                node_id,
                sandbox_id,
                detail,
            },
        }
    }

    /// A node this process could not open a connection to.
    ///
    /// 🔴 Never [`CaptureAbsent`][Self::CaptureAbsent]. A refused connection is
    /// the one failure with no answer in it at all.
    pub(super) fn unreachable(node_id: &str, sandbox_id: SandboxId, detail: String) -> Self {
        Self::NodeUnreachable {
            node_id: node_id.to_string(),
            sandbox_id,
            detail,
        }
    }
}

pub(super) fn serialize<T: serde::Serialize>(value: &T, what: &str) -> Result<pb::SerializedValue> {
    crate::proto::node::encode_value(value).with_context(|| format!("encode {what}"))
}

pub(super) fn serialized<T: serde::de::DeserializeOwned>(
    value: Option<&pb::SerializedValue>,
    what: &str,
) -> Result<Option<T>> {
    let Some(raw) = serialized_value(value, what)? else {
        return Ok(None);
    };
    serde_json::from_value(raw)
        .map(Some)
        .with_context(|| format!("decode {what}"))
}

pub(super) fn serialized_value(
    value: Option<&pb::SerializedValue>,
    what: &str,
) -> Result<Option<serde_json::Value>> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.schema_version != SERIALIZED_VALUE_VERSION {
        return Err(anyhow!(
            "{what} was encoded with schema version {}, and this build only reads version \
             {SERIALIZED_VALUE_VERSION}",
            value.schema_version
        ));
    }
    if value.json.is_empty() {
        return Ok(None);
    }
    serde_json::from_slice(&value.json)
        .map(Some)
        .with_context(|| format!("decode {what}"))
}

/// An address a node reported, or `None` when it reported none.
///
/// 🔴 An unparseable address is `None` rather than an error: the field is
/// informational — it is what the proxy would dial — and failing a whole create
/// over it would trade a degraded sandbox for no sandbox.
pub(super) fn host_ip(raw: &str) -> Option<Ipv4Addr> {
    (!raw.is_empty()).then(|| raw.parse().ok()).flatten()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 🔴 The three answers a resume can come back with stay three.
    ///
    /// One call shape, one value different — the status code — and the
    /// classification has to move with it. The pair that matters is the first
    /// two: `NotFound` says the only copy of a sandbox is gone and licenses
    /// throwing the sandbox away, and `Unavailable` says a machine is down and
    /// licenses nothing at all.
    #[test]
    fn an_unreachable_node_is_not_a_capture_that_is_gone() {
        let sandbox_id = SandboxId::new();
        let classify = |status| RemoteResumeFailure::from_status("node-a", sandbox_id, status);

        assert!(matches!(
            classify(Status::not_found("no paused capture here")),
            RemoteResumeFailure::CaptureAbsent { .. }
        ));
        assert!(matches!(
            classify(Status::unavailable("the node is restarting")),
            RemoteResumeFailure::NodeUnreachable { .. }
        ));
        assert!(matches!(
            classify(Status::deadline_exceeded("the node did not answer")),
            RemoteResumeFailure::NodeUnreachable { .. }
        ));
        assert!(matches!(
            classify(Status::failed_precondition("that run is not the one here")),
            RemoteResumeFailure::Refused { .. }
        ));
        // 🔴 And the default arm is a refusal rather than an absence: a node
        // that failed inside has not told anyone the capture is gone.
        assert!(matches!(
            classify(Status::internal("something went wrong")),
            RemoteResumeFailure::Refused { .. }
        ));

        // A connection that was never opened carries no answer at all.
        assert!(matches!(
            RemoteResumeFailure::unreachable("node-a", sandbox_id, "connection refused".into()),
            RemoteResumeFailure::NodeUnreachable { .. }
        ));
    }

    /// The classification survives being carried as an ordinary error, which is
    /// how it reaches the layer that acts on it.
    #[test]
    fn the_classification_survives_the_error_it_travels_in() {
        let sandbox_id = SandboxId::new();
        let err = anyhow::Error::new(RemoteResumeFailure::from_status(
            "node-a",
            sandbox_id,
            Status::unavailable("the node is restarting"),
        ))
        .context("resume sandbox on node node-a");

        assert!(matches!(
            err.downcast_ref::<RemoteResumeFailure>(),
            Some(RemoteResumeFailure::NodeUnreachable { .. })
        ));
        // 🔴 The control face: the same wrapping around the other answer comes
        // back as the other answer, so this is not a downcast that matches
        // whatever it is handed.
        let absent = anyhow::Error::new(RemoteResumeFailure::from_status(
            "node-a",
            sandbox_id,
            Status::not_found("no paused capture here"),
        ))
        .context("resume sandbox on node node-a");
        assert!(matches!(
            absent.downcast_ref::<RemoteResumeFailure>(),
            Some(RemoteResumeFailure::CaptureAbsent { .. })
        ));
    }

    #[test]
    fn a_classified_failure_comes_back_with_its_classification() {
        for terminal in [true, false] {
            let status = crate::proto::node::capture_failure_status(
                tonic::Code::Internal,
                "capture failed",
                terminal,
                "the memory snapshot did not land",
            );
            let err = into_capture_error(status);
            assert_eq!(err.is_terminal(), terminal, "{err}");
            assert!(err.to_string().contains("did not land"), "{err}");
        }
    }

    /// 🔴 The direction that matters. An error with no classification — a
    /// transport failure, a node from an older build, a status somebody wrote
    /// by hand — must be read as terminal.
    #[test]
    fn an_unclassified_failure_is_terminal() {
        let err = into_capture_error(Status::internal("something went wrong"));
        assert!(err.is_terminal(), "{err}");

        let err = into_capture_error(Status::deadline_exceeded("the node did not answer"));
        assert!(
            err.is_terminal(),
            "a node that did not answer left a sandbox in an unknown state: {err}"
        );
    }

    #[test]
    fn a_blob_from_another_schema_version_is_refused() {
        let value = serialize(&serde_json::json!({"a": 1}), "test").expect("encode");
        assert!(serialized_value(Some(&value), "test")
            .expect("decode")
            .is_some());

        let stale = pb::SerializedValue {
            schema_version: SERIALIZED_VALUE_VERSION + 1,
            ..value
        };
        let err = serialized_value(Some(&stale), "test").expect_err("a newer schema");
        assert!(err.to_string().contains("schema version"), "{err}");
    }

    #[test]
    fn an_address_that_is_not_one_is_absent_rather_than_fatal() {
        assert_eq!(host_ip(""), None);
        assert_eq!(host_ip("not-an-address"), None);
        assert_eq!(host_ip("10.0.0.7"), Some(Ipv4Addr::new(10, 0, 0, 7)));
    }
}
