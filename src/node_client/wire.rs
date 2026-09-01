//! Reading what a node said, and refusing what it did not.

use std::error::Error as _;
use std::net::Ipv4Addr;

use anyhow::{anyhow, Context, Result};
use prost::Message;
use tonic::Status;

use crate::proto::node as pb;
use crate::proto::node::SERIALIZED_VALUE_VERSION;
use crate::sandbox::SandboxCaptureError;
use crate::types::SandboxId;

/// Converts any node gRPC status into an ordinary error without interpreting it as absence.
pub fn into_error(status: Status) -> anyhow::Error {
    anyhow!("{}: {}", status.code(), status.message())
}

/// Returns true only for client-generated `Unavailable` statuses with a transport source.
///
/// A source-less status came from the node and must not be replayed.
pub fn is_unreachable(status: &Status) -> bool {
    status.code() == tonic::Code::Unavailable && status.source().is_some()
}

/// Converts a capture failure, treating missing or undecodable classification as terminal.
pub fn into_capture_error(status: Status) -> SandboxCaptureError {
    let message = format!("{}: {}", status.code(), status.message());
    let unclassified = || {
        SandboxCaptureError::terminal(anyhow!(
            "{message} (no capture classification, so the sandbox is assumed unsafe to resume)"
        ))
    };

    // Prost decodes empty bytes as recoverable defaults, so reject them before decoding.
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

/// Classifies why a node did not reopen its local capture.
///
/// Absence, temporary unreachability, and node refusal must remain distinct.
#[derive(Debug, thiserror::Error)]
pub enum RemoteResumeFailure {
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
    /// Classifies a node-returned resume status.
    pub fn from_status(node_id: &str, sandbox_id: SandboxId, status: Status) -> Self {
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

    /// Constructs a failure for a connection that never reached the node.
    pub fn unreachable(node_id: &str, sandbox_id: SandboxId, detail: String) -> Self {
        Self::NodeUnreachable {
            node_id: node_id.to_string(),
            sandbox_id,
            detail,
        }
    }

    /// Whether the node answered that the capture this reopen needed is not on it.
    ///
    /// A row whose snapshot is published can still be rebuilt from the
    /// repository, so this classification is a fallback signal rather than a
    /// verdict about the sandbox.
    pub fn is_capture_absent(&self) -> bool {
        matches!(self, Self::CaptureAbsent { .. })
    }
}

/// Whether anything in this chain says the capture a reopen needed is not on
/// the node that was supposed to hold it.
///
/// Walks the chain because callers add context around the reopen failure.
pub fn capture_absent(error: &anyhow::Error) -> bool {
    error
        .chain()
        .filter_map(|cause| cause.downcast_ref::<RemoteResumeFailure>())
        .any(RemoteResumeFailure::is_capture_absent)
}

pub fn serialize<T: serde::Serialize>(value: &T, what: &str) -> Result<pb::SerializedValue> {
    crate::proto::node::encode_value(value).with_context(|| format!("encode {what}"))
}

pub fn serialized<T: serde::de::DeserializeOwned>(
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

pub fn serialized_value(
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

/// Parses an informational IPv4 address, returning `None` for absent or invalid input.
pub fn host_ip(raw: &str) -> Option<Ipv4Addr> {
    (!raw.is_empty()).then(|| raw.parse().ok()).flatten()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_status_with_no_answer_behind_it_counts_as_unreachable() {
        let transport_failure = Status::from_error(Box::new(tonic::ConnectError(Box::new(
            std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "tcp connect error"),
        ))));
        assert_eq!(transport_failure.code(), tonic::Code::Unavailable);
        assert!(
            is_unreachable(&transport_failure),
            "a connect failure must be retried"
        );

        let sent_by_the_node = Status::unavailable("draining, try another node");
        assert_eq!(sent_by_the_node.code(), tonic::Code::Unavailable);
        assert!(
            !is_unreachable(&sent_by_the_node),
            "a status the node actually sent must not be retried, even at the same code"
        );

        let refused = Status::not_found("no such sandbox here");
        assert!(!is_unreachable(&refused));
    }

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
        assert!(matches!(
            classify(Status::internal("something went wrong")),
            RemoteResumeFailure::Refused { .. }
        ));

        assert!(matches!(
            RemoteResumeFailure::unreachable("node-a", sandbox_id, "connection refused".into()),
            RemoteResumeFailure::NodeUnreachable { .. }
        ));
    }

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
