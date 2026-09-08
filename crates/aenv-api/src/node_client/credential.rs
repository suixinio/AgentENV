//! The credential this half presents at a node's gRPC gate.
//!
//! The node reads its accepted credentials from the same configuration keys,
//! so one mounted Secret opens both ends. It is read when a channel is opened,
//! not per request; a rotation reaches an established channel when it
//! reconnects, which the gate's union of accepted credentials covers.

use std::path::Path;

use tonic::service::interceptor::InterceptedService;
use tonic::service::Interceptor;
use tonic::transport::Channel;
use tonic::{metadata::MetadataValue, Request, Status};
use tracing::warn;

use crate::api::{ControlPlaneGate, CONTROL_PLANE_HEADER};
use crate::cfg::ConfigManager;
use crate::proto::node::node_sandbox_service_client::NodeSandboxServiceClient;

/// Stamps every node RPC with the control-plane credential, if there is one.
#[derive(Clone, Default)]
pub struct NodeGateCredential {
    credential: Option<MetadataValue<tonic::metadata::Ascii>>,
}

impl NodeGateCredential {
    pub fn from_global_config() -> Self {
        let config = &ConfigManager::global_config().api;
        let Some(credential) = outbound_credential(&config.node_client_token_file, || {
            ControlPlaneGate::from_global_config().presented()
        }) else {
            return Self::default();
        };
        match MetadataValue::try_from(credential.as_str()) {
            Ok(credential) => Self {
                credential: Some(credential),
            },
            Err(_) => {
                warn!(
                    "the control-plane credential is not a valid header value; node calls will \
                     present none"
                );
                Self::default()
            }
        }
    }
}

/// The credential to stamp: `api.node_client_token_file`'s first non-empty
/// line when that key names a readable file, otherwise whatever the gate
/// presents. An unreadable or empty file falls back rather than sending
/// nothing, and says so: the alternative is a node client that silently stops
/// authenticating.
fn outbound_credential(
    token_file: &str,
    fallback: impl FnOnce() -> Option<String>,
) -> Option<String> {
    let path = token_file.trim();
    if !path.is_empty() {
        match std::fs::read_to_string(Path::new(path)) {
            Ok(contents) => {
                if let Some(token) = contents.lines().map(str::trim).find(|l| !l.is_empty()) {
                    return Some(token.to_string());
                }
                warn!(
                    path,
                    "api.node_client_token_file holds no credential; falling back to the \
                     control-plane one"
                );
            }
            Err(err) => warn!(
                path,
                error = %err,
                "cannot read api.node_client_token_file; falling back to the control-plane one"
            ),
        }
    }
    fallback()
}

impl Interceptor for NodeGateCredential {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        if let Some(credential) = self.credential.clone() {
            request
                .metadata_mut()
                .insert(CONTROL_PLANE_HEADER, credential);
        }
        Ok(request)
    }
}

/// A node channel that carries the control-plane credential.
pub type GatedChannel = InterceptedService<Channel, NodeGateCredential>;

/// The node client every caller on this half uses.
pub type NodeClient = NodeSandboxServiceClient<GatedChannel>;

/// Wraps a connected channel in the credential the node's gate expects.
pub fn client(channel: Channel) -> NodeClient {
    NodeSandboxServiceClient::with_interceptor(channel, NodeGateCredential::from_global_config())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_configured_credential_reaches_the_request_metadata() {
        let mut credential = NodeGateCredential {
            credential: Some(MetadataValue::try_from("node-token").unwrap()),
        };

        let request = credential.call(Request::new(())).unwrap();

        assert_eq!(
            request.metadata().get(CONTROL_PLANE_HEADER).unwrap(),
            "node-token"
        );
    }

    #[test]
    fn the_dedicated_token_file_wins_over_the_gates_own_credential() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let path = dir.path().join("node-client-token");
        std::fs::write(&path, "\n  outbound-token  \nsecond\n").expect("write the token file");

        assert_eq!(
            outbound_credential(path.to_str().unwrap(), || Some("gate-token".to_string())),
            Some("outbound-token".to_string())
        );
    }

    #[test]
    fn an_unreadable_or_empty_token_file_falls_back_rather_than_stamping_nothing() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let empty = dir.path().join("empty");
        std::fs::write(&empty, "\n \n").expect("write an empty token file");

        for path in [
            empty.to_str().unwrap(),
            "/nonexistent/node-client-token",
            "",
        ] {
            assert_eq!(
                outbound_credential(path, || Some("gate-token".to_string())),
                Some("gate-token".to_string()),
                "{path}"
            );
        }
    }

    #[test]
    fn no_credential_leaves_the_request_unstamped() {
        let request = NodeGateCredential::default()
            .call(Request::new(()))
            .unwrap();

        assert!(request.metadata().get(CONTROL_PLANE_HEADER).is_none());
    }
}
