//! Credential gate for the node's gRPC surface.
//!
//! The surface carries `update_network`, `delete` and `pause`, and a guest
//! reaches the node's address from inside its namespace. The gate reads the
//! same credential file as the node's REST gate, so one Secret opens or closes
//! both.

use std::sync::Arc;

use crate::api::{ControlPlaneGate, GateDecision, CONTROL_PLANE_HEADER};
use tonic::{Request, Status};
use tracing::debug;

/// Refuses gRPC calls that present no accepted credential. With no credential
/// configured every caller is admitted, the way the REST gate behaves.
#[derive(Clone)]
pub struct NodeGrpcGate {
    gate: Arc<ControlPlaneGate>,
}

impl NodeGrpcGate {
    pub fn from_global_config() -> Self {
        Self {
            gate: Arc::new(ControlPlaneGate::from_global_config()),
        }
    }

    /// Uses credentials the caller already resolved instead of the process's.
    pub fn new(gate: ControlPlaneGate) -> Self {
        Self {
            gate: Arc::new(gate),
        }
    }

    fn check<T>(&self, request: Request<T>) -> Result<Request<T>, Status> {
        let presented = request
            .metadata()
            .get(CONTROL_PLANE_HEADER)
            .and_then(|value| value.to_str().ok());
        let decision = self.gate.admits(presented);

        metrics::counter!(
            "agentenv_node_grpc_gate_total",
            "decision" => decision.label(),
        )
        .increment(1);

        if decision == GateDecision::Refused {
            debug!("refusing a node gRPC call that presented no accepted credential");
            return Err(Status::unauthenticated("control plane credential required"));
        }

        Ok(request)
    }
}

impl tonic::service::Interceptor for NodeGrpcGate {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        self.check(request)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::metadata::MetadataValue;

    fn request(credential: Option<&str>) -> Request<()> {
        let mut request = Request::new(());
        if let Some(credential) = credential {
            request.metadata_mut().insert(
                CONTROL_PLANE_HEADER,
                MetadataValue::try_from(credential).unwrap(),
            );
        }
        request
    }

    #[test]
    fn the_configured_credential_is_admitted() {
        let gate = NodeGrpcGate::new(ControlPlaneGate::new(vec!["node-token".into()], ""));

        assert!(gate.check(request(Some("node-token"))).is_ok());
    }

    #[test]
    fn another_credential_is_unauthenticated() {
        let gate = NodeGrpcGate::new(ControlPlaneGate::new(vec!["node-token".into()], ""));

        for presented in [None, Some(""), Some("gateway-token"), Some("node-token ")] {
            let status = gate
                .check(request(presented))
                .err()
                .unwrap_or_else(|| panic!("{presented:?} must not be admitted"));
            assert_eq!(status.code(), tonic::Code::Unauthenticated);
        }
    }

    #[test]
    fn no_configured_credential_leaves_the_surface_open() {
        let gate = NodeGrpcGate::new(ControlPlaneGate::new(Vec::new(), ""));

        assert!(gate.check(request(None)).is_ok());
        assert!(gate.check(request(Some("anything"))).is_ok());
    }

    #[test]
    fn a_credential_file_that_is_absent_leaves_the_surface_open() {
        let missing = std::env::temp_dir().join("aenv-node-gate-absent-token");
        let _ = std::fs::remove_file(&missing);
        let gate = NodeGrpcGate::new(ControlPlaneGate::new(Vec::new(), missing.to_str().unwrap()));

        assert!(gate.check(request(None)).is_ok());
    }
}
