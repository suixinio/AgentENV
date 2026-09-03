use super::DiskMetric;
use crate::orchestrator::SandboxRosterEntry;

/// How this node reaches the egress broker. The api half places a sandbox
/// that declares network rules only on a `RemoteOk` node.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum EgressBrokerState {
    #[default]
    Disabled,
    Embedded,
    RemoteOk,
    RemoteUnreachable,
}

impl EgressBrokerState {
    /// Whether a sandbox whose rules name the public `http` handler can run
    /// here. The embedded transport dispatches the identity-echo handler the
    /// node's own integration tests use and nothing else, so it is not a
    /// placement target for public rules.
    pub fn can_broker(&self) -> bool {
        matches!(self, Self::RemoteOk)
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Embedded => "embedded",
            Self::RemoteOk => "remote_ok",
            Self::RemoteUnreachable => "remote_unreachable",
        }
    }
}

/// Answers the current broker state at heartbeat time.
pub trait EgressBrokerProbe: Send + Sync {
    fn egress_broker_state(&self) -> EgressBrokerState;
}

impl<F: Fn() -> EgressBrokerState + Send + Sync> EgressBrokerProbe for F {
    fn egress_broker_state(&self) -> EgressBrokerState {
        self()
    }
}

/// Static machine descriptors reported as part of node observability.
#[derive(Clone, Debug)]
pub struct MachineInfo {
    pub cpu_family: String,
    pub cpu_model: String,
    pub cpu_model_name: String,
    pub cpu_architecture: String,
    pub cpu_config_json: Option<String>,
}

/// Node-level metrics projected from orchestrator runtime counters and the
/// latest sampled host snapshot.
#[derive(Clone, Debug)]
pub struct NodeMetricsSnapshot {
    pub allocated_cpu: u32,
    pub allocated_memory_bytes: u64,
    pub cpu_percent: u32,
    pub cpu_count: u32,
    pub memory_used_bytes: u64,
    pub memory_total_bytes: u64,
    pub disks: Vec<DiskMetric>,
}

/// Request-time node snapshot returned by the admin/node APIs.
///
/// This joins together identity metadata, static machine information,
/// orchestrator runtime accounting, and host resource snapshots.
#[derive(Clone, Debug)]
pub struct NodeSnapshot {
    pub version: String,
    pub commit: String,
    pub node_id: String,
    pub service_instance_id: String,
    pub cluster_id: uuid::Uuid,
    pub machine_info: MachineInfo,
    pub sandbox_count: u32,
    /// The sandboxes alive on this node, each with the incarnation it is
    /// running under and the TTL its routing projection should carry.
    pub sandbox_roster: Vec<SandboxRosterEntry>,
    pub metrics: NodeMetricsSnapshot,
    /// Whether this node is isolated: still serving what it holds, refusing new
    /// sandboxes. Reported so the scheduler stops picking it without having to
    /// wait for the node to disappear.
    pub draining: bool,
    pub create_successes: u64,
    pub create_fails: u64,
    pub sandbox_starting_count: u32,
    pub egress_broker: EgressBrokerState,
}

#[cfg(test)]
mod tests {
    use super::EgressBrokerState;

    #[test]
    fn only_a_reachable_remote_broker_can_serve_public_rules() {
        assert!(EgressBrokerState::RemoteOk.can_broker());
        assert!(!EgressBrokerState::Embedded.can_broker());
        assert!(!EgressBrokerState::RemoteUnreachable.can_broker());
        assert!(!EgressBrokerState::Disabled.can_broker());
    }
}
