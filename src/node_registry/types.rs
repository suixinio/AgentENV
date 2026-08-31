//! Shared node discovery, observation, and roster shapes.

use std::time::{Duration, SystemTime};

use crate::proto::scheduler::NodeSnapshot;

/// Discovered node identity and address.
///
/// `pod_name` aliases a previous identity during rolling replacement.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct Node {
    pub id: String,
    pub endpoint: String,
    pub pod_name: String,
}

/// Discovery identity plus the latest heartbeat snapshot, absent before first report.
#[derive(Debug, Clone, Default)]
pub struct RichNode {
    pub node: Node,
    pub snapshot: Option<NodeSnapshot>,
}

impl RichNode {
    pub fn new(node: Node) -> Self {
        Self {
            node,
            snapshot: None,
        }
    }

    pub fn with_snapshot(node: Node, snapshot: NodeSnapshot) -> Self {
        Self {
            node,
            snapshot: Some(snapshot),
        }
    }
}

/// Sandbox entry from a node's latest heartbeat roster.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RosterEntry {
    pub sandbox_id: String,
    /// Canonical execution UUID, or empty when absent or invalid.
    pub execution_id: String,
    /// Projection TTL; zero delegates to the binding store's default.
    pub projection_ttl: Duration,
    /// Whether the sandbox is parked without a VM.
    ///
    /// Paused entries still renew paused-registry leases but binding reconciliation must
    /// exclude them because a routing projection promises a running VM.
    pub paused: bool,
}

/// Node heartbeat roster; absent `last_seen` means discovery knows it but no heartbeat arrived.
#[derive(Debug, Clone, Default)]
pub struct Roster {
    pub node_id: String,
    pub entries: Vec<RosterEntry>,
    pub last_seen: Option<SystemTime>,
}

impl Roster {
    pub fn sandbox_ids(&self) -> Vec<String> {
        self.entries.iter().map(|e| e.sandbox_id.clone()).collect()
    }
}
