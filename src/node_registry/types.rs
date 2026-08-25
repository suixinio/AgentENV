//! Shared shapes for the node-registry port. Mirrors two small Go types:
//!
//! - `Node` ports `services/shared/routing.Node` — the record
//!   `services/scheduler/internal/types.go` aliases in as `scheduler.Node`
//!   ("an alias, not a copy," per that file's own comment, because the
//!   gateway reads the same records out of the same Redis and cannot import
//!   the scheduler package). Rust has no equivalent cross-process sharing
//!   constraint yet — this struct is the local, Stage A-only stand-in, not
//!   wired to any storage.
//! - `RichNode` ports `services/scheduler/internal/types.go`'s `RichNode`:
//!   discovery identity plus the most recent heartbeat-reported snapshot.
//!   Go embeds `Node` so `n.ID` reads directly off a `RichNode`; Rust has no
//!   embedding, so callers spell it `n.node.id`.

use crate::proto::scheduler::NodeSnapshot;

/// A discovered node's identity and address.
///
/// 🔴 `pod_name` carries the *previous* identity this node reported itself
/// under (its pod name), kept so a heartbeat sent under the old name during a
/// fleet upgrade is still recognised rather than rejected as unknown — see
/// `node_registry.go`'s `canonicalIDLocked` doc comment, ported in
/// `super::registry`. Empty when the node's id already *is* its pod name
/// (nothing to alias).
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct Node {
    pub id: String,
    pub endpoint: String,
    pub pod_name: String,
}

/// Discovery identity combined with the most recent heartbeat-reported
/// runtime state. `snapshot` is `None` until the node's first heartbeat.
///
/// `Clone` mirrors Go's by-value slice element: [`super::strategy::Strategy`]
/// hands back an owned `RichNode` picked out of a borrowed candidate slice.
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
