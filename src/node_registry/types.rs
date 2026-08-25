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

use std::time::{Duration, SystemTime};

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

/// One sandbox on a node, as that node reported it in its last heartbeat.
/// Ports `services/scheduler/internal/store.go`'s `RosterEntry` — defined
/// there (Stage D's file) rather than in `node_registry.go` because Go's
/// `BindingStore` also consumes it, but `node_registry.go` depends on the
/// type directly (`observedNodeRecord.entries`). Stage D's future binding
/// store port should reuse this type rather than define a second one.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RosterEntry {
    pub sandbox_id: String,
    /// Lowercase canonical UUIDv7, normalized on the way in. Empty where the
    /// node did not report one (or reported something that did not look like
    /// one — see `super::registry`'s `normalize_execution_id`).
    pub execution_id: String,
    /// The same budget `RecordAssignmentRequest.projection_ttl_secs` carries,
    /// on the repair path rather than the create path. `Duration::ZERO`
    /// means "use the store's binding_ttl", never "never expires".
    pub projection_ttl: Duration,
}

/// One node's heartbeat-reported sandbox list, ported from
/// `services/scheduler/internal/node_registry.go`'s `Roster`.
///
/// `last_seen: None` is Go's zero `time.Time{}` sentinel, spelled as an
/// `Option` instead: the node is known to discovery but has never sent a
/// heartbeat. That is a real, reportable state — a machine that came up and
/// never checked in is exactly the one an operator needs to see — not a
/// placeholder to special-case away.
#[derive(Debug, Clone, Default)]
pub struct Roster {
    pub node_id: String,
    pub entries: Vec<RosterEntry>,
    pub last_seen: Option<SystemTime>,
}

impl Roster {
    /// The ids alone, for the consumers that only need the set.
    pub fn sandbox_ids(&self) -> Vec<String> {
        self.entries.iter().map(|e| e.sandbox_id.clone()).collect()
    }
}
