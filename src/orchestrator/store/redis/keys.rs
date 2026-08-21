//! The key layout, in one place.
//!
//! **Flat.** e2b shards its keys by team so that one Lua script can touch a
//! sandbox key and that team's index key in the same Redis Cluster hash slot.
//! That is a Cluster constraint dressed as a data model, and we have no tenant
//! to shard by: `team_id` appears in this repository only inside the generated
//! E2B-compatible schema, and the auth layer describes itself as checking
//! "presence, not validity". There is no subject, so there is no sharding.
//!
//! 🔴 **But the multi-key constraint does not go away with the tenant.** The
//! deployment is a single Redis instance today, so every key is trivially in
//! one slot and the four-step `add` script works. On a Cluster it would not:
//! the record key and the shared index/expiry keys would hash to different
//! slots and `EVAL` would refuse with `CROSSSLOT`. So the shared structures
//! carry a `{global}` hash tag now, while the key names are still free to
//! change. They will not be free later — a live deployment's keys are as
//! immovable as its record format.

use uuid::Uuid;

use crate::types::{ExecutionId, SandboxId};

/// The hash tag every key in this module shares.
///
/// Costs nothing on a single instance and buys a Cluster migration that does
/// not have to rename keys.
const GLOBAL_TAG: &str = "{global}";

/// Builds every key and channel name this store uses.
#[derive(Clone, Debug)]
pub struct KeySpace {
    prefix: String,
}

impl KeySpace {
    pub fn new(prefix: impl Into<String>) -> Self {
        Self {
            prefix: prefix.into(),
        }
    }

    fn scoped(&self, rest: &str) -> String {
        format!("{}:{GLOBAL_TAG}:{rest}", self.prefix)
    }

    /// The record itself: a JSON blob.
    pub fn record(&self, sandbox_id: &SandboxId) -> String {
        self.scoped(&format!("sbx:{sandbox_id}"))
    }

    /// 🔴 The authoritative membership set, and the reason `list_ids` is not a
    /// `SCAN`. `SCAN` gives a best-effort view of a keyspace that may be
    /// changing; membership is a fact the writes maintain.
    pub fn index(&self) -> String {
        self.scoped("index")
    }

    /// ZSET of `<sandbox_id>:<execution_id>` scored by expiry in unix millis.
    pub fn expiry(&self) -> String {
        self.scoped("expiry")
    }

    /// The in-flight transition for a sandbox, holding its transition id.
    pub fn transition(&self, sandbox_id: &SandboxId) -> String {
        self.scoped(&format!("txn:{sandbox_id}"))
    }

    /// Where a finished transition leaves its outcome for waiters.
    pub fn transition_result(&self, sandbox_id: &SandboxId, transition_id: &Uuid) -> String {
        self.scoped(&format!("txn:{sandbox_id}:{transition_id}"))
    }

    /// ZSET of transitions scored by their deadline.
    ///
    /// 🔴 e2b has no equivalent, and we need one. Its crash recovery ends at
    /// the expiry sweep's stale-cutoff branch, which only ever sees sandboxes
    /// that have an expiry. Ours may have `timeout = None`, and such a sandbox
    /// is never indexed by expiry at all — so a replica that dies mid-`Pausing`
    /// leaves it stuck in `Pausing` with nothing anywhere that will ever look
    /// at it again. To the user that is a sandbox which cannot be deleted.
    ///
    /// The name cannot collide with [`KeySpace::transition`]: that one is
    /// always followed by a UUID, and `index` is not one.
    pub fn transition_index(&self) -> String {
        self.scoped("txn:index")
    }

    pub fn lock(&self, sandbox_id: &SandboxId) -> String {
        self.scoped(&format!("lock:sbx:{sandbox_id}"))
    }

    /// ZSET of sandbox ids being created, scored by when the window opened.
    ///
    /// 🔴 This is the only cluster-visible evidence that a sandbox is being
    /// built. `store.add` happens *after* the VM exists, so between minting an
    /// id and inserting the record there is a window of seconds to tens of
    /// seconds in which reconciliation would see a VM with no record — and the
    /// treatment for that is to kill it.
    pub fn pending(&self) -> String {
        self.scoped("pending")
    }

    /// Where a finished creation leaves its outcome for waiters.
    pub fn reserve_result(&self, sandbox_id: &SandboxId) -> String {
        self.scoped(&format!("reserve:{sandbox_id}"))
    }

    /// The single wake-up channel.
    ///
    /// One connection per replica is enough because the routing key travels in
    /// the payload rather than in the channel name.
    ///
    /// 🔴 Not the lifecycle-event channel. This carries "something you were
    /// waiting on moved", whose payload is a routing key and never changes;
    /// lifecycle events carry `SandboxLifecycleEvent`, whose shape follows the
    /// orchestration logic. Merging them would tie the heartbeat's reporting
    /// format to the state machine.
    pub fn notify_channel(&self) -> String {
        format!("{}:notify", self.prefix)
    }
}

/// Routing keys carried inside a notification payload.
pub mod routing {
    use crate::types::SandboxId;

    pub fn record(sandbox_id: &SandboxId) -> String {
        format!("sbx:{sandbox_id}")
    }

    pub fn lock(sandbox_id: &SandboxId) -> String {
        format!("lock:{sandbox_id}")
    }

    pub fn transition(sandbox_id: &SandboxId) -> String {
        format!("txn:{sandbox_id}")
    }

    pub fn reservation(sandbox_id: &SandboxId) -> String {
        format!("reserve:{sandbox_id}")
    }
}

/// A member of the expiry ZSET.
///
/// 🔴 Scoped to the incarnation, which is what makes every `ZREM` structurally
/// safe: removing a dead incarnation's member can never unindex a live one,
/// even when a lockless `add` for the same sandbox id races a removal or the
/// evictor's stale sweep. Drop the second segment and one `ZREM` for a dead
/// incarnation silently un-expires the sandbox that replaced it — which then
/// never expires again, because nothing else ever puts it back.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExpiryMember {
    pub sandbox_id: SandboxId,
    pub execution_id: ExecutionId,
}

impl ExpiryMember {
    pub fn new(sandbox_id: SandboxId, execution_id: ExecutionId) -> Self {
        Self {
            sandbox_id,
            execution_id,
        }
    }

    pub fn encode(&self) -> String {
        format!("{}:{}", self.sandbox_id, self.execution_id)
    }

    /// 🔴 Both segments are parsed as UUIDs, not merely split on `:`. A member
    /// that does not parse is garbage to be swept, and must never be mistaken
    /// for a sandbox — including the "sandbox" whose id is the empty string.
    pub fn decode(raw: &str) -> Option<Self> {
        let (sandbox, execution) = raw.split_once(':')?;
        Some(Self {
            sandbox_id: SandboxId::from_uuid(Uuid::parse_str(sandbox).ok()?),
            execution_id: ExecutionId::from_uuid(Uuid::parse_str(execution).ok()?),
        })
    }
}

/// A member of the transition index.
///
/// 🔴 Three segments for the same reason the expiry member has two, plus one
/// more: the transition id. Without it, a reaper that removes a dead
/// transition's member would also unindex a live transition started on the
/// same incarnation a moment later.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TransitionMember {
    pub sandbox_id: SandboxId,
    pub execution_id: ExecutionId,
    pub transition_id: Uuid,
}

impl TransitionMember {
    pub fn new(sandbox_id: SandboxId, execution_id: ExecutionId, transition_id: Uuid) -> Self {
        Self {
            sandbox_id,
            execution_id,
            transition_id,
        }
    }

    pub fn encode(&self) -> String {
        format!(
            "{}:{}:{}",
            self.sandbox_id, self.execution_id, self.transition_id
        )
    }

    pub fn decode(raw: &str) -> Option<Self> {
        let mut parts = raw.split(':');
        let sandbox = parts.next()?;
        let execution = parts.next()?;
        let transition = parts.next()?;
        if parts.next().is_some() {
            return None;
        }
        Some(Self {
            sandbox_id: SandboxId::from_uuid(Uuid::parse_str(sandbox).ok()?),
            execution_id: ExecutionId::from_uuid(Uuid::parse_str(execution).ok()?),
            transition_id: Uuid::parse_str(transition).ok()?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys() -> KeySpace {
        KeySpace::new("agentenv:api")
    }

    #[test]
    fn every_shared_structure_shares_one_hash_tag() {
        let k = keys();
        let sandbox_id = SandboxId::new();
        // 🔴 If any of these loses the tag, a Redis Cluster deployment starts
        // refusing the multi-key scripts with CROSSSLOT, and the fix is a key
        // rename on a live deployment.
        for key in [
            k.record(&sandbox_id),
            k.index(),
            k.expiry(),
            k.pending(),
            k.transition(&sandbox_id),
            k.transition_index(),
            k.transition_result(&sandbox_id, &Uuid::now_v7()),
            k.lock(&sandbox_id),
            k.reserve_result(&sandbox_id),
        ] {
            assert!(key.contains(GLOBAL_TAG), "{key} has no hash tag");
            assert!(key.starts_with("agentenv:api:"), "{key}");
            assert!(
                !key.starts_with("agentenv:scheduler"),
                "{key} collides with the routing projection"
            );
        }
    }

    #[test]
    fn the_transition_index_cannot_collide_with_a_sandbox_transition_key() {
        let k = keys();
        // A sandbox id is a UUID, and `index` is not one, so these are
        // structurally distinct however the prefix is configured.
        assert_ne!(k.transition_index(), k.transition(&SandboxId::new()));
        assert!(Uuid::parse_str("index").is_err());
    }

    #[test]
    fn expiry_members_round_trip() {
        let member = ExpiryMember::new(SandboxId::new(), ExecutionId::new());
        assert_eq!(ExpiryMember::decode(&member.encode()), Some(member));
    }

    #[test]
    fn transition_members_round_trip() {
        let member = TransitionMember::new(SandboxId::new(), ExecutionId::new(), Uuid::now_v7());
        assert_eq!(TransitionMember::decode(&member.encode()), Some(member));
    }

    /// 🔴 Garbage must decode to nothing, so that the sweeper deletes it
    /// instead of treating it as a sandbox that no longer has a record — which
    /// is a very different conclusion with a very different consequence.
    #[test]
    fn unparseable_members_are_garbage_not_sandboxes() {
        for raw in [
            "",
            ":",
            "not-a-uuid:also-not",
            "0198f0a1-0000-7000-8000-0000000c0ffe",
            "0198f0a1-0000-7000-8000-0000000c0ffe:not-a-uuid",
        ] {
            assert_eq!(ExpiryMember::decode(raw), None, "{raw}");
        }
        let two_segments = ExpiryMember::new(SandboxId::new(), ExecutionId::new()).encode();
        assert_eq!(
            TransitionMember::decode(&two_segments),
            None,
            "a two-segment member is not a transition member"
        );
        let four_segments = format!("{two_segments}:{}:{}", Uuid::now_v7(), Uuid::now_v7());
        assert_eq!(TransitionMember::decode(&four_segments), None);
    }

    /// The separator is only safe because neither id can contain it.
    #[test]
    fn ids_never_contain_the_separator() {
        assert!(!SandboxId::new().to_string().contains(':'));
        assert!(!ExecutionId::new().to_string().contains(':'));
        assert!(!Uuid::now_v7().to_string().contains(':'));
    }
}
