//! Redis key layout.
//! All multi-key structures share one `{global}` hash tag for Redis Cluster scripts.

use uuid::Uuid;

use crate::types::{ExecutionId, SandboxId};

// Shared Redis Cluster hash tag.
const GLOBAL_TAG: &str = "{global}";

/// Builds store keys and notification channels.
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

    /// Sandbox record JSON.
    pub fn record(&self, sandbox_id: &SandboxId) -> String {
        self.scoped(&format!("sbx:{sandbox_id}"))
    }

    /// Authoritative sandbox membership set.
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

    /// Transition deadline index used by crash recovery.
    pub fn transition_index(&self) -> String {
        self.scoped("txn:index")
    }

    pub fn lock(&self, sandbox_id: &SandboxId) -> String {
        self.scoped(&format!("lock:sbx:{sandbox_id}"))
    }

    /// Shared wake-up channel carrying routing keys as payloads.
    pub fn notify_channel(&self) -> String {
        format!("{}:notify", self.prefix)
    }
}

/// Routing keys carried in notification payloads.
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
}

/// Incarnation-scoped expiry-index member.
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

    /// Parses both UUID segments, rejecting malformed members.
    pub fn decode(raw: &str) -> Option<Self> {
        let (sandbox, execution) = raw.split_once(':')?;
        Some(Self {
            sandbox_id: SandboxId::from_uuid(Uuid::parse_str(sandbox).ok()?),
            execution_id: ExecutionId::from_uuid(Uuid::parse_str(execution).ok()?),
        })
    }
}

/// Sandbox-, execution-, and transition-scoped reaper member.
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
        for key in [
            k.record(&sandbox_id),
            k.index(),
            k.expiry(),
            k.transition(&sandbox_id),
            k.transition_index(),
            k.transition_result(&sandbox_id, &Uuid::now_v7()),
            k.lock(&sandbox_id),
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

    #[test]
    fn ids_never_contain_the_separator() {
        assert!(!SandboxId::new().to_string().contains(':'));
        assert!(!ExecutionId::new().to_string().contains(':'));
        assert!(!Uuid::now_v7().to_string().contains(':'));
    }
}
