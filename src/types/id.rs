use std::fmt;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SandboxId(Uuid);

impl SandboxId {
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    pub fn parse_str(id: &str) -> Result<Self, uuid::Error> {
        Ok(Self(Uuid::parse_str(id)?))
    }

    pub fn into_inner(self) -> Uuid {
        self.0
    }

    pub fn max() -> Self {
        Self(Uuid::max())
    }
}

impl Default for SandboxId {
    fn default() -> Self {
        Self::new()
    }
}

impl TryFrom<String> for SandboxId {
    type Error = uuid::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse_str(&value)
    }
}

impl TryFrom<&str> for SandboxId {
    type Error = uuid::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::parse_str(value)
    }
}

impl From<SandboxId> for String {
    fn from(value: SandboxId) -> Self {
        value.0.to_string()
    }
}

impl fmt::Display for SandboxId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl PartialOrd for SandboxId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SandboxId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

/// One run of one sandbox.
///
/// The sandbox id names the machine a user owns; this names a single boot of
/// that machine. Two things need to tell those apart. Fencing does, because a
/// write from a VM that has already been replaced has to be refused rather
/// than applied; and routing does, because a request addressed to the previous
/// boot must not be served by the current one.
///
/// Minted in exactly three places — the two `LaunchPlan::for_create_*`
/// constructors, and the resume claim, whose value reaches `for_resume` inside
/// a [`ClaimedExecution`][crate::orchestrator::ClaimedExecution]. Anything else
/// that produces one is a bug by construction.
///
/// UUIDv7, so the lexicographic order of the canonical lowercase string is the
/// order the incarnations happened in. The whole comparison in `proxy.rs` and
/// in the controller's arbitration rests on that, which is why the string form
/// is always the lowercase canonical one `Uuid`'s `Display` produces.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ExecutionId(Uuid);

impl ExecutionId {
    /// Mints a new incarnation.
    // 🔴 No `Default`, and the lint that asks for one is silenced rather than
    // satisfied. See the note below the impl: a `Default` incarnation is an
    // incarnation nobody authorised, minted wherever `..Default::default()`
    // appears.
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    pub fn parse_str(id: &str) -> Result<Self, uuid::Error> {
        Ok(Self(Uuid::parse_str(id)?))
    }

    pub fn into_inner(self) -> Uuid {
        self.0
    }
}

// 🔴 No `Default`, on purpose, and unlike `SandboxId` right above.
//
// `SandboxId: Default` exists so `SandboxMetadata::default()` can build a test
// object. An `ExecutionId: Default` would do the same thing with a very
// different meaning: it would mint an incarnation nobody authorised, in any
// context that ever calls `..Default::default()`, silently. Requiring every
// producer to say `ExecutionId::new()` out loud is the whole mechanism — a
// missing incarnation has to be a compile error, never a fresh one.

impl TryFrom<String> for ExecutionId {
    type Error = uuid::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse_str(&value)
    }
}

impl TryFrom<&str> for ExecutionId {
    type Error = uuid::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::parse_str(value)
    }
}

impl From<ExecutionId> for String {
    fn from(value: ExecutionId) -> Self {
        value.0.to_string()
    }
}

impl fmt::Display for ExecutionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[cfg(test)]
mod execution_id_tests {
    use super::ExecutionId;

    /// A5's refusal rule is an ordered comparison over incarnations, and the
    /// order it uses is the one the canonical lowercase string sorts in. This
    /// pins the two to each other: if `Ord` ever stops agreeing with the string
    /// form, the node and the gateway start disagreeing about which of two
    /// incarnations is newer, and the disagreement is silent.
    #[test]
    fn ordering_matches_the_canonical_string_order() {
        let mut minted: Vec<ExecutionId> = (0..16).map(|_| ExecutionId::new()).collect();
        minted.sort();

        let rendered: Vec<String> = minted.iter().map(ToString::to_string).collect();
        let mut sorted_strings = rendered.clone();
        sorted_strings.sort();

        assert_eq!(rendered, sorted_strings);
        for id in &rendered {
            assert_eq!(*id, id.to_lowercase(), "execution ids render lowercase");
        }
    }

    #[test]
    fn minting_never_repeats_and_round_trips_through_its_string_form() {
        let first = ExecutionId::new();
        let second = ExecutionId::new();
        assert_ne!(first, second);
        assert_eq!(ExecutionId::parse_str(&first.to_string()).unwrap(), first);
    }
}
