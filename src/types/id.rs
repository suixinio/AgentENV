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
/// UUIDv7 ordering is the incarnation order used by fencing and routing.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ExecutionId(Uuid);

impl ExecutionId {
    /// Mints a new incarnation.
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

// Intentionally no `Default`: every incarnation must be explicitly minted.
// Missing incarnations must remain compile errors.

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
