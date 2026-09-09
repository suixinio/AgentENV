//! What a sandbox's network is allowed to reach.

pub mod policy;

pub use policy::{BaseSandboxNetworkPolicy, SandboxNetworkEgressPolicy, SandboxNetworkPolicy};
