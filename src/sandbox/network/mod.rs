//! What a sandbox's network is allowed to reach.
//!
//! 🔴 The half that creates namespaces, veth pairs and slots is `aenv-node`'s
//! `sandbox::network`. What is here is the policy a request carries and the
//! iptables rendering it turns into — both of which the deciding half
//! validates and stores without ever applying.

pub mod iptables_util;
pub mod policy;

pub use policy::{BaseSandboxNetworkPolicy, SandboxNetworkEgressPolicy, SandboxNetworkPolicy};
