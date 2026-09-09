//! The sandbox lifecycle state machine, and the pieces only a process that
//! runs sandboxes has: the handle table, the proxy route table and the
//! launches in flight inside it.
//!
//! The model both halves share -- states, the metadata record, the store
//! contract, the error -- stays in `aenv-core` and is re-exported here, so a
//! path into this module reaches whichever half owns the name.

mod facade;
mod launch_claim;
mod launch_plan;
mod proxy;
mod service;
pub mod store;

pub use aenv_core::orchestrator::*;

pub use facade::NodeOrchestration;
pub use launch_claim::{LaunchFailure, LaunchSettlement};
pub use proxy::{ProxyLookupResult, ProxyTarget};
pub use service::Orchestrator;
pub use store::InMemoryMetadataStore;
