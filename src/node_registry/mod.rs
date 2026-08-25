//! The node clean-up half of Stage A (`docs/proposals/_sd-phase4-stageA-node-inventory.md`):
//! an api-side, growing-in-place port of `services/scheduler/internal`'s node
//! inventory (`node_registry.go`, `kubernetes_discovery.go`, `filter.go`,
//! `strategy.go`, `warmup.go`, `cpu_template.go`).
//!
//! Nothing in this module is wired into any runtime path yet — see each
//! submodule's own doc comment for what Go file it ports and how far the port
//! goes. `[cluster].node_placement_source` (once it exists) is what flips a
//! consumer onto this module's answers instead of the scheduler's; until
//! then this tree is inert, load-bearing only for its own tests.

pub mod cpu_template;
pub mod filter;
pub mod strategy;
pub mod types;
