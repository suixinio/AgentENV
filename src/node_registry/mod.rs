//! The node clean-up half of Stage A (`docs/proposals/_sd-phase4-stageA-node-inventory.md`):
//! an api-side, growing-in-place port of `services/scheduler/internal`'s node
//! inventory (`node_registry.go`, `kubernetes_discovery.go`, `filter.go`,
//! `strategy.go`, `warmup.go`, `cpu_template.go`).
//!
//! `[cluster].node_placement_source = "native"` is what flips `--role api`
//! onto this module's answers instead of the scheduler's — see
//! [`crate::node_client::NativeNodePlacement`] (the placement-side consumer)
//! and [`grpc_service`] (the heartbeat-receiving plane that feeds it). Under
//! the default `"scheduler"`, `assemble_api` builds none of this — no
//! registry, no kube client, no gRPC service — so this tree stays exactly as
//! inert as it was before the switch existed.

pub mod cpu_template;
pub mod dump;
pub mod filter;
pub mod grpc_service;
pub mod kubernetes_discovery;
pub mod registry;
pub mod strategy;
pub mod types;
pub mod warmup;
