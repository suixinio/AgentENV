//! API-side node inventory: discovery, heartbeats, placement, warm-up, and CPU templates.
//!
//! [`grpc_service`] receives observations and [`crate::node_client::NativeNodePlacement`]
//! consumes the same registry and scheduler surface in process.

pub mod cpu_template;
pub mod filter;
pub mod fleet;
pub mod grpc_service;
pub mod kubernetes_discovery;
pub mod placement;
pub mod redis;
pub mod registry;
pub mod static_discovery;
pub mod strategy;
pub mod types;
pub mod warmup;
