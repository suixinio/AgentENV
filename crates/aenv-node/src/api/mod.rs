//! What a node serves over HTTP: its own report, and the sandbox data plane.

pub use aenv_core::api::{
    constant_time_eq, require_control_plane, ControlPlaneGate, GateDecision, CONTROL_PLANE_HEADER,
};

pub mod node_api;
pub mod proxy;
pub mod server;

pub use node_api::NodeApi;
pub use server::DataPlane;
