//! What both halves need of the HTTP surface: the internal credential gate.

mod control_plane_gate;
pub use control_plane_gate::{
    constant_time_eq, require_control_plane, ControlPlaneGate, GateDecision, CONTROL_PLANE_HEADER,
};

pub mod wire;
