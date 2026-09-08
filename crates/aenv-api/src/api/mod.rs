//! The api half's HTTP surface: the generated routes' implementations, its
//! composition, and the gRPC services beside it.

pub use aenv_core::api::{
    constant_time_eq, require_control_plane, ControlPlaneGate, GateDecision, CONTROL_PLANE_HEADER,
};

pub mod grpc;
pub mod impls;
pub mod server;

/// The public pagination token's codec.
pub use impls::{snapshot_cursor_from_token, snapshot_next_token, PaginationError};
pub use impls::{ApiImpl, ResumeWiring};
