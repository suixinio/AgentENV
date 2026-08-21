mod control_plane_gate;
mod impls;
/// The public pagination token's codec.
///
/// 🔴 Exported because the format is public API and the thing that proves it
/// still works is a walk of a *real* catalog's cursor through it — render the
/// token the server's cursor produced, parse it back, ask for the next page.
/// A test that could not reach this would have to spell the format out a second
/// time, and two spellings of one format agree with each other rather than with
/// the clients holding a token.
pub use impls::{snapshot_cursor_from_token, snapshot_next_token, PaginationError};
mod isolation;
mod proxy;
mod role_gate;
pub mod server;

pub use impls::{ApiImpl, PausedSandboxWiring, StaleReleaseOutcome};
