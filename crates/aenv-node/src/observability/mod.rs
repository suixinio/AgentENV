//! `aenv-core`'s node snapshot projection, plus the heartbeat only a machine
//! that runs sandboxes sends.

pub use aenv_core::observability::*;

mod reporter;

pub use reporter::ObservabilityReporter;
