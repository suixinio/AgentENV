//! `aenv-core`'s repository backends, plus the byte half.
//!
//! The shared assembly and both backends' durable halves are `aenv-core`'s;
//! what this crate adds is [`storage`] — the importing halves, the runtime
//! resolvers, and the overlaybd layer store they all reach.

pub use aenv_core::snapshot::repository::backends::*;

pub mod common;
pub mod oss;
pub mod posixfs;
pub mod storage;
