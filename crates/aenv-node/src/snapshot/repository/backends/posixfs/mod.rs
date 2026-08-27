//! `aenv-core`'s durable POSIX halves, plus the importing half and the resolver.

pub use aenv_core::snapshot::repository::backends::posixfs::*;

mod backend;
mod import;
mod runtime;

pub use backend::{PosixFsBackend, PosixFsBackendConfig};
