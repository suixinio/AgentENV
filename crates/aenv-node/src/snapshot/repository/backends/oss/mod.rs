//! `aenv-core`'s durable OSS halves, plus the importing half and the resolver.

pub use aenv_core::snapshot::repository::backends::oss::*;

mod backend;
mod import;
mod resolver;

pub use backend::OssBackend;
