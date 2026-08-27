//! `aenv-core`'s template builder contract, plus the runner that executes a
//! build on this machine.

pub use aenv_core::template::*;

mod builder;
mod runner;
mod runtime_versions;
mod step_executor;

pub use builder::TemplateBuilder;
pub use runner::{TemplateBuildExecution, TemplateBuildRunner};
