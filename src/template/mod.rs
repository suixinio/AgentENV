mod build_spec;
mod builder;
mod errors;
mod runner;
mod runtime_versions;
mod step_executor;

pub use build_spec::TemplateBuildSpec;
pub use builder::TemplateBuilder;
pub(crate) use errors::TemplateBuildFailure;
pub use errors::{
    TemplateBuildError, TemplateBuildResult, TemplatePipelineError, TemplatePipelineResult,
};

// 🔴 For `NodeSandboxService::build_template` (`src/node_server/service.rs`),
// which drives `TemplateBuildRunner` itself rather than going through
// `TemplateBuilder::execute_and_publish` — see the note on
// `TemplateBuilder::prepare_remote_context`. `build_spec`'s and `runner`'s
// modules stay private; only the names that service actually spells cross
// out. `TemplateBuildContext` does not: it flows through that service
// entirely by inference (`prepare_remote_context`'s return value, borrowed
// straight into `execute`), so re-exporting it too would just be one more
// unused name to keep in step.
pub(crate) use build_spec::TemplateBuildStep;
pub(crate) use runner::{TemplateBuildExecution, TemplateBuildRunner};
