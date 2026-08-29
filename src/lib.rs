pub mod api;
pub mod binding_store;
pub mod cfg;
pub mod digest;
pub mod identity;
pub mod image;
pub mod leader_task;
pub mod local_store;
pub mod logging;
pub mod node_client;
pub mod node_registry;
pub mod observability;
pub mod orchestrator;
pub mod p2p;
pub mod privileges;
pub mod proto;
/// 🔴 Test-only, and `cfg(test)` rather than `cfg(any(test, feature =
/// "test-support"))`.
///
/// The three consumers — `orchestrator::store::redis::harness`,
/// `binding_store::redis::harness`, `node_registry::redis::harness` — are all
/// `cfg(test)` modules of *this* crate, so `cfg(test)` reaches every one of
/// them. `test-support` exists for the other case: scaffolding this crate's
/// *siblings* need (`aenv-node`, `aenv-api`), compiled without this crate's
/// `cfg(test)`. No sibling spawns a `redis-server`, and putting one behind
/// that feature would compile a process spawner into their dev builds for no
/// consumer.
#[cfg(test)]
mod redis_test_server;
pub mod runtime_snapshot;
pub mod sandbox;
pub mod scheduler_endpoint;
pub mod server_main;
pub mod snapshot;
pub mod template;
pub mod types;
pub mod virtualization;
