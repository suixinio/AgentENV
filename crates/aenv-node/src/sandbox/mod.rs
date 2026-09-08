//! `aenv-core`'s sandbox contract, plus the Firecracker implementation of it.

pub use aenv_core::sandbox::*;

pub mod egress;
pub mod envd;
pub mod firecracker;
pub mod network;
pub mod process;
pub mod ublk;

mod extra_drive;

pub use ::envd::process::Signal;
pub use extra_drive::ExtraDrive;
pub use firecracker::{
    FirecrackerCapturedSnapshot, FirecrackerCommonConfig, FirecrackerPool,
    FirecrackerRuntimePolicy, FirecrackerSandbox, FirecrackerSandboxConfig,
    FirecrackerSandboxFactory, FirecrackerSnapshotConfig, FirecrackerSnapshotManifest,
};
pub use network::{prepare_runtime as prepare_network_runtime, NetworkManager};
pub use process::{Executor, ProcessHandle, ProcessOpts, ProcessOutput, SandboxExecutor};
pub use ublk::{OverlaybdConfig, UblkBackend, UblkConfig, UblkDaemonConfig, UblkDeviceManager};
