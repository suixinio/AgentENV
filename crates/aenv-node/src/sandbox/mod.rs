//! `aenv-core`'s sandbox contract, plus the Firecracker implementation of it.

pub use aenv_core::sandbox::*;

pub mod firecracker;
pub mod network;
pub mod ublk;

mod extra_drive;

pub use extra_drive::ExtraDrive;
pub use firecracker::{
    FirecrackerCapturedSnapshot, FirecrackerCommonConfig, FirecrackerPool,
    FirecrackerRuntimePolicy, FirecrackerSandbox, FirecrackerSandboxConfig,
    FirecrackerSandboxFactory, FirecrackerSnapshotConfig, FirecrackerSnapshotManifest,
};
pub use network::{prepare_runtime as prepare_network_runtime, NetworkManager};
pub use ublk::{OverlaybdConfig, UblkBackend, UblkConfig, UblkDaemonConfig, UblkDeviceManager};
