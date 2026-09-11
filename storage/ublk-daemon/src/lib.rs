pub mod client;
pub(crate) mod memory_uffd;
pub mod protocol;
pub(crate) mod runtime;
pub mod server;
pub mod transport;

pub use client::{
    CreateOverlaybdRuntimeDeviceRequest, InvalidRequestError, MemoryUffdServeOptions,
    MemoryUffdStatus, OverlaybdRuntimeDevice, RestackSnapshotTerminalFailure, UblkDaemonClient,
    UblkDaemonSpawnConfig,
};
pub use protocol::{
    AccessMode, DaemonRequest, DaemonResponse, MemoryUffdRegion, MemoryUffdState, MemoryUffdStats,
    ResizeToolSpec, RestackSnapshotStats,
};
pub use server::UblkDaemonServer;
pub use transport::{nbd_transport_usable, Transport, TransportHandle};
pub use warm_pool::PoolConfig;
