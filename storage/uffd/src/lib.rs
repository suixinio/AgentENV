//! userfaultfd page-fault serving for Firecracker memory snapshot restore:
//! Firecracker loads a snapshot with a `Uffd` memory backend, connects to a
//! socket, hands over the descriptor and the region mappings, and this crate
//! fills each faulting page from a `PageSource` (the stacked overlaybd memory
//! layers in production).

pub mod handler;
pub mod handshake;
pub mod impls;
pub mod pagemap;
pub mod prefetch;
pub mod proto;
pub mod source;
pub mod testing;

pub use handler::{HandlerOptions, HandlerState, StatsSnapshot, UffdHandler};
pub use handshake::{recv_handshake, send_handshake, GuestRegionUffdMapping, Handshake};
pub use impls::{FileSource, MemSource, OverlaybdSource};
pub use pagemap::{dirty_ranges, DirtyRange};
pub use prefetch::PrefetchList;
pub use proto::{Event, Uffd};
pub use source::{LocalBoxFuture, PageSource};
