//! Sandbox egress credential brokering: the contract between the sandbox
//! runtime and the broker, and the broker's dispatch core.
//!
//! The runtime side opens a byte stream through a [`transport::BrokerTransport`]
//! and prefixes it with a [`header::IdentityHeader`]. The broker side picks a
//! [`handler::Handler`] by name and hands it the stream together with a
//! [`handler::ConnCtx`]. Handlers reach upstreams only through
//! [`policy::UpstreamGuard`] and read secrets only through
//! [`credential::CredentialSource`].
//!
//! The `core` feature is the whole of this contract and the embedded
//! transport; `local` adds the node-local Unix socket both halves speak over,
//! and `tls` adds the openssl-backed pieces the broker binary needs.

#[cfg(feature = "tls")]
pub mod audit;
pub mod credential;
pub mod dispatch;
pub mod framing;
pub mod handler;
pub mod handlers;
pub mod header;
pub mod marker;
pub mod policy;
#[cfg(feature = "resolver")]
pub mod resolver;
pub mod runtime;
pub mod sni;
#[cfg(feature = "tls")]
pub mod tls;
pub mod transport;

pub use credential::{CredentialError, CredentialFields, CredentialSource, Secret};
pub use dispatch::{Dispatcher, Reject};
pub use handler::{ConnCtx, Handler, HandlerError};
pub use header::{Ack, EgressPolicySummary, IdentityHeader};
pub use policy::{BrokerDenyList, DenyReason, UpstreamError, UpstreamGuard};
pub use transport::{AsyncStream, BrokerTransport, EmbeddedTransport, TransportError};
