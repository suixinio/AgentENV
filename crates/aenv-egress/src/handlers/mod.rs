//! Handlers shipped with the crate. `echo` and `tcp` are part of `core`; the
//! `http` and `postgres` handlers arrive with the `tls` feature, which is what
//! gives them an upstream TLS client.

pub mod echo;
#[cfg(feature = "tls")]
pub mod http;
#[cfg(feature = "tls")]
pub mod postgres;
#[cfg(feature = "tls")]
pub mod scram;
pub mod tcp;
