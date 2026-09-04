//! Handlers shipped with the crate. `echo` and `tcp` are part of `core`; the
//! `http` handler arrives with the `tls` feature.

pub mod echo;
#[cfg(feature = "tls")]
pub mod http;
pub mod tcp;
