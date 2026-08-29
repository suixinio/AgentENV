//! Tests for `aenv-core` code whose other half is here.
//!
//! # 🔴 Why these are not in `aenv-core`
//!
//! A crate's `#[cfg(test)]` code cannot reach a crate that depends on it: a
//! dev-dependency back onto `aenv-node` would link the *library* build of
//! `aenv-core` next to the `--test` build of the same source, and the two
//! builds' types are not the same types. So a test that drives an `aenv-core`
//! surface against a real image resolver, a real POSIX repository or a real
//! node service belongs on this side of the split, where all of that exists.
//!
//! What is under test is still `aenv-core`'s; this crate only supplies the
//! half that makes the test possible.

mod api_cold_start;
pub mod snapshot_manager;
