//! Registry-facing helpers shared by both backends.
//!
//! 🔴 The dense overlaybd export lives in `aenv-node`'s `common::dense`: it
//! reads overlaybd layer files, which is exactly what the deciding half does
//! not do.

pub mod acr;
