//! `aenv-core`'s registry-facing helpers, plus the ones that read overlaybd.

pub mod acr;
pub mod dense;

pub use dense::{
    write_dense_overlaybd_layer_to_file, write_dense_overlaybd_layer_to_file_blocking,
};
