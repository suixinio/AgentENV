//! `aenv-core`'s registry-facing helpers, plus the ones that read overlaybd.

// 🔴 No glob of `aenv-core`'s `common`: everything it holds is its `acr`
// module, which this one shadows with a wider version of the same name.
pub mod acr;
pub mod dense;

pub use dense::{
    write_dense_overlaybd_layer_to_file, write_dense_overlaybd_layer_to_file_blocking,
};
