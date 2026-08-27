//! Custom extension integration: an optional external HTTP service
//! (`[custom_extension].url`) extending sandbox behavior. Its only current
//! capability is sandbox lifecycle hooks; see [`client`].

pub mod client;

pub use crate::types::CustomExtensionParams;
pub use client::{
    custom_extension_params_is_empty, CustomExtensionClient, CustomExtensionHookGuard,
};
