//! How the types `aenv-core` owns render on the wire.
//!
//! The conversions live here because neither `aenv-core`'s types nor the
//! generated models belong to the half that serves them, so no other crate may
//! write them.

mod error;
mod sandbox;
mod snapshot;

use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub use sandbox::{
    allow_internet_access_from_base_policy, base_policy_from_allow_internet_access,
    endpoints_from_model, network_config_model, params_map_to_model, params_model_to_map,
    rules_from_model,
};
pub use snapshot::{
    build_record_envd_version, build_record_names, datetime_from_unix_ms, template_build_status,
};

/// Reads a millisecond stamp back, before the epoch as readily as after it.
pub fn system_time_from_unix_ms(unix_ms: i64) -> SystemTime {
    if unix_ms >= 0 {
        UNIX_EPOCH + Duration::from_millis(unix_ms as u64)
    } else {
        UNIX_EPOCH - Duration::from_millis(unix_ms.unsigned_abs())
    }
}

/// The message an error's whole cause chain reads as, at 500.
pub fn internal_error(err: &dyn std::error::Error) -> agentenv_http_server::models::Error {
    let mut message = err.to_string();
    let mut current = err.source();
    while let Some(source) = current {
        let cause = source.to_string();
        if cause != message {
            message.push_str(": ");
            message.push_str(&cause);
        }
        current = source.source();
    }
    agentenv_http_server::models::Error::new(500, message)
}
