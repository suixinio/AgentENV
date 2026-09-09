//! Creation outcomes this process accumulated.
//!
//! These cannot be derived from the metadata store: they count attempts,
//! including failures whose record was removed.

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct ControlCounters {
    create_successes: AtomicU64,
    create_fails: AtomicU64,
}

impl ControlCounters {
    pub fn record_create_success(&self, count: u64) {
        self.create_successes.fetch_add(count, Ordering::Relaxed);
    }

    pub fn record_create_fail(&self, count: u64) {
        self.create_fails.fetch_add(count, Ordering::Relaxed);
    }

    pub fn create_successes(&self) -> u64 {
        self.create_successes.load(Ordering::Relaxed)
    }

    pub fn create_fails(&self) -> u64 {
        self.create_fails.load(Ordering::Relaxed)
    }
}
