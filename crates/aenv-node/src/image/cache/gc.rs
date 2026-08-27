use std::collections::BTreeMap;

use super::graph::{HardCommitId, ImageCacheHoldOwner};

pub type ImageCacheLiveRuntimeRefs = BTreeMap<HardCommitId, Vec<ImageCacheHoldOwner>>;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ImageCacheGcReport {
    pub collected: usize,
    pub freed_bytes: u64,
    pub blocked: Vec<ImageCacheGcBlocked>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ImageCacheGcBlocked {
    pub digest: HardCommitId,
    pub reason: ImageCacheGcBlockedReason,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ImageCacheGcBlockedReason {
    RootedByConfig {
        configs: Vec<String>,
    },
    Held {
        owners: Vec<ImageCacheHoldOwner>,
    },
    LiveRuntime {
        owners: Vec<ImageCacheHoldOwner>,
    },
    /// Fail-closed: the candidate could not be verified safe to delete (missing
    /// or changed file, path outside the commit store, missing object record,
    /// failed stat, ...), so it is kept. The string carries the specific cause
    /// for logs; these causes are only ever surfaced as a blocked count.
    Unverifiable(String),
    /// Deletion was attempted but the file removal failed.
    DeleteFailed(String),
}

/// Flat, dependency-free summary of a GC pass for callers outside the cache
/// subsystem. Replaces external dependence on `ImageCacheGcBlocked`/`Reason`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ImageCacheGcSummary {
    pub collected: usize,
    pub freed_bytes: u64,
    pub retained: usize,
}

impl ImageCacheGcSummary {
    pub fn from_report(report: &ImageCacheGcReport) -> Self {
        Self {
            collected: report.collected,
            freed_bytes: report.freed_bytes,
            retained: report.blocked.len(),
        }
    }
}
