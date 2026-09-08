//! Pure classification of post-placement declared-resource pressure.
//!
//! Pressure is the larger of post-request CPU and memory allocation ratios, using
//! heartbeat capacities and request declarations rather than moving utilization signals.

use crate::proto::scheduler::NodeSnapshot;

/// Bytes per MiB; request memory converts exactly once through [`RequestResources`].
pub const BYTES_PER_MIB: u64 = 1_048_576;

/// Freshness derived from API receive time and each record's report TTL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotFreshness {
    Fresh,
    Stale,
    /// Receive time is in the future, possible across replicas with clock skew.
    ClockSkew,
}

/// Closed metric-label reasons a candidate cannot be scored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnknownReason {
    NoSnapshot,
    Stale,
    ClockSkew,
    ZeroDenominator,
    Overflow,
}

impl UnknownReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NoSnapshot => "no_snapshot",
            Self::Stale => "stale",
            Self::ClockSkew => "clock_skew",
            Self::ZeroDenominator => "zero_denominator",
            Self::Overflow => "overflow",
        }
    }
}

/// Candidate pressure or a reason it is unknown.
///
/// Pressure above one remains scoreable and orders as over-allocation.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Classification {
    Scored { pressure: f64 },
    Unknown { reason: UnknownReason },
}

impl Classification {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Scored { .. } => "scored",
            Self::Unknown { reason } => reason.as_str(),
        }
    }
}

/// Request CPU and MiB memory declarations.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RequestResources {
    pub cpu: u32,
    pub memory_mib: u64,
}

impl RequestResources {
    /// Converts MiB to bytes, returning `None` on overflow.
    pub fn memory_bytes(self) -> Option<u64> {
        self.memory_mib.checked_mul(BYTES_PER_MIB)
    }
}

/// Totally and infallibly classifies one candidate.
pub fn classify(
    snapshot: Option<&NodeSnapshot>,
    freshness: Option<SnapshotFreshness>,
    request: RequestResources,
) -> Classification {
    let (Some(snapshot), Some(freshness)) = (snapshot, freshness) else {
        return Classification::Unknown {
            reason: UnknownReason::NoSnapshot,
        };
    };
    match freshness {
        SnapshotFreshness::Stale => {
            return Classification::Unknown {
                reason: UnknownReason::Stale,
            }
        }
        SnapshotFreshness::ClockSkew => {
            return Classification::Unknown {
                reason: UnknownReason::ClockSkew,
            }
        }
        SnapshotFreshness::Fresh => {}
    }

    if snapshot.cpu_count == 0 || snapshot.memory_total_bytes == 0 {
        return Classification::Unknown {
            reason: UnknownReason::ZeroDenominator,
        };
    }

    let overflow = Classification::Unknown {
        reason: UnknownReason::Overflow,
    };
    let Some(req_mem_bytes) = request.memory_bytes() else {
        return overflow;
    };
    let Some(after_cpu_units) = snapshot.allocated_cpu.checked_add(request.cpu) else {
        return overflow;
    };
    let Some(after_mem_bytes) = snapshot.allocated_memory_bytes.checked_add(req_mem_bytes) else {
        return overflow;
    };

    let after_cpu = f64::from(after_cpu_units) / f64::from(snapshot.cpu_count);
    let after_mem = after_mem_bytes as f64 / snapshot.memory_total_bytes as f64;
    Classification::Scored {
        // The bottleneck resource determines pressure.
        pressure: after_cpu.max(after_mem),
    }
}
