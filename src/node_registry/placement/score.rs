//! Classification and post-placement pressure — pure, total, infallible.
//!
//! Nothing here reads a clock, takes a lock, or emits a metric: it is the
//! arithmetic half of the shadow scorer, so a fixture can pin an exact
//! `f64` without standing up a registry (see `super::tests`).
//!
//! # What "pressure" measures
//!
//! The *declared* resource pressure a node would be under if this request
//! landed on it — allocated CPU and allocated memory as reported by the
//! node's own last heartbeat, plus the request's own size, over the node's
//! capacity. It is deliberately not `cpu_percent` or RSS: those move on
//! their own between heartbeats, and a placement decision has to be
//! reproducible from the inputs the decision was made on.
//!
//! The two dimensions are combined with `max`, not a sum or an average:
//! the bottleneck dimension is what actually refuses the next sandbox, and
//! a node at 10% CPU / 99% memory is not "55% full".

use crate::proto::scheduler::NodeSnapshot;

/// Bytes per MiB. 🔴 The *only* place this codebase's shadow path converts
/// MiB to bytes — see [`RequestResources::memory_bytes`] for why it is
/// applied exactly once (K-7: the heartbeat side is bytes, the request side
/// is MiB, and adding them raw turns 512 MiB into 512 bytes).
pub const BYTES_PER_MIB: u64 = 1_048_576;

/// How the API half judges the snapshot it holds for a node, against that
/// node's *own* report TTL.
///
/// 🔴 Judged from the API-receive-side `last_seen`, never from the
/// node-self-reported `NodeSnapshot::reported_at_unix_ms`: the latter is a
/// clock the API half does not own, so a node with a fast clock would look
/// permanently fresh and one with a slow clock permanently stale.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotFreshness {
    /// Received within this record's own report TTL.
    Fresh,
    /// Older than this record's own report TTL — the registry would derive
    /// `UNHEALTHY` for it.
    Stale,
    /// `last_seen` is in the future. Reachable across API replicas (the
    /// record travels through Redis and carries the *writing* replica's
    /// clock), so it is a third state rather than an `unwrap` — K-5.
    ClockSkew,
}

/// Why a candidate could not be scored. A closed set: it is the `class`
/// label's value space on
/// `agentenv_api_placement_shadow_classification_total`, so a new variant
/// is a cardinality change and has to be a deliberate one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnknownReason {
    /// Discovered, but has never sent a heartbeat.
    NoSnapshot,
    /// [`SnapshotFreshness::Stale`].
    Stale,
    /// [`SnapshotFreshness::ClockSkew`].
    ClockSkew,
    /// `cpu_count` or `memory_total_bytes` is zero — nothing to divide by.
    ZeroDenominator,
    /// A `checked_*` step overflowed.
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

/// One candidate's shadow verdict.
///
/// 🔴 `pressure > 1.0` is legal and still [`Self::Scored`]: over-allocation
/// is a real, orderable state (it is *worse* than 0.9, which is exactly
/// what best-of-K needs to know), not an error. This phase does no hard fit
/// check, so there is no branch that turns an over-allocated node into an
/// `Unknown`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Classification {
    Scored { pressure: f64 },
    Unknown { reason: UnknownReason },
}

impl Classification {
    /// The `class` label value — `"scored"` plus [`UnknownReason::as_str`].
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Scored { .. } => "scored",
            Self::Unknown { reason } => reason.as_str(),
        }
    }
}

/// The request side of the pressure sum, in the units it arrives in.
///
/// 🔴 `memory_mib`, not bytes, and the conversion lives in exactly one
/// method below. Carrying bytes here would put a second `* 1_048_576` one
/// refactor away from the first.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RequestResources {
    pub cpu: u32,
    pub memory_mib: u64,
}

impl RequestResources {
    /// The one and only MiB → bytes conversion on this path. `None` on
    /// overflow, which classifies as [`UnknownReason::Overflow`].
    pub fn memory_bytes(self) -> Option<u64> {
        self.memory_mib.checked_mul(BYTES_PER_MIB)
    }
}

/// Classifies one candidate.
///
/// Total and infallible by construction: every arm returns a
/// [`Classification`], no `?` escapes to a caller, and nothing here can
/// panic — the only division is guarded by the zero-denominator arm above
/// it.
///
/// 🔴 The invariant `freshness.is_none()` iff `snapshot.is_none()` is the
/// caller's (`super::ShadowCandidate`); this function is written so that
/// *either* being `None` still lands on [`UnknownReason::NoSnapshot`]
/// rather than on an unreachable branch.
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
        // 🔴 `max`, the bottleneck dimension. `min` would rank a node by
        // whichever resource it happens to have most of.
        pressure: after_cpu.max(after_mem),
    }
}
