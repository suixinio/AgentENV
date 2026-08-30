//! Shadow resource scoring for placement.
//!
//! # What this is, and what it deliberately is not
//!
//! Placement in this build is [`super::strategy::RoundRobinStrategy`], and
//! it stays that way: nothing here can change which node a `Schedule` or a
//! paused-sandbox restore lands on. What runs here runs *after* the real
//! strategy has already answered, reads the candidate list the real
//! strategy was handed, and produces four new metric series and nothing
//! else.
//!
//! That is not caution for its own sake. Best-of-K only beats round-robin
//! if the scorer can see the placements already in flight, and this control
//! plane cannot: `Schedule` picks a node, the create happens afterwards
//! (`crate::node_client::stub`), heartbeats land every 5s by default, and
//! `aenv-api` runs as several replicas, so an in-process counter of
//! "sandboxes I just placed" is incomplete by construction. E2B's
//! equivalent (`placement.go`'s `StartPlacing` plus an optimistic update on
//! success) has no counterpart here yet. Until it does, a scorer that
//! *decided* would herd every concurrent burst onto whichever node last
//! reported itself emptiest. So this one only watches, and the metrics it
//! produces are the evidence for whether flipping the default would be
//! worth building that accounting for.
//!
//! # Shape
//!
//! - [`score`] — classification and post-placement pressure. Pure, total.
//! - [`sample`] — without-replacement best-of-K sampling, RNG injectable.
//! - this module — the entry point [`ShadowPlacement::evaluate`], the
//!   closed [`ShadowSource`] label set, the request-side mapping, and the
//!   metric handles.
//!
//! # Two disciplines that are not stylistic
//!
//! **Handles, not macros.** Every counter and histogram below is resolved
//! once, when the service is constructed. `metrics::counter!` is not free:
//! the Prometheus recorder's `register_counter` goes through
//! `get_or_create_counter`, which takes an `RwLock`
//! (`metrics-exporter-prometheus`'s `recorder.rs`). Calling a macro per
//! candidate would put an O(N) lock sequence on every placement.
//!
//! **Aggregate on the stack.** Classification counts are tallied in a
//! fixed-size array over all candidates and flushed as at most one
//! `increment` per class per call.

pub mod sample;
pub mod score;

use std::sync::Mutex;

use metrics::{Counter, Histogram};

use crate::node_registry::types::RichNode;
use crate::proto::scheduler::{schedule_request_hint::Kind, ScheduleRequestHint};

use sample::{sample_without_replacement, ScriptedRng, ThreadShadowRng};
use score::{classify, Classification, RequestResources, SnapshotFreshness, UnknownReason};

/// `[cluster].placement_shadow_k`'s default, and the value every
/// non-production construction of [`ShadowPlacement`] gets.
pub const DEFAULT_PLACEMENT_SHADOW_K: u32 = 3;

const AGREEMENT_METRIC: &str = "agentenv_api_placement_shadow_agreement_total";
const CLASSIFICATION_METRIC: &str = "agentenv_api_placement_shadow_classification_total";
const PRESSURE_SPREAD_METRIC: &str = "agentenv_api_placement_shadow_pressure_spread";
const MISSING_REQUEST_RESOURCES_METRIC: &str =
    "agentenv_api_placement_missing_request_resources_total";

/// Which caller asked for a placement.
///
/// 🔴 A closed internal enum passed explicitly by each of the two real call
/// sites, *not* derived from `hint.is_some()`. The paused-restore path calls
/// [`crate::binding_store::lookup::select_node`] with `hint = None`, so
/// inferring the source from the hint would file every restore under
/// "`Schedule` with no hint" and make the missing-resources counter
/// unreadable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShadowSource {
    /// `NodeRegistryGrpcService::schedule`.
    Schedule,
    /// `lookup_node`'s `Paused` branch.
    PausedLookup,
}

impl ShadowSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Schedule => "schedule",
            Self::PausedLookup => "paused_lookup",
        }
    }

    const fn index(self) -> usize {
        match self {
            Self::Schedule => 0,
            Self::PausedLookup => 1,
        }
    }
}

const SOURCES: [ShadowSource; 2] = [ShadowSource::Schedule, ShadowSource::PausedLookup];

/// The `class` label's value space, in metric-handle index order.
const CLASSES: [&str; 6] = [
    "scored",
    "no_snapshot",
    "stale",
    "clock_skew",
    "zero_denominator",
    "overflow",
];

const fn class_index(classification: &Classification) -> usize {
    match classification {
        Classification::Scored { .. } => 0,
        Classification::Unknown { reason } => match reason {
            UnknownReason::NoSnapshot => 1,
            UnknownReason::Stale => 2,
            UnknownReason::ClockSkew => 3,
            UnknownReason::ZeroDenominator => 4,
            UnknownReason::Overflow => 5,
        },
    }
}

/// One eligible candidate, plus the freshness verdict for the very same
/// registry read its `snapshot` came from.
///
/// 🔴 Invariant: `freshness.is_none()` **iff** `rich.snapshot.is_none()`.
/// Both come out of a single `peek_observed_with_freshness` call, so a
/// candidate can never carry a snapshot whose freshness was judged against
/// a different read — or a second `RwLock` acquisition.
///
/// 🔴 A borrow of the real candidate list, never a copy of it. The real
/// list is what round-robin was handed and what the placement returns; a
/// scorer that owned its own copy could drift from it, and cloning one
/// `NodeSnapshot` per node per placement would be a real cost paid for
/// nothing.
#[derive(Debug, Clone, Copy)]
pub struct ShadowCandidate<'a> {
    pub rich: &'a RichNode,
    pub freshness: Option<SnapshotFreshness>,
}

/// The request side of the score, plus whether the caller told us anything.
///
/// `missing` is not "the values are zero": an explicit `Some(0)` is a real
/// answer and is counted as present. That distinction is the whole reason
/// `NewSandboxHint`'s two fields use proto3 explicit presence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShadowRequest {
    pub resources: RequestResources,
    pub missing: bool,
}

impl ShadowRequest {
    /// The paused-restore path's request: it has no hint to read, and that
    /// is not a caller omission, so it does not count as missing.
    pub const PAUSED_LOOKUP: Self = Self {
        resources: RequestResources {
            cpu: 0,
            memory_mib: 0,
        },
        missing: false,
    };

    const ABSENT: Self = Self {
        resources: RequestResources {
            cpu: 0,
            memory_mib: 0,
        },
        missing: true,
    };
}

/// Maps a `Schedule` hint onto the request side of the score.
///
/// The table is closed — every shape `ScheduleRequest` can carry has a row,
/// including the two that look like they cannot happen:
///
/// - `hint: None` — a legacy or third-party caller. Missing.
/// - `Some(hint)` whose `kind` is `None` — reachable, because the `oneof`
///   is itself an `Option` on the Rust side: an empty hint decodes this
///   way, and so does one carrying only a `oneof` tag a future build
///   assigns. Missing.
///
/// 🔴 "Missing is always zero" holds only for
/// `NativeNodePlacement::place_new`, this repository's own producer. It is
/// not a property of `Schedule` traffic in general, which is why the
/// counter exists rather than an assertion.
pub fn request_from_hint(hint: Option<&ScheduleRequestHint>) -> ShadowRequest {
    let Some(kind) = hint.and_then(|h| h.kind.as_ref()) else {
        return ShadowRequest::ABSENT;
    };
    match kind {
        Kind::NewSandbox(new_sandbox) => {
            match (new_sandbox.cpu_count, new_sandbox.memory_mib) {
                (None, None) => ShadowRequest::ABSENT,
                // One field present and one absent is a partial answer: the
                // present half is used at its stated value, the absent half
                // contributes nothing, and the call is still counted as
                // missing so the counter does not read as full coverage.
                (cpu, memory_mib) => ShadowRequest {
                    resources: RequestResources {
                        cpu: cpu.unwrap_or(0),
                        memory_mib: memory_mib.unwrap_or(0),
                    },
                    missing: cpu.is_none() || memory_mib.is_none(),
                },
            }
        }
        // 🔴 `memory_mb` is read as MiB. Nothing in this repository
        // constructs this hint (the production producer always builds
        // `NewSandbox`), and the HTTP cold-create path this hint describes
        // reads its own `memoryMB` as MiB (`SandboxResources::memory_mib`,
        // `src/api/impls/sandbox.rs`). The historical field name is left
        // alone; the unit it is read in is settled here.
        Kind::NewColdSandbox(cold) => ShadowRequest {
            resources: RequestResources {
                cpu: cold.cpu_count,
                memory_mib: cold.memory_mb,
            },
            missing: false,
        },
    }
}

/// The 20 metric handles, resolved once at service construction.
struct ShadowMetricHandles {
    /// `[source][agrees]`, `agrees` indexed `0 = false`, `1 = true`.
    agreement: [[Counter; 2]; 2],
    /// `[source][class]`, class in [`CLASSES`] order.
    classification: [[Counter; CLASSES.len()]; 2],
    pressure_spread: [Histogram; 2],
    missing_request_resources: [Counter; 2],
}

impl ShadowMetricHandles {
    fn new() -> Self {
        Self {
            agreement: SOURCES.map(|source| {
                ["false", "true"].map(|agrees| {
                    metrics::counter!(
                        AGREEMENT_METRIC,
                        "source" => source.as_str(),
                        "agrees" => agrees,
                    )
                })
            }),
            classification: SOURCES.map(|source| {
                CLASSES.map(|class| {
                    metrics::counter!(
                        CLASSIFICATION_METRIC,
                        "source" => source.as_str(),
                        "class" => class,
                    )
                })
            }),
            pressure_spread: SOURCES.map(
                |source| metrics::histogram!(PRESSURE_SPREAD_METRIC, "source" => source.as_str()),
            ),
            missing_request_resources: SOURCES.map(|source| {
                metrics::counter!(
                    MISSING_REQUEST_RESOURCES_METRIC,
                    "source" => source.as_str(),
                )
            }),
        }
    }
}

impl std::fmt::Debug for ShadowMetricHandles {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ShadowMetricHandles")
    }
}

/// What one shadow evaluation concluded. Returned for tests; production
/// drops it on the floor, which is the point.
#[derive(Debug, Clone, PartialEq)]
pub struct ShadowOutcome {
    /// The node best-of-K would have picked, if there was a sample at all.
    pub chosen_node_id: Option<String>,
    /// Whether that node is the one the real strategy actually returned.
    pub agrees: bool,
    /// The sampled candidates' indices into the candidate slice, in draw
    /// order.
    pub sampled: Vec<usize>,
    /// `max - min` over the sample's `Scored` pressures, present only when
    /// the sample held at least two of them — a "spread" over one value is
    /// zero by definition and would only dilute the histogram.
    pub pressure_spread: Option<f64>,
}

/// The shadow scorer: a sampling width, its metric handles, and (in tests
/// only) a scripted draw transcript.
#[derive(Debug)]
pub struct ShadowPlacement {
    k: u32,
    metrics: ShadowMetricHandles,
    /// 🔴 `None` in every production build: `with_scripted_rng` is
    /// test-gated, so the `Mutex` below is never constructed, never
    /// contended, and never locked on a placement. It exists so a test can
    /// pin which nodes the sample drew — the one input that decides a tie.
    scripted: Option<Mutex<ScriptedRng>>,
}

impl ShadowPlacement {
    /// 🔴 Constructs the metric handles. Call this while the process's
    /// recorder is installed — for `NodeRegistryGrpcService` that is its own
    /// construction, which is where this belongs anyway.
    pub fn new(k: u32) -> Self {
        Self {
            k,
            metrics: ShadowMetricHandles::new(),
            scripted: None,
        }
    }

    /// The sampling width in force. `0` is refused at config load
    /// (`AppConfig::validate`); a `ShadowPlacement` built with it evaluates
    /// nothing rather than panicking.
    pub fn k(&self) -> u32 {
        self.k
    }

    #[cfg(any(test, feature = "test-support"))]
    #[must_use]
    pub fn with_scripted_rng(mut self, draws: impl Into<Vec<usize>>) -> Self {
        self.scripted = Some(Mutex::new(ScriptedRng::new(draws)));
        self
    }

    /// Rewinds the scripted transcript — §4.2's "每例重置 scripted RNG",
    /// for a fixture that runs the same sampler twice.
    #[cfg(any(test, feature = "test-support"))]
    pub fn reset_scripted_rng(&self) {
        if let Some(scripted) = &self.scripted {
            if let Ok(mut rng) = scripted.lock() {
                rng.reset();
            }
        }
    }

    /// Scores the candidate list the real strategy was just handed, and
    /// records what best-of-K would have done differently.
    ///
    /// Total and infallible: no `?`, no `unwrap`, no panic, no IO, no
    /// registry access, and no path back into the caller's return value.
    /// `actual_node_id` is the node the real strategy already chose — it is
    /// read, never written.
    pub fn evaluate(
        &self,
        candidates: &[ShadowCandidate<'_>],
        request: ShadowRequest,
        source: ShadowSource,
        actual_node_id: &str,
    ) -> ShadowOutcome {
        let src = source.index();
        if request.missing {
            self.metrics.missing_request_resources[src].increment(1);
        }

        // Pass 1: classify every candidate, tallying on the stack. This is
        // where the O(N) work is, and it emits nothing.
        let mut class_counts = [0u64; CLASSES.len()];
        for candidate in candidates {
            let classification = classify(
                candidate.rich.snapshot.as_ref(),
                candidate.freshness,
                request.resources,
            );
            class_counts[class_index(&classification)] += 1;
        }
        for (index, count) in class_counts.iter().enumerate() {
            if *count > 0 {
                self.metrics.classification[src][index].increment(*count);
            }
        }

        let sampled = self.draw(candidates.len());
        if sampled.is_empty() {
            return ShadowOutcome {
                chosen_node_id: None,
                agrees: false,
                sampled,
                pressure_spread: None,
            };
        }

        // Pass 2: re-classify only the sample. Cheaper than carrying an
        // N-element `Vec<Classification>` across the draw, and `classify`
        // is pure, so the two passes cannot disagree.
        let mut best: Option<(usize, f64)> = None;
        let mut lowest = f64::INFINITY;
        let mut highest = f64::NEG_INFINITY;
        let mut scored_in_sample = 0usize;
        for &index in &sampled {
            let Some(candidate) = candidates.get(index) else {
                continue;
            };
            let Classification::Scored { pressure } = classify(
                candidate.rich.snapshot.as_ref(),
                candidate.freshness,
                request.resources,
            ) else {
                continue;
            };
            scored_in_sample += 1;
            lowest = lowest.min(pressure);
            highest = highest.max(pressure);
            // Strictly less than: a tie keeps whichever the *sample order*
            // reached first. Breaking ties by node id would make a
            // homogeneous or brand-new cluster's shadow point at the same
            // machine forever and report agreement that means nothing.
            if best.is_none_or(|(_, best_pressure)| pressure < best_pressure) {
                best = Some((index, pressure));
            }
        }

        let pressure_spread = if scored_in_sample >= 2 {
            let spread = highest - lowest;
            self.metrics.pressure_spread[src].record(spread);
            Some(spread)
        } else {
            None
        };

        // All-unknown samples still produce a choice: the first drawn. The
        // cluster having told us nothing is not a reason to stop reporting
        // what best-of-K would have done.
        let chosen_index = best
            .map(|(index, _)| index)
            .or_else(|| sampled.first().copied());
        let chosen_node_id = chosen_index
            .and_then(|index| candidates.get(index))
            .map(|candidate| candidate.rich.node.id.clone());
        let agrees = chosen_node_id
            .as_deref()
            .is_some_and(|id| id == actual_node_id);
        self.metrics.agreement[src][usize::from(agrees)].increment(1);

        ShadowOutcome {
            chosen_node_id,
            agrees,
            sampled,
            pressure_spread,
        }
    }

    fn draw(&self, n: usize) -> Vec<usize> {
        match &self.scripted {
            Some(scripted) => match scripted.lock() {
                Ok(mut rng) => sample_without_replacement(n, self.k, &mut *rng),
                // A poisoned test-only mutex must not take the placement
                // path down with it.
                Err(_) => Vec::new(),
            },
            None => sample_without_replacement(n, self.k, &mut ThreadShadowRng),
        }
    }
}

impl Default for ShadowPlacement {
    fn default() -> Self {
        Self::new(DEFAULT_PLACEMENT_SHADOW_K)
    }
}

#[cfg(test)]
mod tests;
