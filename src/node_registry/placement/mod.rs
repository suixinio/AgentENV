//! Shadow best-of-K resource scoring for placement metrics.
//!
//! Evaluation runs only after round-robin has chosen and cannot affect the returned node.
//! Metric handles resolve at construction, classification aggregates on the stack, and
//! candidates borrow the exact list used by real placement.
//!
//! This remains shadow-only until placement has cluster-wide pending-assignment accounting;
//! heartbeat snapshots alone lag concurrent creates and would herd bursts onto stale minima.
//! Owner and review condition: `docs/proposals/2026-08-31-residue-decisions.md` §D4.

pub mod sample;
pub mod score;

use std::sync::Mutex;

use metrics::{Counter, Histogram};

use crate::node_registry::types::RichNode;
use crate::proto::scheduler::{schedule_request_hint::Kind, ScheduleRequestHint};

use sample::{sample_without_replacement, ScriptedRng, ThreadShadowRng};
use score::{classify, Classification, RequestResources, SnapshotFreshness, UnknownReason};

/// Default shadow sample width.
pub const DEFAULT_PLACEMENT_SHADOW_K: u32 = 3;

const AGREEMENT_METRIC: &str = "agentenv_api_placement_shadow_agreement_total";
const CLASSIFICATION_METRIC: &str = "agentenv_api_placement_shadow_classification_total";
const PRESSURE_SPREAD_METRIC: &str = "agentenv_api_placement_shadow_pressure_spread";
const MISSING_REQUEST_RESOURCES_METRIC: &str =
    "agentenv_api_placement_missing_request_resources_total";

/// Closed label set identifying the placement caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShadowSource {
    Schedule,
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

/// Candidate plus freshness from the same registry read.
///
/// `freshness` is absent exactly when the borrowed candidate has no snapshot.
#[derive(Debug, Clone, Copy)]
pub struct ShadowCandidate<'a> {
    pub rich: &'a RichNode,
    pub freshness: Option<SnapshotFreshness>,
}

/// Request resources plus explicit-presence coverage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShadowRequest {
    pub resources: RequestResources,
    pub missing: bool,
}

impl ShadowRequest {
    /// Resource-free paused lookup, not counted as missing caller data.
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

/// Maps every schedule-hint shape onto scoring resources and presence.
pub fn request_from_hint(hint: Option<&ScheduleRequestHint>) -> ShadowRequest {
    let Some(kind) = hint.and_then(|h| h.kind.as_ref()) else {
        return ShadowRequest::ABSENT;
    };
    match kind {
        Kind::NewSandbox(new_sandbox) => {
            match (new_sandbox.cpu_count, new_sandbox.memory_mib) {
                (None, None) => ShadowRequest::ABSENT,
                // Partial presence uses zero for the absent half and counts as missing.
                (cpu, memory_mib) => ShadowRequest {
                    resources: RequestResources {
                        cpu: cpu.unwrap_or(0),
                        memory_mib: memory_mib.unwrap_or(0),
                    },
                    missing: cpu.is_none() || memory_mib.is_none(),
                },
            }
        }
        // Historical `memory_mb` is interpreted as MiB.
        Kind::NewColdSandbox(cold) => ShadowRequest {
            resources: RequestResources {
                cpu: cold.cpu_count,
                memory_mib: cold.memory_mb,
            },
            missing: false,
        },
    }
}

struct ShadowMetricHandles {
    agreement: [[Counter; 2]; 2],
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

/// Result of one shadow evaluation.
#[derive(Debug, Clone, PartialEq)]
pub struct ShadowOutcome {
    pub chosen_node_id: Option<String>,
    pub agrees: bool,
    pub sampled: Vec<usize>,
    /// Pressure range when at least two sampled candidates were scoreable.
    pub pressure_spread: Option<f64>,
}

/// Shadow scorer and its pre-resolved metric handles.
#[derive(Debug)]
pub struct ShadowPlacement {
    k: u32,
    metrics: ShadowMetricHandles,
    /// Test-only scripted sampler; absent in production.
    scripted: Option<Mutex<ScriptedRng>>,
}

impl ShadowPlacement {
    /// Constructs the scorer and resolves all metric handles.
    pub fn new(k: u32) -> Self {
        Self {
            k,
            metrics: ShadowMetricHandles::new(),
            scripted: None,
        }
    }

    /// Sampling width; zero produces no evaluation.
    pub fn k(&self) -> u32 {
        self.k
    }

    #[cfg(any(test, feature = "test-support"))]
    #[must_use]
    pub fn with_scripted_rng(mut self, draws: impl Into<Vec<usize>>) -> Self {
        self.scripted = Some(Mutex::new(ScriptedRng::new(draws)));
        self
    }

    /// Rewinds the test-scripted draw sequence.
    #[cfg(any(test, feature = "test-support"))]
    pub fn reset_scripted_rng(&self) {
        if let Some(scripted) = &self.scripted {
            if let Ok(mut rng) = scripted.lock() {
                rng.reset();
            }
        }
    }

    /// Infallibly records how best-of-K compares with an already-chosen real placement.
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

        // Classify all candidates and aggregate counts without allocation.
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

        // Re-classifying only sampled entries avoids an O(N) classification vector.
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
            // Ties preserve sample order rather than biasing one node ID.
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

        // All-unknown samples still choose the first draw for comparison.
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
                // Test-only mutex poisoning must not affect placement.
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
