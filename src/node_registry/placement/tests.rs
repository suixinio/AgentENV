//! The shadow scorer's fixture suite.
//!
//! # Why these fixtures and not "a couple of plausible nodes"
//!
//! Every fixture below exists to falsify one specific wrong implementation,
//! and the set is chosen so that no single wrong implementation survives all
//! of them:
//!
//! - **F-1a** kills "score CPU only" — its answer is decided purely by the
//!   memory dimension.
//! - **F-1b** kills "score memory only" — its answer is decided purely by
//!   the CPU dimension, and it is the *other* node than F-1a's, so
//!   "always pick the first candidate" and "always pick the last" die
//!   together across the pair.
//! - **F-2** kills "pick the most absolute free capacity": `node-a` has 8
//!   free vCPU to `node-z`'s 3 and still loses, because pressure is a ratio.
//! - **F-3** kills "ignore the request size": the same two nodes, scored
//!   twice with a small and a large request, must answer differently.
//! - **F-8** kills both MiB/byte mistakes at once by pinning an exact
//!   `f64`.
//!
//! Every numeric value here is a specification value from
//! `docs/proposals/2026-08-30-e2b-alignment-placement-scoring.md` §4.2,
//! independently verified there. They are not "about right" — an
//! implementation that computes something else is wrong, not differently
//! rounded.
//!
//! Each fixture is also run with its candidate list reversed. The answer is
//! a property of the nodes, not of the order they were discovered in.

use super::sample::{sample_without_replacement, ScriptedRng};
use super::score::{
    classify, Classification, RequestResources, SnapshotFreshness, UnknownReason, BYTES_PER_MIB,
};
use super::*;

use crate::node_registry::types::Node;
use crate::proto::scheduler::{NodeSnapshot, NodeStatus};

const GIB: u64 = 1024 * 1024 * 1024;

/// One fixture node, in the units §4.2's table is written in.
#[derive(Debug, Clone, Copy)]
struct NodeSpec {
    id: &'static str,
    cpu_count: u32,
    allocated_cpu: u32,
    memory_total_bytes: u64,
    allocated_memory_bytes: u64,
}

fn rich(spec: NodeSpec) -> RichNode {
    RichNode::with_snapshot(
        Node {
            id: spec.id.to_string(),
            endpoint: String::new(),
            pod_name: String::new(),
        },
        NodeSnapshot {
            status: NodeStatus::Ready as i32,
            allocated_cpu: spec.allocated_cpu,
            allocated_memory_bytes: spec.allocated_memory_bytes,
            cpu_count: spec.cpu_count,
            memory_total_bytes: spec.memory_total_bytes,
            ..Default::default()
        },
    )
}

fn request(cpu: u32, memory_mib: u64) -> ShadowRequest {
    ShadowRequest {
        resources: RequestResources { cpu, memory_mib },
        missing: false,
    }
}

fn fresh_candidates(nodes: &[RichNode]) -> Vec<ShadowCandidate<'_>> {
    nodes
        .iter()
        .map(|rich| ShadowCandidate {
            rich,
            freshness: Some(SnapshotFreshness::Fresh),
        })
        .collect()
}

fn pressure_of(spec: NodeSpec, req: ShadowRequest) -> f64 {
    let node = rich(spec);
    match classify(
        node.snapshot.as_ref(),
        Some(SnapshotFreshness::Fresh),
        req.resources,
    ) {
        Classification::Scored { pressure } => pressure,
        other => panic!("{} should be scored, got {other:?}", spec.id),
    }
}

/// Runs one fixture forwards and backwards. `draws` is the scripted
/// shrinking-pool transcript; `K = N`, so the whole candidate list is
/// sampled and the sample order is exactly what `draws` says.
fn assert_fixture(label: &str, specs: [NodeSpec; 2], req: ShadowRequest, expected: &str) {
    for (direction, ordered) in [("forward", specs), ("reversed", [specs[1], specs[0]])] {
        let nodes: Vec<RichNode> = ordered.iter().copied().map(rich).collect();
        let candidates = fresh_candidates(&nodes);
        let shadow = ShadowPlacement::new(candidates.len() as u32).with_scripted_rng([0, 0]);
        // §4.2: every case starts from a reset transcript.
        shadow.reset_scripted_rng();
        let outcome = shadow.evaluate(&candidates, req, ShadowSource::Schedule, "");
        assert_eq!(
            outcome.chosen_node_id.as_deref(),
            Some(expected),
            "{label} ({direction}) must pick {expected}: {outcome:?}"
        );
    }
}

/// F-1a — the memory dimension decides. `node-a` is the emptier machine on
/// CPU and still loses, because the request fills its memory exactly.
#[test]
fn f1a_memory_dimension_decides() {
    let node_a = NodeSpec {
        id: "node-a",
        cpu_count: 8,
        allocated_cpu: 1,
        memory_total_bytes: 8 * GIB,
        allocated_memory_bytes: 7 * GIB,
    };
    let node_z = NodeSpec {
        id: "node-z",
        cpu_count: 8,
        allocated_cpu: 6,
        memory_total_bytes: 64 * GIB,
        allocated_memory_bytes: 8 * GIB,
    };
    let req = request(1, 1024);

    // 🔴 The choice first, and the per-node pressures after it. The order
    // matters for what a failure *says*: a scorer that returns a constant
    // makes every candidate tie, the tie falls to the first in sample
    // order, and F-1a is the fixture whose sample order is pinned — so the
    // constant is caught as "picked node-a, should have picked node-z",
    // which names the defect, rather than as an arithmetic mismatch on one
    // node.
    assert_fixture("F-1a", [node_a, node_z], req, "node-z");

    // after_cpu 0.25 / after_mem 1.0 -> 1.0
    assert_eq!(pressure_of(node_a, req), 1.0);
    // after_cpu 0.875 / after_mem 0.140625 -> 0.875
    assert_eq!(pressure_of(node_z, req), 0.875);
}

/// F-1b — the CPU dimension decides, and the winner is the *other* end of
/// the list than F-1a's. Together the two kill "always take the first
/// candidate" and "always take the last".
#[test]
fn f1b_cpu_dimension_decides() {
    let node_a = NodeSpec {
        id: "node-a",
        cpu_count: 8,
        allocated_cpu: 2,
        memory_total_bytes: 16 * GIB,
        allocated_memory_bytes: 12 * GIB,
    };
    let node_z = NodeSpec {
        id: "node-z",
        cpu_count: 8,
        allocated_cpu: 7,
        memory_total_bytes: 64 * GIB,
        allocated_memory_bytes: 8 * GIB,
    };
    let req = request(1, 1024);

    assert_fixture("F-1b", [node_a, node_z], req, "node-a");

    // after_cpu 0.375 / after_mem 0.8125 -> 0.8125
    assert_eq!(pressure_of(node_a, req), 0.8125);
    // after_cpu 1.0 / after_mem 0.140625 -> 1.0
    assert_eq!(pressure_of(node_z, req), 1.0);
}

/// F-2 — capacity is not the answer; occupancy is. `node-a` has 8 free vCPU
/// to `node-z`'s 3 and 32 GiB free memory to `node-z`'s 6, and still loses.
#[test]
fn f2_ratio_beats_absolute_free_capacity() {
    let node_a = NodeSpec {
        id: "node-a",
        cpu_count: 64,
        allocated_cpu: 56,
        memory_total_bytes: 256 * GIB,
        allocated_memory_bytes: 224 * GIB,
    };
    let node_z = NodeSpec {
        id: "node-z",
        cpu_count: 4,
        allocated_cpu: 1,
        memory_total_bytes: 8 * GIB,
        allocated_memory_bytes: 2 * GIB,
    };
    let req = request(1, 1024);

    assert_fixture("F-2", [node_a, node_z], req, "node-z");

    // after_cpu 0.890625 / after_mem 0.87890625 -> 0.890625
    assert_eq!(pressure_of(node_a, req), 0.890625);
    // after_cpu 0.5 / after_mem 0.375 -> 0.5
    assert_eq!(pressure_of(node_z, req), 0.5);
}

/// F-3 — one pair of nodes, two request sizes, two different answers. An
/// implementation that ignores `requested_*` gives the same answer twice
/// and fails whichever half it does not happen to match.
#[test]
fn f3_request_size_reverses_the_answer() {
    let node_a = NodeSpec {
        id: "node-a",
        cpu_count: 8,
        allocated_cpu: 4,
        memory_total_bytes: 8 * GIB,
        allocated_memory_bytes: GIB,
    };
    let node_z = NodeSpec {
        id: "node-z",
        cpu_count: 8,
        allocated_cpu: 6,
        memory_total_bytes: 64 * GIB,
        allocated_memory_bytes: 8 * GIB,
    };

    let small = request(1, 1024);
    let large = request(1, 7168);

    // The two answers, first: an implementation that ignores the request
    // size fails on whichever of these two it does not happen to match,
    // which is the defect stated plainly.
    assert_fixture("F-3 small", [node_a, node_z], small, "node-a");
    assert_fixture("F-3 large", [node_a, node_z], large, "node-z");

    // after_cpu 0.625 / after_mem 0.25 -> 0.625
    assert_eq!(pressure_of(node_a, small), 0.625);
    // after_cpu 0.875 / after_mem 0.140625 -> 0.875
    assert_eq!(pressure_of(node_z, small), 0.875);
    // after_cpu 0.625 / after_mem 1.0 -> 1.0
    assert_eq!(pressure_of(node_a, large), 1.0);
    // after_cpu 0.875 / after_mem 0.234375 -> 0.875
    assert_eq!(pressure_of(node_z, large), 0.875);
}

/// F-8 — the unit fixture. Memory must dominate here, and only if the
/// request's MiB are converted to bytes exactly once:
///
/// - no conversion: `(268435456 + 512) / 1073741824` ~= 0.2500004768, and
///   CPU's 0.25 would no longer be the loser.
/// - converted twice: `(268435456 + 562949953421312) / 1073741824`
///   ~= 524288.25.
#[test]
fn f8_mib_is_converted_exactly_once() {
    let single = NodeSpec {
        id: "node-only",
        cpu_count: 8,
        allocated_cpu: 0,
        memory_total_bytes: GIB,
        allocated_memory_bytes: 256 * 1024 * 1024,
    };
    let req = request(2, 512);

    // after_cpu 0.25 / after_mem 0.75 -> 0.75, exactly.
    assert_eq!(pressure_of(single, req), 0.75);

    // The two wrong answers, spelled out, so this test says what it is
    // guarding rather than only that 0.75 held.
    let unconverted = (single.allocated_memory_bytes + req.resources.memory_mib) as f64
        / single.memory_total_bytes as f64;
    let double_converted = (single.allocated_memory_bytes
        + req.resources.memory_mib * BYTES_PER_MIB * BYTES_PER_MIB)
        as f64
        / single.memory_total_bytes as f64;
    assert_ne!(unconverted, 0.75);
    assert_ne!(double_converted, 0.75);
}

/// F-4 — the classification boundary, including the one case that is
/// deliberately *not* a boundary.
#[test]
fn f4_classification_boundaries() {
    let healthy = NodeSnapshot {
        status: NodeStatus::Ready as i32,
        allocated_cpu: 1,
        allocated_memory_bytes: GIB,
        cpu_count: 8,
        memory_total_bytes: 8 * GIB,
        ..Default::default()
    };
    let req = RequestResources {
        cpu: 1,
        memory_mib: 1024,
    };

    assert_eq!(
        classify(Some(&healthy), Some(SnapshotFreshness::Stale), req),
        Classification::Unknown {
            reason: UnknownReason::Stale
        }
    );
    assert_eq!(
        classify(Some(&healthy), Some(SnapshotFreshness::ClockSkew), req),
        Classification::Unknown {
            reason: UnknownReason::ClockSkew
        }
    );
    assert_eq!(
        classify(None, None, req),
        Classification::Unknown {
            reason: UnknownReason::NoSnapshot
        }
    );

    let no_cpus = NodeSnapshot {
        cpu_count: 0,
        ..healthy.clone()
    };
    assert_eq!(
        classify(Some(&no_cpus), Some(SnapshotFreshness::Fresh), req),
        Classification::Unknown {
            reason: UnknownReason::ZeroDenominator
        }
    );
    let no_memory = NodeSnapshot {
        memory_total_bytes: 0,
        ..healthy.clone()
    };
    assert_eq!(
        classify(Some(&no_memory), Some(SnapshotFreshness::Fresh), req),
        Classification::Unknown {
            reason: UnknownReason::ZeroDenominator
        }
    );

    let saturated_cpu = NodeSnapshot {
        allocated_cpu: u32::MAX,
        ..healthy.clone()
    };
    assert_eq!(
        classify(Some(&saturated_cpu), Some(SnapshotFreshness::Fresh), req),
        Classification::Unknown {
            reason: UnknownReason::Overflow
        }
    );
    let saturated_memory = NodeSnapshot {
        allocated_memory_bytes: u64::MAX,
        ..healthy.clone()
    };
    assert_eq!(
        classify(Some(&saturated_memory), Some(SnapshotFreshness::Fresh), req),
        Classification::Unknown {
            reason: UnknownReason::Overflow
        }
    );
    // The request side can overflow on its own: MiB -> bytes is a
    // multiplication by 2^20.
    assert_eq!(
        classify(
            Some(&healthy),
            Some(SnapshotFreshness::Fresh),
            RequestResources {
                cpu: 0,
                memory_mib: u64::MAX,
            },
        ),
        Classification::Unknown {
            reason: UnknownReason::Overflow
        }
    );

    // 🔴 And the one that must NOT be an Unknown. An over-allocated node is
    // a real, orderable state — it is worse than a node at 0.9, which is
    // exactly what best-of-K needs to know. Turning it into `Unknown` would
    // make the most overloaded machine in the fleet indistinguishable from
    // one that has never reported.
    let overallocated = NodeSnapshot {
        allocated_cpu: 16,
        cpu_count: 8,
        allocated_memory_bytes: GIB,
        memory_total_bytes: 8 * GIB,
        ..healthy.clone()
    };
    assert_eq!(
        classify(Some(&overallocated), Some(SnapshotFreshness::Fresh), req),
        // after_cpu (16+1)/8 = 2.125, after_mem (1+1)/8 = 0.25
        Classification::Scored { pressure: 2.125 }
    );
}

/// F-5 — a sample that is partly, then wholly, unscoreable.
#[test]
fn f5_unknown_mixtures_still_produce_a_choice() {
    let scored = rich(NodeSpec {
        id: "node-scored",
        cpu_count: 8,
        allocated_cpu: 1,
        memory_total_bytes: 8 * GIB,
        allocated_memory_bytes: GIB,
    });
    let never_reported = RichNode::new(Node {
        id: "node-silent".to_string(),
        endpoint: String::new(),
        pod_name: String::new(),
    });

    // Partly unknown: the one scoreable candidate wins even though it was
    // drawn second.
    let nodes = [never_reported.clone(), scored.clone()];
    let candidates = vec![
        ShadowCandidate {
            rich: &nodes[0],
            freshness: None,
        },
        ShadowCandidate {
            rich: &nodes[1],
            freshness: Some(SnapshotFreshness::Fresh),
        },
    ];
    let shadow = ShadowPlacement::new(2).with_scripted_rng([0, 0]);
    let outcome = shadow.evaluate(&candidates, request(1, 1024), ShadowSource::Schedule, "");
    assert_eq!(outcome.chosen_node_id.as_deref(), Some("node-scored"));
    // One `Scored` in the sample is not a spread.
    assert_eq!(outcome.pressure_spread, None);

    // Wholly unknown: still a choice, and it is the first one drawn.
    let silent_b = RichNode::new(Node {
        id: "node-silent-b".to_string(),
        endpoint: String::new(),
        pod_name: String::new(),
    });
    let nodes = [never_reported, silent_b];
    let candidates = vec![
        ShadowCandidate {
            rich: &nodes[0],
            freshness: None,
        },
        ShadowCandidate {
            rich: &nodes[1],
            freshness: None,
        },
    ];
    let shadow = ShadowPlacement::new(2).with_scripted_rng([1, 0]);
    let outcome = shadow.evaluate(&candidates, request(1, 1024), ShadowSource::Schedule, "");
    assert_eq!(outcome.chosen_node_id.as_deref(), Some("node-silent-b"));
    assert_eq!(outcome.sampled, vec![1, 0]);
}

/// F-6 — the K boundary. `K = 0` is refused at config load, so what is
/// pinned here is the rest of the range plus the default's value.
#[test]
fn f6_k_boundaries() {
    let specs = [
        NodeSpec {
            id: "node-a",
            cpu_count: 8,
            allocated_cpu: 7,
            memory_total_bytes: 8 * GIB,
            allocated_memory_bytes: GIB,
        },
        NodeSpec {
            id: "node-z",
            cpu_count: 8,
            allocated_cpu: 0,
            memory_total_bytes: 8 * GIB,
            allocated_memory_bytes: GIB,
        },
    ];
    let nodes: Vec<RichNode> = specs.iter().copied().map(rich).collect();
    let candidates = fresh_candidates(&nodes);
    let req = request(1, 1024);

    // K = 1: only the drawn candidate is considered, so the *worse* node
    // wins when it is the one drawn. This is what makes K a real knob
    // rather than decoration.
    let shadow = ShadowPlacement::new(1).with_scripted_rng([0]);
    let outcome = shadow.evaluate(&candidates, req, ShadowSource::Schedule, "");
    assert_eq!(outcome.sampled.len(), 1);
    assert_eq!(outcome.chosen_node_id.as_deref(), Some("node-a"));

    // K = N: the whole list, so the better node wins from either draw
    // order.
    for draws in [[0usize, 0usize], [1, 0]] {
        let shadow = ShadowPlacement::new(2).with_scripted_rng(draws);
        let outcome = shadow.evaluate(&candidates, req, ShadowSource::Schedule, "");
        assert_eq!(outcome.sampled.len(), 2);
        assert_eq!(outcome.chosen_node_id.as_deref(), Some("node-z"));
    }

    // K > N: capped at N, never a panic and never a repeat.
    let shadow = ShadowPlacement::new(9).with_scripted_rng([0, 0]);
    let outcome = shadow.evaluate(&candidates, req, ShadowSource::Schedule, "");
    assert_eq!(outcome.sampled, vec![0, 1]);
    assert_eq!(outcome.chosen_node_id.as_deref(), Some("node-z"));

    assert_eq!(DEFAULT_PLACEMENT_SHADOW_K, 3);
    assert_eq!(ShadowPlacement::default().k(), 3);
}

/// M-6's direct target. A sampler that returned "the first K" would answer
/// `[a, b]` here; the scripted shrinking-pool transcript `[3, 0]` says
/// `[d, a]`.
#[test]
fn sampling_follows_the_draw_transcript() {
    let mut rng = ScriptedRng::new([3, 0]);
    assert_eq!(sample_without_replacement(4, 2, &mut rng), vec![3, 0]);
}

/// M-7's direct target. Two draws of slot 0 over a shrinking pool of two
/// must yield two *different* elements: the second slot-0 addresses what is
/// left, not the original list.
#[test]
fn sampling_is_without_replacement() {
    let mut rng = ScriptedRng::new([0, 0]);
    let picked = sample_without_replacement(2, 2, &mut rng);
    assert_eq!(picked, vec![0, 1]);
    assert_eq!(picked.len(), 2, "the sample must be as wide as K");
    let distinct: std::collections::HashSet<usize> = picked.iter().copied().collect();
    assert_eq!(
        distinct.len(),
        2,
        "with replacement would give a set of one: {picked:?}"
    );
}

/// The spread is a property of the sample, and only exists when there is
/// something to spread between.
#[test]
fn pressure_spread_needs_two_scored_candidates() {
    let specs = [
        NodeSpec {
            id: "node-a",
            cpu_count: 8,
            allocated_cpu: 7,
            memory_total_bytes: 8 * GIB,
            allocated_memory_bytes: GIB,
        },
        NodeSpec {
            id: "node-z",
            cpu_count: 8,
            allocated_cpu: 0,
            memory_total_bytes: 8 * GIB,
            allocated_memory_bytes: GIB,
        },
    ];
    let nodes: Vec<RichNode> = specs.iter().copied().map(rich).collect();
    let candidates = fresh_candidates(&nodes);
    let req = request(1, 1024);

    // node-a: after_cpu (7+1)/8 = 1.0, after_mem 0.25 -> 1.0
    // node-z: after_cpu 0.125,        after_mem 0.25 -> 0.25
    let shadow = ShadowPlacement::new(2).with_scripted_rng([0, 0]);
    let outcome = shadow.evaluate(&candidates, req, ShadowSource::Schedule, "");
    assert_eq!(outcome.pressure_spread, Some(0.75));

    let shadow = ShadowPlacement::new(1).with_scripted_rng([0]);
    let outcome = shadow.evaluate(&candidates, req, ShadowSource::Schedule, "");
    assert_eq!(outcome.pressure_spread, None);
}

/// §3.4's table, at the mapping level. The wiring-level half (a real
/// `Schedule` call, and the producer that fills the hint in) lives in
/// `super::super::grpc_service`'s tests — this one pins the mapping
/// itself, including the two shapes that look unreachable and are not.
#[test]
fn hint_shapes_map_onto_a_closed_request_table() {
    use crate::proto::scheduler::{
        schedule_request_hint::Kind, NewColdSandboxHint, NewSandboxHint,
    };

    let absent = ShadowRequest {
        resources: RequestResources {
            cpu: 0,
            memory_mib: 0,
        },
        missing: true,
    };

    // hint = None
    assert_eq!(request_from_hint(None), absent);

    // Some(hint) with no kind at all.
    assert_eq!(
        request_from_hint(Some(&ScheduleRequestHint { kind: None })),
        absent
    );

    let new_sandbox = |cpu_count, memory_mib| ScheduleRequestHint {
        kind: Some(Kind::NewSandbox(NewSandboxHint {
            metadata: Default::default(),
            cpu_count,
            memory_mib,
        })),
    };

    assert_eq!(request_from_hint(Some(&new_sandbox(None, None))), absent);

    // CPU-only.
    assert_eq!(
        request_from_hint(Some(&new_sandbox(Some(4), None))),
        ShadowRequest {
            resources: RequestResources {
                cpu: 4,
                memory_mib: 0
            },
            missing: true,
        }
    );
    // Memory-only.
    assert_eq!(
        request_from_hint(Some(&new_sandbox(None, Some(2048)))),
        ShadowRequest {
            resources: RequestResources {
                cpu: 0,
                memory_mib: 2048
            },
            missing: true,
        }
    );
    // Both stated.
    assert_eq!(
        request_from_hint(Some(&new_sandbox(Some(2), Some(512)))),
        ShadowRequest {
            resources: RequestResources {
                cpu: 2,
                memory_mib: 512
            },
            missing: false,
        }
    );
    // 🔴 Explicit zero is an answer, not an omission. This is the whole
    // reason both fields carry proto3 presence.
    assert_eq!(
        request_from_hint(Some(&new_sandbox(Some(0), Some(0)))),
        ShadowRequest {
            resources: RequestResources {
                cpu: 0,
                memory_mib: 0
            },
            missing: false,
        }
    );

    // The cold hint has no presence to lose: both fields are plain scalars,
    // and `memory_mb` is read as MiB.
    assert_eq!(
        request_from_hint(Some(&ScheduleRequestHint {
            kind: Some(Kind::NewColdSandbox(NewColdSandboxHint {
                cpu_count: 3,
                memory_mb: 4096,
                images: Vec::new(),
                metadata: Default::default(),
            })),
        })),
        ShadowRequest {
            resources: RequestResources {
                cpu: 3,
                memory_mib: 4096
            },
            missing: false,
        }
    );

    // And the paused-restore path, which reads no hint at all and is not
    // counted as a caller omission.
    assert_eq!(
        ShadowRequest::PAUSED_LOOKUP,
        ShadowRequest {
            resources: RequestResources {
                cpu: 0,
                memory_mib: 0
            },
            missing: false,
        }
    );
}

/// The `class` label's value space is closed and is the one
/// `Classification` reports. A new variant that forgot to extend `CLASSES`
/// would land on the wrong handle, silently.
#[test]
fn class_labels_cover_every_classification() {
    let all = [
        Classification::Scored { pressure: 0.0 },
        Classification::Unknown {
            reason: UnknownReason::NoSnapshot,
        },
        Classification::Unknown {
            reason: UnknownReason::Stale,
        },
        Classification::Unknown {
            reason: UnknownReason::ClockSkew,
        },
        Classification::Unknown {
            reason: UnknownReason::ZeroDenominator,
        },
        Classification::Unknown {
            reason: UnknownReason::Overflow,
        },
    ];
    assert_eq!(all.len(), CLASSES.len());
    for classification in &all {
        assert_eq!(
            CLASSES[class_index(classification)],
            classification.as_str()
        );
    }
    assert_eq!(ShadowSource::Schedule.as_str(), "schedule");
    assert_eq!(ShadowSource::PausedLookup.as_str(), "paused_lookup");
}
