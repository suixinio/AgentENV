//! Deterministic fixtures for scoring, sampling, classification, and request mapping.

use super::sample::{sample_without_replacement, ScriptedRng};
use super::score::{
    classify, Classification, RequestResources, SnapshotFreshness, UnknownReason, BYTES_PER_MIB,
};
use super::*;

use crate::node_registry::types::Node;
use crate::proto::scheduler::{NodeSnapshot, NodeStatus};

const GIB: u64 = 1024 * 1024 * 1024;

/// Node fixture using the scoring table's units.
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

/// Runs a fixture in both candidate orders with a fully scripted sample.
fn assert_fixture(label: &str, specs: [NodeSpec; 2], req: ShadowRequest, expected: &str) {
    for (direction, ordered) in [("forward", specs), ("reversed", [specs[1], specs[0]])] {
        let nodes: Vec<RichNode> = ordered.iter().copied().map(rich).collect();
        let candidates = fresh_candidates(&nodes);
        let shadow = ShadowPlacement::new(candidates.len() as u32).with_scripted_rng([0, 0]);
        shadow.reset_scripted_rng();
        let outcome = shadow.evaluate(&candidates, req, ShadowSource::Schedule, "");
        assert_eq!(
            outcome.chosen_node_id.as_deref(),
            Some(expected),
            "{label} ({direction}) must pick {expected}: {outcome:?}"
        );
    }
}

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

    assert_fixture("F-1a", [node_a, node_z], req, "node-z");

    assert_eq!(pressure_of(node_a, req), 1.0);
    assert_eq!(pressure_of(node_z, req), 0.875);
}

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

    assert_eq!(pressure_of(node_a, req), 0.8125);
    assert_eq!(pressure_of(node_z, req), 1.0);
}

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

    assert_eq!(pressure_of(node_a, req), 0.890625);
    assert_eq!(pressure_of(node_z, req), 0.5);
}

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

    assert_fixture("F-3 small", [node_a, node_z], small, "node-a");
    assert_fixture("F-3 large", [node_a, node_z], large, "node-z");

    assert_eq!(pressure_of(node_a, small), 0.625);
    assert_eq!(pressure_of(node_z, small), 0.875);
    assert_eq!(pressure_of(node_a, large), 1.0);
    assert_eq!(pressure_of(node_z, large), 0.875);
}

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

    assert_eq!(pressure_of(single, req), 0.75);

    let unconverted = (single.allocated_memory_bytes + req.resources.memory_mib) as f64
        / single.memory_total_bytes as f64;
    let double_converted = (single.allocated_memory_bytes
        + req.resources.memory_mib * BYTES_PER_MIB * BYTES_PER_MIB)
        as f64
        / single.memory_total_bytes as f64;
    assert_ne!(unconverted, 0.75);
    assert_ne!(double_converted, 0.75);
}

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

    let overallocated = NodeSnapshot {
        allocated_cpu: 16,
        cpu_count: 8,
        allocated_memory_bytes: GIB,
        memory_total_bytes: 8 * GIB,
        ..healthy.clone()
    };
    assert_eq!(
        classify(Some(&overallocated), Some(SnapshotFreshness::Fresh), req),
        Classification::Scored { pressure: 2.125 }
    );
}

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
    assert_eq!(outcome.pressure_spread, None);

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

    let shadow = ShadowPlacement::new(1).with_scripted_rng([0]);
    let outcome = shadow.evaluate(&candidates, req, ShadowSource::Schedule, "");
    assert_eq!(outcome.sampled.len(), 1);
    assert_eq!(outcome.chosen_node_id.as_deref(), Some("node-a"));

    for draws in [[0usize, 0usize], [1, 0]] {
        let shadow = ShadowPlacement::new(2).with_scripted_rng(draws);
        let outcome = shadow.evaluate(&candidates, req, ShadowSource::Schedule, "");
        assert_eq!(outcome.sampled.len(), 2);
        assert_eq!(outcome.chosen_node_id.as_deref(), Some("node-z"));
    }

    let shadow = ShadowPlacement::new(9).with_scripted_rng([0, 0]);
    let outcome = shadow.evaluate(&candidates, req, ShadowSource::Schedule, "");
    assert_eq!(outcome.sampled, vec![0, 1]);
    assert_eq!(outcome.chosen_node_id.as_deref(), Some("node-z"));

    assert_eq!(DEFAULT_PLACEMENT_SHADOW_K, 3);
    assert_eq!(ShadowPlacement::default().k(), 3);
}

#[test]
fn sampling_follows_the_draw_transcript() {
    let mut rng = ScriptedRng::new([3, 0]);
    assert_eq!(sample_without_replacement(4, 2, &mut rng), vec![3, 0]);
}

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

    let shadow = ShadowPlacement::new(2).with_scripted_rng([0, 0]);
    let outcome = shadow.evaluate(&candidates, req, ShadowSource::Schedule, "");
    assert_eq!(outcome.pressure_spread, Some(0.75));

    let shadow = ShadowPlacement::new(1).with_scripted_rng([0]);
    let outcome = shadow.evaluate(&candidates, req, ShadowSource::Schedule, "");
    assert_eq!(outcome.pressure_spread, None);
}

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

    assert_eq!(request_from_hint(None), absent);

    assert_eq!(
        request_from_hint(Some(&ScheduleRequestHint { kind: None })),
        absent
    );

    let new_sandbox = |cpu_count, memory_mib| ScheduleRequestHint {
        kind: Some(Kind::NewSandbox(NewSandboxHint {
            requires_egress_broker: false,
            metadata: Default::default(),
            cpu_count,
            memory_mib,
            preferred_node_id: String::new(),
            excluded_node_ids: Vec::new(),
        })),
    };

    assert_eq!(request_from_hint(Some(&new_sandbox(None, None))), absent);

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
    // Explicit zero is present data, not omission.
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
