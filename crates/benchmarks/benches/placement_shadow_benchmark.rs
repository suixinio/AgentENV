//! What the placement shadow scorer costs, at three cluster sizes.
//!
//! The shadow scorer runs on every `Schedule` and on every paused-sandbox
//! restore that reaches the strategy — the hot path of sandbox creation.
//! It is allowed to cost something; it is not allowed to cost something
//! surprising, and "surprising" here has a specific shape: an `O(N)`
//! sequence of `metrics::counter!` macro calls, each of which takes the
//! Prometheus recorder's `RwLock`. That mistake is invisible to every
//! correctness test in the tree and would only show up as placement
//! latency under a wide fleet.
//!
//! So this measures the *whole selection*, twice:
//!
//! - `baseline` reconstructs the pre-shadow body out of the same public
//!   pieces it always used: one registry read per node, `filter_unschedulable`,
//!   `RoundRobinStrategy::select`. There is no production switch that turns
//!   the shadow off — deliberately, see `placement`'s module doc — so the
//!   comparison has to be built rather than toggled.
//! - `with_shadow` calls `select_node`, which is the same body plus the
//!   scorer.
//!
//! The difference between the two, at N = 2 / 100 / 1000, is the scorer's
//! cost.
//!
//! Run with: `cargo bench -p agentenv-benchmarks --bench placement_shadow`

use std::time::{Duration, SystemTime};

use aenv_core::binding_store::lookup::{select_node, ScheduleDeps};
use aenv_core::node_registry::filter::filter_unschedulable;
use aenv_core::node_registry::placement::{ShadowPlacement, ShadowSource};
use aenv_core::node_registry::registry::{AtomicNodeRegistry, NodeRegistry};
use aenv_core::node_registry::strategy::RoundRobinStrategy;
use aenv_core::node_registry::types::{Node, RichNode};
use aenv_core::proto::scheduler::{
    schedule_request_hint::Kind, HeartbeatRequest, NewSandboxHint, NodeSnapshot, NodeStatus,
    ScheduleRequestHint,
};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};

const SAMPLE_SIZE: usize = 50;
const WARM_UP_TIME: Duration = Duration::from_millis(200);
const MEASUREMENT_TIME: Duration = Duration::from_secs(1);

const GIB: u64 = 1024 * 1024 * 1024;

/// Cluster sizes: two (what a small deployment actually runs), a hundred,
/// and a thousand — the last one being where an accidental per-candidate
/// metric call would be unmistakable.
const CLUSTER_SIZES: [usize; 3] = [2, 100, 1000];

fn registry_with(node_count: usize) -> AtomicNodeRegistry {
    let nodes: Vec<Node> = (0..node_count)
        .map(|index| Node {
            id: format!("node-{index:05}"),
            endpoint: format!("http://10.0.0.{}:8000", index % 250),
            pod_name: String::new(),
        })
        .collect();
    let registry = AtomicNodeRegistry::new(nodes, Duration::from_secs(30));
    let now = SystemTime::now();
    for index in 0..node_count {
        let node_id = format!("node-{index:05}");
        registry
            .heartbeat(
                &HeartbeatRequest {
                    node_id: node_id.clone(),
                    cluster_id: "bench".to_string(),
                    service_instance_id: format!("{node_id}-instance"),
                    snapshot: Some(NodeSnapshot {
                        status: NodeStatus::Ready as i32,
                        // Spread the load so the scorer has a real ordering
                        // to compute rather than a field of ties.
                        allocated_cpu: (index % 8) as u32,
                        allocated_memory_bytes: (index as u64 % 8) * GIB,
                        cpu_count: 8,
                        memory_total_bytes: 8 * GIB,
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                now,
            )
            .expect("the node is in discovery");
    }
    registry
}

fn hint() -> ScheduleRequestHint {
    ScheduleRequestHint {
        kind: Some(Kind::NewSandbox(NewSandboxHint {
            metadata: Default::default(),
            cpu_count: Some(2),
            memory_mib: Some(2048),
        })),
    }
}

/// The selection as it was before the scorer existed.
fn baseline_select(
    registry: &AtomicNodeRegistry,
    strategy: &RoundRobinStrategy,
    hint: Option<&ScheduleRequestHint>,
) -> String {
    let discovered = registry.snapshot(false);
    let rich: Vec<RichNode> = discovered
        .into_iter()
        .map(|node| {
            let snapshot = registry.peek_observed(&node.id);
            RichNode { node, snapshot }
        })
        .collect();
    let eligible = filter_unschedulable(rich);
    strategy
        .select(&eligible, hint)
        .expect("the fixture always has candidates")
        .node
        .id
}

fn bench_placement_shadow(c: &mut Criterion) {
    let mut group = c.benchmark_group("placement_selection");
    let hint = hint();

    for node_count in CLUSTER_SIZES {
        let registry = registry_with(node_count);
        let strategy = RoundRobinStrategy::new();
        let shadow = ShadowPlacement::default();

        group.bench_with_input(
            BenchmarkId::new("baseline", node_count),
            &node_count,
            |b, _| {
                b.iter(|| std::hint::black_box(baseline_select(&registry, &strategy, Some(&hint))));
            },
        );

        group.bench_with_input(
            BenchmarkId::new("with_shadow", node_count),
            &node_count,
            |b, _| {
                let deps = ScheduleDeps {
                    node_registry: &registry,
                    strategy: &strategy,
                    shadow: &shadow,
                };
                b.iter(|| {
                    std::hint::black_box(
                        select_node(
                            &deps,
                            Some(&hint),
                            "",
                            ShadowSource::Schedule,
                            SystemTime::now(),
                        )
                        .expect("the fixture always has candidates"),
                    )
                });
            },
        );
    }

    group.finish();
}

fn criterion_config() -> Criterion {
    Criterion::default()
        .sample_size(SAMPLE_SIZE)
        .warm_up_time(WARM_UP_TIME)
        .measurement_time(MEASUREMENT_TIME)
}

criterion_group! {
    name = benches;
    config = criterion_config();
    targets = bench_placement_shadow
}
criterion_main!(benches);
