//! Measures whole placement selection with and without shadow scoring at
//! 2, 100, and 1000 nodes.
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
            requires_egress_broker: false,
            metadata: Default::default(),
            cpu_count: Some(2),
            memory_mib: Some(2048),
            preferred_node_id: String::new(),
            excluded_node_ids: Vec::new(),
        })),
    }
}

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
                            &[],
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
