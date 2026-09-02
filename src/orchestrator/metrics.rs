use std::sync::atomic::{AtomicU64, Ordering};

use super::SandboxState;
use crate::types::SandboxResources;

/// A point-in-time snapshot of orchestrator metrics, returned by
/// `Orchestrator::metrics_snapshot()`.
///
/// The two creation counters are accumulated incrementally (see
/// [`OrchestratorCounters`]). All other resource fields are derived directly
/// from the live sandbox metadata at the time of the snapshot, so they cannot
/// drift from the orchestrator's actual state.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OrchestratorMetrics {
    pub create_successes: u64,
    pub create_fails: u64,
    pub running_sandbox_count: u32,
    pub starting_sandbox_count: u32,
    pub allocated_cpu: u32,
    pub allocated_memory_bytes: u64,
}

/// Monotonic creation counters maintained by the orchestrator.
///
/// These cannot be derived from the metadata store because they accumulate
/// outcomes of historical attempts (including failures whose metadata was
/// removed). They are stored as atomics so updates do not require a lock.
#[derive(Debug, Default)]
pub struct OrchestratorCounters {
    create_successes: AtomicU64,
    create_fails: AtomicU64,
    stale_handles_discarded: AtomicU64,
}

impl OrchestratorCounters {
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

    /// Records a cached handle discarded after its execution was superseded.
    pub fn record_stale_handle_discarded(&self) {
        self.stale_handles_discarded.fetch_add(1, Ordering::Relaxed);
    }

    pub fn stale_handles_discarded(&self) -> u64 {
        self.stale_handles_discarded.load(Ordering::Relaxed)
    }
}

/// The metrics contribution of a single sandbox in a given state.
///
/// This is the single source of truth for how a sandbox state maps to runtime
/// resource metrics:
///
/// - `running_sandbox_count` counts every state in which the VM is alive and
///   logically owned by the orchestrator from a serving / capacity
///   perspective: `Running`, plus the short-lived transitional states
///   `Pausing`, `Snapshotting`, and `Killing` where the VM is still up but is
///   being moved out of the running set. This matches the historical
///   incremental-counter behavior, where `running_sandbox_count` was not
///   decremented on entry to those transitional states.
/// - `starting_sandbox_count` counts only `Creating`.
/// - Allocated CPU / memory are counted in every state: every record names a
///   VM that holds its resources.
#[derive(Clone, Copy, Debug, Default)]
pub struct SandboxContribution {
    running_sandbox_count: u32,
    starting_sandbox_count: u32,
    allocated_cpu: u32,
    allocated_memory_bytes: u64,
}

impl SandboxContribution {
    pub fn new(state: SandboxState, resources: SandboxResources) -> Self {
        let counts_as_running = matches!(
            state,
            SandboxState::Running
                | SandboxState::Pausing
                | SandboxState::Snapshotting
                | SandboxState::Forking
                | SandboxState::Killing
        );
        let counts_as_starting = matches!(state, SandboxState::Creating);
        let memory_bytes = u64::from(resources.memory_mib) * 1024 * 1024;
        Self {
            running_sandbox_count: u32::from(counts_as_running),
            starting_sandbox_count: u32::from(counts_as_starting),
            allocated_cpu: resources.cpu_count,
            allocated_memory_bytes: memory_bytes,
        }
    }
}

/// Adds one sandbox's runtime resource contribution into an
/// [`OrchestratorMetrics`] snapshot under construction.
///
/// The counter fields (`create_successes` / `create_fails`) are intentionally
/// untouched and must be filled in by the caller from [`OrchestratorCounters`].
pub fn aggregate_resource_metrics(
    metrics: &mut OrchestratorMetrics,
    contribution: SandboxContribution,
) {
    metrics.running_sandbox_count = metrics
        .running_sandbox_count
        .saturating_add(contribution.running_sandbox_count);
    metrics.starting_sandbox_count = metrics
        .starting_sandbox_count
        .saturating_add(contribution.starting_sandbox_count);
    metrics.allocated_cpu = metrics
        .allocated_cpu
        .saturating_add(contribution.allocated_cpu);
    metrics.allocated_memory_bytes = metrics
        .allocated_memory_bytes
        .saturating_add(contribution.allocated_memory_bytes);
}

#[cfg(test)]
mod tests {
    use super::{
        aggregate_resource_metrics, OrchestratorCounters, OrchestratorMetrics, SandboxContribution,
    };
    use crate::orchestrator::{SandboxMetadata, SandboxState};
    use crate::types::SandboxResources;

    fn meta(state: SandboxState, cpu: u32, memory_mib: u32) -> SandboxMetadata {
        SandboxMetadata {
            state,
            resources: SandboxResources {
                cpu_count: cpu,
                memory_mib,
                disk_size_mib: 0,
            },
            ..Default::default()
        }
    }

    fn aggregate<'a>(metas: impl IntoIterator<Item = &'a SandboxMetadata>) -> OrchestratorMetrics {
        let mut metrics = OrchestratorMetrics::default();
        for metadata in metas {
            aggregate_resource_metrics(
                &mut metrics,
                SandboxContribution::new(metadata.state, metadata.resources),
            );
        }
        metrics
    }

    #[test]
    fn counters_accumulate_monotonically() {
        let counters = OrchestratorCounters::default();
        counters.record_create_success(1);
        counters.record_create_success(1);
        counters.record_create_fail(1);
        counters.record_create_fail(3);
        assert_eq!(
            (counters.create_successes(), counters.create_fails()),
            (2, 4)
        );
    }

    #[test]
    fn counters_accumulate_multiple_successes() {
        let counters = OrchestratorCounters::default();
        counters.record_create_success(3);
        counters.record_create_fail(1);
        assert_eq!(
            (counters.create_successes(), counters.create_fails()),
            (3, 1)
        );
    }

    #[test]
    fn aggregate_counts_running_and_starting() {
        let metas = [
            meta(SandboxState::Running, 2, 256),
            meta(SandboxState::Running, 1, 128),
            meta(SandboxState::Creating, 4, 512),
            meta(SandboxState::Creating, 1, 64),
        ];
        let metrics = aggregate(metas.iter());
        assert_eq!(metrics.running_sandbox_count, 2);
        assert_eq!(metrics.starting_sandbox_count, 2);
        assert_eq!(metrics.allocated_cpu, 2 + 1 + 4 + 1);
        assert_eq!(
            metrics.allocated_memory_bytes,
            u64::from(256u32 + 128 + 512 + 64) * 1024 * 1024
        );
    }

    #[test]
    fn aggregate_counts_every_transitional_state_out_of_running_as_running_with_resources() {
        let metas = [
            meta(SandboxState::Pausing, 2, 256),
            meta(SandboxState::Snapshotting, 1, 128),
            meta(SandboxState::Forking, 4, 512),
            meta(SandboxState::Killing, 8, 1024),
        ];
        let metrics = aggregate(metas.iter());
        assert_eq!(metrics.running_sandbox_count, 4);
        assert_eq!(metrics.starting_sandbox_count, 0);
        assert_eq!(metrics.allocated_cpu, 2 + 1 + 4 + 8);
        assert_eq!(
            metrics.allocated_memory_bytes,
            (256u64 + 128 + 512 + 1024) * 1024 * 1024
        );
    }

    #[test]
    fn aggregate_empty_produces_default() {
        let metrics = aggregate(std::iter::empty());
        assert_eq!(metrics.running_sandbox_count, 0);
        assert_eq!(metrics.starting_sandbox_count, 0);
        assert_eq!(metrics.allocated_cpu, 0);
        assert_eq!(metrics.allocated_memory_bytes, 0);
    }
}
