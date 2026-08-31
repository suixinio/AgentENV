//! PostgreSQL catalog metrics retaining the existing `agentenv_scheduler_*`
//! series names for dashboard compatibility.

/// Catalog calls by operation and outcome label.
pub const CATALOG_RPC_TOTAL: &str = "agentenv_scheduler_catalog_rpc_total";

/// Catalog refusals by operation and rejection reason.
pub const CATALOG_REJECTED_TOTAL: &str = "agentenv_scheduler_catalog_rejected_total";

/// Builds ended by the reaper.
pub const CATALOG_BUILDS_REAPED_TOTAL: &str = "agentenv_scheduler_catalog_builds_reaped_total";

/// Reaper passes withheld during warm-up.
pub const CATALOG_BUILD_REAPER_WARMUP_PASSES_TOTAL: &str =
    "agentenv_scheduler_catalog_build_reaper_warmup_passes_total";

/// Reserved clock-skew series; the direct catalog has no asserted builder clock.
#[allow(dead_code)]
pub const CATALOG_BUILD_CLOCK_SKEW_TOTAL: &str =
    "agentenv_scheduler_catalog_build_clock_skew_total";

pub fn record_catalog_rpc(op: &'static str, code: &str) {
    metrics::counter!(CATALOG_RPC_TOTAL, "rpc" => op, "code" => code.to_string()).increment(1);
}

pub fn record_catalog_rejected(op: &'static str, reason: &'static str) {
    metrics::counter!(CATALOG_REJECTED_TOTAL, "rpc" => op, "reason" => reason).increment(1);
}

/// Records call and rejection metrics, returning `result` unchanged.
pub fn record_catalog_outcome<T>(
    op: &'static str,
    result: crate::snapshot::repository::RepositoryResult<T>,
) -> crate::snapshot::repository::RepositoryResult<T> {
    match &result {
        Ok(_) => record_catalog_rpc(op, "ok"),
        Err(err) => {
            let label = err.as_metric_label();
            record_catalog_rpc(op, label);
            if err.is_rejection() {
                record_catalog_rejected(op, label);
            }
        }
    }
    result
}

pub fn record_builds_reaped(count: u64) {
    metrics::counter!(CATALOG_BUILDS_REAPED_TOTAL).increment(count);
}

pub fn record_build_reaper_warmup_pass() {
    metrics::counter!(CATALOG_BUILD_REAPER_WARMUP_PASSES_TOTAL).increment(1);
}
