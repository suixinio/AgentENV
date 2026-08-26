//! The five Prometheus series `catalog_service.go` published, rebuilt here so
//! folding the catalog into `--role api` does not also make the build queue
//! and the RPC surface silently unobservable.
//!
//! Names are kept identical to the Go originals — `agentenv_scheduler_*` — on
//! the theory that an unchanged name is one less thing for an existing
//! dashboard or alert to be updated for; renaming is a later, deliberate
//! decision for whoever owns those dashboards; this module does not make it.
//!
//! Uses the `metrics` crate's macros, the same convention
//! `crate::snapshot::repository::mirror::metrics` already established for
//! this same double-write phase's own series, rather than Go's
//! `promauto`/`prometheus` client directly.

/// Every catalog RPC-equivalent call, by operation and outcome.
///
/// 🔴 `code`, not a coarser "ok/error": mirrors Go's gRPC status code label
/// exactly enough to keep an existing dashboard's grouping meaningful. Since
/// there is no gRPC status on this side of Stage B, `code` carries `"ok"` or
/// [`crate::snapshot::repository::RepositoryError::as_metric_label`] — the
/// variant name for whichever error this build actually has to report.
///
/// Wired at [`super::PostgresSnapshotCatalog`]'s `SnapshotCatalog` and
/// `CentralCatalogWrites` trait method bodies, each through
/// [`record_catalog_outcome`] — one wrapper around every write-path and
/// read-path call, rather than duplicating this match inside each of
/// `postgres::writes`'s dozen functions.
pub(crate) const CATALOG_RPC_TOTAL: &str = "agentenv_scheduler_catalog_rpc_total";

/// Refusals answered as a decision the caller acts on rather than as a
/// failure — matches Go's `catalogRejections`.
/// [`crate::snapshot::repository::RepositoryError::is_rejection`] decides
/// which of [`CATALOG_RPC_TOTAL`]'s errors also count here; see
/// [`record_catalog_outcome`].
pub(crate) const CATALOG_REJECTED_TOTAL: &str = "agentenv_scheduler_catalog_rejected_total";

/// Builds the reaper ended because their heartbeat lapsed.
pub(crate) const CATALOG_BUILDS_REAPED_TOTAL: &str =
    "agentenv_scheduler_catalog_builds_reaped_total";

/// Reaping passes held back by the reaper's own warm-up window.
pub(crate) const CATALOG_BUILD_REAPER_WARMUP_PASSES_TOTAL: &str =
    "agentenv_scheduler_catalog_build_reaper_warmup_passes_total";

/// Build heartbeats carrying a caller-asserted clock far from this replica's
/// own.
///
/// 🔴 Not wired to anything yet. Go's version compares the *node's* asserted
/// clock (`RenewBuildLeaseRequest.heartbeat_at_unix_ms`, carried over gRPC
/// purely for this comparison) against the controller's — a signal that only
/// exists because two separate machines and a wire hop sit between "the
/// builder says it is alive" and "the process judging that claim". Stage B's
/// direct-PG catalog removes that hop for the admitting `--role api`
/// replica: `renew_build_lease` never carries a caller-asserted timestamp in
/// the `SnapshotCatalog` trait today (see
/// `src/snapshot/repository/interfaces.rs`), and the heartbeat this schema
/// stores is always the database's own `clock_timestamp()` regardless (see
/// `queries_admin.go`'s `nowMs` note, ported verbatim in `reaper.rs`). Wiring
/// this metric to something real would mean either adding an asserted-clock
/// field to `node_client/build.rs`'s dispatch protocol (comparing the
/// *executing* node's clock against the admitting replica's, which is a
/// different comparison from Go's and a change outside this catalog port's
/// scope) or leaving it comparing nothing. Left declared and unused rather
/// than silently dropped, so the gap is visible to whoever wires build
/// dispatch's clock reporting next, instead of being rediscovered from a
/// blank dashboard panel.
#[allow(dead_code)]
pub(crate) const CATALOG_BUILD_CLOCK_SKEW_TOTAL: &str =
    "agentenv_scheduler_catalog_build_clock_skew_total";

pub(crate) fn record_catalog_rpc(op: &'static str, code: &str) {
    metrics::counter!(CATALOG_RPC_TOTAL, "rpc" => op, "code" => code.to_string()).increment(1);
}

pub(crate) fn record_catalog_rejected(op: &'static str, reason: &'static str) {
    metrics::counter!(CATALOG_REJECTED_TOTAL, "rpc" => op, "reason" => reason).increment(1);
}

/// Records one call's outcome on both series in one place — `op` on
/// [`CATALOG_RPC_TOTAL`] always, and again on [`CATALOG_REJECTED_TOTAL`] when
/// the error is a refusal rather than a failure
/// ([`crate::snapshot::repository::RepositoryError::is_rejection`]). Returns
/// `result` unchanged so a call site can wrap its own return expression with
/// this rather than pre-binding a local.
pub(crate) fn record_catalog_outcome<T>(
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

pub(crate) fn record_builds_reaped(count: u64) {
    metrics::counter!(CATALOG_BUILDS_REAPED_TOTAL).increment(count);
}

pub(crate) fn record_build_reaper_warmup_pass() {
    metrics::counter!(CATALOG_BUILD_REAPER_WARMUP_PASSES_TOTAL).increment(1);
}
