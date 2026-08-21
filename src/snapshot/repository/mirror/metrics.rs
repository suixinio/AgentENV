//! What the double-write phase is measured by.
//!
//! 🔴 None of these are the object-store request counter, and that is
//! deliberate. The acceptance number for the catalog migration is how many
//! object-storage requests one `GET /snapshots` costs, measured on
//! `agentenv_snapshot_object_store_requests_total{surface="catalog"}` and
//! compared against a baseline taken before any of this existed. Central
//! catalog traffic counted onto that series would move the number without the
//! read path changing, and the comparison the whole phase turns on would be
//! meaningless. Central traffic is counted here, on its own names.

/// One increment per catalog RPC this node issues.
pub(crate) const CENTRAL_REQUESTS_TOTAL: &str = "agentenv_snapshot_catalog_central_requests_total";

/// Writes the central catalog refused with a reason the caller acts on.
pub(crate) const CENTRAL_REFUSED_TOTAL: &str = "agentenv_snapshot_catalog_central_refused_total";

/// 🔴 Writes the central catalog would not take because its row is behind what
/// the object store's already is — the batch's one known, named gap. Separate
/// from `CENTRAL_REFUSED_TOTAL` because these are expected, and separate from
/// the mirror lag because the compensator cannot repair them: repairing would
/// need the build-admission transition this batch does not wire.
pub(crate) const CENTRAL_DIVERGED_TOTAL: &str = "agentenv_snapshot_catalog_central_diverged_total";

/// I3: the object-store mirror write that failed after the central write
/// succeeded. The operation still succeeded; this is what says so.
pub(crate) const MIRROR_FAILED_TOTAL: &str = "agentenv_snapshot_catalog_mirror_failed_total";

/// I4/I5: how many writes the object store still owes.
///
/// 🔴 A gauge over durable state, not a counter of events. It is read at
/// startup to decide whether the read side may be pointed back at the object
/// store, and a number that reset with the process would make that check pass
/// by forgetting.
pub(crate) const MIRROR_LAG: &str = "agentenv_snapshot_catalog_mirror_lag";

/// Backlog entries the compensator replayed successfully.
pub(crate) const MIRROR_REPAIRED_TOTAL: &str = "agentenv_snapshot_catalog_mirror_repaired_total";

/// Backlog entries a pass could not clear, by why.
pub(crate) const MIRROR_REPAIR_FAILED_TOTAL: &str =
    "agentenv_snapshot_catalog_mirror_repair_failed_total";

/// What one central call did.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CentralOutcome {
    /// The catalog took the write, or answered the read.
    Ok,
    /// The catalog answered, and the answer was a refusal.
    Refused,
    /// Nothing was learned: transport, deadline, or a response off contract.
    Error,
}

impl CentralOutcome {
    fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Refused => "refused",
            Self::Error => "error",
        }
    }
}

pub(crate) fn record_central_request(op: &'static str, outcome: CentralOutcome) {
    metrics::counter!(
        CENTRAL_REQUESTS_TOTAL,
        "op" => op,
        "outcome" => outcome.as_str(),
    )
    .increment(1);
}

pub(crate) fn record_central_refused(op: &'static str, reason: &'static str) {
    metrics::counter!(CENTRAL_REFUSED_TOTAL, "op" => op, "reason" => reason).increment(1);
}

pub(crate) fn record_central_diverged(op: &'static str, reason: &'static str) {
    metrics::counter!(CENTRAL_DIVERGED_TOTAL, "op" => op, "reason" => reason).increment(1);
}

pub(crate) fn record_mirror_failed(op: &'static str) {
    metrics::counter!(MIRROR_FAILED_TOTAL, "op" => op).increment(1);
}

pub(crate) fn record_mirror_repaired(op: &'static str) {
    metrics::counter!(MIRROR_REPAIRED_TOTAL, "op" => op).increment(1);
}

pub(crate) fn record_mirror_repair_failed(op: &'static str, verdict: &'static str) {
    metrics::counter!(MIRROR_REPAIR_FAILED_TOTAL, "op" => op, "verdict" => verdict).increment(1);
}

pub(crate) fn set_mirror_lag(lag: u64) {
    metrics::gauge!(MIRROR_LAG).set(lag as f64);
}
