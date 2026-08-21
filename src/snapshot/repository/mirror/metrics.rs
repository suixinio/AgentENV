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

use super::backlog::MirrorDirection;

/// One increment per catalog RPC this node issues.
pub(crate) const CENTRAL_REQUESTS_TOTAL: &str = "agentenv_snapshot_catalog_central_requests_total";

/// Writes the central catalog refused with a reason the caller acts on.
pub(crate) const CENTRAL_REFUSED_TOTAL: &str = "agentenv_snapshot_catalog_central_refused_total";

/// 🔴 Writes the central catalog would not take because its row is behind what
/// the object store's already is — the batch's one known, named gap. Separate
/// from `CENTRAL_REFUSED_TOTAL` because these are expected, and separate from
/// the mirror lag because the compensator cannot repair them: repairing would
/// need the build-admission transition this batch does not wire.
///
/// This counts the *events*. [`MIRROR_DIVERGED`] is the standing number of
/// snapshots still in that state, which is the one a switch can be judged on.
pub(crate) const CENTRAL_DIVERGED_TOTAL: &str = "agentenv_snapshot_catalog_central_diverged_total";

/// I3: a mirror write that failed after the other store took it. The operation
/// still succeeded; this is what says so.
pub(crate) const MIRROR_FAILED_TOTAL: &str = "agentenv_snapshot_catalog_mirror_failed_total";

/// I4/I5: how many writes the store on `direction` still owes.
///
/// 🔴 A gauge over durable state, not a counter of events. It is read at
/// startup to decide whether the read side may be moved, and a number that
/// reset with the process would make that check pass by forgetting.
///
/// 🔴 Labelled by direction, and the label is load-bearing. The two switches
/// this number guards care about two different debts: moving reads back to
/// object storage is only lossless while object storage owes nothing, and
/// moving them to PostgreSQL is only lossless while the central catalog owes
/// nothing. A single unlabelled series would answer neither question. Summing
/// the label back up is safe — it is the more conservative reading, never the
/// less.
pub(crate) const MIRROR_LAG: &str = "agentenv_snapshot_catalog_mirror_lag";

/// 🔴 Snapshots the two catalogs disagree about that no replay can settle.
///
/// A gauge over durable state, like the lag, and deliberately *not* part of it:
/// the lag is debt the compensator will pay, and this is disagreement it
/// cannot. Zero lag has never meant the catalogs agree — `try_start_build`
/// writes object storage alone in this batch, so every template's central row
/// stays `waiting` forever with nothing owed — and a switch authorised on the
/// lag alone would move reads onto a store missing exactly those rows. This is
/// the number that makes "they agree" answerable, and the read-side guard reads
/// it alongside the lag.
pub(crate) const MIRROR_DIVERGED: &str = "agentenv_snapshot_catalog_mirror_diverged";

/// Owed writes this process could not write down.
///
/// Counted into [`MIRROR_LAG`] as well, because the write is owed either way.
/// Separate because these are the only entries a restart forgets, so a non-zero
/// value here is the one case where the lag can come back smaller than it was.
pub(crate) const MIRROR_UNRECORDED_TOTAL: &str =
    "agentenv_snapshot_catalog_mirror_unrecorded_total";

/// Backlog entries the compensator replayed successfully.
pub(crate) const MIRROR_REPAIRED_TOTAL: &str = "agentenv_snapshot_catalog_mirror_repaired_total";

/// Backlog entries a pass could not clear, by why.
pub(crate) const MIRROR_REPAIR_FAILED_TOTAL: &str =
    "agentenv_snapshot_catalog_mirror_repair_failed_total";

/// 🔴 Entries the target *took* and the two catalogs still disagreed about.
///
/// The number that says [`MIRROR_LAG`] is now the stronger claim it always
/// read as. Before it existed, "repaired" meant only that the store accepted
/// the write — which is how a backfill replayed thirty-two publishes into rows
/// whose creation times were all the moment the backfill ran, with every gauge
/// reporting agreement. Labelled by the field that differed, so an operator
/// sees *what* disagrees rather than only that something does.
///
/// A non-zero value here with a non-zero lag is the honest state: the write
/// landed, the rows do not match, and the entry is still counted as debt.
pub(crate) const MIRROR_CONTENT_MISMATCH_TOTAL: &str =
    "agentenv_snapshot_catalog_mirror_content_mismatch_total";

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

pub(crate) fn record_mirror_failed(direction: MirrorDirection, op: &'static str) {
    metrics::counter!(
        MIRROR_FAILED_TOTAL,
        "direction" => direction.as_str(),
        "op" => op,
    )
    .increment(1);
}

pub(crate) fn record_mirror_repaired(direction: MirrorDirection, op: &'static str) {
    metrics::counter!(
        MIRROR_REPAIRED_TOTAL,
        "direction" => direction.as_str(),
        "op" => op,
    )
    .increment(1);
}

pub(crate) fn record_mirror_repair_failed(
    direction: MirrorDirection,
    op: &'static str,
    verdict: &'static str,
) {
    metrics::counter!(
        MIRROR_REPAIR_FAILED_TOTAL,
        "direction" => direction.as_str(),
        "op" => op,
        "verdict" => verdict,
    )
    .increment(1);
}

pub(crate) fn record_mirror_content_mismatch(direction: MirrorDirection, field: &'static str) {
    metrics::counter!(
        MIRROR_CONTENT_MISMATCH_TOTAL,
        "direction" => direction.as_str(),
        "field" => field,
    )
    .increment(1);
}

pub(crate) fn record_mirror_unrecorded(direction: MirrorDirection, op: &'static str) {
    metrics::counter!(
        MIRROR_UNRECORDED_TOTAL,
        "direction" => direction.as_str(),
        "op" => op,
    )
    .increment(1);
}

pub(crate) fn set_mirror_lag(direction: MirrorDirection, lag: u64) {
    metrics::gauge!(MIRROR_LAG, "direction" => direction.as_str()).set(lag as f64);
}

pub(crate) fn set_mirror_diverged(direction: MirrorDirection, diverged: u64) {
    metrics::gauge!(MIRROR_DIVERGED, "direction" => direction.as_str()).set(diverged as f64);
}
