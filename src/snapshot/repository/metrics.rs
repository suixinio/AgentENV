//! Object-storage request accounting shared by the snapshot repository
//! backends.
//!
//! The `surface` label existed to separate two bodies of data that shared one
//! bucket: rows under `catalog/`, and the snapshot bytes everything else. Only
//! the `surface="catalog"` series answered "how many object-storage requests
//! does listing snapshots cost", which is what the catalog migration was
//! measured against. That migration is finished — PostgreSQL is the catalog,
//! object storage holds no rows at all — so every request a backend issues
//! today is byte traffic and the label is emitted as the constant
//! `surface="artifact"`.
//!
//! 🔴 The label stays. `agentenv_snapshot_object_store_requests_total` is a
//! published series with three labels, and dropping one because it currently
//! has a single value silently rewrites every query and dashboard built on it
//! — a PromQL selector naming `surface` matches nothing at all against a series
//! that no longer carries it. See the test that pins the name and the three
//! keys.

/// One increment per request a repository backend issues against its durable
/// store.
///
/// Counts *backend operations*, not literal HTTP round-trips: opendal's retry
/// layer and multipart uploads can turn one increment into several requests on
/// the wire.
pub const OBJECT_STORE_REQUESTS_TOTAL: &str = "agentenv_snapshot_object_store_requests_total";

/// Publish rollbacks that deliberately left a snapshot's artifacts in place.
///
/// 🔴 A counter over a leak, and the leak is on purpose. A failed publish asks
/// both catalogs whether anything still points at the bytes and keeps them if
/// *either* says yes, because the alternative — deleting on a single "no" — is
/// what once destroyed the bytes of a sandbox a user had just paused. The trade
/// is right and it is not being revisited here; what was missing is that
/// nothing said when it fired. Measured on the cluster: one orphan prefix, two
/// objects, 22,194 bytes, and no way to tell from outside that it existed.
///
/// Nothing collects these. Every increment is bytes that will sit in the store
/// until somebody looks, so a rising number is the signal to go and look.
pub const ARTIFACTS_RETAINED_TOTAL: &str = "agentenv_snapshot_artifacts_retained_total";

pub fn record_artifacts_retained() {
    metrics::counter!(ARTIFACTS_RETAINED_TOTAL).increment(1);
}

/// Which body of data a request touched.
///
/// 🔴 One variant, on purpose. A `Catalog` variant sat beside it, chosen by a
/// `for_key` classifier that tested the key against a `catalog/` prefix; the
/// only key builders left are `oss::layout`'s `managed-layers/{digest}` and
/// `artifacts/{id}/…`, so every one of the eight call sites resolved to
/// `Artifact` and the classifier was a branch that could not be taken. Kept as
/// an enum rather than folded into the emission site because the label is part
/// of the published series either way, and a second body of data arriving is
/// then a variant and eight compiler errors instead of a silent mislabelling.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObjectStoreSurface {
    /// Snapshot artifacts and managed layers — the bytes.
    Artifact,
}

impl ObjectStoreSurface {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Artifact => "artifact",
        }
    }
}

/// The object-storage verb a backend operation maps to.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObjectStoreOp {
    Get,
    Put,
    Head,
    List,
    Delete,
    /// Composite: one LIST followed by one DELETE per listed key. Kept distinct
    /// from [`ObjectStoreOp::Delete`] because those constituent requests are
    /// each counted in their own right, so the leaf verbs still sum to the true
    /// request count and a reader can exclude this series.
    DeletePrefix,
}

impl ObjectStoreOp {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Get => "get",
            Self::Put => "put",
            Self::Head => "head",
            Self::List => "list",
            Self::Delete => "delete",
            Self::DeletePrefix => "delete_prefix",
        }
    }
}

/// What the store answered.
///
/// 🔴 `NotFound` exists because the absence of an object is a *successful*
/// answer from the store, and repository code asks that question constantly:
/// "is this alias already bound", "does this record exist yet", "has this
/// content-addressed layer already been uploaded". Folding those into
/// `outcome="error"` made a plain snapshot creation emit two catalog errors
/// (`load_alias_target` before the bind, plus the pre-bind or pre-commit
/// record read), so anyone alerting on the error series fired on entirely
/// healthy traffic. `Error` now means only what it says: the request did not
/// produce a usable answer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObjectStoreOutcome {
    /// The store returned the object, the listing, or the write acknowledgement.
    Ok,
    /// The request completed and the store answered "no such object". Callers
    /// routinely turn this into `Ok(None)` or `Ok(false)`.
    NotFound,
    /// The request produced no usable answer: transport, credentials, a
    /// server-side failure, or a response the caller could not use.
    Error,
}

impl ObjectStoreOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::NotFound => "not_found",
            Self::Error => "error",
        }
    }

    /// Classifies an operation that cannot miss: a write, a listing, or any
    /// composite whose constituent requests are counted in their own right.
    pub fn from_success(ok: bool) -> Self {
        if ok {
            Self::Ok
        } else {
            Self::Error
        }
    }

    /// Classifies a lookup: `true` means the object was there, `false` means
    /// the store said it was not.
    pub fn from_present(present: bool) -> Self {
        if present {
            Self::Ok
        } else {
            Self::NotFound
        }
    }
}

/// Records one object-storage request against the repository backend store.
pub fn record_object_store_request(
    op: ObjectStoreOp,
    surface: ObjectStoreSurface,
    outcome: ObjectStoreOutcome,
) {
    metrics::counter!(
        OBJECT_STORE_REQUESTS_TOTAL,
        "op" => op.as_str(),
        "surface" => surface.as_str(),
        "outcome" => outcome.as_str(),
    )
    .increment(1);
}

/// Test-only counter readouts, shared by the backend test modules so they all
/// assert against the same series naming.
///
/// 🔴 `object_store_requests` — the per-`{op,surface,outcome}` readout of
/// [`OBJECT_STORE_REQUESTS_TOTAL`] — lived here too, and its only callers were
/// the POSIX and OSS *catalog* test modules. Both stores are byte repositories
/// now and neither has a catalog test module, so the helper went with them.
#[cfg(test)]
pub mod test_support {
    use metrics_util::debugging::{DebugValue, Snapshotter};

    /// The total of every sample of one unlabelled counter.
    pub fn counter_total(snapshotter: &Snapshotter, name: &str) -> u64 {
        let mut total = 0u64;
        for (composite, _unit, _description, value) in snapshotter.snapshot().into_vec() {
            if composite.key().name() != name {
                continue;
            }
            if let DebugValue::Counter(count) = value {
                total += count;
            }
        }
        total
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics_util::debugging::DebuggingRecorder;

    /// The name and the full label set of the one sample
    /// [`record_object_store_request`] emits.
    fn emitted_series(
        op: ObjectStoreOp,
        surface: ObjectStoreSurface,
        outcome: ObjectStoreOutcome,
    ) -> (String, Vec<(String, String)>) {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let guard = metrics::set_default_local_recorder(&recorder);
        record_object_store_request(op, surface, outcome);
        drop(guard);

        let samples = snapshotter.snapshot().into_vec();
        assert_eq!(samples.len(), 1, "one request is one sample");
        let key = samples[0].0.key().clone();
        let mut labels: Vec<(String, String)> = key
            .labels()
            .map(|label| (label.key().to_owned(), label.value().to_owned()))
            .collect();
        labels.sort();
        (key.name().to_owned(), labels)
    }

    /// 🔴 The published contract of
    /// `agentenv_snapshot_object_store_requests_total`, pinned by value.
    ///
    /// The metric name and all three label keys are consumed from outside this
    /// repository, so renaming the series or dropping a label is a breaking
    /// change that compiles, passes every other test, and shows up as an empty
    /// graph. `surface` in particular now has one possible value — which is
    /// exactly the state in which somebody deletes it as redundant.
    #[test]
    fn the_object_store_counter_keeps_its_name_and_all_three_labels() {
        let (name, labels) = emitted_series(
            ObjectStoreOp::Get,
            ObjectStoreSurface::Artifact,
            ObjectStoreOutcome::Ok,
        );

        assert_eq!(name, "agentenv_snapshot_object_store_requests_total");
        assert_eq!(name, OBJECT_STORE_REQUESTS_TOTAL);
        assert_eq!(
            labels,
            vec![
                ("op".to_owned(), "get".to_owned()),
                ("outcome".to_owned(), "ok".to_owned()),
                ("surface".to_owned(), "artifact".to_owned()),
            ],
            "the series carries op/surface/outcome and nothing else"
        );
    }

    /// The other label values a reader can select on, so a renamed variant
    /// string is caught here rather than in a dashboard.
    #[test]
    fn every_op_and_outcome_keeps_its_label_value() {
        for (op, expected) in [
            (ObjectStoreOp::Get, "get"),
            (ObjectStoreOp::Put, "put"),
            (ObjectStoreOp::Head, "head"),
            (ObjectStoreOp::List, "list"),
            (ObjectStoreOp::Delete, "delete"),
            (ObjectStoreOp::DeletePrefix, "delete_prefix"),
        ] {
            let (_, labels) =
                emitted_series(op, ObjectStoreSurface::Artifact, ObjectStoreOutcome::Ok);
            assert!(
                labels.contains(&("op".to_owned(), expected.to_owned())),
                "{op:?} must be labelled '{expected}', got {labels:?}"
            );
        }

        for (outcome, expected) in [
            (ObjectStoreOutcome::Ok, "ok"),
            (ObjectStoreOutcome::NotFound, "not_found"),
            (ObjectStoreOutcome::Error, "error"),
        ] {
            let (_, labels) =
                emitted_series(ObjectStoreOp::Get, ObjectStoreSurface::Artifact, outcome);
            assert!(
                labels.contains(&("outcome".to_owned(), expected.to_owned())),
                "{outcome:?} must be labelled '{expected}', got {labels:?}"
            );
        }
    }
}
