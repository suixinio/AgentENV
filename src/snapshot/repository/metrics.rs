//! Object-storage request accounting shared by snapshot repositories.
//!
//! The published `surface` label remains even while every request is artifact traffic.

/// Counts backend operations against durable storage.
///
/// Retries and multipart uploads may produce multiple HTTP requests per increment.
pub const OBJECT_STORE_REQUESTS_TOTAL: &str = "agentenv_snapshot_object_store_requests_total";

/// Counts publish rollbacks that deliberately retained artifacts.
///
/// Retained bytes have no collector, so a rising value requires operator cleanup.
pub const ARTIFACTS_RETAINED_TOTAL: &str = "agentenv_snapshot_artifacts_retained_total";

pub fn record_artifacts_retained() {
    metrics::counter!(ARTIFACTS_RETAINED_TOTAL).increment(1);
}

/// Published label for the body of data touched by a request.
///
/// Only artifact bytes currently use object storage.
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
    /// Composite LIST plus per-key DELETEs, distinct from the counted leaf operations.
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

/// Classifies successful answers, including object absence, separately from failures.
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

/// Test-only counter readouts shared by backend tests.
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
