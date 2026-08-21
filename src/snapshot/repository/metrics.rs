//! Object-storage request accounting shared by the snapshot repository
//! backends.
//!
//! The `surface` label is the reason this counter exists. Object storage keeps
//! carrying every snapshot *byte* even once the catalog moves out of it, so
//! `op="put"` stays permanently high and would drown out the thing we care
//! about. Only the `surface="catalog"` series answers "how many object-storage
//! requests does listing snapshots cost", which is what the catalog migration
//! is measured against.

/// Repository-relative key prefix that holds catalog rows: snapshot records
/// under `catalog/records/` and alias bindings under `catalog/aliases/`.
const CATALOG_PREFIX: &str = "catalog/";

/// One increment per request a repository backend issues against its durable
/// store.
///
/// Counts *backend operations*, not literal HTTP round-trips: opendal's retry
/// layer and multipart uploads can turn one increment into several requests on
/// the wire. Catalog objects are small single-shot JSON blobs, so on the
/// catalog surface the two coincide unless a request is retried.
pub(crate) const OBJECT_STORE_REQUESTS_TOTAL: &str =
    "agentenv_snapshot_object_store_requests_total";

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
pub(crate) const ARTIFACTS_RETAINED_TOTAL: &str = "agentenv_snapshot_artifacts_retained_total";

pub(crate) fn record_artifacts_retained() {
    metrics::counter!(ARTIFACTS_RETAINED_TOTAL).increment(1);
}

/// Which body of data a request touched.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ObjectStoreSurface {
    /// Snapshot records and alias bindings — the rows.
    Catalog,
    /// Snapshot artifacts and managed layers — the bytes.
    Artifact,
}

impl ObjectStoreSurface {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Catalog => "catalog",
            Self::Artifact => "artifact",
        }
    }

    /// Classifies a repository-relative key (the backend prefix is already
    /// stripped by the time a key reaches here).
    ///
    /// Deriving the surface from the key instead of threading it through every
    /// call site keeps composite callers labelled correctly without each of
    /// them having to remember to say so: `list()`'s LIST plus one GET per
    /// record, and `bind_alias()`'s read / write / read-back / delete, all land
    /// on `catalog/` keys and are counted as catalog traffic for free.
    pub(crate) fn for_key(key: &str) -> Self {
        let key = key.trim_start_matches('/');
        if key.starts_with(CATALOG_PREFIX) || key == CATALOG_PREFIX.trim_end_matches('/') {
            Self::Catalog
        } else {
            Self::Artifact
        }
    }
}

/// The object-storage verb a backend operation maps to.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ObjectStoreOp {
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
    pub(crate) fn as_str(self) -> &'static str {
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
pub(crate) enum ObjectStoreOutcome {
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
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::NotFound => "not_found",
            Self::Error => "error",
        }
    }

    /// Classifies an operation that cannot miss: a write, a listing, or any
    /// composite whose constituent requests are counted in their own right.
    pub(crate) fn from_success(ok: bool) -> Self {
        if ok {
            Self::Ok
        } else {
            Self::Error
        }
    }

    /// Classifies a lookup: `true` means the object was there, `false` means
    /// the store said it was not.
    pub(crate) fn from_present(present: bool) -> Self {
        if present {
            Self::Ok
        } else {
            Self::NotFound
        }
    }
}

/// Records one object-storage request against the repository backend store.
pub(crate) fn record_object_store_request(
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

/// Test-only readout of [`OBJECT_STORE_REQUESTS_TOTAL`], shared by the backend
/// test modules so they all assert against the same series naming.
#[cfg(test)]
pub(crate) mod test_support {
    use std::collections::BTreeMap;

    use metrics_util::debugging::{DebugValue, Snapshotter};

    /// The total of every sample of one unlabelled counter.
    pub(crate) fn counter_total(snapshotter: &Snapshotter, name: &str) -> u64 {
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

    /// Collects every [`super::OBJECT_STORE_REQUESTS_TOTAL`] sample seen by
    /// `snapshotter`, keyed by `"{op}/{surface}/{outcome}"`. Series that were
    /// never touched are absent rather than zero, so a missing key and a zero
    /// count are distinguishable.
    pub(crate) fn object_store_requests(snapshotter: &Snapshotter) -> BTreeMap<String, u64> {
        let mut counters: BTreeMap<String, u64> = BTreeMap::new();
        for (composite, _unit, _description, value) in snapshotter.snapshot().into_vec() {
            let key = composite.key();
            if key.name() != super::OBJECT_STORE_REQUESTS_TOTAL {
                continue;
            }
            let labels: BTreeMap<String, String> = key
                .labels()
                .map(|label| (label.key().to_owned(), label.value().to_owned()))
                .collect();
            let field = |name: &str| {
                labels
                    .get(name)
                    .cloned()
                    .unwrap_or_else(|| "<missing>".to_owned())
            };
            let series = format!("{}/{}/{}", field("op"), field("surface"), field("outcome"));
            if let DebugValue::Counter(count) = value {
                *counters.entry(series).or_default() += count;
            }
        }
        counters
    }
}

#[cfg(test)]
mod tests {
    use super::ObjectStoreSurface;

    #[test]
    fn catalog_keys_are_separated_from_byte_keys() {
        for catalog_key in [
            "catalog/records/0198f0a1-0000-7000-8000-000000000000.json",
            "catalog/aliases/my-template.json",
            "catalog/records/",
            "catalog/",
            "catalog",
        ] {
            assert_eq!(
                ObjectStoreSurface::for_key(catalog_key),
                ObjectStoreSurface::Catalog,
                "'{catalog_key}' should be catalog traffic"
            );
        }

        // Everything that is not a catalog row is bytes: per-snapshot
        // artifacts, managed overlaybd layers, and anything added later.
        for artifact_key in [
            "artifacts/0198f0a1-0000-7000-8000-000000000000/vm_state.bin",
            "artifacts/0198f0a1-0000-7000-8000-000000000000/",
            "managed-layers/sha256:deadbeef",
            "catalogue/records/not-a-catalog-key.json",
            "",
        ] {
            assert_eq!(
                ObjectStoreSurface::for_key(artifact_key),
                ObjectStoreSurface::Artifact,
                "'{artifact_key}' should be byte traffic"
            );
        }
    }
}
