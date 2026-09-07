//! Deletes sandboxes a node reports that nothing in the control plane names.
//!
//! The evidence is the heartbeat roster, which is the only place a node's own
//! account of what it runs meets the api half's records. A launch in flight
//! looks the same for as long as it takes the launch to write its record, so a
//! candidate has to survive consecutive heartbeats and a grace period before it
//! is reaped.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;

use super::types::{Node, RosterEntry};
use crate::types::{ExecutionId, SandboxId};

/// A candidate has to be reported this long before it is deleted.
///
/// At the 5 s heartbeat cadence this is several heartbeats, which is more than
/// a launch needs between the node's create returning and the api half writing
/// the record that answers for it.
pub const ORPHAN_REAP_GRACE: Duration = Duration::from_secs(20);

/// Consecutive heartbeats naming a candidate before it is deleted. One sighting
/// is a snapshot; two are a node that still says the same thing.
const ORPHAN_REAP_SIGHTINGS: u32 = 2;

const REAPED_METRIC: &str = "agentenv_api_orphan_sandboxes_reaped_total";

/// The executions the control plane records for a set of sandbox ids.
#[async_trait]
pub trait SandboxRecordIndex: Send + Sync + 'static {
    /// An id absent from the map has no record. `Err` is unknown, and a read
    /// that could not cover every id asked about must be one.
    async fn recorded_executions(
        &self,
        sandbox_ids: &[SandboxId],
    ) -> anyhow::Result<HashMap<SandboxId, ExecutionId>>;
}

/// Deletes one sandbox on the node that reported it.
#[async_trait]
pub trait NodeSandboxDeleter: Send + Sync + 'static {
    /// Fenced on `execution_id`: a node running a newer incarnation refuses it.
    async fn delete(
        &self,
        node: &Node,
        sandbox_id: SandboxId,
        execution_id: ExecutionId,
    ) -> anyhow::Result<()>;
}

/// The metadata store as the answer to what the control plane records.
pub struct StoreRecordIndex<S>(S);

impl<S: crate::orchestrator::store::MetadataStore + 'static> StoreRecordIndex<S> {
    pub fn new(store: S) -> Self {
        Self(store)
    }
}

#[async_trait]
impl<S: crate::orchestrator::store::MetadataStore + 'static> SandboxRecordIndex
    for StoreRecordIndex<S>
{
    async fn recorded_executions(
        &self,
        sandbox_ids: &[SandboxId],
    ) -> anyhow::Result<HashMap<SandboxId, ExecutionId>> {
        let rows = self.0.get_many(sandbox_ids).await?;
        // A partial read cannot say that the ids it missed have no record.
        if !rows.covers(sandbox_ids) {
            anyhow::bail!(
                "read the records for {} sandboxes and got {}",
                sandbox_ids.len(),
                rows.covered.len()
            );
        }
        Ok(rows
            .entries
            .into_iter()
            .map(|(id, metadata)| (id, metadata.execution_id))
            .collect())
    }
}

#[derive(Clone, Copy)]
struct Sighting {
    first_seen: SystemTime,
    count: u32,
}

/// Reaps sandboxes the heartbeat roster names and the metadata store does not.
pub struct OrphanReaper {
    records: Box<dyn SandboxRecordIndex>,
    deleter: Box<dyn NodeSandboxDeleter>,
    grace: Duration,
    /// First sighting per (node, sandbox, execution). A candidate that stops
    /// being one drops out and starts its grace over if it comes back.
    seen: Mutex<HashMap<(String, SandboxId, ExecutionId), Sighting>>,
}

impl OrphanReaper {
    pub fn new(records: Box<dyn SandboxRecordIndex>, deleter: Box<dyn NodeSandboxDeleter>) -> Self {
        Self::with_grace(records, deleter, ORPHAN_REAP_GRACE)
    }

    pub fn with_grace(
        records: Box<dyn SandboxRecordIndex>,
        deleter: Box<dyn NodeSandboxDeleter>,
        grace: Duration,
    ) -> Self {
        Self {
            records,
            deleter,
            grace,
            seen: Mutex::new(HashMap::new()),
        }
    }

    /// Compares one heartbeat's roster against the control plane's records and
    /// deletes what has been an orphan for long enough.
    ///
    /// Best-effort throughout: a store that cannot answer, or a node that
    /// refuses the delete, leaves everything where it is.
    pub async fn observe(&self, node: &Node, roster: &[RosterEntry], now: SystemTime) {
        let reported: Vec<(SandboxId, ExecutionId)> = roster
            .iter()
            .filter_map(|entry| {
                // An entry with no readable incarnation cannot be fenced, and a
                // delete that names no run could take one this reaper has never
                // seen.
                Some((
                    SandboxId::parse_str(&entry.sandbox_id).ok()?,
                    ExecutionId::parse_str(&entry.execution_id).ok()?,
                ))
            })
            .collect();

        let ids: Vec<SandboxId> = reported.iter().map(|(id, _)| *id).collect();
        let recorded = match self.records.recorded_executions(&ids).await {
            Ok(recorded) => recorded,
            Err(error) => {
                tracing::warn!(
                    node_id = %node.id,
                    error = %format_args!("{error:#}"),
                    "could not read the records for this node's roster; reaping nothing this \
                     heartbeat"
                );
                return;
            }
        };

        let candidates: Vec<(SandboxId, ExecutionId)> = reported
            .into_iter()
            .filter(|(sandbox_id, execution_id)| {
                recorded
                    .get(sandbox_id)
                    .is_none_or(|recorded| recorded != execution_id)
            })
            .collect();

        for (sandbox_id, execution_id) in self.due(&node.id, &candidates, now) {
            tracing::warn!(
                node_id = %node.id,
                %sandbox_id,
                %execution_id,
                "reaping a sandbox the node reports that nothing in the control plane names"
            );
            match self.deleter.delete(node, sandbox_id, execution_id).await {
                Ok(()) => {
                    metrics::counter!(REAPED_METRIC, "outcome" => "reaped").increment(1);
                    self.forget(&node.id, sandbox_id, execution_id);
                }
                Err(error) => {
                    metrics::counter!(REAPED_METRIC, "outcome" => "failed").increment(1);
                    tracing::warn!(
                        node_id = %node.id,
                        %sandbox_id,
                        %execution_id,
                        error = %format_args!("{error:#}"),
                        "the node refused to delete a sandbox nothing names; leaving it for the \
                         next heartbeat"
                    );
                }
            }
        }
    }

    /// Records this heartbeat's candidates and returns the ones old enough and
    /// seen often enough to delete.
    fn due(
        &self,
        node_id: &str,
        candidates: &[(SandboxId, ExecutionId)],
        now: SystemTime,
    ) -> Vec<(SandboxId, ExecutionId)> {
        let mut seen = self.seen.lock().expect("orphan sightings mutex poisoned");
        // A sandbox this node no longer reports as a candidate starts over.
        seen.retain(|(seen_node, sandbox_id, execution_id), _| {
            seen_node != node_id || candidates.contains(&(*sandbox_id, *execution_id))
        });

        let mut due = Vec::new();
        for (sandbox_id, execution_id) in candidates {
            let sighting = seen
                .entry((node_id.to_string(), *sandbox_id, *execution_id))
                .and_modify(|sighting| sighting.count = sighting.count.saturating_add(1))
                .or_insert(Sighting {
                    first_seen: now,
                    count: 1,
                });
            let waited = now
                .duration_since(sighting.first_seen)
                .unwrap_or(Duration::ZERO);
            if sighting.count >= ORPHAN_REAP_SIGHTINGS && waited >= self.grace {
                due.push((*sandbox_id, *execution_id));
            }
        }
        due
    }

    fn forget(&self, node_id: &str, sandbox_id: SandboxId, execution_id: ExecutionId) {
        self.seen
            .lock()
            .expect("orphan sightings mutex poisoned")
            .remove(&(node_id.to_string(), sandbox_id, execution_id));
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[derive(Default)]
    struct FakeRecords {
        recorded: HashMap<SandboxId, ExecutionId>,
        unreadable: bool,
    }

    #[async_trait]
    impl SandboxRecordIndex for Arc<FakeRecords> {
        async fn recorded_executions(
            &self,
            sandbox_ids: &[SandboxId],
        ) -> anyhow::Result<HashMap<SandboxId, ExecutionId>> {
            if self.unreadable {
                anyhow::bail!("the metadata store is unreachable");
            }
            Ok(sandbox_ids
                .iter()
                .filter_map(|id| self.recorded.get(id).map(|execution| (*id, *execution)))
                .collect())
        }
    }

    #[derive(Default)]
    struct FakeDeleter {
        deleted: Mutex<Vec<(String, SandboxId, ExecutionId)>>,
    }

    impl FakeDeleter {
        fn deleted(&self) -> Vec<(String, SandboxId, ExecutionId)> {
            self.deleted.lock().expect("deleted mutex").clone()
        }
    }

    #[async_trait]
    impl NodeSandboxDeleter for Arc<FakeDeleter> {
        async fn delete(
            &self,
            node: &Node,
            sandbox_id: SandboxId,
            execution_id: ExecutionId,
        ) -> anyhow::Result<()> {
            self.deleted.lock().expect("deleted mutex").push((
                node.id.clone(),
                sandbox_id,
                execution_id,
            ));
            Ok(())
        }
    }

    fn node() -> Node {
        Node {
            id: "node-a".to_string(),
            endpoint: "http://10.0.0.7:8000".to_string(),
            pod_name: "node-a-0".to_string(),
        }
    }

    fn entry(sandbox_id: SandboxId, execution_id: ExecutionId) -> RosterEntry {
        RosterEntry {
            sandbox_id: sandbox_id.to_string(),
            execution_id: execution_id.to_string(),
            projection_ttl: Duration::ZERO,
        }
    }

    fn reaper(
        records: Arc<FakeRecords>,
        deleter: Arc<FakeDeleter>,
        grace: Duration,
    ) -> OrphanReaper {
        OrphanReaper::with_grace(Box::new(records), Box::new(deleter), grace)
    }

    const GRACE: Duration = Duration::from_secs(20);

    #[tokio::test]
    async fn one_sighting_reaps_nothing_however_old_the_clock_says_it_is() {
        let deleter = Arc::new(FakeDeleter::default());
        let reaper = reaper(
            Arc::new(FakeRecords::default()),
            Arc::clone(&deleter),
            GRACE,
        );
        let roster = vec![entry(SandboxId::new(), ExecutionId::new())];

        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
        reaper.observe(&node(), &roster, now).await;

        assert!(
            deleter.deleted().is_empty(),
            "a single heartbeat is a snapshot, not an orphan"
        );
    }

    #[tokio::test]
    async fn two_sightings_inside_the_grace_reap_nothing() {
        let deleter = Arc::new(FakeDeleter::default());
        let reaper = reaper(
            Arc::new(FakeRecords::default()),
            Arc::clone(&deleter),
            GRACE,
        );
        let roster = vec![entry(SandboxId::new(), ExecutionId::new())];

        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
        reaper.observe(&node(), &roster, now).await;
        reaper
            .observe(&node(), &roster, now + Duration::from_secs(5))
            .await;

        assert!(
            deleter.deleted().is_empty(),
            "a launch that has not written its record yet looks exactly like this"
        );
    }

    #[tokio::test]
    async fn a_candidate_still_there_past_the_grace_is_deleted_under_the_run_the_node_reports() {
        let deleter = Arc::new(FakeDeleter::default());
        let reaper = reaper(
            Arc::new(FakeRecords::default()),
            Arc::clone(&deleter),
            GRACE,
        );
        let sandbox_id = SandboxId::new();
        let execution_id = ExecutionId::new();
        let roster = vec![entry(sandbox_id, execution_id)];

        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
        reaper.observe(&node(), &roster, now).await;
        reaper
            .observe(&node(), &roster, now + GRACE + Duration::from_secs(1))
            .await;

        assert_eq!(
            deleter.deleted(),
            vec![("node-a".to_string(), sandbox_id, execution_id)],
            "the delete has to name the node that reported it and the run it reported"
        );
    }

    #[tokio::test]
    async fn a_sandbox_whose_record_names_the_same_run_is_never_a_candidate() {
        let sandbox_id = SandboxId::new();
        let execution_id = ExecutionId::new();
        let deleter = Arc::new(FakeDeleter::default());
        let reaper = reaper(
            Arc::new(FakeRecords {
                recorded: [(sandbox_id, execution_id)].into_iter().collect(),
                unreadable: false,
            }),
            Arc::clone(&deleter),
            GRACE,
        );
        let roster = vec![entry(sandbox_id, execution_id)];

        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
        for offset in [0, 5, 60, 600] {
            reaper
                .observe(&node(), &roster, now + Duration::from_secs(offset))
                .await;
        }

        assert!(
            deleter.deleted().is_empty(),
            "a sandbox the control plane records is not an orphan at any age"
        );
    }

    #[tokio::test]
    async fn a_record_naming_another_run_makes_the_reported_one_an_orphan() {
        let sandbox_id = SandboxId::new();
        let deleter = Arc::new(FakeDeleter::default());
        let reaper = reaper(
            Arc::new(FakeRecords {
                recorded: [(sandbox_id, ExecutionId::new())].into_iter().collect(),
                unreadable: false,
            }),
            Arc::clone(&deleter),
            GRACE,
        );
        let stale_run = ExecutionId::new();
        let roster = vec![entry(sandbox_id, stale_run)];

        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
        reaper.observe(&node(), &roster, now).await;
        reaper
            .observe(&node(), &roster, now + GRACE + Duration::from_secs(1))
            .await;

        assert_eq!(
            deleter.deleted(),
            vec![("node-a".to_string(), sandbox_id, stale_run)],
            "the delete must be fenced on the run the node reports, not the recorded one"
        );
    }

    #[tokio::test]
    async fn a_store_that_cannot_answer_reaps_nothing() {
        let deleter = Arc::new(FakeDeleter::default());
        let reaper = reaper(
            Arc::new(FakeRecords {
                recorded: HashMap::new(),
                unreadable: true,
            }),
            Arc::clone(&deleter),
            GRACE,
        );
        let roster = vec![entry(SandboxId::new(), ExecutionId::new())];

        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
        reaper.observe(&node(), &roster, now).await;
        reaper
            .observe(&node(), &roster, now + GRACE + Duration::from_secs(1))
            .await;

        assert!(
            deleter.deleted().is_empty(),
            "not knowing what the control plane records is not evidence of an orphan"
        );
    }

    #[tokio::test]
    async fn a_candidate_that_stops_being_reported_starts_its_grace_over() {
        let deleter = Arc::new(FakeDeleter::default());
        let reaper = reaper(
            Arc::new(FakeRecords::default()),
            Arc::clone(&deleter),
            GRACE,
        );
        let sandbox_id = SandboxId::new();
        let execution_id = ExecutionId::new();
        let roster = vec![entry(sandbox_id, execution_id)];

        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
        reaper.observe(&node(), &roster, now).await;
        reaper
            .observe(&node(), &[], now + Duration::from_secs(5))
            .await;
        reaper
            .observe(&node(), &roster, now + GRACE + Duration::from_secs(1))
            .await;

        assert!(
            deleter.deleted().is_empty(),
            "the sighting that aged out was one the node had already stopped reporting"
        );
    }

    #[tokio::test]
    async fn a_roster_entry_with_no_incarnation_is_never_reaped() {
        let deleter = Arc::new(FakeDeleter::default());
        let reaper = reaper(
            Arc::new(FakeRecords::default()),
            Arc::clone(&deleter),
            GRACE,
        );
        let roster = vec![RosterEntry {
            sandbox_id: SandboxId::new().to_string(),
            execution_id: String::new(),
            projection_ttl: Duration::ZERO,
        }];

        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
        reaper.observe(&node(), &roster, now).await;
        reaper
            .observe(&node(), &roster, now + GRACE + Duration::from_secs(1))
            .await;

        assert!(
            deleter.deleted().is_empty(),
            "a delete that names no run is not fenced against a newer one"
        );
    }
}
