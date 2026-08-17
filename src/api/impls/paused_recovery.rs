//! Cross-node resume, and keeping a node's local paused records honest.
//!
//! A pause leaves the sandbox resumable on its own node through the node-local
//! persister. That is the fast path and it is untouched here. What this module
//! adds is the slow path: rebuilding a sandbox from the shared snapshot
//! repository on a node that has never run it — including when the node that
//! paused it is gone for good — and, on the other side of that move, making
//! sure the node it came from stops claiming to hold it.
//!
//! Publishing itself lives in [`PausedSandboxCoordinator`](super::PausedSandboxCoordinator),
//! which the orchestrator drives directly so that every pause reaches the
//! cluster, not just the ones that arrive through the API.
//!
//! Everything here is inert unless the paused-sandbox registry is configured
//! with a cluster backend.

use tracing::{info, warn};

use super::ApiImpl;
use crate::orchestrator::{
    ClusterRegistration, CreateSandboxRequest, NewTimeout, PausedRegistryState, PausedSandboxEntry,
    ResumeClaim, SandboxLaunchSource, SandboxListFilter, SandboxMetadata,
};
use crate::types::SandboxId;

/// Outcome of trying to resume a sandbox this node has never seen.
pub(super) enum CrossNodeResume {
    /// The sandbox is running here again, under its original ID.
    Restored(Box<SandboxMetadata>),
    /// The cluster has no paused record for it.
    NotFound,
    /// The snapshot has not landed in the repository yet, so only the origin
    /// node can serve this resume.
    NotReady { origin_node_id: String },
    /// Another node is already resuming it, or still holds it.
    Conflict { origin_node_id: String },
    /// The record exists and was claimed, but rebuilding failed.
    Failed(String),
}

/// Why a node's local paused record is no longer the truth.
enum Superseded {
    /// The cluster has moved past this sandbox: resumed elsewhere, or deleted.
    Gone,
    /// Another node holds it now.
    HeldBy(String),
    /// Another node has claimed it and is bringing it back up.
    ClaimedBy(String),
}

impl Superseded {
    fn reason(&self) -> String {
        match self {
            Self::Gone => "the cluster no longer knows this sandbox".to_string(),
            Self::HeldBy(node) => format!("node '{node}' holds it now"),
            Self::ClaimedBy(node) => format!("node '{node}' is resuming it"),
        }
    }
}

impl ApiImpl {
    /// Rebuilds a sandbox this node has never run, from the cluster registry.
    ///
    /// Called only after the local resume reported the sandbox unknown, so a
    /// node that holds the sandbox locally never reaches this path.
    pub(super) async fn resume_from_registry(
        &self,
        sandbox_id: SandboxId,
        timeout: NewTimeout,
    ) -> CrossNodeResume {
        let claim = match self
            .paused
            .registry()
            .claim_for_resume(&sandbox_id, self.paused.node_id())
            .await
        {
            Ok(claim) => claim,
            Err(err) => {
                warn!(error = %err, %sandbox_id, "failed to claim paused sandbox for resume");

                return CrossNodeResume::Failed(err.to_string());
            }
        };

        let entry = match claim {
            ResumeClaim::Claimed(entry) => *entry,
            ResumeClaim::NotFound => return CrossNodeResume::NotFound,
            ResumeClaim::NotReady { origin_node_id } => {
                return CrossNodeResume::NotReady { origin_node_id }
            }
            ResumeClaim::Conflict { origin_node_id } => {
                return CrossNodeResume::Conflict { origin_node_id }
            }
        };

        // The claim only matches rows that name a snapshot, so this cannot be
        // None here; treat it as a failed claim rather than panicking.
        let Some(snapshot_id) = entry.snapshot_id.clone() else {
            self.release_claim(&sandbox_id, entry.generation).await;

            return CrossNodeResume::Failed("paused sandbox has no published snapshot".to_string());
        };

        info!(
            %sandbox_id,
            %snapshot_id,
            origin_node_id = %entry.origin_node_id,
            "restoring paused sandbox from another node's snapshot"
        );

        let snapshot = match self
            .snapshot_manager
            .load_runnable(&snapshot_id.to_string())
            .await
        {
            Ok(Some(snapshot)) => snapshot,
            Ok(None) => {
                // The registry names a snapshot the repository no longer has.
                // Releasing the claim would just make the next resume fail the
                // same way, so drop the record and report it as unknown.
                warn!(%sandbox_id, %snapshot_id, "paused snapshot is missing from the repository");
                if let Err(err) = self.paused.registry().remove(&sandbox_id).await {
                    warn!(error = %err, %sandbox_id, "failed to drop the dangling registry row");
                }

                return CrossNodeResume::NotFound;
            }
            Err(err) => {
                warn!(error = ?err, %sandbox_id, %snapshot_id, "failed to load paused snapshot");
                self.release_claim(&sandbox_id, entry.generation).await;

                return CrossNodeResume::Failed(format!("failed to load paused snapshot: {err}"));
            }
        };

        let request = restore_request(&entry.metadata, snapshot, timeout);

        match self.orchestrator.restore_sandbox(sandbox_id, request).await {
            Ok(metadata) => {
                // The sandbox lives here now. Repointing the row is what tells
                // its former node that its copy is stale, and keeps the
                // snapshot around as this sandbox's durable fallback.
                self.paused.mark_sandbox_running(sandbox_id).await;

                CrossNodeResume::Restored(Box::new(metadata))
            }
            Err(err) => {
                warn!(error = ?err, %sandbox_id, "failed to restore paused sandbox");
                self.release_claim(&sandbox_id, entry.generation).await;

                CrossNodeResume::Failed(err.to_string())
            }
        }
    }

    /// Drops the cluster record and the snapshot for a sandbox that is gone.
    ///
    /// Only used where the orchestrator cannot do it itself: a delete for a
    /// sandbox this node does not hold, which exists in the cluster purely as a
    /// published snapshot.
    pub(super) async fn forget_paused_sandbox(&self, sandbox_id: SandboxId) {
        self.paused.forget_sandbox(sandbox_id).await;
    }

    /// Drops local paused records the cluster has moved past.
    ///
    /// Runs at startup and then on a timer. A node that keeps a paused record
    /// for a sandbox another node has since resumed does more than waste disk:
    /// it keeps reporting that sandbox in its heartbeat roster, so the
    /// scheduler's binding for it flaps between the two nodes and traffic for a
    /// perfectly healthy sandbox lands half the time on the node that only has
    /// a corpse of it. Left long enough, a resume aimed here would start a
    /// second copy, and the two would write to their own rootfs layers from the
    /// same starting point.
    ///
    /// Being periodic is what makes it work: the node that lost the sandbox
    /// gets no notification, so noticing is entirely on it.
    pub async fn reconcile_local_paused_records(&self) {
        // A disabled registry reports every sandbox as missing, which this
        // would read as "all of them moved on".
        if !self.paused.registry().is_cluster_backed() {
            return;
        }

        let paused = match self
            .orchestrator
            .list_sandboxes_filtered(SandboxListFilter {
                states: Some(vec![crate::orchestrator::SandboxState::Paused]),
                ..Default::default()
            })
            .await
        {
            Ok(paused) => paused,
            Err(err) => {
                warn!(error = ?err, "failed to list local paused sandboxes for reconciliation");

                return;
            }
        };

        let mut discarded = 0usize;
        for metadata in paused {
            let sandbox_id = metadata.id;

            let superseded = match self.superseded_by_cluster(sandbox_id).await {
                Ok(Some(superseded)) => superseded,
                Ok(None) => continue,
                Err(()) => {
                    // Never guess when the registry cannot answer: one
                    // unreadable response must not cascade into deleting local
                    // records.
                    warn!(%sandbox_id, "registry unreadable; stopping paused-record reconciliation");

                    return;
                }
            };

            match self
                .orchestrator
                .discard_local_paused_record(sandbox_id)
                .await
            {
                Ok(true) => {
                    info!(%sandbox_id, reason = %superseded.reason(), "discarded superseded paused record");
                    discarded += 1;
                }
                Ok(false) => {}
                Err(err) => {
                    warn!(error = ?err, %sandbox_id, "failed to discard stranded paused record")
                }
            }
        }

        if discarded > 0 {
            info!(
                discarded,
                "discarded paused records the cluster has moved past"
            );
        }
    }

    /// Discards this node's paused record when the cluster says the sandbox has
    /// moved on, so a resume cannot start a second copy.
    ///
    /// Returns whether a record was discarded. Any doubt — registry disabled,
    /// registry unreachable, row still ours — leaves the local record alone:
    /// refusing a resume that would have worked is worse than the narrow race
    /// this closes.
    pub(super) async fn discard_if_superseded(&self, sandbox_id: SandboxId) -> bool {
        if !self.paused.registry().is_cluster_backed() {
            return false;
        }

        let Ok(Some(superseded)) = self.superseded_by_cluster(sandbox_id).await else {
            return false;
        };

        match self
            .orchestrator
            .discard_local_paused_record(sandbox_id)
            .await
        {
            Ok(true) => {
                info!(%sandbox_id, reason = %superseded.reason(), "discarded superseded paused record");

                true
            }
            _ => false,
        }
    }

    /// Decides whether this node's paused copy of a sandbox has been superseded.
    ///
    /// `Ok(None)` means keep it. `Err(())` means the registry could not answer,
    /// which is never a reason to discard anything.
    async fn superseded_by_cluster(&self, sandbox_id: SandboxId) -> Result<Option<Superseded>, ()> {
        // Never reason about a record the cluster was never told about: for
        // those the local copy is the only copy, and absence from the registry
        // carries no information at all. This is also what makes switching an
        // existing node from the node-local backend to a cluster one safe —
        // every record it already holds is untouched.
        let registration = match self
            .orchestrator
            .paused_record_cluster_registration(sandbox_id)
            .await
        {
            Ok(ClusterRegistration::Never) => return Ok(None),
            Ok(registration) => registration,
            Err(err) => {
                warn!(error = ?err, %sandbox_id, "failed to read local registration marker");

                return Ok(None);
            }
        };

        let entry = match self.paused.registry().get(&sandbox_id).await {
            Ok(entry) => entry,
            Err(err) => {
                warn!(error = %err, %sandbox_id, "failed to read the paused registry row");

                return Err(());
            }
        };

        let Some(entry) = entry else {
            // Registered once, no row now: resumed elsewhere, or deleted.
            return Ok(Some(Superseded::Gone));
        };

        Ok(supersession(&entry, &registration))
    }

    async fn release_claim(&self, sandbox_id: &SandboxId, generation: i64) {
        if let Err(err) = self
            .paused
            .registry()
            .release_claim(sandbox_id, generation)
            .await
        {
            warn!(
                error = %err,
                %sandbox_id,
                "failed to release the resume claim; the sandbox stays marked as resuming"
            );
        }
    }
}

/// Decides whether a local paused copy has been superseded by what the registry
/// says, judged against the identity that copy was registered under.
///
/// Kept separate from the I/O around it because this is the judgement that
/// decides whether a node deletes its own copy of a sandbox, and getting it
/// wrong in either direction is expensive: too eager throws away a user's
/// workspace, too shy leaves two nodes claiming the same sandbox.
///
/// Note what it is *not* compared against: the node's current ID. That ID is
/// only as stable as whatever supplies it — under Kubernetes it is commonly the
/// pod name, which changes on every pod recreation — so a node restarting would
/// read every one of its own rows as another node's and discard the lot.
fn supersession(
    entry: &PausedSandboxEntry,
    registration: &ClusterRegistration,
) -> Option<Superseded> {
    // True whatever this node is called: a row that says the sandbox is live,
    // or being brought up, cannot be describing the paused copy sitting here.
    // A claim is safe to yield to without checking who took it — a node only
    // claims once it has found it holds no local record, and a row reaches
    // `Resuming` only from a state that names a snapshot, so there is always a
    // durable copy behind what gets discarded.
    match entry.state {
        PausedRegistryState::Running => {
            return Some(Superseded::HeldBy(entry.origin_node_id.clone()))
        }
        PausedRegistryState::Resuming => {
            return Some(Superseded::ClaimedBy(
                entry
                    .claimed_by_node_id
                    .clone()
                    .unwrap_or_else(|| entry.origin_node_id.clone()),
            ))
        }
        _ => {}
    }

    // Parked, but under someone else's name: another node has paused it since,
    // so this copy is a leftover. Only decidable when this record remembers the
    // identity it was registered under.
    match registration {
        ClusterRegistration::As(node_id) if entry.origin_node_id != *node_id => {
            Some(Superseded::HeldBy(entry.origin_node_id.clone()))
        }
        _ => None,
    }
}

/// Builds the launch request that brings a paused sandbox back.
///
/// Every field is carried over from the record the pausing node wrote, so the
/// restored sandbox keeps the timeout policy, metadata, network policy and
/// extension params it had. `env_vars` is intentionally absent: environment is
/// applied at first boot and is already baked into the snapshot.
fn restore_request(
    metadata: &SandboxMetadata,
    snapshot: crate::snapshot::RunnableSnapshot,
    timeout: NewTimeout,
) -> CreateSandboxRequest {
    CreateSandboxRequest {
        source: SandboxLaunchSource::Snapshot(Box::new(snapshot)),
        // A restore is a resume: the request's timeout wins when it set one,
        // otherwise the sandbox keeps the timeout it was paused with.
        timeout: match timeout {
            NewTimeout::Set(duration) | NewTimeout::EnsureMinimum(duration) => Some(duration),
            NewTimeout::UseExisting => metadata.timeout,
            NewTimeout::None => None,
        },
        timeout_action: metadata.timeout_action,
        auto_resume: metadata.auto_resume,
        user_metadata: metadata.user_metadata.clone(),
        env_vars: None,
        network_policy: metadata.network_policy.clone(),
        secure: metadata.secure,
        custom_extension_params: metadata.custom_extension_params.clone(),
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::*;
    use crate::snapshot::SnapshotId;

    const SELF: &str = "node-a";
    const OTHER: &str = "node-b";

    fn registered_as(node_id: &str) -> ClusterRegistration {
        ClusterRegistration::As(node_id.to_string())
    }

    fn entry(
        state: PausedRegistryState,
        origin: &str,
        claimed_by: Option<&str>,
    ) -> PausedSandboxEntry {
        PausedSandboxEntry {
            sandbox_id: SandboxId::new(),
            cluster_id: uuid::Uuid::nil(),
            state,
            generation: 1,
            origin_node_id: origin.to_string(),
            claimed_by_node_id: claimed_by.map(str::to_string),
            snapshot_id: Some(SnapshotId::generate()),
            metadata: SandboxMetadata::default(),
            paused_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    /// The steady state after a cross-node recovery. Until this node notices,
    /// it keeps advertising the sandbox in its heartbeat roster and the
    /// scheduler binding flaps between the two nodes.
    #[test]
    fn a_row_held_by_another_node_supersedes_the_local_copy() {
        let superseded = supersession(
            &entry(PausedRegistryState::Running, OTHER, None),
            &registered_as(SELF),
        );

        assert!(matches!(superseded, Some(Superseded::HeldBy(node)) if node == OTHER));
    }

    /// A pause that happened elsewhere counts just the same: whoever the row
    /// names as origin owns the sandbox, whatever state it is in.
    #[test]
    fn a_paused_row_owned_by_another_node_also_supersedes() {
        let superseded = supersession(
            &entry(PausedRegistryState::Paused, OTHER, None),
            &registered_as(SELF),
        );

        assert!(matches!(superseded, Some(Superseded::HeldBy(_))));
    }

    /// The claim window. `origin` still points here because the artifacts are
    /// still here, so origin alone cannot catch this — and resuming locally
    /// anyway is exactly how two live copies of one sandbox get started.
    #[test]
    fn a_claim_by_another_node_supersedes_our_own_row() {
        let superseded = supersession(
            &entry(PausedRegistryState::Resuming, SELF, Some(OTHER)),
            &registered_as(SELF),
        );

        assert!(matches!(superseded, Some(Superseded::ClaimedBy(node)) if node == OTHER));
    }

    /// Our row, our sandbox: the ordinary paused case, and by far the most
    /// common one. Discarding here would delete a live user's workspace.
    #[test]
    fn our_own_paused_row_is_not_superseded() {
        assert!(supersession(
            &entry(PausedRegistryState::Paused, SELF, None),
            &registered_as(SELF)
        )
        .is_none());
    }

    /// A pause whose publish failed keeps a `local_only` row naming this node.
    /// That row is the marker saying the local copy is the *only* copy.
    #[test]
    fn our_own_local_only_row_is_not_superseded() {
        assert!(supersession(
            &entry(PausedRegistryState::LocalOnly, SELF, None),
            &registered_as(SELF)
        )
        .is_none());
    }

    /// 🔴 The one that bites hardest. `AENV_NODE_ID` is commonly the pod name
    /// (`metadata.name` in the DaemonSet), so it changes every single time the
    /// pod is recreated — an ordinary rollout. Comparing the registry row
    /// against the node's *current* ID would then read every one of its own
    /// paused rows as another node's, and the first reconciliation pass after a
    /// rollout would delete every paused sandbox on the node.
    #[test]
    fn a_restart_under_a_new_node_id_does_not_supersede_our_own_records() {
        let ours = entry(PausedRegistryState::Paused, "agentenv-old-pod", None);

        // Same physical node, same artifacts on disk, brand new pod name.
        let after_restart = supersession(&ours, &registered_as("agentenv-old-pod"));

        assert!(
            after_restart.is_none(),
            "a record must be judged against the identity it was registered under"
        );
    }

    /// Records announced by an older build carry no identity, so nothing about
    /// ownership can be concluded — but "it is running elsewhere" still can.
    #[test]
    fn anonymous_registration_still_yields_to_a_live_holder() {
        assert!(supersession(
            &entry(PausedRegistryState::Running, OTHER, None),
            &ClusterRegistration::Anonymous
        )
        .is_some());
        assert!(supersession(
            &entry(PausedRegistryState::Paused, OTHER, None),
            &ClusterRegistration::Anonymous
        )
        .is_none());
    }

    /// A claim always wins over a local paused copy, whoever took it.
    ///
    /// Safe because of two invariants that hold together: a node only claims
    /// after finding it has no local record, so "the claimer is us" cannot
    /// coexist with the record being judged here; and a row can only reach
    /// `Resuming` from `Paused`/`Running` with a snapshot, so there is always a
    /// durable copy in the repository behind whatever gets discarded.
    #[test]
    fn any_claim_supersedes_a_local_paused_copy() {
        for claimer in [Some(OTHER), None] {
            assert!(
                supersession(
                    &entry(PausedRegistryState::Resuming, SELF, claimer),
                    &registered_as(SELF)
                )
                .is_some(),
                "a resuming row must never leave a local paused copy resumable"
            );
        }
    }
}
