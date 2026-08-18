//! Cluster-wide bookkeeping for paused sandboxes.
//!
//! This is the half of cross-node pause that does not need the orchestrator:
//! publishing a capture to the shared snapshot repository and keeping the
//! registry row in step with it. The orchestrator drives it through
//! [`PausedSandboxPublisher`], so every pause, resume and delete reaches the
//! cluster no matter which of them started it — the API, the expiry evictor, or
//! graceful shutdown.
//!
//! Everything here is inert unless the registry is configured with a cluster
//! backend.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tracing::{error, info, warn};

use crate::orchestrator::{
    PauseOutcome, PausedRegistryState, PausedSandboxEntry, PausedSandboxPublisher,
    PausedSandboxRegistry, SandboxMetadata,
};
use crate::snapshot::{
    SnapshotId, SnapshotManager, SnapshotPublishMetadata, SnapshotPublishSource,
};
use crate::types::SandboxId;

/// Which sandboxes running on this node the registry has confirmed this node as
/// the holder of, and the identity it confirmed each of them under.
///
/// The paused half of this bookkeeping is durable (`registered_as` on the
/// persisted record) because a paused sandbox outlives the process. A running
/// one does not: it is never persisted, and a restart leaves its VM behind as a
/// process nobody tracks. So this in-process map is not a weaker version of the
/// durable marker, it is the matching one — and it answers exactly what
/// reconciliation must know before acting on a registry row: *this* process put
/// that row there.
///
/// Without it an absent row is ambiguous, and ambiguous in the expensive
/// direction: a sandbox created here a second ago has no row either, and
/// reading that as "the cluster has moved past it" would tear down a live
/// sandbox nobody asked to remove.
#[derive(Default)]
struct RunningRegistrations(Mutex<HashMap<SandboxId, String>>);

impl RunningRegistrations {
    /// Applies the registry's answer to a `mark_running`.
    ///
    /// Only a confirmed write enrols the sandbox: an unacknowledged one is no
    /// evidence that the row says what this node believes it says, and
    /// reconciliation acts on that evidence.
    ///
    /// A refusal leaves any earlier confirmation standing rather than clearing
    /// it. That is the point, not laziness — a refusal means another node holds
    /// the claim, which is precisely the situation the earlier confirmation
    /// makes recognisable.
    fn observe(&self, sandbox_id: SandboxId, node_id: &str, confirmed: bool) {
        if !confirmed {
            return;
        }
        self.lock().insert(sandbox_id, node_id.to_string());
    }

    fn get(&self, sandbox_id: &SandboxId) -> Option<String> {
        self.lock().get(sandbox_id).cloned()
    }

    fn forget(&self, sandbox_id: &SandboxId) {
        self.lock().remove(sandbox_id);
    }

    fn retain(&self, live: &HashSet<SandboxId>) {
        self.lock()
            .retain(|sandbox_id, _| live.contains(sandbox_id));
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<SandboxId, String>> {
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// Owns the cluster's view of this node's paused sandboxes.
pub struct PausedSandboxCoordinator {
    registry: Arc<dyn PausedSandboxRegistry>,
    snapshot_manager: Arc<SnapshotManager>,
    node_id: String,
    running_registrations: RunningRegistrations,
}

impl PausedSandboxCoordinator {
    pub fn new(
        registry: Arc<dyn PausedSandboxRegistry>,
        snapshot_manager: Arc<SnapshotManager>,
        node_id: String,
    ) -> Self {
        Self {
            registry,
            snapshot_manager,
            node_id,
            running_registrations: RunningRegistrations::default(),
        }
    }

    pub fn registry(&self) -> &Arc<dyn PausedSandboxRegistry> {
        &self.registry
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Publishes a just-paused sandbox and records it cluster-wide.
    ///
    /// Deliberately best-effort: by the time this runs the sandbox is already
    /// paused, persisted and locally resumable, so every failure below costs
    /// cross-node recovery and nothing else. Turning a published-snapshot
    /// failure into a pause failure would trade a working pause for a broken
    /// one.
    async fn publish(&self, outcome: PauseOutcome) -> Option<String> {
        let sandbox_id = outcome.metadata.id;

        // No publishable capture means the pause wrote into backend-managed
        // temporaries that no other node could ever read.
        let publishable = outcome.publishable?;

        let entry = PausedSandboxEntry {
            sandbox_id,
            // Filled in by the registry from its own configured cluster.
            cluster_id: uuid::Uuid::nil(),
            state: PausedRegistryState::Publishing,
            generation: 0,
            origin_node_id: self.node_id.clone(),
            claimed_by_node_id: None,
            snapshot_id: None,
            metadata: outcome.metadata.clone(),
            paused_at: DateTime::<Utc>::from(outcome.metadata.created_at),
            updated_at: Utc::now(),
        };

        let began = match self.registry.begin_pause(&entry).await {
            Ok(began) => began,
            Err(err) => {
                warn!(
                    error = %err,
                    %sandbox_id,
                    "failed to register paused sandbox; it stays resumable on this node only"
                );

                return None;
            }
        };

        let published = self
            .snapshot_manager
            .publish_captured(publish_metadata(&outcome.metadata), publishable)
            .await;

        match published {
            Ok(record) => {
                let snapshot_id = record.id.clone();
                match self
                    .registry
                    .complete_pause(&sandbox_id, began.generation, &snapshot_id)
                    .await
                {
                    Ok(()) => {
                        info!(%sandbox_id, %snapshot_id, "paused sandbox is recoverable cluster-wide");

                        // Only now is the previous snapshot safe to drop: until
                        // this point it was the sandbox's one durable copy.
                        if let Some(previous) = began.previous_snapshot_id {
                            if previous != snapshot_id {
                                self.delete_snapshot(
                                    &sandbox_id,
                                    &previous,
                                    "superseded by a newer pause",
                                )
                                .await;
                            }
                        }
                    }
                    Err(err) => {
                        // The snapshot is durable but the registry disagrees,
                        // which means either the write never landed or some
                        // other writer moved the sandbox on while we published.
                        // Either way nothing is going to reference what we just
                        // uploaded.
                        warn!(
                            error = %err,
                            %sandbox_id,
                            %snapshot_id,
                            "published paused snapshot but could not mark it durable"
                        );
                        self.discard_unreferenced_snapshot(&sandbox_id, &snapshot_id)
                            .await;
                    }
                }
            }
            Err(err) => {
                warn!(
                    error = %err,
                    %sandbox_id,
                    "failed to publish paused snapshot; it stays resumable on this node only"
                );
                // Keep the row and downgrade it instead of deleting it: the
                // sandbox is genuinely paused here, just not recoverable
                // anywhere else. A deleted row would be indistinguishable from
                // "already resumed elsewhere", and reconciliation would then
                // throw away the only copy that exists.
                if let Err(err) = self
                    .registry
                    .mark_local_only(&sandbox_id, began.generation)
                    .await
                {
                    warn!(error = %err, %sandbox_id, "failed to mark the registry row local-only");
                }
            }
        }

        Some(self.node_id.clone())
    }

    /// Records that the sandbox is live on this node.
    ///
    /// Called on both resume paths — the local fast path and a cross-node
    /// restore — because both end with this node holding a sandbox the registry
    /// still describes as parked somewhere else.
    ///
    /// A confirmed write is also what enrols the sandbox in
    /// [`running_registration`](Self::running_registration), and only a
    /// confirmed one: an unacknowledged write is no evidence that the row says
    /// what this node thinks it says, and reconciliation acts on that evidence.
    /// A refusal or an error leaves any earlier confirmation standing — it
    /// remains true that the cluster once named this node the holder, which is
    /// precisely the premise reconciliation needs to notice that it no longer
    /// does.
    pub async fn mark_sandbox_running(&self, sandbox_id: SandboxId) {
        let confirmed = match self.registry.mark_running(&sandbox_id, &self.node_id).await {
            Ok(confirmed) => confirmed,
            Err(err) => {
                warn!(
                    error = %err,
                    %sandbox_id,
                    "failed to record the sandbox as running here; another node may keep a stale copy"
                );

                false
            }
        };

        self.running_registrations
            .observe(sandbox_id, &self.node_id, confirmed);
    }

    /// The identity this node was confirmed as the holder of `sandbox_id`
    /// under, if the registry ever confirmed it.
    ///
    /// `None` means the cluster has never been told this node runs the sandbox,
    /// and therefore that nothing the registry says (or fails to say) about it
    /// is about this node's copy.
    pub fn running_registration(&self, sandbox_id: &SandboxId) -> Option<String> {
        self.running_registrations.get(sandbox_id)
    }

    /// Drops registrations for sandboxes this node no longer has, so the map
    /// tracks the node's roster rather than growing with every resume it has
    /// ever served.
    pub fn retain_running_registrations(&self, live: &HashSet<SandboxId>) {
        self.running_registrations.retain(live);
    }

    /// Drops a sandbox's registry row and the snapshot behind it.
    ///
    /// Only correct once the sandbox itself is gone: the snapshot is what makes
    /// the sandbox recoverable, so removing it while the sandbox still exists
    /// somewhere would quietly strip its last durable copy.
    pub async fn forget_sandbox(&self, sandbox_id: SandboxId) {
        self.running_registrations.forget(&sandbox_id);

        let entry = match self.registry.get(&sandbox_id).await {
            Ok(Some(entry)) => entry,
            // Nothing recorded (the common case with a node-local registry).
            Ok(None) => return,
            Err(err) => {
                warn!(error = %err, %sandbox_id, "failed to read the paused registry row");

                return;
            }
        };

        if let Some(holder) = live_elsewhere(&entry, &self.node_id) {
            // Deleting the local copy of a sandbox that is live on another node
            // says nothing about that node's copy — it keeps running. Clearing
            // the row here would only strip it of the snapshot it can be
            // recovered from, so this node's delete stays local.
            warn!(
                %sandbox_id,
                holder,
                "not clearing the cluster record: the sandbox is live on another node"
            );

            return;
        }

        if let Err(err) = self.registry.remove(&sandbox_id).await {
            warn!(error = %err, %sandbox_id, "failed to clear the paused registry row");

            // Keep the snapshot: the row still points at it.
            return;
        }

        if let Some(snapshot_id) = entry.snapshot_id {
            self.delete_snapshot(&sandbox_id, &snapshot_id, "sandbox deleted")
                .await;
        }
    }

    /// Deletes a snapshot the registry never came to reference.
    ///
    /// Re-reads the row first, because a failed `complete_pause` does not prove
    /// the write was rejected — a dropped response looks exactly the same from
    /// here, and the row would then be pointing at the snapshot about to be
    /// removed. Only a positive answer that the row does *not* reference it is
    /// enough to delete; when the registry cannot answer at all the snapshot is
    /// left behind and named in the log, because an operator can collect
    /// garbage but cannot un-delete a referenced snapshot.
    async fn discard_unreferenced_snapshot(
        &self,
        sandbox_id: &SandboxId,
        snapshot_id: &SnapshotId,
    ) {
        let reread = self.registry.get(sandbox_id).await;
        let readable = reread.is_ok();
        let referenced = reread
            .as_ref()
            .ok()
            .map(|entry| snapshot_is_referenced(entry.as_ref(), snapshot_id));

        match orphan_verdict(readable, referenced.unwrap_or(false)) {
            OrphanVerdict::Delete => {
                self.delete_snapshot(sandbox_id, snapshot_id, "never referenced by the registry")
                    .await
            }
            OrphanVerdict::Referenced => info!(
                %sandbox_id,
                %snapshot_id,
                "registry does reference the published snapshot after all; keeping it"
            ),
            OrphanVerdict::Unknown => error!(
                %sandbox_id,
                %snapshot_id,
                "cannot tell whether the published snapshot is referenced; \
                 leaving it in the repository for manual collection"
            ),
        }
    }

    async fn delete_snapshot(
        &self,
        sandbox_id: &SandboxId,
        snapshot_id: &SnapshotId,
        reason: &'static str,
    ) {
        match self.snapshot_manager.delete(snapshot_id.to_string()).await {
            Ok(()) => info!(%sandbox_id, %snapshot_id, reason, "deleted paused snapshot"),
            Err(err) => {
                warn!(error = ?err, %sandbox_id, %snapshot_id, reason, "failed to delete paused snapshot")
            }
        }
    }
}

#[async_trait]
impl PausedSandboxPublisher for PausedSandboxCoordinator {
    async fn publish_paused(&self, outcome: PauseOutcome) -> Option<String> {
        self.publish(outcome).await
    }

    async fn mark_running(&self, sandbox_id: SandboxId) {
        self.mark_sandbox_running(sandbox_id).await;
    }

    async fn forget(&self, sandbox_id: SandboxId) {
        self.forget_sandbox(sandbox_id).await;
    }
}

/// Names the node running the sandbox, when that is someone other than us.
///
/// A row in any parked state — paused, publishing, local-only — is fair game to
/// clear from any node: the delete then completes on whichever node holds the
/// artifacts, when it next reconciles. A sandbox that is *live* elsewhere is
/// not, because nothing this node does will stop it.
fn live_elsewhere(entry: &PausedSandboxEntry, node_id: &str) -> Option<String> {
    match entry.state {
        PausedRegistryState::Running if entry.origin_node_id != node_id => {
            Some(entry.origin_node_id.clone())
        }
        PausedRegistryState::Resuming => entry
            .claimed_by_node_id
            .as_ref()
            .filter(|claimer| *claimer != node_id)
            .cloned(),
        _ => None,
    }
}

/// What to do with a snapshot whose `complete_pause` did not report success.
#[derive(Debug, PartialEq, Eq)]
enum OrphanVerdict {
    /// Nothing references it: collect it.
    Delete,
    /// The row does reference it, so the write landed after all.
    Referenced,
    /// The registry could not answer.
    Unknown,
}

fn snapshot_is_referenced(entry: Option<&PausedSandboxEntry>, snapshot_id: &SnapshotId) -> bool {
    entry.is_some_and(|entry| entry.snapshot_id.as_ref() == Some(snapshot_id))
}

/// Decides the fate of a freshly published snapshot the registry did not
/// acknowledge.
///
/// A failed `complete_pause` does not prove the write was rejected — a dropped
/// response looks exactly the same from here — so a re-read has to settle it.
/// When even the re-read fails, the snapshot stays: an operator can collect
/// garbage, but cannot un-delete the one durable copy of a sandbox.
fn orphan_verdict(registry_readable: bool, referenced: bool) -> OrphanVerdict {
    match (registry_readable, referenced) {
        (false, _) => OrphanVerdict::Unknown,
        (true, true) => OrphanVerdict::Referenced,
        (true, false) => OrphanVerdict::Delete,
    }
}

/// Describes the snapshot a pause publishes.
fn publish_metadata(metadata: &SandboxMetadata) -> SnapshotPublishMetadata {
    SnapshotPublishMetadata {
        id: SnapshotId::generate(),
        // Unnamed on purpose: this snapshot is an implementation detail of
        // pause, not something a user asked to be able to launch by name.
        alias: None,
        source: SnapshotPublishSource::Sandbox {
            source_sandbox_id: metadata.id.to_string(),
        },
        context: metadata.context.clone(),
        startup: metadata.startup.clone(),
        resources: metadata.resources,
        runtime_versions: metadata.runtime_versions.clone(),
        virtualization_mode: metadata.virtualization_mode,
        image_configs: metadata.image_configs.clone(),
        custom_extension_params: metadata.custom_extension_params.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The failure that motivated all this: `complete_pause` fails, nothing
    /// ever points at the snapshot, and it sits in the repository forever.
    #[test]
    fn unreferenced_snapshot_is_collected() {
        assert_eq!(orphan_verdict(true, false), OrphanVerdict::Delete);
    }

    /// A `complete_pause` whose response was lost still applied the write. The
    /// re-read is the only thing that can tell that apart from a rejection, and
    /// deleting here would strip the sandbox's cross-node copy.
    #[test]
    fn snapshot_the_registry_points_at_is_kept() {
        assert_eq!(orphan_verdict(true, true), OrphanVerdict::Referenced);
    }

    /// The registry being unreachable is exactly when both calls fail together,
    /// so this is the common case, not a corner one. Leaking is recoverable by
    /// hand; deleting a referenced snapshot is not.
    #[test]
    fn unreadable_registry_never_deletes() {
        assert_eq!(orphan_verdict(false, false), OrphanVerdict::Unknown);
        assert_eq!(orphan_verdict(false, true), OrphanVerdict::Unknown);
    }

    /// 🔴 The one that decides whether reconciliation may act at all. A
    /// sandbox created on this node and never resumed from the cluster has no
    /// registry row, and neither does one the cluster has genuinely forgotten —
    /// only this map tells them apart, and only a confirmed write may put an
    /// entry in it.
    #[test]
    fn an_unconfirmed_mark_leaves_the_sandbox_unregistered() {
        let registrations = RunningRegistrations::default();
        let sandbox_id = SandboxId::new();

        registrations.observe(sandbox_id, "node-a", false);

        assert_eq!(registrations.get(&sandbox_id), None);
    }

    #[test]
    fn a_confirmed_mark_records_the_identity_it_was_confirmed_under() {
        let registrations = RunningRegistrations::default();
        let sandbox_id = SandboxId::new();

        registrations.observe(sandbox_id, "node-a", true);

        assert_eq!(registrations.get(&sandbox_id), Some("node-a".to_string()));
    }

    /// A later refusal means another node took the claim — the very case the
    /// earlier confirmation exists to make recognisable. Clearing it here would
    /// silently disarm the reaper for exactly the sandbox it is meant to catch.
    #[test]
    fn a_refusal_does_not_clear_an_earlier_confirmation() {
        let registrations = RunningRegistrations::default();
        let sandbox_id = SandboxId::new();

        registrations.observe(sandbox_id, "node-a", true);
        registrations.observe(sandbox_id, "node-a", false);

        assert_eq!(registrations.get(&sandbox_id), Some("node-a".to_string()));
    }

    #[test]
    fn retain_drops_sandboxes_this_node_no_longer_has() {
        let registrations = RunningRegistrations::default();
        let kept = SandboxId::new();
        let dropped = SandboxId::new();
        registrations.observe(kept, "node-a", true);
        registrations.observe(dropped, "node-a", true);

        registrations.retain(&HashSet::from([kept]));

        assert_eq!(registrations.get(&kept), Some("node-a".to_string()));
        assert_eq!(registrations.get(&dropped), None);
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

    /// Deleting a stale local copy must not take the live sandbox's snapshot
    /// with it. The delete does not reach the node actually running it, so
    /// clearing the row would leave that node's sandbox unrecoverable.
    #[test]
    fn a_sandbox_running_on_another_node_is_not_forgotten() {
        assert_eq!(
            live_elsewhere(
                &entry(PausedRegistryState::Running, "node-b", None),
                "node-a"
            ),
            Some("node-b".to_string())
        );
    }

    /// Same for a resume in flight somewhere else.
    #[test]
    fn a_sandbox_claimed_by_another_node_is_not_forgotten() {
        assert_eq!(
            live_elsewhere(
                &entry(PausedRegistryState::Resuming, "node-a", Some("node-b")),
                "node-a"
            ),
            Some("node-b".to_string())
        );
    }

    /// The ordinary delete: this node holds it, so it clears its own record.
    #[test]
    fn our_own_sandbox_is_forgotten() {
        assert!(live_elsewhere(
            &entry(PausedRegistryState::Running, "node-a", None),
            "node-a"
        )
        .is_none());
    }

    /// A parked sandbox can be cleared from anywhere: the node holding the
    /// artifacts finishes the delete when it next reconciles. Without this a
    /// sandbox that only exists as a published snapshot could never be deleted.
    #[test]
    fn a_parked_sandbox_is_forgotten_from_any_node() {
        for state in [
            PausedRegistryState::Paused,
            PausedRegistryState::Publishing,
            PausedRegistryState::LocalOnly,
        ] {
            assert!(
                live_elsewhere(&entry(state, "node-b", None), "node-a").is_none(),
                "{state:?} should be clearable from another node"
            );
        }
    }
}
