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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tracing::{error, info, warn};

use crate::orchestrator::{
    MarkRunningOutcome, PauseOutcome, PausedRegistryState, PausedSandboxEntry,
    PausedSandboxPublisher, PausedSandboxRegistry, SandboxMetadata,
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

    fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<SandboxId, String>> {
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// The one window in which this node may hand back the sandboxes a previous
/// process on the same machine died holding.
///
/// 🔴 The window is not a period of time, it is a state of this process:
/// it stays open exactly as long as this process has done nothing that could
/// put this node's name on a `running` or `resuming` row. Once it has, the
/// rows [`release_node_holdings`](PausedSandboxRegistry::release_node_holdings)
/// selects — by node identity alone — stop being only the dead process's and
/// start including this one's own live sandboxes.
///
/// It closes on the *attempt*, not on the confirmation, and that difference is
/// the whole reason this exists as its own latch rather than as
/// `running_registrations.is_empty()`. The case a retry exists for is a
/// registry that was unreachable at startup; in that same outage a resume
/// fails open and runs the sandbox here anyway, while its `mark_running` goes
/// unconfirmed and enrols nothing. Fencing on confirmations alone would read
/// that node as holding nothing and release the rows of the sandboxes it is
/// actually running — the duplication this whole module exists to prevent.
/// `retain_running_registrations` can empty the map again for the same reason.
///
/// The mutex is held across the release itself, so a resume arriving mid-flight
/// waits rather than slipping past a fence that has already been read.
struct TakeoverWindow(tokio::sync::Mutex<bool>);

impl TakeoverWindow {
    fn new() -> Self {
        Self(tokio::sync::Mutex::new(true))
    }

    /// Takes the window for one release attempt, or `None` once it has closed.
    async fn enter(&self) -> Option<TakeoverAttempt<'_>> {
        let guard = self.0.lock().await;
        if !*guard {
            return None;
        }

        Some(TakeoverAttempt(guard))
    }

    /// Closes the window for good. Monotonic: nothing reopens it.
    async fn close(&self) {
        *self.0.lock().await = false;
    }
}

/// A held-open [`TakeoverWindow`]. Dropping it without
/// [`settle`](TakeoverAttempt::settle) leaves the window open for a retry.
struct TakeoverAttempt<'a>(tokio::sync::MutexGuard<'a, bool>);

impl TakeoverAttempt<'_> {
    fn settle(mut self) {
        *self.0 = false;
    }
}

/// How an attempt to release a previous process's holdings settled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StaleReleaseOutcome {
    /// The rows were handed back, or there were none. Nothing more to do.
    Released,
    /// The takeover window has closed — this process has taken a sandbox live,
    /// so releasing by node identity would now give away one of its own.
    /// Whatever was still stranded stays stranded until the next start.
    Fenced,
    /// The registry could not be reached. Worth retrying while the window is
    /// still open.
    Failed,
}

/// Owns the cluster's view of this node's paused sandboxes.
pub struct PausedSandboxCoordinator {
    registry: Arc<dyn PausedSandboxRegistry>,
    snapshot_manager: Arc<SnapshotManager>,
    node_id: String,
    running_registrations: RunningRegistrations,
    takeover_window: TakeoverWindow,
    /// How many lease renewals in a row have failed.
    ///
    /// 🔴 A single missed renewal is nothing — the configured TTL is held to
    /// three renewal intervals precisely so that two may be missed. It is the
    /// *run* that matters, and no single failure can report one: each one is
    /// logged and forgotten, so the third in a row looks exactly like the
    /// first, while it is the third that hands this node's parked sandboxes to
    /// whoever claims them next. Against a database on a permanently open pool
    /// three in a row was close to unreachable; against a service that rolls it
    /// is a normal deployment.
    consecutive_renew_failures: AtomicU64,
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
            takeover_window: TakeoverWindow::new(),
            consecutive_renew_failures: AtomicU64::new(0),
        }
    }

    /// Records how a lease renewal went and publishes the current run of
    /// failures.
    ///
    /// A gauge rather than a counter: what an operator has to see is the run
    /// standing right now against the number of misses the TTL allows, and a
    /// total of renewals that ever failed does not answer that.
    pub fn observe_lease_renewal(&self, renewed: bool) {
        let failures = if renewed {
            self.consecutive_renew_failures.store(0, Ordering::SeqCst);
            0
        } else {
            self.consecutive_renew_failures
                .fetch_add(1, Ordering::SeqCst)
                + 1
        };

        metrics::gauge!("agentenv_paused_registry_renew_consecutive_failures").set(failures as f64);
    }

    #[cfg(test)]
    pub(crate) fn consecutive_renew_failures(&self) -> u64 {
        self.consecutive_renew_failures.load(Ordering::SeqCst)
    }

    pub fn registry(&self) -> &Arc<dyn PausedSandboxRegistry> {
        &self.registry
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Records that this process is about to try to make a sandbox live here,
    /// closing the window in which releasing rows by node identity is exact.
    ///
    /// Called before the write, not after it: a claim whose response was lost
    /// still wrote `resuming` with this node's name on it, and a `mark_running`
    /// that went unacknowledged still wrote `running`. Either is enough to make
    /// a later release by node identity give away a sandbox this process holds.
    pub async fn note_taking_sandbox_live(&self) {
        self.takeover_window.close().await;
    }

    /// Takes the takeover window for one attempt at releasing what a previous
    /// process on this machine left behind, or `None` once it has closed.
    ///
    /// The returned guard keeps the window shut to everything else for as long
    /// as it is held, so the release either happens entirely before this
    /// process takes on a sandbox or does not happen at all.
    async fn enter_takeover_window(&self) -> Option<TakeoverAttempt<'_>> {
        // Belt and braces: a confirmed registration is by construction preceded
        // by `note_taking_sandbox_live`, so this can only fire if some future
        // caller enrols a sandbox without going through `mark_sandbox_running`.
        if !self.running_registrations.is_empty() {
            return None;
        }

        self.takeover_window.enter().await
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
            metadata: Some(outcome.metadata.clone()),
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
                        let _ = self
                            .discard_unreferenced_snapshot(&sandbox_id, &snapshot_id)
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

    /// Hands back the rows a previous process on this machine died holding.
    ///
    /// Idempotent by construction: the first success closes the takeover
    /// window, and so does the first sandbox this process takes live, so this
    /// can be called repeatedly and will act at most once.
    pub(super) async fn release_stale_holdings(&self) -> StaleReleaseOutcome {
        let Some(attempt) = self.enter_takeover_window().await else {
            return StaleReleaseOutcome::Fenced;
        };

        match self.registry.release_node_holdings(&self.node_id).await {
            Ok(released) => {
                attempt.settle();
                if !released.is_empty() {
                    metrics::counter!("agentenv_paused_registry_node_holdings_released_total")
                        .increment(released.released);
                    metrics::counter!("agentenv_paused_registry_node_holdings_discarded_total")
                        .increment(released.discarded);
                    info!(
                        node_id = %self.node_id,
                        released = released.released,
                        discarded = released.discarded,
                        "took over the sandboxes a previous process on this node was holding"
                    );
                }

                StaleReleaseOutcome::Released
            }
            Err(err) => {
                // Nothing else releases these rows, so the sandboxes stay
                // stranded until either a retry lands or a later start
                // succeeds. That is the safe direction — the alternative is a
                // timeout deciding it, which is the thing this replaced.
                metrics::counter!("agentenv_paused_registry_stale_release_failed_total")
                    .increment(1);
                warn!(
                    error = %err,
                    node_id = %self.node_id,
                    "could not release the previous process's holdings; \
                     sandboxes it was running stay unclaimable until a retry lands"
                );

                StaleReleaseOutcome::Failed
            }
        }
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
        // Before the write: see `note_taking_sandbox_live`.
        self.note_taking_sandbox_live().await;

        let confirmed = match self.registry.mark_running(&sandbox_id, &self.node_id).await {
            Ok(MarkRunningOutcome::HeldElsewhere) => {
                // 🔴 The cluster says another node holds the resume claim on a
                // sandbox this one has just brought up. Both are about to run
                // it. Until D11 this arrived as the same `false` as the healthy
                // "never paused, nothing to track" case and went unremarked.
                //
                // Not fatal here on purpose: the VM is already live, and the
                // node that can safely stand down is decided by reconciliation
                // with the whole row in front of it, not by this write's
                // return value.
                warn!(
                    %sandbox_id,
                    "the cluster holds this sandbox's resume claim elsewhere; two nodes may be running it"
                );

                false
            }
            Ok(outcome) => outcome.adopted(),
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

        // The generation is the one read above, so the delete is conditional on
        // the row not having moved since. The check above — "is it live
        // elsewhere?" — is a read, and between it and this write a resume
        // somewhere else can start; quoting the generation is what makes the
        // two behave as one decision.
        match self.registry.remove(&sandbox_id, entry.generation).await {
            Ok(true) => {}
            Ok(false) => {
                // The row moved between the read and the delete, which means
                // somebody else now owns it and its snapshot is theirs.
                warn!(
                    %sandbox_id,
                    "not clearing the cluster record: it changed hands while this delete was being decided"
                );

                return;
            }
            Err(err) => {
                warn!(error = %err, %sandbox_id, "failed to clear the paused registry row");

                // Keep the snapshot: the row still points at it.
                return;
            }
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
    ///
    /// Returns the verdict it acted on, so a test can tell "read the row and
    /// concluded nothing references it" apart from "deleted without looking" —
    /// two behaviours that are indistinguishable from the repository's side and
    /// differ only in whether they can destroy a sandbox's one durable copy.
    async fn discard_unreferenced_snapshot(
        &self,
        sandbox_id: &SandboxId,
        snapshot_id: &SnapshotId,
    ) -> OrphanVerdict {
        let reread = self.registry.get(sandbox_id).await;
        let readable = reread.is_ok();
        let referenced = reread
            .as_ref()
            .ok()
            .map(|entry| snapshot_is_referenced(entry.as_ref(), snapshot_id));

        let verdict = orphan_verdict(readable, referenced.unwrap_or(false));
        match verdict {
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

        verdict
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
    use super::test_support::{CountingRegistry, GetAnswer};
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
            metadata: Some(SandboxMetadata::default()),
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

    /// 🔴 The re-read is the whole safety property, and it is invisible from
    /// the repository's side: "read the row, nothing references this snapshot,
    /// delete it" and "delete it without looking" produce the same repository
    /// afterwards on the happy path and differ only when the write landed and
    /// its response was lost — which is exactly the case that costs a sandbox
    /// its one durable copy. So the test asserts the read happened, not just
    /// that the verdict was right.
    #[tokio::test]
    async fn a_failed_complete_pause_rereads_the_row_before_touching_the_snapshot() {
        let snapshot_id = SnapshotId::generate();
        let registry = Arc::new(CountingRegistry::answering(GetAnswer::Referencing(
            snapshot_id.clone(),
        )));
        let coordinator = coordinator(Arc::clone(&registry));

        let verdict = coordinator
            .discard_unreferenced_snapshot(&SandboxId::new(), &snapshot_id)
            .await;

        assert_eq!(registry.get_calls(), 1, "the row must be re-read");
        assert_eq!(
            verdict,
            OrphanVerdict::Referenced,
            "a complete_pause whose response was lost still applied the write"
        );
    }

    /// The registry being unreachable is exactly when both calls fail together,
    /// so this is the common case rather than a corner one. Leaking a snapshot
    /// is recoverable by hand; deleting a referenced one is not.
    #[tokio::test]
    async fn an_unreadable_registry_leaves_the_snapshot_alone() {
        let registry = Arc::new(CountingRegistry::answering(GetAnswer::Unreachable));
        let coordinator = coordinator(Arc::clone(&registry));

        let verdict = coordinator
            .discard_unreferenced_snapshot(&SandboxId::new(), &SnapshotId::generate())
            .await;

        assert_eq!(registry.get_calls(), 1);
        assert_eq!(verdict, OrphanVerdict::Unknown);
    }

    /// And the case the re-read exists to permit: the write really was
    /// rejected, nothing points at the upload, so it is collected.
    #[tokio::test]
    async fn a_snapshot_no_row_points_at_is_collected() {
        let registry = Arc::new(CountingRegistry::answering(GetAnswer::Missing));
        let coordinator = coordinator(Arc::clone(&registry));

        let verdict = coordinator
            .discard_unreferenced_snapshot(&SandboxId::new(), &SnapshotId::generate())
            .await;

        assert_eq!(registry.get_calls(), 1);
        assert_eq!(verdict, OrphanVerdict::Delete);
    }

    fn coordinator(registry: Arc<CountingRegistry>) -> PausedSandboxCoordinator {
        PausedSandboxCoordinator::new(
            registry,
            Arc::new(crate::snapshot::mock::mock_snapshot_manager()),
            "node-a".to_string(),
        )
    }

    /// The failure this whole retry exists for. Nothing else releases these
    /// rows and `claim_for_resume` refuses them outright, so a single failed
    /// attempt used to strand every sandbox the previous process was running
    /// until somebody restarted the node again.
    #[tokio::test]
    async fn a_registry_failure_leaves_the_window_open_for_another_attempt() {
        let registry = Arc::new(CountingRegistry::new(1, false));
        let coordinator = coordinator(Arc::clone(&registry));

        assert_eq!(
            coordinator.release_stale_holdings().await,
            StaleReleaseOutcome::Failed
        );
        assert_eq!(
            coordinator.release_stale_holdings().await,
            StaleReleaseOutcome::Released
        );
        assert_eq!(registry.release_calls(), 2);
    }

    /// Once the rows are back there is nothing left to release, and asking
    /// again on a node that has since started serving would be the dangerous
    /// call. The window shuts on success as firmly as it does on a takeover.
    #[tokio::test]
    async fn a_successful_release_closes_the_window() {
        let registry = Arc::new(CountingRegistry::new(0, false));
        let coordinator = coordinator(Arc::clone(&registry));

        assert_eq!(
            coordinator.release_stale_holdings().await,
            StaleReleaseOutcome::Released
        );
        assert_eq!(
            coordinator.release_stale_holdings().await,
            StaleReleaseOutcome::Fenced
        );
        assert_eq!(registry.release_calls(), 1);
    }

    /// 🔴 The fence itself. Releasing by node identity is only exact while this
    /// process holds nothing; a claim taken since then names this node on a
    /// `resuming` row, and releasing it would hand a live sandbox to whoever
    /// resumes it next.
    #[tokio::test]
    async fn taking_a_sandbox_live_fences_the_release() {
        let registry = Arc::new(CountingRegistry::always_failing());
        let coordinator = coordinator(Arc::clone(&registry));

        coordinator.note_taking_sandbox_live().await;

        assert_eq!(
            coordinator.release_stale_holdings().await,
            StaleReleaseOutcome::Fenced
        );
        assert_eq!(
            registry.release_calls(),
            0,
            "a fenced release must not reach the registry at all"
        );
    }

    /// 🔴 The case the retry was added for, and the reason the fence is not
    /// `running_registrations.is_empty()`. During a registry outage a resume
    /// fails open and runs the sandbox here, while its `mark_running` goes
    /// unacknowledged and enrols nothing. A fence that only counted confirmed
    /// registrations would read this node as holding nothing, and the retry
    /// that fires when the registry comes back would release the rows of the
    /// sandboxes it is actually running.
    #[tokio::test]
    async fn an_unconfirmed_mark_running_still_fences_the_release() {
        let registry = Arc::new(CountingRegistry::new(usize::MAX, true));
        let coordinator = coordinator(Arc::clone(&registry));

        coordinator.mark_sandbox_running(SandboxId::new()).await;

        assert_eq!(
            coordinator.running_registration(&SandboxId::new()),
            None,
            "an unacknowledged mark must not enrol anything"
        );
        assert_eq!(
            coordinator.release_stale_holdings().await,
            StaleReleaseOutcome::Fenced
        );
        assert_eq!(registry.release_calls(), 0);
    }

    /// Reconciliation prunes the registration map down to the node's live
    /// roster, so it empties again as soon as the sandbox goes away. The window
    /// is monotonic precisely so that cannot reopen it.
    #[tokio::test]
    async fn pruning_the_registrations_does_not_reopen_the_window() {
        let registry = Arc::new(CountingRegistry::always_failing());
        let coordinator = coordinator(Arc::clone(&registry));

        coordinator.mark_sandbox_running(SandboxId::new()).await;
        coordinator.retain_running_registrations(&HashSet::new());

        assert_eq!(
            coordinator.release_stale_holdings().await,
            StaleReleaseOutcome::Fenced
        );
        assert_eq!(registry.release_calls(), 0);
    }
}

#[cfg(test)]
pub(super) mod test_support {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;

    use crate::orchestrator::{
        BeganPause, HeldSandbox, MarkRunningOutcome, PausedRegistryError, PausedSandboxEntry,
        PausedSandboxRegistry, ReclaimedHoldings, RegistryResult, ReleasedHoldings, ResumeClaim,
    };
    use crate::snapshot::SnapshotId;
    use crate::types::SandboxId;

    /// A cluster-backed registry that counts what it is asked to do and can be
    /// told to fail a fixed number of times first.
    ///
    /// Only the two calls the takeover window turns on are interesting here;
    /// everything else answers the way the disabled registry does.
    pub(crate) struct CountingRegistry {
        release_calls: AtomicUsize,
        releases_to_fail: usize,
        mark_running_fails: bool,
        get_calls: AtomicUsize,
        get_answer: GetAnswer,
        claim_fails: bool,
        renew_calls: AtomicUsize,
        renewals_to_fail: usize,
    }

    /// What a programmed `get` should answer.
    pub(crate) enum GetAnswer {
        /// No row at all.
        Missing,
        /// A row naming this snapshot.
        Referencing(SnapshotId),
        /// The registry could not answer.
        Unreachable,
    }

    impl CountingRegistry {
        pub(crate) fn new(releases_to_fail: usize, mark_running_fails: bool) -> Self {
            Self {
                release_calls: AtomicUsize::new(0),
                releases_to_fail,
                mark_running_fails,
                get_calls: AtomicUsize::new(0),
                get_answer: GetAnswer::Missing,
                claim_fails: false,
                renew_calls: AtomicUsize::new(0),
                renewals_to_fail: 0,
            }
        }

        /// A registry nobody can reach: the shape of a controller mid-rollout.
        pub(crate) fn unreachable() -> Self {
            Self::unreachable_for(usize::MAX)
        }

        /// Unreachable for the first `renewals_to_fail` renewals, then back.
        pub(crate) fn unreachable_for(renewals_to_fail: usize) -> Self {
            Self {
                claim_fails: true,
                renewals_to_fail,
                get_answer: GetAnswer::Unreachable,
                ..Self::new(usize::MAX, true)
            }
        }

        /// Fails every release, forever.
        pub(crate) fn always_failing() -> Self {
            Self::new(usize::MAX, false)
        }

        pub(crate) fn answering(get_answer: GetAnswer) -> Self {
            Self {
                get_answer,
                ..Self::new(0, false)
            }
        }

        pub(crate) fn release_calls(&self) -> usize {
            self.release_calls.load(Ordering::SeqCst)
        }

        pub(crate) fn get_calls(&self) -> usize {
            self.get_calls.load(Ordering::SeqCst)
        }
    }

    fn unreachable_backend(operation: &'static str) -> PausedRegistryError {
        PausedRegistryError::Backend {
            operation,
            source: anyhow::anyhow!("registry is unreachable"),
        }
    }

    #[async_trait]
    impl PausedSandboxRegistry for CountingRegistry {
        async fn begin_pause(&self, _entry: &PausedSandboxEntry) -> RegistryResult<BeganPause> {
            Ok(BeganPause {
                generation: 0,
                previous_snapshot_id: None,
            })
        }

        async fn complete_pause(
            &self,
            _sandbox_id: &SandboxId,
            _generation: i64,
            _snapshot_id: &SnapshotId,
        ) -> RegistryResult<()> {
            Ok(())
        }

        async fn mark_local_only(
            &self,
            _sandbox_id: &SandboxId,
            _generation: i64,
        ) -> RegistryResult<()> {
            Ok(())
        }

        async fn get(&self, sandbox_id: &SandboxId) -> RegistryResult<Option<PausedSandboxEntry>> {
            self.get_calls.fetch_add(1, Ordering::SeqCst);

            match &self.get_answer {
                GetAnswer::Missing => Ok(None),
                GetAnswer::Unreachable => Err(unreachable_backend("get")),
                GetAnswer::Referencing(snapshot_id) => Ok(Some(PausedSandboxEntry {
                    sandbox_id: *sandbox_id,
                    cluster_id: uuid::Uuid::nil(),
                    state: crate::orchestrator::PausedRegistryState::Paused,
                    generation: 1,
                    origin_node_id: "node-a".to_string(),
                    claimed_by_node_id: None,
                    snapshot_id: Some(snapshot_id.clone()),
                    metadata: Some(crate::orchestrator::SandboxMetadata::default()),
                    paused_at: chrono::Utc::now(),
                    updated_at: chrono::Utc::now(),
                })),
            }
        }

        async fn get_many(
            &self,
            _sandbox_ids: &[SandboxId],
        ) -> RegistryResult<HashMap<SandboxId, PausedSandboxEntry>> {
            Ok(HashMap::new())
        }

        async fn claim_for_resume(
            &self,
            _sandbox_id: &SandboxId,
            _node_id: &str,
        ) -> RegistryResult<ResumeClaim> {
            if self.claim_fails {
                return Err(unreachable_backend("claim_for_resume"));
            }

            Ok(ResumeClaim::NotFound)
        }

        async fn release_claim(
            &self,
            _sandbox_id: &SandboxId,
            _generation: i64,
        ) -> RegistryResult<bool> {
            Ok(true)
        }

        async fn renew_lease(&self, _node_id: &str, _held: &[HeldSandbox]) -> RegistryResult<u64> {
            let seen = self.renew_calls.fetch_add(1, Ordering::SeqCst);
            if seen < self.renewals_to_fail {
                return Err(unreachable_backend("renew_lease"));
            }

            Ok(0)
        }

        async fn reclaim_expired_holdings(&self) -> RegistryResult<ReclaimedHoldings> {
            Ok(ReclaimedHoldings::default())
        }

        async fn mark_running(
            &self,
            _sandbox_id: &SandboxId,
            _node_id: &str,
        ) -> RegistryResult<MarkRunningOutcome> {
            if self.mark_running_fails {
                return Err(unreachable_backend("mark_running"));
            }

            Ok(MarkRunningOutcome::Adopted)
        }

        async fn release_node_holdings(&self, _node_id: &str) -> RegistryResult<ReleasedHoldings> {
            let seen = self.release_calls.fetch_add(1, Ordering::SeqCst);
            if seen < self.releases_to_fail {
                return Err(unreachable_backend("release_node_holdings"));
            }

            Ok(ReleasedHoldings::default())
        }

        async fn remove(&self, _sandbox_id: &SandboxId, _generation: i64) -> RegistryResult<bool> {
            Ok(true)
        }

        fn is_cluster_backed(&self) -> bool {
            true
        }
    }
}
