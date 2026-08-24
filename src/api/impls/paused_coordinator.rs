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
use std::time::SystemTime;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tracing::{debug, error, info, warn};

use crate::orchestrator::{
    MarkRunningOutcome, PauseOutcome, PausedRegistryState, PausedSandboxEntry,
    PausedSandboxPublisher, PausedSandboxRegistry, SandboxMetadata,
};
use crate::snapshot::{
    SnapshotId, SnapshotManager, SnapshotPublishMetadata, SnapshotPublishSource,
};
use crate::types::{ExecutionId, SandboxId};

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

/// What a pause left behind in the cluster registry.
///
/// # 🔴 Three answers, and the two that leave no row are not the same answer
///
/// A pause that could not reach the registry and a pause that had nothing to
/// register both end with no row, and for months they also ended with the same
/// silent `None` out of the same `?`. That is how `--role api` came to record
/// nothing at all without anybody noticing: every pause it performs takes the
/// second branch, which said nothing, and the empty table was indistinguishable
/// from a cluster where nobody had paused anything.
///
/// "Could not be reached" and "does not exist" have to be different answers
/// here, because they call for opposite responses from whoever is looking: the
/// first is an outage to chase, the second is the topology working as designed.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ClusterRecord {
    /// A row now describes this sandbox, under the node identity carried here.
    /// That identity is what the orchestrator stamps on the local record, and
    /// what reconciliation later compares the row against.
    Registered(String),
    /// No row, and why not.
    Unrecorded(Unrecorded),
}

/// Why a pause left the cluster registry untouched.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Unrecorded {
    /// The capture went into this process's own temporary artifacts, which are
    /// reclaimed with the paused state. There is no machine a row could point
    /// at, and there never will be: this is the sandbox that really is
    /// recoverable nowhere but inside this process.
    NoDurableCapture,
    /// The capture is durable, on the machine named here, and this half still
    /// cannot record it.
    ///
    /// 🔴 The `--role api` case, and the reason `paused_sandboxes` is empty on
    /// a split cluster. The bytes are on the node that ran the sandbox, which
    /// keeps its own record of them, so a row saying "parked on that node" is
    /// exactly what the registry is for — but it cannot be written under the
    /// identity this half would have to write it under. `begin_pause` names the
    /// machine whose disk holds the artifacts
    /// (`CentralPausedSandboxRegistry::begin_pause`), while the resume
    /// arbitration on this half reads a row naming any node but its own as a
    /// refusal (`arbitration` in `super::paused_recovery`, which answers
    /// `NotReady`/`Blocked` and becomes a 409). Writing the row today would
    /// therefore trade an empty registry for a resume path that refuses every
    /// paused sandbox it just recorded. See the report on this batch.
    HeldByAnotherMachine(String),
    /// The registry could not be reached. The sandbox is paused either way; what
    /// was lost is the cluster's knowledge of it.
    Unreachable,
}

impl Unrecorded {
    /// The label this reason is counted under, so an operator can tell an
    /// outage from the topology from a scrape rather than from a log line.
    fn reason(&self) -> &'static str {
        match self {
            Self::NoDurableCapture => "no_durable_capture",
            Self::HeldByAnotherMachine(_) => "held_by_another_machine",
            Self::Unreachable => "unreachable",
        }
    }
}

impl ClusterRecord {
    /// The identity the row was written under, or `None` when there is no row.
    ///
    /// 🔴 Both `Unrecorded` reasons collapse here, and only here. The
    /// orchestrator stamps the local record with this so reconciliation may
    /// later act on the registry's answers about it, and a stamp on a record
    /// the registry has never heard of is what makes reconciliation discard
    /// sandboxes it should not touch — so every reason that leaves no row has to
    /// leave no stamp either. The distinction between them is kept where it is
    /// useful, which is the log and the counter, not this return value.
    fn registered_as(self) -> Option<String> {
        match self {
            Self::Registered(node_id) => Some(node_id),
            Self::Unrecorded(_) => None,
        }
    }
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
    /// one. That is also the answer to "what if the row cannot be written": the
    /// pause stands, the loss is named, and nothing is retried inside the call —
    /// a pause that hangs waiting for a registry is a worse pause than one whose
    /// cluster record is missing and says so.
    ///
    /// Returns [`ClusterRecord`], not a bare `Option`, so that "no row" always
    /// arrives with the reason attached.
    async fn publish(&self, outcome: PauseOutcome) -> ClusterRecord {
        let sandbox_id = outcome.metadata.id;

        // 🔴 Asked before `publishable`, because it is the question that
        // separates the two ways a pause can offer nothing to commit here. The
        // capture may be sitting, durable, on the machine that took it.
        let holding_node = outcome
            .metadata
            .paused_state
            .as_ref()
            .and_then(|state| state.holding_node_id())
            .map(str::to_string);

        let publishable = match outcome.publishable {
            Some(publishable) => publishable,
            // The capture is on another machine, which keeps its own durable
            // record of it. Nothing to commit from here, and — for now — no row
            // either; `Unrecorded::HeldByAnotherMachine` carries why.
            None => {
                if let Some(holding_node) = holding_node {
                    return self
                        .unrecorded(sandbox_id, Unrecorded::HeldByAnotherMachine(holding_node));
                }

                // And the case the early return here was originally written
                // for, which is still a case: the pause wrote into
                // backend-managed temporaries that no other node could ever
                // read, and that are reclaimed with the paused state.
                return self.unrecorded(sandbox_id, Unrecorded::NoDurableCapture);
            }
        };

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
            // The run that produced this pause. `begin_pause` quotes it, and the
            // registry refuses the write if the row is fenced against a newer
            // one.
            execution_id: Some(outcome.metadata.execution_id),
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

                return self.unrecorded(sandbox_id, Unrecorded::Unreachable);
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

        ClusterRecord::Registered(self.node_id.clone())
    }

    /// Says, once and at the right volume, that a pause left the cluster
    /// registry untouched — and which of the reasons it was.
    ///
    /// 🔴 Every branch that leaves no row comes through here, so none of them
    /// can go back to being an unremarked `return`. The levels differ because
    /// the reasons do: an unreachable registry is a failure to chase, while the
    /// other two are the topology behaving as designed and would be noise as
    /// warnings on every pause.
    fn unrecorded(&self, sandbox_id: SandboxId, reason: Unrecorded) -> ClusterRecord {
        metrics::counter!(
            "agentenv_paused_registry_pause_unrecorded_total",
            "reason" => reason.reason()
        )
        .increment(1);

        match &reason {
            Unrecorded::Unreachable => {
                // Already warned at the call site with the error in hand; this
                // only counts it.
            }
            Unrecorded::HeldByAnotherMachine(holding_node) => info!(
                %sandbox_id,
                holding_node = %holding_node,
                "paused sandbox is parked on another machine and has no cluster record: this half \
                 cannot write a row naming a node it is not, so the sandbox is resumable only \
                 through that machine"
            ),
            Unrecorded::NoDurableCapture => debug!(
                %sandbox_id,
                "pause produced no capture that outlives this process; nothing to record cluster-wide"
            ),
        }

        ClusterRecord::Unrecorded(reason)
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
    pub async fn mark_sandbox_running(
        &self,
        sandbox_id: SandboxId,
        execution_id: ExecutionId,
        expires_at: Option<SystemTime>,
    ) {
        // Before the write: see `note_taking_sandbox_live`.
        self.note_taking_sandbox_live().await;

        let confirmed = match self
            .registry
            .mark_running(&sandbox_id, &self.node_id, execution_id, expires_at)
            .await
        {
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
        self.publish(outcome).await.registered_as()
    }

    async fn mark_running(
        &self,
        sandbox_id: SandboxId,
        execution_id: ExecutionId,
        expires_at: Option<SystemTime>,
    ) {
        self.mark_sandbox_running(sandbox_id, execution_id, expires_at)
            .await;
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
    use crate::orchestrator::{
        BeganPause, HeldSandbox, PausedRegistryError, ReclaimedHoldings, RegistryResult,
        ReleasedHoldings, ResumeClaim,
    };

    /// T-A1-9. 🔴 A committed snapshot must not carry the incarnation that
    /// produced it.
    ///
    /// A snapshot is a template: any node may launch it, any number of times,
    /// at any later moment. An incarnation baked into it would hand every
    /// sandbox ever created from it the same identity, and fencing would then
    /// be unable to tell any of them apart.
    ///
    /// Asserted through `Debug` because `SnapshotPublishMetadata` is built
    /// field by field and derives it: a field added to the struct shows up here
    /// whether or not anyone remembers to update this test.
    #[test]
    fn the_snapshot_a_sandbox_publishes_carries_no_execution() {
        let metadata = SandboxMetadata::default();
        let rendered = format!("{:?}", publish_metadata(&metadata));

        assert!(
            !rendered.contains(&metadata.execution_id.to_string()),
            "the published snapshot names the run that produced it: {rendered}"
        );
        assert!(
            !rendered.to_lowercase().contains("execution"),
            "the published snapshot has an incarnation-shaped field: {rendered}"
        );
    }

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
            execution_id: Some(ExecutionId::new()),
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

        coordinator
            .mark_sandbox_running(SandboxId::new(), ExecutionId::new(), None)
            .await;

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

        coordinator
            .mark_sandbox_running(SandboxId::new(), ExecutionId::new(), None)
            .await;
        coordinator.retain_running_registrations(&HashSet::new());

        assert_eq!(
            coordinator.release_stale_holdings().await,
            StaleReleaseOutcome::Fenced
        );
        assert_eq!(registry.release_calls(), 0);
    }

    /// A registry that keeps the rows it is told to write.
    ///
    /// 🔴 The only fake in this module that can answer "is there a row".
    /// `CountingRegistry` counts calls, and a call count cannot tell a pause
    /// that left the cluster knowing about a sandbox from one that merely tried
    /// — which is the entire question on this path.
    #[derive(Default)]
    struct RecordingRegistry {
        rows: Mutex<HashMap<SandboxId, PausedSandboxEntry>>,
        begin_pause_fails: bool,
    }

    impl RecordingRegistry {
        /// A registry nobody can reach on the one call that creates a row.
        fn unreachable() -> Self {
            Self {
                begin_pause_fails: true,
                ..Self::default()
            }
        }

        fn row(&self, sandbox_id: &SandboxId) -> Option<PausedSandboxEntry> {
            self.rows.lock().unwrap().get(sandbox_id).cloned()
        }

        fn row_count(&self) -> usize {
            self.rows.lock().unwrap().len()
        }
    }

    #[async_trait]
    impl PausedSandboxRegistry for RecordingRegistry {
        async fn begin_pause(&self, entry: &PausedSandboxEntry) -> RegistryResult<BeganPause> {
            if self.begin_pause_fails {
                return Err(PausedRegistryError::Backend {
                    operation: "begin_pause",
                    source: anyhow::anyhow!("registry is unreachable"),
                });
            }

            let mut row = entry.clone();
            row.generation = 1;
            row.state = PausedRegistryState::Publishing;
            self.rows.lock().unwrap().insert(entry.sandbox_id, row);

            Ok(BeganPause {
                generation: 1,
                previous_snapshot_id: None,
            })
        }

        async fn complete_pause(
            &self,
            sandbox_id: &SandboxId,
            generation: i64,
            snapshot_id: &SnapshotId,
        ) -> RegistryResult<()> {
            if let Some(row) = self.rows.lock().unwrap().get_mut(sandbox_id) {
                if row.generation == generation {
                    row.state = PausedRegistryState::Paused;
                    row.snapshot_id = Some(snapshot_id.clone());
                }
            }

            Ok(())
        }

        async fn mark_local_only(
            &self,
            sandbox_id: &SandboxId,
            generation: i64,
        ) -> RegistryResult<()> {
            if let Some(row) = self.rows.lock().unwrap().get_mut(sandbox_id) {
                if row.generation == generation {
                    row.state = PausedRegistryState::LocalOnly;
                }
            }

            Ok(())
        }

        async fn get(&self, sandbox_id: &SandboxId) -> RegistryResult<Option<PausedSandboxEntry>> {
            Ok(self.row(sandbox_id))
        }

        async fn get_many(
            &self,
            sandbox_ids: &[SandboxId],
        ) -> RegistryResult<HashMap<SandboxId, PausedSandboxEntry>> {
            let rows = self.rows.lock().unwrap();

            Ok(sandbox_ids
                .iter()
                .filter_map(|id| rows.get(id).map(|row| (*id, row.clone())))
                .collect())
        }

        async fn claim_for_resume(
            &self,
            _sandbox_id: &SandboxId,
            _node_id: &str,
            _execution_id: ExecutionId,
        ) -> RegistryResult<ResumeClaim> {
            Ok(ResumeClaim::NotFound)
        }

        async fn release_claim(
            &self,
            _sandbox_id: &SandboxId,
            _generation: i64,
        ) -> RegistryResult<bool> {
            Ok(false)
        }

        async fn renew_lease(&self, _node_id: &str, _held: &[HeldSandbox]) -> RegistryResult<u64> {
            Ok(0)
        }

        async fn reclaim_expired_holdings(&self) -> RegistryResult<ReclaimedHoldings> {
            Ok(ReclaimedHoldings::default())
        }

        async fn mark_running(
            &self,
            _sandbox_id: &SandboxId,
            _node_id: &str,
            _execution_id: ExecutionId,
            _expires_at: Option<SystemTime>,
        ) -> RegistryResult<MarkRunningOutcome> {
            Ok(MarkRunningOutcome::Untracked)
        }

        async fn release_node_holdings(&self, _node_id: &str) -> RegistryResult<ReleasedHoldings> {
            Ok(ReleasedHoldings::default())
        }

        async fn remove(&self, sandbox_id: &SandboxId, generation: i64) -> RegistryResult<bool> {
            let mut rows = self.rows.lock().unwrap();
            match rows.get(sandbox_id) {
                Some(row) if row.generation == generation => {
                    rows.remove(sandbox_id);
                    Ok(true)
                }
                _ => Ok(false),
            }
        }

        fn is_cluster_backed(&self) -> bool {
            true
        }
    }

    /// A paused state that says where its bytes are, and nothing else.
    #[derive(Debug)]
    struct CapturedOn(Option<String>);

    impl crate::sandbox::PausedSandboxState for CapturedOn {
        fn encode(&self) -> anyhow::Result<serde_json::Value> {
            Ok(serde_json::Value::Null)
        }

        fn runtime_artifacts(&self) -> crate::sandbox::RuntimeArtifactSet {
            crate::sandbox::RuntimeArtifactSet::empty()
        }

        fn holding_node_id(&self) -> Option<&str> {
            self.0.as_deref()
        }
    }

    const THIS_REPLICA: &str = "api-replica-1";

    fn recording_coordinator(registry: Arc<RecordingRegistry>) -> PausedSandboxCoordinator {
        PausedSandboxCoordinator::new(
            registry,
            Arc::new(crate::snapshot::mock::mock_snapshot_manager()),
            THIS_REPLICA.to_string(),
        )
    }

    /// One pause, described by the only two things this decision reads: whether
    /// a capture can be committed from here, and which machine the bytes are on.
    fn pause_outcome(
        sandbox_id: SandboxId,
        committable: bool,
        holding_node: Option<&str>,
    ) -> PauseOutcome {
        let mut metadata = SandboxMetadata {
            id: sandbox_id,
            ..SandboxMetadata::default()
        };
        metadata.paused_state = Some(Arc::new(CapturedOn(holding_node.map(str::to_string))));

        PauseOutcome {
            metadata,
            // The concrete capture is never read on the paths under test: the
            // mock snapshot manager refuses to stage anything it did not
            // produce, which lands on the `mark_local_only` arm and leaves the
            // row where these tests can see it.
            publishable: committable.then(|| crate::sandbox::CapturedSandboxSnapshot::new(())),
        }
    }

    /// 🔴 Both halves are one test, and deliberately. "The registry has a row"
    /// is only evidence if something in the same run, through the same fake,
    /// leaves it without one — otherwise a green assertion proves the fixture
    /// wrote a row, not that the code under test did. The two pauses differ in
    /// exactly one value: whether the capture can be committed from here.
    #[tokio::test]
    async fn only_the_pause_this_half_can_commit_leaves_a_row() {
        let registry = Arc::new(RecordingRegistry::default());
        let coordinator = recording_coordinator(Arc::clone(&registry));

        let committable = SandboxId::new();
        let elsewhere = SandboxId::new();

        let committed = coordinator
            .publish(pause_outcome(committable, true, None))
            .await;
        let parked = coordinator
            .publish(pause_outcome(elsewhere, false, Some("node-7")))
            .await;

        assert_eq!(
            committed,
            ClusterRecord::Registered(THIS_REPLICA.to_string()),
            "a capture this half can commit must be registered under this identity"
        );
        assert!(
            registry.row(&committable).is_some(),
            "a capture this half can commit must leave the cluster a row"
        );

        assert_eq!(
            parked,
            ClusterRecord::Unrecorded(Unrecorded::HeldByAnotherMachine("node-7".to_string())),
            "a capture on another machine must say so rather than report nothing"
        );
        assert!(
            registry.row(&elsewhere).is_none(),
            "a capture on another machine leaves no row on this half yet"
        );
        assert_eq!(
            registry.row_count(),
            1,
            "exactly one of the two pauses reached the registry"
        );
    }

    /// 🔴 The distinction the empty table on 203/204 turned on. A pause that
    /// could not reach the registry and a pause that had nothing to put in it
    /// both end with no row; answering both with the same silent nothing is
    /// what made "the api half records nothing" look like "nobody has paused
    /// anything".
    #[tokio::test]
    async fn out_of_reach_and_nothing_to_record_are_different_answers() {
        let unreachable_registry = Arc::new(RecordingRegistry::unreachable());
        let unreachable = recording_coordinator(Arc::clone(&unreachable_registry));
        let reachable_registry = Arc::new(RecordingRegistry::default());
        let reachable = recording_coordinator(Arc::clone(&reachable_registry));

        let out_of_reach = unreachable
            .publish(pause_outcome(SandboxId::new(), true, None))
            .await;
        let nothing_to_record = reachable
            .publish(pause_outcome(SandboxId::new(), false, None))
            .await;

        assert_eq!(
            out_of_reach,
            ClusterRecord::Unrecorded(Unrecorded::Unreachable)
        );
        assert_eq!(
            nothing_to_record,
            ClusterRecord::Unrecorded(Unrecorded::NoDurableCapture)
        );
        assert_ne!(
            out_of_reach, nothing_to_record,
            "a registry that could not be reached must not read as a pause with nothing to record"
        );
        assert_eq!(
            unreachable_registry.row_count(),
            0,
            "a registry that refused the write holds no row"
        );
        assert_eq!(
            reachable_registry.row_count(),
            0,
            "a pause with nothing durable behind it writes no row to a registry that would take one"
        );
    }

    /// The machine comes off the paused state, not off the process reporting
    /// it. The two differ on the api half — that is the whole point of the
    /// split — and a reason naming this replica would send whoever reads it
    /// looking for the bytes on a Pod that has never held any.
    #[tokio::test]
    async fn a_parked_pause_names_the_machine_the_bytes_are_on() {
        let registry = Arc::new(RecordingRegistry::default());
        let coordinator = recording_coordinator(Arc::clone(&registry));

        let record = coordinator
            .publish(pause_outcome(SandboxId::new(), false, Some("node-203")))
            .await;

        assert_eq!(
            record,
            ClusterRecord::Unrecorded(Unrecorded::HeldByAnotherMachine("node-203".to_string()))
        );
        assert_ne!(
            record,
            ClusterRecord::Unrecorded(Unrecorded::HeldByAnotherMachine(THIS_REPLICA.to_string())),
            "the machine holding the bytes is not the replica that drove the pause"
        );
    }

    /// 🔴 What the orchestrator does with each answer, which is the one place
    /// the three collapse back into two. Only a row may be stamped onto the
    /// local record: that stamp is reconciliation's licence to act on what the
    /// registry says about the sandbox, and a stamp for a sandbox the registry
    /// has never heard of is how reconciliation comes to discard records it
    /// must not touch.
    #[tokio::test]
    async fn only_a_written_row_is_handed_back_for_the_local_record() {
        let registry = Arc::new(RecordingRegistry::default());
        let coordinator = recording_coordinator(Arc::clone(&registry));

        let registered = coordinator
            .publish_paused(pause_outcome(SandboxId::new(), true, None))
            .await;
        let parked = coordinator
            .publish_paused(pause_outcome(SandboxId::new(), false, Some("node-7")))
            .await;
        let nothing = coordinator
            .publish_paused(pause_outcome(SandboxId::new(), false, None))
            .await;

        assert_eq!(registered, Some(THIS_REPLICA.to_string()));
        assert_eq!(parked, None);
        assert_eq!(nothing, None);

        let unreachable_registry = Arc::new(RecordingRegistry::unreachable());
        let unreachable = recording_coordinator(Arc::clone(&unreachable_registry));
        assert_eq!(
            unreachable
                .publish_paused(pause_outcome(SandboxId::new(), true, None))
                .await,
            None,
            "a registry that could not be reached must not stamp the local record either"
        );
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
    use crate::types::{ExecutionId, SandboxId};

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
        remove_calls: AtomicUsize,
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
                remove_calls: AtomicUsize::new(0),
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

        /// How many rows this registry was told to drop.
        ///
        /// 🔴 The destructive one. A row dropped here is the cluster's only
        /// record that a paused sandbox exists, so a test about "was the row
        /// thrown away" has to be able to see it.
        pub(crate) fn remove_calls(&self) -> usize {
            self.remove_calls.load(Ordering::SeqCst)
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
                    execution_id: Some(ExecutionId::new()),
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
            _execution_id: ExecutionId,
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
            _execution_id: ExecutionId,
            _expires_at: Option<std::time::SystemTime>,
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
            self.remove_calls.fetch_add(1, Ordering::SeqCst);
            Ok(true)
        }

        fn is_cluster_backed(&self) -> bool {
            true
        }
    }
}
