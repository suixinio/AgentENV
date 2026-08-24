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
use crate::snapshot::{SnapshotId, SnapshotManager, SnapshotPublishMetadata};
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
    ///
    /// 🔴 The same string the row's `origin_node_id` was written with, and it
    /// has to be read from the same variable rather than re-derived. The stamp
    /// is compared against that column by field equality
    /// (`supersession` in `super::paused_recovery`: `entry.origin_node_id !=
    /// *node_id` means "another node has paused this since, discard the local
    /// copy"). Two expressions that happen to agree today would silently become
    /// a rule that discards every local paused record the first time they stop
    /// agreeing — which is exactly what a process whose own name is not the
    /// name on the row would do.
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
    /// The capture is durable, on the machine named here, and this pause has
    /// nothing to commit from here.
    ///
    /// 🔴 Corrected. This variant used to say that the row could not be
    /// written because `begin_pause` "names the machine whose disk holds the
    /// artifacts" and this half is not that machine. Both halves of that were
    /// wrong. `CentralPausedSandboxRegistry::begin_pause` names whatever the
    /// caller put in `PausedSandboxEntry::origin_node_id` — it states the
    /// identity, it does not discover it — so the constraint was never "cannot
    /// write", it was "would write the wrong name". And it did: the moment a
    /// node started staging captures the early return here stopped being
    /// reached, and every row the api half wrote carried the api Pod's name as
    /// the machine holding the bytes. See [`PausedSandboxCoordinator::publish`]
    /// for what that name costs a resume.
    ///
    /// What is left is the narrow case that was always the real one: the pause
    /// produced no capture *to commit*, because the pause did not happen on
    /// this call. A sandbox that was already paused, or a pause this call
    /// joined, answers with no `publishable` — the capture belongs to the pause
    /// that made it, and so does the row. Calling `begin_pause` again here
    /// would take that row's completed pause back to `publishing` under a fresh
    /// generation that nothing is going to complete.
    HeldByAnotherMachine(String),
    /// There is no cluster registry, so there is nothing a published snapshot
    /// could be referenced by — and nothing was published.
    ///
    /// 🔴 The `--role node` case, and `--role all` with
    /// `paused_registry.backend = "local"`. Both wire in
    /// [`DisabledPausedSandboxRegistry`](crate::orchestrator::DisabledPausedSandboxRegistry),
    /// whose `begin_pause` and `complete_pause` are no-ops — but the
    /// `publish_captured` between them was not, and uploaded the whole capture
    /// to the shared repository on every pause. Nothing could ever reach it:
    /// `claim_for_resume` answers `NotFound`, so no resume resolves it, and
    /// `forget_sandbox` reads `get` -> `None` and returns before the delete, so
    /// deleting the sandbox never collected it either. One orphaned snapshot
    /// per pause, growing for as long as the node ran.
    NoClusterRegistry,
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
            Self::NoClusterRegistry => "no_cluster_registry",
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
            // The pause produced nothing to commit, which on this arm means the
            // pause did not happen on this call — the sandbox was already
            // paused, or this call joined one in flight. The capture, and the
            // row, belong to the pause that made them.
            // `Unrecorded::HeldByAnotherMachine` carries which machine that was.
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

        // 🔴 Before the upload, not after it, and this is the only line that
        // stops it. Everything below against a registry that tracks nothing is
        // a no-op — `begin_pause` returns generation 0, `complete_pause`
        // returns `Ok(())` — except the call in the middle, which is not:
        // `publish_captured` writes the whole capture into the shared
        // repository and commits a catalog row for it. Nothing ever reaches
        // that row. `claim_for_resume` answers `NotFound`, so no resume
        // resolves it; `forget_sandbox` reads `get` -> `None` and returns
        // before the delete, so deleting the sandbox never collects it. One
        // orphaned snapshot per pause, for as long as the process runs.
        //
        // 🔴 Skipping it takes nothing away from this pause. The origin node
        // reopens a paused sandbox from its own persisted record
        // (`Orchestrator::paused_state_for_resume` asks the store for the
        // handle, never the repository), and the one path that does read a
        // published pause snapshot — `resume_from_registry` — starts from a
        // claim only a cluster-backed registry can grant. The capture dropped
        // here owns no directory either: a pause's publishable capture is
        // `FirecrackerCapturedSnapshot::in_caller_owned_dir`, whose artifacts
        // belong to the persister and outlive it.
        if !self.registry.is_cluster_backed() {
            return self.unrecorded(sandbox_id, Unrecorded::NoClusterRegistry);
        }

        // 🔴 The machine whose disk the bytes are on, which is what
        // `origin_node_id` means on this row — not the process that decided the
        // pause. The two are the same on every role that runs its own
        // sandboxes, and `holding_node` is `None` there precisely because a
        // local capture has nothing to say about a machine other than this one
        // (`PausedSandboxState::holding_node_id`). They part on `--role api`,
        // and the wrong one of them costs the sandbox:
        //
        // - `publishing` and `local_only` have no copy in shared storage, so
        //   the scheduler pins them to the named node and refuses anything else
        //   (`lookup.go`, `SANDBOX_LOCATION_PINNED`). Named after an api Pod,
        //   the pin resolves to a process that does not heartbeat — this half
        //   reports no machine because it is not one — and the lookup is
        //   answered `FailedPrecondition: node is not reporting`, on every node
        //   there is.
        //
        //   🔴 What keeps that off most resumes is not this row: `lookup.go`
        //   consults the holding node's own heartbeat roster first, and reaches
        //   the registry only when no roster covers the sandbox. So the wrong
        //   name here is invisible right up until the moment the registry is
        //   the only thing left — a node that has just rolled, a roster gone
        //   stale, or a sandbox being recovered onto a different machine, which
        //   is the case this table exists for.
        // - `paused` has a copy in shared storage, so the name is only a
        //   locality preference — but it is the preference that decides whether
        //   a resume reuses the layers already on disk or pulls the whole
        //   snapshot back out of object storage.
        //
        // 🔴 Stated once and used twice. See [`ClusterRecord::Registered`] on
        // why the value handed back has to be this same variable.
        let origin_node_id = holding_node.unwrap_or_else(|| self.node_id.clone());

        let entry = PausedSandboxEntry {
            sandbox_id,
            // Filled in by the registry from its own configured cluster.
            cluster_id: uuid::Uuid::nil(),
            state: PausedRegistryState::Publishing,
            generation: 0,
            origin_node_id: origin_node_id.clone(),
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

        ClusterRecord::Registered(origin_node_id)
    }

    /// Says, once and at the right volume, that a pause left the cluster
    /// registry untouched — and which of the reasons it was.
    ///
    /// 🔴 Every branch that leaves no row comes through here, so none of them
    /// can go back to being an unremarked `return`. The levels differ because
    /// the reasons do: an unreachable registry is a failure to chase, while the
    /// rest are the topology behaving as designed and would be noise as
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
                "this pause produced nothing to commit: the sandbox was already parked on that \
                 machine, and the row belongs to the pause that put it there"
            ),
            Unrecorded::NoClusterRegistry => info!(
                %sandbox_id,
                "no cluster registry is configured, so this pause published no snapshot: the \
                 sandbox is resumable from this node's own persisted record and nowhere else. \
                 Publishing one would have uploaded a capture nothing can reference and nothing \
                 deletes"
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

    /// Records that the sandbox is live on the machine that actually brought
    /// it up.
    ///
    /// Called on both resume paths — the local fast path and a cross-node
    /// restore — because both end with a sandbox the registry still describes
    /// as parked somewhere else being live on some machine again.
    ///
    /// # 🔴 Two identities out, one `self.node_id` used only once
    ///
    /// The registry write takes a claimant and a holder — see
    /// [`PausedSandboxRegistry::mark_running`]'s doc for why they must never be
    /// the same *parameter*, only sometimes the same *value*. The claimant is
    /// always `self.node_id`, unconditionally: it is the CAS guard, and it must
    /// be the exact identity `ApiImpl::arbitrate_resume` claimed under for this
    /// resume, which is that method's own `self.paused.node_id()` — never a
    /// value read out of shared state, for the same reason arbitration's
    /// self-comparison needs a process-unique identity.
    ///
    /// `holding_node_id` is the holder — that machine, as read off the backend
    /// that just started it, see
    /// [`SandboxBackend::holding_node_id`][crate::sandbox::SandboxBackend::holding_node_id].
    /// `None` covers every backend that runs the VM in this same process,
    /// which is the correct, common answer and the reason the fallback below
    /// is silent rather than a warning: on the roles this holds for, `None` is
    /// not a degraded case, it is *the* case, on every resume they ever serve.
    /// It mirrors exactly the fallback [`publish`](Self::publish) uses for the
    /// paused half of the same question, for the same reason — see that
    /// method's note on why a role that runs its own sandboxes answering
    /// `None` here is not the failure this fallback exists to paper over.
    ///
    /// A confirmed write is also what enrols the sandbox in
    /// [`running_registration`](Self::running_registration) — under the
    /// **holder**, not the claimant. `origin_node_id` is what the row now
    /// holds and what reconciliation (`running_supersession`) later compares a
    /// registration against; recording the claimant there would make every
    /// reconciliation pass on a cross-node resume read `entry.origin_node_id
    /// != registered_as` as true and tear the sandbox down as "held
    /// elsewhere" — the opposite of the fix this exists to make durable. Only
    /// a confirmed write registers anything: an unacknowledged one is no
    /// evidence that the row says what was just written, and reconciliation
    /// acts on that evidence. A refusal or an error leaves any earlier
    /// confirmation standing — it remains true that the cluster once named
    /// that identity the holder, which is precisely the premise reconciliation
    /// needs to notice that it no longer does.
    pub async fn mark_sandbox_running(
        &self,
        sandbox_id: SandboxId,
        execution_id: ExecutionId,
        expires_at: Option<SystemTime>,
        holding_node_id: Option<String>,
    ) {
        // Before the write: see `note_taking_sandbox_live`.
        self.note_taking_sandbox_live().await;

        let holder = holding_node_id.unwrap_or_else(|| self.node_id.clone());

        let confirmed = match self
            .registry
            .mark_running(
                &sandbox_id,
                &self.node_id,
                &holder,
                execution_id,
                expires_at,
            )
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
            .observe(sandbox_id, &holder, confirmed);
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
    ///
    /// `holding_node_id` is the real machine this delete just stopped the
    /// sandbox on — see [`PausedSandboxPublisher::forget`]'s doc for where it
    /// comes from and why `self.node_id` (this process's own identity, the
    /// claimant) is never a substitute for it here.
    pub async fn forget_sandbox(&self, sandbox_id: SandboxId, holding_node_id: Option<String>) {
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

        // The real machine this delete just stopped the sandbox on, falling
        // back to this process's own identity exactly the way
        // `mark_sandbox_running` does for the mirror-image question — correct
        // on every role that runs the VM in this same process, and the only
        // answer a caller with no better one can give.
        let holder_node_id = holding_node_id
            .as_deref()
            .unwrap_or(self.node_id.as_str());

        if let Some(holder) = live_elsewhere(&entry, &self.node_id, holder_node_id) {
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

    /// 🔴 The same predicate [`PausedSandboxCoordinator::publish`] gates the
    /// upload on, read one step earlier. Asking it here and asking it there
    /// have to give the same answer, or a pause spends a durable write on a
    /// capture the very next check throws away — which is the whole reason the
    /// question is asked twice rather than the answer being carried.
    fn wants_publishable_capture(&self) -> bool {
        self.registry.is_cluster_backed()
    }

    async fn mark_running(
        &self,
        sandbox_id: SandboxId,
        execution_id: ExecutionId,
        expires_at: Option<SystemTime>,
        holding_node_id: Option<String>,
    ) {
        self.mark_sandbox_running(sandbox_id, execution_id, expires_at, holding_node_id)
            .await;
    }

    async fn forget(&self, sandbox_id: SandboxId, holding_node_id: Option<String>) {
        self.forget_sandbox(sandbox_id, holding_node_id).await;
    }
}

/// Names the node running the sandbox, when that is someone other than us.
///
/// A row in any parked state — paused, publishing, local-only — is fair game to
/// clear from any node: the delete then completes on whichever node holds the
/// artifacts, when it next reconciles. A sandbox that is *live* elsewhere is
/// not, because nothing this node does will stop it.
///
/// # 🔴 Two identities in, deliberately never the same argument
///
/// Mirrors the claimant/holder split [`PausedSandboxCoordinator::mark_sandbox_running`]
/// documents for the opposite question. `claimant_node_id` is always
/// `self.node_id` — this process's own identity — and is what a `Resuming`
/// row's `claimed_by_node_id` is compared against, because that field is
/// itself always a claimant (the identity a resume claimed under, written by
/// `mark_running`'s CAS guard — see that method's doc). `holder_node_id` is
/// the real machine, and is what a `Running` row's `origin_node_id` is
/// compared against, because `origin_node_id` has been a real machine, never
/// a claimant, since 259d0de split `mark_running`'s write in two.
///
/// Before that split — and still, on a role that runs its own sandboxes —
/// `self.node_id` was itself a real machine, so passing it for both
/// arguments reproduces the old behavior exactly. On the api half, it is a
/// Pod identity that no `origin_node_id` will ever equal, and comparing a
/// `Running` row against it made this function report every such row as live
/// elsewhere forever — the delete kept succeeding, but the row and the
/// snapshot behind it were never cleared. `forget_sandbox` computes
/// `holder_node_id` with the same fallback `mark_sandbox_running` uses for
/// the mirror-image write, so a role with nothing better to report still
/// gets its own identity, and a role that knows the real machine — because
/// the backend it just stopped told it — gets that instead.
fn live_elsewhere(
    entry: &PausedSandboxEntry,
    claimant_node_id: &str,
    holder_node_id: &str,
) -> Option<String> {
    match entry.state {
        PausedRegistryState::Running if entry.origin_node_id != holder_node_id => {
            Some(entry.origin_node_id.clone())
        }
        PausedRegistryState::Resuming => entry
            .claimed_by_node_id
            .as_ref()
            .filter(|claimer| *claimer != claimant_node_id)
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
///
/// 🔴 Unnamed, always. This snapshot is an implementation detail of pause, not
/// something a user asked to be able to launch by name — and the alias is the
/// one field of the publish metadata that a *remote* staging adopts from this
/// half rather than deciding for itself, so passing one here would bind a name
/// to a pause's private snapshot on a cluster as readily as on this node.
fn publish_metadata(metadata: &SandboxMetadata) -> SnapshotPublishMetadata {
    crate::orchestrator::capture_publish_metadata(metadata, None)
}

#[cfg(test)]
mod tests {
    use super::test_support::{CountingRegistry, GetAnswer};
    use super::*;
    use crate::orchestrator::{
        BeganPause, HeldSandbox, PausedRegistryError, ReclaimedHoldings, RegistryResult,
        ReleasedHoldings, ResumeClaim,
    };
    use crate::sandbox::{
        CapturedSandboxSnapshot, FirecrackerCapturedSnapshot, FirecrackerSnapshotManifest,
    };
    use crate::snapshot::repository::{
        ImportedSnapshotArtifacts, SnapshotArtifactStore, SnapshotCatalog, SnapshotCommit,
        SnapshotListFilter, SnapshotRepository, StartedBuild,
    };
    use crate::snapshot::{
        PersistedDiskImagePublication, RepositoryResult, SnapshotRecord, TemplateBuildErrorReason,
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
    ///
    /// The claimant argument is deliberately not `"node-a"` here: a `Running`
    /// row is decided on the holder alone, and giving the claimant a value
    /// that would also make the naive (pre-split) comparison pass — while the
    /// holder is the one that actually differs from the row — is what proves
    /// this branch is reading `holder_node_id`, not `claimant_node_id`.
    #[test]
    fn a_sandbox_running_on_another_node_is_not_forgotten() {
        assert_eq!(
            live_elsewhere(
                &entry(PausedRegistryState::Running, "node-b", None),
                "node-b",
                "node-a",
            ),
            Some("node-b".to_string())
        );
    }

    /// Same for a resume in flight somewhere else. Mirrors the test above:
    /// the holder argument is deliberately the value that would make a
    /// `Running`-style comparison pass, to prove the `Resuming` branch reads
    /// `claimant_node_id` and never falls back to `holder_node_id`.
    #[test]
    fn a_sandbox_claimed_by_another_node_is_not_forgotten() {
        assert_eq!(
            live_elsewhere(
                &entry(PausedRegistryState::Resuming, "node-a", Some("node-b")),
                "node-a",
                "node-b",
            ),
            Some("node-b".to_string())
        );
    }

    /// The ordinary delete: this node holds it, so it clears its own record.
    #[test]
    fn our_own_sandbox_is_forgotten() {
        assert!(live_elsewhere(
            &entry(PausedRegistryState::Running, "node-a", None),
            "node-a",
            "node-a",
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
                live_elsewhere(&entry(state, "node-b", None), "node-a", "node-a").is_none(),
                "{state:?} should be clearable from another node"
            );
        }
    }

    /// 🔴 The exact split-topology bug found on the dev cluster after 259d0de
    /// split `mark_running`'s write: `origin_node_id` has been a real machine
    /// ever since, but this comparison was still made against the *claimant*
    /// — `self.node_id`, which on the api half is the api Pod's own identity
    /// and never equals a real node's name. Every delete of a resumed
    /// sandbox therefore found its `Running` row "live elsewhere" forever:
    /// the delete itself kept succeeding, but the registry row and the
    /// snapshot behind it leaked on every single one. The holder argument —
    /// the real machine this delete's own handle just reported stopping —
    /// is what has to be compared, not the claimant.
    #[test]
    fn a_sandbox_this_delete_just_stopped_is_forgotten_even_though_the_claimant_is_never_a_real_node(
    ) {
        assert!(
            live_elsewhere(
                &entry(PausedRegistryState::Running, "aenv-master-01", None),
                // The api replica's own pod identity: structurally never
                // equal to any real node's name.
                "aenv-api-6c9f8d59b4-x7z2q",
                // What this delete's handle reported holding it on.
                "aenv-master-01",
            )
            .is_none(),
            "the row names the machine this delete just stopped it on; it must be forgotten"
        );
    }

    /// 🔴 The end-to-end version of the two tests above: proves the row is
    /// actually gone from the registry after a real `forget_sandbox` call,
    /// not merely that the pure `live_elsewhere` decision returned `None`.
    ///
    /// This is the shape of assertion the dev-cluster leak needed and did not
    /// have: a test that only checked `delete_sandbox`'s `Ok(())` return
    /// would have passed throughout the whole incident, because the delete
    /// itself never failed — only the registry row and the snapshot behind
    /// it were silently left behind on every single one.
    #[tokio::test]
    async fn forget_sandbox_clears_the_row_the_real_holder_just_stopped_it_on() {
        let sandbox_id = SandboxId::new();
        let mut seeded_row = entry(PausedRegistryState::Running, "real-node-1", None);
        seeded_row.sandbox_id = sandbox_id;
        // Keeps this test to the registry row alone; snapshot deletion has
        // its own coverage above.
        seeded_row.snapshot_id = None;
        let registry = Arc::new(RecordingRegistry {
            rows: Mutex::new(HashMap::from([(sandbox_id, seeded_row)])),
            ..RecordingRegistry::default()
        });
        let coordinator = recording_coordinator(Arc::clone(&registry));

        // THIS_REPLICA ("api-replica-1") is the coordinator's own claimant
        // identity, and structurally never equals a real node's name — the
        // exact split-topology shape that leaked every row on the dev
        // cluster. `holding_node_id` is what a real delete threads through
        // from the handle it just stopped, naming the same machine the row
        // already does.
        coordinator
            .forget_sandbox(sandbox_id, Some("real-node-1".to_string()))
            .await;

        assert!(
            registry.row(&sandbox_id).is_none(),
            "the registry row must actually be gone, not just have `forget_sandbox` return"
        );
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
            .mark_sandbox_running(SandboxId::new(), ExecutionId::new(), None, None)
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
            .mark_sandbox_running(SandboxId::new(), ExecutionId::new(), None, None)
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
    struct RecordingRegistry {
        rows: Mutex<HashMap<SandboxId, PausedSandboxEntry>>,
        begin_pause_fails: bool,
        /// 🔴 The one value the publish decision turns on, and therefore the
        /// only value the paired tests below are allowed to differ in. Every
        /// other answer this fake gives is identical either way, so a
        /// difference in what the coordinator did cannot come from anywhere
        /// else.
        cluster_backed: bool,
    }

    impl Default for RecordingRegistry {
        fn default() -> Self {
            Self {
                rows: Mutex::default(),
                begin_pause_fails: false,
                cluster_backed: true,
            }
        }
    }

    impl RecordingRegistry {
        /// A registry nobody can reach on the one call that creates a row.
        fn unreachable() -> Self {
            Self {
                begin_pause_fails: true,
                ..Self::default()
            }
        }

        /// The registry a node-local deployment gets: it answers every call
        /// exactly as the cluster-backed one does, and tracks nothing
        /// cluster-wide.
        fn node_local() -> Self {
            Self {
                cluster_backed: false,
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

        /// Mirrors `markRunningFencedSQL`'s three branches closely enough to
        /// give the guard/write split real discriminating power in a Rust
        /// test: `node_id` (claimant) gates every branch exactly as the real
        /// guard does, `holder_node_id` only ever lands in `origin_node_id`
        /// (branch ③ excepted, matching the real statement — see that
        /// statement's own note on why). A fake that always adopted, the way
        /// this one did before, cannot fail the way a0487f0 failed: quoting
        /// the holder where the claimant belongs would still succeed here,
        /// which is exactly the shape of bug this exists to catch.
        async fn mark_running(
            &self,
            sandbox_id: &SandboxId,
            node_id: &str,
            holder_node_id: &str,
            _execution_id: ExecutionId,
            _expires_at: Option<SystemTime>,
        ) -> RegistryResult<MarkRunningOutcome> {
            let mut rows = self.rows.lock().unwrap();
            let Some(row) = rows.get_mut(sandbox_id) else {
                return Ok(MarkRunningOutcome::Untracked);
            };

            let eligible = match row.state {
                // ① cross-node: this write's claimant must be the one the
                // claim was taken under.
                PausedRegistryState::Resuming => row.claimed_by_node_id.as_deref() == Some(node_id),
                // ② local reopen: no claim was ever taken, so the claimant
                // must already be the row's origin.
                PausedRegistryState::Paused
                | PausedRegistryState::Publishing
                | PausedRegistryState::LocalOnly => {
                    row.origin_node_id == node_id && row.claimed_by_node_id.is_none()
                }
                // ③ a retried write: compared against the holder, since that
                // is what branch ① or ② already wrote into origin_node_id —
                // see markRunningFencedSQL's note on why this is the one
                // branch that reads holder_node_id instead of node_id.
                PausedRegistryState::Running => row.origin_node_id == holder_node_id,
            };

            if !eligible {
                return Ok(MarkRunningOutcome::HeldElsewhere);
            }

            row.origin_node_id = holder_node_id.to_string();
            row.claimed_by_node_id = None;
            row.state = PausedRegistryState::Running;
            row.generation += 1;

            Ok(MarkRunningOutcome::Adopted)
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
            self.cluster_backed
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

    /// Every call the snapshot repository received, in order.
    ///
    /// 🔴 The only fake in this module that can answer "were the bytes
    /// uploaded". `mock_snapshot_manager` refuses to stage anything, so every
    /// test written over it lands on the failure arm — which is exactly why the
    /// leak survived: over that fixture a pause that uploads and a pause that
    /// does not are the same green test.
    #[derive(Debug, Default)]
    struct RepositoryJournal(Mutex<Vec<String>>);

    impl RepositoryJournal {
        fn record(&self, entry: &str) {
            self.0.lock().expect("journal").push(entry.to_string());
        }

        fn entries(&self) -> Vec<String> {
            self.0.lock().expect("journal").clone()
        }
    }

    /// The byte half: the call that puts a capture into shared storage.
    struct JournallingArtifacts(Arc<RepositoryJournal>);

    #[async_trait]
    impl SnapshotArtifactStore for JournallingArtifacts {
        async fn import_built_artifacts(
            &self,
            _metadata: &SnapshotPublishMetadata,
            _manifest: &FirecrackerSnapshotManifest,
            _publications: &mut Vec<PersistedDiskImagePublication>,
        ) -> RepositoryResult<ImportedSnapshotArtifacts> {
            self.0.record("artifacts.upload");

            Ok(ImportedSnapshotArtifacts::default())
        }

        async fn delete_artifacts(
            &self,
            _id: &SnapshotId,
            _publications: &[PersistedDiskImagePublication],
        ) {
            self.0.record("artifacts.delete");
        }
    }

    /// The row half: the call that makes an uploaded capture findable.
    struct JournallingCatalog(Arc<RepositoryJournal>);

    #[async_trait]
    impl SnapshotCatalog for JournallingCatalog {
        async fn create(&self, record: SnapshotRecord) -> RepositoryResult<SnapshotRecord> {
            self.0.record("catalog.create");

            Ok(record)
        }

        async fn publish_commit(&self, commit: SnapshotCommit) -> RepositoryResult<SnapshotRecord> {
            self.0.record("catalog.commit");
            let mut record =
                SnapshotRecord::template_waiting(commit.id, commit.alias.clone(), commit.resources);
            record.mark_committed(
                commit.alias,
                commit.resources,
                commit.committed,
                commit.source,
                0,
            );

            Ok(record)
        }

        async fn get(&self, _id_or_alias: &str) -> RepositoryResult<Option<SnapshotRecord>> {
            self.0.record("catalog.get");

            Ok(None)
        }

        async fn list(&self, _filter: SnapshotListFilter) -> RepositoryResult<Vec<SnapshotRecord>> {
            Ok(Vec::new())
        }

        async fn delete_record(&self, _record: &SnapshotRecord) -> RepositoryResult<()> {
            self.0.record("catalog.delete_record");

            Ok(())
        }

        async fn resolve_alias(&self, _alias: &str) -> RepositoryResult<Option<SnapshotId>> {
            Ok(None)
        }

        async fn try_start_build(&self, _id: &SnapshotId) -> RepositoryResult<StartedBuild> {
            unreachable!("a pause never starts a template build")
        }

        async fn mark_build_error(
            &self,
            _id: &SnapshotId,
            _reason: TemplateBuildErrorReason,
        ) -> RepositoryResult<()> {
            unreachable!("a pause never fails a template build")
        }
    }

    /// A snapshot manager whose repository takes what it is given and says so.
    fn journalling_snapshot_manager(journal: Arc<RepositoryJournal>) -> Arc<SnapshotManager> {
        Arc::new(SnapshotManager::from_parts(
            Arc::new(SnapshotRepository::on_node(
                Arc::new(JournallingCatalog(Arc::clone(&journal))),
                Arc::new(JournallingArtifacts(journal)),
                THIS_REPLICA.to_string(),
            )),
            Arc::new(crate::snapshot::mock::MockSnapshotRuntimeResolver),
            None,
        ))
    }

    fn coordinator_over(
        registry: Arc<dyn PausedSandboxRegistry>,
        snapshot_manager: Arc<SnapshotManager>,
    ) -> PausedSandboxCoordinator {
        PausedSandboxCoordinator::new(registry, snapshot_manager, THIS_REPLICA.to_string())
    }

    /// A pause carrying a capture the repository can really commit.
    ///
    /// 🔴 Not [`pause_outcome`]'s placeholder. `stage_captured` downcasts to
    /// `FirecrackerCapturedSnapshot` and refuses anything else, so a
    /// placeholder capture can only ever prove that the upload failed — never
    /// that it was not attempted.
    fn publishable_pause_outcome(sandbox_id: SandboxId) -> PauseOutcome {
        let mut metadata = SandboxMetadata {
            id: sandbox_id,
            ..SandboxMetadata::default()
        };
        metadata.paused_state = Some(Arc::new(CapturedOn(None)));

        PauseOutcome {
            metadata,
            publishable: Some(CapturedSandboxSnapshot::new(
                FirecrackerCapturedSnapshot::in_caller_owned_dir(
                    FirecrackerSnapshotManifest::for_test(32768, &[]),
                ),
            )),
        }
    }

    /// 🔴 The leak `--role node` ran into, stated as the one question that
    /// separates the two halves: is there anything that could reference what
    /// this pause is about to upload?
    ///
    /// Both halves run in the same test, against the same repository, and
    /// differ in exactly one value — `is_cluster_backed`. That matters twice
    /// over. It is what makes "the disabled half uploaded nothing" evidence at
    /// all: the journal below is not empty, it holds the cluster-backed half's
    /// upload and commit, so a fake that was never wired up would fail the
    /// assertion before it could pass the negative one. And it is what pins the
    /// rollback target: the cluster-backed half here *is* `--role all` with the
    /// central registry, and it uploads, commits, and leaves a row pointing at
    /// what it committed, exactly as before.
    #[tokio::test]
    async fn a_pause_uploads_only_when_something_can_reference_what_it_uploads() {
        let journal = Arc::new(RepositoryJournal::default());
        let snapshots = journalling_snapshot_manager(Arc::clone(&journal));

        let cluster_registry = Arc::new(RecordingRegistry::default());
        let local_registry = Arc::new(RecordingRegistry::node_local());
        let cluster = coordinator_over(Arc::clone(&cluster_registry) as _, Arc::clone(&snapshots));
        let local = coordinator_over(Arc::clone(&local_registry) as _, Arc::clone(&snapshots));

        let referenced = SandboxId::new();
        let unreferenced = SandboxId::new();

        let with_cluster = cluster.publish(publishable_pause_outcome(referenced)).await;
        let without_cluster = local.publish(publishable_pause_outcome(unreferenced)).await;

        // The half that has somewhere to point: bytes, row, and a registry
        // entry naming the snapshot those bytes became.
        assert_eq!(
            with_cluster,
            ClusterRecord::Registered(THIS_REPLICA.to_string()),
            "a pause a cluster registry can reference must still be registered"
        );
        let row = cluster_registry
            .row(&referenced)
            .expect("a cluster-backed pause leaves a row");
        assert_eq!(row.state, PausedRegistryState::Paused);
        assert!(
            row.snapshot_id.is_some(),
            "the row must name the snapshot the upload produced, or the upload is orphaned \
             on this path too"
        );

        // The half that has nowhere to point: no bytes, no row, and a reason
        // that says which of the ways to leave no row this was.
        assert_eq!(
            without_cluster,
            ClusterRecord::Unrecorded(Unrecorded::NoClusterRegistry),
            "a pause with no cluster registry must say so, not report a row it never wrote"
        );
        assert!(
            local_registry.row(&unreferenced).is_none(),
            "a registry that tracks nothing cluster-wide holds no row"
        );

        // 🔴 The whole point, and it is only readable because the same journal
        // has the other half's upload in it: exactly one of the two pauses put
        // anything into the repository.
        assert_eq!(
            journal.entries(),
            vec!["artifacts.upload".to_string(), "catalog.commit".to_string()],
            "exactly one of the two pauses may reach the repository, and it is the one \
             whose upload something references"
        );
    }

    /// The fake's flag is only worth anything if the backend it stands in for
    /// answers the same way. `DisabledPausedSandboxRegistry` is what both
    /// `--role node` and `--role all` with `paused_registry.backend = "local"`
    /// actually wire in, so it is asked here directly, against the same
    /// repository that has just been shown to accept an upload.
    #[tokio::test]
    async fn the_registry_a_node_actually_wires_in_publishes_nothing_either() {
        let journal = Arc::new(RepositoryJournal::default());
        let snapshots = journalling_snapshot_manager(Arc::clone(&journal));

        let accepted = coordinator_over(
            Arc::new(RecordingRegistry::default()) as _,
            Arc::clone(&snapshots),
        )
        .publish(publishable_pause_outcome(SandboxId::new()))
        .await;
        let disabled = coordinator_over(
            Arc::new(crate::orchestrator::DisabledPausedSandboxRegistry) as _,
            Arc::clone(&snapshots),
        )
        .publish(publishable_pause_outcome(SandboxId::new()))
        .await;

        assert_eq!(
            accepted,
            ClusterRecord::Registered(THIS_REPLICA.to_string()),
            "the pause that proves the repository is wired up must still be registered"
        );
        assert_eq!(
            disabled,
            ClusterRecord::Unrecorded(Unrecorded::NoClusterRegistry),
            "the real disabled backend must reach the same answer the flagged fake does"
        );
        assert_eq!(
            journal.entries(),
            vec!["artifacts.upload".to_string(), "catalog.commit".to_string()],
            "the disabled backend must add nothing to what the accepted pause uploaded"
        );
    }

    /// 🔴 What an operator can see afterwards, which is the half a silent skip
    /// would lose. Each reason is counted under its own label, and the labels
    /// have to differ: "there is no registry here" is the topology, while "the
    /// registry did not answer" is an outage, and a single `pause_unrecorded`
    /// count would put a rollout and a broken scheduler in the same series.
    #[tokio::test]
    async fn each_way_of_leaving_no_row_is_counted_under_its_own_reason() {
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        // Held across the awaits, which works only because `#[tokio::test]`
        // runs a current-thread runtime.
        let guard = metrics::set_default_local_recorder(&recorder);

        let journal = Arc::new(RepositoryJournal::default());
        let snapshots = journalling_snapshot_manager(Arc::clone(&journal));

        coordinator_over(
            Arc::new(RecordingRegistry::default()) as _,
            Arc::clone(&snapshots),
        )
        .publish(publishable_pause_outcome(SandboxId::new()))
        .await;
        coordinator_over(
            Arc::new(RecordingRegistry::node_local()) as _,
            Arc::clone(&snapshots),
        )
        .publish(publishable_pause_outcome(SandboxId::new()))
        .await;
        coordinator_over(
            Arc::new(RecordingRegistry::unreachable()) as _,
            Arc::clone(&snapshots),
        )
        .publish(publishable_pause_outcome(SandboxId::new()))
        .await;
        drop(guard);

        let mut counted: Vec<(String, u64)> = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .filter_map(|(composite, _unit, _description, value)| {
                let key = composite.key();
                if key.name() != "agentenv_paused_registry_pause_unrecorded_total" {
                    return None;
                }
                let reason = key
                    .labels()
                    .find(|label| label.key() == "reason")?
                    .value()
                    .to_string();
                match value {
                    DebugValue::Counter(count) => Some((reason, count)),
                    other => panic!("the unrecorded reason must be a counter, not {other:?}"),
                }
            })
            .collect();
        counted.sort();

        assert_eq!(
            counted,
            vec![
                ("no_cluster_registry".to_string(), 1),
                ("unreachable".to_string(), 1),
            ],
            "a pause that was registered must count nothing, and the two that were not must \
             be told apart by their label"
        );
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

    /// 🔴 The regression `1e1bf93` shipped, pinned from the outside.
    ///
    /// Before a node could stage a capture, `publishable` was always `None` on
    /// the api half and this pause left through the early return above. Staging
    /// made it `Some`, the early return stopped being reached, and the write it
    /// had been standing in front of finally happened — under
    /// `self.node_id`, which on that half is the api Pod's name and not a
    /// machine holding anything. `paused_sandboxes` went from empty to wrong,
    /// and wrong is worse: an empty table left the origin node able to resume
    /// its own sandbox, while a row pinned to a Pod that does not heartbeat is
    /// answered `FailedPrecondition` on every node there is.
    ///
    /// Both halves run in one round through one coordinator, and differ in
    /// exactly one value — whether the paused state names a machine other than
    /// this process. The second half is the `--role all` rollback face: a
    /// local capture answers `None`, so the row it writes is the same row that
    /// build wrote, byte for byte.
    #[tokio::test]
    async fn a_row_names_the_machine_holding_the_bytes_not_the_replica_that_wrote_it() {
        let registry = Arc::new(RecordingRegistry::default());
        let coordinator = recording_coordinator(Arc::clone(&registry));

        let staged_on_a_node = SandboxId::new();
        let captured_here = SandboxId::new();

        // `--role api`: the node staged the bytes and handed back a capture to
        // commit, so both the holding machine and something to commit are
        // present. That pair is what the early return above no longer catches.
        let remote = coordinator
            .publish(pause_outcome(staged_on_a_node, true, Some("node-203")))
            .await;
        // `--role all`: captured in this process, so the paused state names no
        // other machine.
        let local = coordinator
            .publish(pause_outcome(captured_here, true, None))
            .await;

        let remote_row = registry
            .row(&staged_on_a_node)
            .expect("a capture this half can commit leaves a row");
        let local_row = registry
            .row(&captured_here)
            .expect("a capture this half can commit leaves a row");

        assert_eq!(
            remote_row.origin_node_id, "node-203",
            "the row must name the machine whose disk the bytes are on"
        );
        assert_ne!(
            remote_row.origin_node_id, THIS_REPLICA,
            "a row naming this replica pins the sandbox to a process that never heartbeats, \
             and the scheduler refuses every resume of it"
        );
        assert_eq!(
            local_row.origin_node_id, THIS_REPLICA,
            "a capture taken in this process is held by this process, and the rollback target \
             must keep writing exactly that"
        );
        assert_ne!(
            remote_row.origin_node_id, local_row.origin_node_id,
            "the two halves must not be able to agree: a build that stated one identity for \
             both would pass every assertion above that is about only one of them"
        );

        assert_eq!(remote, ClusterRecord::Registered("node-203".to_string()));
        assert_eq!(local, ClusterRecord::Registered(THIS_REPLICA.to_string()));
    }

    /// The resume-side twin of the regression above. A resume that lands on
    /// another machine has to name *that* machine when it reports itself
    /// running, not this replica — the same wrong-half-of-the-split mistake,
    /// on the write `mark_running` makes instead of the one `begin_pause`
    /// makes.
    ///
    /// 🔴 Unlike the pause side, this one has no `1e1bf93`-shaped regression to
    /// pin: the bug this closes shipped from the start, because
    /// `mark_sandbox_running` never had a place to receive the real machine at
    /// all. What pins it here is the row: a build that quietly went back to
    /// writing `&self.node_id` compiles and passes every other test in this
    /// file, and only fails the assertions below.
    ///
    /// 🔴 Also pins a0487f0's failure mode, from the opposite direction: a
    /// build that sends the *holder* where `mark_sandbox_running` must send
    /// the claimant (`self.node_id`) has `RecordingRegistry`'s guard refuse
    /// the write outright — `origin_node_id` stays whatever `publish` left it
    /// at, state never reaches `running`, generation never moves, and
    /// `claimed_by_node_id` is never cleared. `origin_node_id` alone cannot
    /// tell that apart from a healthy write that happens to land on the same
    /// value; the conjunction below can.
    #[tokio::test]
    async fn a_resumed_row_names_the_machine_that_ran_it_not_the_replica_that_wrote_it() {
        let registry = Arc::new(RecordingRegistry::default());
        let coordinator = recording_coordinator(Arc::clone(&registry));

        let resumed_elsewhere = SandboxId::new();
        let resumed_here = SandboxId::new();

        // Seed a row for each sandbox the way a pause would — `mark_running`
        // never creates one, matching the real registry's contract.
        coordinator
            .publish(pause_outcome(resumed_elsewhere, true, None))
            .await;
        coordinator
            .publish(pause_outcome(resumed_here, true, None))
            .await;

        // Captured before mark_running, so the conjunction below can tell "the
        // generation actually moved" apart from "the row happens to already
        // look like this".
        let generation_before = registry
            .row(&resumed_elsewhere)
            .expect("publish left a row behind")
            .generation;

        // `--role api`: the resume claim landed on a machine, and the backend
        // that drove it knows which one.
        coordinator
            .mark_sandbox_running(
                resumed_elsewhere,
                ExecutionId::new(),
                None,
                Some("node-203".to_string()),
            )
            .await;
        // `--role all`: the backend ran in this process, so it has nothing to
        // report but `None` — which this process's own identity answers for.
        coordinator
            .mark_sandbox_running(resumed_here, ExecutionId::new(), None, None)
            .await;

        let remote_row = registry
            .row(&resumed_elsewhere)
            .expect("mark_running updates the row a pause left behind");
        let local_row = registry
            .row(&resumed_here)
            .expect("mark_running updates the row a pause left behind");

        // 🔴 Four assertions in conjunction, not one. A write the registry
        // *refused* — exactly a0487f0's shape, guard compared against the
        // wrong identity — also leaves an inspectable row behind: whatever
        // `publish` wrote is still sitting there, generation untouched. Only
        // `origin_node_id` alone cannot tell "adopted, and correctly" apart
        // from "refused, and the row is unchanged" — state, generation and
        // claimed_by_node_id have to move too, or this test would pass on a
        // build that silently dropped every mark_running on the floor.
        assert_eq!(
            remote_row.state,
            PausedRegistryState::Running,
            "a refused write leaves the row in whatever state publish left it, never running"
        );
        assert_eq!(
            remote_row.generation,
            generation_before + 1,
            "the write must have actually landed, not merely left the row looking unchanged"
        );
        assert_eq!(
            remote_row.origin_node_id, "node-203",
            "the row must name the machine the VM actually started on"
        );
        assert!(
            remote_row.claimed_by_node_id.is_none(),
            "an adopted write clears the claim; a lingering claimed_by_node_id is the row a \
             refused mark_running leaves behind, still 'resuming'-shaped"
        );
        assert_ne!(
            remote_row.origin_node_id, THIS_REPLICA,
            "a row naming this replica pins the sandbox to a process that never heartbeats, \
             and the scheduler refuses every resume of it"
        );
        assert_eq!(
            local_row.origin_node_id, THIS_REPLICA,
            "a sandbox this process ran itself is correctly named by this process's own \
             identity, and the rollback target must keep writing exactly that"
        );
        assert_ne!(
            remote_row.origin_node_id, local_row.origin_node_id,
            "the two halves must not be able to agree: a build that stated one identity for \
             both would pass every assertion above that is about only one of them"
        );

        // The registration used for reconciliation must be the same identity
        // the row was actually written under, or `reap_superseded_running_sandboxes`
        // compares the row against a value it can never match — see
        // `running_supersession` in `super::paused_recovery`.
        assert_eq!(
            coordinator.running_registration(&resumed_elsewhere),
            Some("node-203".to_string()),
            "the confirmed registration must be the machine the row names, not this replica"
        );
        assert_eq!(
            coordinator.running_registration(&resumed_here),
            Some(THIS_REPLICA.to_string())
        );
    }

    /// 🔴 The stamp and the column are one string, and this is what says so.
    ///
    /// `publish_paused` hands its answer to the orchestrator, which writes it
    /// onto the local paused record; reconciliation then reads it back and
    /// compares it to `origin_node_id` *by equality* — a row whose origin
    /// differs from the stamp means "another node has paused this since,
    /// discard the local copy" (`supersession` in `super::paused_recovery`).
    /// So a build that made the row say one machine and the stamp say another
    /// would not merely record something inaccurate: it would discard every
    /// local paused record on the next reconcile pass, one machine's worth at
    /// a time.
    ///
    /// Both halves again, because the equality has to hold for a value that
    /// came off the paused state *and* for one that came off this process.
    #[tokio::test]
    async fn the_stamp_handed_back_is_the_identity_the_row_carries() {
        let registry = Arc::new(RecordingRegistry::default());
        let coordinator = recording_coordinator(Arc::clone(&registry));

        let staged_on_a_node = SandboxId::new();
        let captured_here = SandboxId::new();

        let remote_stamp = coordinator
            .publish_paused(pause_outcome(staged_on_a_node, true, Some("node-204")))
            .await;
        let local_stamp = coordinator
            .publish_paused(pause_outcome(captured_here, true, None))
            .await;

        let remote_row = registry.row(&staged_on_a_node).expect("a row");
        let local_row = registry.row(&captured_here).expect("a row");

        assert_eq!(
            remote_stamp.as_deref(),
            Some(remote_row.origin_node_id.as_str()),
            "the identity stamped on the local record must be the one the row carries"
        );
        assert_eq!(
            local_stamp.as_deref(),
            Some(local_row.origin_node_id.as_str()),
            "and the same on the half where the two happen to be this process"
        );
        assert_ne!(
            remote_stamp, local_stamp,
            "the two stamps must differ, or neither assertion above is about the value under test"
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

        // 🔴 The stamp this path used to hand back regardless. `publish`
        // returned `Registered` at the end of its happy path, and the disabled
        // registry made every call on the way there succeed — so a node with no
        // cluster registry marked each of its paused records as announced to
        // one. Inert while the registry stays disabled, and not inert at all
        // afterwards: the stamp is reconciliation's licence to discard a local
        // record the registry has no row for, and it would have had a row for
        // none of them.
        let local_registry = Arc::new(RecordingRegistry::node_local());
        let node_local = recording_coordinator(Arc::clone(&local_registry));
        assert_eq!(
            node_local
                .publish_paused(pause_outcome(SandboxId::new(), true, None))
                .await,
            None,
            "a pause with no cluster registry must not stamp the local record as announced"
        );
    }
}

#[cfg(test)]
pub(super) mod test_support {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;

    use crate::orchestrator::{
        BeganPause, ConflictReason, HeldSandbox, MarkRunningOutcome, PausedRegistryError,
        PausedSandboxEntry, PausedSandboxRegistry, ReclaimedHoldings, RegistryResult,
        ReleasedHoldings, ResumeClaim,
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
        /// Every `node_id` a `claim_for_resume` call was made under, in order.
        ///
        /// This is the value a test needs to tell "this node's own identity"
        /// apart from "the machine the claim actually names" — the whole
        /// question `arbitrate_resume`'s fix is about.
        claimed_as: std::sync::Mutex<Vec<String>>,
        /// When set, `claim_for_resume` answers `Conflict` naming this node
        /// instead of `NotFound` — so a test can drive `arbitration`'s
        /// self-comparison branch, which `NotFound` never reaches.
        conflict_origin: Option<String>,
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
                claimed_as: std::sync::Mutex::new(Vec::new()),
                conflict_origin: None,
            }
        }

        /// Answers every `claim_for_resume` with a `Conflict` naming
        /// `origin_node_id`, instead of `NotFound`.
        pub(crate) fn answering_conflict(origin_node_id: &str) -> Self {
            Self {
                conflict_origin: Some(origin_node_id.to_string()),
                ..Self::new(0, false)
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

        /// Every `node_id` a `claim_for_resume` call was made under, in order.
        pub(crate) fn claimed_as(&self) -> Vec<String> {
            self.claimed_as.lock().unwrap().clone()
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
            node_id: &str,
            _execution_id: ExecutionId,
        ) -> RegistryResult<ResumeClaim> {
            self.claimed_as.lock().unwrap().push(node_id.to_string());

            if self.claim_fails {
                return Err(unreachable_backend("claim_for_resume"));
            }

            if let Some(origin_node_id) = &self.conflict_origin {
                return Ok(ResumeClaim::Conflict {
                    origin_node_id: origin_node_id.clone(),
                    reason: ConflictReason::Unspecified,
                });
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
            _holder_node_id: &str,
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
