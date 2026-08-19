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

use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use tracing::{debug, error, info, warn};

use super::{ApiImpl, StaleReleaseOutcome};
use crate::orchestrator::{
    ClusterRegistration, CreateSandboxRequest, HeldSandbox, NewTimeout, PausedRegistryState,
    PausedSandboxEntry, ResumeClaim, SandboxLaunchSource, SandboxListFilter, SandboxMetadata,
    SandboxState,
};
use crate::types::SandboxId;

/// 本节点既没有本地副本、又没拿到认领权时，一次 resume 该怎么收场。
///
/// 🔴 这个枚举存在的唯一理由：**404 的下游契约是「沙箱没了，可以重建」**。
/// 平台侧收到 resume 的 404 会把 `externalID` 清空并从模板重建沙箱
/// （`apps/agent-platform/internal/sandbox/aenv/service.go`，注释自己写着
/// 「工作区回到模板初始态」）。所以「另一发 resume 正在把它拉起来」
/// 绝不能落成 404 —— 那会把一次良性竞态变成用户工作区被重置。
///
/// e2b 在同一处竞态上的答案是让输家**拿到赢家的结果**：`Reserve` 命中 pending
/// 就订阅赢家的完成通知（`e2b/packages/api/internal/sandbox/reservations/redis/reservation.go`），
/// 命中 storage index 就直接读回既有沙箱（`.../sandbox/store.go`）。它从不把
/// 输家答成「不存在」。这里对齐的是那个保证，不是它的 Redis 形状。
pub(super) enum MissingLocalResume {
    /// 集群也不认识它。**唯一**允许回 404 的情形。
    Unknown,
    /// 另一发 resume 已经把它拉起来了，而且赢家就在本节点 —— 直接返回成品。
    Resumed(Box<SandboxMetadata>),
    /// 有人正在处理它，或它属于别的节点。可重试，但不是「不存在」。
    Busy { holder: String },
    /// 登记表答不上来。宁可报错，也不许说「不存在」。
    Undecided(String),
}

/// [`missing_local_verdict`] 的结论。抽成纯函数是为了能穷举测试：
/// 这里判错一次的代价是用户工作区，而它的输入只有三样事实。
enum MissingLocalVerdict {
    /// 集群没有这一行。
    Unknown,
    /// 本地已经有一台跑着的同 ID 沙箱 —— 赢家落定了。
    Ready,
    /// 赢家在本节点且仍在进行中，它的成品会出现在本地 store，值得等。
    Wait { holder: String },
    /// 别人管着它。等不出结果，交给调用方重试。
    Busy { holder: String },
}

/// 据三样事实判定：集群怎么说、本地 store 怎么说、本节点是谁。
///
/// `paused` 也归入 [`MissingLocalVerdict::Busy`]：走到这里说明我们刚在
/// `claim_for_resume` 上输掉一次认领而赢家又已经释放，重试一次就能拿到 ——
/// 那是「稍后再来」，同样不是「不存在」。
fn missing_local_verdict(
    entry: Option<&PausedSandboxEntry>,
    local: Option<&SandboxMetadata>,
    node_id: &str,
) -> MissingLocalVerdict {
    let Some(entry) = entry else {
        return MissingLocalVerdict::Unknown;
    };

    // 赢家把沙箱加进本地 store 之后，这里就能看到成品 —— 与 e2b 输家
    // 从 storage 读回赢家那台是同一件事。
    if local.is_some_and(|m| m.state == SandboxState::Running) {
        return MissingLocalVerdict::Ready;
    }

    let holder = entry
        .claimed_by_node_id
        .clone()
        .unwrap_or_else(|| entry.origin_node_id.clone());

    match entry.state {
        PausedRegistryState::Resuming if holder == node_id => MissingLocalVerdict::Wait { holder },
        _ => MissingLocalVerdict::Busy { holder },
    }
}

/// Outcome of rebuilding a sandbox from a claim this node already holds.
pub(super) enum CrossNodeResume {
    /// The sandbox is running here again, under its original ID.
    Restored(Box<SandboxMetadata>),
    /// The registry named a snapshot the repository no longer has, so there is
    /// nothing left to rebuild from.
    NotFound,
    /// Rebuilding failed.
    Failed(String),
}

/// Who the cluster says may resume a sandbox.
///
/// Both resume paths — off this node's own disk, and from the shared snapshot
/// repository — pass through this one decision. They have to: each ends with a
/// live sandbox, so arbitrating them separately is what lets two nodes bring the
/// same sandbox up at once. Routing cannot substitute for it, because the
/// gateway hands a resume to an arbitrary node whenever the scheduler holds no
/// binding, and bindings live in memory with a short TTL and are lost outright
/// when the scheduler restarts.
pub(super) enum ResumeArbitration {
    /// The registry has no say: it is not cluster-backed, or it does not track
    /// this sandbox. Whatever is on local disk is the whole truth.
    Proceed,
    /// This node holds the claim, and must release it if the resume fails. The
    /// row travels with it so a rebuild never has to claim a second time —
    /// claiming twice would deadlock against this node's own claim.
    Held(Box<PausedSandboxEntry>),
    /// The newest snapshot is still being published by another node, which is
    /// therefore the only node that can serve this resume.
    NotReady { origin_node_id: String },
    /// Another node holds the sandbox, so resuming here would make a second
    /// live copy of it.
    Blocked { origin_node_id: String },
    /// The registry could not be reached about a sandbox it is known to have a
    /// say over, so nobody can tell whether resuming here would make a second
    /// live copy. Retryable, and never an answer about whether the sandbox
    /// exists.
    Unavailable { reason: String },
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
    /// Asks the cluster who may resume this sandbox, taking the claim when the
    /// answer is "this node".
    ///
    /// Fails open on anything that is not a clear "someone else has it", with
    /// one exception: a registry that cannot answer about a sandbox it was
    /// told about. See [`unreachable_arbitration`] for why that one is
    /// different.
    pub(super) async fn arbitrate_resume(&self, sandbox_id: SandboxId) -> ResumeArbitration {
        if !self.paused.registry().is_cluster_backed() {
            return ResumeArbitration::Proceed;
        }

        // A claim that lands writes `resuming` with this node's name on it, and
        // a claim whose response is lost writes it just the same — so this node
        // stops being able to release rows by identity from here on, not from
        // whenever the answer comes back.
        self.paused.note_taking_sandbox_live().await;

        let node_id = self.paused.node_id();
        let claim = match self
            .paused
            .registry()
            .claim_for_resume(&sandbox_id, node_id)
            .await
        {
            Ok(claim) => claim,
            Err(err) => {
                // What this node's own copy remembers is the only thing left to
                // decide on, so read it before answering.
                let registration = self
                    .orchestrator
                    .paused_record_cluster_registration(sandbox_id)
                    .await
                    .ok();

                return unreachable_arbitration(registration, sandbox_id, &err.to_string());
            }
        };

        arbitration(claim, node_id)
    }

    /// Returns a claim after the resume it was taken for failed.
    /// 有界地等「本节点上另一发 resume」落定，然后据实回答本节点该回什么。
    ///
    /// 只在 resume 已经确认**本地没有副本**、且仲裁**没有给出认领权**之后调用。
    /// 这两个条件合起来在单发请求下意味着"沙箱真没了"，但在并发下不是：
    /// 同一台沙箱的两发 resume 会被网关派到同一个非 origin 节点，赢家拿到认领权、
    /// 输家的 `claim_for_resume` 撞上自己节点的 `resuming` 行 —— 而
    /// `Conflict{origin == 本节点}` 被 [`arbitration`] 判为 `Proceed`（那条判断
    /// 对赢家是对的：它确实持有认领权）。输家于是既无本地副本又无 entry。
    ///
    /// 🧪 实测（pve-sg dev，2026-08-18，隔离 origin 后两发并发，2/2 复现）：
    /// 修复前输家稳定拿到 404 / ~0.19s，而沙箱正在另一节点上健康运行。
    const MISSING_LOCAL_WAIT: Duration = Duration::from_secs(5);
    const MISSING_LOCAL_POLL: Duration = Duration::from_millis(200);

    /// How often to retry a failed release of the previous process's holdings.
    ///
    /// Short on purpose, and not configurable: the whole point is to land
    /// inside the window between this process starting and it taking its first
    /// sandbox live, which on a busy node is seconds. A missed window costs a
    /// node restart, while an extra query every five seconds costs nothing —
    /// the retry stops at the first success, and only runs at all after a
    /// failure.
    const STALE_RELEASE_RETRY_INTERVAL: Duration = Duration::from_secs(5);

    pub(super) async fn resolve_missing_local_resume(
        &self,
        sandbox_id: SandboxId,
    ) -> MissingLocalResume {
        // 单机形态：登记表没有话语权，本地没有就是真没有 —— 保持原语义。
        if !self.paused.registry().is_cluster_backed() {
            return MissingLocalResume::Unknown;
        }

        let node_id = self.paused.node_id().to_string();
        let deadline = Instant::now() + Self::MISSING_LOCAL_WAIT;
        let mut holder = node_id.clone();

        loop {
            let entry = match self.paused.registry().get(&sandbox_id).await {
                Ok(entry) => entry,
                // 读不到就不下结论。这是本方法的全部意义：说错成"不存在"
                // 会让调用方重建，而重建是不可逆的。
                Err(err) => return MissingLocalResume::Undecided(err.to_string()),
            };
            let local = match self.orchestrator.get_sandbox(&sandbox_id).await {
                Ok(local) => local,
                Err(err) => return MissingLocalResume::Undecided(err.to_string()),
            };

            match missing_local_verdict(entry.as_ref(), local.as_ref(), &node_id) {
                MissingLocalVerdict::Unknown => return MissingLocalResume::Unknown,
                MissingLocalVerdict::Ready => {
                    return match local {
                        Some(metadata) => MissingLocalResume::Resumed(Box::new(metadata)),
                        // 判定说 Ready 就一定有 local；这条只为不写 unwrap。
                        None => MissingLocalResume::Busy { holder },
                    };
                }
                MissingLocalVerdict::Busy { holder } => return MissingLocalResume::Busy { holder },
                MissingLocalVerdict::Wait { holder: who } => {
                    holder = who;
                    if Instant::now() >= deadline {
                        // 等不到不等于没有。超时同样回 Busy（可重试），不回 404。
                        warn!(
                            %sandbox_id,
                            holder,
                            "another resume on this node has not landed within the wait window; \
                             reporting it as busy rather than missing"
                        );

                        return MissingLocalResume::Busy { holder };
                    }
                    tokio::time::sleep(Self::MISSING_LOCAL_POLL).await;
                }
            }
        }
    }

    pub(super) async fn abandon_claim(&self, sandbox_id: SandboxId, generation: i64) {
        self.release_claim(&sandbox_id, generation).await;
    }

    /// Rebuilds a sandbox from the claim this node already holds.
    ///
    /// Reached when the local resume reported the sandbox unknown — this node
    /// has never run it, or has already discarded its copy — while the cluster
    /// still has a snapshot to rebuild it from.
    pub(super) async fn restore_claimed_sandbox(
        &self,
        entry: PausedSandboxEntry,
        timeout: NewTimeout,
    ) -> CrossNodeResume {
        let sandbox_id = entry.sandbox_id;

        // The claim only matches rows that name a snapshot, so this cannot be
        // None here; treat it as a failed claim rather than panicking.
        let Some(snapshot_id) = entry.snapshot_id.clone() else {
            self.release_claim(&sandbox_id, entry.generation).await;

            return CrossNodeResume::Failed("paused sandbox has no published snapshot".to_string());
        };

        // A granted claim carries the record; rebuilding from a default one
        // would produce a sandbox with a different identity and configuration
        // from the one the user paused, and say nothing about it. Hand the
        // claim back instead so the sandbox stays claimable.
        let Some(metadata) = entry.metadata.clone() else {
            self.release_claim(&sandbox_id, entry.generation).await;

            return CrossNodeResume::Failed(
                "paused sandbox claim carries no sandbox record".to_string(),
            );
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

        let request = restore_request(&metadata, snapshot, timeout);

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

    /// Renews this node's lease on every registry row it is the holder of.
    ///
    /// What lapsing costs is narrower than it looks. For a sandbox that is
    /// live, nothing: no lease state makes another node willing to rebuild it,
    /// because reaching the database and being alive are not the same thing.
    /// For one that is parked with an unpublished snapshot, it is the signal
    /// that this node has given up on it, and the cluster will bring the
    /// sandbox back elsewhere from the previous snapshot — losing the last
    /// pause's work. Renewal is what buys the time not to do that.
    ///
    /// The whole local roster goes in, whatever state each sandbox is in — the
    /// registry decides which rows this node actually holds, so nothing here
    /// has to duplicate that judgement.
    pub async fn renew_paused_leases(&self) {
        if !self.paused.registry().is_cluster_backed() {
            return;
        }

        let sandboxes = match self
            .orchestrator
            .list_sandboxes_filtered(SandboxListFilter::default())
            .await
        {
            Ok(sandboxes) => sandboxes,
            Err(err) => {
                warn!(error = ?err, "failed to list local sandboxes for lease renewal");

                return;
            }
        };

        // The deadline comes from the live sandbox, not from the row: the row's
        // metadata is a snapshot of what the sandbox looked like when it was
        // paused, and a timeout set or extended since then only exists here.
        // Reclamation compares against this, so a stale value would retire a
        // sandbox that still had time left.
        let held: Vec<HeldSandbox> = sandboxes
            .into_iter()
            .map(|metadata| HeldSandbox {
                sandbox_id: metadata.id,
                expires_at: metadata.expires_at.map(DateTime::<Utc>::from),
            })
            .collect();
        match self
            .paused
            .registry()
            .renew_lease(self.paused.node_id(), &held)
            .await
        {
            Ok(_) => self.paused.observe_lease_renewal(true),
            Err(err) => {
                self.paused.observe_lease_renewal(false);
                warn!(
                    error = %err,
                    "failed to renew paused registry leases; sandboxes parked here with an \
                     unpublished snapshot may be rebuilt elsewhere from an older one"
                );
            }
        }
    }

    /// Hands back the sandboxes the previous process on this node died holding.
    ///
    /// 🔴 **Startup only, and before the listener opens.** It releases rows by
    /// node identity, and this node's identity is the machine's — so once this
    /// process is actually running sandboxes, the very rows it would release
    /// are its own. Call it while it holds nothing and it is exact; call it a
    /// second later and it hands live sandboxes to whoever resumes them next.
    ///
    /// Why this exists at all. A row saying "running on this node" can be stale
    /// for two reasons that look identical from the database: the process that
    /// wrote it died, or it is merely cut off from PostgreSQL. Only the first
    /// makes the sandbox safe to rebuild elsewhere, and no timeout can tell
    /// them apart — which is why `claim_for_resume` refuses live rows outright
    /// and why the decision is made here instead. Being the successor process
    /// on that machine *is* the proof: the previous process's VMs were its
    /// children in its PID namespace and went with it.
    ///
    /// e2b reaches the same end from its control plane, which has the one thing
    /// we do not: a single authority that knows which nodes exist. It never
    /// rebuilds a live sandbox somewhere else either — a resume that finds the
    /// sandbox in its store is refused (`sandbox_resume.go`, StateRunning ⇒
    /// 409), and its orphan sweep only kills what the store has no record of.
    pub async fn release_stale_node_holdings(&self) -> StaleReleaseOutcome {
        if !self.paused.registry().is_cluster_backed() {
            return StaleReleaseOutcome::Released;
        }

        self.paused.release_stale_holdings().await
    }

    /// Keeps trying to release what the previous process left behind, for as
    /// long as doing so is still exact.
    ///
    /// 🔴 Why a failed release used to be permanent, and why that is the wrong
    /// shape. `claim_for_resume` refuses `running` and `resuming` rows outright
    /// — no lease, no timeout, nothing else in the system releases them — so a
    /// single failed attempt at startup left every sandbox the previous process
    /// was running unresumable until somebody restarted the node again. The
    /// only evidence in the failure was one `warn!` that does not survive the
    /// next restart.
    ///
    /// Retrying is safe for the same reason the startup call is: the fence is
    /// not a deadline but the state of this process, and it stays open exactly
    /// while this process has done nothing that could put its node's name on a
    /// live row. A retry that lands inside that window is indistinguishable
    /// from the startup call landing late; one that arrives after it is refused
    /// outright, not merely deprioritised.
    ///
    /// It gives up only when the window closes, and says so loudly when it
    /// does: at that point rows really are stranded until the next start, and
    /// that is worth an operator's attention rather than a debug line.
    pub async fn retry_stale_node_holdings_release(&self) {
        loop {
            tokio::time::sleep(Self::STALE_RELEASE_RETRY_INTERVAL).await;

            match self.release_stale_node_holdings().await {
                StaleReleaseOutcome::Released => {
                    info!(
                        node_id = self.paused.node_id(),
                        "released the previous process's holdings on a retry"
                    );

                    return;
                }
                StaleReleaseOutcome::Fenced => {
                    metrics::counter!("agentenv_paused_registry_stale_release_abandoned_total")
                        .increment(1);
                    error!(
                        node_id = self.paused.node_id(),
                        "gave up releasing the previous process's holdings: this node now runs \
                         sandboxes of its own, so releasing by node identity would give one of \
                         them away. Any sandbox the previous process was running stays \
                         unclaimable until this node restarts"
                    );

                    return;
                }
                // Logged and counted at the point of failure.
                StaleReleaseOutcome::Failed => {}
            }
        }
    }

    /// Reclaims sandboxes that outlived their deadline on a node nobody has
    /// heard from since.
    ///
    /// Unlike everything else in this module, this pass is not about *this*
    /// node — any node runs it against the whole cluster, and running it from
    /// several at once is harmless because the statement is a single
    /// conditional `UPDATE`. That is deliberate: it exists for the case where a
    /// machine never comes back, so it cannot depend on that machine doing
    /// anything.
    ///
    /// It is the counterpart to the node-local eviction task. A reachable node
    /// evicts its own expired sandboxes — pausing them properly, publishing a
    /// fresh snapshot — which is the outcome we want and the reason this waits
    /// for a lapsed lease before touching anything. e2b does not need the
    /// distinction because its eviction was never on the node to begin with:
    /// it runs in the control plane off a cluster-wide expiry index, and drops
    /// the sandbox from its store even when the node cannot be reached to be
    /// told (`e2b/packages/api/internal/orchestrator/delete_instance.go:104`).
    pub async fn reclaim_expired_sandboxes(&self) {
        if !self.paused.registry().is_cluster_backed() {
            return;
        }

        match self.paused.registry().reclaim_expired_holdings().await {
            Ok(reclaimed) if reclaimed.is_empty() => {}
            Ok(reclaimed) => {
                metrics::counter!("agentenv_paused_registry_expired_reclaimed_total")
                    .increment(reclaimed.released);
                metrics::counter!("agentenv_paused_registry_expired_discarded_total")
                    .increment(reclaimed.discarded);
                info!(
                    released = reclaimed.released,
                    discarded = reclaimed.discarded,
                    "reclaimed expired sandboxes from nodes that stopped reporting"
                );
            }
            Err(err) => {
                warn!(error = %err, "failed to reclaim expired sandboxes");
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

    /// Brings this node's copies of sandboxes back in line with the cluster.
    ///
    /// Runs at startup and then on a timer, over both halves of the node's
    /// roster, because a node can be out of step in two different ways and only
    /// one of them used to be checked:
    ///
    /// - a **paused** record for a sandbox another node has since resumed —
    ///   dead weight that still gets advertised in the heartbeat roster, so the
    ///   scheduler's binding flaps between the two nodes;
    /// - a **running** copy of a sandbox another node has taken over — two live
    ///   VMs writing to their own rootfs layers from the same starting point.
    ///
    /// The second is the expensive one and the one the lease cannot prevent:
    /// the lease decides *who may take over*, and a node whose lease lapsed
    /// because it was partitioned rather than dead comes back still running its
    /// copy. Nothing tells it. Noticing is entirely on it, which is why this is
    /// periodic and why it covers running sandboxes too.
    pub async fn reconcile_local_records(&self) {
        // A disabled registry reports every sandbox as missing, which this
        // would read as "all of them moved on".
        if !self.paused.registry().is_cluster_backed() {
            return;
        }

        self.retain_running_registrations().await;
        self.reconcile_local_paused_records().await;
        self.reap_superseded_running_sandboxes().await;
    }

    /// Forgets registrations for sandboxes this node no longer has, so the map
    /// tracks the current roster instead of every resume the process ever
    /// served.
    async fn retain_running_registrations(&self) {
        match self.orchestrator.list_sandbox_ids().await {
            Ok(ids) => self
                .paused
                .retain_running_registrations(&ids.into_iter().collect()),
            Err(err) => {
                warn!(error = ?err, "failed to list local sandboxes to prune registrations")
            }
        }
    }

    /// Tears down running copies of sandboxes the cluster says are held
    /// elsewhere.
    ///
    /// The mirror image of e2b's orphan sweep, which reconciles each node's
    /// reported sandbox list against the store and kills whatever the store
    /// does not account for (`e2b/packages/api/internal/sandbox/store.go:141`,
    /// *"Redis is the source of truth — divergent sandboxes are orphans …
    /// Kill them"*). Same invariant, opposite direction: e2b's control plane
    /// pulls and kills, and here the node that has fallen out of step is the
    /// one that notices and stands down.
    ///
    /// Only sandboxes this process registered are considered, and the registry
    /// row is judged against the identity it was registered under rather than
    /// this node's current ID — the same rule the paused half follows, for the
    /// same reason (§8.4: the ID is a pod name and changes under the node).
    async fn reap_superseded_running_sandboxes(&self) {
        let running = match self
            .orchestrator
            .list_sandboxes_filtered(SandboxListFilter {
                states: Some(vec![crate::orchestrator::SandboxState::Running]),
                ..Default::default()
            })
            .await
        {
            Ok(running) => running,
            Err(err) => {
                warn!(error = ?err, "failed to list local running sandboxes for reconciliation");

                return;
            }
        };

        // Only sandboxes this process registered can be judged at all, so the
        // roster is narrowed before the registry is asked anything.
        let registered: Vec<(SandboxId, String)> = running
            .into_iter()
            .filter_map(|metadata| {
                self.paused
                    .running_registration(&metadata.id)
                    .map(|registered_as| (metadata.id, registered_as))
            })
            .collect();
        if registered.is_empty() {
            return;
        }

        let ids: Vec<SandboxId> = registered.iter().map(|(id, _)| *id).collect();
        let rows = match self.paused.registry().get_many(&ids).await {
            Ok(rows) => rows,
            Err(err) => {
                // One unreadable answer must not cascade into tearing down the
                // node's sandboxes.
                warn!(error = %err, "registry unreadable; stopping running-sandbox reconciliation");

                return;
            }
        };

        for (sandbox_id, registered_as) in registered {
            let Some(superseded) = running_supersession(rows.get(&sandbox_id), &registered_as)
            else {
                continue;
            };

            warn!(
                %sandbox_id,
                reason = %superseded.reason(),
                "tearing down a running sandbox the cluster holds elsewhere"
            );

            match self
                .orchestrator
                .discard_superseded_sandbox(sandbox_id)
                .await
            {
                Ok(()) => {
                    record_supersession("running", "discarded");
                    info!(%sandbox_id, "discarded superseded running sandbox");
                }
                Err(err) => {
                    record_supersession("running", "failed");
                    warn!(error = ?err, %sandbox_id, "failed to discard superseded running sandbox")
                }
            }
        }
    }

    /// Drops local paused records the cluster has moved past.
    ///
    /// A node that keeps a paused record for a sandbox another node has since
    /// resumed does more than waste disk: it keeps reporting that sandbox in
    /// its heartbeat roster, so the scheduler's binding for it flaps between
    /// the two nodes and traffic for a perfectly healthy sandbox lands half the
    /// time on the node that only has a corpse of it. Left long enough, a
    /// resume aimed here would start a second copy, and the two would write to
    /// their own rootfs layers from the same starting point.
    async fn reconcile_local_paused_records(&self) {
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

        // Records that predate the registry, or that were written while it was
        // node-local, carry no cluster registration — for those the local copy
        // is the only copy and the registry's silence says nothing. Dropping
        // them here also keeps them out of the query below.
        let mut registered = Vec::with_capacity(paused.len());
        for metadata in paused {
            match self
                .orchestrator
                .paused_record_cluster_registration(metadata.id)
                .await
            {
                Ok(ClusterRegistration::Never) => continue,
                Ok(registration) => registered.push((metadata.id, registration)),
                Err(err) => {
                    warn!(error = ?err, sandbox_id = %metadata.id, "failed to read local registration marker")
                }
            }
        }
        if registered.is_empty() {
            return;
        }

        let ids: Vec<SandboxId> = registered.iter().map(|(id, _)| *id).collect();
        let rows = match self.paused.registry().get_many(&ids).await {
            Ok(rows) => rows,
            Err(err) => {
                // Never guess when the registry cannot answer: one unreadable
                // response must not cascade into deleting local records.
                warn!(error = %err, "registry unreadable; stopping paused-record reconciliation");

                return;
            }
        };

        let mut discarded = 0usize;
        for (sandbox_id, registration) in registered {
            // Registered once, no row now: resumed elsewhere, or deleted.
            let superseded = match rows.get(&sandbox_id) {
                None => Superseded::Gone,
                Some(entry) => match supersession(entry, &registration) {
                    Some(superseded) => superseded,
                    None => continue,
                },
            };

            match self
                .orchestrator
                .discard_local_paused_record(sandbox_id)
                .await
            {
                Ok(true) => {
                    record_supersession("paused", "discarded");
                    info!(%sandbox_id, reason = %superseded.reason(), "discarded superseded paused record");
                    discarded += 1;
                }
                Ok(false) => {}
                Err(err) => {
                    record_supersession("paused", "failed");
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

        let superseded = match self.superseded_by_cluster(sandbox_id).await {
            Ok(Some(superseded)) => superseded,
            Ok(None) => return false,
            // 🔴 Worth a line of its own. Keeping the copy is right, but the
            // reason is not "the row is still ours" — it is that nobody could
            // be asked, and the resume that follows this call is about to lose
            // its other defence to the same outage. Once the registry is
            // another service rather than a database connection this stops
            // being rare, and without this it is invisible.
            Err(()) => {
                debug!(
                    %sandbox_id,
                    "the registry could not say whether this node's paused copy has been \
                     superseded; leaving it in place"
                );

                return false;
            }
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

/// Decides a resume the registry could not arbitrate, from what this node's own
/// copy remembers.
///
/// 🔴 This is the one direction the rest of this module's caution does not
/// cover. Everywhere else a registry that cannot answer means *stop*; here it
/// used to mean *go*, unconditionally — and the two failures are the same
/// failure. `running` and `resuming` are never claimable, and the last thing
/// enforcing that is the claim coming back as a conflict; when the registry is
/// unreachable that check is simply absent. The same outage also makes
/// `discard_if_superseded` keep a stale local copy instead of dropping it, so
/// the two defences against bringing a sandbox up twice go together, and what
/// is left is a node resuming a sandbox from a copy it has no reason to still
/// believe in. Two VMs then diverge from one snapshot, each writing its own
/// rootfs layers, with the gateway alternating between them.
///
/// That was survivable while the registry was a database in the same cluster
/// with a permanently open pool. It is not once reaching it means reaching
/// another service, whose ordinary rolling restart opens the window on purpose.
///
/// **Narrow, not blanket.** Only a local record that was announced *under a
/// known identity* changes the answer. The alternatives carry no information:
/// a record the cluster was never told about has its only copy right here, and
/// one announced by a build that did not store the identity it used cannot be
/// judged against the row either — which is exactly how [`supersession`] treats
/// the same three cases. Refusing those would fail resumes that were never at
/// risk.
///
/// The cost is a retryable failure while the registry is unreachable. Two live
/// copies of one sandbox is not retryable.
fn unreachable_arbitration(
    registration: Option<ClusterRegistration>,
    sandbox_id: SandboxId,
    reason: &str,
) -> ResumeArbitration {
    if !matches!(registration, Some(ClusterRegistration::As(_))) {
        metrics::counter!(
            "agentenv_paused_registry_resume_unarbitrated_total",
            "outcome" => "proceeded",
        )
        .increment(1);
        warn!(
            error = %reason,
            %sandbox_id,
            "could not reach the registry to arbitrate a resume; proceeding locally because \
             this node holds no copy the cluster was told about"
        );

        return ResumeArbitration::Proceed;
    }

    metrics::counter!(
        "agentenv_paused_registry_resume_unarbitrated_total",
        "outcome" => "refused",
    )
    .increment(1);
    warn!(
        error = %reason,
        %sandbox_id,
        "could not reach the registry to arbitrate a resume for a sandbox this node announced \
         to the cluster; refusing rather than risking a second live copy"
    );

    ResumeArbitration::Unavailable {
        reason: reason.to_string(),
    }
}

/// Turns the registry's answer into a decision about this node.
///
/// Split out from the call that produces it because whether a node may bring a
/// sandbox up is the judgement that decides how many copies of it exist. The
/// one case worth stating twice: an answer naming *this* node is not a refusal.
/// A node is regularly told "not ready, held by X" or "conflict, held by X"
/// where X is itself — its own in-flight publish, its own pause that never
/// published, its own already-running sandbox — and reading those as refusals
/// would make a node unable to resume its own sandboxes.
fn arbitration(claim: ResumeClaim, node_id: &str) -> ResumeArbitration {
    match claim {
        ResumeClaim::Claimed { entry, .. } => ResumeArbitration::Held(entry),
        // The cluster does not track this sandbox, so there is nobody to
        // arbitrate with and a local copy, if any, is the whole truth.
        ResumeClaim::NotFound => ResumeArbitration::Proceed,
        ResumeClaim::NotReady { origin_node_id } if origin_node_id == node_id => {
            ResumeArbitration::Proceed
        }
        ResumeClaim::Conflict { origin_node_id } if origin_node_id == node_id => {
            ResumeArbitration::Proceed
        }
        ResumeClaim::NotReady { origin_node_id } => ResumeArbitration::NotReady { origin_node_id },
        ResumeClaim::Conflict { origin_node_id } => ResumeArbitration::Blocked { origin_node_id },
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

/// Counts what reconciliation found the cluster had moved past.
///
/// Worth a metric rather than only a log line: every increment here is a copy of
/// a sandbox that this node believed it held and did not, so a rate that is
/// anything but near-zero means nodes are routinely losing sandboxes to each
/// other — a lease or partition problem, not a reconciliation one.
fn record_supersession(kind: &'static str, outcome: &'static str) {
    metrics::counter!(
        "agentenv_paused_registry_superseded_total",
        "kind" => kind,
        "outcome" => outcome,
    )
    .increment(1);
}

/// Decides whether a running copy on this node has been superseded, judged
/// against the identity the registry confirmed this node as holder under.
///
/// Deliberately narrower than [`supersession`]: that one decides the fate of a
/// *paused* record, whose artifacts are the sandbox. This one decides the fate
/// of a live VM, so it only ever fires when the row positively names someone
/// else — or has ceased to exist, which for a row this node was confirmed the
/// holder of means the sandbox was removed cluster-wide while this node was
/// away.
///
/// `entry: None` is only reachable for a sandbox this process registered, which
/// is the whole reason the caller must not invoke this without one. For an
/// unregistered sandbox — anything created here and never resumed from the
/// cluster — `None` means nothing at all, and reading it as "gone" would tear
/// down a sandbox seconds after it was created.
fn running_supersession(
    entry: Option<&PausedSandboxEntry>,
    registered_as: &str,
) -> Option<Superseded> {
    let Some(entry) = entry else {
        return Some(Superseded::Gone);
    };

    match entry.state {
        // Held by whoever the row names, and it is not us.
        PausedRegistryState::Running
        | PausedRegistryState::Paused
        | PausedRegistryState::Publishing
        | PausedRegistryState::LocalOnly => (entry.origin_node_id != registered_as)
            .then(|| Superseded::HeldBy(entry.origin_node_id.clone())),
        // Someone is bringing it up. `origin_node_id` still names the node
        // whose disk holds the artifacts — which during a takeover is us — so
        // only the claimer answers the question.
        PausedRegistryState::Resuming => {
            let claimer = entry
                .claimed_by_node_id
                .clone()
                .unwrap_or_else(|| entry.origin_node_id.clone());

            (claimer != registered_as).then_some(Superseded::ClaimedBy(claimer))
        }
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
    use std::sync::Arc;

    use chrono::Utc;

    use super::super::paused_coordinator::test_support::CountingRegistry;
    use super::*;
    use crate::cfg::AppConfig;
    use crate::identity::NodeIdentity;
    use crate::image::ImageResolver;
    use crate::orchestrator::{
        DisabledPausedSandboxRegistry, FileBackedSandboxPersister, InMemoryMetadataStore,
        Orchestrator, PausedSandboxRegistry,
    };
    use crate::sandbox::FirecrackerSandboxFactory;
    use crate::snapshot::mock::mock_snapshot_manager;
    use crate::snapshot::SnapshotId;
    use crate::template::TemplateBuilder;

    const SELF: &str = "node-a";
    const OTHER: &str = "node-b";

    /// An API built over the given registry and nothing else that touches the
    /// host: enough to drive the startup-time release and its retry.
    async fn api_over(registry: Arc<dyn PausedSandboxRegistry>) -> Arc<ApiImpl> {
        let root = tempfile::tempdir().unwrap();

        api_rooted(root.path(), registry).await
    }

    /// The same, over a record store the caller owns — so a test can seed a
    /// paused record into it first.
    ///
    /// The store is opened here and stays open, which is why seeding has to
    /// happen before this is called: it is a RocksDB directory and only one
    /// handle at a time may hold it.
    async fn api_rooted(
        root: &std::path::Path,
        registry: Arc<dyn PausedSandboxRegistry>,
    ) -> Arc<ApiImpl> {
        let orchestrator = Orchestrator::new(
            InMemoryMetadataStore::new(),
            FirecrackerSandboxFactory::new(),
            FileBackedSandboxPersister::new_for_test(root.to_path_buf()),
        )
        .await
        .unwrap();
        let snapshot_manager = Arc::new(mock_snapshot_manager());

        Arc::new(ApiImpl::new(
            orchestrator,
            Arc::clone(&snapshot_manager),
            Arc::new(TemplateBuilder::new()),
            Arc::new(ImageResolver::new(&AppConfig::default())),
            None,
            crate::api::PausedSandboxWiring::new(
                registry,
                snapshot_manager,
                &NodeIdentity::from_config(&Default::default()),
            ),
            Vec::new(),
        ))
    }

    /// Writes a paused record into the store at `root` and reports its id.
    ///
    /// 🔴 The record names the other virtualization mode on purpose. A store
    /// opened afterwards keeps a record it cannot rebuild a VM from, but
    /// discards one it *should* be able to and then cannot — and this test
    /// wants the record, not a resumable sandbox. Seeded the obvious way, the
    /// record is gone by the time the assertion runs and every one of these
    /// tests passes for the wrong reason.
    async fn seed_paused_record(
        root: &std::path::Path,
        registered_as: Option<&str>,
    ) -> crate::types::SandboxId {
        use crate::orchestrator::SandboxPersister;

        let persister = FileBackedSandboxPersister::new_for_test(root.to_path_buf());
        let sandbox_id = crate::types::SandboxId::new();
        let artifacts = root.join("artifacts").join(sandbox_id.to_string());
        std::fs::create_dir_all(&artifacts).unwrap();
        let paused_state: Arc<dyn crate::sandbox::PausedSandboxState> =
            Arc::new(crate::sandbox::mock::MockSnapshot);

        persister
            .persist_paused(
                &SandboxMetadata {
                    id: sandbox_id,
                    virtualization_mode: crate::virtualization::VirtualizationMode::Pvm,
                    ..Default::default()
                },
                Some(&artifacts),
                paused_state.as_ref(),
            )
            .await
            .unwrap();
        if let Some(node_id) = registered_as {
            persister
                .mark_cluster_registered(&sandbox_id, node_id)
                .await
                .unwrap();
        }

        sandbox_id
    }

    /// 🔴 The narrow case, and the only one that changes. A copy this node
    /// announced to the cluster is one the cluster has a say over, and with the
    /// registry unreachable nothing is left to enforce that `running` and
    /// `resuming` are never claimable. Proceeding here is how one sandbox comes
    /// to be live twice.
    #[tokio::test]
    async fn an_unreachable_registry_refuses_a_resume_for_a_copy_the_cluster_knows_about() {
        let root = tempfile::tempdir().unwrap();
        let sandbox_id = seed_paused_record(root.path(), Some(SELF)).await;
        let api = api_rooted(root.path(), Arc::new(CountingRegistry::unreachable())).await;
        let recorder = crate::logging::capture::Recorder::default();
        let _guard = recorder.install();

        assert!(matches!(
            api.arbitrate_resume(sandbox_id).await,
            ResumeArbitration::Unavailable { .. }
        ));
        assert!(
            recorder.saw(tracing::Level::WARN, "refusing rather than risking"),
            "the refusal has to be findable: {:?}",
            recorder.events()
        );
    }

    /// 🔴 What the refusal actually looks like to the only thing that reads it.
    ///
    /// The guardrail is worth nothing if the refusal reaches the caller as a
    /// 404: the downstream contract for a 404 on this route is "the sandbox is
    /// gone, rebuild it", which resets the user's workspace to its template —
    /// exactly the outcome the guardrail exists to prevent, arrived at by a
    /// different road. So the status is pinned here, along with the wording,
    /// which is deliberately unique to this branch.
    ///
    /// 500 rather than 503 is on purpose; see the branch's own comment. The
    /// assertion is that it is *not a 404 and not a success*, not that it is
    /// the number 500 for its own sake.
    #[tokio::test]
    async fn a_resume_nobody_could_arbitrate_is_retryable_not_a_missing_sandbox() {
        use agentenv_http_server::apis::sandboxes::{
            Sandboxes, SandboxesSandboxIdResumePostResponse,
        };
        use agentenv_http_server::models;

        let root = tempfile::tempdir().unwrap();
        let sandbox_id = seed_paused_record(root.path(), Some(SELF)).await;
        let api = api_rooted(root.path(), Arc::new(CountingRegistry::unreachable())).await;

        let answer = api
            .sandboxes_sandbox_id_resume_post(
                &http::Method::POST,
                &headers::Host::from(http::uri::Authority::from_static("localhost")),
                &axum_extra::extract::CookieJar::new(),
                &super::super::Claims,
                &models::SandboxesSandboxIdResumePostPathParams {
                    sandbox_id: sandbox_id.to_string(),
                },
                &models::ResumedSandbox::new(),
            )
            .await
            .expect("the handler answers rather than failing the request");

        let SandboxesSandboxIdResumePostResponse::Status500_ServerError(error) = answer else {
            panic!("an unarbitrated resume must be retryable, got {answer:?}");
        };
        assert!(
            error
                .message
                .contains("cannot determine whether the sandbox is live elsewhere"),
            "the refusal has to say what happened: {error:?}"
        );
    }

    /// The other side of the narrowing. A copy the cluster was never told about
    /// is the only copy there is, so the registry's silence about it carries no
    /// information and refusing would fail a resume that was never at risk.
    #[tokio::test]
    async fn an_unreachable_registry_still_proceeds_for_a_copy_the_cluster_never_saw() {
        let root = tempfile::tempdir().unwrap();
        let sandbox_id = seed_paused_record(root.path(), None).await;
        let api = api_rooted(root.path(), Arc::new(CountingRegistry::unreachable())).await;

        // The record has to still be there, or this passes for the wrong
        // reason — a store that discarded it answers the same way.
        assert_eq!(
            api.orchestrator()
                .paused_record_cluster_registration(sandbox_id)
                .await
                .expect("the seeded record should have survived startup"),
            ClusterRegistration::Never
        );
        assert!(matches!(
            api.arbitrate_resume(sandbox_id).await,
            ResumeArbitration::Proceed
        ));
    }

    /// Nothing local at all: there is no copy here to duplicate, so the resume
    /// goes on to discover that for itself.
    #[tokio::test]
    async fn an_unreachable_registry_still_proceeds_when_this_node_holds_nothing() {
        let api = api_over(Arc::new(CountingRegistry::unreachable())).await;

        assert!(matches!(
            api.arbitrate_resume(crate::types::SandboxId::new()).await,
            ResumeArbitration::Proceed
        ));
    }

    /// A node-local registry has no say at all, so an unreachable one cannot
    /// arise and the resume never asks.
    #[tokio::test]
    async fn a_node_local_registry_never_refuses_a_resume() {
        let api = api_over(Arc::new(DisabledPausedSandboxRegistry)).await;

        assert!(matches!(
            api.arbitrate_resume(crate::types::SandboxId::new()).await,
            ResumeArbitration::Proceed
        ));
    }

    /// The four things a local record can say, and what each is worth when the
    /// registry cannot be reached.
    #[test]
    fn only_a_copy_announced_under_a_known_identity_refuses_a_resume() {
        let sandbox_id = crate::types::SandboxId::new();

        assert!(matches!(
            unreachable_arbitration(
                Some(ClusterRegistration::As(SELF.to_string())),
                sandbox_id,
                "no route to host"
            ),
            ResumeArbitration::Unavailable { .. }
        ));
        // Never announced: the local copy is the only copy.
        assert!(matches!(
            unreachable_arbitration(Some(ClusterRegistration::Never), sandbox_id, "no route"),
            ResumeArbitration::Proceed
        ));
        // Announced by a build that did not store the identity: the row cannot
        // be judged against it either, which is how `supersession` treats it.
        assert!(matches!(
            unreachable_arbitration(Some(ClusterRegistration::Anonymous), sandbox_id, "no route"),
            ResumeArbitration::Proceed
        ));
        // No local record at all: nothing here to duplicate.
        assert!(matches!(
            unreachable_arbitration(None, sandbox_id, "no route"),
            ResumeArbitration::Proceed
        ));
    }

    /// 🔴 The second defence, failing in the same outage as the first. Keeping
    /// the copy is right — but "the row is still ours" and "nobody could be
    /// asked" are different reasons for the same silence, and only one of them
    /// means a resume is about to run unarbitrated.
    #[tokio::test]
    async fn a_registry_that_cannot_be_asked_says_so_before_keeping_the_local_copy() {
        let root = tempfile::tempdir().unwrap();
        let sandbox_id = seed_paused_record(root.path(), Some(SELF)).await;
        let api = api_rooted(root.path(), Arc::new(CountingRegistry::unreachable())).await;
        let recorder = crate::logging::capture::Recorder::default();
        let _guard = recorder.install();

        assert!(!api.discard_if_superseded(sandbox_id).await);
        assert!(
            recorder.saw(tracing::Level::DEBUG, "leaving it in place"),
            "the non-deletion has to be findable: {:?}",
            recorder.events()
        );
    }

    /// 🔴 One missed renewal is nothing; the configured TTL is held to three
    /// renewal intervals so that two may be missed. It is the run that matters,
    /// and no single failure can report one.
    #[tokio::test]
    async fn consecutive_failed_renewals_are_counted() {
        let api = api_over(Arc::new(CountingRegistry::unreachable())).await;

        api.renew_paused_leases().await;
        assert_eq!(api.paused.consecutive_renew_failures(), 1);
        api.renew_paused_leases().await;
        api.renew_paused_leases().await;
        assert_eq!(api.paused.consecutive_renew_failures(), 3);
    }

    /// A run that has ended is not a run: the count is of failures *in a row*,
    /// so one renewal landing clears whatever came before it.
    #[tokio::test]
    async fn a_renewal_that_lands_ends_the_run() {
        let api = api_over(Arc::new(CountingRegistry::unreachable_for(2))).await;

        api.renew_paused_leases().await;
        api.renew_paused_leases().await;
        assert_eq!(api.paused.consecutive_renew_failures(), 2);

        api.renew_paused_leases().await;
        assert_eq!(api.paused.consecutive_renew_failures(), 0);
    }

    /// The single-node default. There is no cluster to hand anything back to,
    /// so this must settle without a retry task ever being spawned.
    #[tokio::test]
    async fn a_node_local_registry_has_nothing_to_release() {
        let api = api_over(Arc::new(DisabledPausedSandboxRegistry)).await;

        assert_eq!(
            api.release_stale_node_holdings().await,
            StaleReleaseOutcome::Released
        );
    }

    /// Runs a retry loop that is supposed to finish, and fails the test rather
    /// than hanging the suite if it does not.
    ///
    /// Under `start_paused` the clock advances whenever the runtime idles, so a
    /// loop that never settles burns virtual time as fast as the CPU allows and
    /// would otherwise spin until somebody killed the run. That is precisely
    /// what removing the fence produces, so the bound is what turns it into a
    /// readable failure.
    async fn run_bounded(work: impl std::future::Future<Output = ()>) {
        tokio::time::timeout(Duration::from_secs(600), work)
            .await
            .expect("the retry loop should settle rather than run forever");
    }

    /// The scheduler-is-rolling case: the release fails at startup and lands on
    /// a later attempt, without anyone having restarted the node.
    #[tokio::test(start_paused = true)]
    async fn the_retry_keeps_going_until_the_release_lands() {
        let registry = Arc::new(CountingRegistry::new(3, false));
        let api = api_over(Arc::clone(&registry) as Arc<dyn PausedSandboxRegistry>).await;

        assert_eq!(
            api.release_stale_node_holdings().await,
            StaleReleaseOutcome::Failed
        );

        run_bounded(api.retry_stale_node_holdings_release()).await;

        assert_eq!(
            registry.release_calls(),
            4,
            "three failures then the one that landed"
        );
    }

    /// 🔴 The retry is bounded by the fence, not by a count or a clock. A node
    /// that has taken a sandbox live must never release rows by node identity
    /// again, however badly the earlier attempt failed.
    #[tokio::test(start_paused = true)]
    async fn the_retry_stops_once_this_node_holds_a_sandbox() {
        let registry = Arc::new(CountingRegistry::always_failing());
        let api = api_over(Arc::clone(&registry) as Arc<dyn PausedSandboxRegistry>).await;

        assert_eq!(
            api.release_stale_node_holdings().await,
            StaleReleaseOutcome::Failed
        );
        api.paused.note_taking_sandbox_live().await;

        run_bounded(api.retry_stale_node_holdings_release()).await;

        assert_eq!(
            registry.release_calls(),
            1,
            "only the startup attempt; the retry must not reach the registry"
        );
    }

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
            metadata: Some(SandboxMetadata::default()),
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

    /// The case the running pass exists for: this node was partitioned, its
    /// lease lapsed, another node legitimately took the sandbox over, and the
    /// partition then healed with the original VM still running. Two live
    /// copies of one sandbox until this fires.
    #[test]
    fn a_running_row_naming_another_node_supersedes_our_live_copy() {
        let superseded = running_supersession(
            Some(&entry(PausedRegistryState::Running, OTHER, None)),
            SELF,
        );

        assert!(matches!(superseded, Some(Superseded::HeldBy(node)) if node == OTHER));
    }

    /// The takeover ran on and paused the sandbox before this node noticed.
    /// Every parked state answers the same way — whoever the row names owns it.
    #[test]
    fn a_parked_row_naming_another_node_supersedes_our_live_copy() {
        for state in [
            PausedRegistryState::Paused,
            PausedRegistryState::Publishing,
            PausedRegistryState::LocalOnly,
        ] {
            assert!(
                matches!(
                    running_supersession(Some(&entry(state, OTHER, None)), SELF),
                    Some(Superseded::HeldBy(_))
                ),
                "{state:?} on another node should supersede our running copy"
            );
        }
    }

    /// Mid-takeover. `origin_node_id` still points here because the artifacts
    /// are here, so only the claimer can answer — exactly as in the paused half.
    #[test]
    fn a_claim_by_another_node_supersedes_our_live_copy() {
        let superseded = running_supersession(
            Some(&entry(PausedRegistryState::Resuming, SELF, Some(OTHER))),
            SELF,
        );

        assert!(matches!(superseded, Some(Superseded::ClaimedBy(node)) if node == OTHER));
    }

    /// The ordinary case, and by far the most common: our row, our sandbox.
    /// Firing here would tear down a healthy sandbox on every reconcile.
    #[test]
    fn our_own_running_row_is_not_superseded() {
        assert!(
            running_supersession(Some(&entry(PausedRegistryState::Running, SELF, None)), SELF)
                .is_none()
        );
    }

    /// This node claimed it and is bringing it back up. `origin_node_id` may
    /// still name the node the artifacts came from, so judging by origin alone
    /// would have this node tear down the sandbox it is in the middle of
    /// resuming.
    #[test]
    fn our_own_claim_is_not_superseded() {
        assert!(running_supersession(
            Some(&entry(PausedRegistryState::Resuming, OTHER, Some(SELF))),
            SELF
        )
        .is_none());
    }

    /// A row this node was confirmed the holder of, now absent: the sandbox was
    /// removed cluster-wide while this node could not see it.
    #[test]
    fn a_vanished_row_supersedes_our_live_copy() {
        assert!(matches!(
            running_supersession(None, SELF),
            Some(Superseded::Gone)
        ));
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

    /// 🔴 The regression that would break every ordinary resume. A node that
    /// holds a sandbox is routinely told "held by X" where X is itself, and
    /// treating that as a refusal would leave it unable to resume its own
    /// sandboxes while another node, seeing no local copy, could not resume
    /// them either.
    #[test]
    fn an_answer_naming_this_node_is_not_a_refusal() {
        for claim in [
            ResumeClaim::NotReady {
                origin_node_id: SELF.to_string(),
            },
            ResumeClaim::Conflict {
                origin_node_id: SELF.to_string(),
            },
        ] {
            assert!(
                matches!(arbitration(claim, SELF), ResumeArbitration::Proceed),
                "a node must not be blocked from resuming by its own hold"
            );
        }
    }

    /// The second-copy case. Another node holding the sandbox is the one answer
    /// that must stop a local resume dead, however resumable the local copy
    /// looks.
    #[test]
    fn another_node_holding_the_sandbox_blocks_a_local_resume() {
        assert!(matches!(
            arbitration(
                ResumeClaim::Conflict {
                    origin_node_id: OTHER.to_string()
                },
                SELF
            ),
            ResumeArbitration::Blocked { .. }
        ));
        assert!(matches!(
            arbitration(
                ResumeClaim::NotReady {
                    origin_node_id: OTHER.to_string()
                },
                SELF
            ),
            ResumeArbitration::NotReady { .. }
        ));
    }

    /// A sandbox the cluster does not track is nobody's business but this
    /// node's, so the registry must not stand in the way of resuming it.
    #[test]
    fn an_untracked_sandbox_resumes_without_arbitration() {
        assert!(matches!(
            arbitration(ResumeClaim::NotFound, SELF),
            ResumeArbitration::Proceed
        ));
    }

    /// The claim carries the row so the rebuild can use it directly. Claiming
    /// again would find this node's own fresh claim in the way and deadlock the
    /// resume against itself.
    #[test]
    fn a_granted_claim_carries_the_row_for_the_rebuild() {
        let row = entry(PausedRegistryState::Paused, SELF, None);
        let snapshot = row.snapshot_id.clone();

        let claim = ResumeClaim::Claimed {
            entry: Box::new(row),
            previous_state: PausedRegistryState::Paused,
        };
        let ResumeArbitration::Held(held) = arbitration(claim, SELF) else {
            panic!("a granted claim must be held");
        };

        assert_eq!(held.snapshot_id, snapshot);
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

    /// 唯一允许回 404 的输入：集群也没有这一行。
    #[test]
    fn no_registry_row_is_the_only_missing_verdict() {
        assert!(matches!(
            missing_local_verdict(None, None, "self"),
            MissingLocalVerdict::Unknown
        ));
    }

    /// 并发 resume 的输家：本节点持有认领权（是赢家那一发拿的），本地还没成品。
    /// 必须等，不能答"不存在" —— 这是 2026-08-18 实测那个 404 的正解。
    #[test]
    fn a_resume_in_flight_on_this_node_is_waited_for_not_reported_missing() {
        let row = entry(PausedRegistryState::Resuming, "other", Some("self"));
        assert!(matches!(
            missing_local_verdict(Some(&row), None, "self"),
            MissingLocalVerdict::Wait { .. }
        ));
    }

    /// 赢家落定之后，输家从本地 store 读回同一台 —— 与 e2b 输家读回赢家结果同义。
    #[test]
    fn a_running_local_copy_settles_the_wait() {
        let row = entry(PausedRegistryState::Resuming, "other", Some("self"));
        let local = SandboxMetadata {
            state: SandboxState::Running,
            ..Default::default()
        };
        assert!(matches!(
            missing_local_verdict(Some(&row), Some(&local), "self"),
            MissingLocalVerdict::Ready
        ));
    }

    /// 任何"别人管着它"的行都不是 404：等在本节点等不到，交给调用方重试。
    #[test]
    fn rows_held_elsewhere_are_busy_never_missing() {
        for row in [
            entry(PausedRegistryState::Resuming, "other", Some("other")),
            entry(PausedRegistryState::Running, "other", None),
            entry(PausedRegistryState::Paused, "other", None),
            entry(PausedRegistryState::Publishing, "other", None),
            entry(PausedRegistryState::LocalOnly, "other", None),
        ] {
            let verdict = missing_local_verdict(Some(&row), None, "self");
            assert!(
                matches!(verdict, MissingLocalVerdict::Busy { .. }),
                "state {:?} must not be reported as missing",
                row.state
            );
        }
    }

    /// 我们自己的 paused 行同样不是 404：说明刚输掉一次认领而赢家已释放，重试即可。
    #[test]
    fn our_own_paused_row_is_busy_not_missing() {
        let row = entry(PausedRegistryState::Paused, "self", None);
        assert!(matches!(
            missing_local_verdict(Some(&row), None, "self"),
            MissingLocalVerdict::Busy { .. }
        ));
    }
}
