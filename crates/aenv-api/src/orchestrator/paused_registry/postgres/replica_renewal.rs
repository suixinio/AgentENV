//! B1: the per-replica, **unelected** half of D2 Fix A (`151d00b`) --
//! proactively renewing `running`/parked-state leases from a fresh heartbeat
//! roster, the path that never existed before Fix A and that the split
//! node/api identity model makes mandatory rather than optional
//! (`renew_lease`'s own `Running` branch never matches an api replica's pod
//! identity -- see `lease.rs`'s doc).
//!
//! # Why this cannot live in the reconcile leader loop
//!
//! It used to (`super::reconcile`, before this fix): Fix A ran only on
//! whichever replica held the `AdvisoryLockKey::PausedRegistryReconcile`
//! lock, reading that one replica's own
//! [`crate::node_registry::registry::NodeRegistry::rosters_in_cluster`]
//! answer. That answer is **per-process, in-memory, with no synchronisation
//! between replicas** (`AtomicNodeRegistry`, `src/node_registry/registry.rs`)
//! -- a node's gRPC heartbeat is a long-lived HTTP/2 stream pinned to
//! whichever one `agentenv-api` Pod it dialled, so a replica that is not the
//! reconcile leader has heartbeats for nodes the leader's own roster answer
//! never mentions. Any node whose heartbeat was not pinned to the reconcile
//! leader had, under that design, no renewal path for its `running` rows at
//! all -- silently reintroducing the "running row lease freeze" failure Fix A
//! exists to prevent, just steady-state (every deploy where the leader is not
//! also the node's own heartbeat sink) rather than only during a failover.
//!
//! # The fix: run it everywhere, unelected, and let the union cover the fleet
//!
//! [`renew_parked_leases`]/[`renew_live_leases`] (`super::lease`) are
//! idempotent, caller-asserted `UPDATE`s -- their own `WHERE` re-checks the
//! asserted `(sandbox_id, node_id)` pair against the row's own
//! `origin_node_id` before writing anything (see their own doc comments), so
//! nothing about them ever required the caller to hold any lock, let alone
//! cluster-wide leadership. [`spawn`] below runs this on **every**
//! `--role api` replica that has a [`NodeRegistry`], on its own timer,
//! contending for nothing. Since every node's heartbeat is pinned to exactly
//! one replica, the *union* of what every replica's own roster covers is the
//! entire cluster -- see `postgres::contract`'s pg-gated
//! `two_replicas_each_holding_part_of_the_roster_together_renew_every_running_row`
//! for this claim proved directly.
//!
//! `--role all` never reaches this module at all: [`spawn`] is only called
//! when a [`NodeRegistry`] is available (M1's `spawn_background_tasks`
//! taking `Option<Arc<dyn NodeRegistry>>`), and under `--role all` the
//! process's own identity coincides with `origin_node_id` for everything it
//! runs -- `renew_lease` (the ordinary trait method, driven by
//! `spawn_paused_record_upkeep` in `src/bin/aenv-api.rs`) already renews those
//! rows under matching identity, so Fix A has nothing to add there.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use tracing::{debug, warn};

use super::lease::{renew_live_leases, renew_parked_leases, LeaseHolder};
use super::PostgresPausedSandboxRegistry;
use crate::node_registry::registry::NodeRegistry;
use crate::node_registry::types::Roster;
use crate::types::SandboxId;

/// `defaultObservedReportTTL` (Stage A, `src/node_registry/registry.rs`) --
/// reused rather than re-declared: a heartbeat roster older than this is
/// exactly as stale for this module's purposes as it is for Stage A's own
/// `NodeStatus` derivation.
const ROSTER_FRESH_TTL: Duration = crate::node_registry::registry::DEFAULT_OBSERVED_REPORT_TTL;

/// How often each replica renews from its own roster.
///
/// Deliberately independent of `reconcile_interval`/`reclaim_interval`: this
/// is not leader-elected work sharing those loops' pacing concerns, just a
/// cheap, purely local operation every replica repeats on its own schedule.
/// Short on purpose -- a `running` row's lease should reflect a healthy
/// node's heartbeat promptly, not lag behind it by up to a full reconcile
/// interval.
pub(super) const RENEWAL_INTERVAL: Duration = Duration::from_secs(10);

/// Every `(sandbox, node)` pair worth attempting a renewal for, built from
/// `rosters` alone -- no database read required. A pure function, testable
/// without either a database or a real [`NodeRegistry`] (see
/// `#[cfg(test)] mod tests` below).
///
/// One list feeds both [`renew_parked_leases`] and [`renew_live_leases`]:
/// each statement's own `WHERE` already restricts itself to the rows in its
/// state (`publishing`/`local_only` vs `running`), so attempting a
/// `(sandbox, node)` pair against the "wrong" statement simply matches zero
/// rows rather than doing anything unsafe -- there is no need to know a
/// row's current state at this layer to decide which statement it belongs
/// to.
pub(super) fn candidates_from_rosters(rosters: &[Roster], now: SystemTime) -> Vec<LeaseHolder> {
    let mut out = Vec::new();
    for roster in rosters {
        let Some(last_seen) = roster.last_seen else {
            continue;
        };
        let fresh = now
            .duration_since(last_seen)
            .map(|age| age <= ROSTER_FRESH_TTL)
            .unwrap_or(true); // last_seen in the future (clock skew): treat as fresh.
        if !fresh {
            continue;
        }
        for entry in &roster.entries {
            let Ok(sandbox_id) = SandboxId::parse_str(&entry.sandbox_id) else {
                warn!(
                    target: "agentenv",
                    node_id = %roster.node_id,
                    sandbox_id = %entry.sandbox_id,
                    "paused registry replica renewal: skipping a roster entry with an unparseable sandbox id"
                );
                continue;
            };
            out.push(LeaseHolder {
                sandbox_id,
                node_id: roster.node_id.clone(),
            });
        }
    }
    out
}

/// One renewal pass using this replica's own roster: `(parked_renewed,
/// live_renewed)`.
pub(super) async fn renew_once(
    registry: &PostgresPausedSandboxRegistry,
    node_registry: &dyn NodeRegistry,
) -> anyhow::Result<(u64, u64)> {
    let rosters = node_registry.rosters_in_cluster(&registry.cluster_id.to_string());
    let candidates = candidates_from_rosters(&rosters, SystemTime::now());
    if candidates.is_empty() {
        return Ok((0, 0));
    }

    let parked = renew_parked_leases(registry, &candidates)
        .await
        .map_err(anyhow::Error::from)?;
    let live = renew_live_leases(registry, &candidates)
        .await
        .map_err(anyhow::Error::from)?;
    Ok((parked, live))
}

/// Starts this replica's own renewal loop. **Not** leader-elected -- every
/// replica that calls this runs it independently, contending for nothing
/// (see the module doc). Returns a plain [`tokio::task::JoinHandle`] rather
/// than a [`crate::pg::SingletonTaskHandle`]: there is no advisory lock to
/// release on shutdown, so this belongs in `Assembly::upkeep`
/// (`src/bin/aenv-api.rs`), which simply aborts it -- safe here since every
/// statement this loop issues is a single idempotent `UPDATE`, and aborting
/// mid-tick loses at most one renewal cycle's worth of freshness, not
/// correctness.
pub(super) fn spawn(
    registry: Arc<PostgresPausedSandboxRegistry>,
    node_registry: Arc<dyn NodeRegistry>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(RENEWAL_INTERVAL);
        loop {
            // `tokio::time::interval` fires its first `tick()` immediately --
            // deliberately not skipped here (unlike
            // `spawn_paused_record_upkeep`'s ticker in `src/bin/aenv-api.rs`):
            // a freshly started replica's roster may already hold entries
            // this pass should act on right away, not after waiting out a
            // full interval first.
            ticker.tick().await;
            match renew_once(&registry, node_registry.as_ref()).await {
                Ok((parked, live)) if parked > 0 || live > 0 => {
                    debug!(
                        target: "agentenv",
                        cluster_id = %registry.cluster_id,
                        parked_leases_renewed = parked,
                        live_leases_renewed = live,
                        "paused registry replica renewal pass"
                    );
                }
                Ok(_) => {}
                Err(err) => {
                    warn!(target: "agentenv", error = %err, "paused registry replica renewal pass failed");
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;
    use crate::node_registry::types::RosterEntry;

    fn roster(node_id: &str, sandbox_ids: &[SandboxId], last_seen: Option<SystemTime>) -> Roster {
        Roster {
            node_id: node_id.to_string(),
            entries: sandbox_ids
                .iter()
                .map(|id| RosterEntry {
                    sandbox_id: id.to_string(),
                    execution_id: Uuid::new_v4().to_string(),
                    projection_ttl: Duration::from_secs(30),
                })
                .collect(),
            last_seen,
        }
    }

    /// The base case: a fresh roster's entries all become candidates.
    #[test]
    fn a_fresh_roster_entry_is_a_renewal_candidate() {
        let id = SandboxId::new();
        let r = roster("node-a", &[id], Some(SystemTime::now()));

        let candidates = candidates_from_rosters(&[r], SystemTime::now());
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].sandbox_id, id);
        assert_eq!(candidates[0].node_id, "node-a");
    }

    /// A roster older than [`ROSTER_FRESH_TTL`] contributes nothing -- a
    /// stale heartbeat is not evidence the node is still alive.
    #[test]
    fn a_stale_roster_is_not_a_renewal_source() {
        let id = SandboxId::new();
        let r = roster(
            "node-a",
            &[id],
            Some(SystemTime::now() - Duration::from_secs(120)),
        );

        let candidates = candidates_from_rosters(&[r], SystemTime::now());
        assert!(candidates.is_empty());
    }

    /// A roster with no heartbeat at all (`last_seen: None`) is the same as
    /// absent for this purpose -- a node this replica has never actually
    /// heard from is not evidence of anything.
    #[test]
    fn a_roster_with_no_last_seen_is_not_a_renewal_source() {
        let id = SandboxId::new();
        let r = roster("node-a", &[id], None);

        let candidates = candidates_from_rosters(&[r], SystemTime::now());
        assert!(candidates.is_empty());
    }

    /// The union claim at the pure-function level: two independent rosters
    /// (as two independent replicas would each hold) each contribute their
    /// own entries -- nothing here special-cases "only the first roster" or
    /// drops entries when more than one roster is present.
    #[test]
    fn candidates_from_multiple_rosters_cover_every_one_of_them() {
        let id_a = SandboxId::new();
        let id_b = SandboxId::new();
        let now = SystemTime::now();
        let rosters = vec![
            roster("node-a", &[id_a], Some(now)),
            roster("node-b", &[id_b], Some(now)),
        ];

        let candidates = candidates_from_rosters(&rosters, now);
        assert_eq!(candidates.len(), 2);
        assert!(candidates
            .iter()
            .any(|c| c.sandbox_id == id_a && c.node_id == "node-a"));
        assert!(candidates
            .iter()
            .any(|c| c.sandbox_id == id_b && c.node_id == "node-b"));
    }

    /// An unparseable sandbox id in a roster entry must not poison the rest
    /// of that roster's own candidates -- mirrors B6's "one bad row must not
    /// take the batch down with it" for this module's own input shape.
    #[test]
    fn an_unparseable_sandbox_id_is_skipped_without_dropping_its_siblings() {
        let good = SandboxId::new();
        let now = SystemTime::now();
        let r = Roster {
            node_id: "node-a".to_string(),
            entries: vec![
                RosterEntry {
                    sandbox_id: "not-a-uuid".to_string(),
                    execution_id: Uuid::new_v4().to_string(),
                    projection_ttl: Duration::from_secs(30),
                },
                RosterEntry {
                    sandbox_id: good.to_string(),
                    execution_id: Uuid::new_v4().to_string(),
                    projection_ttl: Duration::from_secs(30),
                },
            ],
            last_seen: Some(now),
        };

        let candidates = candidates_from_rosters(&[r], now);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].sandbox_id, good);
    }
}
