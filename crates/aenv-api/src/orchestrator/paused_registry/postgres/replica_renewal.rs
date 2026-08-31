//! Per-replica, unelected renewal from each replica's heartbeat roster.
//! SQL rechecks every `(sandbox, node)` pair, so the union of independently
//! pinned rosters safely renews the whole fleet.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use tracing::{debug, warn};

use super::lease::{renew_live_leases, renew_parked_leases, LeaseHolder};
use super::PostgresPausedSandboxRegistry;
use crate::node_registry::registry::NodeRegistry;
use crate::node_registry::types::Roster;
use crate::types::SandboxId;

const ROSTER_FRESH_TTL: Duration = crate::node_registry::registry::DEFAULT_OBSERVED_REPORT_TTL;

/// Per-replica renewal cadence.
pub const RENEWAL_INTERVAL: Duration = Duration::from_secs(10);

/// Builds renewal candidates from fresh heartbeat rosters.
pub fn candidates_from_rosters(rosters: &[Roster], now: SystemTime) -> Vec<LeaseHolder> {
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

/// Runs one parked and live renewal pass from this replica's roster.
pub async fn renew_once(
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

/// Starts the unelected, abort-safe renewal loop for this replica.
///
/// Each tick issues only idempotent conditional updates.
pub fn spawn(
    registry: Arc<PostgresPausedSandboxRegistry>,
    node_registry: Arc<dyn NodeRegistry>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(RENEWAL_INTERVAL);
        loop {
            // Run immediately so an existing fresh roster is renewed without one-cycle delay.
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
                    paused: false,
                })
                .collect(),
            last_seen,
        }
    }

    #[test]
    fn a_fresh_roster_entry_is_a_renewal_candidate() {
        let id = SandboxId::new();
        let r = roster("node-a", &[id], Some(SystemTime::now()));

        let candidates = candidates_from_rosters(&[r], SystemTime::now());
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].sandbox_id, id);
        assert_eq!(candidates[0].node_id, "node-a");
    }

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

    #[test]
    fn a_roster_with_no_last_seen_is_not_a_renewal_source() {
        let id = SandboxId::new();
        let r = roster("node-a", &[id], None);

        let candidates = candidates_from_rosters(&[r], SystemTime::now());
        assert!(candidates.is_empty());
    }

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
                    paused: false,
                },
                RosterEntry {
                    sandbox_id: good.to_string(),
                    execution_id: Uuid::new_v4().to_string(),
                    projection_ttl: Duration::from_secs(30),
                    paused: false,
                },
            ],
            last_seen: Some(now),
        };

        let candidates = candidates_from_rosters(&[r], now);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].sandbox_id, good);
    }
}
