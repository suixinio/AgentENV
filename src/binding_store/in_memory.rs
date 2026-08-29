//! Task's own "D3": ports `services/scheduler/internal/store.go`'s
//! `InMemoryBindingStore` (lines 195-501) — a single-replica, in-process
//! binding store. Used directly by a single-node deployment,
//! and as the cheap half of [`super::contract`]'s dual-backend suite.

use std::collections::{HashMap, HashSet};
use std::sync::RwLock;
use std::time::SystemTime;

use async_trait::async_trait;

use super::arbitration::{arbitrate, BindingDecision};
use super::{Binding, BindingDeleteOutcome, BindingStore, BindingStoreError, BindingStoreSettings};
use crate::node_registry::types::{Node, RosterEntry};

#[derive(Debug, Clone)]
struct BindingRecord {
    node: Node,
    execution_id: String,
    expires_at: SystemTime,
}

/// Which caller drove a write — governs `keeps_deadline` (only a heartbeat
/// refresh of the same incarnation may ever preserve an existing deadline).
/// Mirrors Go's `bindingSourceAssignment`/`bindingSourceHeartbeat` string
/// constants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WriteSource {
    Assignment,
    Heartbeat,
}

struct Inner {
    bindings: HashMap<String, BindingRecord>,
    node_binding: HashMap<String, HashSet<String>>,
}

/// Ports Go's `InMemoryBindingStore`.
pub struct InMemoryBindingStore {
    inner: RwLock<Inner>,
    settings: BindingStoreSettings,
}

impl InMemoryBindingStore {
    pub fn new(settings: BindingStoreSettings) -> Self {
        Self {
            inner: RwLock::new(Inner {
                bindings: HashMap::new(),
                node_binding: HashMap::new(),
            }),
            settings,
        }
    }

    /// Ports `expiryFor`/`keepsDeadline` (`store.go:411-480`): whether a
    /// write may keep the existing record's deadline (`held` must be true,
    /// i.e. the existing record has not expired) rather than arm a fresh
    /// one.
    fn keeps_deadline(
        &self,
        held: bool,
        incumbent: &str,
        binding: &Binding,
        source: WriteSource,
    ) -> bool {
        if !self.settings.projection_authoritative {
            return false;
        }
        if !held || source != WriteSource::Heartbeat {
            return false;
        }
        if binding.projection_ttl.is_zero() {
            return false;
        }
        !binding.execution_id.is_empty() && binding.execution_id == incumbent
    }

    fn expiry_for(
        &self,
        existing: Option<&BindingRecord>,
        held: bool,
        incumbent: &str,
        binding: &Binding,
        now: SystemTime,
        source: WriteSource,
    ) -> SystemTime {
        if self.keeps_deadline(held, incumbent, binding, source) {
            if let Some(existing) = existing {
                return existing.expires_at;
            }
        }
        let ttl = if !binding.projection_ttl.is_zero() {
            binding.projection_ttl
        } else {
            self.settings.binding_ttl
        };
        now + ttl
    }

    /// Ports `upsertLocked` (`store.go:375-409`): the core comparison-and-write
    /// critical section. A refused challenger changes nothing, not even the
    /// reverse index.
    fn upsert_locked(
        &self,
        inner: &mut Inner,
        sandbox_id: &str,
        binding: &Binding,
        now: SystemTime,
        source: WriteSource,
    ) -> BindingDecision {
        let existing = inner.bindings.get(sandbox_id).cloned();
        let mut held = existing.is_some();
        if held {
            if let Some(existing) = &existing {
                if existing.expires_at <= now {
                    held = false;
                }
            }
        }
        let incumbent = if held {
            existing
                .as_ref()
                .map(|e| e.execution_id.as_str())
                .unwrap_or("")
        } else {
            ""
        };

        let (accept, decision) = arbitrate(
            self.settings.arbitration,
            incumbent,
            held,
            &binding.execution_id,
        );
        if !accept {
            return decision;
        }

        if let Some(current) = inner.bindings.get(sandbox_id) {
            if current.node.id != binding.node.id {
                if let Some(set) = inner.node_binding.get_mut(&current.node.id) {
                    set.remove(sandbox_id);
                }
            }
        }
        inner
            .node_binding
            .entry(binding.node.id.clone())
            .or_default()
            .insert(sandbox_id.to_string());

        let expires_at = self.expiry_for(existing.as_ref(), held, incumbent, binding, now, source);
        inner.bindings.insert(
            sandbox_id.to_string(),
            BindingRecord {
                node: binding.node.clone(),
                execution_id: binding.execution_id.clone(),
                expires_at,
            },
        );
        decision
    }

    fn delete_locked(inner: &mut Inner, sandbox_id: &str) {
        if let Some(record) = inner.bindings.remove(sandbox_id) {
            if let Some(set) = inner.node_binding.get_mut(&record.node.id) {
                set.remove(sandbox_id);
            }
        }
    }
}

#[async_trait]
impl BindingStore for InMemoryBindingStore {
    async fn get(
        &self,
        sandbox_id: &str,
        now: SystemTime,
    ) -> Result<Option<Binding>, BindingStoreError> {
        let mut inner = self.inner.write().expect("binding store lock poisoned");
        let Some(record) = inner.bindings.get(sandbox_id) else {
            return Ok(None);
        };
        if record.expires_at <= now {
            Self::delete_locked(&mut inner, sandbox_id);
            return Ok(None);
        }
        Ok(Some(Binding {
            node: record.node.clone(),
            execution_id: record.execution_id.clone(),
            projection_ttl: std::time::Duration::ZERO,
        }))
    }

    async fn record(
        &self,
        sandbox_id: &str,
        binding: Binding,
        now: SystemTime,
    ) -> Result<BindingDecision, BindingStoreError> {
        let sandbox_id = sandbox_id.trim();
        if sandbox_id.is_empty() {
            return Ok(BindingDecision::NotArbitrated);
        }
        let mut inner = self.inner.write().expect("binding store lock poisoned");
        let decision = self.upsert_locked(
            &mut inner,
            sandbox_id,
            &binding,
            now,
            WriteSource::Assignment,
        );
        Ok(decision)
    }

    async fn reconcile_node(
        &self,
        node: Node,
        roster: Vec<RosterEntry>,
        now: SystemTime,
    ) -> Result<Vec<(String, BindingDecision)>, BindingStoreError> {
        // Normalize: trim, drop blanks, dedupe by last-write-wins — mirrors
        // `ReconcileNode`'s own normalization pass (`store.go:320-361`).
        let mut normalized: HashMap<String, RosterEntry> = HashMap::new();
        for entry in roster {
            let sandbox_id = entry.sandbox_id.trim().to_string();
            if sandbox_id.is_empty() {
                continue;
            }
            normalized.insert(
                sandbox_id.clone(),
                RosterEntry {
                    sandbox_id,
                    ..entry
                },
            );
        }

        let mut inner = self.inner.write().expect("binding store lock poisoned");

        if normalized.is_empty() {
            let owned: Vec<String> = inner
                .node_binding
                .get(&node.id)
                .map(|set| set.iter().cloned().collect())
                .unwrap_or_default();
            for sandbox_id in &owned {
                Self::delete_locked(&mut inner, sandbox_id);
            }
            return Ok(Vec::new());
        }

        let mut decisions = Vec::with_capacity(normalized.len());
        for (sandbox_id, entry) in &normalized {
            let binding = Binding {
                node: node.clone(),
                execution_id: entry.execution_id.clone(),
                projection_ttl: entry.projection_ttl,
            };
            let decision = self.upsert_locked(
                &mut inner,
                sandbox_id,
                &binding,
                now,
                WriteSource::Heartbeat,
            );
            decisions.push((sandbox_id.clone(), decision));
        }

        let stale: Vec<String> = inner
            .node_binding
            .get(&node.id)
            .map(|set| {
                set.iter()
                    .filter(|id| !normalized.contains_key(*id))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        for sandbox_id in stale {
            Self::delete_locked(&mut inner, &sandbox_id);
        }

        Ok(decisions)
    }

    async fn delete(
        &self,
        sandbox_id: &str,
        execution_id: &str,
        now: SystemTime,
    ) -> Result<BindingDeleteOutcome, BindingStoreError> {
        let sandbox_id = sandbox_id.trim();
        let execution_id = execution_id.trim();
        if sandbox_id.is_empty() || execution_id.is_empty() {
            return Ok(BindingDeleteOutcome::Absent);
        }
        let mut inner = self.inner.write().expect("binding store lock poisoned");
        let Some(record) = inner.bindings.get(sandbox_id) else {
            return Ok(BindingDeleteOutcome::Absent);
        };
        if record.expires_at <= now {
            Self::delete_locked(&mut inner, sandbox_id);
            return Ok(BindingDeleteOutcome::Absent);
        }
        if !record.execution_id.is_empty() && record.execution_id != execution_id {
            return Ok(BindingDeleteOutcome::RejectedStale);
        }
        let outcome = if record.execution_id.is_empty() {
            BindingDeleteOutcome::DeletedUnknownIncumbent
        } else {
            BindingDeleteOutcome::Deleted
        };
        Self::delete_locked(&mut inner, sandbox_id);
        Ok(outcome)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binding_store::arbitration::ArbitrationMode;
    use std::time::Duration;

    /// The shared contract, run against this backend. The Redis backend
    /// runs the identical list; a change made to one and forgotten for the
    /// other turns red here.
    mod contract {
        use super::super::InMemoryBindingStore;
        use crate::binding_store::BindingStoreSettings;

        async fn new_contract_store(_test: &str) -> Option<InMemoryBindingStore> {
            Some(InMemoryBindingStore::new(BindingStoreSettings::default()))
        }

        async fn new_contract_store_with_mode(
            _test: &str,
            mode: crate::binding_store::ArbitrationMode,
        ) -> Option<InMemoryBindingStore> {
            Some(InMemoryBindingStore::new(BindingStoreSettings {
                arbitration: mode,
                ..BindingStoreSettings::default()
            }))
        }

        crate::binding_store::contract::binding_store_contract!();
        crate::binding_store::contract::binding_store_arbitration_contract!();
    }

    fn unix(secs: u64) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(secs)
    }

    fn node(id: &str) -> Node {
        Node {
            id: id.to_string(),
            endpoint: format!("http://{id}"),
            pod_name: String::new(),
        }
    }

    fn settings() -> BindingStoreSettings {
        BindingStoreSettings {
            binding_ttl: Duration::from_secs(30),
            arbitration: ArbitrationMode::Fenced,
            projection_authoritative: false,
        }
    }

    #[tokio::test]
    async fn get_returns_none_after_expiry_and_removes_the_record() {
        let store = InMemoryBindingStore::new(BindingStoreSettings {
            binding_ttl: Duration::from_secs(10),
            ..settings()
        });
        store
            .record(
                "sbx-1",
                Binding {
                    node: node("node-a"),
                    ..Default::default()
                },
                unix(0),
            )
            .await
            .unwrap();
        assert!(store.get("sbx-1", unix(5)).await.unwrap().is_some());
        assert!(store.get("sbx-1", unix(11)).await.unwrap().is_none());
        // Expiry is a side-effecting delete, provable via the reverse index:
        // reconciling node-a with the same sandbox after the record expired
        // should look exactly like a fresh install (Installed, not
        // Refreshed/Superseded), because upsert_locked no longer sees a
        // held record.
        let decisions = store
            .reconcile_node(
                node("node-a"),
                vec![RosterEntry {
                    sandbox_id: "sbx-1".to_string(),
                    execution_id: "exec-1".to_string(),
                    projection_ttl: Duration::ZERO,
                    paused: false,
                }],
                unix(12),
            )
            .await
            .unwrap();
        assert_eq!(
            decisions,
            vec![("sbx-1".to_string(), BindingDecision::Installed)]
        );
    }

    #[tokio::test]
    async fn moving_a_binding_to_a_new_node_clears_the_old_reverse_index_entry() {
        let store = InMemoryBindingStore::new(settings());
        store
            .record(
                "sbx-1",
                Binding {
                    node: node("node-a"),
                    execution_id: "exec-1".to_string(),
                    projection_ttl: Duration::ZERO,
                },
                unix(0),
            )
            .await
            .unwrap();
        store
            .record(
                "sbx-1",
                Binding {
                    node: node("node-b"),
                    execution_id: "exec-2".to_string(),
                    projection_ttl: Duration::ZERO,
                },
                unix(1),
            )
            .await
            .unwrap();

        // node-a's roster no longer includes sbx-1: reconciling an empty
        // roster for node-a must not delete sbx-1 (it belongs to node-b
        // now) -- proving the reverse index was actually moved, not just
        // the primary record.
        let decisions = store
            .reconcile_node(node("node-a"), vec![], unix(2))
            .await
            .unwrap();
        assert!(decisions.is_empty());
        let binding = store
            .get("sbx-1", unix(2))
            .await
            .unwrap()
            .expect("still bound");
        assert_eq!(binding.node.id, "node-b");
    }

    #[tokio::test]
    async fn a_rejected_challenger_changes_nothing_not_even_the_reverse_index() {
        let store = InMemoryBindingStore::new(settings());
        store
            .record(
                "sbx-1",
                Binding {
                    node: node("node-a"),
                    execution_id: "exec-2".to_string(),
                    projection_ttl: Duration::ZERO,
                },
                unix(0),
            )
            .await
            .unwrap();
        let decision = store
            .record(
                "sbx-1",
                Binding {
                    node: node("node-b"),
                    execution_id: "exec-1".to_string(),
                    projection_ttl: Duration::ZERO,
                },
                unix(1),
            )
            .await
            .unwrap();
        assert_eq!(decision, BindingDecision::RejectedOlder);
        let binding = store
            .get("sbx-1", unix(1))
            .await
            .unwrap()
            .expect("unchanged");
        assert_eq!(
            binding.node.id, "node-a",
            "the rejected write must not have moved the node"
        );
    }
}
