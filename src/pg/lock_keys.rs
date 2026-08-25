//! The one place every `pg_advisory_lock`/`pg_try_advisory_lock` key this
//! process ever takes is enumerated, so two independent singleton tasks can
//! never be handed the same key by accident.
//!
//! 🔴 Distinct namespace from `schemaLockKey`
//! (`services/scheduler/internal/registry/migrate.go`,
//! `services/scheduler/internal/catalog/migrate.go` — both share the literal
//! value `0x0A6E_7653_4348_4D41`). That key protects one-shot schema DDL,
//! held for the duration of a single migration and then released; the keys
//! below protect long-held cluster *leadership* — one replica holds one for
//! as long as it stays leader, which can be the process's whole lifetime.
//! Different purpose, different call sites, but the same 64-bit key space in
//! the same PostgreSQL database, so collision is a correctness bug and not
//! just a style one. `schemaLockKey`'s value is a specific, large,
//! hand-picked 64-bit pattern; every key here is a small hand-enumerated
//! integer, chosen from a disjoint range on purpose so the two namespaces can
//! never collide even by coincidence. `lock_keys_never_collide_with_the_go_schema_lock`
//! pins this.
//!
//! Adding a key: append a new variant with the next integer and a doc comment
//! naming the Stage and the Go RPC/loop it folds. Never reuse a retired
//! variant's integer — leave it retired rather than recycling the number, the
//! same way protobuf field numbers are never reused.

/// A `pg_advisory_lock`/`pg_try_advisory_lock` key reserved for one
/// cluster-wide singleton background task.
///
/// Every variant is a distinct 64-bit session-scoped advisory lock key.
/// Nothing here allocates or configures anything — see
/// [`crate::pg::election::spawn_singleton_task`] for what actually takes the
/// lock.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i64)]
pub enum AdvisoryLockKey {
    /// Reserved for Stage B: the catalog build reaper, folded from Go's
    /// `SnapshotCatalogService.RunBuildReaper`
    /// (`services/scheduler/cmd/main.go`) — today the Go scheduler's only
    /// singleton background task, per `cmd/main.go`'s "there is exactly one
    /// owner of this table's shape" comment.
    CatalogBuildReaper = 1,
    /// Reserved for Stage C: the paused-registry reconcile loop, folded from
    /// Go's `Service.RunRegistryReconcile`.
    PausedRegistryReconcile = 2,
    /// Reserved for Stage C: the paused-registry reclaim loop, folded from
    /// Go's `PausedRegistryService.RunReclaim`
    /// (`ReclaimExpiredHoldings`'s caller).
    PausedRegistryReclaim = 3,
    /// Reserved for Stage C: the restart-grace lease-extension pass, folded
    /// from Go's `Grace.ExtendLeases`. N replicas starting at once must not
    /// each add their own downtime estimate to every lease in the cluster —
    /// exactly the failure this whole primitive exists to prevent for this
    /// one caller.
    PausedRegistryRestartGrace = 4,
}

impl AdvisoryLockKey {
    /// The raw key passed to `pg_try_advisory_lock` / `pg_advisory_unlock`.
    pub const fn as_i64(self) -> i64 {
        self as i64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every variant has to resolve to a distinct key, or two singleton tasks
    /// sharing one number would each believe it is exclusive when it is not
    /// — the one failure mode this whole module exists to rule out.
    #[test]
    fn every_key_is_distinct() {
        let keys = [
            AdvisoryLockKey::CatalogBuildReaper,
            AdvisoryLockKey::PausedRegistryReconcile,
            AdvisoryLockKey::PausedRegistryReclaim,
            AdvisoryLockKey::PausedRegistryRestartGrace,
        ];
        let mut seen = std::collections::HashSet::new();
        for key in keys {
            assert!(
                seen.insert(key.as_i64()),
                "{key:?} = {} collides with another variant",
                key.as_i64()
            );
        }
    }

    /// The other namespace sharing this key space: `schemaLockKey` in both
    /// `services/scheduler/internal/registry/migrate.go` and
    /// `services/scheduler/internal/catalog/migrate.go`. If that constant
    /// ever changes, this is the test that has to be re-checked against the
    /// new value — not silently reused.
    #[test]
    fn lock_keys_never_collide_with_the_go_schema_lock() {
        const GO_SCHEMA_LOCK_KEY: i64 = 0x0A6E_7653_4348_4D41;
        for key in [
            AdvisoryLockKey::CatalogBuildReaper,
            AdvisoryLockKey::PausedRegistryReconcile,
            AdvisoryLockKey::PausedRegistryReclaim,
            AdvisoryLockKey::PausedRegistryRestartGrace,
        ] {
            assert_ne!(
                key.as_i64(),
                GO_SCHEMA_LOCK_KEY,
                "{key:?} collides with schemaLockKey"
            );
        }
    }
}
