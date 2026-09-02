//! Central registry for PostgreSQL advisory-lock keys.
//! Single-argument session and transaction locks share one 64-bit keyspace,
//! including the external Go schema and build-admission constants below.
//! Append new integer variants and never reuse retired values.

/// Session-scoped key reserved for one cluster-wide singleton task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i64)]
pub enum AdvisoryLockKey {
    /// Catalog build reaper.
    CatalogBuildReaper = 1,
}

impl AdvisoryLockKey {
    /// The raw key passed to `pg_try_advisory_lock` / `pg_advisory_unlock`.
    pub const fn as_i64(self) -> i64 {
        self as i64
    }
}

/// Shared schema-migration key owned by the Go and Rust appliers.
pub const GO_SCHEMA_LOCK_KEY: i64 = 0x0A6E_7653_4348_4D41;

/// Transaction-scoped catalog build-admission key shared with Go.
pub const GO_BUILD_ADMISSION_LOCK_KEY: i64 = 3_405_691_582;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_key_is_distinct() {
        let keys = [AdvisoryLockKey::CatalogBuildReaper];
        let mut seen = std::collections::HashSet::new();
        for key in keys {
            assert!(
                seen.insert(key.as_i64()),
                "{key:?} = {} collides with another variant",
                key.as_i64()
            );
        }
    }

    #[test]
    fn lock_keys_never_collide_with_the_go_advisory_locks() {
        assert_eq!(GO_SCHEMA_LOCK_KEY, 0x0A6E_7653_4348_4D41);
        assert_eq!(GO_BUILD_ADMISSION_LOCK_KEY, 3_405_691_582);

        let key = AdvisoryLockKey::CatalogBuildReaper;
        assert_ne!(
            key.as_i64(),
            GO_SCHEMA_LOCK_KEY,
            "{key:?} collides with schemaLockKey"
        );
        assert_ne!(
            key.as_i64(),
            GO_BUILD_ADMISSION_LOCK_KEY,
            "{key:?} collides with buildAdmissionKey"
        );
        assert_ne!(GO_SCHEMA_LOCK_KEY, GO_BUILD_ADMISSION_LOCK_KEY);
    }
}
