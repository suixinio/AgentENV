//! Reads and writes the `catalog_migration_state` row —
//! `docs/proposals/_sd-phase4-stageB-catalog.md` §5.1's structural fix for
//! the read-side admission CrashLoopBackOff: whether this cluster's
//! PostgreSQL catalog has ever been confirmed to hold what object storage
//! does, recorded once for every `--role api`/`--role all` replica to read
//! instead of each one re-deriving the answer from node-local storage.

use anyhow::{Context, Result};
use async_trait::async_trait;
use sqlx::PgPool;
use uuid::Uuid;

use crate::snapshot::repository::mirror::ReadSideConfirmationStore;

/// Whether `cluster_id`'s row says the read side has been confirmed.
///
/// `false` for a cluster with no row at all — the same reading as a fresh
/// [`crate::snapshot::repository::mirror::MirrorBacklog`] answering `None`.
pub async fn read_side_confirmed(pool: &PgPool, cluster_id: Uuid) -> Result<bool> {
    let confirmed: Option<bool> = sqlx::query_scalar(
        "SELECT read_side_confirmed FROM catalog_migration_state WHERE cluster_id = $1",
    )
    .bind(cluster_id)
    .fetch_optional(pool)
    .await
    .context("read catalog_migration_state.read_side_confirmed")?;
    Ok(confirmed.unwrap_or(false))
}

/// Marks `cluster_id`'s read side confirmed, for `node_id`'s benefit (audit
/// only — see the migration's own doc comment on `confirmed_by_node_id`).
///
/// Idempotent and monotonic: a second confirmation from a second replica
/// (or the same one, restarted) just rewrites the same `true` and a newer
/// timestamp, never turns a confirmed cluster back to unconfirmed.
pub async fn confirm_read_side(pool: &PgPool, cluster_id: Uuid, node_id: &str) -> Result<()> {
    sqlx::query(
        "INSERT INTO catalog_migration_state (cluster_id, read_side_confirmed, confirmed_at_ms, confirmed_by_node_id)
         VALUES ($1, true, $2, $3)
         ON CONFLICT (cluster_id) DO UPDATE
            SET read_side_confirmed = true,
                confirmed_at_ms = EXCLUDED.confirmed_at_ms,
                confirmed_by_node_id = EXCLUDED.confirmed_by_node_id",
    )
    .bind(cluster_id)
    .bind(now_ms())
    .bind(node_id)
    .execute(pool)
    .await
    .context("confirm catalog_migration_state.read_side_confirmed")?;
    Ok(())
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

/// [`ReadSideConfirmationStore`] backed by `catalog_migration_state`.
/// 🔴 Owns its pool and node id rather than borrowing them: the one
/// production constructor hands this out as an
/// `Arc<dyn ReadSideConfirmationStore>` (see
/// [`pg_catalog_parts`][super::pg_catalog_parts]), which a borrowing type
/// cannot be. `PgPool` is an `Arc` internally, so the clone is a refcount
/// bump.
pub struct PgReadSideConfirmation {
    pool: PgPool,
    cluster_id: Uuid,
    node_id: String,
}

impl PgReadSideConfirmation {
    pub fn new(pool: &PgPool, cluster_id: Uuid, node_id: &str) -> Self {
        Self {
            pool: pool.clone(),
            cluster_id,
            node_id: node_id.to_string(),
        }
    }
}

#[async_trait]
impl ReadSideConfirmationStore for PgReadSideConfirmation {
    async fn is_confirmed(&self) -> anyhow::Result<bool> {
        read_side_confirmed(&self.pool, self.cluster_id).await
    }

    async fn confirm(&self) -> anyhow::Result<()> {
        confirm_read_side(&self.pool, self.cluster_id, &self.node_id).await
    }
}

#[cfg(test)]
mod pg {
    use super::*;
    use crate::pg::harness::isolated_schema_pool_or_skip;
    use crate::snapshot::repository::backends::postgres::migrate;

    #[tokio::test]
    async fn an_unconfirmed_cluster_with_no_row_reads_as_unconfirmed() {
        let pool = isolated_schema_pool_or_skip!(
            "an_unconfirmed_cluster_with_no_row_reads_as_unconfirmed"
        );
        migrate::migrate(&pool)
            .await
            .expect("migration should succeed");

        let cluster_id = Uuid::new_v4();
        assert!(!read_side_confirmed(&pool, cluster_id)
            .await
            .expect("reading should succeed"));
    }

    #[tokio::test]
    async fn confirming_makes_it_read_as_confirmed() {
        let pool = isolated_schema_pool_or_skip!("confirming_makes_it_read_as_confirmed");
        migrate::migrate(&pool)
            .await
            .expect("migration should succeed");

        let cluster_id = Uuid::new_v4();
        confirm_read_side(&pool, cluster_id, "node-a")
            .await
            .expect("confirming should succeed");
        assert!(read_side_confirmed(&pool, cluster_id)
            .await
            .expect("reading should succeed"));

        let row: (bool, Option<i64>, Option<String>) = sqlx::query_as(
            "SELECT read_side_confirmed, confirmed_at_ms, confirmed_by_node_id \
             FROM catalog_migration_state WHERE cluster_id = $1",
        )
        .bind(cluster_id)
        .fetch_one(&pool)
        .await
        .expect("reading the row directly should succeed");
        assert!(row.0);
        assert!(row.1.is_some());
        assert_eq!(row.2.as_deref(), Some("node-a"));
    }

    /// Confirming twice — two replicas racing, or one replica restarting
    /// after it already confirmed — must not error and must leave the row
    /// confirmed, not toggle it back off.
    #[tokio::test]
    async fn confirming_twice_stays_confirmed() {
        let pool = isolated_schema_pool_or_skip!("confirming_twice_stays_confirmed");
        migrate::migrate(&pool)
            .await
            .expect("migration should succeed");

        let cluster_id = Uuid::new_v4();
        confirm_read_side(&pool, cluster_id, "node-a")
            .await
            .expect("first confirmation should succeed");
        confirm_read_side(&pool, cluster_id, "node-b")
            .await
            .expect("second confirmation should succeed");

        assert!(read_side_confirmed(&pool, cluster_id)
            .await
            .expect("reading should succeed"));
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM catalog_migration_state")
            .fetch_one(&pool)
            .await
            .expect("counting rows should succeed");
        assert_eq!(count, 1, "confirming twice must not create a second row");
    }

    /// Two clusters sharing one database do not see each other's
    /// confirmation — the whole reason the table is keyed by `cluster_id`.
    #[tokio::test]
    async fn two_clusters_confirmations_do_not_leak_into_each_other() {
        let pool =
            isolated_schema_pool_or_skip!("two_clusters_confirmations_do_not_leak_into_each_other");
        migrate::migrate(&pool)
            .await
            .expect("migration should succeed");

        let cluster_a = Uuid::new_v4();
        let cluster_b = Uuid::new_v4();
        confirm_read_side(&pool, cluster_a, "node-a")
            .await
            .expect("confirming cluster a should succeed");

        assert!(read_side_confirmed(&pool, cluster_a)
            .await
            .expect("reading cluster a should succeed"));
        assert!(
            !read_side_confirmed(&pool, cluster_b)
                .await
                .expect("reading cluster b should succeed"),
            "cluster b must not read cluster a's confirmation"
        );
    }

    /// The trait impl itself, exercised the way `admit_read_side_with_confirmation`
    /// actually calls it.
    #[tokio::test]
    async fn the_trait_impl_is_confirmed_then_confirm_round_trips() {
        let pool =
            isolated_schema_pool_or_skip!("the_trait_impl_is_confirmed_then_confirm_round_trips");
        migrate::migrate(&pool)
            .await
            .expect("migration should succeed");

        let cluster_id = Uuid::new_v4();
        let store = PgReadSideConfirmation::new(&pool, cluster_id, "node-a");
        assert!(
            !store.is_confirmed().await.expect("reading should succeed"),
            "a fresh cluster starts unconfirmed"
        );
        store.confirm().await.expect("confirming should succeed");
        assert!(
            store.is_confirmed().await.expect("reading should succeed"),
            "and reads back confirmed through the same trait object"
        );
    }
}
