//! The catalog build reaper: ends builds whose builder stopped saying it was
//! alive, as a cluster-wide singleton — a Rust port of
//! `catalog_service.go`'s `RunBuildReaper`.
//!
//! 🔴 Not optional wherever `builds_one_active_per_template` exists (see
//! migration 0002's own comment on that index): a build stranded at
//! `building`/`waiting` holds its template shut until something ends it, and
//! this is that something.
//!
//! 🔴 A cluster-wide singleton, not "every `--role api` replica runs its own
//! copy". Go had exactly one `scheduler` process ever running this loop;
//! Rust is N replicas. `src/pg::election::spawn_singleton_task` — PostgreSQL
//! session-scoped advisory locks, `AdvisoryLockKey::CatalogBuildReaper`,
//! reserved for exactly this — is what makes "one at a time" true here
//! without N replicas racing to scan and update the same rows. See
//! `docs/proposals/_sd-phase4-stageB-catalog.md` §9 risk 4 for why the
//! obvious alternative (copy `src/orchestrator/`'s auto-eviction task, which
//! runs unelected on every replica) is a reference *against*, not for: that
//! task is safe unelected because it only touches each replica's own
//! in-memory state, and this one touches a table every replica shares.

use std::sync::Arc;
use std::time::{Duration, Instant};

use sqlx::PgPool;
use tokio::sync::Mutex;
use tracing::{info, warn};
use uuid::Uuid;

use crate::pg::{spawn_singleton_task, AdvisoryLockKey, LeaderContext, SingletonTaskHandle};

use super::metrics::{record_build_reaper_warmup_pass, record_builds_reaped};

/// What one reaped build was.
struct ReapedBuild {
    build_id: Uuid,
    template_id: Uuid,
    node_id: Option<String>,
}

/// Starts the reaper. Returns the handle to shut it down with — see this
/// crate's own warning on [`SingletonTaskHandle`]: its `shutdown()` must be
/// awaited on its own path, never pushed into a `Vec<tokio::task::JoinHandle<()>>`
/// and `.abort()`-ed, or the advisory lock this replica may be holding leaks
/// until the pool itself is torn down.
///
/// `interval <= Duration::ZERO` or `ttl <= Duration::ZERO` disables the
/// reaper outright (logged once), matching Go's `RunBuildReaper` refusing to
/// start under the same condition — a reaper with no TTL would either never
/// fire or fire on every row immediately, and neither is "disabled" spelled
/// correctly.
pub(crate) fn spawn(
    pool: PgPool,
    cluster_id: Uuid,
    interval: Duration,
    ttl: Duration,
) -> Option<SingletonTaskHandle> {
    if interval.is_zero() || ttl.is_zero() {
        warn!(
            target: "agentenv",
            "snapshot catalog build reaper disabled: no interval or no TTL configured"
        );
        return None;
    }

    info!(
        target: "agentenv",
        interval_secs = interval.as_secs(),
        heartbeat_ttl_secs = ttl.as_secs(),
        "snapshot catalog build reaper started"
    );

    // 🔴 When *this* leader session first ran the reaper body at all — the
    // Rust analogue of Go's `openedAt`. A replica that has just become
    // leader (a fresh election, or this whole process just started) has no
    // evidence about whether a heartbeat it has never had the chance to
    // observe is stale or merely unheard-from during a rollout; without this
    // window, a leadership handoff during a fleet-wide builder restart would
    // reap every build in flight on its very first pass. `Mutex` rather than
    // an `AtomicU64` of millis: `Instant` has no meaningful bit-pattern to
    // store atomically, and this is one lock acquired once per `interval`,
    // never contended.
    let opened_at: Arc<Mutex<Option<Instant>>> = Arc::new(Mutex::new(None));

    Some(spawn_singleton_task(
        pool,
        AdvisoryLockKey::CatalogBuildReaper,
        interval,
        move |ctx: LeaderContext<'_>| {
            let opened_at = Arc::clone(&opened_at);
            Box::pin(async move {
                let opened = {
                    let mut guard = opened_at.lock().await;
                    *guard.get_or_insert_with(Instant::now)
                };
                let waited = Instant::now().saturating_duration_since(opened);
                if waited < ttl {
                    record_build_reaper_warmup_pass();
                    tracing::debug!(
                        target: "agentenv",
                        waited_secs = waited.as_secs(),
                        heartbeat_ttl_secs = ttl.as_secs(),
                        "snapshot catalog build reaping pass held back: this leader has not been \
                         able to hear a heartbeat for a full TTL yet"
                    );
                    return;
                }

                match reap_once(ctx.conn, cluster_id, ttl).await {
                    Ok(reaped) if reaped.is_empty() => {}
                    Ok(reaped) => {
                        record_builds_reaped(reaped.len() as u64);
                        for build in &reaped {
                            info!(
                                target: "agentenv",
                                build_id = %build.build_id,
                                template_id = %build.template_id,
                                node_id = build.node_id.as_deref().unwrap_or(""),
                                "snapshot catalog build reaper ended a stalled build"
                            );
                        }
                        info!(
                            target: "agentenv",
                            reaped = reaped.len(),
                            "snapshot catalog build reaping pass"
                        );
                    }
                    Err(error) => {
                        warn!(target: "agentenv", %error, "snapshot catalog build reaping pass failed");
                    }
                }
            })
        },
    ))
}

/// The reason recorded on both the `builds` row and the `snapshots` row it
/// held — matches Go's `reapedBuildError` exactly, since both are read by the
/// same node-side `TemplateBuildErrorReason` decoder.
fn reaped_build_error() -> serde_json::Value {
    serde_json::json!({"message": "build heartbeat lapsed", "step": null})
}

/// One reaping pass: ends builds whose heartbeat has lapsed and the template
/// rows they were holding, in one transaction — the Rust equivalent of
/// `reapBuildsSQL` + `failReapedTemplatesSQL` run back to back.
///
/// 🔴 Both statements or neither. `builds_one_active_per_template` stops
/// blocking a template the moment its build row leaves the active group, but
/// `markSnapshotBuildingSQL`'s own predicate (`status IN ('waiting',
/// 'error')`) refuses a template still sitting at `building` — so freeing one
/// half without the other leaves the template unbuildable through a
/// different door.
async fn reap_once(
    conn: &mut sqlx::PgConnection,
    cluster_id: Uuid,
    ttl: Duration,
) -> anyhow::Result<Vec<ReapedBuild>> {
    use anyhow::Context;
    use sqlx::Connection;

    let mut tx = conn
        .begin()
        .await
        .context("begin transaction for the catalog build reaping pass")?;

    let error_reason = reaped_build_error();
    let ttl_ms = i64::try_from(ttl.as_millis()).unwrap_or(i64::MAX);

    let reaped: Vec<(Uuid, Uuid, Option<String>)> = sqlx::query_as(
        "UPDATE builds
            SET status         = 'error',
                finished_at_ms = (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT,
                error_reason   = $2::jsonb
          WHERE cluster_id = $1
            AND status_group IN ('pending', 'in_progress')
            AND heartbeat_at_ms IS NOT NULL
            AND heartbeat_at_ms < (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT - $3
        RETURNING id, template_id, node_id",
    )
    .bind(cluster_id)
    .bind(&error_reason)
    .bind(ttl_ms)
    .fetch_all(&mut *tx)
    .await
    .context("reap expired catalog builds")?;

    if !reaped.is_empty() {
        let template_ids: Vec<Uuid> = reaped
            .iter()
            .map(|(_, template_id, _)| *template_id)
            .collect();
        sqlx::query(
            "UPDATE snapshots
                SET status        = 'error',
                    build_error   = $2::jsonb,
                    updated_at_ms = (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT
              WHERE cluster_id = $1
                AND id = ANY($3::uuid[])
                AND status = 'building'",
        )
        .bind(cluster_id)
        .bind(&error_reason)
        .bind(&template_ids)
        .execute(&mut *tx)
        .await
        .context("fail the template rows the reaped builds were holding")?;
    }

    tx.commit()
        .await
        .context("commit the catalog build reaping pass")?;

    Ok(reaped
        .into_iter()
        .map(|(build_id, template_id, node_id)| ReapedBuild {
            build_id,
            template_id,
            node_id,
        })
        .collect())
}

#[cfg(test)]
mod pg {
    use super::*;
    use crate::pg::harness::isolated_schema_pool_or_skip;
    use crate::snapshot::repository::backends::postgres::migrate;

    /// Inserts one `snapshots` row (a template, `building`) and one `builds`
    /// row, with a heartbeat `age_ms` milliseconds in the past.
    async fn seed_stale_build(pool: &PgPool, cluster_id: Uuid, age_ms: i64) -> (Uuid, Uuid) {
        let template_id = Uuid::new_v4();
        let build_id = Uuid::new_v4();

        sqlx::query(
            "INSERT INTO snapshots (
                id, cluster_id, source_kind, source_sandbox_id,
                cpu_count, memory_mib, disk_size_mib,
                status, status_group, published, origin_node_id,
                created_at_ms, updated_at_ms
             ) VALUES ($1, $2, 'template', NULL, 1, 512, 1024, 'building', 'in_progress', true, NULL, 0, 0)",
        )
        .bind(template_id)
        .bind(cluster_id)
        .execute(pool)
        .await
        .expect("seeding the snapshot row should succeed");

        sqlx::query(
            "INSERT INTO builds (
                id, template_id, cluster_id, status, status_group, node_id,
                heartbeat_at_ms, created_at_ms, started_at_ms
             ) VALUES (
                $1, $2, $3, 'building', 'in_progress', 'node-a',
                (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT - $4, 0, 0
             )",
        )
        .bind(build_id)
        .bind(template_id)
        .bind(cluster_id)
        .bind(age_ms)
        .execute(pool)
        .await
        .expect("seeding the build row should succeed");

        (template_id, build_id)
    }

    #[tokio::test]
    async fn a_build_whose_heartbeat_lapsed_is_reaped_along_with_its_template() {
        let pool = isolated_schema_pool_or_skip!(
            "a_build_whose_heartbeat_lapsed_is_reaped_along_with_its_template"
        );
        migrate::migrate(&pool)
            .await
            .expect("migration should succeed");

        let cluster_id = Uuid::new_v4();
        let ttl = Duration::from_secs(60);
        let (template_id, build_id) = seed_stale_build(&pool, cluster_id, 120_000).await;

        let mut conn = pool
            .acquire()
            .await
            .expect("acquiring a connection should succeed");
        let reaped = reap_once(&mut conn, cluster_id, ttl)
            .await
            .expect("reaping should succeed");
        assert_eq!(reaped.len(), 1);
        assert_eq!(reaped[0].build_id, build_id);
        assert_eq!(reaped[0].template_id, template_id);

        let build_status: String = sqlx::query_scalar("SELECT status FROM builds WHERE id = $1")
            .bind(build_id)
            .fetch_one(&pool)
            .await
            .expect("reading the build status should succeed");
        assert_eq!(build_status, "error");

        let snapshot_status: String =
            sqlx::query_scalar("SELECT status FROM snapshots WHERE id = $1")
                .bind(template_id)
                .fetch_one(&pool)
                .await
                .expect("reading the snapshot status should succeed");
        assert_eq!(snapshot_status, "error");

        // 🔴 The whole reason both statements exist: the partial unique
        // index no longer blocks a fresh build of this template.
        let admits: Option<Uuid> = sqlx::query_scalar(
            "SELECT id FROM builds WHERE template_id = $1 AND status_group IN ('pending', 'in_progress')",
        )
        .bind(template_id)
        .fetch_optional(&pool)
        .await
        .expect("querying active builds should succeed");
        assert!(
            admits.is_none(),
            "a reaped build must leave no row still blocking the template"
        );
    }

    #[tokio::test]
    async fn a_build_whose_heartbeat_is_recent_is_left_alone() {
        let pool = isolated_schema_pool_or_skip!("a_build_whose_heartbeat_is_recent_is_left_alone");
        migrate::migrate(&pool)
            .await
            .expect("migration should succeed");

        let cluster_id = Uuid::new_v4();
        let ttl = Duration::from_secs(300);
        let (_, build_id) = seed_stale_build(&pool, cluster_id, 1_000).await;

        let mut conn = pool
            .acquire()
            .await
            .expect("acquiring a connection should succeed");
        let reaped = reap_once(&mut conn, cluster_id, ttl)
            .await
            .expect("reaping should succeed");
        assert!(reaped.is_empty(), "a fresh heartbeat must not be reaped");

        let build_status: String = sqlx::query_scalar("SELECT status FROM builds WHERE id = $1")
            .bind(build_id)
            .fetch_one(&pool)
            .await
            .expect("reading the build status should succeed");
        assert_eq!(build_status, "building");
    }

    /// The warmup window, exercised through the real `spawn()` — not the
    /// logic re-derived inline. A row whose heartbeat is already stale
    /// relative to the *table's* clock must survive every tick until this
    /// leader has been observing for a full TTL, then be reaped on the
    /// first tick after.
    #[tokio::test]
    async fn a_fresh_leader_does_not_reap_until_it_has_watched_for_a_full_ttl() {
        let pool = isolated_schema_pool_or_skip!(
            "a_fresh_leader_does_not_reap_until_it_has_watched_for_a_full_ttl"
        );
        migrate::migrate(&pool)
            .await
            .expect("migration should succeed");

        let cluster_id = Uuid::new_v4();
        // Already far stale by the time the reaper ever looks at it — the
        // warmup window is the only thing standing between this row and the
        // very first tick.
        let (_, build_id) = seed_stale_build(&pool, cluster_id, 1_000_000).await;

        let tick = Duration::from_millis(30);
        let ttl = Duration::from_millis(150);
        let handle = spawn(pool.clone(), cluster_id, tick, ttl);
        let handle = handle.expect("interval and ttl are both non-zero");

        tokio::time::sleep(ttl / 2).await;
        let status: String = sqlx::query_scalar("SELECT status FROM builds WHERE id = $1")
            .bind(build_id)
            .fetch_one(&pool)
            .await
            .expect("reading the build status should succeed");
        assert_eq!(
            status, "building",
            "still inside the warmup window: the row must survive every tick so far"
        );

        tokio::time::sleep(ttl * 3).await;
        let status: String = sqlx::query_scalar("SELECT status FROM builds WHERE id = $1")
            .bind(build_id)
            .fetch_one(&pool)
            .await
            .expect("reading the build status should succeed");
        assert_eq!(
            status, "error",
            "well past the warmup window now: the next tick should have reaped it"
        );

        handle.shutdown().await;
    }
}
