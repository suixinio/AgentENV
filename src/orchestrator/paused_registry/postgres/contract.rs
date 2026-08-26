//! Behavioural contract tests against a real PostgreSQL database -- the
//! Rust equivalent of `services/scheduler/internal/registry/contract_*.go`
//! (`contract_test.go`/`contract_claim_test.go`/`contract_lease_test.go`,
//! 44 `TestContract*` functions total), covering the state machine end to
//! end rather than the SQL text in isolation.
//!
//! Not a 1:1 port of every one of Go's 44 -- this suite prioritises the
//! transitions D1-D4 in the task brief actually turn on: every state-machine
//! edge, D2 Fix A/Fix B in their final (not "pre-fix") form, and D3's
//! claimant/holder split in `mark_running`. See the Stage C report's own
//! coverage-mapping section for what is and is not mirrored here.

#![cfg(test)]

mod pg {
    use std::time::{Duration, SystemTime};

    use chrono::Utc;
    use uuid::Uuid;

    use super::super::PostgresPausedSandboxRegistry;
    use crate::orchestrator::paused_registry::{
        ConflictReason, DeadlineRenewalOutcome, HeldSandbox, MarkRunningOutcome,
        PausedRegistryError, PausedRegistryState, PausedSandboxEntry, PausedSandboxRegistry,
        ResumeClaim,
    };
    use crate::orchestrator::store::SandboxMetadata;
    use crate::pg::harness::isolated_schema_pool_or_skip;
    use crate::types::{ExecutionId, SandboxId};

    async fn registry(pool: sqlx::PgPool, cluster_id: Uuid) -> PostgresPausedSandboxRegistry {
        super::super::schema::migrate(&pool)
            .await
            .expect("migration should succeed");
        PostgresPausedSandboxRegistry::new(pool, cluster_id, Duration::from_secs(90))
    }

    fn entry(
        sandbox_id: SandboxId,
        cluster_id: Uuid,
        origin_node_id: &str,
        execution_id: ExecutionId,
    ) -> PausedSandboxEntry {
        PausedSandboxEntry {
            sandbox_id,
            cluster_id,
            state: PausedRegistryState::Publishing,
            generation: 0,
            origin_node_id: origin_node_id.to_string(),
            claimed_by_node_id: None,
            snapshot_id: None,
            metadata: Some(SandboxMetadata {
                id: sandbox_id,
                execution_id,
                ..SandboxMetadata::default()
            }),
            execution_id: Some(execution_id),
            paused_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    fn snapshot_id() -> crate::snapshot::SnapshotId {
        crate::snapshot::SnapshotId::generate()
    }

    /// Brings a brand-new sandbox straight to `running` via branch ② of
    /// `mark_running` (a local wake with no claim ever taken): `begin_pause`
    /// then `mark_running` under the *same* execution id, `node_id ==
    /// holder_node_id == node_id`.
    ///
    /// 🔴 `mark_running` never creates a row (Go's own doc, ported
    /// verbatim) -- every test that needs a `running` row must create it
    /// via `begin_pause` first. This is the minimal path when the test does
    /// not care about `snapshot_id` (which stays `NULL`, since neither
    /// `begin_pause` nor `mark_running` ever sets it); tests that do care
    /// (reclaim, claim_for_resume) use [`bring_to_running_with_snapshot`]
    /// instead.
    async fn bring_to_running(
        registry: &PostgresPausedSandboxRegistry,
        sandbox_id: SandboxId,
        cluster_id: Uuid,
        node_id: &str,
        execution_id: ExecutionId,
        expires_at: Option<SystemTime>,
    ) {
        registry
            .begin_pause(&entry(sandbox_id, cluster_id, node_id, execution_id))
            .await
            .expect("begin_pause should succeed");
        let outcome = registry
            .mark_running(&sandbox_id, node_id, node_id, execution_id, expires_at)
            .await
            .expect("mark_running should succeed");
        assert_eq!(
            outcome,
            MarkRunningOutcome::Adopted,
            "bring_to_running's own mark_running call must adopt the row it just created"
        );
    }

    /// Like [`bring_to_running`], but goes through `complete_pause` first so
    /// the row carries a real `snapshot_id` -- required by
    /// `reclaim_expired_holdings`'s `running` arm and by anything that
    /// later calls `claim_for_resume` on the row.
    async fn bring_to_running_with_snapshot(
        registry: &PostgresPausedSandboxRegistry,
        sandbox_id: SandboxId,
        cluster_id: Uuid,
        node_id: &str,
        expires_at: Option<SystemTime>,
    ) {
        let pause_execution = ExecutionId::new();
        let began = registry
            .begin_pause(&entry(sandbox_id, cluster_id, node_id, pause_execution))
            .await
            .expect("begin_pause should succeed");
        registry
            .complete_pause(&sandbox_id, began.generation, &snapshot_id())
            .await
            .expect("complete_pause should succeed");
        let claim = registry
            .claim_for_resume(&sandbox_id, node_id, ExecutionId::new())
            .await
            .expect("claim_for_resume should succeed");
        let ResumeClaim::Claimed { entry: claimed, .. } = claim else {
            panic!("bring_to_running_with_snapshot's own claim must succeed, got {claim:?}");
        };
        let run_execution = claimed
            .execution_id
            .expect("a resuming row always carries an incarnation");
        let outcome = registry
            .mark_running(&sandbox_id, node_id, node_id, run_execution, expires_at)
            .await
            .expect("mark_running should succeed");
        assert_eq!(outcome, MarkRunningOutcome::Adopted);
    }

    // ---------------------------------------------------------------------
    // begin_pause / complete_pause / mark_local_only
    // ---------------------------------------------------------------------

    #[tokio::test]
    async fn begin_pause_then_complete_pause_lands_on_paused_with_a_snapshot() {
        let pool = isolated_schema_pool_or_skip!(
            "begin_pause_then_complete_pause_lands_on_paused_with_a_snapshot"
        );
        let cluster_id = Uuid::new_v4();
        let registry = registry(pool, cluster_id).await;

        let sandbox_id = SandboxId::new();
        let execution_id = ExecutionId::new();
        let began = registry
            .begin_pause(&entry(sandbox_id, cluster_id, "node-a", execution_id))
            .await
            .expect("begin_pause should succeed on a fresh sandbox");
        assert_eq!(began.generation, 1);
        assert!(began.previous_snapshot_id.is_none());

        let snapshot = snapshot_id();
        registry
            .complete_pause(&sandbox_id, began.generation, &snapshot)
            .await
            .expect("complete_pause should succeed");

        let row = registry
            .get(&sandbox_id)
            .await
            .expect("get should succeed")
            .expect("the row should exist");
        assert_eq!(row.state, PausedRegistryState::Paused);
        assert_eq!(row.snapshot_id, Some(snapshot));
        assert!(
            row.execution_id.is_none(),
            "a paused row carries no incarnation"
        );
    }

    #[tokio::test]
    async fn begin_pause_then_mark_local_only_stays_parked_with_no_snapshot() {
        let pool = isolated_schema_pool_or_skip!(
            "begin_pause_then_mark_local_only_stays_parked_with_no_snapshot"
        );
        let cluster_id = Uuid::new_v4();
        let registry = registry(pool, cluster_id).await;

        let sandbox_id = SandboxId::new();
        let execution_id = ExecutionId::new();
        let began = registry
            .begin_pause(&entry(sandbox_id, cluster_id, "node-a", execution_id))
            .await
            .expect("begin_pause should succeed");

        registry
            .mark_local_only(&sandbox_id, began.generation)
            .await
            .expect("mark_local_only should succeed");

        let row = registry.get(&sandbox_id).await.unwrap().unwrap();
        assert_eq!(row.state, PausedRegistryState::LocalOnly);
        assert!(row.snapshot_id.is_none());
    }

    /// `completePauseSQL`'s generation CAS: a stale generation is refused, not
    /// silently ignored.
    #[tokio::test]
    async fn complete_pause_with_a_stale_generation_is_a_generation_conflict() {
        let pool = isolated_schema_pool_or_skip!(
            "complete_pause_with_a_stale_generation_is_a_generation_conflict"
        );
        let cluster_id = Uuid::new_v4();
        let registry = registry(pool, cluster_id).await;

        let sandbox_id = SandboxId::new();
        let execution_id = ExecutionId::new();
        registry
            .begin_pause(&entry(sandbox_id, cluster_id, "node-a", execution_id))
            .await
            .unwrap();

        let err = registry
            .complete_pause(&sandbox_id, 999, &snapshot_id())
            .await
            .expect_err("a stale generation must be refused");
        assert!(matches!(
            err,
            PausedRegistryError::GenerationConflict { .. }
        ));
    }

    /// begin_pause's own fencing: a pause sent under an incarnation the row no
    /// longer names is refused as `ExecutionFenced`, never silently upserted.
    #[tokio::test]
    async fn begin_pause_under_a_superseded_incarnation_is_execution_fenced() {
        let pool = isolated_schema_pool_or_skip!(
            "begin_pause_under_a_superseded_incarnation_is_execution_fenced"
        );
        let cluster_id = Uuid::new_v4();
        let registry = registry(pool, cluster_id).await;

        let sandbox_id = SandboxId::new();
        let first_execution = ExecutionId::new();
        registry
            .begin_pause(&entry(sandbox_id, cluster_id, "node-a", first_execution))
            .await
            .unwrap();
        // Bring the row to `running` under a *new* incarnation the way a
        // cross-node resume would -- the row now names `second_execution`, not
        // `first_execution`.
        let second_execution = ExecutionId::new();
        registry
            .mark_running(&sandbox_id, "node-a", "node-a", second_execution, None)
            .await
            .unwrap();

        // A pause sent under the now-superseded first incarnation must be
        // refused, not accepted and left overwriting the live row's identity.
        let err = registry
            .begin_pause(&entry(sandbox_id, cluster_id, "node-a", first_execution))
            .await
            .expect_err("a pause under a superseded incarnation must be fenced");
        assert!(matches!(err, PausedRegistryError::ExecutionFenced { .. }));
    }

    // ---------------------------------------------------------------------
    // claim_for_resume
    // ---------------------------------------------------------------------

    #[tokio::test]
    async fn claiming_an_untracked_sandbox_reports_not_found() {
        let pool = isolated_schema_pool_or_skip!("claiming_an_untracked_sandbox_reports_not_found");
        let cluster_id = Uuid::new_v4();
        let registry = registry(pool, cluster_id).await;

        let claim = registry
            .claim_for_resume(&SandboxId::new(), "node-b", ExecutionId::new())
            .await
            .unwrap();
        assert!(matches!(claim, ResumeClaim::NotFound));
    }

    #[tokio::test]
    async fn claiming_a_paused_sandbox_succeeds_and_reports_paused_as_the_previous_state() {
        let pool = isolated_schema_pool_or_skip!(
            "claiming_a_paused_sandbox_succeeds_and_reports_paused_as_the_previous_state"
        );
        let cluster_id = Uuid::new_v4();
        let registry = registry(pool, cluster_id).await;

        let sandbox_id = SandboxId::new();
        let execution_id = ExecutionId::new();
        let began = registry
            .begin_pause(&entry(sandbox_id, cluster_id, "node-a", execution_id))
            .await
            .unwrap();
        registry
            .complete_pause(&sandbox_id, began.generation, &snapshot_id())
            .await
            .unwrap();

        let claim_execution = ExecutionId::new();
        let claim = registry
            .claim_for_resume(&sandbox_id, "node-b", claim_execution)
            .await
            .unwrap();
        let ResumeClaim::Claimed {
            entry: claimed,
            previous_state,
        } = claim
        else {
            panic!("expected a successful claim, got {claim:?}");
        };
        assert_eq!(previous_state, PausedRegistryState::Paused);
        assert_eq!(claimed.state, PausedRegistryState::Resuming);
        assert_eq!(claimed.claimed_by_node_id.as_deref(), Some("node-b"));
        assert_eq!(claimed.execution_id, Some(claim_execution));
    }

    /// The `previous_state` must never be read off the post-claim row (always
    /// `Resuming`) -- this pins the same regression the trait doc on
    /// `ResumeClaim` names by history: reading it off the wrong place silently
    /// reports every ordinary resume as a takeover.
    #[tokio::test]
    async fn claiming_a_lapsed_local_only_sandbox_reports_local_only_as_the_previous_state_not_resuming(
    ) {
        let pool = isolated_schema_pool_or_skip!(
            "claiming_a_lapsed_local_only_sandbox_reports_local_only_as_the_previous_state_not_resuming"
        );
        let cluster_id = Uuid::new_v4();
        // A very short lease TTL so this test does not need a long sleep.
        let pool_clone = pool.clone();
        super::super::schema::migrate(&pool_clone).await.unwrap();
        let registry = PostgresPausedSandboxRegistry::new(
            pool_clone.clone(),
            cluster_id,
            Duration::from_millis(50),
        );

        // 🔴 The lapsed-lease claim arm is withheld during a cluster's restart
        // grace window (see `super::super::grace`'s own doc) -- this test is
        // about the ordinary, long-past-grace case, so mark this cluster
        // already serving directly rather than actually waiting out a grace
        // period.
        sqlx::query(
            "INSERT INTO paused_registry_grace (cluster_id, grace_until, downtime_secs, leases_extended)
             VALUES ($1, now() - interval '1 second', 0, 0)",
        )
        .bind(cluster_id)
        .execute(&pool_clone)
        .await
        .expect("seeding the grace row should succeed");

        // A realistic path to a `local_only` row that still carries a
        // *durable* snapshot from an earlier successful pause: pause once,
        // complete it durably, resume it, then pause it again -- this
        // second upload is the one that fails and lands on `local_only`,
        // while the row still carries the first pause's snapshot_id
        // (neither `begin_pause`'s upsert nor `mark_local_only` ever
        // touches that column).
        let sandbox_id = SandboxId::new();
        bring_to_running_with_snapshot(&registry, sandbox_id, cluster_id, "node-a", None).await;
        let running = registry.get(&sandbox_id).await.unwrap().unwrap();
        let live_execution = running
            .execution_id
            .expect("a running row always carries an incarnation");

        let re_paused = registry
            .begin_pause(&entry(sandbox_id, cluster_id, "node-a", live_execution))
            .await
            .expect("re-pausing the same live incarnation should succeed");
        registry
            .mark_local_only(&sandbox_id, re_paused.generation)
            .await
            .expect("mark_local_only should succeed");
        let parked = registry.get(&sandbox_id).await.unwrap().unwrap();
        assert!(
            parked.snapshot_id.is_some(),
            "the row must still carry the earlier pause's durable snapshot"
        );

        tokio::time::sleep(Duration::from_millis(200)).await;

        let claim = registry
            .claim_for_resume(&sandbox_id, "node-b", ExecutionId::new())
            .await
            .unwrap();
        let ResumeClaim::Claimed { previous_state, .. } = claim else {
            panic!("expected a successful claim once the lease has lapsed, got {claim:?}");
        };
        assert_eq!(previous_state, PausedRegistryState::LocalOnly);
    }

    #[tokio::test]
    async fn claiming_a_running_sandbox_is_refused_as_live_elsewhere_however_long_its_lease_has_lapsed(
    ) {
        let pool = isolated_schema_pool_or_skip!(
            "claiming_a_running_sandbox_is_refused_as_live_elsewhere_however_long_its_lease_has_lapsed"
        );
        let cluster_id = Uuid::new_v4();
        let registry =
            PostgresPausedSandboxRegistry::new(pool.clone(), cluster_id, Duration::from_millis(50));
        super::super::schema::migrate(&pool).await.unwrap();

        let sandbox_id = SandboxId::new();
        let execution_id = ExecutionId::new();
        bring_to_running(
            &registry,
            sandbox_id,
            cluster_id,
            "node-a",
            execution_id,
            None,
        )
        .await;

        tokio::time::sleep(Duration::from_millis(200)).await;

        // 🔴 The core claim under test: a lapsed lease on a `running` row must
        // never be treated as evidence the node is dead -- only
        // `release_node_holdings`/`reclaim_expired_holdings` may take a live row
        // away from its holder.
        let claim = registry
            .claim_for_resume(&sandbox_id, "node-b", ExecutionId::new())
            .await
            .unwrap();
        let ResumeClaim::Conflict {
            origin_node_id,
            reason,
        } = claim
        else {
            panic!("expected a conflict, got {claim:?}");
        };
        assert_eq!(origin_node_id, "node-a");
        assert_eq!(reason, ConflictReason::LiveElsewhere);
    }

    // ---------------------------------------------------------------------
    // mark_running -- D3's claimant/holder split
    // ---------------------------------------------------------------------

    /// Branch ①: a cross-node resume claimed on `node-b`, running physically on
    /// `node-b` too (the ordinary shape when the api replica's own identity and
    /// the real machine coincide, e.g. `--role all`).
    #[tokio::test]
    async fn mark_running_adopts_a_freshly_claimed_sandbox() {
        let pool = isolated_schema_pool_or_skip!("mark_running_adopts_a_freshly_claimed_sandbox");
        let cluster_id = Uuid::new_v4();
        let registry = registry(pool, cluster_id).await;

        let sandbox_id = SandboxId::new();
        let execution_id = ExecutionId::new();
        let began = registry
            .begin_pause(&entry(sandbox_id, cluster_id, "node-a", execution_id))
            .await
            .unwrap();
        registry
            .complete_pause(&sandbox_id, began.generation, &snapshot_id())
            .await
            .unwrap();
        let claim_execution = ExecutionId::new();
        registry
            .claim_for_resume(&sandbox_id, "node-b", claim_execution)
            .await
            .unwrap();

        let outcome = registry
            .mark_running(&sandbox_id, "node-b", "node-b", claim_execution, None)
            .await
            .unwrap();
        assert_eq!(outcome, MarkRunningOutcome::Adopted);

        let row = registry.get(&sandbox_id).await.unwrap().unwrap();
        assert_eq!(row.state, PausedRegistryState::Running);
        assert_eq!(row.origin_node_id, "node-b");
        assert!(row.claimed_by_node_id.is_none());
    }

    /// 🔴 D3's actual split, proved end to end: `node_id` (the claimant) is the
    /// CAS guard and `holder_node_id` differs from it -- an api replica marking
    /// running a sandbox a *different* machine actually holds. This is the
    /// exact shape a0487f0 broke (quoting the holder at the guard) and D3
    /// exists to keep separate.
    #[tokio::test]
    async fn mark_running_writes_the_holder_into_origin_node_id_while_guarding_on_the_claimant() {
        let pool = isolated_schema_pool_or_skip!(
            "mark_running_writes_the_holder_into_origin_node_id_while_guarding_on_the_claimant"
        );
        let cluster_id = Uuid::new_v4();
        let registry = registry(pool, cluster_id).await;

        let sandbox_id = SandboxId::new();
        let execution_id = ExecutionId::new();
        let began = registry
            .begin_pause(&entry(sandbox_id, cluster_id, "node-a", execution_id))
            .await
            .unwrap();
        registry
            .complete_pause(&sandbox_id, began.generation, &snapshot_id())
            .await
            .unwrap();
        let claim_execution = ExecutionId::new();
        // "api-replica-7" claims on behalf of the resume; "real-machine-3" is
        // the actual VM host reported by the backend.
        registry
            .claim_for_resume(&sandbox_id, "api-replica-7", claim_execution)
            .await
            .unwrap();

        let outcome = registry
            .mark_running(
                &sandbox_id,
                "api-replica-7",
                "real-machine-3",
                claim_execution,
                None,
            )
            .await
            .unwrap();
        assert_eq!(outcome, MarkRunningOutcome::Adopted);

        let row = registry.get(&sandbox_id).await.unwrap().unwrap();
        // The holder was written into origin_node_id...
        assert_eq!(row.origin_node_id, "real-machine-3");

        // ...and branch ③'s retry check reads the holder, not the claimant: a
        // second, idempotent mark_running call using the SAME claimant identity
        // must still succeed even though origin_node_id no longer equals it.
        let retried = registry
            .mark_running(
                &sandbox_id,
                "api-replica-7",
                "real-machine-3",
                claim_execution,
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            retried,
            MarkRunningOutcome::Adopted,
            "a retried mark_running under the same claimant/holder pair must not be refused just \
             because origin_node_id was already repointed at the holder"
        );
    }

    /// Branch ②: a sandbox parked on this node's own disk, woken with no claim
    /// ever taken -- `node_id == holder_node_id`, both the origin.
    #[tokio::test]
    async fn mark_running_wakes_a_locally_parked_sandbox_with_no_prior_claim() {
        let pool = isolated_schema_pool_or_skip!(
            "mark_running_wakes_a_locally_parked_sandbox_with_no_prior_claim"
        );
        let cluster_id = Uuid::new_v4();
        let registry = registry(pool, cluster_id).await;

        let sandbox_id = SandboxId::new();
        let execution_id = ExecutionId::new();
        registry
            .begin_pause(&entry(sandbox_id, cluster_id, "node-a", execution_id))
            .await
            .unwrap();
        registry.mark_local_only(&sandbox_id, 1).await.unwrap();

        let wake_execution = ExecutionId::new();
        let outcome = registry
            .mark_running(&sandbox_id, "node-a", "node-a", wake_execution, None)
            .await
            .unwrap();
        assert_eq!(outcome, MarkRunningOutcome::Adopted);
    }

    #[tokio::test]
    async fn mark_running_an_untracked_sandbox_reports_untracked_and_creates_no_row() {
        let pool = isolated_schema_pool_or_skip!(
            "mark_running_an_untracked_sandbox_reports_untracked_and_creates_no_row"
        );
        let cluster_id = Uuid::new_v4();
        let registry = registry(pool, cluster_id).await;

        let sandbox_id = SandboxId::new();
        let outcome = registry
            .mark_running(&sandbox_id, "node-a", "node-a", ExecutionId::new(), None)
            .await
            .unwrap();
        assert_eq!(outcome, MarkRunningOutcome::Untracked);
        assert!(registry.get(&sandbox_id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn mark_running_a_claim_another_node_holds_reports_held_elsewhere() {
        let pool = isolated_schema_pool_or_skip!(
            "mark_running_a_claim_another_node_holds_reports_held_elsewhere"
        );
        let cluster_id = Uuid::new_v4();
        let registry = registry(pool, cluster_id).await;

        let sandbox_id = SandboxId::new();
        let execution_id = ExecutionId::new();
        let began = registry
            .begin_pause(&entry(sandbox_id, cluster_id, "node-a", execution_id))
            .await
            .unwrap();
        registry
            .complete_pause(&sandbox_id, began.generation, &snapshot_id())
            .await
            .unwrap();
        registry
            .claim_for_resume(&sandbox_id, "node-b", ExecutionId::new())
            .await
            .unwrap();

        // node-c tries to mark it running -- it never held the claim.
        let outcome = registry
            .mark_running(&sandbox_id, "node-c", "node-c", ExecutionId::new(), None)
            .await
            .unwrap();
        assert_eq!(outcome, MarkRunningOutcome::HeldElsewhere);
    }

    /// A `running` row's own incarnation being superseded (e.g. a very late,
    /// stale `mark_running` retry racing a fresh resume) is `ExecutionFenced`,
    /// never silently applied.
    #[tokio::test]
    async fn mark_running_a_stale_incarnation_on_a_running_row_is_execution_fenced() {
        let pool = isolated_schema_pool_or_skip!(
            "mark_running_a_stale_incarnation_on_a_running_row_is_execution_fenced"
        );
        let cluster_id = Uuid::new_v4();
        let registry = registry(pool, cluster_id).await;

        let sandbox_id = SandboxId::new();
        let first_execution = ExecutionId::new();
        bring_to_running(
            &registry,
            sandbox_id,
            cluster_id,
            "node-a",
            first_execution,
            None,
        )
        .await;

        // A stale retry (or a late message from a superseded incarnation)
        // quoting a *different* execution id than the one currently on the
        // row must be fenced, not silently applied.
        let stale_retry = registry
            .mark_running(&sandbox_id, "node-a", "node-a", ExecutionId::new(), None)
            .await
            .unwrap_err();
        assert!(matches!(
            stale_retry,
            PausedRegistryError::ExecutionFenced { .. }
        ));
    }

    // ---------------------------------------------------------------------
    // renew_sandbox_deadline
    // ---------------------------------------------------------------------

    #[tokio::test]
    async fn renew_sandbox_deadline_updates_only_the_deadline() {
        let pool =
            isolated_schema_pool_or_skip!("renew_sandbox_deadline_updates_only_the_deadline");
        let cluster_id = Uuid::new_v4();
        let registry = registry(pool, cluster_id).await;

        let sandbox_id = SandboxId::new();
        let execution_id = ExecutionId::new();
        bring_to_running(
            &registry,
            sandbox_id,
            cluster_id,
            "node-a",
            execution_id,
            None,
        )
        .await;

        let new_deadline = SystemTime::now() + Duration::from_secs(3600);
        let outcome = registry
            .renew_sandbox_deadline(&sandbox_id, execution_id, Some(new_deadline))
            .await
            .unwrap();
        assert_eq!(outcome, DeadlineRenewalOutcome::Renewed);

        let row = registry.get(&sandbox_id).await.unwrap().unwrap();
        assert_eq!(
            row.origin_node_id, "node-a",
            "identity columns must be untouched"
        );
        assert_eq!(row.state, PausedRegistryState::Running);
    }

    #[tokio::test]
    async fn renew_sandbox_deadline_under_a_superseded_incarnation_reports_superseded() {
        let pool = isolated_schema_pool_or_skip!(
            "renew_sandbox_deadline_under_a_superseded_incarnation_reports_superseded"
        );
        let cluster_id = Uuid::new_v4();
        let registry = registry(pool, cluster_id).await;

        let sandbox_id = SandboxId::new();
        bring_to_running(
            &registry,
            sandbox_id,
            cluster_id,
            "node-a",
            ExecutionId::new(),
            None,
        )
        .await;

        let outcome = registry
            .renew_sandbox_deadline(&sandbox_id, ExecutionId::new(), Some(SystemTime::now()))
            .await
            .unwrap();
        assert_eq!(outcome, DeadlineRenewalOutcome::Superseded);
    }

    #[tokio::test]
    async fn renew_sandbox_deadline_on_an_untracked_sandbox_reports_not_tracked() {
        let pool = isolated_schema_pool_or_skip!(
            "renew_sandbox_deadline_on_an_untracked_sandbox_reports_not_tracked"
        );
        let cluster_id = Uuid::new_v4();
        let registry = registry(pool, cluster_id).await;

        let outcome = registry
            .renew_sandbox_deadline(
                &SandboxId::new(),
                ExecutionId::new(),
                Some(SystemTime::now()),
            )
            .await
            .unwrap();
        assert_eq!(outcome, DeadlineRenewalOutcome::NotTracked);
    }

    // ---------------------------------------------------------------------
    // renew_lease -- the caller's own identity, not a caller-asserted one
    // ---------------------------------------------------------------------

    #[tokio::test]
    async fn renew_lease_only_renews_rows_the_caller_actually_holds() {
        let pool =
            isolated_schema_pool_or_skip!("renew_lease_only_renews_rows_the_caller_actually_holds");
        let cluster_id = Uuid::new_v4();
        let registry = registry(pool, cluster_id).await;

        let held_by_a = SandboxId::new();
        bring_to_running(
            &registry,
            held_by_a,
            cluster_id,
            "node-a",
            ExecutionId::new(),
            None,
        )
        .await;
        let held_by_b = SandboxId::new();
        bring_to_running(
            &registry,
            held_by_b,
            cluster_id,
            "node-b",
            ExecutionId::new(),
            None,
        )
        .await;

        // node-a asserts it holds both -- the SQL predicate, not the caller's
        // own claim, decides which one actually renews.
        let renewed = registry
            .renew_lease(
                "node-a",
                &[
                    HeldSandbox {
                        sandbox_id: held_by_a,
                        expires_at: None,
                    },
                    HeldSandbox {
                        sandbox_id: held_by_b,
                        expires_at: None,
                    },
                ],
            )
            .await
            .unwrap();
        assert_eq!(
            renewed, 1,
            "node-a must only renew the row it actually holds"
        );
    }

    /// D2 Fix A's own precondition, pinned here rather than only in
    /// `postgres::lease`'s doc: `renew_lease` (the per-replica identity-based
    /// call) never renews a `Running` row when the caller's identity is not the
    /// row's `origin_node_id` -- exactly the api-pod-vs-real-machine mismatch
    /// Fix A exists to work around via a *different* path
    /// (`renew_live_leases`/the reconcile loop), not this one.
    #[tokio::test]
    async fn renew_lease_under_an_api_pod_identity_never_renews_a_running_row_it_does_not_hold() {
        let pool = isolated_schema_pool_or_skip!(
            "renew_lease_under_an_api_pod_identity_never_renews_a_running_row_it_does_not_hold"
        );
        let cluster_id = Uuid::new_v4();
        let registry = registry(pool, cluster_id).await;

        let sandbox_id = SandboxId::new();
        // origin_node_id is the *real machine*, never an api pod's own identity.
        bring_to_running(
            &registry,
            sandbox_id,
            cluster_id,
            "real-machine-1",
            ExecutionId::new(),
            None,
        )
        .await;

        let renewed = registry
            .renew_lease(
                "api-replica-pod-abc123",
                &[HeldSandbox {
                    sandbox_id,
                    expires_at: None,
                }],
            )
            .await
            .unwrap();
        assert_eq!(
            renewed, 0,
            "an api replica's own pod identity must never match a running row's origin_node_id"
        );
    }

    // ---------------------------------------------------------------------
    // remove / release_claim / release_node_holdings
    // ---------------------------------------------------------------------

    #[tokio::test]
    async fn remove_deletes_a_matching_generation_and_reports_no_match_otherwise() {
        let pool = isolated_schema_pool_or_skip!(
            "remove_deletes_a_matching_generation_and_reports_no_match_otherwise"
        );
        let cluster_id = Uuid::new_v4();
        let registry = registry(pool, cluster_id).await;

        let sandbox_id = SandboxId::new();
        let execution_id = ExecutionId::new();
        let began = registry
            .begin_pause(&entry(sandbox_id, cluster_id, "node-a", execution_id))
            .await
            .unwrap();
        registry
            .complete_pause(&sandbox_id, began.generation, &snapshot_id())
            .await
            .unwrap();

        assert!(!registry.remove(&sandbox_id, 999).await.unwrap());
        assert!(registry.get(&sandbox_id).await.unwrap().is_some());

        let row = registry.get(&sandbox_id).await.unwrap().unwrap();
        assert!(registry.remove(&sandbox_id, row.generation).await.unwrap());
        assert!(registry.get(&sandbox_id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn release_claim_returns_a_resuming_row_to_paused() {
        let pool = isolated_schema_pool_or_skip!("release_claim_returns_a_resuming_row_to_paused");
        let cluster_id = Uuid::new_v4();
        let registry = registry(pool, cluster_id).await;

        let sandbox_id = SandboxId::new();
        let execution_id = ExecutionId::new();
        let began = registry
            .begin_pause(&entry(sandbox_id, cluster_id, "node-a", execution_id))
            .await
            .unwrap();
        registry
            .complete_pause(&sandbox_id, began.generation, &snapshot_id())
            .await
            .unwrap();
        let claim = registry
            .claim_for_resume(&sandbox_id, "node-b", ExecutionId::new())
            .await
            .unwrap();
        let ResumeClaim::Claimed { entry, .. } = claim else {
            panic!("expected a successful claim");
        };

        assert!(registry
            .release_claim(&sandbox_id, entry.generation)
            .await
            .unwrap());
        let row = registry.get(&sandbox_id).await.unwrap().unwrap();
        assert_eq!(row.state, PausedRegistryState::Paused);
        assert!(row.claimed_by_node_id.is_none());
    }

    /// `release_node_holdings` only reaches `Resuming` rows through the
    /// `claimed_by_node_id` identity match under the split node/api model (see
    /// `reclaim.rs`'s own doc): `node_id` here has to be the claimant identity,
    /// not a real machine.
    #[tokio::test]
    async fn release_node_holdings_frees_a_resuming_claim_this_replica_never_finished() {
        let pool = isolated_schema_pool_or_skip!(
            "release_node_holdings_frees_a_resuming_claim_this_replica_never_finished"
        );
        let cluster_id = Uuid::new_v4();
        let registry = registry(pool, cluster_id).await;

        let sandbox_id = SandboxId::new();
        let execution_id = ExecutionId::new();
        let began = registry
            .begin_pause(&entry(sandbox_id, cluster_id, "node-a", execution_id))
            .await
            .unwrap();
        registry
            .complete_pause(&sandbox_id, began.generation, &snapshot_id())
            .await
            .unwrap();
        registry
            .claim_for_resume(&sandbox_id, "api-replica-dead", ExecutionId::new())
            .await
            .unwrap();

        let released = registry
            .release_node_holdings("api-replica-dead")
            .await
            .unwrap();
        assert_eq!(released.released, 1);
        assert_eq!(released.discarded, 0);

        let row = registry.get(&sandbox_id).await.unwrap().unwrap();
        assert_eq!(row.state, PausedRegistryState::Paused);
    }

    // ---------------------------------------------------------------------
    // D2 Fix B: reclaim_expired_holdings' split running/resuming statements
    // ---------------------------------------------------------------------

    /// 🔴 The exact scenario Fix B closes: a first-ever cross-node resume whose
    /// claiming replica died before `mark_running` landed. `sandbox_expires_at`
    /// was never written (`claim_for_resume` never sets it), so the row must
    /// still be reclaimable on a lapsed lease alone.
    #[tokio::test]
    async fn a_resuming_row_with_a_lapsed_lease_and_no_deadline_is_reclaimed() {
        let pool = isolated_schema_pool_or_skip!(
            "a_resuming_row_with_a_lapsed_lease_and_no_deadline_is_reclaimed"
        );
        let cluster_id = Uuid::new_v4();
        let registry =
            PostgresPausedSandboxRegistry::new(pool.clone(), cluster_id, Duration::from_millis(50));
        super::super::schema::migrate(&pool).await.unwrap();

        let sandbox_id = SandboxId::new();
        let execution_id = ExecutionId::new();
        let began = registry
            .begin_pause(&entry(sandbox_id, cluster_id, "node-a", execution_id))
            .await
            .unwrap();
        registry
            .complete_pause(&sandbox_id, began.generation, &snapshot_id())
            .await
            .unwrap();
        registry
            .claim_for_resume(&sandbox_id, "api-replica-dead", ExecutionId::new())
            .await
            .unwrap();

        // Confirm the precondition Fix B is about: sandbox_expires_at is NULL.
        let stuck = registry.get(&sandbox_id).await.unwrap().unwrap();
        assert_eq!(stuck.state, PausedRegistryState::Resuming);

        tokio::time::sleep(Duration::from_millis(200)).await;

        let outcome = registry.reclaim_expired_holdings().await.unwrap();
        assert_eq!(
            outcome.released, 1,
            "a stuck resuming row with a lapsed lease and no deadline must be reclaimed"
        );

        let row = registry.get(&sandbox_id).await.unwrap().unwrap();
        assert_eq!(row.state, PausedRegistryState::Paused);
    }

    /// The `running` sibling: a lapsed lease alone is never enough -- the
    /// deadline must also have passed, or a partitioned-but-alive node's
    /// sandbox would be duplicated.
    #[tokio::test]
    async fn a_running_row_with_only_a_lapsed_lease_and_no_passed_deadline_is_not_reclaimed() {
        let pool = isolated_schema_pool_or_skip!(
            "a_running_row_with_only_a_lapsed_lease_and_no_passed_deadline_is_not_reclaimed"
        );
        let cluster_id = Uuid::new_v4();
        let registry =
            PostgresPausedSandboxRegistry::new(pool.clone(), cluster_id, Duration::from_millis(50));
        super::super::schema::migrate(&pool).await.unwrap();

        let sandbox_id = SandboxId::new();
        // A deadline far in the future -- the sandbox has plenty of time left,
        // it just has not renewed its lease (e.g. the reconcile loop has not
        // ticked yet).
        let far_future = SystemTime::now() + Duration::from_secs(3600);
        bring_to_running_with_snapshot(
            &registry,
            sandbox_id,
            cluster_id,
            "node-a",
            Some(far_future),
        )
        .await;

        tokio::time::sleep(Duration::from_millis(200)).await;

        let outcome = registry.reclaim_expired_holdings().await.unwrap();
        assert_eq!(
            outcome.released, 0,
            "a running row must not be reclaimed on a lapsed lease alone"
        );
        let row = registry.get(&sandbox_id).await.unwrap().unwrap();
        assert_eq!(row.state, PausedRegistryState::Running);
    }

    /// A `running` row with both conditions -- lapsed lease *and* a passed
    /// deadline -- is the ordinary reclaim case, unaffected by Fix B's split.
    #[tokio::test]
    async fn a_running_row_with_a_lapsed_lease_and_a_passed_deadline_is_reclaimed() {
        let pool = isolated_schema_pool_or_skip!(
            "a_running_row_with_a_lapsed_lease_and_a_passed_deadline_is_reclaimed"
        );
        let cluster_id = Uuid::new_v4();
        let registry =
            PostgresPausedSandboxRegistry::new(pool.clone(), cluster_id, Duration::from_millis(50));
        super::super::schema::migrate(&pool).await.unwrap();

        let sandbox_id = SandboxId::new();
        let execution_id = ExecutionId::new();
        let began = registry
            .begin_pause(&entry(sandbox_id, cluster_id, "node-a", execution_id))
            .await
            .unwrap();
        registry
            .complete_pause(&sandbox_id, began.generation, &snapshot_id())
            .await
            .unwrap();

        let past_deadline = SystemTime::now() - Duration::from_secs(10);
        registry
            .mark_running(
                &sandbox_id,
                "node-a",
                "node-a",
                ExecutionId::new(),
                Some(past_deadline),
            )
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(200)).await;

        let outcome = registry.reclaim_expired_holdings().await.unwrap();
        assert_eq!(outcome.released, 1);
        let row = registry.get(&sandbox_id).await.unwrap().unwrap();
        assert_eq!(row.state, PausedRegistryState::Paused);
    }

    // ---------------------------------------------------------------------
    // D1: two replicas racing the same sandbox
    // ---------------------------------------------------------------------

    /// Direct proof of the D1 claim underlying every fencing statement in this
    /// backend: two "replicas" (two independent registry handles on the same
    /// pool) racing `claim_for_resume` on the same `paused` row must have
    /// exactly one winner, decided by PostgreSQL's own row lock -- not by any
    /// coordination this Rust code performs.
    #[tokio::test]
    async fn two_replicas_racing_claim_for_resume_on_the_same_row_produce_exactly_one_winner() {
        let pool = isolated_schema_pool_or_skip!(
            "two_replicas_racing_claim_for_resume_on_the_same_row_produce_exactly_one_winner"
        );
        let cluster_id = Uuid::new_v4();
        let registry_a = registry(pool.clone(), cluster_id).await;
        let registry_b =
            PostgresPausedSandboxRegistry::new(pool, cluster_id, Duration::from_secs(90));

        let sandbox_id = SandboxId::new();
        let execution_id = ExecutionId::new();
        let began = registry_a
            .begin_pause(&entry(sandbox_id, cluster_id, "node-a", execution_id))
            .await
            .unwrap();
        registry_a
            .complete_pause(&sandbox_id, began.generation, &snapshot_id())
            .await
            .unwrap();

        let (claim_a, claim_b) = tokio::join!(
            registry_a.claim_for_resume(&sandbox_id, "node-b", ExecutionId::new()),
            registry_b.claim_for_resume(&sandbox_id, "node-c", ExecutionId::new())
        );

        let wins = [&claim_a, &claim_b]
            .iter()
            .filter(|c| matches!(c, Ok(ResumeClaim::Claimed { .. })))
            .count();
        assert_eq!(
            wins, 1,
            "exactly one of two concurrent claims on the same paused row must win: a={claim_a:?} b={claim_b:?}"
        );
    }
}
