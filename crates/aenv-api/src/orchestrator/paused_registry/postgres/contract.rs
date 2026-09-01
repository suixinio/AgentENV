//! PostgreSQL paused-registry state-machine contract tests.

#![cfg(test)]

mod pg {
    use std::time::{Duration, SystemTime};

    use chrono::{DateTime, Utc};
    use uuid::Uuid;

    use super::super::PostgresPausedSandboxRegistry;
    use crate::node_registry::registry::NodeRegistry;
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
        let second_execution = ExecutionId::new();
        registry
            .mark_running(&sandbox_id, "node-a", "node-a", second_execution, None)
            .await
            .unwrap();

        let err = registry
            .begin_pause(&entry(sandbox_id, cluster_id, "node-a", first_execution))
            .await
            .expect_err("a pause under a superseded incarnation must be fenced");
        assert!(matches!(err, PausedRegistryError::ExecutionFenced { .. }));
    }

    #[tokio::test]
    async fn two_refusals_are_told_apart() {
        let pool = isolated_schema_pool_or_skip!("two_refusals_are_told_apart");
        let cluster_id = Uuid::new_v4();
        let registry = registry(pool, cluster_id).await;

        let sandbox_a = SandboxId::new();
        let execution_a = ExecutionId::new();
        registry
            .begin_pause(&entry(sandbox_a, cluster_id, "node-a", execution_a))
            .await
            .unwrap();
        let generation_err = registry
            .complete_pause(&sandbox_a, 999, &snapshot_id())
            .await
            .expect_err("a stale generation must be refused");
        assert!(
            matches!(
                generation_err,
                PausedRegistryError::GenerationConflict { .. }
            ),
            "expected GenerationConflict, got {generation_err:?}"
        );
        assert!(
            !matches!(generation_err, PausedRegistryError::ExecutionFenced { .. }),
            "a stale generation on an otherwise-current incarnation must never read as \
             ExecutionFenced -- that would tell the caller to stop retrying for good, when a \
             fresh re-read and retry is exactly right"
        );

        let sandbox_b = SandboxId::new();
        let first_execution = ExecutionId::new();
        registry
            .begin_pause(&entry(sandbox_b, cluster_id, "node-a", first_execution))
            .await
            .unwrap();
        let second_execution = ExecutionId::new();
        registry
            .mark_running(&sandbox_b, "node-a", "node-a", second_execution, None)
            .await
            .unwrap();
        let fenced_err = registry
            .begin_pause(&entry(sandbox_b, cluster_id, "node-a", first_execution))
            .await
            .expect_err("a pause under a superseded incarnation must be fenced");
        assert!(
            matches!(fenced_err, PausedRegistryError::ExecutionFenced { .. }),
            "expected ExecutionFenced, got {fenced_err:?}"
        );
        assert!(
            !matches!(fenced_err, PausedRegistryError::GenerationConflict { .. }),
            "a superseded incarnation must never read as GenerationConflict -- that would send \
             this caller into a re-read-and-retry loop that walks straight around the fence"
        );
    }

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

    #[tokio::test]
    async fn claiming_a_lapsed_local_only_sandbox_reports_local_only_as_the_previous_state_not_resuming(
    ) {
        let pool = isolated_schema_pool_or_skip!(
            "claiming_a_lapsed_local_only_sandbox_reports_local_only_as_the_previous_state_not_resuming"
        );
        let cluster_id = Uuid::new_v4();
        // Short lease keeps the test fast.
        let pool_clone = pool.clone();
        super::super::schema::migrate(&pool_clone).await.unwrap();
        let registry = PostgresPausedSandboxRegistry::new(
            pool_clone.clone(),
            cluster_id,
            Duration::from_millis(50),
        );

        // Seed an expired restart-grace window so the lapsed-lease arm is reachable.
        sqlx::query(
            "INSERT INTO paused_registry_grace (cluster_id, grace_until, downtime_secs, leases_extended)
             VALUES ($1, now() - interval '1 second', 0, 0)",
        )
        .bind(cluster_id)
        .execute(&pool_clone)
        .await
        .expect("seeding the grace row should succeed");

        // Preserve a durable snapshot while the second pause becomes `local_only`.
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

    #[tokio::test]
    async fn claiming_a_fresh_publishing_sandbox_reports_not_ready() {
        let pool =
            isolated_schema_pool_or_skip!("claiming_a_fresh_publishing_sandbox_reports_not_ready");
        let cluster_id = Uuid::new_v4();
        let registry = registry(pool, cluster_id).await;

        let sandbox_id = SandboxId::new();
        let execution_id = ExecutionId::new();
        registry
            .begin_pause(&entry(sandbox_id, cluster_id, "node-a", execution_id))
            .await
            .unwrap();

        let claim = super::super::writes::claim_for_resume(
            &registry,
            false,
            &sandbox_id,
            "node-b",
            ExecutionId::new(),
        )
        .await
        .expect("the read path itself must succeed");

        let ResumeClaim::NotReady { origin_node_id } = claim else {
            panic!("expected NotReady, got {claim:?}");
        };
        assert_eq!(origin_node_id, "node-a");
    }

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
        assert_eq!(row.origin_node_id, "real-machine-3");

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

        let outcome = registry
            .mark_running(&sandbox_id, "node-c", "node-c", ExecutionId::new(), None)
            .await
            .unwrap();
        assert_eq!(outcome, MarkRunningOutcome::HeldElsewhere);
    }

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

        let stale_retry = registry
            .mark_running(&sandbox_id, "node-a", "node-a", ExecutionId::new(), None)
            .await
            .unwrap_err();
        assert!(matches!(
            stale_retry,
            PausedRegistryError::ExecutionFenced { .. }
        ));
    }

    #[tokio::test]
    async fn mark_running_a_running_row_held_by_a_different_node_reports_held_elsewhere() {
        let pool = isolated_schema_pool_or_skip!(
            "mark_running_a_running_row_held_by_a_different_node_reports_held_elsewhere"
        );
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

        let outcome = registry
            .mark_running(&sandbox_id, "node-c", "node-c", ExecutionId::new(), None)
            .await
            .unwrap();
        assert_eq!(outcome, MarkRunningOutcome::HeldElsewhere);

        let row = registry.get(&sandbox_id).await.unwrap().unwrap();
        assert_eq!(row.state, PausedRegistryState::Running);
        assert_eq!(row.origin_node_id, "node-a");
    }

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

    #[tokio::test]
    async fn renew_lease_under_an_api_pod_identity_never_renews_a_running_row_it_does_not_hold() {
        let pool = isolated_schema_pool_or_skip!(
            "renew_lease_under_an_api_pod_identity_never_renews_a_running_row_it_does_not_hold"
        );
        let cluster_id = Uuid::new_v4();
        let registry = registry(pool, cluster_id).await;

        let sandbox_id = SandboxId::new();
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
    async fn list_all_carries_lease_and_execution_columns_scoped_to_the_cluster() {
        let pool = isolated_schema_pool_or_skip!(
            "list_all_carries_lease_and_execution_columns_scoped_to_the_cluster"
        );
        let cluster_id = Uuid::new_v4();
        let other_cluster_id = Uuid::new_v4();
        let reg = registry(pool.clone(), cluster_id).await;
        let other_reg = registry(pool, other_cluster_id).await;

        let sandbox_id = SandboxId::new();
        let expires_at = SystemTime::now() + Duration::from_secs(3600);
        bring_to_running_with_snapshot(&reg, sandbox_id, cluster_id, "node-a", Some(expires_at))
            .await;

        let other_sandbox = SandboxId::new();
        bring_to_running_with_snapshot(&other_reg, other_sandbox, other_cluster_id, "node-z", None)
            .await;

        let before = Utc::now();
        let listing = reg.list_all().await.expect("list_all should succeed");
        let after = Utc::now();

        assert_eq!(
            listing.sandboxes.len(),
            1,
            "must not see the other cluster's row"
        );
        let row = &listing.sandboxes[0];
        assert_eq!(row.sandbox_id, sandbox_id);
        assert_eq!(row.state, PausedRegistryState::Running);
        assert_eq!(row.origin_node_id, "node-a");
        assert_eq!(row.holder(), "node-a");
        assert!(
            row.snapshot_id.is_some(),
            "a running row still names its snapshot"
        );
        assert!(
            row.execution_id.is_some(),
            "a running row is fenced to an incarnation"
        );
        assert!(
            row.sandbox_expires_at.is_some(),
            "mark_running's own expires_at must have been stored"
        );

        assert!(
            listing.now >= before && listing.now <= after,
            "list_all's `now` ({}) must fall between this test's own before/after readings \
             ({before}, {after})",
            listing.now
        );
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

    #[tokio::test]
    async fn a_resuming_row_with_an_inherited_far_future_deadline_is_still_reclaimed_on_lease_alone(
    ) {
        let pool = isolated_schema_pool_or_skip!(
            "a_resuming_row_with_an_inherited_far_future_deadline_is_still_reclaimed_on_lease_alone"
        );
        let cluster_id = Uuid::new_v4();
        let registry =
            PostgresPausedSandboxRegistry::new(pool.clone(), cluster_id, Duration::from_millis(50));
        super::super::schema::migrate(&pool).await.unwrap();

        let sandbox_id = SandboxId::new();
        let far_future = Some(SystemTime::now() + Duration::from_secs(3600));
        bring_to_running_with_snapshot(&registry, sandbox_id, cluster_id, "node-a", far_future)
            .await;
        let running_row = registry.get(&sandbox_id).await.unwrap().unwrap();
        assert_eq!(running_row.state, PausedRegistryState::Running);
        let running_execution = running_row
            .execution_id
            .expect("a running row always carries an incarnation");

        let began = registry
            .begin_pause(&entry(sandbox_id, cluster_id, "node-a", running_execution))
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
        let resuming_row = registry.get(&sandbox_id).await.unwrap().unwrap();
        assert_eq!(resuming_row.state, PausedRegistryState::Resuming);

        tokio::time::sleep(Duration::from_millis(200)).await;

        let outcome = registry.reclaim_expired_holdings().await.unwrap();
        assert_eq!(
            outcome.released, 1,
            "a resuming row must reclaim on a lapsed lease alone even while carrying a real, \
             far-future inherited deadline"
        );
        let row = registry.get(&sandbox_id).await.unwrap().unwrap();
        assert_eq!(row.state, PausedRegistryState::Paused);
    }

    #[tokio::test]
    async fn a_freshly_claimed_resuming_row_with_a_live_lease_survives_a_reclaim_pass() {
        let pool = isolated_schema_pool_or_skip!(
            "a_freshly_claimed_resuming_row_with_a_live_lease_survives_a_reclaim_pass"
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

        let outcome = registry.reclaim_expired_holdings().await.unwrap();
        assert_eq!(
            outcome.released, 0,
            "a resuming row with a still-live lease must not be reclaimed"
        );
        let row = registry.get(&sandbox_id).await.unwrap().unwrap();
        assert_eq!(row.state, PausedRegistryState::Resuming);
    }

    #[tokio::test]
    async fn a_running_row_with_no_snapshot_and_a_passed_deadline_is_discarded() {
        let pool = isolated_schema_pool_or_skip!(
            "a_running_row_with_no_snapshot_and_a_passed_deadline_is_discarded"
        );
        let cluster_id = Uuid::new_v4();
        let registry =
            PostgresPausedSandboxRegistry::new(pool.clone(), cluster_id, Duration::from_millis(50));
        super::super::schema::migrate(&pool).await.unwrap();

        let sandbox_id = SandboxId::new();
        let execution_id = ExecutionId::new();
        registry
            .begin_pause(&entry(sandbox_id, cluster_id, "node-a", execution_id))
            .await
            .unwrap();
        let past_deadline = SystemTime::now() - Duration::from_secs(10);
        let outcome = registry
            .mark_running(
                &sandbox_id,
                "node-a",
                "node-a",
                execution_id,
                Some(past_deadline),
            )
            .await
            .unwrap();
        assert_eq!(outcome, MarkRunningOutcome::Adopted);

        let precondition = registry.get(&sandbox_id).await.unwrap().unwrap();
        assert!(
            precondition.snapshot_id.is_none(),
            "precondition: no durable snapshot exists behind this row"
        );

        tokio::time::sleep(Duration::from_millis(200)).await;

        let reclaim_outcome = registry.reclaim_expired_holdings().await.unwrap();
        assert_eq!(
            reclaim_outcome.discarded, 1,
            "a running row with no snapshot, a lapsed lease and a passed deadline must be \
             discarded, not released"
        );
        assert_eq!(reclaim_outcome.released, 0);
        assert!(
            registry.get(&sandbox_id).await.unwrap().is_none(),
            "a discarded row must be gone entirely, not merely reset"
        );
    }

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

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn many_replicas_racing_to_resume_one_sandbox_start_exactly_one_run() {
        let pool = isolated_schema_pool_or_skip!(
            "many_replicas_racing_to_resume_one_sandbox_start_exactly_one_run"
        );
        let cluster_id = Uuid::new_v4();
        let seeder = registry(pool.clone(), cluster_id).await;

        let sandbox_id = SandboxId::new();
        let began = seeder
            .begin_pause(&entry(sandbox_id, cluster_id, "node-a", ExecutionId::new()))
            .await
            .unwrap();
        seeder
            .complete_pause(&sandbox_id, began.generation, &snapshot_id())
            .await
            .unwrap();

        const CLAIMANTS: usize = 12;
        let mut racing = tokio::task::JoinSet::new();
        for replica in 0..CLAIMANTS {
            let pool = pool.clone();
            racing.spawn(async move {
                let registry =
                    PostgresPausedSandboxRegistry::new(pool, cluster_id, Duration::from_secs(90));
                let execution_id = ExecutionId::new();
                let claim = registry
                    .claim_for_resume(&sandbox_id, &format!("api-replica-{replica}"), execution_id)
                    .await;
                (replica, execution_id, claim)
            });
        }

        let mut winners = Vec::new();
        while let Some(finished) = racing.join_next().await {
            let (replica, execution_id, claim) = finished.expect("a claimant task");
            if let Ok(ResumeClaim::Claimed { .. }) = claim {
                winners.push((replica, execution_id));
            }
        }

        assert_eq!(
            winners.len(),
            1,
            "the claim CAS is the only thing standing between {CLAIMANTS} replicas and \
             {CLAIMANTS} copies of one sandbox, so every run of this must elect one: {winners:?}"
        );

        // The losers must not be able to start a run behind the winner's back.
        let (winner, won_execution) = winners[0];
        let holder = "real-machine-3";
        for replica in 0..CLAIMANTS {
            if replica == winner {
                continue;
            }
            let outcome = seeder
                .mark_running(
                    &sandbox_id,
                    &format!("api-replica-{replica}"),
                    holder,
                    ExecutionId::new(),
                    None,
                )
                .await
                .expect("the registry answers");
            assert_ne!(
                outcome,
                MarkRunningOutcome::Adopted,
                "replica {replica} lost the claim and still started a second run"
            );
        }

        assert_eq!(
            seeder
                .mark_running(
                    &sandbox_id,
                    &format!("api-replica-{winner}"),
                    holder,
                    won_execution,
                    None,
                )
                .await
                .expect("the registry answers"),
            MarkRunningOutcome::Adopted,
            "the one claimant that won has to be able to finish its resume"
        );
    }

    struct SingleRosterRegistry(crate::node_registry::types::Roster);

    impl NodeRegistry for SingleRosterRegistry {
        fn snapshot(&self, _allow_lingering: bool) -> Vec<crate::node_registry::types::Node> {
            unreachable!("not exercised by renew_once")
        }
        fn contains(&self, _node: &crate::node_registry::types::Node) -> bool {
            unreachable!("not exercised by renew_once")
        }
        fn resolve(&self, _node_id: &str) -> Option<crate::node_registry::types::Node> {
            unreachable!("not exercised by renew_once")
        }
        fn heartbeat(
            &self,
            _req: &crate::proto::scheduler::HeartbeatRequest,
            _now: SystemTime,
        ) -> Result<
            (crate::node_registry::types::Node, String),
            crate::node_registry::registry::NodeNotInRegistry,
        > {
            unreachable!("not exercised by renew_once")
        }
        fn list_observed(
            &self,
            _cluster_id: &str,
            _now: SystemTime,
        ) -> Vec<crate::proto::scheduler::ObservedNode> {
            unreachable!("not exercised by renew_once")
        }
        fn list_p2p_peers(
            &self,
            _cluster_id: &str,
            _backend: &str,
            _exclude_node_id: &str,
            _now: SystemTime,
        ) -> Vec<crate::proto::scheduler::P2pPeer> {
            unreachable!("not exercised by renew_once")
        }
        fn filter_p2p_peers(
            &self,
            _cluster_id: &str,
            _backend: &str,
            _node_ids: &[String],
            _exclude_node_id: &str,
            _now: SystemTime,
        ) -> Vec<crate::proto::scheduler::P2pPeer> {
            unreachable!("not exercised by renew_once")
        }
        fn get_observed(
            &self,
            _node_id: &str,
            _cluster_id: &str,
            _now: SystemTime,
        ) -> Option<crate::proto::scheduler::ObservedNode> {
            unreachable!("not exercised by renew_once")
        }
        fn peek_observed(&self, _node_id: &str) -> Option<crate::proto::scheduler::NodeSnapshot> {
            unreachable!("not exercised by renew_once")
        }
        fn peek_observed_with_freshness(
            &self,
            _node_id: &str,
            _now: SystemTime,
        ) -> Option<(
            crate::proto::scheduler::NodeSnapshot,
            crate::node_registry::placement::score::SnapshotFreshness,
        )> {
            unreachable!("not exercised by renew_once")
        }
        fn roster_of(
            &self,
            _node_id: &str,
        ) -> Option<(Vec<crate::node_registry::types::RosterEntry>, SystemTime)> {
            unreachable!("not exercised by renew_once")
        }
        fn nodes_holding(&self, _sandbox_id: &str) -> Vec<String> {
            unreachable!("not exercised by renew_once")
        }
        fn rosters_in_cluster(
            &self,
            _cluster_id: &str,
        ) -> Vec<crate::node_registry::types::Roster> {
            vec![self.0.clone()]
        }
        fn unregister_observed(
            &self,
            _node_id: &str,
            _service_instance_id: &str,
        ) -> Result<(), crate::node_registry::registry::ServiceInstanceMismatch> {
            unreachable!("not exercised by renew_once")
        }
        fn applied_cpu_intersection(&self, _cluster_id: &str) -> Option<String> {
            unreachable!("not exercised by renew_once")
        }
    }

    async fn raw_lease_expires_at(pool: &sqlx::PgPool, sandbox_id: SandboxId) -> DateTime<Utc> {
        sqlx::query_scalar("SELECT lease_expires_at FROM paused_sandboxes WHERE sandbox_id = $1")
            .bind(sandbox_id.into_inner())
            .fetch_one(pool)
            .await
            .expect("the row should exist")
    }

    #[tokio::test]
    async fn two_replicas_each_holding_part_of_the_roster_together_renew_every_running_row() {
        let pool = isolated_schema_pool_or_skip!(
            "two_replicas_each_holding_part_of_the_roster_together_renew_every_running_row"
        );
        let cluster_id = Uuid::new_v4();
        let registry =
            PostgresPausedSandboxRegistry::new(pool.clone(), cluster_id, Duration::from_millis(50));
        super::super::schema::migrate(&pool).await.unwrap();

        let sandbox_a = SandboxId::new();
        let sandbox_b = SandboxId::new();
        bring_to_running(
            &registry,
            sandbox_a,
            cluster_id,
            "node-a",
            ExecutionId::new(),
            None,
        )
        .await;
        bring_to_running(
            &registry,
            sandbox_b,
            cluster_id,
            "node-b",
            ExecutionId::new(),
            None,
        )
        .await;

        tokio::time::sleep(Duration::from_millis(200)).await;
        let expired_a = raw_lease_expires_at(&pool, sandbox_a).await;
        let expired_b = raw_lease_expires_at(&pool, sandbox_b).await;
        let now = Utc::now();
        assert!(expired_a < now, "sandbox_a's lease should have lapsed");
        assert!(expired_b < now, "sandbox_b's lease should have lapsed");

        let roster_1 = crate::node_registry::types::Roster {
            node_id: "node-a".to_string(),
            entries: vec![crate::node_registry::types::RosterEntry {
                sandbox_id: sandbox_a.to_string(),
                execution_id: Uuid::new_v4().to_string(),
                projection_ttl: Duration::from_secs(30),
                paused: false,
            }],
            last_seen: Some(SystemTime::now()),
        };
        let roster_2 = crate::node_registry::types::Roster {
            node_id: "node-b".to_string(),
            entries: vec![crate::node_registry::types::RosterEntry {
                sandbox_id: sandbox_b.to_string(),
                execution_id: Uuid::new_v4().to_string(),
                projection_ttl: Duration::from_secs(30),
                paused: false,
            }],
            last_seen: Some(SystemTime::now()),
        };
        let replica_1 = SingleRosterRegistry(roster_1);
        let replica_2 = SingleRosterRegistry(roster_2);

        let (parked_1, live_1) = super::super::replica_renewal::renew_once(&registry, &replica_1)
            .await
            .expect("replica 1's renewal pass should succeed");
        assert_eq!((parked_1, live_1), (0, 1));

        let renewed_a = raw_lease_expires_at(&pool, sandbox_a).await;
        assert!(
            renewed_a > now,
            "sandbox_a must have been renewed by replica 1's own pass"
        );
        let still_expired_b = raw_lease_expires_at(&pool, sandbox_b).await;
        assert!(
            still_expired_b < now,
            "sandbox_b must still be expired -- replica 1's roster never mentioned it"
        );

        let (parked_2, live_2) = super::super::replica_renewal::renew_once(&registry, &replica_2)
            .await
            .expect("replica 2's renewal pass should succeed");
        assert_eq!((parked_2, live_2), (0, 1));

        let renewed_b = raw_lease_expires_at(&pool, sandbox_b).await;
        assert!(
            renewed_b > now,
            "sandbox_b must have been renewed by replica 2's own, independent pass"
        );

        assert!(registry.get(&sandbox_a).await.unwrap().is_some());
        assert!(registry.get(&sandbox_b).await.unwrap().is_some());
    }
}
