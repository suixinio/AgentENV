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

    /// **H2(b)**: the two refusals this backend can give a caller mean
    /// opposite things -- `GenerationConflict` says "your view of the row is
    /// stale, re-read and try again"; `ExecutionFenced` says "the run you
    /// are writing on behalf of is over, stop retrying for good". Every
    /// other test in this file only asserts the variant it *expects*; this
    /// one also asserts the *other* variant did not come back instead, for
    /// both scenarios. Mirrors Go's `TestTwoRefusalsAreToldApart`: "a build
    /// that wrapped one in the other would look correct in every test that
    /// asserts 'the write was refused', and would send a node into a
    /// re-read loop that walks straight around the fence."
    #[tokio::test]
    async fn two_refusals_are_told_apart() {
        let pool = isolated_schema_pool_or_skip!("two_refusals_are_told_apart");
        let cluster_id = Uuid::new_v4();
        let registry = registry(pool, cluster_id).await;

        // Scenario 1: a stale generation on `complete_pause` -- the row is
        // still under the *same* incarnation, just a different generation
        // than the caller last observed. Must be `GenerationConflict`, never
        // `ExecutionFenced`.
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

        // Scenario 2: a pause sent under an incarnation the row has already
        // moved past (a cross-node resume happened since). Must be
        // `ExecutionFenced`, never `GenerationConflict`.
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

    /// **H2(d)**: `ResumeClaim::NotReady` had never once been constructed or
    /// asserted on anywhere in this suite before this test -- an entire
    /// result variant with zero coverage. A `publishing` row whose lease has
    /// *not* yet lapsed is claimable by nobody (only its origin node can
    /// serve it while the upload is still in flight): calls
    /// `writes::claim_for_resume` directly with `durable_only: false` so the
    /// outcome is decided purely by the row's own state/lease, independent
    /// of this cluster's restart-grace phase (`super::grace`), which this
    /// test is not about.
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

        // The row is `publishing` with a fresh (default 90s) lease -- not
        // claimable by a degraded takeover, and never claimable outright
        // (publishing/local_only never are -- only a lapsed lease makes them
        // eligible, and only when `durable_only` is false).
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

    /// **H2(c)**: the one cell of `mark_running`'s outcome table this suite
    /// had zero coverage for -- a `running` row held by a *different* node
    /// entirely (not a stale retry of the same incarnation, which is
    /// `ExecutionFenced` above, and not a `resuming` claim someone else
    /// holds, which `mark_running_a_claim_another_node_holds_reports_held_
    /// elsewhere` already covers). This is
    /// `writes.rs::mark_running`'s `(entry.state == Running &&
    /// entry.origin_node_id == holder)` branch's *false* half: without it,
    /// a build that swapped the two outcomes for this specific cell (`Held
    /// Elsewhere` versus `ExecutionFenced`) would pass every other test in
    /// this file.
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

        // node-c never held any claim on this sandbox and is not its
        // current holder -- this must read as the healthy "somebody else
        // has it" case, not as this node's own incarnation having gone
        // stale.
        let outcome = registry
            .mark_running(&sandbox_id, "node-c", "node-c", ExecutionId::new(), None)
            .await
            .unwrap();
        assert_eq!(outcome, MarkRunningOutcome::HeldElsewhere);

        // And the row itself must be untouched -- node-a still holds it.
        let row = registry.get(&sandbox_id).await.unwrap().unwrap();
        assert_eq!(row.state, PausedRegistryState::Running);
        assert_eq!(row.origin_node_id, "node-a");
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

    // ---------------------------------------------------------------------
    // list_all
    // ---------------------------------------------------------------------

    /// `ListRegistrySandboxes`'s own read carries what [`PausedSandboxEntry`]
    /// deliberately does not: `lease_expires_at`/`sandbox_expires_at`/
    /// `execution_id`, scoped to this registry's own cluster, alongside a
    /// database clock reading a real transaction was read against.
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

        // A row in a different cluster must never appear in this cluster's
        // listing -- the same isolation `get`/`get_many` already enforce via
        // their own `cluster_id` bind.
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

        // The database clock the rows were read against must be a real,
        // recent reading -- not a zero value, and not this process's own
        // clock read at a different instant than the query.
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

    /// **H2(a)**, half one: Go's `TestContractAResumingClaimIsReleasedEven
    /// WithAFarFutureInheritedDeadline` -- the resuming SQL arm's own
    /// omission of `sandbox_expires_at` is proved with a row that actually
    /// *carries* a non-null, far-future deadline (inherited from a previous
    /// `running` incarnation -- `begin_pause`/`complete_pause`/
    /// `claim_for_resume` none of them touch that column, so it survives an
    /// ordinary pause/resume cycle untouched), not merely a row where the
    /// column happens to be `NULL`
    /// (`a_resuming_row_with_a_lapsed_lease_and_no_deadline_is_reclaimed`,
    /// above). Without this test, a "fix" that re-merged the two reclaim
    /// statements behind a `COALESCE(sandbox_expires_at, 'epoch')` guard
    /// would still pass every other test in this file while leaving a
    /// resuming row with a real future deadline permanently stuck.
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

        // An ordinary pause -- sandbox_expires_at is not in either
        // statement's SET list, so it carries over untouched.
        let began = registry
            .begin_pause(&entry(sandbox_id, cluster_id, "node-a", running_execution))
            .await
            .unwrap();
        registry
            .complete_pause(&sandbox_id, began.generation, &snapshot_id())
            .await
            .unwrap();

        // A cross-node resume claim -- `claim_for_resume` does not touch
        // sandbox_expires_at either.
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

    /// **H2(a)**, half two: Go's `TestContractAFreshResumeClaimSurvivesRe
    /// claimWhileItsLeaseIsStillLive`. Without this test, a "fix" that made
    /// reclaim release *every* `resuming` row unconditionally (ignoring the
    /// lease entirely) would still pass every other reclaim test in this
    /// file -- they all reclaim only after sleeping past a short lease.
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

        // No sleep -- the (default, 90s) lease is still live.
        let outcome = registry.reclaim_expired_holdings().await.unwrap();
        assert_eq!(
            outcome.released, 0,
            "a resuming row with a still-live lease must not be reclaimed"
        );
        let row = registry.get(&sandbox_id).await.unwrap().unwrap();
        assert_eq!(row.state, PausedRegistryState::Resuming);
    }

    /// **H2(d)**: `ReleasedHoldings::discarded` had only ever been asserted
    /// `== 0` in this file -- the non-zero path had never actually run. A
    /// `running` row with no durable snapshot behind it (a pause that never
    /// got as far as `complete_pause`) whose lease *and* deadline have both
    /// passed is gone for good, not merely parked -- `RECLAIM_DISCARDED_SQL`
    /// deletes it outright rather than releasing it to `paused`.
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
        // No `complete_pause` -- snapshot_id stays NULL.
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

    // ---------------------------------------------------------------------
    // B1: replica_renewal's per-replica coverage
    // ---------------------------------------------------------------------

    /// A [`NodeRegistry`] that answers `rosters_in_cluster` with exactly one
    /// fixed, always-fresh [`Roster`] -- everything else is unreachable from
    /// [`super::super::replica_renewal::renew_once`], the only method this
    /// test exercises. Simulates one `--role api` replica whose heartbeat
    /// connections happen to cover exactly one node.
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

    /// **B1's own regression pin**: two replicas, each holding only *part*
    /// of the cluster's heartbeat roster (as `AtomicNodeRegistry` actually
    /// is -- per-process, unsynchronised), each independently renew only the
    /// `running` row their own roster names -- and the **union** of the two
    /// covers every row. Neither replica's pass alone would have.
    ///
    /// Before B1, this exact renewal only ran on whichever replica held the
    /// reconcile leader lock, using *that* replica's own roster alone -- a
    /// `running` row whose node's heartbeat was pinned to any other replica
    /// had no renewal path here at all. This test's own "before" half (the
    /// mid-test assertion that node-b's lease is *still* expired after only
    /// replica 1's pass) is the direct evidence that coverage really is
    /// partial per replica, not an artifact of this test's own setup.
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

        // Let both leases lapse -- the ordinary steady-state condition
        // Fix A's renewal exists to prevent from mattering.
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
            }],
            last_seen: Some(SystemTime::now()),
        };
        let roster_2 = crate::node_registry::types::Roster {
            node_id: "node-b".to_string(),
            entries: vec![crate::node_registry::types::RosterEntry {
                sandbox_id: sandbox_b.to_string(),
                execution_id: Uuid::new_v4().to_string(),
                projection_ttl: Duration::from_secs(30),
            }],
            last_seen: Some(SystemTime::now()),
        };
        let replica_1 = SingleRosterRegistry(roster_1);
        let replica_2 = SingleRosterRegistry(roster_2);

        // Replica 1's pass, alone: only node-a's row is in its roster.
        let (parked_1, live_1) = super::super::replica_renewal::renew_once(&registry, &replica_1)
            .await
            .expect("replica 1's renewal pass should succeed");
        assert_eq!((parked_1, live_1), (0, 1));

        let renewed_a = raw_lease_expires_at(&pool, sandbox_a).await;
        assert!(
            renewed_a > now,
            "sandbox_a must have been renewed by replica 1's own pass"
        );
        // 🔴 The coverage-is-partial claim: replica 1's pass, which never
        // saw node-b at all, must not have touched sandbox_b's row.
        let still_expired_b = raw_lease_expires_at(&pool, sandbox_b).await;
        assert!(
            still_expired_b < now,
            "sandbox_b must still be expired -- replica 1's roster never mentioned it"
        );

        // Replica 2's pass, independently: only node-b's row is in its roster.
        let (parked_2, live_2) = super::super::replica_renewal::renew_once(&registry, &replica_2)
            .await
            .expect("replica 2's renewal pass should succeed");
        assert_eq!((parked_2, live_2), (0, 1));

        let renewed_b = raw_lease_expires_at(&pool, sandbox_b).await;
        assert!(
            renewed_b > now,
            "sandbox_b must have been renewed by replica 2's own, independent pass"
        );

        // The union: both rows are now healthy, even though neither replica
        // ever saw the other's node.
        assert!(registry.get(&sandbox_a).await.unwrap().is_some());
        assert!(registry.get(&sandbox_b).await.unwrap().is_some());
    }
}
