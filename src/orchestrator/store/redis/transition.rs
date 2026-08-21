//! The transition key, its result, its guard — and the fourth piece e2b does
//! not have.
//!
//! # 🔴 Why a fourth piece
//!
//! e2b's crash recovery is three things acting together, and only the first is
//! usually named: the transition key's TTL lets the *next* operation start; the
//! allowed-transition table permits leaving a transitional state; and its
//! expiry sweep has a stale-cutoff branch that puts a sandbox stuck in a
//! transitional state onto the eviction list. The third is the actual exit.
//!
//! That exit has a hole for us. Every e2b sandbox has an end time. Ours has
//! `Option<SystemTime>`, and a sandbox with no timeout is **not in the expiry
//! index at all**. So a replica that dies half way through pausing such a
//! sandbox leaves a record in `Pausing` that no index anywhere will ever be
//! read for. Resume refuses it, delete compare-and-sets against `[Running,
//! Paused]`, misses, waits sixty seconds and fails. What the user sees is a
//! sandbox that cannot be deleted, for ever.
//!
//! Hence the transition index and the reaper below.

use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use tracing::{debug, info, warn};
use uuid::Uuid;

use super::super::{
    is_allowed_transition, state_token, Result, StoreError, TransitionCompleter, TransitionEffect,
    TransitionGuard, TransitionOutcome, TransitionRequest, TransitionSettlement,
};
use super::crud::{wake_or_poll, TtlMode};
use super::keys::{routing, TransitionMember};
use super::record::StoredSandboxRecord;
use super::{
    backend, backend_msg, duration_to_secs_ceil, now_millis, scripts, RoundReadiness, StoreInner,
};
use crate::orchestrator::SandboxState;
use crate::types::{ExecutionId, SandboxId};

/// How many index members one reaper round looks at.
const REAP_BATCH: usize = 128;

pub(super) async fn start_transition(
    inner: &Arc<StoreInner>,
    sandbox_id: &SandboxId,
    request: TransitionRequest,
) -> Result<TransitionOutcome> {
    let config = inner.config();
    let mut attempts = 0u32;

    loop {
        // 🔴 Re-read on every attempt rather than reusing what was read before
        // the wait. e2b's own comment on this retry says the parameter it
        // matters most for is the expected incarnation, "because the waiting
        // window is exactly the stretch in which the sandbox may be replaced".
        let current = inner.require_record(sandbox_id).await?;
        let from_state = current.metadata.state;

        // 🔴 The record already sitting in the state being asked for is not an
        // illegal edge — the transition script parks the record in its target
        // *before* it publishes the transition key, so "already there" is what
        // an in-flight transition towards the same state looks like from
        // outside. Checking legality first turned every join into
        // `InvalidTransition { from: Pausing, to: Pausing }`.
        if from_state == request.target_state {
            let mut connection = inner.connection();
            let inflight: Option<String> = redis::cmd("GET")
                .arg(inner.keys().transition(sandbox_id))
                .query_async(&mut connection)
                .await
                .map_err(backend)?;
            if let Some(transition_id) = inflight {
                debug!(
                    %sandbox_id,
                    %transition_id,
                    "joined a transition already heading for the same state"
                );
                return Ok(TransitionOutcome::InFlight { transition_id });
            }
            // Nobody is transitioning; somebody has simply already arrived.
            // That is being a step late, not asking for something impossible.
            return Err(StoreError::StateConflict {
                sandbox_id: *sandbox_id,
                expected_states: request.expected_states.clone(),
                actual_state: from_state,
            });
        }

        if !is_allowed_transition(from_state, request.target_state) {
            return Err(StoreError::InvalidTransition {
                sandbox_id: *sandbox_id,
                from: from_state,
                to: request.target_state,
            });
        }

        let transition_id = Uuid::now_v7();
        let member = TransitionMember::new(*sandbox_id, current.execution_id(), transition_id);

        let mut metadata = current.metadata.clone();
        metadata.state = request.target_state;
        metadata.sync_running_clock(SystemTime::now());
        let mut next = StoredSandboxRecord::new(&metadata, current.rev.saturating_add(1))?;
        next.inherit_placement_from(&current);

        let now = SystemTime::now();
        let deadline_ms = now_millis(now) + config.transition_key_ttl.as_millis() as i64;

        let mut connection = inner.connection();
        let mut invocation = scripts::start_transition().prepare_invoke();
        invocation
            .key(inner.keys().record(sandbox_id))
            .key(inner.keys().transition(sandbox_id))
            .key(inner.keys().transition_index())
            .arg(next.encode()?)
            .arg(transition_id.to_string())
            .arg(duration_to_secs_ceil(config.transition_key_ttl))
            .arg(
                request
                    .expected_execution_id
                    .map(|id| id.to_string())
                    .unwrap_or_default(),
            )
            .arg(member.encode())
            .arg(deadline_ms)
            .arg(if request.eviction { "1" } else { "0" })
            .arg(now_millis(now))
            // 🔴 A mid-operation state stamp must not touch the lifetime
            // budget: the sandbox is still running, and this write says only
            // that somebody has started doing something to it.
            .arg(TtlMode::Keep.resolve(&next, config.record_ttl_grace));
        // The expected states are variadic, and the script scans from the fixed
        // argument count onwards. Keep the two in step.
        for state in &request.expected_states {
            invocation.arg(state_token(*state));
        }

        let (code, reason, detail): (i64, String, String) = invocation
            .invoke_async(&mut connection)
            .await
            .map_err(backend)?;

        if code == 1 {
            inner.invalidate_listing_memo().await;
            inner.notify(&routing::record(sandbox_id)).await;
            let completer = Arc::new(RedisTransitionCompleter {
                inner: Arc::clone(inner),
                sandbox_id: *sandbox_id,
                from_state,
                transitional_state: request.target_state,
                effect: request.effect,
                member,
            });
            return Ok(TransitionOutcome::Started(TransitionGuard::new(
                *sandbox_id,
                transition_id.to_string(),
                request.target_state,
                completer,
            )));
        }

        match reason.as_str() {
            "not_found" => {
                return Err(StoreError::SandboxNotFound {
                    sandbox_id: *sandbox_id,
                })
            }
            "undecodable" => {
                return Err(backend_msg(format!(
                    "sandbox {sandbox_id} record could not be decoded by the transition script"
                )))
            }
            "execution_superseded" => {
                let actual = ExecutionId::parse_str(&detail).ok();
                warn!(
                    %sandbox_id,
                    refusal_code = "execution_superseded",
                    expected = ?request.expected_execution_id,
                    ?actual,
                    "refused to start a transition for a superseded incarnation"
                );
                return Err(StoreError::ExecutionSuperseded {
                    sandbox_id: *sandbox_id,
                    expected: request
                        .expected_execution_id
                        .unwrap_or(current.execution_id()),
                    actual,
                });
            }
            "state_conflict" => {
                return Err(StoreError::StateConflict {
                    sandbox_id: *sandbox_id,
                    expected_states: request.expected_states.clone(),
                    actual_state: from_state,
                })
            }
            // 🔴 Not an error. A `keep_alive` landing between the expiry scan
            // and this script is the system working.
            "not_expired" => return Ok(TransitionOutcome::NotExpired),
            "in_flight" => {
                // Branch one, as a backstop: the state can have changed between
                // the read above and this script. The common case is handled
                // before the script runs.
                if from_state == request.target_state {
                    return Ok(TransitionOutcome::InFlight {
                        transition_id: detail,
                    });
                }

                // Branch two: a different transition is in flight and the edge
                // out of it to ours is legal. Wait for it, then retry the whole
                // request.
                attempts += 1;
                if attempts > config.max_transition_retries {
                    warn!(
                        %sandbox_id,
                        attempts,
                        target = %request.target_state,
                        refusal_code = "transition_in_progress",
                        "gave up waiting for an in-flight transition"
                    );
                    return Err(StoreError::TransitionInProgress {
                        sandbox_id: *sandbox_id,
                        target: request.target_state,
                    });
                }
                debug!(
                    %sandbox_id,
                    attempts,
                    in_flight = %detail,
                    "waiting for an in-flight transition before retrying"
                );
                wait_for_state_to_settle(inner, sandbox_id, from_state).await?;
            }
            other => {
                return Err(backend_msg(format!(
                    "transition script returned an unexpected reason {other:?} for sandbox {sandbox_id}"
                )))
            }
        }
    }
}

/// Waits for the record to leave `state`, bounded so a caller cannot be parked
/// for ever inside a single attempt.
async fn wait_for_state_to_settle(
    inner: &Arc<StoreInner>,
    sandbox_id: &SandboxId,
    state: SandboxState,
) -> Result<()> {
    let config = inner.config();
    let deadline = tokio::time::Instant::now() + config.wait_transition_timeout;
    let mut wake = inner.subscribe(&routing::record(sandbox_id));

    while tokio::time::Instant::now() < deadline {
        match inner.read_record(sandbox_id).await? {
            None => return Ok(()),
            Some(record) if record.metadata.state != state => return Ok(()),
            Some(_) => {}
        }
        wake_or_poll(&mut wake, config.poll_interval).await;
    }
    Ok(())
}

struct RedisTransitionCompleter {
    inner: Arc<StoreInner>,
    sandbox_id: SandboxId,
    /// Where the record came from, and where a failure puts it back.
    from_state: SandboxState,
    /// Where the record is parked while the transition runs.
    ///
    /// 🔴 Held explicitly rather than derived from `effect`: `Transient` and
    /// `Terminal` both park in the caller's target state and differ only in
    /// where they go afterwards, so deriving it would need the target anyway.
    transitional_state: SandboxState,
    effect: TransitionEffect,
    member: TransitionMember,
}

#[async_trait]
impl TransitionCompleter for RedisTransitionCompleter {
    async fn complete(
        &self,
        transition_id: &str,
        outcome: std::result::Result<(), String>,
    ) -> Result<()> {
        let inner = &self.inner;
        let sandbox_id = self.sandbox_id;
        let transitional = self.transitional_state;

        // Step one: settle the record, in the order e2b settles it — state
        // first, then the result, then the key.
        let settle = match (&self.effect, outcome.is_ok()) {
            (TransitionEffect::Transient, _) => Settle::State(self.from_state),
            (TransitionEffect::Terminal(state), true) => Settle::State(*state),
            (TransitionEffect::Terminal(_), false) => Settle::State(self.from_state),
            (TransitionEffect::Removal, true) => Settle::Remove,
            (TransitionEffect::Removal, false) => Settle::State(self.from_state),
        };

        let settled = match settle {
            Settle::State(state) => inner
                .compare_and_set_state(&sandbox_id, state, &[transitional])
                .await
                .map(|_| ()),
            Settle::Remove => inner.remove_record(&sandbox_id).await.map(|_| ()),
        };

        if let Err(error) = &settled {
            // 🔴 Reported, then carried on with. Leaving the transition key
            // behind because the settling write failed would wedge the sandbox
            // until the TTL expired *and* leave nothing in the result key for a
            // waiter to read.
            warn!(
                %sandbox_id,
                %error,
                "failed to settle a sandbox after its transition; releasing the transition anyway"
            );
        }

        let payload = match &outcome {
            Ok(()) => String::new(),
            Err(message) => message.clone(),
        };

        let mut connection = inner.connection();
        let code: i64 = scripts::complete_transition()
            .key(inner.keys().transition(&sandbox_id))
            .key(
                inner
                    .keys()
                    .transition_result(&sandbox_id, &self.member.transition_id),
            )
            .key(inner.keys().transition_index())
            .arg(transition_id)
            .arg(payload)
            .arg(duration_to_secs_ceil(inner.config().transition_result_ttl))
            .arg(self.member.encode())
            .invoke_async(&mut connection)
            .await
            .map_err(backend)?;

        if code == 0 {
            warn!(
                %sandbox_id,
                transition_id,
                "completed a transition whose key had already been taken by another transition"
            );
        } else if code == 2 {
            warn!(
                %sandbox_id,
                transition_id,
                "completed a transition whose key had already expired; it took longer than its TTL"
            );
        }

        inner.notify(&routing::transition(&sandbox_id)).await;
        inner.notify(&routing::record(&sandbox_id)).await;
        settled
    }
}

enum Settle {
    State(SandboxState),
    Remove,
}

/// One reaper round.
///
/// 🔴 The recovery action is **not** "put it back to `Running`". A replica that
/// died mid-pause may or may not have already stopped the VM, and an `api`
/// replica cannot know which — the only process that knows is the node, whose
/// `ListSandboxes` either has that VM or does not. Guessing `Running` is
/// guessing. Instead the record is made eligible for eviction and handed to the
/// evictor, which goes down the full pause/delete path, which asks the node.
/// Hand the question to whoever can answer it.
pub(super) async fn reap_stuck_transitions(
    inner: &Arc<StoreInner>,
    now: SystemTime,
) -> Result<Vec<SandboxId>> {
    // 🔴 Re-read every round, so it works as a kill switch without a redeploy.
    // And a warm-up, so that the first round after a restart does not read
    // "nobody has been listening for the last minute" as "everything is stuck".
    match inner.reaper_readiness() {
        RoundReadiness::Disabled => {
            debug!("transition reaper is switched off");
            return Ok(Vec::new());
        }
        RoundReadiness::WarmingUp => {
            debug!("transition reaper is still warming up");
            return Ok(Vec::new());
        }
        RoundReadiness::Ready => {}
    }

    let mut connection = inner.connection();
    let members: Vec<String> = redis::cmd("ZRANGEBYSCORE")
        .arg(inner.keys().transition_index())
        .arg("-inf")
        .arg(now_millis(now))
        .arg("LIMIT")
        .arg(0)
        .arg(REAP_BATCH)
        .query_async(&mut connection)
        .await
        .map_err(backend)?;

    let mut reaped = Vec::new();
    for raw in members {
        let Some(member) = TransitionMember::decode(&raw) else {
            metrics::counter!("agentenv_store_transition_reaped_total", "reason" => "invalid")
                .increment(1);
            drop_member(inner, &raw).await?;
            continue;
        };

        let Some(record) = inner.read_record(&member.sandbox_id).await? else {
            metrics::counter!("agentenv_store_transition_reaped_total", "reason" => "orphan")
                .increment(1);
            drop_member(inner, &raw).await?;
            continue;
        };

        if record.execution_id() != member.execution_id {
            metrics::counter!("agentenv_store_transition_reaped_total", "reason" => "dead_execution")
                .increment(1);
            drop_member(inner, &raw).await?;
            continue;
        }

        let live: Option<String> = redis::cmd("GET")
            .arg(inner.keys().transition(&member.sandbox_id))
            .query_async(&mut connection)
            .await
            .map_err(backend)?;
        if live.as_deref() == Some(member.transition_id.to_string().as_str()) {
            // The key has not expired yet; the deadline in the index was simply
            // computed a little early. Leave it alone.
            continue;
        }

        match make_evictable(inner, &record, now).await {
            Ok(true) => {
                metrics::counter!("agentenv_store_transition_reaped_total", "reason" => "stuck")
                    .increment(1);
                info!(
                    sandbox_id = %member.sandbox_id,
                    state = %record.metadata.state,
                    "a transition outlived the replica that started it; \
                     handing the sandbox to the evictor rather than guessing what happened to its VM"
                );
                reaped.push(member.sandbox_id);
            }
            Ok(false) => {}
            Err(error) => {
                warn!(sandbox_id = %member.sandbox_id, %error, "failed to reap a stuck transition");
                continue;
            }
        }
        drop_member(inner, &raw).await?;
    }

    Ok(reaped)
}

async fn drop_member(inner: &Arc<StoreInner>, member: &str) -> Result<()> {
    let mut connection = inner.connection();
    let _: i64 = redis::cmd("ZREM")
        .arg(inner.keys().transition_index())
        .arg(member)
        .query_async(&mut connection)
        .await
        .map_err(backend)?;
    Ok(())
}

/// Gives a stuck record a coordinate the evictor can find it by.
async fn make_evictable(
    inner: &Arc<StoreInner>,
    record: &StoredSandboxRecord,
    now: SystemTime,
) -> Result<bool> {
    if record.metadata.expires_at.is_some_and(|at| at <= now) {
        // Already in the evictor's reach.
        return Ok(false);
    }

    let previous = record.metadata.clone();
    let mut metadata = record.metadata.clone();
    // 🔴 `expires_at` is set directly rather than through `set_timeout`: the
    // sandbox's configured timeout has not changed and must not appear to have.
    // What has changed is that this record now needs somebody to look at it.
    metadata.expires_at = Some(now);

    let mut next = StoredSandboxRecord::new(&metadata, record.rev.saturating_add(1))?;
    next.inherit_placement_from(record);

    match inner
        .write_record(
            &previous,
            &next,
            record.rev,
            Some(record.execution_id()),
            TtlMode::Recompute,
        )
        .await
    {
        Ok(()) => Ok(true),
        // Somebody else got there first, which is the desired outcome anyway.
        Err(StoreError::ConcurrentUpdate { .. }) | Err(StoreError::ExecutionSuperseded { .. }) => {
            Ok(false)
        }
        Err(other) => Err(other),
    }
}

pub(super) async fn read_transition_result(
    inner: &Arc<StoreInner>,
    sandbox_id: &SandboxId,
    transition_id: &Uuid,
) -> Result<TransitionSettlement> {
    let mut connection = inner.connection();
    let result: Option<String> = redis::cmd("GET")
        .arg(inner.keys().transition_result(sandbox_id, transition_id))
        .query_async(&mut connection)
        .await
        .map_err(backend)?;
    if let Some(result) = result {
        return Ok(TransitionSettlement::Settled(if result.is_empty() {
            Ok(())
        } else {
            Err(result)
        }));
    }

    let live: Option<String> = redis::cmd("GET")
        .arg(inner.keys().transition(sandbox_id))
        .query_async(&mut connection)
        .await
        .map_err(backend)?;
    if live.as_deref() == Some(transition_id.to_string().as_str()) {
        return Ok(TransitionSettlement::Running);
    }
    Ok(TransitionSettlement::OwnerVanished)
}
