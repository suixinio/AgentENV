//! Fenced transition lifecycle and crash-recovery index.
//! The reaper makes ownerless transitions evictable even without sandbox expiry.

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

const REAP_BATCH: usize = 128;

pub async fn start_transition(
    inner: &Arc<StoreInner>,
    sandbox_id: &SandboxId,
    request: TransitionRequest,
) -> Result<TransitionOutcome> {
    let config = inner.config();
    let mut attempts = 0u32;

    loop {
        // Re-read after every wait because the execution may have changed.
        let current = inner.require_record(sandbox_id).await?;
        let from_state = current.metadata.state;

        // Same target state denotes a joinable in-flight transition.
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
            // The state settled before the transition key was observed.
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
            // A transition stamp must preserve the record lifetime.
            .arg(TtlMode::Keep.resolve(&next, config.record_ttl_grace));
        // Variadic expected states follow fixed script arguments.
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
            // Keep-alive winning the expiry race is not an error.
            "not_expired" => return Ok(TransitionOutcome::NotExpired),
            "in_flight" => {
                // Handle a state change between the initial read and script.
                if from_state == request.target_state {
                    return Ok(TransitionOutcome::InFlight {
                        transition_id: detail,
                    });
                }

                // Wait for compatible in-flight work, then retry from a fresh read.
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

// Waits boundedly for the record to leave one state.
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
    from_state: SandboxState,
    // Explicit target state is not derivable from every settlement effect.
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

        // Settle the record before publishing the result and releasing the key.
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
            // Publish the failure result even when record settlement fails.
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

    async fn release(&self, transition_id: &str) -> Result<()> {
        let inner = &self.inner;
        let sandbox_id = self.sandbox_id;
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
            .arg(String::new())
            .arg(duration_to_secs_ceil(inner.config().transition_result_ttl))
            .arg(self.member.encode())
            .invoke_async(&mut connection)
            .await
            .map_err(backend)?;

        if code == 0 {
            warn!(
                %sandbox_id,
                transition_id,
                "released a transition whose key had already been taken by another transition"
            );
        }

        inner.notify(&routing::transition(&sandbox_id)).await;
        Ok(())
    }
}

enum Settle {
    State(SandboxState),
    Remove,
}

/// Makes ownerless transitions evictable without guessing runtime state.
pub async fn reap_stuck_transitions(
    inner: &Arc<StoreInner>,
    now: SystemTime,
) -> Result<Vec<SandboxId>> {
    // Re-evaluate kill switch and warm-up every round.
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
            // Transition key is still live; leave its index entry.
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

// Gives a stuck record a coordinate in the expiry index.
async fn make_evictable(
    inner: &Arc<StoreInner>,
    record: &StoredSandboxRecord,
    now: SystemTime,
) -> Result<bool> {
    if record.metadata.expires_at.is_some_and(|at| at <= now) {
        // Already visible to the evictor.
        return Ok(false);
    }

    let previous = record.metadata.clone();
    let mut metadata = record.metadata.clone();
    // Set expiry directly without changing the configured timeout.
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
        // A concurrent winner already achieved the desired recovery.
        Err(StoreError::ConcurrentUpdate { .. }) | Err(StoreError::ExecutionSuperseded { .. }) => {
            Ok(false)
        }
        Err(other) => Err(other),
    }
}

pub async fn read_transition_result(
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
