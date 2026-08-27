//! The reservation primitive: three answers, not four.
//!
//! # 🔴 What this is actually for here
//!
//! The usual justification — de-duplicating concurrent creates, so that a
//! client's retry waits for the first attempt instead of getting a conflict —
//! **does not apply to this API**. `NewSandbox` has no `sandboxID` field; the
//! server mints one with `SandboxId::new()`. A client that retries `POST
//! /sandboxes` gets a second sandbox, not a collision, so `AlreadyInStorage`
//! and `AlreadyPending` can never be returned on that path.
//!
//! Two other things do need it, and the second is the dangerous one:
//!
//! 1. `restore_sandbox` takes a **caller-supplied** id. Two replicas handling a
//!    resume of the same paused sandbox both call it with the same id.
//!
//! 2. 🔴 The creation window is otherwise **invisible to the cluster**.
//!    `store.add` runs *after* the VM exists — after image pulls, after boot,
//!    after waiting for envd — so for seconds to tens of seconds there is a
//!    running VM with no record anywhere. Anything reconciling a node's
//!    `ListSandboxes` against the store sees a VM with no record and concludes
//!    orphan, and the treatment for an orphan is to kill it. The pending set is
//!    what makes that window visible.
//!
//! Getting the reason wrong matters, because the obvious conclusion from "the
//! server mints the id" is that this primitive can be skipped — which would
//! take the second reason with it.

use std::sync::Arc;

use async_trait::async_trait;
use tracing::warn;

use super::super::Reservation;
use super::super::{
    ReservationFinisher, ReservationGuard, Result, SandboxMetadata, StartWaiter, StoreError,
    WaitForStart,
};
use super::crud::wake_or_poll;
use super::keys::routing;
use super::{backend, backend_msg, duration_to_secs_ceil, scripts, StoreInner};
use crate::types::SandboxId;

pub async fn reserve(inner: &Arc<StoreInner>, sandbox_id: &SandboxId) -> Result<Reservation> {
    let config = inner.config();
    let now = std::time::SystemTime::now();
    let now_secs = now
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or_default();
    let cutoff = now_secs - config.reserve_stale_ttl.as_secs() as i64;

    let mut connection = inner.connection();
    let outcome: i64 = scripts::reserve()
        .key(inner.keys().index())
        .key(inner.keys().pending())
        .key(inner.keys().reserve_result(sandbox_id))
        .arg(sandbox_id.to_string())
        .arg(now_secs)
        .arg(cutoff)
        .invoke_async(&mut connection)
        .await
        .map_err(backend)?;

    match outcome {
        scripts::RESERVE_RESERVED => Ok(Reservation::Reserved(ReservationGuard::new(
            *sandbox_id,
            Arc::new(RedisReservationFinisher {
                inner: Arc::clone(inner),
            }),
        ))),
        scripts::RESERVE_ALREADY_IN_STORAGE => Ok(Reservation::AlreadyInStorage),
        scripts::RESERVE_ALREADY_PENDING => Ok(Reservation::AlreadyPending(WaitForStart::new(
            *sandbox_id,
            Arc::new(RedisStartWaiter {
                inner: Arc::clone(inner),
            }),
        ))),
        // 🔴 The script has no branch that produces this. Reported as a backend
        // fault rather than panicking, because a `LimitExceeded` arriving from
        // a build that cannot produce one means the script in Redis is not the
        // script in this binary.
        scripts::RESERVE_LIMIT_EXCEEDED => Err(backend_msg(
            "reservation script reported a quota rejection, but this build has no tenant model \
             and no script branch that can produce one; the loaded script does not match this binary",
        )),
        other => Err(backend_msg(format!(
            "reservation script returned an unexpected code {other} for sandbox {sandbox_id}"
        ))),
    }
}

struct RedisReservationFinisher {
    inner: Arc<StoreInner>,
}

#[async_trait]
impl ReservationFinisher for RedisReservationFinisher {
    async fn finish(
        &self,
        sandbox_id: &SandboxId,
        outcome: std::result::Result<(), String>,
    ) -> Result<()> {
        let inner = &self.inner;
        let payload = match &outcome {
            Ok(()) => String::new(),
            Err(message) if message.is_empty() => "sandbox creation failed".to_string(),
            Err(message) => message.clone(),
        };

        let mut connection = inner.connection();
        let _: i64 = scripts::finish_reservation()
            .key(inner.keys().pending())
            .key(inner.keys().reserve_result(sandbox_id))
            .arg(sandbox_id.to_string())
            .arg(payload)
            .arg(duration_to_secs_ceil(inner.config().reserve_result_ttl))
            .invoke_async(&mut connection)
            .await
            .map_err(backend)?;

        inner.notify(&routing::reservation(sandbox_id)).await;
        Ok(())
    }
}

struct RedisStartWaiter {
    inner: Arc<StoreInner>,
}

#[async_trait]
impl StartWaiter for RedisStartWaiter {
    async fn wait(&self, sandbox_id: &SandboxId) -> Result<SandboxMetadata> {
        let inner = &self.inner;
        // Subscribe before the first probe: the creation may finish between the
        // two otherwise, and the waiter then sleeps until the poll fires.
        let mut wake = inner.subscribe(&routing::reservation(sandbox_id));

        loop {
            if let Some(result) = try_read_result(inner, sandbox_id).await? {
                return result;
            }
            wake_or_poll(&mut wake, inner.config().poll_interval).await;
        }
    }
}

/// `None` means "still pending"; `Some` means the creation has an answer.
async fn try_read_result(
    inner: &Arc<StoreInner>,
    sandbox_id: &SandboxId,
) -> Result<Option<Result<SandboxMetadata>>> {
    if let Some(result) = read_settled(inner, sandbox_id).await? {
        return Ok(Some(result));
    }

    let mut connection = inner.connection();
    let pending: Option<f64> = redis::cmd("ZSCORE")
        .arg(inner.keys().pending())
        .arg(sandbox_id.to_string())
        .query_async(&mut connection)
        .await
        .map_err(backend)?;
    if pending.is_some() {
        return Ok(None);
    }

    // 🔴 Read the result once more. e2b found this race in production: the
    // owner can publish its result between the first read above and this
    // pending check, and without the second read a creation that succeeded is
    // reported as one that vanished.
    if let Some(result) = read_settled(inner, sandbox_id).await? {
        return Ok(Some(result));
    }

    warn!(
        %sandbox_id,
        "the creation window closed with no result behind it; whoever held it did not finish"
    );
    Ok(Some(Err(backend_msg(format!(
        "sandbox {sandbox_id} is no longer pending and left no creation result"
    )))))
}

async fn read_settled(
    inner: &Arc<StoreInner>,
    sandbox_id: &SandboxId,
) -> Result<Option<Result<SandboxMetadata>>> {
    let mut connection = inner.connection();
    let payload: Option<String> = redis::cmd("GET")
        .arg(inner.keys().reserve_result(sandbox_id))
        .query_async(&mut connection)
        .await
        .map_err(backend)?;

    let Some(payload) = payload else {
        return Ok(None);
    };
    if !payload.is_empty() {
        return Ok(Some(Err(backend_msg(payload))));
    }

    // An empty payload means success, and success means there is a record.
    match inner.read_record(sandbox_id).await? {
        Some(record) => Ok(Some(Ok(record.into_metadata()))),
        None => Ok(Some(Err(StoreError::SandboxNotFound {
            sandbox_id: *sandbox_id,
        }))),
    }
}
