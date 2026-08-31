//! Creation reservation visible across replicas.
//! It deduplicates caller-supplied ids and keeps in-progress VMs from appearing orphaned.

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
        // This build has no script branch producing quota rejection.
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
        // Subscribe before probing to close the lost-wake window.
        let mut wake = inner.subscribe(&routing::reservation(sandbox_id));

        loop {
            if let Some(result) = try_read_result(inner, sandbox_id).await? {
                return result;
            }
            wake_or_poll(&mut wake, inner.config().poll_interval).await;
        }
    }
}

/// `None` means pending; `Some` carries the creation result.
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

    // Re-read after pending disappears to close the owner-publication race.
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

    match inner.read_record(sandbox_id).await? {
        Some(record) => Ok(Some(Ok(record.into_metadata()))),
        None => Ok(Some(Err(StoreError::SandboxNotFound {
            sandbox_id: *sandbox_id,
        }))),
    }
}
