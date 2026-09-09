//! Global bounded expiry index and repair loop.

use std::sync::Arc;
use std::time::SystemTime;

use tracing::{debug, warn};

use super::super::{Result, SandboxMetadata};
use super::keys::ExpiryMember;
use super::record::{to_unix_millis, StoredSandboxRecord};
use super::{backend, now_millis, scripts, RoundReadiness, StoreInner};
use crate::orchestrator::SandboxState;

const HEAL_SCAN_COUNT: usize = 256;

// Expiry-less records use their lifetime deadline when one exists.
fn desired_score(metadata: &SandboxMetadata, now: SystemTime) -> Option<i64> {
    metadata
        .expires_at
        .or_else(|| metadata.lifetime_deadline(now))
        .map(to_unix_millis)
}

pub async fn expired_batch(
    inner: &Arc<StoreInner>,
    now: SystemTime,
    limit: usize,
) -> Result<Vec<SandboxMetadata>> {
    let config = inner.config();
    let now_ms = now_millis(now);
    let count: i64 = if limit == usize::MAX {
        -1
    } else {
        limit.min(config.expired_batch_limit) as i64
    };

    let mut connection = inner.connection();
    let members: Vec<String> = redis::cmd("ZRANGEBYSCORE")
        .arg(inner.keys().expiry())
        .arg("-inf")
        .arg(now_ms)
        .arg("LIMIT")
        .arg(0)
        .arg(count)
        .query_async(&mut connection)
        .await
        .map_err(backend)?;

    let mut parsed = Vec::with_capacity(members.len());
    let mut stale: Vec<String> = Vec::new();
    for raw in members {
        match ExpiryMember::decode(&raw) {
            Some(member) => parsed.push((raw, member)),
            None => {
                metrics::counter!("agentenv_store_expiry_index_swept_total", "reason" => "invalid")
                    .increment(1);
                stale.push(raw);
            }
        }
    }

    let mut expired = Vec::new();
    let mut rescore: Vec<(i64, String)> = Vec::new();

    for chunk in parsed.chunks(config.batch_chunk) {
        let keys: Vec<String> = chunk
            .iter()
            .map(|(_, member)| inner.keys().record(&member.sandbox_id))
            .collect();
        // Abort the whole round if any chunk cannot be read.
        let raws: Vec<Option<Vec<u8>>> = redis::cmd("MGET")
            .arg(&keys)
            .query_async(&mut connection)
            .await
            .map_err(backend)?;

        for ((raw_member, member), raw) in chunk.iter().zip(raws) {
            let Some(raw) = raw else {
                metrics::counter!("agentenv_store_expiry_index_swept_total", "reason" => "orphan")
                    .increment(1);
                stale.push(raw_member.clone());
                continue;
            };
            let record = StoredSandboxRecord::decode(&raw)?;

            // Incarnation-scoped members can be removed without unindexing replacements.
            if record.execution_id() != member.execution_id {
                metrics::counter!(
                    "agentenv_store_expiry_index_swept_total",
                    "reason" => "dead_execution"
                )
                .increment(1);
                stale.push(raw_member.clone());
                continue;
            }

            if !record.metadata.is_expired(now) {
                match desired_score(&record.metadata, now) {
                    Some(score) => rescore.push((score, raw_member.clone())),
                    None => stale.push(raw_member.clone()),
                }
                continue;
            }

            // Transitional records become evictable only after the stale cutoff.
            if record.metadata.state != SandboxState::Running {
                let overdue = record
                    .metadata
                    .expires_at
                    .and_then(|at| now.duration_since(at).ok())
                    .unwrap_or_default();
                if overdue <= config.stale_cutoff {
                    debug!(
                        sandbox_id = %member.sandbox_id,
                        state = %record.metadata.state,
                        "expired sandbox is mid-transition; leaving it to finish"
                    );
                    continue;
                }
            }

            expired.push(record.into_metadata());
        }
    }

    if !stale.is_empty() {
        let _: i64 = redis::cmd("ZREM")
            .arg(inner.keys().expiry())
            .arg(&stale)
            .query_async(&mut connection)
            .await
            .map_err(backend)?;
    }

    if !rescore.is_empty() {
        rescore_existing_members(&mut connection, &inner.keys().expiry(), &rescore).await?;
        metrics::counter!("agentenv_store_expiry_index_rescored_total")
            .increment(rescore.len() as u64);
    }

    Ok(expired)
}

/// Rescores existing members with `ZADD XX`, never recreating removed entries.
pub async fn rescore_existing_members(
    connection: &mut redis::aio::ConnectionManager,
    key: &str,
    members: &[(i64, String)],
) -> Result<()> {
    let mut pipeline = redis::pipe();
    for (score, member) in members {
        pipeline
            .cmd("ZADD")
            .arg(key)
            .arg("XX")
            .arg(*score)
            .arg(member)
            .ignore();
    }
    let _: () = pipeline.query_async(connection).await.map_err(backend)?;
    Ok(())
}

/// Repairs missing members with `ZADD NX`, safe to run concurrently.
pub async fn heal_expiry_index(inner: &Arc<StoreInner>) -> Result<usize> {
    // Kill switches and warm-up state are re-evaluated every round.
    match inner.healer_readiness() {
        RoundReadiness::Disabled => {
            debug!("expiry healer is switched off");
            return Ok(0);
        }
        RoundReadiness::WarmingUp => {
            debug!("expiry healer is still warming up");
            return Ok(0);
        }
        RoundReadiness::Ready => {}
    }

    let now = SystemTime::now();
    let mut connection = inner.connection();
    let mut cursor: u64 = 0;
    let mut healed = 0usize;

    loop {
        // Use SSCAN so a repair round does not fetch the full set at once.
        let (next, members): (u64, Vec<String>) = redis::cmd("SSCAN")
            .arg(inner.keys().index())
            .arg(cursor)
            .arg("COUNT")
            .arg(HEAL_SCAN_COUNT)
            .query_async(&mut connection)
            .await
            .map_err(backend)?;
        cursor = next;

        if !members.is_empty() {
            healed += heal_batch(inner, &mut connection, &members, now).await?;
        }
        if cursor == 0 {
            break;
        }
    }

    if healed > 0 {
        metrics::counter!("agentenv_store_expiry_index_healed_total").increment(healed as u64);
    }
    Ok(healed)
}

async fn heal_batch(
    inner: &Arc<StoreInner>,
    connection: &mut redis::aio::ConnectionManager,
    members: &[String],
    now: SystemTime,
) -> Result<usize> {
    let config = inner.config();
    let ids: Vec<_> = members
        .iter()
        .filter_map(|raw| crate::types::SandboxId::parse_str(raw).ok())
        .collect();
    if ids.is_empty() {
        return Ok(0);
    }

    let keys: Vec<String> = ids.iter().map(|id| inner.keys().record(id)).collect();
    let raws: Vec<Option<Vec<u8>>> = redis::cmd("MGET")
        .arg(&keys)
        .query_async(connection)
        .await
        .map_err(backend)?;

    let mut wanted: Vec<(i64, String)> = Vec::new();
    for raw in raws.into_iter().flatten() {
        let record = StoredSandboxRecord::decode(&raw)?;
        // Skip records young enough to still be under construction.
        if now
            .duration_since(record.metadata.created_at)
            .map(|age| age < config.heal_grace)
            .unwrap_or(true)
        {
            continue;
        }
        let Some(score) = desired_score(&record.metadata, now) else {
            continue;
        };
        wanted.push((
            score,
            ExpiryMember::new(record.sandbox_id(), record.execution_id()).encode(),
        ));
    }
    if wanted.is_empty() {
        return Ok(0);
    }

    let existing: Vec<Option<f64>> = redis::cmd("ZMSCORE")
        .arg(inner.keys().expiry())
        .arg(wanted.iter().map(|(_, member)| member).collect::<Vec<_>>())
        .query_async(connection)
        .await
        .map_err(backend)?;

    let mut invocation = scripts::heal_expiry().prepare_invoke();
    invocation.key(inner.keys().expiry());
    let mut missing = 0usize;
    for ((score, member), present) in wanted.iter().zip(existing) {
        // Unix-millisecond scores are never zero.
        if present.is_some_and(|score| score != 0.0) {
            continue;
        }
        invocation.arg(*score).arg(member);
        missing += 1;
    }
    if missing == 0 {
        return Ok(0);
    }

    let added: i64 = invocation.invoke_async(connection).await.map_err(backend)?;
    if added > 0 {
        warn!(
            repaired = added,
            "expiry index was missing entries for live records; repaired them"
        );
    }
    Ok(added.max(0) as usize)
}
