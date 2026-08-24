//! Every write that carries a predicate, as Lua.
//!
//! # 🔴 Why the predicates live in here and not in Rust
//!
//! e2b's own comment says it best, and it is worth repeating because e2b then
//! fails to follow it on one of its own paths:
//!
//! > it has to live in here rather than in Go: Add is lockless, so a resume can
//! > install a new incarnation between a Go-side comparison and this write, and
//! > the SET below would then overwrite the new live record with the stale one
//!
//! That argument applies to us more strongly, not less. `add` is lockless here
//! too, and `restore_sandbox` takes a caller-supplied id, so a new incarnation
//! can appear under an id at any moment. And the read-modify-write in the pause
//! path spans a disk write of tens to hundreds of milliseconds — an enormous
//! window by these standards.
//!
//! e2b's `Update` is a bare `SET` with no predicate at all. Copying the shape
//! without the predicate copies the defect; every write script below carries
//! one.

use std::sync::OnceLock;

use redis::Script;

macro_rules! lazy_script {
    ($name:ident, $body:expr) => {
        pub fn $name() -> &'static Script {
            static SCRIPT: OnceLock<Script> = OnceLock::new();
            SCRIPT.get_or_init(|| Script::new($body))
        }
    };
}

// ---------------------------------------------------------------------------
// add
// ---------------------------------------------------------------------------

/// `KEYS = [record, index, expiry, pending]`
///
/// `ARGV = [json, ttl_ms|'', expiry_score|'', expiry_member, sandbox_id]`
///
/// Returns `1` when the record was created, `0` when one already existed.
///
/// 🔴 The final `ZREM` on the pending set is not tidiness. A creation window
/// that is not closed when the record lands keeps occupying the id for the
/// whole stale cutoff, and a queue that only empties on failure is the exact
/// shape of a defect that took a build system cluster-wide: every *success*
/// left its slot occupied, and once enough successes had accumulated nothing
/// could start again.
const ADD: &str = r#"
if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end
if ARGV[2] ~= '' then
  redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
else
  redis.call('SET', KEYS[1], ARGV[1])
end
redis.call('SADD', KEYS[2], ARGV[5])
if ARGV[3] ~= '' then
  redis.call('ZADD', KEYS[3], ARGV[3], ARGV[4])
end
redis.call('ZREM', KEYS[4], ARGV[5])
return 1
"#;

lazy_script!(add, ADD);

// ---------------------------------------------------------------------------
// update
// ---------------------------------------------------------------------------

pub const UPDATE_OK: i64 = 1;
pub const UPDATE_NOT_FOUND: i64 = 0;
pub const UPDATE_UNDECODABLE: i64 = -1;
pub const UPDATE_REV_MISMATCH: i64 = 2;
pub const UPDATE_EXECUTION_MISMATCH: i64 = 3;

/// The one write that every read-modify-write path funnels through.
///
/// `KEYS = [record, expiry]`
///
/// `ARGV = [json, expected_rev|'', expected_execution|'', ttl_mode,
///          rescore_flag, old_member|'', new_member|'', new_score|'']`
///
/// `ttl_mode` is either the literal `keep` or a millisecond count.
///
/// 🔴 Two predicates, and neither is redundant:
///
/// * `rev` catches somebody having changed this record since it was read, even
///   within the same incarnation — two concurrent `keep_alive` calls, for
///   instance.
/// * `execution_id` catches the record having been replaced by a *different
///   run* of the same sandbox, which `rev` cannot see because a fresh record
///   starts its revisions over.
///
/// 🔴 `ttl_mode` is mandatory rather than defaulted, because both of its wrong
/// answers are silent. A bare `SET` clears the key's TTL, turning a bounded
/// record into an immortal one; recomputing a TTL on a path that should not
/// have touched it shortens the record's life below the sandbox's.
const UPDATE: &str = r#"
local raw = redis.call('GET', KEYS[1])
if not raw then return 0 end
local ok, cur = pcall(cjson.decode, raw)
if not ok then return -1 end
if ARGV[2] ~= '' and tostring(cur['rev']) ~= ARGV[2] then return 2 end
if ARGV[3] ~= '' and cur['execution_id'] ~= ARGV[3] then return 3 end
if ARGV[4] == 'keep' then
  redis.call('SET', KEYS[1], ARGV[1], 'KEEPTTL')
else
  redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[4])
end
if ARGV[5] == '1' then
  if ARGV[6] ~= '' then redis.call('ZREM', KEYS[2], ARGV[6]) end
  if ARGV[7] ~= '' then redis.call('ZADD', KEYS[2], ARGV[8], ARGV[7]) end
end
return 1
"#;

lazy_script!(update, UPDATE);

/// The same script with both predicates removed.
///
/// 🔴 Test builds only, and the only reason it exists is to be the negative
/// control: with it in place, two replicas racing the same sandbox produce a
/// double execution, which is what proves the predicates above are what stops
/// them. A runtime switch that could do this in production would be an
/// instance of precisely the class of thing this work removes.
#[cfg(test)]
const UPDATE_WITHOUT_PREDICATES: &str = r#"
local raw = redis.call('GET', KEYS[1])
if not raw then return 0 end
if ARGV[4] == 'keep' then
  redis.call('SET', KEYS[1], ARGV[1], 'KEEPTTL')
else
  redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[4])
end
if ARGV[5] == '1' then
  if ARGV[6] ~= '' then redis.call('ZREM', KEYS[2], ARGV[6]) end
  if ARGV[7] ~= '' then redis.call('ZADD', KEYS[2], ARGV[8], ARGV[7]) end
end
return 1
"#;

#[cfg(test)]
lazy_script!(update_without_predicates, UPDATE_WITHOUT_PREDICATES);

// ---------------------------------------------------------------------------
// remove
// ---------------------------------------------------------------------------

/// `KEYS = [record, index, expiry, pending]`, `ARGV = [sandbox_id]`
///
/// Returns the removed JSON, or nil.
///
/// 🔴 The expiry member is rebuilt from the incarnation in the record this
/// script *just deleted*, never from one the caller was holding. A caller's
/// copy can be one incarnation behind, and a `ZREM` built from it would delete
/// the live incarnation's member — after which the sandbox is in the store, has
/// an expiry, and is in nobody's expiry index, so it never expires again.
const REMOVE: &str = r#"
local raw = redis.call('GET', KEYS[1])
redis.call('DEL', KEYS[1])
redis.call('SREM', KEYS[2], ARGV[1])
redis.call('ZREM', KEYS[4], ARGV[1])
if raw then
  local ok, cur = pcall(cjson.decode, raw)
  if ok and cur['execution_id'] then
    redis.call('ZREM', KEYS[3], ARGV[1] .. ':' .. cur['execution_id'])
  end
end
if raw then return raw end
return false
"#;

lazy_script!(remove, REMOVE);

pub const FENCED_REMOVE_ABSENT: i64 = 0;
pub const FENCED_REMOVE_REMOVED: i64 = 1;
pub const FENCED_REMOVE_SUPERSEDED: i64 = 2;
pub const FENCED_REMOVE_UNDECODABLE: i64 = 3;

/// `KEYS = [record, index, expiry, pending]`
///
/// `ARGV = [sandbox_id, expected_execution, expected_state...]`
///
/// Returns `{code, state, execution_id}`, where `code` is one of the
/// `FENCED_REMOVE_*` constants above and the two detail fields are filled in
/// only for `FENCED_REMOVE_SUPERSEDED`.
///
/// 🔴 The predicate and the `DEL` are one script for the same reason
/// `SWEEP_INDEX_MEMBER` is: `add` is lockless and `restore_sandbox` supplies
/// its own id, so the record under an id can change owner between a `GET` in
/// Rust and the `DEL` that follows it — and this script's whole job is to
/// refuse to delete a record somebody else owns.
///
/// 🔴 An undecodable record is refused rather than deleted, which is the one
/// place this deliberately differs from [`REMOVE`]. `REMOVE` is an
/// unconditional instruction and may sweep bytes it cannot read; this one is a
/// claim of ownership, and bytes nobody can read prove no ownership.
const REMOVE_IF_EXECUTION: &str = r#"
local raw = redis.call('GET', KEYS[1])
if not raw then return {0, '', ''} end
local ok, cur = pcall(cjson.decode, raw)
if not ok then return {3, '', ''} end
local state = tostring(cur['state'])
local execution = tostring(cur['execution_id'])
if execution ~= ARGV[2] then return {2, state, execution} end
local matched = false
for i = 3, #ARGV do
  if ARGV[i] == cur['state'] then matched = true break end
end
if not matched then return {2, state, execution} end
redis.call('DEL', KEYS[1])
redis.call('SREM', KEYS[2], ARGV[1])
redis.call('ZREM', KEYS[4], ARGV[1])
redis.call('ZREM', KEYS[3], ARGV[1] .. ':' .. execution)
return {1, '', ''}
"#;

lazy_script!(remove_if_execution, REMOVE_IF_EXECUTION);

/// `KEYS = [index, record]`, `ARGV = [sandbox_id]`
///
/// Drops an index member whose record is gone, and returns how many it dropped.
///
/// 🔴 The existence check and the `SREM` must be in the same script. Doing the
/// check in Rust and the `SREM` afterwards is the "unindex a live one" defect
/// again: a lockless `add` landing in between would have its brand-new record
/// removed from the membership set while the record itself stayed.
const SWEEP_INDEX_MEMBER: &str = r#"
if redis.call('EXISTS', KEYS[2]) == 1 then return 0 end
return redis.call('SREM', KEYS[1], ARGV[1])
"#;

lazy_script!(sweep_index_member, SWEEP_INDEX_MEMBER);

// ---------------------------------------------------------------------------
// transitions
// ---------------------------------------------------------------------------

/// `KEYS = [record, transition, transition_index]`
///
/// `ARGV = [json, txn_id, txn_key_ttl_s, expected_execution|'', txn_member,
///          txn_deadline_ms, eviction_flag, now_ms, ttl_mode, expected_state...]`
///
/// Returns `{code, reason, detail}` where `code` is `1` for started and `0`
/// otherwise.
///
/// 🔴 Three checks e2b does outside its script are inside this one:
///
/// * the incarnation predicate, for the reason at the top of this file;
/// * whether a transition is already in flight, which is what lets this
///   primitive work without taking a lock at all — one fewer lock, and one
///   fewer TTL that has to be kept in order with the others;
/// * for an eviction, whether the sandbox is *still* expired. Today's evictor
///   reads the expiry index and then compare-and-sets on **state**, so a
///   `keep_alive` arriving in between pushes the expiry out and the sandbox is
///   paused anyway. That is a defect the current in-process store already has;
///   Redis only widens the window from two `await`s to two round trips.
const START_TRANSITION: &str = r#"
local raw = redis.call('GET', KEYS[1])
if not raw then return {0, 'not_found', ''} end
local ok, cur = pcall(cjson.decode, raw)
if not ok then return {0, 'undecodable', ''} end
if ARGV[4] ~= '' and cur['execution_id'] ~= ARGV[4] then
  return {0, 'execution_superseded', tostring(cur['execution_id'])}
end
local matched = false
for i = 10, #ARGV do
  if ARGV[i] == cur['state'] then matched = true break end
end
if not matched then return {0, 'state_conflict', tostring(cur['state'])} end
local inflight = redis.call('GET', KEYS[2])
if inflight then return {0, 'in_flight', inflight} end
if ARGV[7] == '1' then
  local exp = cur['expires_at_ms']
  if (not exp) or tonumber(exp) > tonumber(ARGV[8]) then
    return {0, 'not_expired', ''}
  end
end
if ARGV[9] == 'keep' then
  redis.call('SET', KEYS[1], ARGV[1], 'KEEPTTL')
else
  redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[9])
end
redis.call('SET', KEYS[2], ARGV[2], 'EX', ARGV[3])
redis.call('ZADD', KEYS[3], ARGV[6], ARGV[5])
return {1, 'started', ARGV[2]}
"#;

lazy_script!(start_transition, START_TRANSITION);

/// `KEYS = [transition, transition_result, transition_index]`
///
/// `ARGV = [txn_id, result_payload, result_ttl_s, txn_member]`
///
/// Returns `1` normally, `2` when the transition key had already expired, and
/// `0` when the key belongs to a different transition.
///
/// 🔴 Deleting the key and removing the index member are one script, where e2b
/// uses two commands and has no index at all. They describe the same fact and
/// must stop being true together: an index entry that survives its transition
/// is a permanent item in the reaper's queue, and the reaper would then keep
/// re-examining a sandbox that finished long ago.
const COMPLETE_TRANSITION: &str = r#"
local inflight = redis.call('GET', KEYS[1])
if inflight and inflight ~= ARGV[1] then return 0 end
redis.call('SET', KEYS[2], ARGV[2], 'EX', ARGV[3])
if inflight then redis.call('DEL', KEYS[1]) end
redis.call('ZREM', KEYS[3], ARGV[4])
if inflight then return 1 end
return 2
"#;

lazy_script!(complete_transition, COMPLETE_TRANSITION);

// ---------------------------------------------------------------------------
// reservations
// ---------------------------------------------------------------------------

pub const RESERVE_RESERVED: i64 = 0;
pub const RESERVE_ALREADY_IN_STORAGE: i64 = 1;
pub const RESERVE_ALREADY_PENDING: i64 = 2;
/// 🔴 Reserved, never returned. See [`super::super::Reservation::LimitExceeded`].
pub const RESERVE_LIMIT_EXCEEDED: i64 = 3;

/// `KEYS = [index, pending, reserve_result]`
///
/// `ARGV = [sandbox_id, now_s, stale_cutoff_s]`
///
/// 🔴 e2b's script counts the tenant's sandboxes against a quota here and can
/// return `3`. Those three lines are the only ones omitted, because there is no
/// tenant to count. The return code stays reserved so that adding one later is
/// a change to the middle of this script rather than to every caller.
const RESERVE: &str = r#"
redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', ARGV[3])
if redis.call('SISMEMBER', KEYS[1], ARGV[1]) == 1 then return 1 end
if redis.call('ZSCORE', KEYS[2], ARGV[1]) then return 2 end
redis.call('DEL', KEYS[3])
redis.call('ZADD', KEYS[2], ARGV[2], ARGV[1])
return 0
"#;

lazy_script!(reserve, RESERVE);

/// `KEYS = [pending, reserve_result]`
///
/// `ARGV = [sandbox_id, payload, result_ttl_s]`
///
/// 🔴 Removing the pending entry and publishing the result are one script for
/// the same reason completion is: a caller waiting on this sandbox decides it
/// has failed when the id leaves the pending set with no result behind it, and
/// between two separate commands that is briefly true of a creation that
/// succeeded.
const FINISH_RESERVATION: &str = r#"
redis.call('SET', KEYS[2], ARGV[2], 'EX', ARGV[3])
redis.call('ZREM', KEYS[1], ARGV[1])
return 1
"#;

lazy_script!(finish_reservation, FINISH_RESERVATION);

// ---------------------------------------------------------------------------
// locks and index repair
// ---------------------------------------------------------------------------

/// `KEYS = [lock]`, `ARGV = [token]`
///
/// 🔴 Compares the token before deleting. A bare `DEL` releases whichever lock
/// happens to be there, including the one the *next* holder just took after
/// this one's TTL expired.
const RELEASE_LOCK: &str = r#"
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
"#;

lazy_script!(release_lock, RELEASE_LOCK);

/// `KEYS = [expiry]`, `ARGV = [score, member, score, member, ...]`
///
/// 🔴 `NX`, so that several replicas healing at once cannot fight, and so that
/// healing never moves a score some other write has just set correctly.
const HEAL_EXPIRY: &str = r#"
local added = 0
for i = 1, #ARGV, 2 do
  added = added + redis.call('ZADD', KEYS[1], 'NX', ARGV[i], ARGV[i + 1])
end
return added
"#;

lazy_script!(heal_expiry, HEAL_EXPIRY);

#[cfg(test)]
mod tests {
    use super::*;

    /// Every script is valid Lua as far as the `Script` wrapper is concerned,
    /// and each has a stable SHA. This is a cheap guard against a typo in a
    /// literal that would otherwise only show up the first time that path runs
    /// against a live Redis.
    #[test]
    fn every_script_has_a_hash() {
        let scripts = [
            add(),
            update(),
            remove(),
            remove_if_execution(),
            sweep_index_member(),
            start_transition(),
            complete_transition(),
            reserve(),
            finish_reservation(),
            release_lock(),
            heal_expiry(),
        ];
        let mut hashes = std::collections::HashSet::new();
        for script in scripts {
            assert_eq!(script.get_hash().len(), 40);
            assert!(hashes.insert(script.get_hash().to_string()));
        }
    }

    /// 🔴 Named here and not only in the Lua, because dropping either
    /// predicate turns a fenced take-back into the unconditional [`REMOVE`] it
    /// must never become — and both scripts would still be valid Lua.
    #[test]
    fn the_fenced_remove_really_checks_both_predicates() {
        assert!(REMOVE_IF_EXECUTION.contains("cur['execution_id']"));
        assert!(REMOVE_IF_EXECUTION.contains("cur['state']"));
        // The control: the unconditional one checks neither, which is what
        // makes it the wrong script for a rollback to reach for.
        assert!(!REMOVE.contains("cur['state']"));
        assert_ne!(remove().get_hash(), remove_if_execution().get_hash());
    }

    /// 🔴 The negative control has to differ from the real script, or the
    /// mutation probe it exists for proves nothing.
    #[test]
    fn the_predicate_free_control_really_lacks_the_predicates() {
        assert!(UPDATE.contains("cur['rev']"));
        assert!(UPDATE.contains("cur['execution_id']"));
        assert!(!UPDATE_WITHOUT_PREDICATES.contains("cur['rev']"));
        assert!(!UPDATE_WITHOUT_PREDICATES.contains("cur['execution_id']"));
        assert_ne!(update().get_hash(), update_without_predicates().get_hash());
    }

    /// 🔴 The single most load-bearing four letters in this file. A `SET`
    /// without them turns a record with a bounded life into one that never
    /// expires, and nothing downstream can tell the difference until the
    /// keyspace has grown past `maxmemory` and `noeviction` starts refusing
    /// writes.
    #[test]
    fn the_update_script_keeps_the_ttl_when_asked_to() {
        assert!(UPDATE.contains("'KEEPTTL'"));
        assert!(START_TRANSITION.contains("'KEEPTTL'"));
        // And neither reaches a bare `SET` on the record key: both branches of
        // both scripts name an expiry policy.
        assert!(!UPDATE.contains("redis.call('SET', KEYS[1], ARGV[1])"));
        assert!(!START_TRANSITION.contains("redis.call('SET', KEYS[1], ARGV[1])"));
    }
}
