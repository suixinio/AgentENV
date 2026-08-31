//! Atomic Lua writes carrying revision, execution, and state predicates.

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

// KEYS: record, index, expiry, pending.
// ARGV: JSON, TTL, expiry score/member, sandbox id.
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

pub const UPDATE_OK: i64 = 1;
pub const UPDATE_NOT_FOUND: i64 = 0;
pub const UPDATE_UNDECODABLE: i64 = -1;
pub const UPDATE_REV_MISMATCH: i64 = 2;
pub const UPDATE_EXECUTION_MISMATCH: i64 = 3;

// KEYS: record, expiry. ARGV includes JSON, CAS predicates, TTL policy, and rescore data.
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

// Test-only update script without CAS predicates.
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

// Unconditional removal using the execution stored in the deleted record.
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

// Fenced removal atomically checks execution and expected states before deletion.
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

// Drops a membership entry only when its record is absent in the same script.
const SWEEP_INDEX_MEMBER: &str = r#"
if redis.call('EXISTS', KEYS[2]) == 1 then return 0 end
return redis.call('SREM', KEYS[1], ARGV[1])
"#;

lazy_script!(sweep_index_member, SWEEP_INDEX_MEMBER);

// Starts one fenced transition and optionally revalidates eviction expiry.
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

// Completes a transition and removes its reaper index member atomically.
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

pub const RESERVE_RESERVED: i64 = 0;
pub const RESERVE_ALREADY_IN_STORAGE: i64 = 1;
pub const RESERVE_ALREADY_PENDING: i64 = 2;
/// Reserved for a future tenant quota model; never returned today.
pub const RESERVE_LIMIT_EXCEEDED: i64 = 3;

// Reserves an id after removing stale pending entries.
const RESERVE: &str = r#"
redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', ARGV[3])
if redis.call('SISMEMBER', KEYS[1], ARGV[1]) == 1 then return 1 end
if redis.call('ZSCORE', KEYS[2], ARGV[1]) then return 2 end
redis.call('DEL', KEYS[3])
redis.call('ZADD', KEYS[2], ARGV[2], ARGV[1])
return 0
"#;

lazy_script!(reserve, RESERVE);

// Publishes the result before removing the pending entry.
const FINISH_RESERVATION: &str = r#"
redis.call('SET', KEYS[2], ARGV[2], 'EX', ARGV[3])
redis.call('ZREM', KEYS[1], ARGV[1])
return 1
"#;

lazy_script!(finish_reservation, FINISH_RESERVATION);

// Releases a lock only when its token still matches.
const RELEASE_LOCK: &str = r#"
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
"#;

lazy_script!(release_lock, RELEASE_LOCK);

// Adds missing expiry members with `ZADD NX`.
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

    #[test]
    fn the_fenced_remove_really_checks_both_predicates() {
        assert!(REMOVE_IF_EXECUTION.contains("cur['execution_id']"));
        assert!(REMOVE_IF_EXECUTION.contains("cur['state']"));
        assert!(!REMOVE.contains("cur['state']"));
        assert_ne!(remove().get_hash(), remove_if_execution().get_hash());
    }

    #[test]
    fn the_predicate_free_control_really_lacks_the_predicates() {
        assert!(UPDATE.contains("cur['rev']"));
        assert!(UPDATE.contains("cur['execution_id']"));
        assert!(!UPDATE_WITHOUT_PREDICATES.contains("cur['rev']"));
        assert!(!UPDATE_WITHOUT_PREDICATES.contains("cur['execution_id']"));
        assert_ne!(update().get_hash(), update_without_predicates().get_hash());
    }

    #[test]
    fn the_update_script_keeps_the_ttl_when_asked_to() {
        assert!(UPDATE.contains("'KEEPTTL'"));
        assert!(START_TRANSITION.contains("'KEEPTTL'"));
        assert!(!UPDATE.contains("redis.call('SET', KEYS[1], ARGV[1])"));
        assert!(!START_TRANSITION.contains("redis.call('SET', KEYS[1], ARGV[1])"));
    }
}
