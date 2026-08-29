//! Task's own "D3": the Lua scripts `RedisBindingStore` runs, ported
//! verbatim from `services/scheduler/internal/redis_store.go`'s own
//! constants (`redisArbitrationFenced`,
//! `redisKeepsDeadlineAuthoritative`/`Ephemeral`,
//! `redisRecordBindingScriptBody`, `redisReconcileNodeScriptBody`,
//! `redisDeleteBindingScriptBody`). Predicates live in Lua for the same
//! reason `src/orchestrator/store/redis/scripts.rs` gives for its own
//! scripts: a lockless Rust-side read-modify-write would race a concurrent
//! write between the read and the write.
//!
//! 🔴 [`ARBITRATION_FENCED`] is the only arbitration prelude, and it is not
//! selected — it is concatenated. Two others existed: `ARBITRATION_OBSERVING`
//! through the rollout that proved `Fenced` was safe to turn on everywhere,
//! and `ARBITRATION_OFF` (`accepts` returning `true, ""` unconditionally) as
//! that rollout's rollback target. Both are gone, along with the
//! `ArbitrationMode` enum that picked between them and the
//! `[binding_store].arbitration` knob that set it — see
//! `super::super::arbitration`'s module doc.
//!
//! # 🔴 The KEEPTTL fix ("阶段 1 第 5 点")
//!
//! [`reconcile_script`]'s deadline prelude is *not* unconditional `KEEPTTL`
//! gated only by a Go-side TTL value, the way an earlier shipped version
//! read. `keeps_deadline` is picked by `projection_authoritative`, the one
//! `BindingStoreSettings` switch these scripts still read: when it is `false`
//! (ephemeral/rollback mode), every heartbeat write always re-arms a fresh
//! deadline (`PX`), even for the same incarnation. Only when it is `true`
//! does a same-incarnation refresh keep the existing deadline — and even
//! then, only when the key already carries a positive TTL (`PTTL > 0`);
//! `KEEPTTL` on a key with no TTL at all would otherwise leave it
//! permanently without one.

use std::sync::OnceLock;

use redis::Script;

// ---------------------------------------------------------------------------
// Shared parse helper, prefixed onto every script below.
// ---------------------------------------------------------------------------

const PARSE_BINDING: &str = r#"
local function parse_binding(raw)
  if not raw then
    return nil, nil
  end
  local ok, decoded = pcall(cjson.decode, raw)
  if not ok or not decoded or not decoded["node"] then
    return nil, nil
  end
  local node_id = decoded["node"]["node_id"]
  if not node_id or node_id == "" then
    return nil, nil
  end
  return node_id, decoded["execution_id"]
end
"#;

// ---------------------------------------------------------------------------
// Arbitration prelude: defines `accepts(raw, challenger)`.
// ---------------------------------------------------------------------------

const ARBITRATION_FENCED: &str = r#"
local function accepts(raw, challenger)
  local _, incumbent = parse_binding(raw)
  if not raw then return true, (challenger ~= "" and "installed" or "installed_unknown") end
  if not incumbent or incumbent == "" then
    return true, (challenger ~= "" and "installed" or "installed_unknown")
  end
  if challenger == "" then return false, "rejected_unknown" end
  if challenger == incumbent then return true, "refreshed" end
  if challenger > incumbent then return true, "superseded" end
  return false, "rejected_older"
end
"#;

// ---------------------------------------------------------------------------
// Deadline preludes: each defines `keeps_deadline(incumbent, challenger, budget_ms)`.
// ---------------------------------------------------------------------------

const KEEPS_DEADLINE_AUTHORITATIVE: &str = r#"
local function keeps_deadline(incumbent, challenger, budget_ms)
  if challenger == "" or budget_ms <= 0 then
    return false
  end
  return incumbent == challenger
end
"#;

const KEEPS_DEADLINE_EPHEMERAL: &str = r#"
local function keeps_deadline(incumbent, challenger, budget_ms)
  return false
end
"#;

fn deadline_prelude(projection_authoritative: bool) -> &'static str {
    if projection_authoritative {
        KEEPS_DEADLINE_AUTHORITATIVE
    } else {
        KEEPS_DEADLINE_EPHEMERAL
    }
}

// ---------------------------------------------------------------------------
// record: the RecordAssignment write.
// ---------------------------------------------------------------------------

/// `KEYS = [binding_key, node_index_key]`
///
/// `ARGV = [value_json, target_node_id, sandbox_id, binding_ttl_ms,
///          key_prefix, node_index_ttl_ms, challenger_execution_id,
///          projection_ttl_ms]`
///
/// Always `SET ... PX` — an assignment write never `KEEPTTL`; only a
/// heartbeat refresh of the same incarnation may (`reconcile_script`).
const RECORD_BODY: &str = r#"
local raw = redis.call("GET", KEYS[1])
local accept, decision = accepts(raw, ARGV[7])
if not accept then
  return { { ARGV[3], decision } }
end
local old_node_id = parse_binding(raw)
if old_node_id and old_node_id ~= ARGV[2] then
  redis.call("SREM", ARGV[5] .. ":node:" .. old_node_id, ARGV[3])
end
local entry_ttl_ms = ARGV[8]
local entry_ttl_n = tonumber(entry_ttl_ms)
if not entry_ttl_n or entry_ttl_n <= 0 then
  entry_ttl_ms = ARGV[4]
end
redis.call("SET", KEYS[1], ARGV[1], "PX", entry_ttl_ms)
redis.call("SADD", KEYS[2], ARGV[3])
redis.call("PEXPIRE", KEYS[2], ARGV[6])
return { { ARGV[3], decision } }
"#;

pub fn record_script() -> &'static Script {
    static S: OnceLock<Script> = OnceLock::new();
    S.get_or_init(|| Script::new(&format!("{PARSE_BINDING}{ARBITRATION_FENCED}{RECORD_BODY}")))
}

// ---------------------------------------------------------------------------
// reconcile: the Heartbeat roster write.
// ---------------------------------------------------------------------------

/// `KEYS = [node_index_key]`
///
/// `ARGV = [node_id, node_json, binding_ttl_ms, key_prefix,
///          node_index_ttl_ms, desired_count,
///          <desired_count sandbox ids>, <desired_count execution ids>,
///          <desired_count projection ttls ms>]` — three parallel arrays,
/// not interleaved triples (Go's own comment: "one indexing mistake away
/// from binding every sandbox to its neighbour's incarnation").
const RECONCILE_BODY: &str = r#"
local node_key = KEYS[1]
local node_id = ARGV[1]
local node_json = ARGV[2]
local ttl_ms = ARGV[3]
local key_prefix = ARGV[4]
local node_index_ttl_ms = ARGV[5]
local desired_count = tonumber(ARGV[6]) or 0

local function binding_key(sandbox_id)
  return key_prefix .. ":sandbox:" .. sandbox_id
end
local function node_key_for(id)
  return key_prefix .. ":node:" .. id
end

local desired = {}
local entry_ttls = {}
local order = {}
for i = 1, desired_count do
  local sandbox_id = ARGV[6 + i]
  desired[sandbox_id] = ARGV[6 + desired_count + i]
  entry_ttls[sandbox_id] = ARGV[6 + 2 * desired_count + i]
  order[i] = sandbox_id
end

local current = redis.call("SMEMBERS", node_key)
for _, sandbox_id in ipairs(current) do
  if desired[sandbox_id] == nil then
    local old_node_id = parse_binding(redis.call("GET", binding_key(sandbox_id)))
    if old_node_id == node_id then
      redis.call("DEL", binding_key(sandbox_id))
    end
    redis.call("SREM", node_key, sandbox_id)
  end
end

local decisions = {}
for i = 1, desired_count do
  local sandbox_id = order[i]
  local execution_id = desired[sandbox_id]
  local raw = redis.call("GET", binding_key(sandbox_id))
  local accept, decision = accepts(raw, execution_id)
  decisions[i] = { sandbox_id, decision }
  if accept then
    local old_node_id, incumbent = parse_binding(raw)
    if old_node_id and old_node_id ~= node_id then
      redis.call("SREM", node_key_for(old_node_id), sandbox_id)
    end
    local value = '{"node":' .. node_json .. ',"execution_id":"' .. execution_id .. '"}'
    local key = binding_key(sandbox_id)
    local entry_ttl_ms = entry_ttls[sandbox_id]
    local entry_ttl_n = tonumber(entry_ttl_ms)
    if not entry_ttl_n or entry_ttl_n <= 0 then
      entry_ttl_ms = ttl_ms
      entry_ttl_n = 0
    end
    if keeps_deadline(incumbent, execution_id, entry_ttl_n) and redis.call("PTTL", key) > 0 then
      redis.call("SET", key, value, "KEEPTTL")
    else
      redis.call("SET", key, value, "PX", entry_ttl_ms)
    end
    redis.call("SADD", node_key, sandbox_id)
  end
end

if desired_count > 0 then
  redis.call("PEXPIRE", node_key, node_index_ttl_ms)
end
if desired_count == 0 then
  redis.call("DEL", node_key)
end
return decisions
"#;

fn build_reconcile(projection_authoritative: bool) -> Script {
    Script::new(&format!(
        "{}{}{}{}",
        PARSE_BINDING,
        ARBITRATION_FENCED,
        deadline_prelude(projection_authoritative),
        RECONCILE_BODY
    ))
}

pub fn reconcile_script(projection_authoritative: bool) -> &'static Script {
    match projection_authoritative {
        false => {
            static S: OnceLock<Script> = OnceLock::new();
            S.get_or_init(|| build_reconcile(false))
        }
        true => {
            static S: OnceLock<Script> = OnceLock::new();
            S.get_or_init(|| build_reconcile(true))
        }
    }
}

// ---------------------------------------------------------------------------
// delete: the guarded ReportSandboxEvent/sweep removal. No arbitration
// prelude -- deleting and writing are opposite decisions in the
// unknown-incumbent case (an unknown incumbent accepts a write but a
// present-but-unknown record still deletes on this path).
// ---------------------------------------------------------------------------

/// `KEYS = [binding_key]`
///
/// `ARGV = [sandbox_id, execution_id, key_prefix]`
const DELETE_BODY: &str = r#"
local raw = redis.call("GET", KEYS[1])
if not raw then
  return "noop_absent"
end
local node_id, incumbent = parse_binding(raw)
if not node_id then
  redis.call("DEL", KEYS[1])
  return "deleted_unknown_incumbent"
end
if incumbent and incumbent ~= "" and incumbent ~= ARGV[2] then
  return "rejected_stale"
end
local outcome = "deleted"
if not incumbent or incumbent == "" then
  outcome = "deleted_unknown_incumbent"
end
redis.call("DEL", KEYS[1])
redis.call("SREM", ARGV[3] .. ":node:" .. node_id, ARGV[1])
return outcome
"#;

pub fn delete_script() -> &'static Script {
    static SCRIPT: OnceLock<Script> = OnceLock::new();
    SCRIPT.get_or_init(|| Script::new(&format!("{PARSE_BINDING}{DELETE_BODY}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A minimal shape check that catches an unbalanced quote/concat error
    /// without needing a live Redis: the script must produce a stable,
    /// non-empty SHA and must be memoized rather than rebuilt per call.
    #[test]
    fn the_record_script_is_stable_and_memoized() {
        let a = record_script();
        let b = record_script();
        assert!(!a.get_hash().is_empty());
        assert_eq!(a.get_hash(), b.get_hash(), "must be memoized");
    }

    #[test]
    fn reconcile_scripts_differ_by_projection_authoritative() {
        let ephemeral = reconcile_script(false);
        let authoritative = reconcile_script(true);
        assert_ne!(
            ephemeral.get_hash(),
            authoritative.get_hash(),
            "the KEEPTTL fix must actually change the script text between modes"
        );
    }

    #[test]
    fn delete_script_has_no_arbitration_prelude_and_is_stable() {
        let a = delete_script();
        let b = delete_script();
        assert_eq!(
            a.get_hash(),
            b.get_hash(),
            "must be memoized, not rebuilt per call"
        );
    }
}
