//! Atomic Lua predicates for Redis binding writes.
//! Authoritative heartbeat refreshes keep an existing positive TTL; other writes re-arm it.

use std::sync::OnceLock;

use redis::Script;

const PARSE_BINDING: &str = r#"
local function parse_binding(raw)
  if not raw then
    return nil, nil, nil, nil
  end
  local ok, decoded = pcall(cjson.decode, raw)
  if not ok or not decoded or not decoded["node"] then
    return nil, nil, nil, nil
  end
  local node_id = decoded["node"]["node_id"]
  if not node_id or node_id == "" then
    return nil, nil, nil, nil
  end
  return node_id, decoded["execution_id"], decoded["state"], decoded["reserved_at_ms"]
end

local function is_reservation(raw)
  local _, _, state = parse_binding(raw)
  return state == "starting"
end
"#;

// inflight_ttl_ms is binding_store::LAUNCH_RESERVATION_EXCLUSIVE_TTL, handed in
// by the caller; a reservation younger than it is not superseded, because
// superseding it starts a second runtime under an id somebody is starting. A
// reservation with no stamp is one an older writer left and is not exclusive.
const ARBITRATION_FENCED: &str = r#"
local function accepts(raw, challenger, now_ms, inflight_ttl_ms)
  local _, incumbent, state, reserved_at_ms = parse_binding(raw)
  if not raw then return true, (challenger ~= "" and "installed" or "installed_unknown") end
  if not incumbent or incumbent == "" then
    return true, (challenger ~= "" and "installed" or "installed_unknown")
  end
  if challenger == "" then return false, "rejected_unknown" end
  if challenger == incumbent then return true, "refreshed" end
  if challenger > incumbent then
    local reserved = tonumber(reserved_at_ms)
    local now = tonumber(now_ms) or 0
    local ttl = tonumber(inflight_ttl_ms) or 0
    if state == "starting" and reserved and (now - reserved) < ttl then
      return false, "rejected_inflight"
    end
    return true, "superseded"
  end
  return false, "rejected_older"
end
"#;

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

// KEYS: binding, node index. ARGV: value, node, sandbox, TTLs, prefix, execution,
// entry TTL, the caller's clock, and the reservation exclusivity window.
// Assignment writes always set a fresh TTL.
const RECORD_BODY: &str = r#"
local raw = redis.call("GET", KEYS[1])
local accept, decision = accepts(raw, ARGV[7], ARGV[9], ARGV[10])
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

// KEYS: node index. ARGV carries node metadata and three parallel roster arrays.
const RECONCILE_BODY: &str = r#"
local node_key = KEYS[1]
local node_id = ARGV[1]
local node_json = ARGV[2]
local ttl_ms = ARGV[3]
local key_prefix = ARGV[4]
local node_index_ttl_ms = ARGV[5]
local desired_count = tonumber(ARGV[6]) or 0
local now_ms = ARGV[6 + 3 * desired_count + 1]
local inflight_ttl_ms = ARGV[6 + 3 * desired_count + 2]

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
    local raw = redis.call("GET", binding_key(sandbox_id))
    local old_node_id = parse_binding(raw)
    -- A reservation names a create this node has not acknowledged yet, so its
    -- roster is right to omit it. Only its TTL may retire it.
    if old_node_id == node_id and not is_reservation(raw) then
      redis.call("DEL", binding_key(sandbox_id))
      redis.call("SREM", node_key, sandbox_id)
    elseif old_node_id ~= node_id then
      redis.call("SREM", node_key, sandbox_id)
    end
  end
end

local decisions = {}
for i = 1, desired_count do
  local sandbox_id = order[i]
  local execution_id = desired[sandbox_id]
  local raw = redis.call("GET", binding_key(sandbox_id))
  local accept, decision = accepts(raw, execution_id, now_ms, inflight_ttl_ms)
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

// KEYS: binding. ARGV: sandbox id, execution id, key prefix.
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

// KEYS: binding. ARGV: sandbox id, execution id, key prefix.
// Withdraws only a reservation; a confirmation names a runtime a node acknowledged.
const RELEASE_BODY: &str = r#"
local raw = redis.call("GET", KEYS[1])
if not raw then
  return "noop_absent"
end
local node_id, incumbent, state = parse_binding(raw)
if not node_id then
  return "noop_absent"
end
if state ~= "starting" then
  return "rejected_confirmed"
end
if incumbent and incumbent ~= "" and incumbent ~= ARGV[2] then
  return "rejected_stale"
end
redis.call("DEL", KEYS[1])
redis.call("SREM", ARGV[3] .. ":node:" .. node_id, ARGV[1])
return "deleted"
"#;

pub fn release_script() -> &'static Script {
    static SCRIPT: OnceLock<Script> = OnceLock::new();
    SCRIPT.get_or_init(|| Script::new(&format!("{PARSE_BINDING}{RELEASE_BODY}")))
}

// KEYS: reservation. ARGV: execution id, now_ms, window_ms, value.
// First writer wins for the window: a launch has not chosen a node yet, so
// there is nothing to order two of them by, only who asked first.
const RESERVE_LAUNCH_BODY: &str = r#"
local raw = redis.call("GET", KEYS[1])
if raw then
  local ok, decoded = pcall(cjson.decode, raw)
  local holder = ""
  if ok and decoded and type(decoded["execution_id"]) == "string" then
    holder = decoded["execution_id"]
  end
  if holder ~= "" and holder ~= ARGV[1] then
    local reserved = tonumber(decoded["reserved_at_ms"])
    local now = tonumber(ARGV[2]) or 0
    local window = tonumber(ARGV[3]) or 0
    if reserved and (now - reserved) < window then
      return { "held_elsewhere", holder }
    end
    redis.call("SET", KEYS[1], ARGV[4], "PX", ARGV[3])
    return { "claimed_from_expired", holder }
  end
end
redis.call("SET", KEYS[1], ARGV[4], "PX", ARGV[3])
return { "claimed", "" }
"#;

pub fn reserve_launch_script() -> &'static Script {
    static SCRIPT: OnceLock<Script> = OnceLock::new();
    SCRIPT.get_or_init(|| Script::new(RESERVE_LAUNCH_BODY))
}

// KEYS: reservation. ARGV: execution id.
const RELEASE_LAUNCH_BODY: &str = r#"
local raw = redis.call("GET", KEYS[1])
if not raw then
  return "noop_absent"
end
local ok, decoded = pcall(cjson.decode, raw)
local holder = ""
if ok and decoded and type(decoded["execution_id"]) == "string" then
  holder = decoded["execution_id"]
end
if holder == "" then
  redis.call("DEL", KEYS[1])
  return "deleted_unknown_incumbent"
end
if holder ~= ARGV[1] then
  return "rejected_stale"
end
redis.call("DEL", KEYS[1])
return "deleted"
"#;

// KEYS: reservation. ARGV: now_ms, window_ms.
// The window is re-read inside the same call that deletes, so a reservation
// renewed between a sweep's read and its delete is not reaped.
const REAP_LAUNCH_BODY: &str = r#"
local raw = redis.call("GET", KEYS[1])
if not raw then
  return 0
end
local ok, decoded = pcall(cjson.decode, raw)
if ok and decoded then
  local reserved = tonumber(decoded["reserved_at_ms"])
  local now = tonumber(ARGV[1]) or 0
  local window = tonumber(ARGV[2]) or 0
  if reserved and (now - reserved) < window then
    return 0
  end
end
return redis.call("DEL", KEYS[1])
"#;

pub fn reap_launch_script() -> &'static Script {
    static SCRIPT: OnceLock<Script> = OnceLock::new();
    SCRIPT.get_or_init(|| Script::new(REAP_LAUNCH_BODY))
}

pub fn release_launch_script() -> &'static Script {
    static SCRIPT: OnceLock<Script> = OnceLock::new();
    SCRIPT.get_or_init(|| Script::new(RELEASE_LAUNCH_BODY))
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn both_arbitrated_scripts_are_built_from_the_one_accepts_prelude() {
        assert_eq!(
            Script::new(&format!("{PARSE_BINDING}{ARBITRATION_FENCED}{RECORD_BODY}")).get_hash(),
            record_script().get_hash(),
            "the record script must be the shared preludes and nothing else"
        );
        assert_eq!(
            Script::new(&format!(
                "{PARSE_BINDING}{ARBITRATION_FENCED}{KEEPS_DEADLINE_EPHEMERAL}{RECONCILE_BODY}"
            ))
            .get_hash(),
            reconcile_script(false).get_hash(),
            "a reservation refused on the assignment path and superseded on the heartbeat path \
             would be two different arbitrations under one name"
        );
    }

    #[test]
    fn the_launch_reservation_scripts_are_their_own_bodies_and_are_memoized() {
        assert_eq!(
            reserve_launch_script().get_hash(),
            Script::new(RESERVE_LAUNCH_BODY).get_hash(),
            "the reservation arbitrates on its own key and borrows no routing prelude"
        );
        assert_eq!(
            release_launch_script().get_hash(),
            Script::new(RELEASE_LAUNCH_BODY).get_hash()
        );
        assert_ne!(
            reserve_launch_script().get_hash(),
            release_launch_script().get_hash()
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
