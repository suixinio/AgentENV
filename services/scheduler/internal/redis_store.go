package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"agentenv/services/shared/routing"

	"github.com/redis/go-redis/v9"
)

const defaultRedisOperationTimeout = routing.DefaultOperationTimeout
const defaultRedisBindingKeyPrefix = routing.DefaultKeyPrefix

// defaultRedisNodeIndexTTL bounds the reverse index, and 🔴 is deliberately not
// tied to the projection's TTL. The index says which sandboxes a node holds; it
// is reconciliation bookkeeping, not a route. Stretching it to match a
// long-lived projection would keep a permanently departed node's membership set
// alive longer than the records it points at.
const defaultRedisNodeIndexTTL = time.Hour

// redisBindingRecord is the shared record type, not a copy of it: the gateway
// decodes the same bytes out of the same keys and cannot import this package.
type redisBindingRecord = routing.Record

type RedisBindingStore struct {
	client           *redis.Client
	bindingTTL       time.Duration
	keyPrefix        string
	operationTimeout time.Duration
	// The two scripts are chosen once, at construction, from three variants
	// each. 🔴 Not one script with a mode argument: a script covering all
	// three can never be exercised in any one of them.
	recordScript    *redis.Script
	reconcileScript *redis.Script
	// 🔴 The third script takes no arbitration prelude. Deleting is not the
	// same decision as writing, and in the one case that matters they are
	// opposites: a record naming no incarnation yields to any named challenger
	// on the write path, and is deleted rather than protected on this one.
	// Prepending `accepts` here would invite somebody to call it.
	deleteScript *redis.Script
}

func NewRedisBindingStore(addr string, bindingTTL time.Duration) (*RedisBindingStore, error) {
	return NewRedisBindingStoreWithArbitration(addr, bindingTTL, redisArbitrationFenced)
}

// NewRedisBindingStoreWithArbitration builds the store around one of the three
// Lua arbitration preludes.
func NewRedisBindingStoreWithArbitration(addr string, bindingTTL time.Duration, arbitration string) (*RedisBindingStore, error) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return nil, fmt.Errorf("redis address is required")
	}
	if bindingTTL <= 0 {
		bindingTTL = defaultBindingTTL
	}

	var opts *redis.Options
	if strings.Contains(addr, "://") {
		parsed, err := redis.ParseURL(addr)
		if err != nil {
			return nil, fmt.Errorf("parse redis address: %w", err)
		}
		opts = parsed
	} else {
		opts = &redis.Options{Addr: addr}
	}

	store := &RedisBindingStore{
		client:           redis.NewClient(opts),
		bindingTTL:       bindingTTL,
		keyPrefix:        defaultRedisBindingKeyPrefix,
		operationTimeout: defaultRedisOperationTimeout,
		recordScript:     redis.NewScript(arbitration + redisRecordBindingScriptBody),
		reconcileScript:  redis.NewScript(arbitration + redisReconcileNodeScriptBody),
		deleteScript:     redis.NewScript(redisLuaHelpers + redisDeleteBindingScriptBody),
	}
	ctx, cancel := store.context()
	defer cancel()
	if err := store.client.Ping(ctx).Err(); err != nil {
		_ = store.client.Close()
		return nil, fmt.Errorf("connect redis: %w", err)
	}
	return store, nil
}

func (s *RedisBindingStore) Close() error {
	if s == nil || s.client == nil {
		return nil
	}
	return s.client.Close()
}

func (s *RedisBindingStore) Get(sandboxID string, _ time.Time) (Binding, bool, error) {
	sandboxID = strings.TrimSpace(sandboxID)
	if sandboxID == "" {
		return Binding{}, false, nil
	}

	ctx, cancel := s.context()
	defer cancel()
	raw, err := s.client.Get(ctx, s.bindingKey(sandboxID)).Bytes()
	if err != nil {
		if err == redis.Nil {
			return Binding{}, false, nil
		}
		return Binding{}, false, fmt.Errorf("redis get binding: %w", err)
	}
	binding, ok := parseRedisBindingBytes(raw)
	return binding, ok, nil
}

// Delete is the guarded delete, entirely inside one script.
//
// 🔴 GET, compare, DEL from Go would be three round trips with a window in the
// middle, and the window is precisely long enough for a resume to install a new
// incarnation — after which the DEL removes a live record. The comparison and
// the removal are one atomic step for the same reason the write path's
// arbitration is.
func (s *RedisBindingStore) Delete(sandboxID string, executionID string, _ time.Time) (BindingDeleteOutcome, error) {
	sandboxID = strings.TrimSpace(sandboxID)
	executionID = strings.TrimSpace(executionID)
	if sandboxID == "" || executionID == "" {
		return BindingDeleteAbsent, nil
	}

	ctx, cancel := s.context()
	defer cancel()
	raw, err := s.deleteScript.Run(ctx, s.client,
		[]string{s.bindingKey(sandboxID)},
		sandboxID,
		executionID,
		s.keyPrefix,
	).Result()
	if err != nil {
		return BindingDeleteAbsent, fmt.Errorf("redis delete binding: %w", err)
	}
	outcome, ok := raw.(string)
	if !ok {
		return BindingDeleteAbsent, fmt.Errorf("redis delete binding returned %T, want string", raw)
	}
	return BindingDeleteOutcome(outcome), nil
}

func (s *RedisBindingStore) Record(sandboxID string, binding Binding, _ time.Time) error {
	sandboxID = strings.TrimSpace(sandboxID)
	node := binding.Node
	node.ID = strings.TrimSpace(node.ID)
	node.Endpoint = strings.TrimSpace(node.Endpoint)
	if sandboxID == "" || node.ID == "" || node.Endpoint == "" {
		return nil
	}
	value, err := marshalRedisBindingRecord(node, binding.ExecutionID)
	if err != nil {
		return err
	}

	ctx, cancel := s.context()
	defer cancel()
	// 🔴 Through the same script — and therefore the same rule — as a
	// heartbeat. An assignment written unguarded would be the way around
	// everything the heartbeat path enforces.
	raw, err := s.recordScript.Run(ctx, s.client,
		[]string{s.bindingKey(sandboxID), s.nodeKey(node.ID)},
		value,
		node.ID,
		sandboxID,
		int64(s.bindingTTL/time.Millisecond),
		s.keyPrefix,
		s.nodeIndexTTLMillis(),
		binding.ExecutionID,
		projectionTTLMillis(binding.ProjectionTTL),
	).Result()
	if err != nil {
		return fmt.Errorf("redis record binding: %w", err)
	}
	reportRedisDecisions(bindingSourceAssignment, raw)
	return nil
}

func (s *RedisBindingStore) ReconcileNode(node Node, roster []RosterEntry, _ time.Time) error {
	node.ID = strings.TrimSpace(node.ID)
	node.Endpoint = strings.TrimSpace(node.Endpoint)
	if node.ID == "" {
		return nil
	}

	desired := normalizeRosterEntries(roster)
	if len(desired) > 0 && node.Endpoint == "" {
		return nil
	}

	// 🔴 The node half of the record is marshalled here, once, and the script
	// splices the incarnation in beside it. Re-encoding the node inside Lua
	// would work today and would quietly drop any field this type grows that
	// cjson does not know to keep — the shape of a stored record is defined by
	// the Go type and must not be defined a second time in a script.
	nodeJSON, err := json.Marshal(node)
	if err != nil {
		return err
	}

	// The incarnation and the projection budget both travel per sandbox: one
	// heartbeat reports many sandboxes and each has its own.
	args := make([]any, 0, 6+3*len(desired))
	args = append(args,
		node.ID,
		string(nodeJSON),
		int64(s.bindingTTL/time.Millisecond),
		s.keyPrefix,
		s.nodeIndexTTLMillis(),
		len(desired),
	)
	for _, entry := range desired {
		args = append(args, entry.SandboxID)
	}
	for _, entry := range desired {
		args = append(args, entry.ExecutionID)
	}
	// 🔴 A third parallel run rather than triples interleaved with the ids.
	// The reason is the one already written against the second run: a flat
	// pair-wise list is one indexing mistake away from giving every sandbox
	// its neighbour's value, and a wrong TTL is as silent as a wrong
	// incarnation.
	for _, entry := range desired {
		args = append(args, projectionTTLMillis(entry.ProjectionTTL))
	}

	ctx, cancel := s.context()
	defer cancel()
	raw, err := s.reconcileScript.Run(ctx, s.client, []string{s.nodeKey(node.ID)}, args...).Result()
	if err != nil {
		return fmt.Errorf("redis reconcile node bindings: %w", err)
	}
	reportRedisDecisions(bindingSourceHeartbeat, raw)
	return nil
}

// reportRedisDecisions turns the script's per-sandbox verdicts into metrics and
// warnings.
//
// 🔴 The script has to hand them back. A refusal decided inside Lua and never
// reported is a silent narrowing — and what it would be swallowing is the one
// fact worth knowing: that two copies of a sandbox are both reporting.
func reportRedisDecisions(source string, raw any) {
	entries, ok := raw.([]any)
	if !ok {
		return
	}
	for _, item := range entries {
		pair, ok := item.([]any)
		if !ok || len(pair) != 2 {
			continue
		}
		sandboxID, _ := pair[0].(string)
		decision, _ := pair[1].(string)
		if decision == "" {
			continue
		}
		recordBindingArbitration(source, bindingDecision(decision))
		warnRefusedBinding(source, sandboxID, bindingDecision(decision))
	}
}

// normalizeRosterEntries trims, drops blanks and de-duplicates, keeping the
// first spelling of each sandbox id.
func normalizeRosterEntries(roster []RosterEntry) []RosterEntry {
	seen := make(map[string]struct{}, len(roster))
	result := make([]RosterEntry, 0, len(roster))
	for _, entry := range roster {
		sandboxID := strings.TrimSpace(entry.SandboxID)
		if sandboxID == "" {
			continue
		}
		if _, ok := seen[sandboxID]; ok {
			continue
		}
		seen[sandboxID] = struct{}{}
		result = append(result, RosterEntry{
			SandboxID:     sandboxID,
			ExecutionID:   entry.ExecutionID,
			ProjectionTTL: entry.ProjectionTTL,
		})
	}
	return result
}

func (s *RedisBindingStore) context() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), s.operationTimeout)
}

func (s *RedisBindingStore) bindingKey(sandboxID string) string {
	return routing.BindingKey(s.keyPrefix, sandboxID)
}

func (s *RedisBindingStore) nodeKey(nodeID string) string {
	return routing.NodeIndexKey(s.keyPrefix, nodeID)
}

func (s *RedisBindingStore) nodeIndexTTLMillis() int64 {
	return int64(defaultRedisNodeIndexTTL / time.Millisecond)
}

// projectionTTLMillis turns a budget into the argument the scripts expect.
//
// 🔴 Zero is the only thing a non-positive budget may become, and zero means
// "fall back to binding_ttl" on the other side. It must never become an omitted
// expiry, which in Redis is a key that lives until something deletes it — and
// the paths that delete one are exactly the paths a lost projection write means
// we cannot rely on.
func projectionTTLMillis(ttl time.Duration) int64 {
	if ttl <= 0 {
		return 0
	}
	return int64(ttl / time.Millisecond)
}

func marshalRedisBindingRecord(node Node, executionID string) (string, error) {
	return routing.MarshalRecord(node, executionID)
}

func parseRedisBindingBytes(raw []byte) (Binding, bool) {
	record, ok := routing.ParseRecord(raw)
	if !ok {
		return Binding{}, false
	}
	// The stored record carries no TTL: what it had is the key's own expiry,
	// which Redis owns and nothing here needs to read back.
	return Binding{Node: record.Node, ExecutionID: record.ExecutionID}, true
}

// redisLuaHelpers is the shared prologue: decoding a stored record, and
// nothing else.
//
// parse_binding replaced parse_node_id when the incarnation arrived. The old
// name is worth remembering: the scripts already read the previous value, they
// simply read it to fix the reverse index and never to decide anything. Adding
// the decision costs no extra round trip at all.
const redisLuaHelpers = `
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
`

// The three arbitration preludes. Each defines `accepts`, and one of them is
// prepended to a script body when a store is built.
//
// 🔴 Three separate constants rather than one with a mode argument. A script
// carrying all three behaviours can never be exercised in any one of them, and
// the mode is a deployment decision that changes once — at construction — not a
// per-call one.
const (
	// redisArbitrationFenced is the rule, in the same six cases as
	// arbitrateFenced. Redis compares strings byte by byte, which for
	// lower-case canonical uuids is the same order Go's `>` gives.
	redisArbitrationFenced = redisLuaHelpers + `
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
`

	// redisArbitrationObserving works the decision out and writes anyway, so a
	// release can see what enforcing would do before it does it.
	redisArbitrationObserving = redisLuaHelpers + `
local function accepts(raw, challenger)
  local _, incumbent = parse_binding(raw)
  if not raw then return true, (challenger ~= "" and "installed" or "installed_unknown") end
  if not incumbent or incumbent == "" then
    return true, (challenger ~= "" and "installed" or "installed_unknown")
  end
  if challenger == "" then return true, "rejected_unknown" end
  if challenger == incumbent then return true, "refreshed" end
  if challenger > incumbent then return true, "superseded" end
  return true, "rejected_older"
end
`

	// redisArbitrationOff is the rollback: last writer wins, nothing counted.
	redisArbitrationOff = redisLuaHelpers + `
local function accepts(raw, challenger)
  return true, ""
end
`
)

// redisRecordBindingScriptBody is the assignment write.
//
// ARGV[7] is the challenger's incarnation, and the reverse-index fix that used
// to be unconditional now happens only when the write is accepted — a refused
// challenger must leave no trace at all, or its next empty roster deletes a
// binding it does not own.
const redisRecordBindingScriptBody = `
-- KEYS[1]: sandbox binding key, e.g. {prefix}:sandbox:{sandbox_id}
-- KEYS[2]: target node reverse-index set key, e.g. {prefix}:node:{node_id}
-- ARGV[1]: binding value JSON ({"node":{...},"execution_id":"..."})
-- ARGV[2]: target node ID
-- ARGV[3]: sandbox ID
-- ARGV[4]: binding TTL in milliseconds
-- ARGV[5]: redis binding key prefix, used to remove stale reverse-index entries from old nodes
-- ARGV[6]: node reverse-index set TTL in milliseconds
-- ARGV[7]: the challenger's execution id, empty when unknown
-- ARGV[8]: this sandbox's projection TTL in milliseconds; <= 0 falls back to ARGV[4]
local raw = redis.call("GET", KEYS[1])
local accept, decision = accepts(raw, ARGV[7])
if not accept then
  return { { ARGV[3], decision } }
end
local old_node_id = parse_binding(raw)
if old_node_id and old_node_id ~= ARGV[2] then
  redis.call("SREM", ARGV[5] .. ":node:" .. old_node_id, ARGV[3])
end
-- <= 0 is "the writer had no budget to give", which is the store's own default.
-- It is never "no expiry": an assignment write is the one place a record with
-- no deadline at all could be created.
local entry_ttl_ms = ARGV[8]
local entry_ttl_n = tonumber(entry_ttl_ms)
if not entry_ttl_n or entry_ttl_n <= 0 then
  entry_ttl_ms = ARGV[4]
end
redis.call("SET", KEYS[1], ARGV[1], "PX", entry_ttl_ms)
redis.call("SADD", KEYS[2], ARGV[3])
redis.call("PEXPIRE", KEYS[2], ARGV[6])
return { { ARGV[3], decision } }
`

// redisReconcileNodeScriptBody is the heartbeat write.
//
// The desired set arrives as two parallel runs of arguments — N sandbox ids
// followed by N incarnations — because Lua tables do not survive the boundary
// and a flat pair-wise list would be one indexing mistake away from binding
// every sandbox to its neighbour's incarnation.
const redisReconcileNodeScriptBody = `
-- KEYS[1]: reconciled node reverse-index set key, e.g. {prefix}:node:{node_id}
-- ARGV[1]: reconciled node ID
-- ARGV[2]: the reconciled node, marshalled by the caller (never re-encoded here)
-- ARGV[3]: binding TTL in milliseconds
-- ARGV[4]: redis binding key prefix
-- ARGV[5]: node reverse-index set TTL in milliseconds
-- ARGV[6]: desired sandbox count (N)
-- ARGV[7 .. 6+N]:      desired sandbox IDs
-- ARGV[7+N .. 6+2N]:   their execution ids, positionally aligned with the ids above
-- ARGV[7+2N .. 6+3N]:  their projection TTLs in milliseconds, aligned the same way
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
    local old_node_id = parse_binding(raw)
    if old_node_id and old_node_id ~= node_id then
      redis.call("SREM", node_key_for(old_node_id), sandbox_id)
    end
    -- Spliced rather than encoded: execution_id has already been validated as
    -- a canonical uuid or an empty string, so it needs no escaping, and the
    -- node object travels through untouched.
    local value = '{"node":' .. node_json .. ',"execution_id":"' .. execution_id .. '"}'
    if raw and decision == "refreshed" then
      -- 🔴 The same incarnation reporting again. This is the periodic tick, and
      -- it must not extend the record: a heartbeat every five seconds rewriting
      -- the deadline would put the projection's survival back on a periodic
      -- write path, which is the whole thing the long TTL exists to get it off.
      -- The record's contents are still rewritten — the node's endpoint or pod
      -- name may have changed — but its deadline is left exactly as it was.
      redis.call("SET", binding_key(sandbox_id), value, "KEEPTTL")
    else
      -- installed / installed_unknown (repairing a write that was lost) and
      -- superseded (a new incarnation, arriving with a new budget). All three
      -- are real lifecycle events rather than ticks, so all three set the
      -- deadline from the budget they came with.
      local entry_ttl_ms = entry_ttls[sandbox_id]
      local entry_ttl_n = tonumber(entry_ttl_ms)
      if not entry_ttl_n or entry_ttl_n <= 0 then
        entry_ttl_ms = ttl_ms
      end
      redis.call("SET", binding_key(sandbox_id), value, "PX", entry_ttl_ms)
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
`

// redisDeleteBindingScriptBody is the guarded delete a PAUSE or DELETE event
// drives.
//
// 🔴 It takes redisLuaHelpers alone and never an arbitration prelude. The rule
// here is not `accepts` with the sign flipped — in the one case that separates
// them they are outright opposites, and the reason is written on
// BindingDeleteUnknownIncumbent.
//
// 🔴 GET, compare and DEL are one script because they are one decision. The Go
// spelling of this — read it, compare it, delete it — has a window between the
// read and the delete that a resume fits inside, and the delete that follows
// removes a record naming an incarnation nobody ever compared.
const redisDeleteBindingScriptBody = `
-- KEYS[1]: sandbox binding key, e.g. {prefix}:sandbox:{sandbox_id}
-- ARGV[1]: sandbox ID
-- ARGV[2]: the event's execution id; never empty, the caller refuses those
-- ARGV[3]: redis binding key prefix, used to keep the reverse index in step
local raw = redis.call("GET", KEYS[1])
if not raw then
  return "noop_absent"
end
local node_id, incumbent = parse_binding(raw)
if not node_id then
  -- Undecodable, or naming no node: it routes nowhere, so nothing is lost by
  -- removing it. The reverse index cannot be repaired without a node id, and
  -- the next reconciliation of whichever node holds the membership will drop
  -- the stale member on its way past.
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
-- In step with the record, or the reverse index keeps a member pointing at a
-- key that is gone, and the next empty roster spends a GET rediscovering that.
redis.call("SREM", ARGV[3] .. ":node:" .. node_id, ARGV[1])
return outcome
`
