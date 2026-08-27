#!/usr/bin/env bash
set -euo pipefail

SUITE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "${SUITE_DIR}/../lib/helpers.sh"
init_suite "04_metadata"

log "Suite: Sandbox Metadata Filtering"

# -- Create two sandboxes with different metadata --
id_a=$(create_sandbox "$AENV_TEMPLATE_ID" 60 '{"metadata":{"team":"alpha","env":"staging"}}'); _sync_http
assert_status "$HTTP_STATUS" "201" "create sandbox A"
assert_not_empty "$id_a" "sandbox A ID"
track_sandbox "$id_a"

id_b=$(create_sandbox "$AENV_TEMPLATE_ID" 60 '{"metadata":{"team":"beta","env":"production"}}'); _sync_http
assert_status "$HTTP_STATUS" "201" "create sandbox B"
assert_not_empty "$id_b" "sandbox B ID"
track_sandbox "$id_b"

wait_for_sandbox_state "$id_a" "running" 30
wait_for_sandbox_state "$id_b" "running" 30

# -- Filter by metadata team=alpha --
api_get "/sandboxes?metadata=team%3Dalpha"
assert_status "$HTTP_STATUS" "200" "GET /sandboxes with metadata filter"
assert_contains "$HTTP_BODY" "$id_a" "filter returns sandbox A"

count=$(echo "$HTTP_BODY" | jq '[.[] | select(.sandboxID == "'"$id_b"'")] | length')
assert_eq "$count" "0" "filter excludes sandbox B"

if e2e_mode_is_clustered && ! node_rest_is_served; then
  # 🔴 Lost coverage, stated as such. The contrast below — a node lists its own
  # sandboxes and not the other node's — cannot be expressed against the split:
  # nodes serve no `/sandboxes`, and no user-facing API says which node owns a
  # sandbox. Restoring it needs an API affordance, not a test change.
  _pass "cross-node local metadata contrast $(node_rest_skip_reason)"
elif e2e_mode_is_clustered; then
  owner_a=$(find_sandbox_node_url "$id_a")
  owner_b=$(find_sandbox_node_url "$id_b")
  assert_not_empty "$owner_a" "sandbox A resolves to a runtime node"
  assert_not_empty "$owner_b" "sandbox B resolves to a runtime node"

  if [[ -n "$owner_a" && -n "$owner_b" && "$owner_a" != "$owner_b" ]]; then
    api_get_at "$owner_a" "/sandboxes?metadata=team%3Dalpha"
    assert_status "$HTTP_STATUS" "200" "owner node A metadata list returns 200"
    count=$(echo "$HTTP_BODY" | jq '[.[] | select(.sandboxID == "'"$id_a"'")] | length')
    assert_eq "$count" "1" "owner node A local filter returns sandbox A"

    api_get_at "$owner_b" "/sandboxes?metadata=team%3Dalpha"
    assert_status "$HTTP_STATUS" "200" "owner node B metadata list returns 200"
    count=$(echo "$HTTP_BODY" | jq '[.[] | select(.sandboxID == "'"$id_a"'")] | length')
    assert_eq "$count" "0" "non-owner local filter does not return sandbox A"
  else
    _pass "cross-node local metadata contrast skipped because both sandboxes landed on the same node"
  fi
fi

suite_summary "04_metadata"
