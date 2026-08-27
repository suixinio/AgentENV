#!/usr/bin/env bash
set -euo pipefail

SUITE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "${SUITE_DIR}/../lib/helpers.sh"
init_suite "10_cluster_deployment"

log "Suite: Cluster Deployment"

cluster_group="cluster-$(date +%s%N)"

if ! e2e_mode_is_clustered; then
  warn "Skipping cluster deployment checks outside clustered modes."
  _pass "skipped outside clustered modes"
  suite_summary "10_cluster_deployment"
  exit 0
fi

node_count=$(candidate_node_urls | sed '/^$/d' | wc -l | tr -d ' ')
if [[ "${node_count}" -eq 0 ]]; then
  die "cluster mode requires at least one runtime node URL"
fi

api_get_no_auth_at "${AENV_NODE_A_URL}" "/health"
assert_status "$HTTP_STATUS" "204" "runtime node A health endpoint is reachable"

if [[ -n "${AENV_NODE_B_URL:-}" ]]; then
  api_get_no_auth_at "${AENV_NODE_B_URL}" "/health"
  assert_status "$HTTP_STATUS" "204" "runtime node B health endpoint is reachable"
else
  _pass "runtime node B health check skipped because only one node endpoint is available"
fi

# Consecutive creates should alternate across runtime nodes with round-robin.
first_id=$(create_sandbox "$AENV_TEMPLATE_ID" 60 "{\"metadata\":{\"suite\":\"${cluster_group}\",\"team\":\"alpha\",\"slot\":\"first\"}}"); _sync_http
assert_status "$HTTP_STATUS" "201" "create sandbox #1 through gateway"
assert_not_empty "$first_id" "sandbox #1 ID is present"
track_sandbox "$first_id"

second_id=$(create_sandbox "$AENV_TEMPLATE_ID" 60 "{\"metadata\":{\"suite\":\"${cluster_group}\",\"team\":\"beta\",\"slot\":\"second\"}}"); _sync_http
assert_status "$HTTP_STATUS" "201" "create sandbox #2 through gateway"
assert_not_empty "$second_id" "sandbox #2 ID is present"
track_sandbox "$second_id"

wait_for_sandbox_state "$first_id" "running" 30
wait_for_sandbox_state "$second_id" "running" 30

if ! node_rest_is_served; then
  # 🔴 Lost coverage, stated as such. Everything below rests on asking a node
  # which sandboxes it holds, and the split took that route away without
  # putting the fact anywhere else: no user-facing endpoint names a sandbox's
  # owning node. Restoring round-robin-distribution coverage needs an API
  # affordance, not a test change.
  _pass "cross-node distribution and local exclusion $(node_rest_skip_reason)"
  suite_summary "10_cluster_deployment"
  exit 0
fi

first_owner_url=$(find_sandbox_node_url "$first_id")
second_owner_url=$(find_sandbox_node_url "$second_id")
assert_not_empty "$first_owner_url" "sandbox #1 resolves to a runtime node"
assert_not_empty "$second_owner_url" "sandbox #2 resolves to a runtime node"

if [[ -n "$first_owner_url" && -n "$second_owner_url" ]]; then
  first_owner_label=$(node_label_for_url "$first_owner_url")
  second_owner_label=$(node_label_for_url "$second_owner_url")

  if [[ "${node_count}" -lt 2 ]]; then
    _pass "cross-node distribution check skipped because only one runtime node endpoint is available"
  elif [[ "$first_owner_url" != "$second_owner_url" ]]; then
    _pass "gateway distributed sandboxes across ${first_owner_label} and ${second_owner_label}"
  else
    _fail \
      "gateway distributed consecutive sandboxes across both runtime nodes" \
      "different runtime nodes" \
      "${first_owner_label} and ${second_owner_label}"
  fi
fi

api_get "/sandboxes/${first_id}"
assert_status "$HTTP_STATUS" "200" "gateway routes sandbox #1 detail requests"

api_get "/sandboxes/${second_id}"
assert_status "$HTTP_STATUS" "200" "gateway routes sandbox #2 detail requests"

if [[ -n "$first_owner_url" ]]; then
  api_get_at "$first_owner_url" "/sandboxes"
  assert_status "$HTTP_STATUS" "200" "owner node #1 local /sandboxes returns 200"
  count=$(echo "$HTTP_BODY" | jq '[.[] | select(.sandboxID == "'"$first_id"'")] | length')
  assert_eq "$count" "1" "owner node #1 local /sandboxes includes sandbox #1"
  count=$(echo "$HTTP_BODY" | jq '[.[] | select(.sandboxID == "'"$second_id"'")] | length')
  if [[ "$first_owner_url" != "$second_owner_url" ]]; then
    assert_eq "$count" "0" "owner node #1 local /sandboxes excludes sandbox #2 from another node"
  else
    _pass "owner node #1 local exclusion skipped because both sandboxes landed on the same node"
  fi
fi

if [[ -n "$second_owner_url" ]]; then
  api_get_at "$second_owner_url" "/sandboxes"
  assert_status "$HTTP_STATUS" "200" "owner node #2 local /sandboxes returns 200"
  count=$(echo "$HTTP_BODY" | jq '[.[] | select(.sandboxID == "'"$second_id"'")] | length')
  assert_eq "$count" "1" "owner node #2 local /sandboxes includes sandbox #2"
  count=$(echo "$HTTP_BODY" | jq '[.[] | select(.sandboxID == "'"$first_id"'")] | length')
  if [[ "$first_owner_url" != "$second_owner_url" ]]; then
    assert_eq "$count" "0" "owner node #2 local /sandboxes excludes sandbox #1 from another node"
  else
    _pass "owner node #2 local exclusion skipped because both sandboxes landed on the same node"
  fi
fi

api_get "/sandboxes?metadata=suite%3D${cluster_group}"
assert_status "$HTTP_STATUS" "200" "gateway filtered /sandboxes returns 200 in cluster mode"
count=$(echo "$HTTP_BODY" | jq '[.[] | select(.sandboxID == "'"$first_id"'")] | length')
assert_eq "$count" "1" "gateway /sandboxes includes sandbox #1"
count=$(echo "$HTTP_BODY" | jq '[.[] | select(.sandboxID == "'"$second_id"'")] | length')
assert_eq "$count" "1" "gateway /sandboxes includes sandbox #2"

api_get "/sandboxes?metadata=suite%3D${cluster_group}%26team%3Dalpha"
assert_status "$HTTP_STATUS" "200" "gateway metadata-filtered /sandboxes returns 200 in cluster mode"
count=$(echo "$HTTP_BODY" | jq '[.[] | select(.sandboxID == "'"$first_id"'")] | length')
assert_eq "$count" "1" "gateway metadata filter includes sandbox #1"
count=$(echo "$HTTP_BODY" | jq '[.[] | select(.sandboxID == "'"$second_id"'")] | length')
assert_eq "$count" "0" "gateway metadata filter excludes sandbox #2"

api_get_with_headers "/v2/sandboxes?metadata=suite%3D${cluster_group}&limit=1"
assert_status "$HTTP_STATUS" "200" "gateway /v2/sandboxes pagination returns 200 in cluster mode"
page_count=$(echo "$HTTP_BODY" | jq 'length')
assert_eq "$page_count" "1" "cluster v2 first page has 1 item"
next_token=$(echo "$HTTP_HEADERS" | grep -i '^x-next-token:' | head -1 | sed 's/^[^:]*: *//;s/\r$//' || true)
assert_not_empty "$next_token" "cluster v2 first page has x-next-token"
first_page_ids=$(echo "$HTTP_BODY" | jq -r '.[].sandboxID')

api_get_with_headers "/v2/sandboxes?metadata=suite%3D${cluster_group}&limit=1&nextToken=${next_token}"
assert_status "$HTTP_STATUS" "200" "gateway /v2/sandboxes second page returns 200 in cluster mode"
page_count=$(echo "$HTTP_BODY" | jq 'length')
assert_eq "$page_count" "1" "cluster v2 second page has 1 item"
second_page_ids=$(echo "$HTTP_BODY" | jq -r '.[].sandboxID')

seen_two_pages=$(printf '%s\n%s\n' "$first_page_ids" "$second_page_ids" | sed '/^$/d' | sort -u)
count=$(printf '%s\n' "$seen_two_pages" | sed '/^$/d' | wc -l | tr -d ' ')
assert_eq "$count" "2" "cluster v2 pagination exposes both sandboxes across two pages"

suite_summary "10_cluster_deployment"
