#!/usr/bin/env bash
set -euo pipefail

SUITE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "${SUITE_DIR}/../lib/helpers.sh"
init_suite "07_pagination"

log "Suite: Pagination (v2 endpoint)"

pagination_group="pagination-$(date +%s%N)"

# -- Create 3 sandboxes --
for i in 1 2 3; do
  sid=$(create_sandbox "$AENV_TEMPLATE_ID" 60 "{\"metadata\":{\"suite\":\"${pagination_group}\",\"ordinal\":\"${i}\"}}"); _sync_http
  assert_status "$HTTP_STATUS" "201" "create sandbox #${i}"
  assert_not_empty "$sid" "sandbox #${i} ID"
  track_sandbox "$sid"
done

# Wait for all to be running
for sid in "${_TRACKED_SANDBOX_IDS[@]}"; do
  wait_for_sandbox_state "$sid" "running" 30
done

if e2e_mode_is_clustered && ! node_rest_is_served; then
  # Informational either way — the pagination assertions below are what this
  # suite is for, and they run against the gateway regardless.
  _skip "pagination fixture node spread $(node_rest_skip_reason)"
elif e2e_mode_is_clustered; then
  owner_count=$(for sid in "${_TRACKED_SANDBOX_IDS[@]}"; do find_sandbox_node_url "$sid"; done | sed '/^$/d' | sort -u | wc -l | tr -d ' ')
  if [[ "${owner_count}" -ge 2 ]]; then
    _pass "pagination fixture spans multiple runtime nodes"
  else
    _pass "pagination fixture stayed on a single runtime node; gateway pagination is still exercised"
  fi
fi

# -- First page: limit=1 --
# v2 returns a JSON array in body, x-next-token in response header.
api_get_with_headers "/v2/sandboxes?metadata=suite%3D${pagination_group}&limit=1"
assert_status "$HTTP_STATUS" "200" "GET /v2/sandboxes?metadata=suite%3D${pagination_group}&limit=1"

page_count=$(echo "$HTTP_BODY" | jq 'length')
assert_eq "$page_count" "1" "first page has 1 item"

next_token=$(echo "$HTTP_HEADERS" | grep -i '^x-next-token:' | head -1 | sed 's/^[^:]*: *//;s/\r$//' || true)
assert_not_empty "$next_token" "x-next-token header present on first page"

seen_ids=$(echo "$HTTP_BODY" | jq -r '.[].sandboxID')

# -- Follow pages until exhausted --
total_seen=$((page_count))
max_pages=10
for ((p = 0; p < max_pages; p++)); do
  [[ -z "$next_token" ]] && break
  api_get_with_headers "/v2/sandboxes?metadata=suite%3D${pagination_group}&limit=1&nextToken=${next_token}"
  assert_status "$HTTP_STATUS" "200" "page $((p + 2)) returns 200"
  count=$(echo "$HTTP_BODY" | jq 'length')
  total_seen=$((total_seen + count))
  page_ids=$(echo "$HTTP_BODY" | jq -r '.[].sandboxID')
  if [[ -n "$page_ids" ]]; then
    seen_ids+=$'\n'"${page_ids}"
  fi
  next_token=$(echo "$HTTP_HEADERS" | grep -i '^x-next-token:' | head -1 | sed 's/^[^:]*: *//;s/\r$//' || true)
done

if [[ "$total_seen" -ge 3 ]]; then
  _pass "paginated through >= 3 sandboxes in the filtered suite group (saw ${total_seen})"
else
  _fail "expected >= 3 sandboxes via pagination" ">= 3" "$total_seen"
fi

unique_seen=$(printf '%s\n' "$seen_ids" | sed '/^$/d' | sort -u)
count=$(printf '%s\n' "$unique_seen" | sed '/^$/d' | wc -l | tr -d ' ')
assert_eq "$count" "3" "pagination returned 3 unique sandbox IDs for the filtered suite group"

for sid in "${_TRACKED_SANDBOX_IDS[@]}"; do
  if printf '%s\n' "$unique_seen" | grep -qx "$sid"; then
    _pass "pagination includes tracked sandbox ${sid}"
  else
    _fail "pagination includes tracked sandbox ${sid}" "$sid" "(missing)"
  fi
done

suite_summary "07_pagination"
