#!/usr/bin/env bash
set -euo pipefail

SUITE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "${SUITE_DIR}/../lib/helpers.sh"
init_suite "12_template_shared_repository"

log "Suite: Shared POSIX Template Repository"

if ! e2e_mode_is_clustered; then
  warn "Skipping shared repository checks outside clustered modes."
  _pass "skipped outside clustered modes"
  suite_summary "12_template_shared_repository"
  exit 0
fi

if [[ -z "${AENV_NODE_A_URL:-}" || -z "${AENV_NODE_B_URL:-}" ]]; then
  warn "Skipping shared repository checks because fewer than two AgentENV node endpoints are available."
  _pass "skipped because clustered runtime exposed fewer than two AgentENV nodes"
  suite_summary "12_template_shared_repository"
  exit 0
fi

# 🔴 This suite's whole premise is two independent REST surfaces: build a
# template through node A, read it back through node B, and thereby prove the
# repository underneath is shared. The split leaves one REST surface — the api
# half — in front of both nodes, so there is no longer a second endpoint to
# read back from, and the property is no longer observable from outside.
if ! node_rest_is_served; then
  warn "Skipping shared repository checks: nodes no longer serve user-facing REST."
  _pass "cross-node template flow $(node_rest_skip_reason)"
  suite_summary "12_template_shared_repository"
  exit 0
fi

assert_cross_node_template_flow() {
  local builder_url="$1"
  local builder_label="$2"
  local consumer_url="$3"
  local consumer_label="$4"
  local alias="$5"

  create_template_at "$builder_url" "$alias"
  assert_status "$HTTP_STATUS" "202" "create and start template ${alias} on ${builder_label}"

  local template_id build_id
  template_id=$(echo "$HTTP_BODY" | jq -r '.templateID // empty')
  build_id=$(echo "$HTTP_BODY" | jq -r '.buildID // empty')
  assert_not_empty "$template_id" "templateID present for ${alias}"
  track_template "$template_id"

  if wait_for_template_build_at "$builder_url" "$template_id" 120 "$build_id"; then
    _pass "template ${alias} built on ${builder_label}"
  else
    _fail "template ${alias} built on ${builder_label}" "ready" "failed or timed out"
    return 1
  fi

  api_get_at "$consumer_url" "/v2/templates"
  assert_status "$HTTP_STATUS" "200" "list templates on ${consumer_label}"
  assert_contains "$HTTP_BODY" "$template_id" "${consumer_label} sees template ${alias}"

  api_get_at "$consumer_url" "/templates/${template_id}"
  assert_status "$HTTP_STATUS" "200" "get template ${alias} on ${consumer_label}"
  assert_contains "$HTTP_BODY" "$alias" "${consumer_label} template payload includes alias ${alias}"

  local sandbox_id
  sandbox_id=$(create_sandbox_at "$consumer_url" "$alias" 60); _sync_http
  assert_status "$HTTP_STATUS" "201" "create sandbox from ${alias} on ${consumer_label}"
  assert_not_empty "$sandbox_id" "sandboxID present for ${alias} on ${consumer_label}"
  track_sandbox "$sandbox_id"
  wait_for_sandbox_state_at "$consumer_url" "$sandbox_id" "running" 30

  delete_sandbox_at "$consumer_url" "$sandbox_id"
  assert_status "$HTTP_STATUS" "204" "delete sandbox from ${alias} on ${consumer_label}"

  api_delete_at "$consumer_url" "/templates/${template_id}"
  assert_status "$HTTP_STATUS" "204" "delete template ${alias} on ${consumer_label}"

  api_get_at "$builder_url" "/v2/templates"
  assert_status "$HTTP_STATUS" "200" "list templates on ${builder_label} after deleting ${alias}"
  if echo "$HTTP_BODY" | grep -Fq "$template_id"; then
    _fail "${builder_label} no longer lists deleted template ${alias}" "template removed" "$template_id still listed"
    return 1
  fi
  _pass "${builder_label} no longer lists deleted template ${alias}"
}

assert_cross_node_template_flow \
  "${AENV_NODE_A_URL}" \
  "${AENV_NODE_A_LABEL:-agentenv-a}" \
  "${AENV_NODE_B_URL}" \
  "${AENV_NODE_B_LABEL:-agentenv-b}" \
  "shared-posix-a-$(date +%s%N)"

assert_cross_node_template_flow \
  "${AENV_NODE_B_URL}" \
  "${AENV_NODE_B_LABEL:-agentenv-b}" \
  "${AENV_NODE_A_URL}" \
  "${AENV_NODE_A_LABEL:-agentenv-a}" \
  "shared-posix-b-$(date +%s%N)"

suite_summary "12_template_shared_repository"
