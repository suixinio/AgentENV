#!/usr/bin/env bash
set -euo pipefail

SUITE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "${SUITE_DIR}/../lib/helpers.sh"
init_suite "06_proxy"

log "Suite: Proxy Routing"

# -- Create sandbox --
sandbox_id=$(create_sandbox); _sync_http
assert_status "$HTTP_STATUS" "201" "create sandbox for proxy test"
assert_not_empty "$sandbox_id" "sandboxID present"
track_sandbox "$sandbox_id"
wait_for_sandbox_state "$sandbox_id" "running" 30

# -- Proxy request using AgentENV headers --
# The envd health endpoint runs on port 49983 inside the sandbox.
_curl_do -s --max-time 5 \
  -H "X-API-Key: ${AENV_API_KEY}" \
  -H "x-agentenv-sandbox-id: ${sandbox_id}" \
  -H "x-agentenv-target-port: 49983" \
  "${AENV_PROXY_URL}/health"
log "Proxy (agentenv headers) returned HTTP ${HTTP_STATUS}"
assert_status "$HTTP_STATUS" "204" "proxy with agentenv headers"

# -- Proxy request using E2B compat headers --
_curl_do -s --max-time 5 \
  -H "X-API-Key: ${AENV_API_KEY}" \
  -H "e2b-sandbox-id: ${sandbox_id}" \
  -H "e2b-sandbox-port: 49983" \
  "${AENV_PROXY_URL}/health"
log "Proxy (e2b headers) returned HTTP ${HTTP_STATUS}"
assert_status "$HTTP_STATUS" "204" "proxy with e2b headers"

# -- Proxy request without sandbox header returns 400 --
# Address a runtime node's own /proxy entrypoint, not ${AENV_URL}.
#
# 🔴 The gateway cannot carry this request to a node, and no longer pretends
# to. Routing a data-plane request needs a sandbox to look up; with no sandbox
# header there is nothing to look up, so the gateway classifies /proxy/health
# as an unrecognised REST path and forwards it to the api half -- which since
# 2338993 mounts no /proxy at all, deliberately. Through the gateway this path
# therefore only ever reaches aenv-api's generated router 404, which asserts
# nothing about the proxy. Header validation lives in the node's proxy, so
# that is what this assertion has to talk to.
#
# The node half's *user-facing REST* is gone in a split deployment (see
# node_rest_is_served), but its *data plane* is exactly what it still serves,
# so this needs no skip gate: in clustered modes the first runtime node
# endpoint answers, and in single-node mode ${AENV_URL} is itself the node.
# (`| head -n1` also keeps candidate_node_urls' empty-case exit status out of
# `set -e`, the same way node_rest_is_served calls it.)
proxy_entrypoint_url="$(candidate_node_urls | head -n 1)"
[[ -n "$proxy_entrypoint_url" ]] || proxy_entrypoint_url="${AENV_URL}"
_curl_do -s --max-time 5 \
  -H "X-API-Key: ${AENV_API_KEY}" \
  "${proxy_entrypoint_url}/proxy/health"
log "Proxy (no sandbox header) at ${proxy_entrypoint_url} returned HTTP ${HTTP_STATUS}: ${HTTP_BODY}"
assert_status "$HTTP_STATUS" "400" "proxy without sandbox header"

# -- Paused sandbox with auto-resume disabled returns 410 --
paused_no_resume_id=$(create_sandbox "$AENV_TEMPLATE_ID" 60 '{"autoResume":{"enabled":false}}'); _sync_http
assert_status "$HTTP_STATUS" "201" "create paused sandbox with auto-resume disabled"
assert_not_empty "$paused_no_resume_id" "paused sandbox ID present (auto-resume disabled)"
track_sandbox "$paused_no_resume_id"
wait_for_sandbox_state "$paused_no_resume_id" "running" 30

api_post "/sandboxes/${paused_no_resume_id}/pause"
assert_status "$HTTP_STATUS" "204" "pause sandbox with auto-resume disabled"
if wait_for_sandbox_state "$paused_no_resume_id" "paused" 20; then
  _pass "sandbox paused (auto-resume disabled)"
else
  _fail "sandbox paused (auto-resume disabled)" "paused" "timeout"
fi

# Outlast one heartbeat (5 s): the probe must find the sandbox listed as paused in the node's roster, not pause and probe within the same second.
sleep 7
_curl_do_with_headers -s --max-time 10 \
  -H "X-API-Key: ${AENV_API_KEY}" \
  -H "x-agentenv-sandbox-id: ${paused_no_resume_id}" \
  -H "x-agentenv-target-port: 49983" \
  "${AENV_PROXY_URL}/health"
log "Proxy (paused + auto-resume disabled) returned HTTP ${HTTP_STATUS}"
assert_status "$HTTP_STATUS" "410" "paused sandbox without auto-resume returns 410"
# The gateway synthesizes this refusal and stamps CORS on it; a node's raw 410 carries no such header.
assert_contains "$(printf '%s' "$HTTP_HEADERS" | tr -d '\r' | tr '[:upper:]' '[:lower:]')" \
  "access-control-allow-origin: *" "the 410 is the gateway's own answer (CORS header present)"

# -- Paused sandbox with auto-resume enabled resumes and forwards --
paused_auto_resume_id=$(create_sandbox "$AENV_TEMPLATE_ID" 60 '{"autoResume":{"enabled":true}}'); _sync_http
assert_status "$HTTP_STATUS" "201" "create paused sandbox with auto-resume enabled"
assert_not_empty "$paused_auto_resume_id" "paused sandbox ID present (auto-resume enabled)"
track_sandbox "$paused_auto_resume_id"
wait_for_sandbox_state "$paused_auto_resume_id" "running" 30

api_post "/sandboxes/${paused_auto_resume_id}/pause"
assert_status "$HTTP_STATUS" "204" "pause sandbox with auto-resume enabled"
if wait_for_sandbox_state "$paused_auto_resume_id" "paused" 20; then
  _pass "sandbox paused (auto-resume enabled)"
else
  _fail "sandbox paused (auto-resume enabled)" "paused" "timeout"
fi

# Outlast one heartbeat (5 s) here too: the wake-up must work once the roster lists the sandbox as paused.
sleep 7
# Auto-resume may wait up to 60s in non-test runtime. Keep client timeout above that.
_curl_do -s --max-time 75 \
  -H "X-API-Key: ${AENV_API_KEY}" \
  -H "e2b-sandbox-id: ${paused_auto_resume_id}" \
  -H "e2b-sandbox-port: 49983" \
  "${AENV_PROXY_URL}/health"
log "Proxy (paused + auto-resume enabled) returned HTTP ${HTTP_STATUS}"
assert_status "$HTTP_STATUS" "204" "paused sandbox auto-resumes on proxy request"

if wait_for_sandbox_state "$paused_auto_resume_id" "running" 30; then
  _pass "sandbox is running after proxy-triggered auto-resume"
else
  _fail "sandbox is running after proxy-triggered auto-resume" "running" "timeout"
fi

suite_summary "06_proxy"
