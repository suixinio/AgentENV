#!/usr/bin/env bash
set -euo pipefail

SUITE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "${SUITE_DIR}/../lib/helpers.sh"
init_suite "17_traffic_access_token"

log "Suite: Traffic Access Token (allowPublicTraffic=false)"

# envd's own port. It is the one port the token does not bound: envd has its
# own credential, and a second one for the same door would be two ledgers.
ENVD_PORT=49983

# -- 1. A sandbox nobody locked is reachable without a token ---------------------
open_id=$(create_sandbox); _sync_http
assert_status "$HTTP_STATUS" "201" "create an open sandbox"
assert_not_empty "$open_id" "sandboxID present"
track_sandbox "$open_id"
assert_eq "$(echo "$HTTP_BODY" | jq -r '.trafficAccessToken // "null"')" "null" \
  "an open sandbox is minted no token"
wait_for_sandbox_state "$open_id" "running" 30

_curl_do -s --max-time 10 \
  -H "X-API-Key: ${AENV_API_KEY}" \
  -H "x-agentenv-sandbox-id: ${open_id}" \
  -H "x-agentenv-target-port: ${ENVD_PORT}" \
  "${AENV_PROXY_URL}/health"
assert_status "$HTTP_STATUS" "204" "an open sandbox answers without a token"

# -- 2. Locking without secure=true is refused ------------------------------------
api_post "/sandboxes" "$(jq -nc --arg t "${AENV_TEMPLATE_ID}" \
  '{templateID: $t, timeout: 60, network: {allowPublicTraffic: false}}')"
assert_status "$HTTP_STATUS" "400" "allowPublicTraffic=false without secure=true is refused"

# -- 3. A locked sandbox mints a token, once ---------------------------------------
locked_id=$(create_sandbox "${AENV_TEMPLATE_ID}" 180 \
  '{"secure": true, "network": {"allowPublicTraffic": false}}'); _sync_http
if [[ "$HTTP_STATUS" != "201" ]]; then
  warn "creating a locked sandbox answered ${HTTP_STATUS}: ${HTTP_BODY}"
  _skip "skipped: this deployment did not create a locked sandbox"
  suite_summary "17_traffic_access_token"
  exit 0
fi
token=$(echo "$HTTP_BODY" | jq -r '.trafficAccessToken // empty')
assert_not_empty "$token" "the create response carries the token it minted"
track_sandbox "$locked_id"
wait_for_sandbox_state "$locked_id" "running" 30

# The listing must not carry it: the create is the only place it appears.
api_get "/sandboxes/${locked_id}"
if echo "$HTTP_BODY" | grep -qF "$token"; then
  _fail "the token in a detail response" "absent" "present"
else
  _pass "no detail response carries the token"
fi

# -- 4. envd's port is not bound by this token ------------------------------------
_curl_do -s --max-time 10 \
  -H "X-API-Key: ${AENV_API_KEY}" \
  -H "x-agentenv-sandbox-id: ${locked_id}" \
  -H "x-agentenv-target-port: ${ENVD_PORT}" \
  "${AENV_PROXY_URL}/health"
assert_status "$HTTP_STATUS" "204" "envd's own port answers a locked sandbox without the token"

# -- 5. envd's control paths are refused whatever the port ------------------------
for internal in /init /freeze /upgrade; do
  _curl_do -s --max-time 10 \
    -H "X-API-Key: ${AENV_API_KEY}" \
    -H "x-agentenv-sandbox-id: ${locked_id}" \
    -H "x-agentenv-target-port: ${ENVD_PORT}" \
    "${AENV_PROXY_URL}${internal}"
  assert_status "$HTTP_STATUS" "403" "envd control path ${internal} is not proxied"
done

# -- 6. Any other port needs the token --------------------------------------------
#
# Port 8080 has nothing listening in the default template, so a request that
# passes the token check fails at the upstream instead — which is exactly what
# separates "refused here" (403) from "carried and failed there" (502/504).
_curl_do -s --max-time 10 \
  -H "X-API-Key: ${AENV_API_KEY}" \
  -H "x-agentenv-sandbox-id: ${locked_id}" \
  -H "x-agentenv-target-port: 8080" \
  "${AENV_PROXY_URL}/"
assert_status "$HTTP_STATUS" "403" "a locked port refuses a request with no token"

_curl_do -s --max-time 10 \
  -H "X-API-Key: ${AENV_API_KEY}" \
  -H "x-agentenv-sandbox-id: ${locked_id}" \
  -H "x-agentenv-target-port: 8080" \
  -H "e2b-traffic-access-token: not-the-token" \
  "${AENV_PROXY_URL}/"
assert_status "$HTTP_STATUS" "403" "a locked port refuses a request with the wrong token"

for header in e2b-traffic-access-token x-agentenv-traffic-access-token; do
  _curl_do -s --max-time 10 \
    -H "X-API-Key: ${AENV_API_KEY}" \
    -H "x-agentenv-sandbox-id: ${locked_id}" \
    -H "x-agentenv-target-port: 8080" \
    -H "${header}: ${token}" \
    "${AENV_PROXY_URL}/"
  if [[ "$HTTP_STATUS" == "403" ]]; then
    _fail "the token in ${header} is accepted" "not 403" "403"
  else
    _pass "the token in ${header} passes the proxy (HTTP ${HTTP_STATUS})"
  fi
done

suite_summary "17_traffic_access_token"
