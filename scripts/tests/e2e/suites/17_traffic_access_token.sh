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

# The default template has nothing on 8080, and "not 403" alone would also
# accept `000` — a request that never left this machine proves nothing about
# the proxy. Put a listener there so the accepted request has a status of its
# own to carry back.
listener_up=0
if python3 -c 'import e2b' >/dev/null 2>&1; then
  start_listener='setsid nohup python3 -m http.server 8080 --bind 0.0.0.0 </dev/null >/tmp/http8080.log 2>&1 &
sleep 2
curl -sS -o /dev/null -w "%{http_code}" --max-time 5 http://127.0.0.1:8080/'
  local_probe=$(E2B_API_URL="${AENV_URL}" E2B_SANDBOX_URL="${AENV_PROXY_URL}" \
    E2B_API_KEY="${AENV_API_KEY}" \
    python3 "${SUITE_DIR}/../egress_credentials_e2e.py" run "$locked_id" "$start_listener" \
    2>/dev/null | tail -n 1 || true)
  [[ "$local_probe" == 2* ]] && listener_up=1
fi
[[ "$listener_up" -eq 1 ]] || warn "no listener on 8080 in the sandbox; the accepted request has no upstream"

for header in e2b-traffic-access-token x-agentenv-traffic-access-token; do
  _curl_do -s --max-time 10 \
    -H "X-API-Key: ${AENV_API_KEY}" \
    -H "x-agentenv-sandbox-id: ${locked_id}" \
    -H "x-agentenv-target-port: 8080" \
    -H "${header}: ${token}" \
    "${AENV_PROXY_URL}/"
  if [[ "$listener_up" -eq 1 ]]; then
    if [[ "$HTTP_STATUS" == 2* ]]; then
      _pass "the token in ${header} reaches the listener (HTTP ${HTTP_STATUS})"
    else
      _fail "the token in ${header} reaches the listener" "2xx" "HTTP ${HTTP_STATUS}"
    fi
  elif [[ "$HTTP_STATUS" != "403" && "$HTTP_STATUS" != "000" ]]; then
    # Carried and failed at the upstream, which is still the proxy's answer and
    # not its refusal.
    _pass "the token in ${header} passes the proxy (HTTP ${HTTP_STATUS})"
  else
    _fail "the token in ${header} passes the proxy" "not 403 and not a connection failure" \
      "HTTP ${HTTP_STATUS}"
  fi
done

suite_summary "17_traffic_access_token"
