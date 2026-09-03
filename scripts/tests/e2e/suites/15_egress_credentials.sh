#!/usr/bin/env bash
set -euo pipefail

SUITE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "${SUITE_DIR}/../lib/helpers.sh"
init_suite "15_egress_credentials"

log "Suite: Egress Credentials (network.rules + /secrets)"

# The upstream a rule points at. It must echo request headers back as JSON
# under `.headers`, the way httpbin's /anything does; override for a private
# mirror. The cluster's nodes and the broker need egress to it.
EGRESS_UPSTREAM="${E2E_EGRESS_UPSTREAM:-httpbin.org}"
UNDECLARED_UPSTREAM="${E2E_EGRESS_UNDECLARED_UPSTREAM:-github.com}"
EGRESS_PY="${SUITE_DIR}/../egress_credentials_e2e.py"

run_in_sandbox() {
  # Prints the command's stdout; exit status is the command's.
  local sandbox_id="$1" cmd="$2"
  E2B_API_URL="${AENV_URL}" E2B_SANDBOX_URL="${AENV_PROXY_URL}" E2B_API_KEY="${AENV_API_KEY}" \
    python3 "${EGRESS_PY}" run "${sandbox_id}" "${cmd}"
}

# -- Preconditions -------------------------------------------------------------
if ! python3 -c 'import e2b' >/dev/null 2>&1; then
  warn "python e2b SDK not importable; the suite cannot run commands inside sandboxes"
  _pass "skipped: python e2b SDK not installed"
  suite_summary
  exit 0
fi

secret_name="e2e-egress-$(date +%s)-$RANDOM"
secret_value="sk-e2e-$(head -c 12 /dev/urandom | base64 | tr -dc 'A-Za-z0-9' | head -c 16)"
api_post "/secrets" "$(jq -nc --arg n "$secret_name" --arg v "$secret_value" '{name: $n, value: $v}')"
if [[ "$HTTP_STATUS" == "503" ]]; then
  warn "POST /secrets answered 503: no secrets store on this deployment"
  _pass "skipped: no secrets store configured"
  suite_summary
  exit 0
fi
assert_status "$HTTP_STATUS" "201" "create secret"
secret_id=$(echo "$HTTP_BODY" | jq -r '.secretID // empty')
assert_not_empty "$secret_id" "secretID present"
assert_eq "$(echo "$HTTP_BODY" | jq -r '.currentVersion')" "1" "first version is 1"
if echo "$HTTP_BODY" | grep -qF "$secret_value"; then
  _fail "secret value in create response" "absent" "present"
else
  _pass "create response carries no value"
fi

rules_json=$(jq -nc --arg d "$EGRESS_UPSTREAM" --arg n "$secret_name" \
  '{network: {rules: {($d): [{transform: {headers: {Authorization: ("Bearer ${aenv.secrets." + $n + "}")}}}]}}}')

# -- Placement: a node with a broker ------------------------------------------
sandbox_id=$(create_sandbox "$AENV_TEMPLATE_ID" 120 "$rules_json"); _sync_http
if [[ "$HTTP_STATUS" == "503" ]]; then
  warn "no node reports a usable egress broker; the deployment has rules support off"
  _pass "skipped: no egress broker on any node"
  api_delete "/secrets/${secret_id}"
  suite_summary
  exit 0
fi
assert_status "$HTTP_STATUS" "201" "create sandbox with network.rules"
track_sandbox "$sandbox_id"
wait_for_sandbox_state "$sandbox_id" "running" 60

api_get "/sandboxes/${sandbox_id}"
assert_status "$HTTP_STATUS" "200" "get sandbox with rules"
assert_eq "$(echo "$HTTP_BODY" | jq -r --arg d "$EGRESS_UPSTREAM" '.network.rules[$d] | length')" "1" "GET /sandboxes/{id} reports the rule"
if echo "$HTTP_BODY" | grep -qF "$secret_value"; then
  _fail "secret value in sandbox detail" "absent" "present"
else
  _pass "sandbox detail carries no value"
fi

# -- 1. A declared domain gets the header ---------------------------------------
echo_json=$(run_in_sandbox "$sandbox_id" "curl -sS --max-time 20 https://${EGRESS_UPSTREAM}/anything" || true)
auth_seen=$(echo "$echo_json" | jq -r '.headers.Authorization // .headers.authorization // empty' 2>/dev/null || true)
assert_eq "$auth_seen" "Bearer ${secret_value}" "upstream saw the injected Authorization header"

guest_env=$(run_in_sandbox "$sandbox_id" "env; cat /proc/1/environ 2>/dev/null | tr '\\0' '\\n'" || true)
if echo "$guest_env" | grep -qF "$secret_value"; then
  _fail "secret value in guest environment" "absent" "present"
else
  _pass "guest environment carries no value"
fi

ca_lines=$(run_in_sandbox "$sandbox_id" "grep -c 'BEGIN CERTIFICATE' /etc/ssl/certs/ca-certificates.crt" || echo 0)
if [[ "${ca_lines:-0}" -gt 0 ]]; then
  _pass "guest trust store carries certificates (CA delivered)"
else
  _fail "guest trust store" "at least one certificate" "${ca_lines}"
fi

# -- 2. Undeclared domains pass through unchanged --------------------------------
undeclared_code=$(run_in_sandbox "$sandbox_id" "curl -sS -o /dev/null -w '%{http_code}' --max-time 20 https://${UNDECLARED_UPSTREAM}/" || echo 000)
if [[ "$undeclared_code" =~ ^(200|301|302)$ ]]; then
  _pass "undeclared HTTPS domain passes through (HTTP ${undeclared_code})"
else
  _fail "undeclared HTTPS domain passthrough" "2xx/3xx" "$undeclared_code"
fi
git_probe=$(run_in_sandbox "$sandbox_id" "command -v git >/dev/null && (git ls-remote --exit-code https://${UNDECLARED_UPSTREAM}/git/git.git HEAD >/dev/null 2>&1 && echo ok || echo fail) || echo nogit" || echo fail)
case "$git_probe" in
  ok) _pass "git ls-remote to an undeclared domain works" ;;
  nogit) warn "git not installed in the template; skipping the git probe"; _pass "skipped: git probe" ;;
  *) _fail "git ls-remote to an undeclared domain" "ok" "$git_probe" ;;
esac

# -- 6. HTTP/3 falls back to TCP -------------------------------------------------
h3=$(run_in_sandbox "$sandbox_id" "curl --http3 -sS -o /dev/null -w '%{http_version}' --max-time 20 https://${EGRESS_UPSTREAM}/anything 2>&1 || echo unsupported" || echo unsupported)
case "$h3" in
  1.1|2) _pass "curl --http3 fell back to TCP (HTTP/${h3})" ;;
  3) _fail "HTTP/3 must not bypass the intercept" "TCP fallback" "HTTP/3" ;;
  *) warn "guest curl has no HTTP/3 support (${h3}); skipping the fallback probe"; _pass "skipped: http3 probe" ;;
esac

# -- 4. A forked child has its own grant ---------------------------------------
api_post "/sandboxes/${sandbox_id}/fork" '{"count": 1}'
if [[ "$HTTP_STATUS" == "200" || "$HTTP_STATUS" == "201" ]]; then
  child_id=$(echo "$HTTP_BODY" | jq -r '.. | objects | .sandboxID? // empty' | head -n 1)
  if [[ -n "$child_id" ]]; then
    track_sandbox "$child_id"
    wait_for_sandbox_state "$child_id" "running" 60 || true
    child_auth=$(run_in_sandbox "$child_id" "curl -sS --max-time 20 https://${EGRESS_UPSTREAM}/anything" | jq -r '.headers.Authorization // .headers.authorization // empty' 2>/dev/null || true)
    assert_eq "$child_auth" "Bearer ${secret_value}" "fork child is granted the parent's secrets"
  else
    _fail "fork response" "a child sandboxID" "$HTTP_BODY"
  fi
else
  warn "fork answered HTTP ${HTTP_STATUS}; skipping the child grant check"
  _pass "skipped: fork unavailable"
fi

# -- 3. allow_internet_access=false is denied at the broker too -------------------
closed_json=$(echo "$rules_json" | jq -c '. + {allow_internet_access: false}')
closed_id=$(create_sandbox "$AENV_TEMPLATE_ID" 120 "$closed_json"); _sync_http
assert_status "$HTTP_STATUS" "201" "create closed sandbox with rules"
track_sandbox "$closed_id"
wait_for_sandbox_state "$closed_id" "running" 60
closed_code=$(run_in_sandbox "$closed_id" "curl -sS -o /dev/null -w '%{http_code}' --max-time 20 https://${EGRESS_UPSTREAM}/anything" || echo 000)
if [[ "$closed_code" == "403" || "$closed_code" == "000" ]]; then
  _pass "closed sandbox cannot reach the rule domain through the broker (HTTP ${closed_code})"
else
  _fail "closed sandbox through the broker" "403 or connection failure" "$closed_code"
fi

# -- 5 and 7. Broker restart and outage (Kubernetes only) --------------------------
if [[ "${E2E_MODE:-}" == "k8s" ]] && command -v kubectl >/dev/null 2>&1 \
   && kubectl -n "${K8S_NAMESPACE:-agentenv-system}" get deploy/aenv-egress >/dev/null 2>&1; then
  ns="${K8S_NAMESPACE:-agentenv-system}"
  kubectl -n "$ns" rollout restart deploy/aenv-egress >/dev/null
  kubectl -n "$ns" rollout status deploy/aenv-egress --timeout=180s >/dev/null
  state=$(get_sandbox_state "$sandbox_id")
  assert_eq "$state" "running" "sandbox survives a broker rollout"
  after_roll=$(run_in_sandbox "$sandbox_id" "curl -sS --max-time 20 https://${EGRESS_UPSTREAM}/anything" | jq -r '.headers.Authorization // .headers.authorization // empty' 2>/dev/null || true)
  assert_eq "$after_roll" "Bearer ${secret_value}" "brokered requests resume after the rollout"

  replicas=$(kubectl -n "$ns" get deploy/aenv-egress -o jsonpath='{.spec.replicas}')
  kubectl -n "$ns" scale deploy/aenv-egress --replicas=0 >/dev/null
  kubectl -n "$ns" rollout status deploy/aenv-egress --timeout=120s >/dev/null || true
  sleep 12
  outage_code=$(run_in_sandbox "$sandbox_id" "curl -sS -o /dev/null -w '%{http_code}' --max-time 10 https://${EGRESS_UPSTREAM}/anything" || echo 000)
  if [[ "$outage_code" == "000" ]]; then
    _pass "guest connection fails fast while the broker is down"
  else
    _fail "broker outage visible to the guest" "connection failure" "HTTP ${outage_code}"
  fi
  kubectl -n "$ns" scale deploy/aenv-egress --replicas="${replicas:-2}" >/dev/null
  kubectl -n "$ns" rollout status deploy/aenv-egress --timeout=180s >/dev/null
  sleep 12
  recovered=$(run_in_sandbox "$sandbox_id" "curl -sS --max-time 20 https://${EGRESS_UPSTREAM}/anything" | jq -r '.headers.Authorization // .headers.authorization // empty' 2>/dev/null || true)
  assert_eq "$recovered" "Bearer ${secret_value}" "brokered requests recover after the broker returns"
else
  warn "not a Kubernetes run with kubectl; skipping broker rollout and outage checks"
  _pass "skipped: broker rollout/outage checks"
fi

# -- Cleanup ----------------------------------------------------------------------
api_delete "/secrets/${secret_id}"
assert_status "$HTTP_STATUS" "204" "delete secret"
api_get "/secrets/${secret_id}"
assert_status "$HTTP_STATUS" "404" "deleted secret is gone"

suite_summary
