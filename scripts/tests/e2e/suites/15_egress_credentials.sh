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

# The HTTP status of one guest request, with a connection failure reported as
# `000`. Two shapes have to read the same: curl's own `-w '%{http_code}'`
# prints `000` when it never connected, and the e2b SDK's blocking
# `commands.run` raises on a non-zero exit, so today run_in_sandbox prints
# nothing at all and only its exit status carries the failure. Take the first
# line and default it rather than depending on which one arrives.
guest_http_code() {
  local sandbox_id="$1" url="$2" code
  code=$(run_in_sandbox "$sandbox_id" \
    "curl -sS -o /dev/null -w '%{http_code}' --max-time ${3:-20} ${url}" | head -n 1 || true)
  printf '%s' "${code:-000}"
}

# -- Preconditions -------------------------------------------------------------
if ! python3 -c 'import e2b' >/dev/null 2>&1; then
  warn "python e2b SDK not importable; the suite cannot run commands inside sandboxes"
  _skip "skipped: python e2b SDK not installed"
  suite_summary
  exit 0
fi

secret_name="e2e-egress-$(date +%s)-$RANDOM"
secret_value="sk-e2e-$(head -c 12 /dev/urandom | base64 | tr -dc 'A-Za-z0-9' | head -c 16)"
api_post "/secrets" "$(jq -nc --arg n "$secret_name" --arg v "$secret_value" '{name: $n, value: $v}')"
if [[ "$HTTP_STATUS" == "503" ]]; then
  warn "POST /secrets answered 503: no secrets store on this deployment"
  _skip "skipped: no secrets store configured"
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
  _skip "skipped: no egress broker on any node"
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
  nogit) warn "git not installed in the template; skipping the git probe"; _skip "git probe" ;;
  *) _fail "git ls-remote to an undeclared domain" "ok" "$git_probe" ;;
esac

# -- 6. HTTP/3 falls back to TCP -------------------------------------------------
h3=$(run_in_sandbox "$sandbox_id" "curl --http3 -sS -o /dev/null -w '%{http_version}' --max-time 20 https://${EGRESS_UPSTREAM}/anything 2>&1 || echo unsupported" || echo unsupported)
case "$h3" in
  1.1|2) _pass "curl --http3 fell back to TCP (HTTP/${h3})" ;;
  3) _fail "HTTP/3 must not bypass the intercept" "TCP fallback" "HTTP/3" ;;
  *) warn "guest curl has no HTTP/3 support (${h3}); skipping the fallback probe"; _skip "http3 probe" ;;
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
  _skip "skipped: fork unavailable"
fi

# -- 3. allow_internet_access=false is denied at the broker too -------------------
closed_json=$(echo "$rules_json" | jq -c '. + {allow_internet_access: false}')
closed_id=$(create_sandbox "$AENV_TEMPLATE_ID" 120 "$closed_json"); _sync_http
assert_status "$HTTP_STATUS" "201" "create closed sandbox with rules"
track_sandbox "$closed_id"
wait_for_sandbox_state "$closed_id" "running" 60
closed_code=$(guest_http_code "$closed_id" "https://${EGRESS_UPSTREAM}/anything" 20)
if [[ "$closed_code" == "403" || "$closed_code" == "000" ]]; then
  _pass "closed sandbox cannot reach the rule domain through the broker (HTTP ${closed_code})"
else
  _fail "closed sandbox through the broker" "403 or connection failure" "$closed_code"
fi

# The Authorization header the upstream echoes back, retried until it appears or
# the deadline passes: the nodes learn that the broker is back from a periodic
# probe, so the first request after a rollout may land inside that window.
brokered_auth_within() {
  local deadline=$(( $(date +%s) + $1 )) auth=""
  while (( $(date +%s) < deadline )); do
    auth=$(run_in_sandbox "$sandbox_id" "curl -sS --max-time 20 https://${EGRESS_UPSTREAM}/anything" 2>/dev/null | jq -r '.headers.Authorization // .headers.authorization // empty' 2>/dev/null || true)
    [[ -n "$auth" ]] && break
    sleep 3
  done
  printf '%s' "$auth"
}

# The certificate chain the guest is served for a rule domain, one PEM subject
# per line, leaf first. `-showcerts` prints the whole chain the broker sent.
guest_chain_subjects() {
  local sandbox_id="$1"
  run_in_sandbox "$sandbox_id" \
    "echo | timeout 20 openssl s_client -showcerts -servername ${EGRESS_UPSTREAM} \
     -connect ${EGRESS_UPSTREAM}:443 2>/dev/null | grep -E '^ *[0-9]+ s:' || true"
}

# -- 5 and 7. Broker restart and outage (Kubernetes only) --------------------------
if [[ "${E2E_MODE:-}" == "k8s" ]] && command -v kubectl >/dev/null 2>&1 \
   && kubectl -n "${K8S_NAMESPACE:-agentenv-system}" get ds/aenv-egress >/dev/null 2>&1; then
  ns="${K8S_NAMESPACE:-agentenv-system}"

  # 🔴 The chain a rule domain presents is leaf + this node's intermediate, and
  # the guest trusts neither of them directly — it trusts the root they chain
  # to. Recorded before the rollout so the comparison after it means something.
  chain_before=$(guest_chain_subjects "$sandbox_id")
  chain_depth=$(printf '%s\n' "$chain_before" | grep -c 's:' || true)
  if [[ "${chain_depth:-0}" -ge 2 ]]; then
    _pass "a rule domain is served leaf + intermediate (${chain_depth} certificates)"
  else
    _fail "brokered chain depth" ">= 2" "${chain_depth:-0}"
  fi
  issuer_before=$(printf '%s\n' "$chain_before" | grep -o 'AgentENV Egress Node [^,/]*' | head -n 1 || true)
  assert_not_empty "$issuer_before" "the chain names the node that issued it"

  kubectl -n "$ns" rollout restart ds/aenv-egress >/dev/null
  kubectl -n "$ns" rollout status ds/aenv-egress --timeout=180s >/dev/null
  state=$(get_sandbox_state "$sandbox_id")
  assert_eq "$state" "running" "sandbox survives a broker rollout"
  after_roll=$(brokered_auth_within 60)
  assert_eq "$after_roll" "Bearer ${secret_value}" "brokered requests resume after the rollout"
  # The restarted broker took a fresh intermediate and re-minted the leaf under
  # it; the guest's trust store never changed, so the handshake still verifies.
  chain_after=$(guest_chain_subjects "$sandbox_id")
  assert_not_empty \
    "$(printf '%s\n' "$chain_after" | grep -o 'AgentENV Egress Node [^,/]*' | head -n 1 || true)" \
    "a cached name still handshakes after the intermediate is replaced"

  # A DaemonSet has no replica count to take to zero. Selecting a label no node
  # carries is the same statement, and it is reversible by the same trap.
  #
  # Registered before the patch, not after it: an interrupt or a runner timeout
  # between the two would otherwise leave every node without a broker. Chained
  # onto the harness's own EXIT trap (`_cleanup_e2e`) rather than replacing it.
  _restore_egress_daemonset() {
    kubectl -n "$ns" patch ds/aenv-egress --type=json \
      -p '[{"op":"remove","path":"/spec/template/spec/nodeSelector/aenv-egress"}]' \
      >/dev/null 2>&1 || true
  }
  trap '_restore_egress_daemonset; _cleanup_e2e' EXIT
  kubectl -n "$ns" patch ds/aenv-egress --type=merge \
    -p '{"spec":{"template":{"spec":{"nodeSelector":{"aenv-egress":"parked"}}}}}' >/dev/null
  for _ in $(seq 1 40); do
    remaining=$(kubectl -n "$ns" get pods -l app.kubernetes.io/name=aenv-egress \
      --no-headers 2>/dev/null | wc -l)
    [[ "${remaining:-1}" -eq 0 ]] && break
    sleep 3
  done
  outage_code=$(guest_http_code "$sandbox_id" "https://${EGRESS_UPSTREAM}/anything" 10)
  if [[ "$outage_code" == "000" ]]; then
    _pass "guest connection fails fast while the broker is down"
  else
    _fail "broker outage visible to the guest" "connection failure" "HTTP ${outage_code}"
  fi
  _restore_egress_daemonset
  kubectl -n "$ns" rollout status ds/aenv-egress --timeout=180s >/dev/null
  recovered=$(brokered_auth_within 60)
  assert_eq "$recovered" "Bearer ${secret_value}" "brokered requests recover after the broker returns"

  # -- A broker asks only about the sandboxes on its own machine ------------------
  #
  # The broker image carries no HTTP client, so its own projected token is read
  # out of it and presented from a node Pod, which does carry curl (its preStop
  # hook uses it). Both Pods are the deployment's own; nothing here mints a
  # credential the cluster would not otherwise have.
  sandbox_node=$(api_get "/registry/sandboxes" >/dev/null 2>&1; \
    echo "$HTTP_BODY" | jq -r --arg id "$sandbox_id" \
      '.sandboxes[]? | select(.sandboxID == $id) | .nodeID // empty' 2>/dev/null | head -n 1 || true)
  other_broker=$(kubectl -n "$ns" get pods -l app.kubernetes.io/name=aenv-egress \
    -o jsonpath="{range .items[?(@.spec.nodeName!='${sandbox_node}')]}{.metadata.name}{'\n'}{end}" \
    2>/dev/null | head -n 1 || true)
  node_pod=$(kubectl -n "$ns" get pods -l app.kubernetes.io/name=agentenv-node \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
  if [[ -n "$sandbox_node" && -n "$other_broker" && -n "$node_pod" ]]; then
    foreign_token=$(kubectl -n "$ns" exec "$other_broker" -- \
      cat /var/run/secrets/aenv/api/token 2>/dev/null || true)
    if [[ -n "$foreign_token" ]]; then
      body=$(jq -nc --arg s "$sandbox_id" --arg n "$secret_name" \
        '{sandboxId: $s, executionId: "unknown", name: $n}')
      foreign_code=$(kubectl -n "$ns" exec "$node_pod" -- \
        curl -sS -o /dev/null -w '%{http_code}' --max-time 15 \
        -X POST http://agentenv-api:8000/internal/credentials/resolve \
        -H "Authorization: Bearer ${foreign_token}" \
        -H 'content-type: application/json' -d "$body" 2>/dev/null || true)
      assert_eq "${foreign_code:-000}" "404" \
        "a broker on another node is told nothing about this sandbox"
    else
      _skip "skipped: the other node's broker token is unreadable"
    fi
  else
    warn "no second node with its own broker; skipping the cross-node resolve check"
    _skip "skipped: cross-node resolve check needs two nodes"
  fi
else
  warn "not a Kubernetes run with kubectl; skipping broker rollout and outage checks"
  _skip "skipped: broker rollout/outage checks"
fi

# -- Cleanup ----------------------------------------------------------------------
api_delete "/secrets/${secret_id}"
assert_status "$HTTP_STATUS" "204" "delete secret"
api_get "/secrets/${secret_id}"
assert_status "$HTTP_STATUS" "404" "deleted secret is gone"

suite_summary
