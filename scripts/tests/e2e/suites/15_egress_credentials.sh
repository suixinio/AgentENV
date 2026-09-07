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
SPEAKS_PL_B64="$(base64 -w0 < "${SUITE_DIR}/../postgres_speaks_probe.pl")"

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

# What `openssl s_client` says about the chain the guest was served: the
# subject of every certificate, and the verification verdict. The verdict is
# the half that matters — a chain of the right shape that does not verify is a
# guest that cannot use it.
#
# The verdict lines are matched unanchored: OpenSSL 3 indents `Verify return
# code:` four spaces inside the session block and prints `Verification: OK`
# above it, so a pattern anchored at the line start finds neither.
guest_chain_report() {
  local sandbox_id="$1"
  run_in_sandbox "$sandbox_id" \
    "echo | timeout 20 openssl s_client -showcerts -servername ${EGRESS_UPSTREAM} \
     -connect ${EGRESS_UPSTREAM}:443 2>/dev/null \
     | grep -E '^ *[0-9]+ s:|Verify return code:|Verification:' || true"
}

# Whether that report says the guest's own trust store accepted the chain.
chain_verifies() {
  printf '%s\n' "$1" | grep -Eq '^[[:space:]]*Verify return code: 0 \(ok\)|^[[:space:]]*Verification: OK'
}

# -- 4b. An explicit endpoint across a PUT ----------------------------------------
#
# The rules listener above asks for port 0, so the namespace hands it a fresh
# port every time and a `PUT` that rebuilt it would still succeed. An explicit
# endpoint names its port, and that is where both halves of the update show:
# rebuilding it asks the namespace for a port it is already holding (500), and
# keeping it without taking the new spec leaves the intercept where it was
# (204 and nothing happens).
#
# Whether the intercept is installed is read from the guest, by asking what
# answers on the documentation address: only the broker speaks postgres there,
# so a reply to a startup packet is the DNAT and nothing else.
#
# Whether the *connection completes* proves nothing: a network whose gateway
# answers TEST-NET-3 on every port — which is what the cluster this runs on
# does — completes both ways round, and that made the "turning it on" leg pass
# with the intercept never installed. The peer saying postgres cannot be
# faked by a network: it is the handler behind the listener.
speaks_broker_on_port() {
  local sandbox_id="$1" port="$2"
  run_in_sandbox "$sandbox_id" \
    "printf %s '${SPEAKS_PL_B64}' | base64 -d > /tmp/pgspeak.pl && \
     (perl /tmp/pgspeak.pl 203.0.113.10 ${port} 3 2>/dev/null || true)" \
    2>/dev/null | tail -n 1 || true
}

# `yes`, `no`, or whatever the guest said instead, so the caller can skip on it.
intercepted_on_port() {
  local said
  said=$(speaks_broker_on_port "$1" "$2")
  case "$said" in
    bytes=*)                   echo yes ;;
    silent|eof|connect_failed) echo no ;;
    *)                         echo "${said:-nothing}" ;;
  esac
}

endpoint_port=15433
endpoint_secret="e2e-endpoint-$(date +%s)-$RANDOM"
endpoint_secret_id=""
endpoint_sandbox=""
endpoint_network() {
  jq -nc --argjson p "$endpoint_port" --arg c "$endpoint_secret" --argjson i "$1" \
    '{network: {"x-aenv-endpoints": [{port: $p, handler: "postgres",
                                      params: {credential: $c}, interceptPort: $i}]}}'
}

# An endpoint credential is a fields secret; the header marker above is not one
# and no secret satisfies both shapes. Nothing here ever completes a session,
# so the fields only have to exist.
api_post "/secrets" "$(jq -nc --arg n "$endpoint_secret" \
  '{name: $n, fields: {host: "203.0.113.20", port: "5432", user: "e2e", password: "e2e"}}')"
if [[ "$HTTP_STATUS" == "201" ]]; then
  endpoint_secret_id=$(echo "$HTTP_BODY" | jq -r '.secretID // empty')
  endpoint_sandbox=$(create_sandbox "$AENV_TEMPLATE_ID" 120 "$(endpoint_network false)"); _sync_http
fi
if [[ -n "$endpoint_sandbox" && "$HTTP_STATUS" == "201" ]]; then
  track_sandbox "$endpoint_sandbox"
  wait_for_sandbox_state "$endpoint_sandbox" "running" 60
  probe_kind=$(intercepted_on_port "$endpoint_sandbox" "$endpoint_port")

  if [[ "$probe_kind" == "yes" || "$probe_kind" == "no" ]]; then
    assert_eq "$probe_kind" "no" "an endpoint declared without interceptPort captures nothing"

    api_put "/sandboxes/${endpoint_sandbox}/network" "$(endpoint_network false | jq -c '.network')"
    assert_status "$HTTP_STATUS" "204" "re-sending the same explicit endpoint is accepted"

    api_put "/sandboxes/${endpoint_sandbox}/network" "$(endpoint_network true | jq -c '.network')"
    assert_status "$HTTP_STATUS" "204" "turning interceptPort on is accepted"
    assert_eq "$(intercepted_on_port "$endpoint_sandbox" "$endpoint_port")" "yes" \
      "turning interceptPort on installs the intercept"

    api_put "/sandboxes/${endpoint_sandbox}/network" "$(endpoint_network false | jq -c '.network')"
    assert_status "$HTTP_STATUS" "204" "turning interceptPort off is accepted"
    assert_eq "$(intercepted_on_port "$endpoint_sandbox" "$endpoint_port")" "no" \
      "turning interceptPort off removes the intercept"
  else
    warn "the guest answered ${probe_kind:-nothing} to the intercept probe; no perl in the template?"
    _skip "skipped: explicit endpoint intercept checks"
  fi
  api_delete "/sandboxes/${endpoint_sandbox}" >/dev/null 2>&1 || true
else
  warn "no sandbox with an explicit endpoint (HTTP ${HTTP_STATUS}); the broker may serve no postgres handler"
  _skip "skipped: explicit endpoint checks"
fi
if [[ -n "$endpoint_secret_id" ]]; then
  api_delete "/secrets/${endpoint_secret_id}" >/dev/null 2>&1 || true
fi

# -- 5 and 7. Broker restart and outage (Kubernetes only) --------------------------
if [[ "${E2E_MODE:-}" == "k8s" ]] && command -v kubectl >/dev/null 2>&1 \
   && kubectl -n "${K8S_NAMESPACE:-agentenv-system}" get ds/aenv-egress-node >/dev/null 2>&1; then
  ns="${K8S_NAMESPACE:-agentenv-system}"

  # What the guest's own trust store makes of a rule domain, recorded before
  # the rollout so the same question after it means something.
  chain_before=$(guest_chain_report "$sandbox_id")
  if chain_verifies "$chain_before"; then
    _pass "the guest verifies the brokered chain against its own trust store"
  else
    _fail "brokered chain verification" "Verify return code: 0 (ok)" \
      "$(printf '%s' "$chain_before" | tr '\n' ' ')"
  fi

  kubectl -n "$ns" rollout restart ds/aenv-egress-node >/dev/null
  kubectl -n "$ns" rollout status ds/aenv-egress-node --timeout=180s >/dev/null
  state=$(get_sandbox_state "$sandbox_id")
  assert_eq "$state" "running" "sandbox survives a broker rollout"
  after_roll=$(brokered_auth_within 60)
  assert_eq "$after_roll" "Bearer ${secret_value}" "brokered requests resume after the rollout"
  # The restarted broker minted the leaf again from the same root it mounts,
  # and the guest's trust store never changed.
  chain_after=$(guest_chain_report "$sandbox_id")
  if chain_verifies "$chain_after"; then
    _pass "the chain still verifies against the same root after the rollout"
  else
    _fail "chain verification after the rollout" "Verify return code: 0 (ok)" \
      "$(printf '%s' "$chain_after" | tr '\n' ' ')"
  fi

  # -- Which broker may resolve for this sandbox, and only that one --------------
  #
  # No node name is asked for and none is needed. `GET /registry/sandboxes` is
  # the *paused* registry, so a running sandbox is never in it and every
  # assertion that read a node name out of it skipped instead of running.
  # Present each broker's own projected token in turn: exactly one of them may
  # resolve this sandbox's grant, and that one names the machine it runs on.
  #
  # The broker image carries no HTTP client, so the tokens are read out of the
  # broker Pods and presented from a node Pod, which does carry curl (its
  # preStop hook uses it). Both Pods are the deployment's own; nothing here
  # mints a credential the cluster would not otherwise have.
  api_get "/sandboxes/${sandbox_id}"
  execution_id=$(echo "$HTTP_BODY" | jq -r '.executionID // empty' 2>/dev/null || true)

  resolve_as() {
    local pod="$1" token="$2" sandbox="$3" execution="$4" body
    body=$(jq -nc --arg s "$sandbox" --arg e "$execution" --arg n "$secret_name" \
      '{sandboxId: $s, executionId: $e, name: $n}')
    kubectl -n "$ns" exec "$pod" -- \
      curl -sS -o /dev/null -w '%{http_code}' --max-time 15 \
      -X POST http://agentenv-api:8000/internal/credentials/resolve \
      -H "Authorization: Bearer ${token}" \
      -H 'content-type: application/json' -d "$body" 2>/dev/null || true
  }
  broker_token_of() {
    kubectl -n "$ns" exec "$1" -- cat /var/run/secrets/aenv/api/token 2>/dev/null || true
  }
  # One line per broker: "<pod> <node> <status>".
  resolve_matrix() {
    local sandbox="$1" execution="$2" pod node token
    while read -r pod node; do
      [[ -z "$pod" ]] && continue
      token=$(broker_token_of "$pod")
      if [[ -z "$token" ]]; then
        printf '%s %s no-token\n' "$pod" "$node"
        continue
      fi
      printf '%s %s %s\n' "$pod" "$node" "$(resolve_as "$node_pod" "$token" "$sandbox" "$execution")"
    done <<< "$broker_rows"
  }

  node_pod=$(kubectl -n "$ns" get pods -l app.kubernetes.io/name=agentenv-node \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
  # The label the per-node broker's Pods carry, which is also its object name.
  broker_rows=$(kubectl -n "$ns" get pods -l app.kubernetes.io/name=aenv-egress-node \
    -o jsonpath='{range .items[*]}{.metadata.name} {.spec.nodeName}{"\n"}{end}' 2>/dev/null |
    sed '/^$/d' || true)
  broker_count=$(printf '%s' "$broker_rows" | grep -c . || true)

  sandbox_broker=""
  sandbox_node=""
  if [[ -n "$execution_id" && -n "$node_pod" && "${broker_count:-0}" -ge 1 ]]; then
    matrix=$(resolve_matrix "$sandbox_id" "$execution_id")
    resolved=$(printf '%s\n' "$matrix" | awk '$3 == "200"' | wc -l | tr -d ' ')
    refused=$(printf '%s\n' "$matrix" | awk '$3 == "404"' | wc -l | tr -d ' ')
    assert_eq "$resolved" "1" "exactly one broker resolves this sandbox's grant"
    if [[ "$broker_count" -ge 2 ]]; then
      assert_eq "$refused" "$((broker_count - 1))" \
        "every broker on another node is told nothing about it"
    else
      warn "only one broker in this deployment; the cross-node half needs two"
      _skip "skipped: cross-node resolve check needs two brokers"
    fi
    sandbox_broker=$(printf '%s\n' "$matrix" | awk '$3 == "200" {print $1; exit}')
    sandbox_node=$(printf '%s\n' "$matrix" | awk '$3 == "200" {print $2; exit}')
  else
    warn "no executionID, node Pod or broker Pod; skipping the resolve scope checks"
    _skip "skipped: resolve scope checks"
  fi

  # -- A node whose broker stopped answering takes no new sandbox ----------------
  #
  # The outage is one node's, and it is the socket that goes away rather than
  # the process. A deleted Pod is replaced in about ten seconds, which closes
  # the window before anything can be observed in it; and a signal is no help
  # either — PID 1 in a container carries `SIGNAL_UNKILLABLE`, so a SIGSTOP
  # sent from inside its own namespace is discarded and the broker keeps
  # serving. Renaming the socket file is what both probes actually read: the
  # node's connect gets ENOENT, the readiness probe fails the same way, and
  # the listener stays bound to the inode so nothing is lost when it comes
  # back.
  socket_dir=/run/aenv-egress
  if [[ -n "$sandbox_broker" ]]; then
    _restore_broker_socket() {
      kubectl -n "$ns" exec "$sandbox_broker" -- \
        sh -c "mv -f ${socket_dir}/broker.sock.hidden ${socket_dir}/broker.sock" \
        >/dev/null 2>&1 || true
    }
    # Registered before the socket moves, not after: an interrupt in between
    # would otherwise leave that node without a broker. Chained onto the
    # harness's own EXIT trap rather than replacing it.
    trap '_restore_broker_socket; _cleanup_e2e' EXIT
    kubectl -n "$ns" exec "$sandbox_broker" -- \
      sh -c "mv ${socket_dir}/broker.sock ${socket_dir}/broker.sock.hidden" >/dev/null 2>&1 \
      || warn "could not move the broker socket aside"

    outage_code=$(guest_http_code "$sandbox_id" "https://${EGRESS_UPSTREAM}/anything" 10)
    if [[ "$outage_code" == "000" ]]; then
      _pass "guest requests stop while its node's broker socket is gone"
    else
      _fail "broker outage visible to the guest" "connection failure" "HTTP ${outage_code}"
    fi

    reported=""
    for _ in $(seq 1 15); do
      api_admin_get "/nodes"
      reported=$(echo "$HTTP_BODY" | jq -r --arg n "$sandbox_node" \
        '.[]? | select(.id == $n) | .egressBroker // empty' 2>/dev/null | head -n 1 || true)
      [[ "$reported" == "local_unreachable" ]] && break
      sleep 2
    done
    assert_eq "$reported" "local_unreachable" \
      "the node whose broker stopped answering reports it unreachable"

    if [[ "$broker_count" -ge 2 ]]; then
      elsewhere_id=$(create_sandbox "$AENV_TEMPLATE_ID" 120 "$rules_json"); _sync_http
      if [[ "$HTTP_STATUS" == "201" ]]; then
        track_sandbox "$elsewhere_id"
        wait_for_sandbox_state "$elsewhere_id" "running" 60
        api_get "/sandboxes/${elsewhere_id}"
        elsewhere_execution=$(echo "$HTTP_BODY" | jq -r '.executionID // empty' 2>/dev/null || true)
        landed=$(resolve_matrix "$elsewhere_id" "$elsewhere_execution" |
          awk '$3 == "200" {print $2; exit}')
        if [[ -n "$landed" && "$landed" != "$sandbox_node" ]]; then
          _pass "a sandbox with rules is placed away from the node with no broker"
        else
          _fail "placement avoids the node with no broker" "a node other than ${sandbox_node}" \
            "${landed:-nothing resolved it}"
        fi
        api_delete "/sandboxes/${elsewhere_id}"
      else
        _fail "create with rules while one node's broker is unreachable" "201" "$HTTP_STATUS"
      fi
    else
      warn "one broker only; with none left to place on, this would be a 503 by design"
      _skip "skipped: placement-away check needs two brokers"
    fi

    _restore_broker_socket
    back=""
    for _ in $(seq 1 15); do
      api_admin_get "/nodes"
      back=$(echo "$HTTP_BODY" | jq -r --arg n "$sandbox_node" \
        '.[]? | select(.id == $n) | .egressBroker // empty' 2>/dev/null | head -n 1 || true)
      [[ "$back" == "local_ok" ]] && break
      sleep 2
    done
    assert_eq "$back" "local_ok" "the node reports its broker healthy again"
    recovered=$(brokered_auth_within 60)
    assert_eq "$recovered" "Bearer ${secret_value}" \
      "brokered requests recover once the socket is back"
  else
    warn "no broker resolved this sandbox; skipping the outage check"
    _skip "skipped: broker outage check"
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
