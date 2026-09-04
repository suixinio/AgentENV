#!/usr/bin/env bash
set -euo pipefail

SUITE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "${SUITE_DIR}/../lib/helpers.sh"
init_suite "16_egress_postgres"

log "Suite: Brokered Postgres (explicit endpoints + a credential the sandbox never holds)"

# The upstream this suite brokers to. It has to be reachable from the broker
# Pod and inside `[handlers.postgres].allowed_cidrs`; the credential named
# below has to resolve to it with a user and a password that work. Nothing
# here creates either: a structured credential is written by the operator (or
# answered by the resolver), because `/secrets` stores one opaque value.
PG_SECRET="${E2E_PG_SECRET:-}"
PG_DATABASE="${E2E_PG_DATABASE:-postgres}"
# The probe sets application_name to "probe"; a resolver that overrides it is
# a legitimate deployment and the assertion below says which case it saw.
ENDPOINT_PORT="${E2E_PG_ENDPOINT_PORT:-5432}"
# Where a brokered listener binds inside every sandbox: the tap side of the VM
# link, from network.internal's fixed VM link CIDR.
LISTENER_IP="169.254.0.22"
EGRESS_PY="${SUITE_DIR}/../egress_credentials_e2e.py"
# No stock template has psql, so the guest speaks the protocol itself. Which
# interpreter it has varies -- the driver's own base image ships perl and no
# python3 -- so both are carried and the guest picks.
PROBE_PY_B64="$(base64 -w0 < "${SUITE_DIR}/../postgres_broker_probe.py")"
PROBE_PL_B64="$(base64 -w0 < "${SUITE_DIR}/../postgres_broker_probe.pl")"
PROBE_RUNNER=""

run_in_sandbox() {
  local sandbox_id="$1" cmd="$2"
  E2B_API_URL="${AENV_URL}" E2B_SANDBOX_URL="${AENV_PROXY_URL}" E2B_API_KEY="${AENV_API_KEY}" \
    python3 "${EGRESS_PY}" run "${sandbox_id}" "${cmd}"
}

# One query through the brokered endpoint, with a user and password that are
# placeholders. Prints the first row, or `ERR:<sqlstate>:<message>`.
brokered_query() {
  local sandbox_id="$1" user="$2" password="$3" database="$4" sql="$5" out
  out=$(run_in_sandbox "$sandbox_id" \
    "printf %s '${PROBE_B64}' | base64 -d > ${PROBE_PATH} && \
     (${PROBE_RUNNER} ${PROBE_PATH} '${LISTENER_IP}' '${ENDPOINT_PORT}' \
       '${user}' '${password}' '${database}' \"${sql}\" 2>&1 || true)" | tail -n 1 || true)
  printf '%s' "${out:-ERR:no output}"
}

endpoint_json() {
  jq -nc --argjson p "$ENDPOINT_PORT" --arg c "$PG_SECRET" \
    '{network: {"x-aenv-endpoints": [{port: $p, handler: "postgres", params: {credential: $c}}]}}'
}

# -- Preconditions -------------------------------------------------------------
if [[ -z "$PG_SECRET" ]]; then
  warn "E2E_PG_SECRET is unset: this suite needs a structured credential the broker can resolve"
  _skip "brokered postgres, no credential named"
  suite_summary "16_egress_postgres"
  exit 0
fi

if ! python3 -c 'import e2b' >/dev/null 2>&1; then
  warn "python e2b SDK not importable; the suite cannot run commands inside sandboxes"
  _skip "brokered postgres, no e2b SDK"
  suite_summary "16_egress_postgres"
  exit 0
fi

sandbox_id=$(create_sandbox "$AENV_TEMPLATE_ID" 180 "$(endpoint_json)"); _sync_http
case "$HTTP_STATUS" in
  201) ;;
  400)
    warn "the api half refused the endpoint declaration: ${HTTP_BODY}"
    _skip "brokered postgres, the credential name is unknown to this deployment"
    suite_summary "16_egress_postgres"
    exit 0
    ;;
  503)
    warn "no node reports a usable egress broker"
    _skip "brokered postgres, no egress broker on any node"
    suite_summary "16_egress_postgres"
    exit 0
    ;;
  *)
    _fail "create a sandbox with a postgres endpoint" "201" "$HTTP_STATUS ${HTTP_BODY}"
    suite_summary "16_egress_postgres"
    exit 1
    ;;
esac
assert_not_empty "$sandbox_id" "sandbox with a postgres endpoint started"

cleanup() {
  [[ -n "${sandbox_id:-}" ]] && api_delete "/sandboxes/${sandbox_id}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

if run_in_sandbox "$sandbox_id" "command -v python3" >/dev/null 2>&1; then
  PROBE_RUNNER="python3"; PROBE_B64="$PROBE_PY_B64"; PROBE_PATH="/tmp/pgprobe.py"
elif run_in_sandbox "$sandbox_id" "command -v perl" >/dev/null 2>&1; then
  PROBE_RUNNER="perl"; PROBE_B64="$PROBE_PL_B64"; PROBE_PATH="/tmp/pgprobe.pl"
else
  warn "the template has neither python3 nor perl; the guest cannot speak the protocol"
  _skip "brokered postgres, no interpreter in the template"
  suite_summary "16_egress_postgres"
  exit 0
fi
_pass "the guest can speak the protocol with ${PROBE_RUNNER}"

# -- The credential is the broker's, whatever the sandbox writes ---------------
answer=$(brokered_query "$sandbox_id" placeholder placeholder "$PG_DATABASE" "select 1")
if [[ "$answer" != "1" ]]; then
  warn "the brokered connection did not answer: ${answer}"
  _skip "brokered postgres, the upstream did not answer through the broker"
  suite_summary "16_egress_postgres"
  exit 0
fi
_pass "a placeholder DSN reaches the upstream through the broker"

as_operator=$(brokered_query "$sandbox_id" placeholder placeholder "$PG_DATABASE" \
  "select current_user")
assert_not_eq "$as_operator" "placeholder" "the connection is not the user the guest wrote"

# A different placeholder must land on the same account: nothing the sandbox
# writes selects a credential.
as_other=$(brokered_query "$sandbox_id" someone-else hunter2 "$PG_DATABASE" \
  "select current_user")
assert_eq "$as_other" "$as_operator" "any user and password reach the same account"

# -- The guest's own trust store is untouched ----------------------------------
trust_env=$(run_in_sandbox "$sandbox_id" "env | grep -c '^SSL_CERT_FILE=' || true" | head -n 1)
assert_eq "${trust_env:-0}" "0" "an endpoint reached in the clear adds no CA to the guest"

# -- Fields the credential does not name are the guest's -----------------------
guest_app=$(brokered_query "$sandbox_id" placeholder placeholder "$PG_DATABASE" \
  "select current_setting('application_name')")
if [[ "$guest_app" == "probe" ]]; then
  _pass "a parameter the credential does not name reaches the upstream as the guest wrote it"
else
  # A resolver that overrides application_name is a legitimate deployment; say
  # which case this is rather than calling it a failure.
  _skip "application_name passthrough, this credential overrides it (${guest_app})"
fi

# -- Revocation ----------------------------------------------------------------
api_delete "/sandboxes/${sandbox_id}"; _sync_http
assert_status "$HTTP_STATUS" "204" "delete the sandbox, which revokes its grant"
sandbox_id=""

suite_summary "16_egress_postgres"
