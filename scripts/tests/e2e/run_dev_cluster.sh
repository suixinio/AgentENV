#!/usr/bin/env bash
# Run the e2e suites against an already-running, long-lived k3s cluster
# (currently pve-mf: k3s on 10.1.0.200 + 10.1.0.201,
# namespace agentenv-system) without touching its deployment state.
#
# The previous dev cluster, pve-sg (aenv-master-01 10.10.10.203 +
# aenv-worker-01 10.10.10.204), has been decommissioned; nothing there is
# reachable any more.
#
# ---------------------------------------------------------------------------
# Why not `make test-e2e-k8s`
# ---------------------------------------------------------------------------
# `make test-e2e-k8s` runs run_e2e.sh with E2E_MODE=k8s, and
# lib/runtime.sh's start_k8s_runtime() for that mode unconditionally:
#   - calls stop_k8s_runtime(), which runs `make k8s-delete-dev`
#     (deletes every resource the kustomize overlay manages), then
#   - runs `make k8s-apply-dev` (reapplies the whole overlay).
# Against a disposable kind/minikube cluster that is exactly what you want.
# Pointed at pve-mf (or any other shared cluster) it deletes and reapplies a
# shared deployment other people/sessions rely on. This script never calls
# k8s-apply-dev, k8s-delete-dev, k8s-load-dev, or any image build target -- it only reads
# from the cluster (kubectl get/describe, port-forward, curl) and assumes
# whatever is already running there is the version you want to test.
#
# ---------------------------------------------------------------------------
# Why a new script instead of a new E2E_MODE in lib/runtime.sh
# ---------------------------------------------------------------------------
# Every suite's clustering behavior (e2e_mode_is_clustered, node fan-out via
# AENV_NODE_URLS, etc.) is gated purely on the *value* of E2E_MODE -- setting
# E2E_MODE=k8s before sourcing lib/runtime.sh already makes every suite
# behave exactly as it does under `make test-e2e-k8s`, with zero changes to
# the shared lib/*.sh files. The functions that *do* need to differ for an
# already-running cluster -- start_k8s_runtime (apply is unconditional, no
# skip-apply knob) and wait_for_k8s_runtime (does `kubectl rollout status
# deploy/agentenv-scheduler`, a Deployment phase-four's k8s overlay no
# longer manages here) -- are precisely the ones this script must not call.
# Threading a new `existing-k8s` mode value through configure_runtime_endpoints,
# the start/wait/stop dispatchers, and *both* copies of e2e_mode_is_clustered
# (lib/runtime.sh and lib/helpers.sh define it identically; whichever sources
# second wins) would only be in service of machinery this script never uses.
# So: reuse E2E_MODE=k8s for the parts that are just data (suite branching),
# and supply our own read-only connect/preflight/cleanup for the parts that
# are apply/delete-coupled in lib/runtime.sh. Everything else (HTTP wrappers,
# assertions, sandbox/template helpers, the suite loop shape) is reused
# as-is by sourcing the same lib/*.sh files run_e2e.sh sources.
#
# ---------------------------------------------------------------------------
# Usage
# ---------------------------------------------------------------------------
#   ./scripts/tests/e2e/run_dev_cluster.sh
#   SUITE_FILTER="07*" ./scripts/tests/e2e/run_dev_cluster.sh
#   AENV_TEMPLATE_ID=my-template ./scripts/tests/e2e/run_dev_cluster.sh
#
# Environment variables:
#   KUBECONFIG                            - default: ~/.kube/config-aenv-mf
#   E2E_K8S_NAMESPACE                     - default: agentenv-system
#   E2E_DEV_CLUSTER_GATEWAY_NODEPORT_URL  - default: http://10.1.0.200:30800
#   E2E_DEV_CLUSTER_GATEWAY_MODE          - auto (default) | nodeport | port-forward
#   E2E_K8S_GATEWAY_LOCAL_PORT            - local port-forward port if the
#                                            NodePort path isn't used/reachable
#                                            (default: 18080)
#   E2E_SPLIT_ADDRESSES                   - 1 to drive REST at svc/agentenv-api
#                                            and leave the gateway holding only
#                                            the data plane (default 0: one
#                                            address, the gateway, for both)
#   E2E_K8S_API_LOCAL_PORT                - local port for the api port-forward
#                                            when the run is split (default:
#                                            18079, below the node range)
#   AENV_TEMPLATE_ID                      - skip self-building a base template
#   SUITE_FILTER                          - glob against suites/*.sh (default: *.sh)
#   AENV_CONTROL_PLANE_TOKEN              - credential for a REST address that
#                                            sits behind a control-plane gate;
#                                            a split run addresses it directly
#                                            and the gateway is not there to
#                                            stamp it (default: empty, no gate)
#   AENV_API_KEY / AENV_ADMIN_TOKEN       - default to the same e2e-test-key /
#                                            e2e-admin-token values run_e2e.sh
#                                            uses; override if this cluster's
#                                            secrets differ.
set -Eeuo pipefail
IFS=$'\n\t'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="${SCRIPT_DIR}/../../.."

COMMON_SH="${REPO_ROOT}/scripts/lib/common.sh"
[[ -f "$COMMON_SH" ]] || { echo "Missing ${COMMON_SH}" >&2; exit 1; }

: "${KUBECONFIG:=$HOME/.kube/config-aenv-mf}"
export KUBECONFIG
: "${E2E_K8S_NAMESPACE:=agentenv-system}"
export E2E_K8S_NAMESPACE
: "${E2E_DEV_CLUSTER_GATEWAY_NODEPORT_URL:=http://10.1.0.200:30800}"
: "${E2E_DEV_CLUSTER_GATEWAY_MODE:=auto}"
: "${E2E_DEV_CLUSTER_HEALTH_TIMEOUT:=30}"

# Hard safety invariant, not a user knob: this script must never apply,
# delete, or load anything, no matter what a caller's shell environment
# carries over from a previous `make test-e2e-k8s` run. lib/runtime.sh's
# start_k8s_runtime()/stop_k8s_runtime() are never called by name below, but
# stop_k8s_runtime() *is* reused for its port-forward cleanup (see cleanup()
# further down), so its delete-target guard is forced off here too, before
# lib/runtime.sh's own `:=` default has a chance to apply.
export E2E_K8S_APPLY_TARGET=none
export E2E_K8S_DELETE_TARGET=none
export E2E_K8S_LOAD_TARGET=none

# Track whether the caller pinned a template before helpers.sh defaults it.
_E2E_TEMPLATE_FROM_USER=0
[[ -n "${AENV_TEMPLATE_ID:-}" ]] && _E2E_TEMPLATE_FROM_USER=1

# Reuse E2E_MODE=k8s so every suite's e2e_mode_is_clustered / node fan-out
# branch hits exactly as it does under `make test-e2e-k8s` (see header).
export E2E_MODE=k8s

# shellcheck source=/dev/null
source "$COMMON_SH"
# shellcheck source=/dev/null
source "${SCRIPT_DIR}/lib/runtime.sh"
# shellcheck source=/dev/null
source "${SCRIPT_DIR}/lib/assertions.sh"
# shellcheck source=/dev/null
source "${SCRIPT_DIR}/lib/helpers.sh"

LOG_TAG="E2E-DEVCLUSTER"

_E2E_PHASE="initialization"
report_unexpected_error() {
  local status=$?
  local command="${BASH_COMMAND:-unknown}"
  local source_file="${BASH_SOURCE[1]:-${BASH_SOURCE[0]:-unknown}}"
  local line="${BASH_LINENO[0]:-0}"

  e2e_report_unexpected_error "${status}" "${command}" "${source_file}" "${line}" "${_E2E_PHASE}"
  return "${status}"
}
trap report_unexpected_error ERR

require_cmd kubectl
require_cmd curl
require_cmd jq

# ---- Cleanup -------------------------------------------------------------
# Replaces helpers.sh's own `trap _cleanup_e2e EXIT` (last trap set wins), so
# re-invoke it explicitly -- same pattern run_e2e.sh uses.

cleanup() {
  local status=$?
  if [[ "${status}" -ne 0 ]]; then
    (dump_test_runtime_diagnostics) || true
  fi
  _cleanup_e2e
  # Safe: E2E_K8S_DELETE_TARGET is forced to "none" above, so this only
  # kills the port-forward processes it started and resets AENV_NODE_* /
  # AENV_URL -- it never runs `make k8s-delete-dev`.
  stop_k8s_runtime
  return "${status}"
}
trap cleanup EXIT

# ---- Read-only pod-readiness check ----------------------------------------

_pod_ready_lines() {
  local selector="$1"
  kubectl -n "${E2E_K8S_NAMESPACE}" get pods -l "${selector}" \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.status.conditions[?(@.type=="Ready")].status}{"\n"}{end}' \
    2>/dev/null
}

require_pods_ready() {
  local label="$1" selector="$2"
  local total=0 ready=0
  local -a not_ready=()
  local name cond

  while IFS=$'\t' read -r name cond; do
    [[ -z "${name}" ]] && continue
    total=$((total + 1))
    if [[ "${cond}" == "True" ]]; then
      ready=$((ready + 1))
    else
      not_ready+=("${name} (Ready=${cond:-unknown})")
    fi
  done < <(_pod_ready_lines "${selector}")

  [[ "${total}" -gt 0 ]] ||
    die "No ${label} pods found in namespace ${E2E_K8S_NAMESPACE} (selector: ${selector})"

  if [[ "${#not_ready[@]}" -gt 0 ]]; then
    warn "${label}: ${ready}/${total} pods Ready; not Ready:"
    printf '  - %s\n' "${not_ready[@]}" >&2
    die "${label} pods are not all Ready in namespace ${E2E_K8S_NAMESPACE}"
  fi
  log "${label}: ${ready}/${total} pods Ready."
}

# ---- Gateway connection: NodePort direct, port-forward as fallback --------
#
# NodePort is preferred when reachable: it is one fewer long-lived local
# subprocess to keep alive for the whole suite run (a `kubectl port-forward`
# that dies mid-run silently strands every subsequent request), and this
# cluster already has `agentenv-gateway-nodeport` (30800 -> gateway:8080)
# provisioned for exactly this. port-forward stays as the fallback for a
# caller that cannot reach the cluster's node IPs directly.

_dev_cluster_use_nodeport() {
  log "Using gateway NodePort directly: ${E2E_DEV_CLUSTER_GATEWAY_NODEPORT_URL}"
  AENV_URL="${E2E_DEV_CLUSTER_GATEWAY_NODEPORT_URL}"
  AENV_PROXY_URL="${AENV_URL}"
}

_dev_cluster_use_port_forward() {
  log "Using kubectl port-forward for the gateway (svc/${E2E_K8S_GATEWAY_SERVICE} -> 127.0.0.1:${E2E_K8S_GATEWAY_LOCAL_PORT})"
  _start_k8s_port_forward "svc/${E2E_K8S_GATEWAY_SERVICE}" "${E2E_K8S_GATEWAY_LOCAL_PORT}" 8080 "gateway" ||
    die "Failed to port-forward the gateway service"
  AENV_URL="http://127.0.0.1:${E2E_K8S_GATEWAY_LOCAL_PORT}"
  AENV_PROXY_URL="${AENV_URL}"
}

# A split run moves REST off the gateway onto the api Service and leaves the
# gateway holding the data plane only.
_dev_cluster_split_rest_address() {
  e2e_addresses_are_split || return 0
  log "Using kubectl port-forward for the api half (svc/${E2E_K8S_API_SERVICE} -> 127.0.0.1:${E2E_K8S_API_LOCAL_PORT})"
  _start_k8s_port_forward "svc/${E2E_K8S_API_SERVICE}" "${E2E_K8S_API_LOCAL_PORT}" 8000 "api" ||
    die "Failed to port-forward the api service"
  AENV_URL="http://127.0.0.1:${E2E_K8S_API_LOCAL_PORT}"
}

connect_gateway() {
  case "${E2E_DEV_CLUSTER_GATEWAY_MODE}" in
    nodeport)
      _dev_cluster_use_nodeport
      ;;
    port-forward)
      _dev_cluster_use_port_forward
      ;;
    auto)
      log "Probing gateway NodePort at ${E2E_DEV_CLUSTER_GATEWAY_NODEPORT_URL}/health ..."
      if curl -sf -o /dev/null --max-time 5 "${E2E_DEV_CLUSTER_GATEWAY_NODEPORT_URL}/health"; then
        _dev_cluster_use_nodeport
      else
        warn "NodePort not reachable within 5s; falling back to kubectl port-forward."
        _dev_cluster_use_port_forward
      fi
      ;;
    *)
      die "Invalid E2E_DEV_CLUSTER_GATEWAY_MODE='${E2E_DEV_CLUSTER_GATEWAY_MODE}' (expected auto|nodeport|port-forward)"
      ;;
  esac
  # The gateway address is the data plane in both cases above; only the REST
  # address moves when the run is split.
  _dev_cluster_split_rest_address
  export AENV_URL AENV_PROXY_URL
}

# ---- Node connections: one port-forward per agentenv-node pod -------------
# Reuses lib/runtime.sh's _export_k8s_node_endpoints() unmodified: it lists
# Running pods matching E2E_K8S_NODE_SELECTOR and port-forwards each
# (read-only kubectl get + port-forward, no apply/delete), populating
# AENV_NODE_URLS / AENV_NODE_A_URL / AENV_NODE_URL_LABEL_MAP the same way
# `make test-e2e-k8s` does -- which is what e2e_mode_is_clustered suites'
# node-list branches read.

connect_nodes() {
  log "Port-forwarding agentenv-node pods (selector: ${E2E_K8S_NODE_SELECTOR}) ..."
  _export_k8s_node_endpoints
}

# ---- Preflight --------------------------------------------------------------

report_existing_sandboxes() {
  api_get "/sandboxes"
  if [[ "${HTTP_STATUS}" == "200" ]]; then
    local count
    count=$(printf '%s' "${HTTP_BODY}" | jq 'length' 2>/dev/null || printf 'unknown')
    log "Existing sandboxes on this cluster right now: ${count} (informational -- confirm this isn't noise someone else left running before assuming a suite caused it)."
  else
    warn "Could not query existing sandbox count (HTTP ${HTTP_STATUS}); check AENV_API_KEY against this cluster's secret."
  fi
}

report_ready_nodes() {
  local response status body ready_count total_count
  # The REST address may sit behind a control-plane gate; the suites carry that
  # credential through _curl_do, and a preflight addressing REST directly needs
  # it too or it reports a 403 as an unreachable node list.
  _e2e_control_plane_args "${AENV_URL}"
  response=$(curl -s "${_E2E_CP_ARGS[@]}" -H "X-Admin-Token: ${AENV_ADMIN_TOKEN}" -w $'\n%{http_code}' "${AENV_URL}/nodes" 2>/dev/null || true)
  status="${response##*$'\n'}"
  body="${response%$'\n'*}"
  if [[ "${status}" == "200" ]]; then
    ready_count=$(printf '%s' "${body}" | jq '[.[] | select(.status == "ready")] | length' 2>/dev/null || printf 'unknown')
    total_count=$(printf '%s' "${body}" | jq 'length' 2>/dev/null || printf 'unknown')
    log "Scheduler-visible nodes: ${ready_count}/${total_count} ready."
  else
    warn "Could not query /nodes (HTTP ${status}); check AENV_ADMIN_TOKEN against this cluster's secret."
  fi
}

preflight() {
  log "==== Preflight: pods Ready, gateway health, existing sandboxes ===="

  kubectl get ns "${E2E_K8S_NAMESPACE}" >/dev/null 2>&1 ||
    die "Namespace ${E2E_K8S_NAMESPACE} not reachable via KUBECONFIG=${KUBECONFIG}"

  require_pods_ready "agentenv-api" "app.kubernetes.io/name=agentenv-api"
  require_pods_ready "agentenv-node" "${E2E_K8S_NODE_SELECTOR}"

  connect_gateway
  _wait_for_health_url "rest" "${AENV_URL}" "${E2E_DEV_CLUSTER_HEALTH_TIMEOUT}" ||
    die "The REST address is not reachable at ${AENV_URL}/health"
  if e2e_addresses_are_split; then
    _wait_for_health_url "gateway" "${AENV_PROXY_URL}" "${E2E_DEV_CLUSTER_HEALTH_TIMEOUT}" ||
      die "Gateway not reachable at ${AENV_PROXY_URL}/health"
  fi

  connect_nodes
  local node_url label
  while IFS= read -r node_url; do
    [[ -z "${node_url}" ]] && continue
    label="$(node_label_for_url "${node_url}")"
    _wait_for_health_url "${label}" "${node_url}" "${E2E_DEV_CLUSTER_HEALTH_TIMEOUT}" ||
      die "${label} not reachable at ${node_url}/health"
  done < <(printf '%s\n' "${AENV_NODE_URLS}" | tr ' ' '\n')

  report_existing_sandboxes
  report_ready_nodes

  log "==== Preflight passed ===="
}

_E2E_PHASE="preflight"
preflight

# ---- Create base template ----------------------------------------------------
# Identical to run_e2e.sh: skipped when the caller pinned AENV_TEMPLATE_ID
# before helpers.sh defaulted it.

if [[ "${_E2E_TEMPLATE_FROM_USER:-0}" != "1" ]]; then
  _E2E_PHASE="create base template"
  E2E_TEMPLATE_NAME="e2e-devcluster-$(date +%s)"
  log "Creating base template '${E2E_TEMPLATE_NAME}' ..."

  create_template "$E2E_TEMPLATE_NAME" "$E2E_TEMPLATE_USER_IMAGE"

  if [[ "$HTTP_STATUS" != "202" ]]; then
    die "Failed to create base template (HTTP ${HTTP_STATUS}): ${HTTP_BODY}"
  fi

  E2E_TEMPLATE_ID=$(echo "$HTTP_BODY" | jq -r '.templateID // empty')
  E2E_TEMPLATE_BUILD_ID=$(echo "$HTTP_BODY" | jq -r '.buildID // empty')
  [[ -n "$E2E_TEMPLATE_ID" ]] || die "No templateID in response"
  track_template "$E2E_TEMPLATE_ID"

  log "Waiting for template build to complete (id: ${E2E_TEMPLATE_ID}) ..."
  wait_for_template_build "$E2E_TEMPLATE_ID" 120 "$E2E_TEMPLATE_BUILD_ID" || die "Template build failed or timed out"
  log "Template build ready."

  export AENV_TEMPLATE_ID="$E2E_TEMPLATE_NAME"
  export E2E_TEMPLATE_UUID="$E2E_TEMPLATE_ID"
  log "Using template: ${AENV_TEMPLATE_ID}"
else
  log "Using caller-supplied template: ${AENV_TEMPLATE_ID}"
fi

# ---- Run suites --------------------------------------------------------------

SUITES_DIR="${SCRIPT_DIR}/suites"
suite_filter="${SUITE_FILTER:-*.sh}"

total_suites=0
passed_suites=0
failed_suites=0
failed_names=()

for suite in "${SUITES_DIR}"/${suite_filter}; do
  [[ -f "$suite" ]] || continue
  suite_name="$(basename "$suite" .sh)"
  ((total_suites++)) || true

  _E2E_PHASE="suite ${suite_name}"
  log "Running suite: ${suite_name}"
  if bash "$suite"; then
    ((passed_suites++)) || true
  else
    ((failed_suites++)) || true
    failed_names+=("$suite_name")
  fi
done

# ---- Summary -----------------------------------------------------------------

echo ""
echo "========================================"
log "E2E (dev cluster) Test Summary"
echo "========================================"
printf "  Total:  %d\n" "$total_suites"
printf "  %bPassed: %d%b\n" "${LOG_COLOR_GREEN}" "$passed_suites" "${LOG_COLOR_RESET}"
if [[ "$failed_suites" -gt 0 ]]; then
  printf "  %bFailed: %d%b\n" "${LOG_COLOR_RED}" "$failed_suites" "${LOG_COLOR_RESET}"
  for name in "${failed_names[@]}"; do
    printf "    - %s\n" "$name"
  done
fi
echo "========================================"

if [[ "$failed_suites" -gt 0 ]]; then
  exit 1
fi
