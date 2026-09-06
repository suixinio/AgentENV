#!/usr/bin/env bash
#
# Rolls a cluster from the cluster-wide egress broker Deployment onto the
# per-node DaemonSet, one step per invocation.
#
# 🔴 Step 4 destroys every sandbox on every node: rolling `ds/agentenv-node`
# replaces the Pods whose children the Firecracker processes are. Nothing else
# here does. Run step 4 in the window a planned node roll already occupies.
#
# Each step is idempotent and prints what it is about to do. Steps are not
# chained on purpose: the order matters, and the operator confirming each one
# is the point.
#
#   NS=agentenv-system ./scripts/roll-egress-per-node.sh <1|2|3|4|5>
#
set -euo pipefail

NS="${NS:-agentenv-system}"
KUBECTL="${KUBECTL:-kubectl}"

step="${1:-}"
if [[ -z "$step" ]]; then
  sed -n '3,15p' "$0" | sed 's/^# \{0,1\}//'
  exit 2
fi

say() { printf '\n== %s\n' "$*"; }

broker_states() {
  # What each node last reported, which is what placement reads.
  "$KUBECTL" -n "$NS" exec deploy/agentenv-api -- \
    sh -c 'curl -sS http://127.0.0.1:8000/nodes' 2>/dev/null |
    jq -r '.[]? | "\(.id)\t\(.egressBroker // "unreported")"' || true
}

case "$step" in
  1)
    say "Step 1: roll the api half, so it knows the new wire values and both credentials."
    say "It must be first: an older api half decodes a node's local_ok as unspecified and"
    say "places no sandbox with rules at all."
    "$KUBECTL" -n "$NS" apply -f deploy/k8s/base/role.yaml -f deploy/k8s/base/rolebinding.yaml
    "$KUBECTL" -n "$NS" rollout restart deploy/agentenv-api
    "$KUBECTL" -n "$NS" rollout status deploy/agentenv-api --timeout=300s
    ;;
  2)
    say "Step 2: apply the broker DaemonSet beside the old Deployment."
    say "Nothing routes to it yet; the nodes are still on their old mode."
    "$KUBECTL" -n "$NS" apply -f deploy/k8s/base/serviceaccount.yaml
    "$KUBECTL" -n "$NS" apply -f deploy/k8s/base/aenv-egress-daemonset.yaml
    "$KUBECTL" -n "$NS" apply -f deploy/k8s/base/aenv-egress-networkpolicy.yaml
    "$KUBECTL" -n "$NS" rollout status ds/aenv-egress --timeout=300s
    ;;
  3)
    say "Step 3: park the node ConfigMap on \`disabled\` before rolling node images."
    say "A node image from before this change refuses \`local\`; one from after refuses"
    say "\`remote\`. \`disabled\` is the only value both understand, and creates with rules"
    say "answer 503 for this window."
    "$KUBECTL" -n "$NS" patch configmap egress-broker-config --type merge \
      -p '{"data":{"AENV_EGRESS_BROKER_MODE":"disabled"}}'
    say "Now roll the node image. This does NOT keep the sandboxes:"
    printf '  %s -n %s rollout restart ds/agentenv-node\n' "$KUBECTL" "$NS"
    ;;
  4)
    say "Step 4: flip the ConfigMap to \`local\` and roll the nodes onto it."
    say "🔴 Every sandbox on every node is destroyed by this roll."
    "$KUBECTL" -n "$NS" patch configmap egress-broker-config --type merge \
      -p '{"data":{"AENV_EGRESS_BROKER_MODE":"local","AENV_EGRESS_BROKER_SOCKET_PATH":"/run/aenv-egress/broker.sock"}}'
    "$KUBECTL" -n "$NS" rollout restart ds/agentenv-node
    "$KUBECTL" -n "$NS" rollout status ds/agentenv-node --timeout=900s
    say "What each node reports now:"
    broker_states
    ;;
  5)
    say "Step 5: retire what the cluster-wide broker needed. Keep it one release."
    say "Nothing below is reversible once the Secrets are gone, so read the list first:"
    printf '  deploy/aenv-egress, svc/aenv-egress, secret/egress-server, secret/egress-hmac, secret/egress-transport-ca\n'
    "$KUBECTL" -n "$NS" scale deploy/aenv-egress --replicas=0 2>/dev/null || true
    say "Delete them only after a release with every node on local_ok:"
    printf '  %s -n %s delete deploy/aenv-egress svc/aenv-egress\n' "$KUBECTL" "$NS"
    printf '  %s -n %s delete secret egress-server egress-hmac egress-transport-ca\n' "$KUBECTL" "$NS"
    ;;
  *)
    printf 'unknown step %q; expected 1..5\n' "$step" >&2
    exit 2
    ;;
esac
