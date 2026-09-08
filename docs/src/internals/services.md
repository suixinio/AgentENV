# Distributed Control Plane

The multi-node control plane's Go half lives in `services/` as a separate Go
module. It routes client traffic across multiple AgentENV backend nodes.

🔴 `services/scheduler` — the original standalone Go implementation of node
selection, sandbox-to-node binding, observed node snapshots, and P2P peer
endpoint discovery — has been deleted. That RPC surface is now answered
in-process by the Rust `aenv-api` binary (`crates/aenv-api/src/node_registry/`); see
CLAUDE.md's "Distributed Control Plane" section for the full picture.

## Components

- **Gateway** (`services/gateway/`): HTTP reverse proxy that routes by sandbox ID — the only Go binary this module ships now
- **aenv-api native registry** (`crates/aenv-api/src/node_registry/`): answers the same gRPC contract in-process; see `docs/src/internals/architecture.md`'s "Distributed Control Plane" section

## Build and Test

Prerequisites: Go 1.21+

```bash
# From services/
make build      # builds gateway
make test       # tests gateway, shared, api
make tidy       # go mod tidy + formatting
make proto      # regenerate protobuf
```

## Run Locally

```bash
# Start gateway
make -C services run-gateway
```

Point `gateway.scheduler_addr` at whichever process answers
`services/api/proto/scheduler.proto` — `aenv-api`'s gRPC listener on every
current deployment.

## Discovery Modes

`aenv-api`'s native registry supports two node discovery modes
(`[cluster].node_discovery_mode`):

- **kubernetes** (default): watches EndpointSlices for a headless Service, using ready Pod IPs as backends
- **static**: explicit node list from `[cluster].static_discovery_nodes` (config-file/overlay only — no environment-variable binding), seeded once at startup with no ongoing watch. `deploy/docker-compose.yml` sets this explicitly, since it has no Kubernetes API to discover against.

## Deployment

### Docker Compose

```bash
make deploy-up      # gateway + agentenv-api (native placement) + 2 backend nodes
make deploy-ps      # status
make deploy-logs    # logs
make deploy-down    # teardown
```

### Kubernetes

```bash
make k8s-render     # render manifests
make k8s-apply      # apply to cluster
```

Deployment model:
- `gateway`: Deployment + ClusterIP Service
- `agentenv-node`: privileged DaemonSet with `/dev/kvm`, one host-compatible
  KVM/PVM mode, and hostPath
- `agentenv-api`: Deployment answering the `Scheduler` RPCs
- `agentenv-nodes`: headless Service for Kubernetes-mode node discovery
- `aenv-egress-node`: unprivileged DaemonSet, one broker per node, reached over
  the `/run/aenv-egress` hostPath both it and `agentenv-node` mount. No Service:
  nothing reaches it over the network.

### Bringing the per-node broker up

`deploy/k8s` describes the **end state** and nothing else: `AENV_EGRESS_BROKER_MODE`
is `local`, and applying the tree is the whole of the move. There is no
migration script and no intermediate value to patch in.

Coming from the cluster-wide broker is a **breaking upgrade**, not a rolling
one. The node image refuses to start on a mode it does not know, and rolling
`ds/agentenv-node` destroys every sandbox on every node it touches, so the
sandboxes go either way:

1. **Drain the sandboxes.** Delete or let expire everything running. Anything
   still up when the nodes roll is lost with its Firecracker process.
2. **Delete the old broker's objects.** They are not in this tree any more and
   `apply` will not remove them:

   ```bash
   NS=agentenv-system
   # Every line carries --ignore-not-found: which of these a cluster has
   # depends on how far it got, and a missing one must not stop the rest —
   # pasted into a `set -e` script, an rc=1 here would leave the old-name
   # DaemonSet below in place.
   kubectl -n $NS delete deploy/aenv-egress svc/aenv-egress cm/aenv-egress-config \
     --ignore-not-found
   kubectl -n $NS delete secret/egress-server secret/egress-hmac secret/egress-transport-ca \
     --ignore-not-found
   kubectl -n $NS delete secret/egress-resolver --ignore-not-found
   # Only on a cluster that ran an interim build: the per-node broker under
   # the old object name, whose selector this tree cannot apply over.
   kubectl -n $NS delete ds/aenv-egress networkpolicy/aenv-egress --ignore-not-found
   ```

   `egress-ca` stays: every broker signs its leaves with it, and the guests'
   trust store comes from it. So does `sa/aenv-egress` — the broker's identity
   did not move with the object family, and the api half's resolve endpoint
   resolves a token to a Pod and a machine, never to a ServiceAccount name.
3. **Apply the end state.** `bash deploy/k8s/run.sh apply` — the file is not
   executable, which is why the Makefile invokes it the same way.
4. **Wait for the three rollouts.**

   ```bash
   kubectl -n $NS rollout status deploy/agentenv-api --timeout=600s
   kubectl -n $NS rollout status ds/aenv-egress-node --timeout=600s
   kubectl -n $NS rollout status ds/agentenv-node --timeout=600s
   ```

   In that order: the api half first, because an older one decodes a node's
   `local_ok` as unspecified and places no sandbox with rules at all; the
   broker before the nodes, because a node in `local` mode refuses to start
   until the broker's init container has prepared `/run/aenv-egress` on its
   machine, and waits sixty seconds before saying so.

**Rolling `ds/agentenv-node` destroys every sandbox on the nodes it rolls.**
That is true of any node roll, not only this one. Rolling `ds/aenv-egress-node`
does not, which is the whole reason the broker is a separate workload: while
its Pod restarts, that node reports `local_unreachable` and takes no new
sandbox with rules, and the connections already open on it fail and are
retried by the guest.

### Rolling the egress broker back

Three workloads, three answers. What each one needs *in place* to run is what
decides whether it can be rolled back on its own. None of them rolls back
across the per-node boundary: the shape before it is not in this tree, and
coming back to it is the runbook above run in reverse, sandboxes drained.

**`aenv-egress-node` (the broker)** — rolling its image back is free, and it takes
no sandbox with it. That is the whole reason it is not a sidecar.

| Needs to be there | Why |
|---|---|
| a broker that answers the readiness probe | the node's own probe writes a zero-length frame and waits for a byte back. A broker from before that answer existed reads the frame and closes, and every node then reports `local_unreachable` and takes no sandbox with rules — free to roll back means free within the versions that answer it |
| a broker that prepares `/run/aenv-egress` | its init container is what chowns the hostPath to gid 65532 with mode 0770. A broker image from before that init container leaves the directory as kubelet made it, and every node on the machine then refuses to start |
| the `egress-ca` Secret mounted with `ca.key`, mode 0440 | it is the broker's only signing key and there is no other source: a Pod without it never starts. 0400 is not enough — `fsGroup` gives the file to root and the Pod's group, and the broker is neither root nor its owner |
| its own projected ServiceAccount token | the only credential the internal endpoints accept. A broker image from before per-node identity presents a shared bearer this half no longer knows, and every lookup answers 401 |

**`agentenv-node`** — rolling it back is **not** free: it destroys every
sandbox on every node it touches. Combine it with a planned node roll.

| Needs to be there | Why |
|---|---|
| a ConfigMap that version can read | `egress-broker-config` holds only environment variables and both images read it, so this one costs nothing — the mode value inside it is the thing to set, in the row below |
| `AENV_EGRESS_BROKER_MODE` the image understands | an image from before the per-node move refuses to start on `local`; one from after refuses on `remote`. There is no value both accept, so rolling between the two is the breaking upgrade above, not a roll |
| `/run/aenv-egress` in the Pod spec, prepared once | a node image expecting `local` with no such volume can never reach a broker. The sixty-second startup refusal is about the *directory*, not the broker: it fires on a machine where nothing ever chowned the hostPath. A node whose broker is merely gone starts normally — the directory the init container prepared outlives the DaemonSet — and reports `local_unreachable` until one comes back |

**`agentenv-api`** — rolling it back is an image change, no flags, and it takes
no sandbox with it. It is also the half that must roll **first** on the way
forward and **last** on the way back.

| Needs to be there | Why |
|---|---|
| a ConfigMap that version can read | this half reads `secrets-store-config` and `agentenv-k8s-config`, neither of which changed shape across this move, so this row costs nothing here |
| the `agentenv-api-token-review` ClusterRole | without it the resolve endpoint answers 503, and every brokered credential lookup fails |
| `EGRESS_BROKER_STATE_LOCAL_OK` in its proto | an older api half decodes a node's `local_ok` as unspecified, places no sandbox with rules, and answers `503` |

## gRPC API

Proto contract: `services/api/proto/scheduler.proto`

RPCs: `Schedule`, `Heartbeat`, `ReportSandboxEvent`, `ListP2pPeers`, `RecordP2pArtifact`, `ForgetP2pArtifact`, `LookupP2pArtifact`, `GetNode`, `UnregisterNode`. This is the node-to-api face: `aenv-node` is its only client.

The gateway calls none of them: its control-plane edges are the Redis routing projection, read directly, and `apiproxy.ResumeSandbox` (`services/api/proto/apiproxy/apiproxy.proto`), which answers a running sandbox as it stands, restores a paused one from its snapshot-catalog row (a create under the same id, placed with the row's `origin_node_id` as a preference), and writes the projection back. The sandbox lookup and the projection writes behind that surface are in-process methods on `NodeRegistryGrpcService` (`lookup_sandbox`, `record_running`, `record_assignment`), not RPCs; `/nodes` and `/registry/sandboxes` read the registry in-process on `aenv-api`'s REST surface.

Runtime node heartbeats may include an opaque `P2pEndpoint` containing a backend name and backend-specific address. `aenv-api`'s native registry stores that endpoint with the observed-node record and returns ready peers through `ListP2pPeers(cluster_id, backend, exclude_node_id)`. It does not query artifact catalogs and never forwards artifact data.

## Metric Name Cross-Walk

`aenv-api` exports the node-registry metrics under `agentenv_api_*`. Dashboards
and alerts written against the deleted Go scheduler's names need this mapping:

| Deleted `services/scheduler` name | Current `aenv-api` name |
| --- | --- |
| `agentenv_scheduler_observed_nodes` | `agentenv_api_node_registry_observed_nodes` |
| `agentenv_scheduler_lookup_node_total` | `agentenv_api_lookup_node_total` |
| `agentenv_scheduler_lookup_execution_authority_total` | `agentenv_api_lookup_execution_authority_total` |
| `agentenv_scheduler_binding_execution_total` | `agentenv_api_binding_execution_total` |
| `agentenv_scheduler_schedule_duration_seconds` | `agentenv_api_schedule_duration_seconds` |
| `agentenv_scheduler_schedule_assignments_total` | `agentenv_api_schedule_assignments_total` |

Constants live in `crates/aenv-api/src/node_registry/grpc_service.rs`.

For full configuration details (header compatibility, timeouts, logging), see the [services README](https://github.com/kvcache-ai/AgentENV/blob/main/services/README.md).
