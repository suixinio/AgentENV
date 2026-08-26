# Kubernetes (Multi-Node)

Deploy AgentENV across a Kubernetes cluster with a gateway, an api Deployment, and runtime nodes on every worker.

🔴 **阶段四**: `deploy/k8s/base`'s default render no longer includes the
`agentenv-scheduler` Deployment/Service/PodDisruptionBudget — they are
commented out of `kustomization.yaml`'s `resources:`, not deleted, and
`services/scheduler`'s Go source stays in the tree as the rollback target
(one release image tag is still pinned in `kustomization.yaml`'s `images:`
transformer). `--role api` (`agentenv-api-deployment.yaml`) runs with
`[cluster].node_placement_source = "native"` and
`[orchestrator.paused_registry].backend = "postgres"` by default, folding
node discovery, heartbeat receipt, placement, and the paused-sandbox registry
into itself over the shared `[pg]` pool instead of dialling a scheduler
process; the gateway's own `scheduler_addr` points at `agentenv-api:8002`
instead of `agentenv-scheduler:9090` for the same reason. The one RPC that
gap does not close is `ListRegistrySandboxes`
(`services/gateway/internal/registry_list.go`'s debug endpoint) — see
`services/README.md` for the current status and how to bring the scheduler
workload back if you need it. The rest of this page describes the
architecture as it runs today; where it still names `agentenv-scheduler`
directly (the paused-registry migration history below, in particular) that
is describing what the *node* half still needs from that gRPC contract, now
answered by `--role api` rather than a separate process.

## Architecture

| Workload | Kind | Description |
|----------|------|-------------|
| `agentenv-gateway` | Deployment + ClusterIP Service | HTTP reverse proxy for client traffic |
| `agentenv-api` | Deployment (2+ replicas) + ClusterIP Service | User-facing REST, sandbox ownership, and (阶段四, `node_placement_source = "native"`) node discovery/placement/paused-registry — the scheduler's former job, folded in |
| `agentenv-scheduler` | Deployment (single replica) + ClusterIP Service, **not deployed by default since 阶段四** | The original standalone gRPC node selection and sandbox binding service; manifests kept for rollback (see the 阶段四 note above) |
| `agentenv-node` | DaemonSet (privileged) | One runtime Pod per Kubernetes node |
| `agentenv-nodes` | Headless Service | Used for EndpointSlice discovery — by `agentenv-scheduler` when it is deployed, by `agentenv-api`'s own `src/node_registry/kubernetes_discovery.rs` under `node_placement_source = "native"` otherwise |

### Why a DaemonSet for Runtime Nodes

- Each Pod needs host-local access to `/dev/kvm`
- Sandbox networking uses host iptables and network namespaces
- Runtime assets and committed snapshot state are cached per-host at `/var/lib/aenv`

## Prerequisites

- Kubernetes worker nodes with **Linux kernel 6.8+**
- `/dev/kvm` access on every runtime worker
- Runtime Pods run privileged
- Docker
- `build-essential` (`sudo apt install -y build-essential`)
- `kubectl` with Kustomize support
- shared storage across all runtime nodes, using either POSIXFS or OSS

The provided manifests use standard KVM. To prepare a separate PVM node pool
when standard KVM is unavailable, see [PVM Deployment](./pvm.md).

## Clone the Repository
```bash
git clone https://github.com/kvcache-ai/AgentENV.git
cd AgentENV
```

## Build Container Images

```bash
make k8s-build
```

This builds three images: `agentenv-runtime:latest`, `agentenv-gateway:latest`, and `agentenv-scheduler:latest`.

## Configure the Access-Token Seed (Optional)

See [Secure Sandboxes](../security/secure-sandboxes.md) for the optional shared seed configuration and Kubernetes Secret example.

## Deploy

```bash
# Run on each worker node before deploying
sudo bash scripts/docker-setup.sh

# Render manifests (preview)
make k8s-render

# Apply to cluster
make k8s-apply
```

To enable host-based sandbox data-plane URLs, set the shared sandbox proxy
domain variable when rendering or applying manifests:

```bash
SANDBOX_PROXY_DOMAINS=sandbox.example.com make k8s-apply
```

The helper writes this value into the generated ConfigMap and applies it to
both the gateway routing allowlist and runtime nodes' sandbox response metadata.
The domain must resolve to the gateway Ingress or LoadBalancer, usually through
wildcard DNS for `*.sandbox.example.com`.

The three AgentENV images are named in `kustomization.yaml` without a registry
and on the `latest` tag, which is right for a local cluster that loads images
directly and wrong for one that pulls them. Set either or both of
`IMAGE_REGISTRY` and `IMAGE_TAG` when rendering or applying, and the render
rewrites all three:

```bash
IMAGE_REGISTRY=registry.example.com:5000 IMAGE_TAG=sd3a-9a9de13 make k8s-render
```

`IMAGE_REGISTRY` prefixes the image names; `IMAGE_TAG` replaces the tags. One
tag covers all three because they are built together from one commit. Both
substitutions verify themselves and fail the render if they did not take —
without them the symptom is `ImagePullBackOff` on every Pod, one step away from
its cause.

The default overlay is `deploy/k8s/overlays/default`, targeting the `agentenv-system` namespace. The gateway is exposed as ClusterIP by default. Add your own Ingress or LoadBalancer for external access.

The make targets build a temporary Kustomize context so runtime Pods mount the repository's `config/default.toml` rather than a separate checked-in copy.

The runtime DaemonSet injects scheduler-report wiring for each node Pod:

- `AENV_UBLK_DAEMON_BINARY_PATH=/usr/local/bin/uvm-ublk-daemon` so the Pod uses the `uvm-ublk-daemon` binary included in the runtime image
- `AENV_NODE_ID` from the node the Pod is on (`fieldRef: spec.nodeName`), not the Pod's own name — a paused sandbox's registry row records it as the holder, and it has to survive the Pod being replaced
- `AENV_OBSERVABILITY_SCHEDULER_REPORT_ENABLED=true`
- `AENV_OBSERVABILITY_SCHEDULER_ENDPOINT=http://agentenv-scheduler:9090`
- `AENV_SANDBOX_PROXY_DOMAINS` from the shared sandbox proxy ConfigMap

The P2P listen address must be reachable Pod-to-Pod; use a concrete container port or a Pod-reachable address if your cluster policy does not allow dialing ephemeral ports.

## The API Half

`agentenv-api` is the deciding half of the split control plane: user-facing REST,
sandbox ownership, placement. It runs the same `agentenv-runtime` image as the
node DaemonSet with `--role api`, on an ordinary Deployment with no `/dev/kvm`,
no host paths and no privileges. It needs
`agentenv-runtime-secrets/sandbox-access-token-hash-seed` to exist — the
reference is not optional, because an API replica that invents its own seed
mints envd tokens the other replica cannot derive, and that failure is silent.
The `default` overlay does not create that Secret; create it once before the
first apply, or the api Pods stop at `CreateContainerConfigError`:

```bash
kubectl -n agentenv-system create secret generic agentenv-runtime-secrets \
  --from-literal=sandbox-access-token-hash-seed="$(openssl rand -hex 32)"
```

Whether the value actually reached every process is a separate question from
whether it was set, and `agentenv_access_token_seed_fingerprint{fingerprint=...}`
is where it is answered: every role publishes it at startup, the label is the
first eight bytes of `SHA-256(seed)`, and two processes that hold the same seed
report the same label. Two replicas each configured with a *different* non-empty
seed pass every startup check there is, so comparing this label across the
replicas is the only place that divergence shows up.

Bringing the Deployment up does not move any traffic. Two switches do, and both
are read by the gateway:

| Switch | Read by | Flipping it costs |
|--------|---------|-------------------|
| `GATEWAY_REST_UPSTREAM_ADDR` (`api-upstream-config`) | the gateway | a gateway roll, seconds |
| `GATEWAY_RESUME_ADDR` (`api-upstream-config`) | the gateway | a gateway roll, seconds |

Point the gateway at `http://agentenv-api:8000` and `agentenv-api:8002`. Both
live in one ConfigMap and ride one gateway roll, so there is no ordering to get
right and no preparatory step to take first.

🔴 **There is no node-side switch to throw beforehand.** Earlier revisions of
this page opened with `AENV_NODE_SERVICE_ENABLED` (`node-service-config`), billed
as the first step and costed at a serial DaemonSet roll with a drain per machine.
No AgentENV code has ever read that variable, so setting it changed nothing; the
ConfigMap and the DaemonSet reference are both gone, and
`no_manifest_sets_a_node_service_gate_nothing_reads` (`src/cfg.rs`) fails if
either comes back. What the switch was supposed to buy — a node serving the gRPC
surface the API half drives it through — comes from running the DaemonSet with
`--role node`. `--role all` deliberately does not bind that listener: it is the
rollback target and is defined as the pre-split process verbatim, which
`only_the_split_roles_bind_a_second_listener` (`src/bin/server.rs`) asserts.

🔴 **Rolling back is the two gateway switches.** Emptying them puts every REST
call back on the nodes, which never stopped being able to serve them, and the API
half stops driving machines the moment it stops receiving REST — so the rollback
is one ConfigMap and one gateway roll, seconds, with the DaemonSet untouched.

🔴 **Do not turn a gateway switch on by editing `config/gateway.json`.** An
environment variable set to the empty string is ignored by the loader, so a
value that lives in the file cannot be cleared from the environment — and the
rollback above would stop working. Keep the file's values empty and drive both
switches from `api-upstream-config` or `kubectl set env`.

The API half reaches a node's gRPC service by substituting
`AENV_NODE_SERVICE_PORT` into the address the scheduler gives it, which is the
node Pod's own IP. No Service fronts that port, and none should: every one of
those calls is addressed to one named machine, and a ClusterIP would
load-balance them across the fleet.

## Cluster-wide Paused Sandboxes

By default a paused sandbox is resumable only on the node that paused it. Pointing the nodes at the cluster-wide registry makes a pause publish its snapshot to the shared repository as well, so any node can resume the sandbox under its original ID.

The registry lives in PostgreSQL and the **scheduler** owns it. Nodes reach it over gRPC; no node holds database credentials, a connection, or any say over the schema. Enabling it takes one object:

```bash
kubectl -n agentenv-system create configmap paused-registry-config \
  --from-literal=AENV_PAUSED_REGISTRY_BACKEND=central
```

The setting reaches the DaemonSet through an optional reference, so a cluster without it starts normally on the node-local default.

🔴 **Do not select the backend by editing the `agentenv-k8s-config` ConfigMap.** The make targets rebuild that ConfigMap from `config/default.toml` on every apply, so an edit there is undone by the next `make k8s-apply` — quietly, and in the direction that loses cross-node recovery: pauses go back to being node-local and nothing reports an error until a node is lost and its sandboxes turn out to have gone with it. `AENV_PAUSED_REGISTRY_BACKEND` exists so the choice lives somewhere the file cannot overwrite it.

`central` needs `[cluster].scheduler_endpoint` (`AENV_OBSERVABILITY_SCHEDULER_ENDPOINT`, already set on the DaemonSet) and refuses to start without one rather than falling back. Reclaiming holdings from nodes that never came back is the scheduler's own timer, not something a node asks for.

Each node reports the backend it assembled, once, at startup:

```
INFO paused sandbox registry ready backend=central cluster_id=… lease_ttl_secs=90 scheduler_endpoint=http://agentenv-api:8002
```

That line is how a rollout is confirmed. A value `AENV_PAUSED_REGISTRY_BACKEND` does not recognise stops the node rather than falling back, but a value that never reached the Pod at all — a ConfigMap that was not created, a key spelled differently — leaves it on `local` with nothing else to say so, and node-local pauses only reveal themselves when a node is lost.

### Cluster identity

Both sides of the registry are scoped to one cluster id, and both read it from the same generated key, `cluster-identity-config/CLUSTER_ID`: the node as `AENV_CLUSTER_ID`, the scheduler as `SCHEDULER_REGISTRY_CLUSTER_ID`. Change it in the one place it is written, the `configMapGenerator` literal in `deploy/k8s/base/kustomization.yaml`, and keep `[node_identity].cluster_id` in `config/default.toml` in step with it — that is what a node falls back to if the ConfigMap is ever absent.

Two different values are not an error anywhere: rows are written, RPCs succeed, and the scheduler serves a cluster that has no rows while nobody reclaims the ones the nodes leave behind. A cluster id is a name rather than a credential, which is why it is not in a Secret — it used to be, on a key nothing ever created, and the result was a scheduler whose registry write surface was permanently cold on every fresh cluster while `/healthz` and the gRPC probe both said it was fine.

The scheduler reports the scope it is running with on its metrics listener:

```bash
curl -s localhost:9101/healthz | jq .registry_write
# { "phase": "serving", "ready": true, "serving": true, "cluster_id": "…", … }
```

An empty `cluster_id` there means the write surface is registered and cold, answering every registry RPC `UNAVAILABLE` until one is supplied.

### Migrating from the removed `postgres` backend (nodes only)

🔴 This subsection is about `--role node` specifically — `--role node` never
holds a `[pg]` DSN and never will (`ServerRole::check_pg_dsn` refuses
startup outright if one is configured). It does **not** describe `--role
api`/`--role all`: 阶段四 gave those two roles a *different*, still-live
`postgres` paused-registry backend that connects to the shared `[pg]` pool
directly and folds the scheduler's own registry service into the process —
`deploy/k8s/base/agentenv-api-deployment.yaml` runs it by default. See
CLAUDE.md's paused-registry note and `[orchestrator.paused_registry]` in
`config/default.toml` for that backend; nothing below applies to it.

Nodes used to connect to the registry database themselves, under `AENV_PAUSED_REGISTRY_BACKEND=postgres`. That backend has been **removed for nodes**, and a node still configured with it refuses to start rather than guessing:

```
paused_registry.backend = "postgres" has been removed: the node no longer connects to the
registry database. Set AENV_PAUSED_REGISTRY_BACKEND=central and point
AENV_OBSERVABILITY_SCHEDULER_ENDPOINT at the scheduler, which owns the database now;
the node's own DSN secret can then be dropped
```

Refusing is deliberate. Reading it as `local` would put the fleet back to node-local pauses — the silent failure this setting exists to prevent — and reading it as `central` would point the node at whatever endpoint happened to be configured, including none.

To migrate:

```bash
# 1. The scheduler must already own the table: SCHEDULER_REGISTRY_DSN set,
#    SCHEDULER_REGISTRY_WRITE_ENABLED=true, and a cluster id supplied.
curl -s localhost:9101/healthz | jq .registry_write   # phase must be "serving"

# 2. Switch the nodes.
kubectl -n agentenv-system create configmap paused-registry-config \
  --from-literal=AENV_PAUSED_REGISTRY_BACKEND=central \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl -n agentenv-system rollout restart daemonset/agentenv-node

# 3. Drop the credentials the nodes no longer need.
kubectl -n agentenv-system patch secret agentenv-runtime-secrets \
  --type=json -p '[{"op":"remove","path":"/data/paused-registry-dsn"}]'
```

The table itself does not change: both backends wrote the same schema and arbitrate through the same generation column, so the rows a `postgres` fleet left behind are the rows a `central` fleet reads. What changes is who may write them.

🔴 **The nodes and the scheduler in this release go out together.** Deleting a
registry row is a conditional write now, and the two sides disagree about it in
both directions: a node from before this release asks for the unconditional
delete, which the scheduler refuses outright, and a node from after it asks for
a kind an older scheduler does not serve. Neither direction corrupts anything —
a refused delete leaves the row and its snapshot in place, which is the safe
side — but during a mixed window `DELETE /sandboxes/{id}` will not clear the
cluster record. Roll the DaemonSet and the Deployment in the same change.

🔴 **Verify the switch on each node** with the assembly line above — `backend=central` with a non-empty `scheduler_endpoint`. A value that never reached the Pod at all (a ConfigMap that was not created, a key spelled differently) leaves the node on `local` with nothing else to say so, and node-local pauses only reveal themselves when a node is lost.

## The node API is protected by the network, not by its headers

🔴 **Port 8000 on a node Pod is an unauthenticated admin surface.** The API
declares `X-Admin-Token` and `X-API-Key`, and the node accepts *any* non-empty
value for either — presence is checked, validity is not. A made-up token is
enough to put a node into `DRAINING` through `POST /nodes/{id}`.

This matches where e2b puts the same boundary: its orchestrator's gRPC server
has no authentication interceptor, because only the control plane is supposed to
be able to reach it. The consequence is the same for us — **whatever can reach
port 8000 can administer the node**, and the headers will not tell you
otherwise.

So the check belongs on the boundary:

```bash
# Nothing should expose the node API beyond the cluster.
kubectl -n agentenv-system get svc -o wide | grep -i nodeport
# And the node's port should be reachable only from the gateway and scheduler.
kubectl -n agentenv-system get networkpolicy
```

A NodePort on the node service, or a cluster without a NetworkPolicy in front of
it, is what turns this from a design choice into an exposure.

## Operations

```bash
# Rollout restart all workloads
make k8s-redeploy

# Delete all resources
make k8s-delete
```

## Local Development (k3s)

A dedicated `local-dev` overlay mounts the repository's `env/` directory directly into the DaemonSet at `/workspace/env`, avoiding runtime asset copies:

This overlay also generates `agentenv-runtime-secrets` with a fixed test-only
seed so local and E2E deployments do not require production secret management.
Do not reuse that value outside local development.

```bash
make k8s-build
make k8s-load-dev       # Import images into k3s/containerd
make k8s-render-dev     # Preview manifests
make k8s-apply-dev      # Apply to cluster
make k8s-refresh-dev    # Build + load + rollout restart (all-in-one)
```

## Service Discovery

The scheduler watches EndpointSlices for the headless `agentenv-nodes` Service and watches Pods for optional label-based discovery policy. It schedules only serving, non-terminating DaemonSet Pods. Pods matching `scheduler.discovery.kubernetes.no_schedule_pod_selector` stay discoverable as lingering/no-schedule nodes, while Pods matching `scheduler.discovery.kubernetes.ignore_pod_selector` are excluded. Both IPv4 and IPv6 endpoint addresses are supported.

> Sandbox bindings remain in-memory, so the scheduler should run as a single replica. Bindings are lost on restart.
