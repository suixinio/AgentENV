# Kubernetes (Multi-Node)

Deploy AgentENV across a Kubernetes cluster with a gateway, an api Deployment, and runtime nodes on every worker.

🔴 **阶段四**: the Go Scheduler — `services/scheduler`, and the
`agentenv-scheduler` Deployment/Service/PodDisruptionBudget that ran it — has
been deleted. `aenv-api` (`agentenv-api-deployment.yaml`) always answers node
discovery/placement/heartbeat from its own in-process node registry now (the
`[cluster].node_placement_source` switch that used to select this is deleted
too — there is no alternative left to choose), and runs with
`[orchestrator.paused_registry].backend = "postgres"` by default, folding
node discovery, heartbeat receipt, placement, and the paused-sandbox registry
into itself over the shared `[pg]` pool instead of dialling a scheduler
process; the gateway's own `scheduler_addr` points at `agentenv-api:8002`
instead of `agentenv-scheduler:9090` for the same reason, including for
`ListRegistrySandboxes` (`services/gateway/internal/registry_list.go`'s
debug endpoint), which `aenv-api` now answers too — and also serves directly as
`GET /registry/sandboxes` on its own REST surface — see `services/README.md`
for the current status. The rest of this page describes the architecture as
it runs today; where it still names "the scheduler" or "the Scheduler
protocol" generically, that is describing the `services/api/proto/scheduler.proto`
gRPC contract itself, which `aenv-api` answers in-process rather than a
separate Go process.

## Architecture

| Workload | Kind | Description |
|----------|------|-------------|
| `agentenv-gateway` | Deployment + ClusterIP Service | HTTP reverse proxy for client traffic |
| `agentenv-api` | Deployment (2+ replicas) + ClusterIP Service | User-facing REST, sandbox ownership, and (阶段四) node discovery/placement/paused-registry — the Go scheduler's former job, folded in, unconditionally |
| `agentenv-node` | DaemonSet (privileged) | One runtime Pod per Kubernetes node |
| `agentenv-nodes` | Headless Service | Used for EndpointSlice discovery, by `agentenv-api`'s own `src/node_registry/kubernetes_discovery.rs` |

### Two Client Addresses

A client talks to two addresses, not one:

| Address | Service | Carries |
|---------|---------|---------|
| REST entry point | `agentenv-api:8000` | every user-facing REST call — sandboxes, snapshots, templates, `/nodes`, `/registry/sandboxes` |
| Data-plane entry point | `agentenv-gateway:8080` | sandbox traffic, addressed by Host (`{port}-{sandboxID}.<domain>`) or by the `x-agentenv-sandbox-id` / `x-agentenv-target-port` headers |

The repository does not presume how these map to names outside the cluster.
Both Services are ClusterIP; an Ingress, a LoadBalancer or a port-forward per
address is a deployment decision, and the only requirement is that each address
is reachable in full.

The `aenv` client names them as `url` and `proxy_url` in its credentials file.
`proxy_url` is optional and falls back to `url`, so a client configured with a
single address that points at the gateway keeps working unchanged: the gateway
still forwards REST to `agentenv-api` (`GATEWAY_REST_UPSTREAM_ADDR`, below).
Point new configuration at both addresses; that forwarding is transitional.

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

This builds three images: `agentenv-runtime:latest`, `agentenv-api:latest`, and `agentenv-gateway:latest`.

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

The default overlay is `deploy/k8s/overlays/default`, targeting the `agentenv-system` namespace. Both client-facing Services — `agentenv-api` for REST and `agentenv-gateway` for sandbox traffic — are ClusterIP by default. Add your own Ingress or LoadBalancer for external access to each.

The make targets build a temporary Kustomize context so runtime Pods mount the repository's `config/default.toml` rather than a separate checked-in copy.

The runtime DaemonSet injects heartbeat-report wiring for each node Pod:

- `AENV_UBLK_DAEMON_BINARY_PATH=/usr/local/bin/uvm-ublk-daemon` so the Pod uses the `uvm-ublk-daemon` binary included in the runtime image
- `AENV_NODE_ID` from the node the Pod is on (`fieldRef: spec.nodeName`), not the Pod's own name — a paused sandbox's registry row records it as the holder, and it has to survive the Pod being replaced
- `AENV_OBSERVABILITY_SCHEDULER_REPORT_ENABLED=true`
- `AENV_OBSERVABILITY_SCHEDULER_ENDPOINT=http://agentenv-api:8002`
- `AENV_SANDBOX_PROXY_DOMAINS` from the shared sandbox proxy ConfigMap

The P2P listen address must be reachable Pod-to-Pod; use a concrete container port or a Pod-reachable address if your cluster policy does not allow dialing ephemeral ports.

## The API Half

`agentenv-api` is the deciding half of the split control plane: user-facing REST,
sandbox ownership, placement. It runs its **own** image, `agentenv-api`, built
from the `aenv-api` crate — a different binary from the node DaemonSet's, not
the same one under a flag. `aenv-api` links no sandbox runtime at all
(`make check-crate-boundaries`), so it runs on an ordinary Deployment with no
`/dev/kvm`, no host paths and no privileges. It needs
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
is where it is answered: both halves publish it at startup, the label is the
first eight bytes of `SHA-256(seed)`, and two processes that hold the same seed
report the same label. Two replicas each configured with a *different* non-empty
seed pass every startup check there is, so comparing this label across the
replicas is the only place that divergence shows up.

A client that names the REST address directly reaches this Deployment without
any gateway involvement. One switch decides where REST arriving at the *gateway*
is sent instead, and the gateway reads it:

| Switch | Read by | Flipping it costs |
|--------|---------|-------------------|
| `GATEWAY_REST_UPSTREAM_ADDR` (`api-upstream-config`) | the gateway | a gateway roll, seconds |

Point the gateway at `http://agentenv-api:8000`. It rides one gateway roll, so
there is no ordering to get right and no preparatory step to take first. It
exists so a client that still names one address keeps working; a client
configured with both addresses never uses it.

🔴 This used to be a pair: `GATEWAY_RESUME_ADDR` sat beside it in the same
ConfigMap, naming the api half's gRPC wake-up surface at `agentenv-api:8002`.
That key is deleted. `SandboxResumeService` and the `Scheduler` service share
one gRPC listener on `agentenv-api`, so the key could only ever hold
`GATEWAY_SCHEDULER_ADDR`'s value, and the gateway now reuses that connection
for the wake-up RPC instead of opening a second one to the same process. A
manifest that still sets `GATEWAY_RESUME_ADDR` is ignored, not refused — the
loader reads no such key.

🔴 **There is no node-side switch to throw beforehand.** Earlier revisions of
this page opened with `AENV_NODE_SERVICE_ENABLED` (`node-service-config`), billed
as the first step and costed at a serial DaemonSet roll with a drain per machine.
No AgentENV code has ever read that variable, so setting it changed nothing; the
ConfigMap and the DaemonSet reference are both gone, and
`no_manifest_sets_a_node_service_gate_nothing_reads` (`src/cfg.rs`) fails if
either comes back. What the switch was supposed to buy — a node serving the gRPC
surface the API half drives it through — is simply what `aenv-node` does: it
binds that listener unconditionally.

🔴 **Rolling back is an image tag, not a flag.** There is no `--role` flag and
no `AENV_ROLE`; a manifest that still passes one is refused by argument parsing
before the process starts, which is deliberate — an un-migrated manifest fails
loudly instead of being ignored. Each half rolls back by deploying an earlier
digest of its own image; see `services/README.md`. Going back to the pre-split
single-process shape is not a supported rollback any more —
`docs/proposals/2026-08-31-residue-decisions.md` §D3 records what it required
and why it is retired.

🔴 **Do not turn a gateway switch on by editing `config/gateway.json`.** An
environment variable set to the empty string is ignored by the loader, so a
value that lives in the file cannot be cleared from the environment — and
losing `api-upstream-config` would then fall back to a real address in the
file instead of the fail-fast refusal that emptying it is meant to produce.
Keep the file's values empty and drive both switches from `api-upstream-config`
or `kubectl set env`.

For `GATEWAY_REST_UPSTREAM_ADDR` this holds only while the gateway forwards
REST at all: the key goes away once clients name the REST address themselves.
The rule stands for `GATEWAY_SCHEDULER_ADDR`, which carries the wake-up RPC and
is part of the gateway's permanent surface.

The API half reaches a node's gRPC service by substituting
`AENV_NODE_SERVICE_PORT` into the address the scheduler gives it, which is the
node Pod's own IP. No Service fronts that port, and none should: every one of
those calls is addressed to one named machine, and a ClusterIP would
load-balance them across the fleet.

## Cluster-wide Paused Sandboxes

By default a paused sandbox is resumable only on the node that paused it — the `"local"` backend. Cluster-wide resume publishes a pause's snapshot to the shared repository and records the sandbox cluster-wide, so any node can resume it under its original ID.

🔴 **This is now entirely an `aenv-api`-side setting; there is no per-node opt-in step.** Through 阶段三 this was a *node*-side choice (`AENV_PAUSED_REGISTRY_BACKEND=central` via a `paused-registry-config` ConfigMap, each node dialling the standalone Go scheduler's own registry service over gRPC). 阶段四 replaced that with a `"postgres"` backend that lives entirely on `aenv-api`, and the node-side switch stopped doing anything:

- The `paused-registry-config` ConfigMap and the `AENV_PAUSED_REGISTRY_BACKEND` key on the DaemonSet are gone from `deploy/k8s/base` (`02117b9`).
- `aenv-node`'s `assemble_node` (`crates/aenv-node/src/bin/aenv-node.rs`) ignores `[orchestrator.paused_registry].backend` whenever it is anything other than `"local"`: it logs one `warn!` naming the configured value and wires in a registry that claims and records nothing, because cluster-wide paused-sandbox state belongs to the API half alone now. A node never refuses to start over this setting — only over a configured `[pg].dsn` (`refuse_configured_pg_dsn`), which a node must never hold regardless of this backend.
- Cluster-wide resume is controlled entirely by `aenv-api`'s own `[orchestrator.paused_registry].backend = "postgres"`. `deploy/k8s/base/agentenv-api-deployment.yaml` sets `AENV_PAUSED_REGISTRY_BACKEND=postgres` unconditionally; that backend requires a heartbeat-roster `NodeRegistry` handle (`build_paused_registry`), which `aenv-api` always builds now, with no separate switch to keep in step.
- `aenv-api` reaches PostgreSQL directly over the shared `[pg]` pool — the same pool the snapshot catalog uses — instead of dialling a separate scheduler process. No node ever holds a `[pg]` DSN, a database connection, or any say over the schema.

In short: on the `deploy/k8s/base` overlay, cluster-wide paused-sandbox resume is on by default and there is no ConfigMap toggle or kubectl step left to run to enable it. See `config/default.toml`'s `[orchestrator.paused_registry]` comment block (the authoritative description of `"local"`/`"postgres"`) and CLAUDE.md's "Distributed Control Plane" section.

🔴 **`"central"` is gone, not merely unused.** It was already dead on every current deployment when this doc first described the 阶段四 switch above — the Go scheduler it dialled no longer exists in this repository — and the cleanup that followed removed `PausedRegistryBackendKind::Central` and its implementation (`CentralPausedSandboxRegistry`) from the code entirely: `AENV_PAUSED_REGISTRY_BACKEND=central` is now a startup-refusing typo like any other unrecognised value, not a legal-but-unreachable option. Both node and api Deployments read the same `cluster-identity-config/CLUSTER_ID` key as `AENV_CLUSTER_ID` today (`[node_identity].cluster_id` in `config/default.toml` is what a node falls back to if that ConfigMap is absent); there is no longer a second, scheduler-specific cluster-id variable.

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
# And the node's port should be reachable only from the gateway and the api half.
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

`agentenv-api`'s own
`src/node_registry/kubernetes_discovery.rs` watches EndpointSlices for the
headless `agentenv-nodes` Service and watches Pods for optional label-based
discovery policy — a port of the same mechanism the deleted Go scheduler
used, configured via `AENV_CLUSTER_KUBERNETES_DISCOVERY_*` (see
`config/default.toml`'s `[cluster]` section) rather than
`scheduler.discovery.kubernetes.*` JSON keys. See that module's own doc
comment for the current state of `ignore_pod_selector`/
`no_schedule_pod_selector` support. Both IPv4 and IPv6 endpoint addresses are
supported.
