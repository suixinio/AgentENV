# services

Go implementation of a distributed Gateway and pluggable Scheduler for AgentENV.

🔴 **阶段四 status**: the Scheduler half of this module is **not deployed by
default** on `deploy/k8s/base` any more — `docs/proposals/2026-08-20-service-decomposition.md`'s
phase four has folded node discovery, heartbeat receipt (including the
cluster CPU-config intersection), placement, P2P peer lookup, and the
cluster-wide paused-sandbox registry into the Rust `aenv-api` binary
(`src/node_registry/`, `crates/aenv-api/src/orchestrator/paused_registry/postgres/`),
reachable over the same `services/api/proto/scheduler.proto` contract this
package still generates from. `agentenv-scheduler`'s Deployment/Service/PDB
are commented out of `deploy/k8s/base/kustomization.yaml`'s `resources:`
(not deleted — see "Deploy on Kubernetes" below for how to bring them back),
and the Gateway's own `scheduler_addr` now points at `agentenv-api` instead
of `agentenv-scheduler`.

This Go source is **kept, not deleted**, as the rollback target: everything
below still describes real, working, tested code, and `make -C services
test` / `test-with-postgres` still exercise it. What has changed is only
which process answers the RPCs on a deployed cluster. `ListRegistrySandboxes`
(`internal/registry_list.go`'s gateway-facing debug endpoint) was the last RPC
group the Rust side had not ported; `src/node_registry/grpc_service.rs`'s
`list_registry_sandboxes` now answers it too, against
`PausedSandboxRegistry::list_all` (`src/orchestrator/paused_registry/mod.rs`),
so no RPC on this contract still requires a real scheduler process reachable
at `gateway.scheduler_addr` on the default deploy.

## Features

- Gateway routes control-plane requests by real-time scheduling.
- Gateway aggregates `GET /sandboxes` and `GET /v2/sandboxes` across all scheduler nodes.
- Gateway aggregates `GET /nodes` across all observed nodes in the scheduler.
- Gateway resolves `GET /nodes/{id}` via scheduler and proxies to the target node.
- Gateway routes sandbox requests to whichever node the scheduler names, in a single `LookupNode` call. The scheduler answers from the sandbox-to-node binding, from the heartbeat roster that seeded it, or — when neither knows the sandbox — from the paused registry the nodes maintain among themselves, and says which of the three it used.
- Scheduler exposes gRPC API and supports pluggable strategy providers.
- Built-in strategies in v1: round_robin and random.
- Scheduler supports both static node configuration and Kubernetes EndpointSlice discovery.
- Scheduler sandbox binding store can be in-memory or Redis-backed.
- Scheduler can run as a primary read/write service or as query-only replicas that serve sandbox data-plane lookups from Redis.
- Scheduler observes node health and sandbox roster from heartbeats, and drops expired sandbox-to-node bindings on heartbeat, node unregistration, or lookup.
- HTTP and WebSocket forwarding.

## Header compatibility

Gateway treats these headers as sandbox-routing markers:

- x-agentenv-sandbox-id
- e2b-sandbox-id

If one of them exists, gateway resolves node from scheduler binding and forwards request there.

When `gateway.sandbox_proxy_domains` is configured, gateway also accepts host-based sandbox
data-plane URLs in the form `{port}-{sandboxID}.{proxy_domain}`. The host-derived
sandbox ID and port take precedence over conflicting routing headers; the gateway logs
that conflict at debug level and forwards the request to the backend node's `/proxy`
endpoint. Host-based routing requires the sandbox ID to be RFC 952/1123 DNS-label compatible
(`[a-z0-9]([a-z0-9-]*[a-z0-9])?`), and the full `{port}-{sandboxID}` label must fit
within the 63-character DNS label limit.

Sandbox data-plane routing is host- or header-based. Path-derived sandbox IDs are
only used for sandbox control-plane APIs such as `/sandboxes/{id}/pause`; clients
that proxy sandbox traffic through the gateway must use a sandbox proxy host or a
sandbox routing header.

## Build

Prerequisites:

- Go 1.21+

Commands (from `services/`):

```bash
make tidy
make proto
make build              # builds both gateway and scheduler
make test               # tests the whole module: gateway, scheduler, shared, api
make test-with-postgres # the same, against a throwaway PostgreSQL: nothing skips
```

The paused-sandbox registry's behaviour is its SQL — which rows a predicate
matches and which it deliberately does not — so none of it can be checked
without a real database. `make test` on its own therefore skips 125 tests in
`scheduler/internal/registry`, and a skip reports as a pass.

`make test-with-postgres` is the coverage CI has. It starts a throwaway
PostgreSQL in Docker, points `SCHEDULER_REGISTRY_TEST_DSN` at it, and sets
`SCHEDULER_REGISTRY_TEST_REQUIRED=1` so that a database which fails to come up
fails the run instead of quietly restoring the skips. `REDIS_SERVER_BIN` and
`SCHEDULER_REDIS_TEST_REQUIRED=1` do the same for the `RedisBindingStore`
tests, which otherwise skip when `redis-server` is not installed. Override
`REGISTRY_TEST_PORT` if 15499 is taken.

🔴 Both redis variables matter, and the second one more than it looks. The
binding-store tests are the only ones that exercise the Redis implementation of
sandbox-to-node bindings, and that implementation is what every HA deployment
runs. A change made to the in-memory store and forgotten for Redis passes every
other test in the module, on any machine, and shows up only in production — as
routing that quietly stops arbitrating. A skip reports as a pass, so without
`SCHEDULER_REDIS_TEST_REQUIRED` a missing `redis-server` and a healthy run look
identical.

To point the suite at a database you already have, set the same two variables
by hand:

```bash
SCHEDULER_REGISTRY_TEST_DSN=postgres://postgres:verify@127.0.0.1:15499/aenv_registry \
SCHEDULER_REGISTRY_TEST_REQUIRED=1 \
SCHEDULER_REDIS_TEST_REQUIRED=1 \
REDIS_SERVER_BIN="$(command -v redis-server)" \
  go test ./scheduler/internal/registry/ ./scheduler/internal/
```

Per-service (from `services/gateway/` or `services/scheduler/`):

```bash
make build
make test
```

Each per-service `test` covers `shared/` and `api/` as well as its own tree,
which is the package set its `vet` and `fmt-check` already check.

## Run locally

Start scheduler:

```bash
make run-scheduler
```

Start gateway:

```bash
make run-gateway
```

The default local config uses `127.0.0.1:9090` for the scheduler.

## Scheduler configuration

Scheduler discovery modes:

- `static` (default): use `scheduler.nodes` from config.
- `kubernetes`: watch EndpointSlices for a headless Service and build the node list from serving Pod endpoints. Terminating endpoints, or Pods matching `no_schedule_pod_selector`, are kept as lingering/no-schedule nodes; Pods matching `ignore_pod_selector` are excluded.

General config notes:

- `scheduler.report_ttl` must be a duration string such as `"30s"` in JSON config files.
- `scheduler.binding_ttl` must be a duration string such as `"30s"` in JSON config files.
- `scheduler.report_ttl` controls how long an observed node heartbeat stays healthy.
- `scheduler.binding_ttl` controls how long sandbox-to-node bindings survive without a fresh `RecordAssignment` or heartbeat roster refresh.
- `scheduler.warmup_timeout` bounds how long a freshly started scheduler withholds `NotFound` for an unknown sandbox while its bindings are still being seeded by node heartbeats; defaults to `"15s"`. During that window a miss is reported as `Unavailable` (the gateway turns it into a 503) instead of `NotFound`, so traffic to live sandboxes does not 404. A sandbox that has never been paused has no paused-registry row by design, so it can only ever be found through a binding or a roster, which is what makes this window matter. The window ends early as soon as every discovered node has delivered a heartbeat.
- `scheduler.redis_addr` selects Redis-backed sandbox binding storage when set; when empty, the scheduler uses the in-memory binding store. It accepts either `host:port` or a Redis URL such as `redis://[:password@]host:6379/db`.
- `--query-only` starts a read-only scheduler that serves `LookupNode` and `ListRegistrySandboxes`; it requires `scheduler.redis_addr` and does not need node discovery config. It runs no discovery and receives no heartbeats, so it answers from bindings and reports `Unavailable` for a sandbox that only the paused registry knows about — never `NotFound`.
- `scheduler.artifact_store_capacity` controls how many distinct P2P artifact keys the in-memory artifact index keeps before LRU eviction; defaults to `1000000`.
- `scheduler.artifact_lookup_node_limit` controls how many node IDs a P2P artifact lookup returns; values `<= 0` return all matching nodes.
- `SCHEDULER_BINDING_TTL=<duration>` overrides `scheduler.binding_ttl` from the environment.
- `SCHEDULER_WARMUP_TIMEOUT=<duration>` overrides `scheduler.warmup_timeout` from the environment.
- `SCHEDULER_REDIS_ADDR=<addr>` overrides `scheduler.redis_addr` from the environment.
- `SCHEDULER_ARTIFACT_STORE_CAPACITY=<count>` overrides `scheduler.artifact_store_capacity` from the environment.
- `SCHEDULER_ARTIFACT_LOOKUP_NODE_LIMIT=<count>` overrides `scheduler.artifact_lookup_node_limit` from the environment.

### Scheduling strategy

`scheduler.strategy` selects the algorithm used to pick a node from the eligible candidate list. Built-in strategies:

| Strategy | Behaviour |
|---|---|
| `round_robin` (default) | Cycles through eligible nodes in stable order |
| `random` | Picks a uniformly random eligible node |

The strategy interface receives `RichNode` values that carry the node identity (ID + endpoint) together with the latest heartbeat `NodeSnapshot` (sandbox counts, CPU, memory, disk metrics). Current built-in strategies ignore the snapshot, but custom strategy implementations can use it for load-aware decisions.

### Node resource limit

`scheduler.node_resource_limit` defines per-node resource thresholds that are evaluated **before** the strategy runs. Any node whose heartbeat snapshot exceeds a configured limit is removed from the candidate list, regardless of which strategy is in use. This is a generic guard-rail that sits above the strategy layer — strategies only see nodes that already passed the resource filter.

Nodes that have not yet sent a heartbeat (no snapshot available) are always kept in the candidate list, since there are no metrics to evaluate.

All fields are optional. Omitting a field (or setting the whole block to `null`) disables that particular check.

| Field | Type | Description |
|---|---|---|
| `max_sandbox_count` | uint32 | Maximum total sandbox count |
| `max_sandbox_starting_count` | uint32 | Maximum concurrently starting sandboxes |
| `max_cpu_used_percent` | uint32 | Maximum observed CPU usage (0–100) |
| `max_cpu_allocated_percent` | uint32 | Maximum allocated-CPU-to-physical-CPU ratio; can exceed 100 when overcommit is allowed |
| `max_memory_used_percent` | uint32 | Maximum observed memory usage (0–100) |
| `max_memory_allocated_percent` | uint32 | Maximum allocated-memory-to-physical-memory ratio; can exceed 100 when overcommit is allowed |

Example:

```json
"node_resource_limit": {
  "max_sandbox_count": 50,
  "max_sandbox_starting_count": 10,
  "max_cpu_used_percent": 90,
  "max_cpu_allocated_percent": 150,
  "max_memory_used_percent": 85,
  "max_memory_allocated_percent": 150
}
```

When all nodes are filtered out, the scheduler returns `Unavailable` to the caller.

## Gateway configuration

- `gateway.scheduler_addr` points to the primary scheduler. The gateway uses it for scheduling, assignment writes, node listing, node detail resolution, and P2P scheduler APIs.
- `gateway.query_only_scheduler_addr` optionally points to a query-only scheduler. When set, sandbox `LookupNode` routing uses this client; when unset, gateway falls back to `gateway.scheduler_addr`.
- `gateway.request_timeout` must be a duration string such as `"30s"` in JSON config files.
- `gateway.request_timeout` applies to regular proxied HTTP requests. Streaming requests and WebSocket connections reuse the client context and are not cut off by this timeout.
- `gateway.forward_response_size` only limits how much of a successful `POST /sandboxes` response the gateway buffers while extracting a sandbox ID for `RecordAssignment`; it is not a global response-size cap for all proxied traffic.
- Cluster list requests (`GET /sandboxes`, `GET /v2/sandboxes`) fan out to every scheduler node and merge results in the gateway. Direct requests to a backend node remain node-scoped.
- Cluster list requests are strict all-or-nothing: if any node times out, returns a non-2xx response, or cannot be reached, the gateway fails the whole list request rather than returning partial data.
- `GET /nodes` returns scheduler-observed node snapshots (including runtime/resource counters), with optional `clusterID` filtering.
- `GET /nodes/{id}` resolves node endpoint via scheduler and then proxies to the runtime node's admin endpoint.
- `GATEWAY_REQUEST_TIMEOUT=<duration>` overrides `gateway.request_timeout` from the environment (for example, `1m30s`).
- `GATEWAY_QUERY_ONLY_SCHEDULER_ADDR=<addr>` overrides `gateway.query_only_scheduler_addr` from the environment.
- `gateway.sandbox_proxy_domains` enables host-based sandbox data-plane routing for `{port}-{sandboxID}.{domain}` URLs. Domains are normalized to lowercase, deduplicated, and must be valid DNS names. Sandbox IDs used in host routes must be lowercase RFC 952/1123 DNS labels, and the full `{port}-{sandboxID}` label must be at most 63 characters.
- `GATEWAY_SANDBOX_PROXY_DOMAINS=<domain>[,<domain>...]` overrides `gateway.sandbox_proxy_domains` from the environment.

Logging format defaults to `auto`:

- `auto`: console when stdout looks like an interactive terminal, otherwise JSON
- `console`: force human-readable terminal logs
- `json`: force structured logs for containers and log pipelines

Examples:

```bash
LOG_FORMAT=console make run-scheduler
LOG_FORMAT=json make run-gateway
```

## Deploy with Docker Compose

From **repository root**, start gateway + scheduler + two backend nodes:

```bash
make deploy-up
```

Optional host-based sandbox data-plane routing:

```bash
SANDBOX_PROXY_DOMAINS=sandbox.example.com \
make deploy-up
```

Repository deployment helpers also accept `SANDBOX_PROXY_DOMAINS=<domain>[,<domain>...]`
and pass it to both gateway and runtime node processes.

Check status / logs / teardown:

```bash
make deploy-ps
make deploy-logs
make deploy-down
```

Container deployments use `deploy/docker/config/default.json`, where scheduler service-discovery and backend node endpoints are set for the Docker network.

The compose stack also wires each runtime node for scheduler heartbeat reporting:

- `AENV_NODE_ID` is set per runtime container (`node-a`, `node-b`).
- `AENV_OBSERVABILITY_SCHEDULER_REPORT_ENABLED=true` enables heartbeat reporting.
- `AENV_OBSERVABILITY_SCHEDULER_ENDPOINT` points runtime nodes at `http://scheduler:9090`.
- `SANDBOX_PROXY_DOMAINS`, when set, is passed through as both `GATEWAY_SANDBOX_PROXY_DOMAINS` and `AENV_SANDBOX_PROXY_DOMAINS`.

## Deploy on Kubernetes

🔴 **The Scheduler workload is not part of this apply by default** (阶段四 —
see the status note at the top of this file). `make k8s-render`/`k8s-apply`
render `deploy/k8s/base/kustomization.yaml`, whose `resources:` list has
`scheduler-service.yaml`/`scheduler-deployment.yaml`/`scheduler-pdb.yaml`
commented out rather than removed. To roll back to a standalone scheduler,
uncomment those three lines (and the matching `scheduler-k8s-config`
`configMapGenerator` entry just above `gateway-k8s-config`), point
`agentenv-api-deployment.yaml`'s `AENV_NODE_PLACEMENT_SOURCE` back to
`"scheduler"` and `AENV_PAUSED_REGISTRY_BACKEND` back to `"central"`, and
point `deploy/k8s/base/config/gateway.json`'s `scheduler_addr` back at
`agentenv-scheduler:9090` — then re-render and apply. The Deployment's own
image tag stays pinned in this file's `images:` transformer the whole time,
so nothing needs rebuilding to bring it back.

### 🔴 Rolling the *node* half back is an image tag, not a flag

This is a different rollback from the one above, and the mechanism changed.
Through 阶段三 the AgentENV server was one binary that could be `--role api`,
`--role node` or `--role all`, and putting the DaemonSet back on `--role all`
was how you got one process serving user-facing REST on every machine again.

There is no `--role` any more, and no `AENV_ROLE`. `aenv-api` and `aenv-node`
are two crates, two dependency graphs (`make check-crate-boundaries`) and two
images — `agentenv-api` and `agentenv-runtime` — and neither binary declares
the argument, so a manifest that still passes it is refused by argument parsing
before the process starts. That is deliberate: an un-migrated manifest fails
loudly rather than being ignored.

To go back to the single-process shape, deploy the **pre-split image tag** on
`agentenv-daemonset.yaml` (the last tag built before the crate split; that
binary still accepts `--role all`) and point the gateway's
`GATEWAY_REST_UPSTREAM_ADDR` and
`GATEWAY_RESUME_ADDR` back at the nodes in the same apply. Three things to
know before starting:

- it is a **serial DaemonSet roll with a drain per machine**, not a value
  change — budget the grace period times the node count;
- leaving the gateway aimed at `agentenv-api` while the nodes go back to
  serving REST is a live half-migration nobody chose, and emptying those two
  keys while the nodes still run `aenv-node` 404s every REST call in the
  cluster. `TestTheRestUpstreamIsAlwaysSetBecauseNodesNeverServeRest`
  (`shared/config/execution_switches_manifest_test.go`) is what refuses the
  second of those in the tree;
- 🔴 **a current-generation gateway can no longer be pointed at empty
  addresses at all.** `services/shared/config`'s `Config.Validate` refuses to
  load a "gateway" config with `GATEWAY_REST_UPSTREAM_ADDR` or
  `GATEWAY_RESUME_ADDR` empty, from either the ConfigMap or
  `config/gateway.json`'s file fallback — the gateway now fails to start
  rather than serving the outage those two keys used to produce when emptied.
  So this rollback needs the **gateway's own image tag pinned back too**, to a
  build from before that refusal existed, in the same apply as the DaemonSet's
  pre-split tag and the two emptied keys — the DaemonSet image tag alone is
  not enough any more.

From the repository root:

```bash
make k8s-render
make k8s-apply
```

Optional host-based sandbox data-plane routing:

```bash
SANDBOX_PROXY_DOMAINS=sandbox.example.com make k8s-apply
```

The default overlay is `deploy/k8s/overlays/default`.
The make targets materialize a temporary Kustomize build context so Kubernetes runtime nodes always consume the repository's single AgentENV runtime config source: `config/default.toml`.

The DaemonSet injects scheduler-report identity and endpoint wiring for runtime nodes:

- `AENV_NODE_ID` comes from Pod metadata name.
- `AENV_OBSERVABILITY_SCHEDULER_REPORT_ENABLED=true` enables heartbeat reporting.
- `AENV_OBSERVABILITY_SCHEDULER_ENDPOINT` is set to `http://agentenv-scheduler:9090`.
- `AENV_SANDBOX_PROXY_DOMAINS` comes from the shared sandbox proxy ConfigMap.

Shared Kubernetes helpers:

```bash
make k8s-build
make k8s-redeploy
```

For single-machine development, `make k8s-apply-dev` uses the `local-dev`
overlay and mounts the repository `env/` directory directly into the AgentENV
DaemonSet at `/workspace/env`. This avoids copying runtime assets into `/var/lib/agentenv/env`.

For local k3s-style development, use:

```bash
make k8s-load-dev
make k8s-refresh-dev
```

`k8s-load-dev` imports the locally built images into k3s/containerd, while
`k8s-refresh-dev` runs build, load, and rollout restart together.

Deployment model:

- `gateway`: Deployment + ClusterIP Service
- `scheduler`: single-replica Deployment + ClusterIP Service
- `agentenv-node`: privileged DaemonSet with `/dev/kvm` and hostPath `/var/lib/agentenv`
- `agentenv-nodes`: headless Service used by scheduler EndpointSlice discovery

Kubernetes config keys:

- `scheduler.discovery.mode`
- `scheduler.discovery.kubernetes.namespace`
- `scheduler.discovery.kubernetes.service_name`
- `scheduler.discovery.kubernetes.port`
- `scheduler.discovery.kubernetes.scheme` (defaults to `http`)
- `scheduler.discovery.kubernetes.ignore_pod_selector` (optional Kubernetes label selector; matching Pods are excluded from discovery)
- `scheduler.discovery.kubernetes.no_schedule_pod_selector` (optional Kubernetes label selector; matching Pods are kept as lingering/no-schedule nodes)

Kubernetes endpoint address handling:

- Scheduler only accepts EndpointSlice addresses that parse as valid IPs.
- Both IPv4 and IPv6 endpoint addresses are supported.
- IPv6 endpoints are emitted using bracketed host:port form (for example, `http://[2001:db8::10]:8000`).

Operational notes:

- The scheduler uses in-cluster Kubernetes config and watches EndpointSlices plus Pods for service discovery.
- Only serving, non-terminating DaemonSet Pods are schedulable. Use `no_schedule_pod_selector` for drain/no-new-work labels and `ignore_pod_selector` for Pods that should be completely hidden from discovery.
- For the default `memory` binding store, `scheduler` should stay single-replica because sandbox bindings are process-local.
- For high availability, run one primary scheduler with `scheduler.redis_addr` set and multiple query-only scheduler replicas started with `--query-only` against the same Redis. Point gateways at the primary with `gateway.scheduler_addr` and at the query-only service with `gateway.query_only_scheduler_addr`. The primary writes sandbox bindings to Redis while query-only replicas continue to serve data-plane `LookupNode` during primary restarts or upgrades. This HA mode is intentionally data-plane only: requests that proxy to existing sandboxes can keep routing, but control-plane operations that need the primary scheduler, such as creating new sandboxes, scheduling, assignment writes, node listing, node detail resolution, and P2P scheduler APIs, still fail while the primary scheduler is unavailable. Artifact store state is still in-memory and is not covered by this HA mode.
- The gateway is intentionally left as ClusterIP by default; attach an Ingress or LoadBalancer based on your environment.

## gRPC API

Proto contract: api/proto/scheduler.proto

Methods:

- Schedule
- ListNodes
- LookupNode
- RecordAssignment
- Heartbeat
- ListObservedNodes
- ReportSandboxEvent
- GetNode
- UnregisterNode
- ListRegistrySandboxes
