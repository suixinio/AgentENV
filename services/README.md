# services

Go implementation of the AgentENV Gateway.

🔴 **阶段四 status**: `services/scheduler` — the Go implementation of the
`Scheduler`/`PausedRegistry`/`SnapshotCatalog` RPCs — has been deleted.
`docs/proposals/2026-08-20-service-decomposition.md`'s phase four folded node
discovery, heartbeat receipt (including the cluster CPU-config intersection),
placement, P2P peer/artifact lookup, and the cluster-wide paused-sandbox
registry into the Rust `aenv-api` binary (`src/node_registry/`,
`crates/aenv-api/src/orchestrator/paused_registry/postgres/`) before this
deletion, so the deletion changes no deployed behaviour: every RPC on
`services/api/proto/scheduler.proto` was already answered by `aenv-api` on
every current deployment (`ListRegistrySandboxes` — `src/node_registry/grpc_service.rs`'s
`list_registry_sandboxes`, against `PausedSandboxRegistry::list_all`
(`src/orchestrator/paused_registry/mod.rs`) — was the last RPC group ported).
`services/gateway` is the only Go binary this module ships now; it still
dials `gateway.scheduler_addr` — which points at `agentenv-api` on every
current deployment — through the same generated `schedulerv1.SchedulerClient`
it always has, so nothing about the gateway itself changed. The `.proto`
contract and its generated Go/Rust bindings are unaffected by the deletion —
see CLAUDE.md's "Distributed Control Plane" section for the full picture.

## Features

- Every user-facing REST call (`sandboxes`/`snapshots`/`templates`, including
  `GET /sandboxes` and `GET /v2/sandboxes`) is forwarded unconditionally to
  `gateway.rest_upstream_addr` — the `aenv-api` half. 🔴 There is no more
  per-node fan-out: `cluster_list.go`, which used to aggregate those two
  routes across every runtime node when no api half was configured, is
  deleted along with the position it existed for (`services/shared/config`'s
  `Config.Validate` now refuses to load a gateway config with
  `rest_upstream_addr` empty).
- Gateway aggregates `GET /nodes` and resolves `GET /nodes/{id}` from the
  Scheduler protocol's `ListObservedNodes`/`GetNode` RPCs — today always
  answered by `aenv-api`.
- Gateway routes sandbox data-plane requests to whichever node the Scheduler
  protocol names, in a single `LookupNode` call. The answer comes from the
  sandbox-to-node binding, from the heartbeat roster that seeded it, or —
  when neither knows the sandbox — from the paused-sandbox registry, and says
  which of the three it used.
- The Scheduler protocol (`api/proto/scheduler.proto`) supports pluggable
  placement strategies; `aenv-api`'s implementation (`src/node_registry/`)
  always places with round-robin today (a `random` strategy is ported in
  `src/node_registry/strategy.rs` but not wired to any config knob yet).
- Node discovery for that registry is static configuration or Kubernetes
  EndpointSlice watching (`[cluster].node_discovery_mode`).
- The sandbox-to-node binding store is Redis, always (`[binding_store]` on
  `aenv-api` — the `backend` switch and its in-memory arm are deleted, so
  `AENV_BINDING_STORE_REDIS_URL` is the whole configuration; `gateway`'s own
  routing-projection reader reads the same Redis keys for its fast path).
- Node health and sandbox roster are observed from heartbeats, and expired
  sandbox-to-node bindings are dropped on heartbeat, node unregistration, or
  lookup.
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
make build              # builds gateway
make test               # tests the whole module: gateway, shared, api
make test-with-postgres # same, plus fails instead of silently skipping when redis-server is missing
```

`services/scheduler`'s PostgreSQL-gated paused-registry suite was deleted
along with the package, so nothing left under `services/` needs a real
PostgreSQL to run its tests. `make test-with-postgres` still starts a
throwaway PostgreSQL in Docker for parity with the CI step it mirrors, but the
only tests it changes the outcome of today are Redis-gated: `REDIS_SERVER_BIN`
and `SCHEDULER_REDIS_TEST_REQUIRED=1` turn a missing `redis-server` into a
failure instead of a silent skip for `shared/routing`'s reader tests. (There is
no Go `RedisBindingStore` suite any more — no type of that name is left in the
module; `aenv-api` owns the binding store's write side, and Go reads those keys.)

🔴 That skip matters more than it looks. Those reader tests are the only
thing in this module that exercises the real Redis routing projection, and that
projection is what every HA deployment's data plane reads. A change made to the
key format on the Rust side and forgotten here passes every other test in the
module, on any machine, and shows up only in production — as routing that
quietly stops arbitrating. A skip reports as a pass, so without
`SCHEDULER_REDIS_TEST_REQUIRED` a missing `redis-server` and a healthy run
look identical.

Per-service (from `services/gateway/`):

```bash
make build
make test
```

`gateway`'s `test` covers `shared/` and `api/` as well as its own tree, which
is the package set its `vet` and `fmt-check` already check.

## Run locally

Start gateway:

```bash
make run-gateway
```

Point `gateway.scheduler_addr` in `services/config/local.json` at whichever
process answers `services/api/proto/scheduler.proto` — `aenv-api`'s gRPC
listener on every current deployment, not a local Go scheduler process
(`make run-scheduler` no longer exists; there is no Go binary left to run it).

## Gateway configuration

- `gateway.scheduler_addr` points to whichever process answers the Scheduler protocol — `agentenv-api` on every current deployment. The gateway uses it for scheduling, assignment writes, node listing, node detail resolution, and P2P scheduler APIs.
- 🔴 `gateway.query_only_scheduler_addr` is **deleted**, knob and client path both. It named a second Scheduler-protocol endpoint that `LookupNode` alone would use, for HA read traffic served by `services/scheduler`'s `--query-only` replica mode; that Go binary is gone, and the client-selection field it configured (`QueryOnlySchedulerClient`) went with it. Every `LookupNode` call now goes to `gateway.scheduler_addr`, like every other Scheduler RPC this process makes. `GATEWAY_QUERY_ONLY_SCHEDULER_ADDR` is in `RemovedGatewayEnvVars`, so a manifest that still sets it makes the gateway **refuse to start** rather than silently ignoring it.
- `gateway.request_timeout` must be a duration string such as `"30s"` in JSON config files.
- `gateway.request_timeout` applies to regular proxied HTTP requests. Streaming requests and WebSocket connections reuse the client context and are not cut off by this timeout.
- `gateway.forward_response_size` only limits how much of a successful `POST /sandboxes` response the gateway buffers while extracting a sandbox ID for `RecordAssignment`; it is not a global response-size cap for all proxied traffic.
- `GET /sandboxes` and `GET /v2/sandboxes` are forwarded unconditionally to `gateway.rest_upstream_addr` (the api half) like any other user-facing REST call. 🔴 There is no more per-node fan-out or cluster-wide merge for these two routes: `cluster_list.go`, which used to aggregate them across every scheduler-known node when no api half was configured, is deleted (`rest_upstream.go` now claims both routes unconditionally).
- `GET /nodes` returns Scheduler-protocol observed node snapshots (including runtime/resource counters), with optional `clusterID` filtering.
- `GET /nodes/{id}` resolves the node endpoint via the Scheduler protocol and then proxies to the runtime node's admin endpoint.
- `GATEWAY_REQUEST_TIMEOUT=<duration>` overrides `gateway.request_timeout` from the environment (for example, `1m30s`).
- `gateway.sandbox_proxy_domains` enables host-based sandbox data-plane routing for `{port}-{sandboxID}.{domain}` URLs. Domains are normalized to lowercase, deduplicated, and must be valid DNS names. Sandbox IDs used in host routes must be lowercase RFC 952/1123 DNS labels, and the full `{port}-{sandboxID}` label must be at most 63 characters.
- `GATEWAY_SANDBOX_PROXY_DOMAINS=<domain>[,<domain>...]` overrides `gateway.sandbox_proxy_domains` from the environment.

Logging format defaults to `auto`:

- `auto`: console when stdout looks like an interactive terminal, otherwise JSON
- `console`: force human-readable terminal logs
- `json`: force structured logs for containers and log pipelines

Examples:

```bash
LOG_FORMAT=json make run-gateway
```

## Deploy with Docker Compose

From **repository root**, start gateway + `agentenv-api` + two backend nodes (`agentenv-api` serves the `Scheduler`/`PausedRegistry` RPCs; there is no separate scheduler container). `deploy/docker-compose.yml` also runs `redis` and `postgres` (`postgres:17-alpine`) containers that `agentenv-api` depends on — it requires a reachable `[pg]` unconditionally, the same as every other deployment of it:

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

Container deployments use `deploy/docker/config/default.json`, where static node discovery and backend node endpoints are set for the Docker network.

The compose stack also wires each runtime node for heartbeat reporting:

- `AENV_NODE_ID` is set per runtime container (`node-a`, `node-b`).
- `AENV_OBSERVABILITY_SCHEDULER_REPORT_ENABLED=true` enables heartbeat reporting.
- `AENV_OBSERVABILITY_SCHEDULER_ENDPOINT` points runtime nodes at `http://agentenv-api:8002`.
- `SANDBOX_PROXY_DOMAINS`, when set, is passed through as both `GATEWAY_SANDBOX_PROXY_DOMAINS` and `AENV_SANDBOX_PROXY_DOMAINS`.

## Deploy on Kubernetes

🔴 **There is no Scheduler workload to deploy.** `deploy/k8s/base` renders
`gateway` and the `agentenv-node`/`agentenv-api` workloads only;
`scheduler-service.yaml`/`scheduler-deployment.yaml`/`scheduler-pdb.yaml` and
the Go source that backed them are deleted (see the status note at the top of
this file). The gateway's `scheduler_addr` points at `agentenv-api` and the
DaemonSet's heartbeat target does too — see CLAUDE.md's "Distributed Control
Plane" section for the current architecture.

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

The DaemonSet injects heartbeat identity and endpoint wiring for runtime nodes:

- `AENV_NODE_ID` comes from Pod metadata name.
- `AENV_OBSERVABILITY_SCHEDULER_REPORT_ENABLED=true` enables heartbeat reporting.
- `AENV_OBSERVABILITY_SCHEDULER_ENDPOINT` is set to `http://agentenv-api:8002` — `aenv-api`'s own gRPC listener, which answers the `Scheduler.Heartbeat` RPC (`src/node_registry/grpc_service.rs`).
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
- `agentenv-node`: privileged DaemonSet with `/dev/kvm` and hostPath `/var/lib/agentenv`
- `agentenv-api`: Deployment answering the `Scheduler`/`PausedRegistry` RPCs unconditionally, in-process — see CLAUDE.md's "Distributed Control Plane" section for the rest of that fold
- `agentenv-nodes`: headless Service used by Kubernetes-mode node discovery

Operational notes:

- The gateway is intentionally left as ClusterIP by default; attach an Ingress or LoadBalancer based on your environment.

## gRPC API

Proto contract: `api/proto/scheduler.proto`. `services/gateway` is a client of
this contract, not a server for it — `aenv-api` (`src/node_registry/grpc_service.rs`)
is the only production implementation left.

Methods:

- Schedule
- ListNodes
- LookupNode
- RecordAssignment
- Heartbeat
- ReportSandboxEvent
- ListObservedNodes
- ListP2pPeers
- RecordP2pArtifact
- ForgetP2pArtifact
- LookupP2pArtifact
- GetNode
- UnregisterNode
- ListRegistrySandboxes
