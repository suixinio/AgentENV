# services

Go implementation of the AgentENV Gateway.

🔴 **阶段四 status**: `services/scheduler` — the Go implementation of the
`Scheduler`/`SnapshotCatalog` RPCs — has been deleted.
`docs/proposals/2026-08-20-service-decomposition.md`'s phase four folded node
discovery, heartbeat receipt (including the cluster CPU-config intersection),
placement and P2P peer/artifact lookup into the Rust `aenv-api` binary
(`src/node_registry/`) before this deletion, so the deletion changes no
deployed behaviour: every RPC on
`services/api/proto/scheduler.proto` was already answered by `aenv-api` on
every current deployment. `services/gateway` is the only Go binary this
module ships now; it dials `gateway.scheduler_addr` — `agentenv-api` on every
current deployment — for `apiproxy.ResumeSandbox` alone, and `scheduler.proto`
is the node-to-api face, with `aenv-node` its only client. See CLAUDE.md's
"Distributed Control Plane" section for the full picture.

## Features

- The gateway carries the sandbox data plane and nothing else. User-facing
  REST — `sandboxes`, `snapshots`, `templates`, `/nodes` and
  `/registry/sandboxes` — is answered by `aenv-api` at its own address. A
  request here that names no sandbox (no proxy host name, no routing header)
  gets a 404.
- Gateway routes sandbox data-plane requests out of the Redis routing
  projection (`aenv-api`'s sandbox-to-node bindings, read directly). A miss
  goes to `aenv-api`'s `apiproxy.ResumeSandbox`, which walks the binding and
  the heartbeat roster, answers a sandbox that is running as it stands,
  restores one that is paused from its snapshot-catalog row (a create under
  the same id), and writes the projection back so the next request is a hit. That is the gateway's whole control
  plane: the projection read and that one RPC; it calls no scheduler.v1 RPC.
  An api half it cannot ask is a 502 (there is no fallback), a positive
  "no such sandbox" is a 404, and a refusal keeps its
  reason (503 with `Retry-After: 1` for a transition in progress, 410 for
  `autoResume` off).
- The Scheduler protocol (`api/proto/scheduler.proto`) supports pluggable
  placement strategies; `aenv-api`'s implementation (`src/node_registry/`)
  always places with round-robin. `RoundRobinStrategy`
  (`src/node_registry/strategy.rs`) is the only strategy in that tree — Go's
  `random` was never ported, there is no config knob to pick another, and the
  one-implementation `Strategy` trait has been deleted. The `strategy` metric
  label survives it, still valued `round_robin`.
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
make build           # builds gateway
make test            # tests the whole module: gateway, shared, api
make test-with-redis # same, plus fails instead of silently skipping when redis-server is missing
```

🔴 **`make test-with-postgres` is gone, and it never started a PostgreSQL.**
This file and CLAUDE.md both used to say it "starts a throwaway PostgreSQL in
Docker for parity with the CI step it mirrors" — untrue since
`services/scheduler` was deleted, and worth more than a corrected sentence: the
recipe only ever checked for `redis-server` and set two Redis variables, so an
operator debugging a red run had a container to go looking for that no target
here has ever started. Nothing left under `services/` imports `database/sql`,
`pgx` or `lib/pq`. The target is renamed to `test-with-redis` with **no
compatibility alias**, so the old name now fails with "No rule to make target"
rather than quietly doing something else than its name says.

What it does: `REDIS_SERVER_BIN` and `AENV_REDIS_TEST_REQUIRED=1` turn a missing
`redis-server` into a failure instead of a silent skip for `shared/routing`'s
reader tests. (There is no Go `RedisBindingStore` suite any more — no type of
that name is left in the module; `aenv-api` owns the binding store's write side,
and Go reads those keys.)

🔴 The variable is `AENV_REDIS_TEST_REQUIRED` — the same name the Rust half's
harness reads, which is the point. One Redis key format is read by two
languages; two names for the switch that arms it would let a run set one and
report green for both halves. It was `SCHEDULER_REDIS_TEST_REQUIRED`, from when
`services/scheduler` owned the write side, and that name is dead with no alias.

🔴 That skip matters more than it looks. Those reader tests are the only
thing in this module that exercises the real Redis routing projection, and that
projection is what every HA deployment's data plane reads. A change made to the
key format on the Rust side and forgotten here passes every other test in the
module, on any machine, and shows up only in production — as routing that
quietly stops arbitrating. A skip reports as a pass, so without
`AENV_REDIS_TEST_REQUIRED` a missing `redis-server` and a healthy run
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

- `gateway.scheduler_addr` points to `agentenv-api`'s gRPC listener. The gateway dials it for one thing, `apiproxy.ResumeSandbox`; the name survives because it is the same listener that serves `scheduler.proto` to the nodes.
- Removed keys are ignored, from the file and the environment: `gateway.rest_upstream_addr` / `GATEWAY_REST_UPSTREAM_ADDR`, `gateway.cold_lookup_timeout` / `GATEWAY_COLD_LOOKUP_TIMEOUT`, `gateway.routing.projection_authoritative` / `GATEWAY_ROUTING_PROJECTION_AUTHORITATIVE`, `gateway.forward_response_size`, `gateway.query_only_scheduler_addr` / `GATEWAY_QUERY_ONLY_SCHEDULER_ADDR`, `GATEWAY_SCHEDULER_FALLBACK_DISABLED` and `GATEWAY_SCHEDULER_FALLBACK_TIMEOUT`. `shared/config/rollback_window_test.go` pins the tolerance, `shared/config/gateway_removed_keys_manifest_test.go` fails if a manifest under `deploy/k8s/base` or `deploy/docker-compose.yml` declares one again, and `docs/src/configuration/env-vars.md` says what each used to do.
- `gateway.request_timeout` must be a duration string such as `"30s"` in JSON config files.
- Metrics that stopped existing when the gateway stopped calling scheduler.v1 (P4 of `docs/proposals/2026-09-01-client-proxy-api-alignment.md`): the whole series `agentenv_gateway_scheduler_rpc_duration_seconds` and `agentenv_gateway_cold_lookup_timeout_total`; the `source` label values `scheduler` and `resume_undecided` of `agentenv_gateway_route_resolution_total`; and the `placed`, `pinned` and `unspecified` values of `agentenv_gateway_sandbox_location_total{location}`, which only a scheduler answer could carry (the series stays, valued `bound`). The gateway's control-plane edges are the projection read and `apiproxy.ResumeSandbox`, counted by `agentenv_gateway_route_resolution_total` and `agentenv_gateway_resume_total`.
- `gateway.request_timeout` applies to regular proxied HTTP requests. Streaming requests and WebSocket connections reuse the client context and are not cut off by this timeout.
- Responses the gateway synthesizes itself — the 404 above, resume errors, scheduler and fencing rejections — carry `Access-Control-Allow-Origin: *`, and a preflight is answered where no upstream can answer it. A response a sandbox produced is never touched: CORS there is envd's or the user's own server's.
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

From **repository root**, start gateway + `agentenv-api` + two backend nodes (`agentenv-api` serves the `Scheduler` RPCs; there is no separate scheduler container). `deploy/docker-compose.yml` also runs `redis` and `postgres` (`postgres:17-alpine`) containers that `agentenv-api` depends on — it requires a reachable `[pg]` unconditionally, the same as every other deployment of it:

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

Container deployments use `deploy/docker/config/default.json` for log and gateway settings. Static node discovery lives in `deploy/docker/config/cluster-static-discovery-overlay.toml`, which `agentenv-api` picks up through `AENV_CONFIG_OVERLAY_PATH`.

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

There is no `--role` flag and no `AENV_ROLE`. `aenv-api` and `aenv-node`
are two crates, two dependency graphs (`make check-crate-boundaries`) and two
images — `agentenv-api` and `agentenv-runtime` — and neither binary declares
the argument, so a manifest that still passes it is refused by argument parsing
before the process starts. That is deliberate: an un-migrated manifest fails
loudly rather than being ignored.

Rolling the node half back means deploying an earlier `agentenv-runtime`
digest on `agentenv-daemonset.yaml` — a serial roll with a drain per machine,
so budget the grace period times the node count. Pin the digest, not a moving
tag.

Going back to the pre-split single-process shape is **not** a supported
rollback any more; the procedure it required, and why it is retired, are in
`docs/proposals/2026-08-31-residue-decisions.md` §D3.

🔴 `GATEWAY_RESUME_ADDR`, which named the api half's wake-up surface, is
deleted: `SandboxResumeService` and the `Scheduler` service share one gRPC
listener on `agentenv-api`, so the key could only ever hold
`gateway.scheduler_addr`'s value, and `cmd/main.go` reuses that one
`ClientConn` for the wake-up RPC. A manifest that still sets it is silently
ignored — the loader reads no such key.

### Rolling the *API* half back

Same mechanism as the node half and no flags: set an earlier `agentenv-api`
digest on `agentenv-api-deployment.yaml` and apply. Pin the digest, not a
moving tag, and move `agentenv-runtime` to the same build in the same apply —
`SERIALIZED_VALUE_SCHEMA_VERSION` fails every cross-half RPC on a skew (see the
note on the `images:` block in `deploy/k8s/base/kustomization.yaml`).

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
- `agentenv-api`: Deployment answering the `Scheduler` RPCs unconditionally, in-process — see CLAUDE.md's "Distributed Control Plane" section for the rest of that fold
- `agentenv-nodes`: headless Service used by Kubernetes-mode node discovery

Operational notes:

- The gateway is intentionally left as ClusterIP by default; attach an Ingress or LoadBalancer based on your environment.

## gRPC API

Proto contract: `api/proto/scheduler.proto`, the node-to-api face. `aenv-api`
(`src/node_registry/grpc_service.rs`) is the only implementation and
`aenv-node` its only client: `services/gateway` calls none of these methods
(its one RPC is `apiproxy.ResumeSandbox`, `api/proto/apiproxy/apiproxy.proto`).
The sandbox lookup and the projection writes that used to be `LookupNode` and
`RecordAssignment` are in-process methods on `NodeRegistryGrpcService`
(`lookup_sandbox`, `record_assignment`, `record_running`), and `/nodes` and
`/registry/sandboxes` read the registry in-process on `aenv-api`'s REST surface.

Methods:

- Schedule
- Heartbeat
- ReportSandboxEvent
- ListP2pPeers
- RecordP2pArtifact
- ForgetP2pArtifact
- LookupP2pArtifact
- GetNode
- UnregisterNode
