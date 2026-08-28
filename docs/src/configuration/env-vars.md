# Environment Variables

## Deployment Helpers

These variables are consumed by the repository's Docker Compose and Kubernetes helpers, then passed to the server or gateway-specific variables listed below.

| Variable | Default | Description |
|----------|---------|-------------|
| `SANDBOX_PROXY_DOMAINS` | empty | Comma-separated DNS domains for host-based sandbox data-plane URLs. In multi-node deployments this single value is applied to both gateway routing and runtime sandbox response metadata. |

## Server

| Variable | Default | Description |
|----------|---------|-------------|
| `API_ADDR` | `0.0.0.0:8000` | Address and port the API server listens on |
| `AENV_CONFIG_PATH` | `config/default.toml` | Path to the TOML configuration file |
| `AENV_CONFIG_OVERLAY_PATH` | unset | Colon-separated list of extra TOML files layered over `AENV_CONFIG_PATH`, left to right. Unset — or a value that is nothing but separators — parses exactly as before this existed. A file that is named but not present is a startup error. See [Layered configuration files](reference.md#layered-configuration-files). |
| `AENV_LOG_FORMAT` | `compact` | Server log output format: `compact`, `pretty`, or `json` |
| `AENV_LOG_SPAN_EVENTS` | `off` | Tracing span lifecycle events to emit: `off`, `new`, `enter`, `exit`, `close`, `active`, or `full` |
| `AENV_NODE_ID` | hostname-derived | Override the runtime node identifier used in observability/admin snapshots |
| `AENV_CLUSTER_ID` | nil UUID | Override the cluster UUID used for P2P peer discovery and scheduler grouping |
| `AENV_SERVICE_INSTANCE_ID` | random UUIDv7 | Override the per-process service instance UUID included in heartbeats |
| `AENV_OBSERVABILITY_SCHEDULER_REPORT_ENABLED` | from config | Enable scheduler heartbeat reporting |
| `AENV_OBSERVABILITY_SCHEDULER_ENDPOINT` | unset | Override the shared `[cluster].scheduler_endpoint` — every scheduler-dialling consumer in this process (heartbeat reporting, P2P discovery, node/resume placement), not only the heartbeat. Read once at process startup. |
| `AENV_CLUSTER_SCHEDULER_ENDPOINT_FILE` | unset | Optional file holding a replacement for `AENV_OBSERVABILITY_SCHEDULER_ENDPOINT`, re-read on the heartbeat interval while the process runs. When set and readable it overrides the static endpoint outright (not a union) for every consumer above, with no pod restart; point it at a ConfigMap volume mounted **without** `subPath` — kubelet does not refresh `subPath` mounts. Unset, or the file never read successfully, falls back to the static value. |
| ~~`AENV_OBSERVABILITY_SCHEDULER_ENDPOINT_FILE`~~ | — | **Removed.** Superseded by `AENV_CLUSTER_SCHEDULER_ENDPOINT_FILE` once every deployment migrated onto it; the deprecated fallback and its silent-drift risk (confique ignores an undeclared environment variable) were removed together. Both binaries now refuse to start with a clear error if this name is still set — see `refuse_removed_scheduler_endpoint_file_env_var` in `src/cfg.rs`. |
| `AENV_OBSERVABILITY_REPORT_INTERVAL_SECS` | `5` | Override heartbeat reporting interval in seconds |
| ~~`AENV_OBSERVABILITY_DUAL_REPORT_API_ENDPOINT`~~ | — | **Removed.** It was a Stage A/D5-only bridge (a second, best-effort heartbeat target dialled concurrently with `[cluster].scheduler_endpoint`) meant to feed `aenv-api`'s own node registry ahead of a wider cutover. Once the primary heartbeat itself could target `agentenv-api` directly (`node-heartbeat-config`), the bridge became redundant — a second, concurrent send to the same target would only double the request rate for no new coverage — and the field, its config struct member, and the reporter's dual-send machinery were deleted. |
| ~~`AENV_NODE_PLACEMENT_SOURCE`~~ | — | **Removed.** It used to choose where `aenv-api` resolved a known node's current address, cluster membership, and sandbox placement/routing: `scheduler` (the code default — asked a Go scheduler process over gRPC for all of it) or `native` (answered all of it from api's own process instead — a local `src/node_registry` node registry fed by Kubernetes discovery and node heartbeats for node identity/membership, and `AENV_BINDING_STORE_BACKEND`'s routing table for `Schedule`/`LookupNode`/`RecordAssignment`). The Go scheduler process is deleted from the tree (see CLAUDE.md's "Distributed Control Plane" section), so `native` is the only behavior left — `aenv-api` now always builds its own node registry, unconditionally, with no scheduler endpoint required for any of it — and the switch was deleted along with the alternative it used to choose. `aenv-api` refuses to start with a clear error if this name is still set — see `refuse_removed_node_placement_source_env_var` in `src/cfg.rs`. |
| `AENV_BINDING_STORE_BACKEND` | `in_memory` | Which sandbox-to-node routing table backend `Schedule`/`LookupNode`/`RecordAssignment` answer from: `in_memory` (one replica's own table — refused at startup, since `aenv-api` runs more than one replica) or `redis` (the shared table every replica reads and writes). |
| `AENV_BINDING_STORE_REDIS_URL` | `redis://127.0.0.1:6379` | Redis connection URL for the binding store, read only when `AENV_BINDING_STORE_BACKEND=redis`. |
| `AENV_BINDING_STORE_REDIS_KEY_PREFIX` | `agentenv:scheduler:bindings` | Redis key prefix for the binding store. Deliberately the same default a bare Go scheduler deployment already used — gateway's routing-projection read path speaks to this exact keyspace; do not rename it. |
| `AENV_CLUSTER_NODE_DISCOVERY_MODE` | `kubernetes` | Which discovery strategy seeds api's native node registry: `kubernetes` (default; watches EndpointSlices — see the `AENV_CLUSTER_KUBERNETES_DISCOVERY_*` variables below) or `static` (seeds once at startup, no ongoing watch, from `[cluster].static_discovery_nodes` — a `{id, endpoint}` list that is **TOML-file-only with no `env =` binding**; set it via the file `AENV_CONFIG_PATH` names or an `AENV_CONFIG_OVERLAY_PATH` overlay, as `deploy/docker/config/cluster-static-discovery-overlay.toml` does for `deploy/docker-compose.yml`, which has no Kubernetes API to discover against). |
| `AENV_CLUSTER_KUBERNETES_DISCOVERY_NAMESPACE` | unset | Namespace of the `EndpointSlice`/`Pod` objects api's node registry watches. Required when `AENV_CLUSTER_NODE_DISCOVERY_MODE=kubernetes` (the default); read (and a Kubernetes client built) only in that mode. |
| `AENV_CLUSTER_KUBERNETES_DISCOVERY_SERVICE_NAME` | unset | The `Service` name whose `EndpointSlice`s are watched (matches the `kubernetes.io/service-name` label). Required alongside the namespace above. |
| `AENV_CLUSTER_KUBERNETES_DISCOVERY_PORT` | `8000` | The node's user-facing HTTP port, as named on the watched `EndpointSlice`. |
| `AENV_CLUSTER_KUBERNETES_DISCOVERY_SCHEME` | `http` | Scheme used when building discovered node endpoints. |
| `AENV_CLUSTER_KUBERNETES_DISCOVERY_IGNORE_POD_SELECTOR` | unset | Label selector for pods to exclude from discovery entirely. Empty disables the filter. |
| `AENV_CLUSTER_KUBERNETES_DISCOVERY_NO_SCHEDULE_POD_SELECTOR` | unset | Label selector for pods to keep discovered but mark unschedulable (lingering). Empty disables the filter. |
| `AENV_CLUSTER_KUBERNETES_DISCOVERY_EMPTY_SYNC_CONFIRMATIONS` | `3` | How many consecutive all-empty discovery syncs in a row confirm that the cluster genuinely has no nodes, rather than one sync being a transient re-LIST, before api's native node registry actually clears discovery/observed state. `0` and `1` both mean "the first empty sync confirms it immediately". |
| `AENV_CLUSTER_KUBERNETES_DISCOVERY_EMPTY_SYNC_WINDOW_SECS` | `60` | How long the first all-empty discovery sync may stand unconfirmed before wall-clock time alone confirms it, independent of the confirmation count above. `0` means the first call confirms it immediately. |
| `AENV_CLUSTER_NATIVE_WARMUP_TIMEOUT_SECS` | `15` | How long api's native node registry withholds a binding-store "not found" answer while waiting for every discovered node to report at least one heartbeat, measured from when the gRPC listener that receives heartbeats actually binds (not from when `assemble_api` starts). `0` also falls back to the default. |
| `AENV_CUSTOM_EXTENSION_URL` | unset | Override `[custom_extension].url`, the HTTP base URL of the custom extension service |
| `AENV_SANDBOX_ACCESS_TOKEN_HASH_SEED` | auto-generated under `$AENV_HOME/secrets` | Optional override for the secret used to derive secure sandbox envd access tokens. Configure the same value on every node when cross-node recovery of the same sandbox ID is required; otherwise each node uses its own managed seed. |
| `AENV_SANDBOX_PROXY_DOMAINS` | from config | Comma-separated DNS domains that enable server-side host-based sandbox proxy URLs like `{port}-{sandboxID}.{domain}` and populate the sandbox response `domain` field. Empty or unset keeps `[sandbox_proxy].domains`. |
| `AENV_HOME_PATH` | `/var/lib/aenv` | Override the base directory from which AgentENV derives local state, caches, logs, generated configs, and downloaded dependencies. Component-specific path settings remain available as advanced overrides. |
| `AENV_RUNTIME_PATH` | `/run/aenv` | Override the transient runtime directory used for network namespace mount points and the default ublk daemon socket. |
| `AENV_DEPS_PATH` | `$AENV_HOME/deps` | Override root directory for auto-downloaded runtime assets (Firecracker, kernel, tools drive). |
| `AENV_VIRTUALIZATION_MODE` | `kvm` | Select the node virtualization mode. Leave unset for normal installations; set to `pvm` only when following the [PVM Deployment](../deployment/pvm.md) guide. |
| `AENV_SNAPSHOT_LOCAL_CACHE_PATH` | `$AENV_HOME/snapshot-local-cache` | Override the snapshot manager's node-local artifact/cache root |
| ~~`AENV_SNAPSHOT_STORE`~~ | — | **Removed. It never worked.** It was declared on `[backend.posix_fs].snapshot_store` and documented here from the day it was written, and the config loader never read it once: confique reaches a field from the environment only through `#[config(nested)]`, `nested` may not be `Option<_>`, and `[backend.posix_fs]` is `Option<PosixFsBackendConfig>`. Set `snapshot_store` in a file named by `AENV_CONFIG_OVERLAY_PATH` instead. |
| `AENV_UBLK_DAEMON_BINARY_PATH` | `$AENV_HOME/ublk/uvm-ublk-daemon` | Override path to the `uvm-ublk-daemon` binary |
| `AENV_UBLK_DAEMON_METRICS_LISTEN_ADDR` | `0.0.0.0:9103` | Override ublk daemon Prometheus metrics listen address; empty string disables it |
| `AENV_FORCE_SYSCTL_TUNING` | unset | Set to `1` to force sysctl tuning in a privileged container with writable host sysctls. Normally skipped automatically inside containers. |
| `AENV_FIRECRACKER_WORK_DIR` | `$AENV_HOME/firecracker-work` | Override the parent directory for per-sandbox Firecracker work directories. |
| `AENV_FIRECRACKER_SERIAL_DIR` | `$AENV_HOME/logs/serial` | Override the directory for persistent Firecracker serial output. Files are grouped under `{serial_dir}/{sandbox_id}/`. |
| `AENV_PERSISTED_SANDBOX_STORE_PATH` | `$AENV_HOME/persisted-sandboxes` | Override the directory where paused sandbox state is persisted across server restarts. |
| `AENV_PAUSED_REGISTRY_BACKEND` | from config (`local`) | Select the cluster-wide paused-sandbox registry backend: `local` keeps a paused sandbox resumable only on the node that paused it — the only backend `aenv-node` runs (`aenv-node` never holds a `[pg]` DSN, and the DaemonSet does not set this variable, so it stays on this default); `postgres` connects directly to the registry database over the shared `[pg]` pool instead of dialling anything — legal only in `aenv-api` (which additionally requires a lease-renewal roster, aenv-api's own node registry, which it always builds; see CLAUDE.md's paused-registry note), and this is what `deploy/k8s/base/agentenv-api-deployment.yaml` runs by default since 阶段四. Set it here rather than in the cluster ConfigMap: `deploy/k8s/run.sh` rewrites that ConfigMap from `config/default.toml` on every apply. |
| ~~`AENV_PAUSED_REGISTRY_DSN`~~ | — | Removed; never existed. The `postgres` backend reuses the process's own `[pg]` pool (see `AENV_PAUSED_REGISTRY_BACKEND` above) rather than taking a DSN of its own. |

## E2B SDK / CLI

These variables configure the E2B SDK and CLI to point at an AgentENV server. Values depend on your deployment mode.

| Variable | Description |
|----------|-------------|
| `E2B_API_URL` | AgentENV server API base URL |
| `E2B_SANDBOX_URL` | Sandbox proxy URL (for WebSocket and process interaction) |
| `E2B_API_KEY` | API key for authentication |
| `E2B_ACCESS_TOKEN` | Access token (used by `e2b template` commands) |

### Values by Deployment Mode

**Manual compile (single node)**:

```bash
export E2B_API_URL=http://127.0.0.1:8000
export E2B_SANDBOX_URL=${E2B_API_URL}
export E2B_API_KEY=e2b_000000
export E2B_ACCESS_TOKEN=dummy
```

**Docker Compose / Kubernetes (multi-node)**:

```bash
export E2B_API_URL=http://127.0.0.1:8080
export E2B_SANDBOX_URL=${E2B_API_URL}
export E2B_API_KEY=e2b_000000
export E2B_ACCESS_TOKEN=dummy
```

> In both modes, sandbox data-plane requests can use routing headers with
> `E2B_SANDBOX_URL=${E2B_API_URL}`. The explicit `/proxy` prefix
> (`${E2B_API_URL}/proxy`) is still accepted for back-compat.

> For local development, any non-empty value works for `E2B_API_KEY` and `E2B_ACCESS_TOKEN` because the server only checks that the auth header is present.

## Gateway

These variables apply to the gateway process (`services/gateway`, the only
Go binary this module ships — the standalone `services/scheduler` process
`LOG_LEVEL`/`LOG_FORMAT` used to also apply to has been deleted; its RPC
surface is now answered by `aenv-api`'s own in-process node registry, always,
with no switch left to choose otherwise — see CLAUDE.md's "Distributed
Control Plane" section).

| Variable | Default | Description |
|----------|---------|-------------|
| `LOG_LEVEL` | `info` | Log level: `debug`, `info`, `warn`, or `error` |
| `LOG_FORMAT` | `auto` | Log output format: `auto`, `console`, or `json` |
| `GATEWAY_HTTP_LISTEN_ADDR` | `:8080` | HTTP listen address |
| `GATEWAY_METRICS_LISTEN_ADDR` | `:9102` | Prometheus metrics listen address |
| `GATEWAY_SCHEDULER_ADDR` | `127.0.0.1:9090` | Scheduler gRPC address for routing and node lookup |
| `GATEWAY_QUERY_ONLY_SCHEDULER_ADDR` | unset | Optional secondary scheduler gRPC address used only for sandbox data-plane `LookupNode` queries. When set, creation and control-plane calls still go to `GATEWAY_SCHEDULER_ADDR`. |
| `GATEWAY_REQUEST_TIMEOUT` | `30s` | Override the gateway's HTTP request timeout (for example, `1m30s`) |
| `GATEWAY_SANDBOX_PROXY_DOMAINS` | from config | Comma-separated DNS domains that enable gateway host-based sandbox proxy URLs like `{port}-{sandboxID}.{domain}`. Empty or unset keeps `gateway.sandbox_proxy_domains`. |
| `GATEWAY_DEBUG_MODE` | `false` | Enable gateway debug mode |
| `GATEWAY_REST_UPSTREAM_ADDR` | from config | Where user-facing REST — the sandbox, snapshot and template routes — is sent: `http://agentenv-api:8000` (a bare `host:port` is read as http) sends those calls to the api half. Never carries data-plane traffic, which always goes to the node holding the sandbox. 🔴 **Required, not optional.** Nodes run `aenv-node`, which answers 404 on every user-facing REST route under any configuration, so `gateway.rest_upstream_addr` empty is refused at startup rather than falling back to a node — the empty value used to mean "the scheduler places each call and a node serves it," and that position no longer exists. An address that cannot be used, or an empty one, stops the gateway at startup. Setting this variable to the empty string does not clear it — an empty value is *ignored* and the config file's value stands, and that file ships empty on purpose, as a fail-fast if this variable's ConfigMap is ever lost. |
| `GATEWAY_RESUME_ADDR` | from config | The api half's gRPC wake-up surface, asked when the routing projection cannot place a sandbox: `agentenv-api:8002` asks the api half. An api half that cannot be reached delays the request rather than failing it: the gateway falls back to the scheduler. 🔴 **Required, not optional**, for the same reason as `GATEWAY_REST_UPSTREAM_ADDR` above — `aenv-node` has no wake-up surface of its own under any configuration, so `gateway.resume_addr` empty is refused at startup. The same note above about an empty value being ignored, and why, applies here too. |

