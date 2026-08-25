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
| `AENV_OBSERVABILITY_SCHEDULER_ENDPOINT` | unset | Override the shared `[cluster].scheduler_endpoint` — every scheduler-dialling consumer in this process (heartbeat reporting, P2P discovery, paused registry, snapshot catalog, node/resume placement), not only the heartbeat. Read once at process startup. |
| `AENV_CLUSTER_SCHEDULER_ENDPOINT_FILE` | unset | Optional file holding a replacement for `AENV_OBSERVABILITY_SCHEDULER_ENDPOINT`, re-read on the heartbeat interval while the process runs. When set and readable it overrides the static endpoint outright (not a union) for every consumer above, with no pod restart; point it at a ConfigMap volume mounted **without** `subPath` — kubelet does not refresh `subPath` mounts. Unset, or the file never read successfully, falls back to the static value. Wins over the deprecated `AENV_OBSERVABILITY_SCHEDULER_ENDPOINT_FILE` below when both are set. |
| `AENV_OBSERVABILITY_SCHEDULER_ENDPOINT_FILE` | unset | **Deprecated** — use `AENV_CLUSTER_SCHEDULER_ENDPOINT_FILE` instead. Originally hot-reloaded the heartbeat's target only; still read as a fallback when the field above is unset, for deployments that have not migrated. |
| `AENV_OBSERVABILITY_REPORT_INTERVAL_SECS` | `5` | Override heartbeat reporting interval in seconds |
| `AENV_OBSERVABILITY_DUAL_REPORT_API_ENDPOINT` | unset | Optional second heartbeat target, dialled and sent to concurrently with `[cluster].scheduler_endpoint` on every tick. Best-effort and one-way — a failure here never affects the primary heartbeat's success, backoff, or `HeartbeatNodeNotConfigured` handling. Empty (the default) sends nothing extra. Point it at `--role api`'s `AENV_API_GRPC_ADDR` (or a Service in front of several replicas) to feed api's own node registry (`AENV_NODE_PLACEMENT_SOURCE=native`) heartbeat data ahead of any wider cutover. |
| `AENV_NODE_PLACEMENT_SOURCE` | `scheduler` | Where `--role api` resolves a known node's current address and cluster membership: `scheduler` (default, unchanged behavior — asks the scheduler over gRPC) or `native` (answers from api's own `src/node_registry` node registry instead, fed by Kubernetes discovery and — once configured — `AENV_OBSERVABILITY_DUAL_REPORT_API_ENDPOINT`). Sandbox placement/routing (`Schedule`/`LookupNode`/`RecordAssignment`) always goes through the scheduler either way. |
| `AENV_CLUSTER_KUBERNETES_DISCOVERY_NAMESPACE` | unset | Namespace of the `EndpointSlice`/`Pod` objects api's node registry watches. Required when `AENV_NODE_PLACEMENT_SOURCE=native`; read (and a Kubernetes client built) only in that mode. |
| `AENV_CLUSTER_KUBERNETES_DISCOVERY_SERVICE_NAME` | unset | The `Service` name whose `EndpointSlice`s are watched (matches the `kubernetes.io/service-name` label). Required when `AENV_NODE_PLACEMENT_SOURCE=native`. |
| `AENV_CLUSTER_KUBERNETES_DISCOVERY_PORT` | `8000` | The node's user-facing HTTP port, as named on the watched `EndpointSlice`. |
| `AENV_CLUSTER_KUBERNETES_DISCOVERY_SCHEME` | `http` | Scheme used when building discovered node endpoints. |
| `AENV_CLUSTER_KUBERNETES_DISCOVERY_IGNORE_POD_SELECTOR` | unset | Label selector for pods to exclude from discovery entirely. Empty disables the filter. |
| `AENV_CLUSTER_KUBERNETES_DISCOVERY_NO_SCHEDULE_POD_SELECTOR` | unset | Label selector for pods to keep discovered but mark unschedulable (lingering). Empty disables the filter. |
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
| `AENV_PAUSED_REGISTRY_BACKEND` | from config (`local`) | Select the cluster-wide paused-sandbox registry backend: `local` keeps a paused sandbox resumable only on the node that paused it; `central` records it in the shared registry so any node can resume it, reached through the scheduler that owns the database (needs `AENV_OBSERVABILITY_SCHEDULER_ENDPOINT`). The former `postgres` value, where the node connected to the database itself, is refused at startup with a message naming `central`. Set it here rather than in the cluster ConfigMap: `deploy/k8s/run.sh` rewrites that ConfigMap from `config/default.toml` on every apply. |
| ~~`AENV_PAUSED_REGISTRY_DSN`~~ | — | Removed. The node no longer connects to the registry database; the scheduler owns it. A node configured with `AENV_PAUSED_REGISTRY_BACKEND=postgres` refuses to start and names `central` as the replacement. |

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

## Gateway and Scheduler

These variables apply to both the gateway and scheduler processes.

| Variable | Default | Description |
|----------|---------|-------------|
| `LOG_LEVEL` | `info` | Log level: `debug`, `info`, `warn`, or `error` |
| `LOG_FORMAT` | `auto` | Log output format: `auto`, `console`, or `json` |

## Gateway

| Variable | Default | Description |
|----------|---------|-------------|
| `GATEWAY_HTTP_LISTEN_ADDR` | `:8080` | HTTP listen address |
| `GATEWAY_METRICS_LISTEN_ADDR` | `:9102` | Prometheus metrics listen address |
| `GATEWAY_SCHEDULER_ADDR` | `127.0.0.1:9090` | Scheduler gRPC address for routing and node lookup |
| `GATEWAY_QUERY_ONLY_SCHEDULER_ADDR` | unset | Optional secondary scheduler gRPC address used only for sandbox data-plane `LookupNode` queries. When set, creation and control-plane calls still go to `GATEWAY_SCHEDULER_ADDR`. |
| `GATEWAY_REQUEST_TIMEOUT` | `30s` | Override the gateway's HTTP request timeout (for example, `1m30s`) |
| `GATEWAY_SANDBOX_PROXY_DOMAINS` | from config | Comma-separated DNS domains that enable gateway host-based sandbox proxy URLs like `{port}-{sandboxID}.{domain}`. Empty or unset keeps `gateway.sandbox_proxy_domains`. |
| `GATEWAY_DEBUG_MODE` | `false` | Enable gateway debug mode |
| `GATEWAY_REST_UPSTREAM_ADDR` | from config | Where user-facing REST — the sandbox, snapshot and template routes — is sent. `gateway.rest_upstream_addr` empty means the scheduler places each call and a node serves it, which is the behaviour that shipped; `http://agentenv-api:8000` (a bare `host:port` is read as http) sends those calls to the api half instead. Never carries data-plane traffic, which always goes to the node holding the sandbox. An address that cannot be used stops the gateway at startup. 🔴 Setting this variable to the empty string does not turn the switch off — an empty value is ignored and the config file's value stands — so keep the file's value empty and drive the switch from here. |
| `GATEWAY_RESUME_ADDR` | from config | The api half's gRPC wake-up surface, asked when the routing projection cannot place a sandbox. `gateway.resume_addr` empty leaves waking to whichever node the request lands on, which is the behaviour that shipped; `agentenv-api:8002` asks the api half. An api half that cannot be reached delays the request rather than failing it: the gateway falls back to the scheduler. The same 🔴 note as above applies to turning it off. |

## Scheduler

| Variable | Default | Description |
|----------|---------|-------------|
| `SCHEDULER_GRPC_LISTEN_ADDR` | `:9090` | gRPC listen address |
| `SCHEDULER_METRICS_LISTEN_ADDR` | `:9101` | Prometheus metrics listen address |
| `SCHEDULER_STRATEGY` | `round_robin` | Node selection strategy for new sandboxes: `round_robin` or `random` |
| `SCHEDULER_REDIS_ADDR` | unset | Redis address for persistent sandbox-to-node bindings (for example, `redis:6379`). Unset = in-memory bindings, lost on scheduler restart. |
| `SCHEDULER_BINDING_TTL` | `30s` | How long a sandbox-to-node binding is kept without a confirming heartbeat. Accepts Go duration strings (for example, `1m`). |
| `SCHEDULER_ARTIFACT_STORE_CAPACITY` | `1000000` | Maximum number of P2P artifact entries held in the scheduler's in-memory index |
| `SCHEDULER_ARTIFACT_LOOKUP_NODE_LIMIT` | `0` | Maximum number of nodes checked per P2P artifact lookup. `0` means no limit. |
