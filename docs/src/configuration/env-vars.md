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
| ~~`AENV_OBSERVABILITY_SCHEDULER_ENDPOINT_FILE`~~ | — | **Removed.** Superseded by `AENV_CLUSTER_SCHEDULER_ENDPOINT_FILE` once every deployment migrated onto it; the deprecated fallback and its silent-drift risk (confique ignores an undeclared environment variable) were removed together. **Setting it today does nothing:** no field declares the name, so it is read by nothing and refused by nothing. A startup refusal carried un-migrated manifests through the move for one release and has since been removed with the rest of that scaffolding. |
| `AENV_OBSERVABILITY_REPORT_INTERVAL_SECS` | `5` | Override heartbeat reporting interval in seconds |
| `AENV_EGRESS_BROKER_MODE` | `disabled` | `[egress_broker].mode`: `disabled`, `embedded` or `remote`. See [`[egress_broker]`](reference.md#egress_broker). |
| `AENV_EGRESS_BROKER_ENDPOINT` | unset | `[egress_broker].endpoint`, required in `remote` mode |
| `AENV_EGRESS_BROKER_CA_CERT_PATH` | unset | `[egress_broker].ca_cert_path`, required in `remote` mode. A node's copy of the broker's CA certificate — never its key. Not to be confused with the broker's own `AENV_EGRESS_CA_CERT_PATH` under [Egress Broker](#egress-broker-aenv-egress). |
| `AENV_EGRESS_BROKER_GUEST_CA_CERT_PATH` | unset | `[egress_broker].guest_ca_cert_path`: the CA guests trust for intercepted names, when it is not the same one that verifies the broker. |
| `AENV_EGRESS_BROKER_SHARED_SECRET` | unset | `[egress_broker].shared_secret`, required in `remote` mode; mount it from a Secret |
| `AENV_EGRESS_BROKER_MAX_SKEW_MS` | `30000` | `[egress_broker].max_skew_ms` |
| `AENV_EGRESS_BROKER_PER_SANDBOX_CONNS` | `256` | `[egress_broker].per_sandbox_conns` |
| `AENV_EGRESS_BROKER_NODE_CONNS` | `20000` | `[egress_broker].node_conns` |
| `AENV_EGRESS_BROKER_OPEN_TIMEOUT_MS` | `3000` | `[egress_broker].open_timeout_ms` |
| — | `[]` | `[egress_broker].embedded_tcp_allowed_cidrs` is a list, so confique cannot bind it from the environment; set it in the config file or an overlay. |
| `AENV_SECRETS_BACKEND` | `disabled` | `[secrets].backend`: `disabled` or `postgres`. Read by `aenv-api` only. Any other value is refused at startup. |
| `AENV_SECRETS_PG_KEY_FILE` | unset | `[secrets.pg].key_file`: the file holding the base64 32-byte master key values are encrypted under. A path, not the key — an environment variable holding one is readable from `/proc` and `kubectl describe`. |
| `AENV_SECRETS_PG_RESOLVER_TOKEN_FILE` | unset | `[secrets.pg].resolver_token_file`: the file holding the bearer the broker presents at the internal resolve endpoint. Read once at startup; the broker's `AENV_EGRESS_RESOLVER_TOKEN_FILE` names a file with the same value. |
| ~~`AENV_SECRETS_VAULT_ADDR`~~, ~~`AENV_SECRETS_VAULT_TOKEN`~~, ~~`AENV_SECRETS_VAULT_MOUNT`~~, ~~`AENV_SECRETS_VAULT_NAMESPACE`~~, ~~`AENV_SECRETS_VAULT_TIMEOUT_MS`~~ | — | **Removed.** They configured `[secrets].backend = "vault"`, which wrote values to `<mount>/secrets/<name>` and grants to `<mount>/grants/<execution_id>` in a HashiCorp Vault KV v2 store the broker read back. `postgres` keeps both in the database this half already holds, so a deployment needs no credential store beside AgentENV and the Vault backend, its config section and the `SecretsBackendKind::Vault` variant were deleted together. **Setting them today does nothing:** no field declares the names, so they are read by nothing and refused by nothing — but `AENV_SECRETS_BACKEND=vault` *is* refused, because the value no longer parses. The `egress-vault` and `secrets-vault-writer` Secrets are gone from `deploy/k8s/base`. |
| ~~`AENV_SECRETS_RESOLVER_URL`~~, ~~`AENV_SECRETS_RESOLVER_TOKEN`~~, ~~`AENV_SECRETS_RESOLVER_TOKEN_FILE`~~, ~~`AENV_SECRETS_RESOLVER_TIMEOUT_MS`~~ | — | **Removed.** They configured `[secrets].backend = "external_resolver"`, where the credentials never entered AgentENV at all: this half posted grants and revocations to a service the operator ran, and the broker resolved values against the same base. It was the only backend whose values stayed outside this deployment, and removing it is the part of the move to `postgres` that costs something — see `docs/proposals/2026-09-04-api-owned-credential-store.md` §9. **Setting them today does nothing:** no field declares the names, so they are read by nothing and refused by nothing; `AENV_SECRETS_BACKEND=external_resolver` is refused, because the value no longer parses. The broker's own `AENV_EGRESS_RESOLVER_URL` is untouched and now points at this half. |
| ~~`AENV_OBSERVABILITY_DUAL_REPORT_API_ENDPOINT`~~ | — | **Removed.** It was a Stage A/D5-only bridge (a second, best-effort heartbeat target dialled concurrently with `[cluster].scheduler_endpoint`) meant to feed `aenv-api`'s own node registry ahead of a wider cutover. Once the primary heartbeat itself could target `agentenv-api` directly (`node-heartbeat-config`), the bridge became redundant — a second, concurrent send to the same target would only double the request rate for no new coverage — and the field, its config struct member, and the reporter's dual-send machinery were deleted. **Setting it today does nothing:** no field declares the name, so it is read by nothing and refused by nothing. |
| ~~`AENV_NODE_PLACEMENT_SOURCE`~~ | — | **Removed.** It used to choose where `aenv-api` resolved a known node's current address, cluster membership, and sandbox placement/routing: `scheduler` (the code default — asked a Go scheduler process over gRPC for all of it) or `native` (answered all of it from api's own process instead — a local `src/node_registry` node registry fed by Kubernetes discovery and node heartbeats for node identity/membership, and `[binding_store]`'s Redis routing table for `Schedule`/`LookupNode`/`RecordAssignment`). The Go scheduler process is deleted from the tree (see CLAUDE.md's "Distributed Control Plane" section), so `native` is the only behavior left — `aenv-api` now always builds its own node registry, unconditionally, with no scheduler endpoint required for any of it — and the switch was deleted along with the alternative it used to choose. **Setting it today does nothing:** no field declares the name, so it is read by nothing and refused by nothing. A startup refusal carried un-migrated manifests through the cutover for one release and has since been removed with the rest of that scaffolding. |
| ~~`AENV_BINDING_STORE_BACKEND`~~ | — | **Removed.** It chose which sandbox-to-node routing table backend `Schedule`/`LookupNode`/`RecordAssignment` answered from: `in_memory` (one replica's own table) or `redis` (the shared table every replica reads and writes). It had exactly one legal value — `build_binding_store` (`crates/aenv-api/src/bin/aenv-api.rs`) refused `in_memory` unconditionally, because a routing table one replica cannot see misroutes silently and nothing at that layer can tell a lone replica from one of several — so the field, its `BindingStoreBackendKind` enum and the refusal arm were all deleted and `aenv-api` always constructs the Redis store. **Setting it today does nothing:** no field declares the name, so it is read by nothing and refused by nothing — and the only value a working deployment could have set it to (`redis`) is exactly what now happens with the variable absent, so leaving it behind cannot degrade anything. `InMemoryBindingStore` still exists, compiled only under `cfg(test)` / the `test-support` feature, so the shared binding-store contract suite can keep running against both backends. |
| ~~`AENV_ROLE`~~ | — | **Removed, and refused.** With the runtime split into the `aenv-api` and `aenv-node` binaries there is no role to select; neither binary declares `--role` or this variable, and a manifest that still passes the flag fails argument parsing before the process starts. Rolling back to one process serving everything is an image-tag change on the DaemonSet (see `services/README.md`), not a flag. |
| `AENV_BINDING_STORE_REDIS_URL` | `redis://127.0.0.1:6379` | Redis connection URL for the binding store. Not optional: Redis is the only binding store `aenv-api` builds, and an unreachable one is a startup failure. |
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
| ~~`AENV_SNAPSHOT_CATALOG_WRITE`~~, ~~`AENV_SNAPSHOT_CATALOG_READ`~~, ~~`AENV_SNAPSHOT_CATALOG_MIRROR_PATH`~~ | — | **Removed.** They steered the object-storage → PostgreSQL catalog migration (double write, mirror backlog, read-side confirmation gate); the PostgreSQL catalog is now the only one and the switches, the mirror and the `catalog_migration_state` table are deleted. **Setting them today does nothing:** no field declares the names, so they are read by nothing and refused by nothing. `services/shared/config/snapshot_catalog_manifest_test.go` fails if any workload under `deploy/k8s/base` still declares one. |
| ~~`AENV_SNAPSHOT_STORE`~~ | — | **Removed. It never worked.** It was declared on `[backend.posix_fs].snapshot_store` and documented here from the day it was written, and the config loader never read it once: confique reaches a field from the environment only through `#[config(nested)]`, `nested` may not be `Option<_>`, and `[backend.posix_fs]` is `Option<PosixFsBackendConfig>`. **Setting it today does nothing**, exactly as it always did: it is read by nothing and refused by nothing. Set `snapshot_store` in a file named by `AENV_CONFIG_OVERLAY_PATH` instead. |
| `AENV_UBLK_DAEMON_BINARY_PATH` | `$AENV_HOME/ublk/uvm-ublk-daemon` | Override path to the `uvm-ublk-daemon` binary |
| `AENV_UBLK_DAEMON_METRICS_LISTEN_ADDR` | `0.0.0.0:9103` | Override ublk daemon Prometheus metrics listen address; empty string disables it |
| `AENV_FORCE_SYSCTL_TUNING` | unset | Set to `1` to force sysctl tuning in a privileged container with writable host sysctls. Normally skipped automatically inside containers. |
| `AENV_FIRECRACKER_WORK_DIR` | `$AENV_HOME/firecracker-work` | Override the parent directory for per-sandbox Firecracker work directories. |
| `AENV_FIRECRACKER_SERIAL_DIR` | `$AENV_HOME/logs/serial` | Override the directory for persistent Firecracker serial output. Files are grouped under `{serial_dir}/{sandbox_id}/`. |
| `AENV_PERSISTED_SANDBOX_STORE_PATH` | `$AENV_HOME/persisted-sandboxes` | Override the node-local scratch root for capture artifacts and node reclaim. Nothing under it survives a pause: a paused sandbox is a row in the snapshot catalog, not a file here. |
| ~~`AENV_PAUSED_REGISTRY_BACKEND`~~ | — | **Removed.** It selected the backend of a cluster-wide paused-sandbox registry (`local`: resumable only on the node that paused it; `postgres`: a `paused_sandboxes` table with leases and reclaim, read over the `[pg]` pool). There is no registry to select any more: a paused sandbox is a sandbox-source row in the snapshot catalog whose committed payload carries its `PausedSandboxConfig`, and resume is a create from that row, so the `paused_sandboxes` and `paused_registry_grace` tables and the `[orchestrator.paused_registry]` section went with the switch. **Setting it today does nothing:** no field declares the name, so it is read by nothing and refused by nothing. |
| ~~`AENV_PAUSED_REGISTRY_DSN`~~ | — | Removed; never existed. Paused-sandbox rows live in the snapshot catalog, which reads the process's own `[pg]` pool; no component ever took a DSN of its own for them (see `AENV_PAUSED_REGISTRY_BACKEND` above). **Setting it does nothing:** it is read by nothing and refused by nothing. The same is true of `AENV_PG_DSN`, another name that has never existed — `[pg]` is `Option<PgConfig>` and confique reaches a field from the environment only through `#[config(nested)]`, which may not be optional, so no key under `[pg]` can carry an `env =` binding at all. Supply `[pg].dsn` through the file `AENV_CONFIG_PATH` names or an `AENV_CONFIG_OVERLAY_PATH` overlay. |

## Egress Broker (`aenv-egress`)

These apply to the egress broker process (`crates/aenv-egress`), the third binary this repository
ships. They override keys of its own configuration file — see
[`aenv-egress.toml`](reference.md#aenv-egresstoml) — not of the server's, and nothing here is read
by `aenv-node` or `aenv-api`.

Two names sit one word apart and mean opposite ends of the same trust relation:
`AENV_EGRESS_CA_CERT_PATH` is the certificate of the CA this process **signs** leaves with, read
alongside its private key; `AENV_EGRESS_BROKER_CA_CERT_PATH` in the Server table above is a
**node's** copy of that certificate, used to verify the broker and handed to guests as an extra
trust anchor. A node never holds the key, and setting either name on the wrong process does
nothing at all.

| Variable | Default | Description |
|----------|---------|-------------|
| `AENV_EGRESS_CONFIG_PATH` | `/etc/aenv-egress/config.toml` | Path to the broker's TOML configuration. `--config` wins over it. |
| `AENV_EGRESS_LISTEN` | `0.0.0.0:8443` | `listen`: where runtimes connect. |
| `AENV_EGRESS_METRICS_LISTEN` | unset | `metrics_listen`: Prometheus scrape address. Unset disables the exporter. |
| `AENV_EGRESS_MAX_SKEW_MS` | `30000` | `max_skew_ms`: identity headers issued outside this window are refused. Keep equal to the nodes' `AENV_EGRESS_BROKER_MAX_SKEW_MS`. |
| `AENV_EGRESS_REPLAY_CAPACITY` | `100000` | `replay_capacity`: nonces remembered inside the skew window. |
| `AENV_EGRESS_ADMISSION_TIMEOUT_MS` | `10000` | `admission_timeout_ms`: one deadline over the TLS handshake and the identity frame behind it — what bounds a peer that has not authenticated yet. |
| `AENV_EGRESS_MAX_CONNECTIONS` | `4096` | `max_connections`: connections held at once; the excess is closed, not queued. |
| `AENV_EGRESS_SHUTDOWN_DRAIN_SECS` | `25` | `shutdown_drain_secs`: how long a shutdown lets live sessions finish. Keep it under the Pod's `terminationGracePeriodSeconds`. |
| `AENV_EGRESS_TLS_CERT_PATH` | from file | `tls.cert_path`: the broker's server certificate. |
| `AENV_EGRESS_TLS_KEY_PATH` | from file | `tls.key_path`: its PKCS#8 PEM private key. |
| `AENV_EGRESS_CA_CERT_PATH` | from file | `ca.cert_path`: the CA this process signs intercepted-name leaves with. Not the node-side variable of the similar name — see above. |
| `AENV_EGRESS_CA_KEY_PATH` | from file | `ca.key_path`: that CA's private key. Held by this process alone; it never reaches a node. |
| ~~`AENV_EGRESS_VAULT_ADDR`~~, ~~`AENV_EGRESS_VAULT_TOKEN_FILE`~~, ~~`AENV_EGRESS_VAULT_MOUNT`~~, ~~`AENV_EGRESS_VAULT_NAMESPACE`~~ | — | **Removed.** They pointed the broker's Vault credential source at the store the api half's `vault` backend wrote to. That backend is gone, and with it the broker's `vault` cargo feature and `[vault]` config section: the broker now has one credential source, an HTTP resolve endpoint. **Setting them today does nothing:** no field declares the names, so they are read by nothing and refused by nothing, and a `[vault]` section in `aenv-egress.toml` is ignored the same way. A broker with no `resolver.url` warns once at startup and answers every credential lookup 502. |
| `AENV_EGRESS_RESOLVER_URL` | unset | `resolver.url`: where the broker resolves one `(sandbox, execution, name)` at a time. With `[secrets].backend = "postgres"` this is the api half itself — `http://agentenv-api:8000/internal` — and the NetworkPolicy has to admit that port. Unset leaves the broker with no credential source and every marker answers 502. |
| `AENV_EGRESS_RESOLVER_TOKEN_FILE` | from file | `resolver.token_file`: a file holding the bearer token, not the token itself. |

`ca.leaf_ttl_secs`, `ca.cache_capacity`, `ca.mints_per_sandbox_per_minute`, `hmac.key_files`,
`upstream.denied_cidrs`, `resolver.timeout_ms`, `resolver.cache_ttl_secs`, `resolver.cache_capacity`, `handlers.echo` and
the `handlers.tcp` / `handlers.postgres` / `handlers.http` tables have no environment binding; set
them in the file.

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
| `GATEWAY_SCHEDULER_ADDR` | `127.0.0.1:9090` | gRPC address of the api half; the gateway dials it for `apiproxy.ResumeSandbox` |
| ~~`GATEWAY_COLD_LOOKUP_TIMEOUT`~~ | — | **Removed.** It bounded the cold-path node-lookup RPC a routing-projection miss and an undecided wake-up fell through to. The gateway makes no such call: a miss goes to `apiproxy.ResumeSandbox`, and an api half that cannot be asked is a 502. **Setting it today does nothing:** the gateway's config loader reads no such key and refuses no such name, and `gateway.cold_lookup_timeout` in a config file is ignored the same way (`services/shared/config/rollback_window_test.go` pins it). The `cold-lookup-config` ConfigMap that carried it is gone from `deploy/k8s/base`. |
| ~~`GATEWAY_QUERY_ONLY_SCHEDULER_ADDR`~~ | — | **Removed.** It named a second Scheduler-protocol endpoint that the gateway's node lookup alone would dial, for HA read traffic served by `services/scheduler`'s `--query-only` replica mode. That Go binary is deleted from the tree, the client-selection path it configured (`QueryOnlySchedulerClient`) went with it, and the gateway dials no scheduler.v1 RPC at all any more. **Setting it today does nothing:** the gateway's config loader reads no such key and refuses no such name. |
| ~~`GATEWAY_SCHEDULER_FALLBACK_DISABLED`~~ | — | **Removed.** It could skip the cold-path node lookup entirely, turning a projection miss into an immediate 503; the lookup it skipped is itself gone. **Setting it today does nothing.** |
| ~~`GATEWAY_SCHEDULER_FALLBACK_TIMEOUT`~~ | — | **Removed.** It was renamed to `GATEWAY_COLD_LOOKUP_TIMEOUT` (above), which has since been removed as well. **Setting either name today does nothing.** |
| `GATEWAY_REQUEST_TIMEOUT` | `30s` | Override the gateway's HTTP request timeout (for example, `1m30s`) |
| `GATEWAY_SANDBOX_PROXY_DOMAINS` | from config | Comma-separated DNS domains that enable gateway host-based sandbox proxy URLs like `{port}-{sandboxID}.{domain}`. Empty or unset keeps `gateway.sandbox_proxy_domains`. |
| `GATEWAY_DEBUG_MODE` | `false` | Enable gateway debug mode |
| ~~`GATEWAY_REST_UPSTREAM_ADDR`~~ | — | **Removed.** It named where the gateway forwarded user-facing REST (the sandbox, snapshot and template routes). The gateway forwards no REST: those routes belong to `aenv-api` at its own address, and a request reaching the gateway that names no sandbox is answered 404. **Setting it today does nothing:** the gateway's config loader reads no such key and refuses no such name, and `gateway.rest_upstream_addr` in a config file is ignored the same way. The `api-upstream-config` ConfigMap that carried it is gone from `deploy/k8s/base`, so a gateway image from before P3 of `docs/proposals/2026-09-01-client-proxy-api-alignment.md` — which refused to start without this key — can no longer be rolled back to by image digest alone. |
| ~~`GATEWAY_ROUTING_PROJECTION_AUTHORITATIVE`~~ | — | **Removed.** It was the gateway's half of the write-side routing-projection switch. The gateway writes no projection: `aenv-api` writes its own on every wake and on every running answer the projection missed, under its own `AENV_BINDING_STORE_PROJECTION_AUTHORITATIVE`, which is untouched. **Setting it today does nothing:** the gateway's config loader reads no such key and refuses no such name, and `gateway.routing.projection_authoritative` in a config file is ignored the same way; so is `gateway.forward_response_size`, which never had an environment variable and was read by nothing. |

