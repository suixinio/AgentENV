# Configuration Reference

AgentENV reads configuration from a TOML file. The default path is `config/default.toml`. Override it with:

```bash
export AENV_CONFIG_PATH=/path/to/config.toml
# or
cargo run -p aenv-node --bin aenv-node -- --config /path/to/config.toml
```

## Layered configuration files

`AENV_CONFIG_OVERLAY_PATH` names extra TOML files that are layered over
`AENV_CONFIG_PATH`, separated by `:` and applied left to right:

```bash
export AENV_CONFIG_OVERLAY_PATH=/etc/agentenv/overlay/oss.toml:/etc/agentenv/secret/oss-credentials.toml
```

**Unset is the default and changes nothing.** A value that is empty or contains
nothing but separators is the same as unset, and empty segments in a real list
are skipped — so a deployment can write `"$(A):$(B)"` and turn one half off by
clearing the value behind it. A file that *is* named and is not on disk is a
startup error, deliberately: the usual reason to name one is a mounted Secret,
and a node that started quietly without it would fall back to the repository's
own defaults and report nothing.

Precedence, lowest to highest:

1. Built-in defaults
2. `AENV_CONFIG_PATH`
3. Each `AENV_CONFIG_OVERLAY_PATH` entry, in order — later entries win
4. Environment variables (`AENV_*`)

The environment stays on top so `kubectl set env` remains a rollback for a value
that arrived in a file nobody can rewrite quickly.

### Merge semantics

The main file and the overlays are merged as TOML documents, key by key, before
any of it becomes configuration:

- Where both sides hold a **table**, the two are merged. A section only the
  earlier file mentions survives a later file that writes into the same section.
- **Everything else replaces**: scalars, strings, and **arrays** — an array is
  taken whole from the last file that sets it, never concatenated.
- There is no way to *remove* a key, only to give it another value.

Deep table merging is the point rather than a detail. It is what lets one
`[backend.oss]` section be assembled out of two files, so that the endpoint,
bucket, region and cache budget can live in a tracked ConfigMap while
`access_key_id` and `access_key_secret` come from a mounted Secret and nowhere
else.

Relative paths inside an overlay are resolved the same way as in the main
file — against the directory containing `AENV_CONFIG_PATH`, not the overlay's
own directory. Prefer absolute paths or the `$AENV_HOME` placeholder.

### When you need this

Most settings can be overridden with an `AENV_*` variable and do not need a
file. Two kinds cannot:

- `[backend.oss]`, `[backend.posix_fs]` and `[pg]`. They are reached as
  `Option<...>`, confique descends into a struct only through
  `#[config(nested)]`, and `nested` may not be `Option<_>` — so no environment
  variable can reach a field inside any of them. An overlay file is the only
  way to set them from outside `AENV_CONFIG_PATH`.
- Anything a deployment overwrites. `deploy/k8s/run.sh` regenerates the cluster
  ConfigMap from `config/default.toml` on every apply, so a value that exists
  only in that ConfigMap is lost on the next one.

## Global Settings

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `home_path` | string | `"/var/lib/aenv"` | Base directory for local AgentENV state. Overridden by `AENV_HOME_PATH` |
| `runtime_path` | string | `"/run/aenv"` | Base directory for transient namespace and daemon-socket state. Overridden by `AENV_RUNTIME_PATH` |
| `deps_path` | string | `"$AENV_HOME/deps"` | Root directory for auto-downloaded runtime assets. Overridden by `AENV_DEPS_PATH` |
| `virtualization_mode` | `"kvm"` or `"pvm"` | `"kvm"` | Virtualization mode for this node. Keep the default unless following the [PVM Deployment](../deployment/pvm.md) guide. Overridden by `AENV_VIRTUALIZATION_MODE` |

Snapshots and paused sandboxes can only be restored in the mode in which they
were created.

`$AENV_HOME` is a literal placeholder in state-path values, not a shell
environment variable. AgentENV replaces it with the resolved `home_path` after
applying `AENV_HOME_PATH`; `ublk.daemon_socket_path` additionally supports
`$AENV_RUNTIME`, which resolves to `runtime_path`. Relative paths without these
placeholders are resolved against the directory containing the configuration
file.

Packaged runtime dependency versions and download URLs live in
`config/deps_manifest.toml`. Only the dependencies for the selected mode are
installed. User configuration should contain runtime behavior and explicit
local path overrides, not the default dependency catalog.

## `[firecracker]`

Firecracker VM binary and boot configuration.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `version` | string | manifest value | Optional Firecracker release override for auto-download |
| `url` | string | manifest value | Optional download URL template override with `{version}` and `{arch}` placeholders |
| `binary_path` | string | derived from manifest/config version | Explicit path to a local `firecracker` binary. Setup skips the Firecracker download and requires this to be a readable, non-empty, executable regular file |
| `boot_args` | string | `"console=ttyS0 reboot=k panic=1 pci=off init=/init …"` | Kernel command line arguments. The shipped default also includes DAMON memory-reclaim parameters; see `config/default.toml` for the full value. |
| `allowed_extra_boot_args_prefixes` | array of strings | `[]` | Allowed prefixes for `extraBootArgs` on cold-start sandboxes. If empty, no request-provided extra boot args are appended |
| `socket_timeout_secs` | integer | `3` | Max seconds to wait for the Firecracker API socket |
| `socket_poll_ms` | integer | `1` | Poll interval (ms) for checking socket availability |
| `work_dir` | string | `"$AENV_HOME/firecracker-work"` | Parent directory for per-sandbox Firecracker work directories. These dirs contain runtime sockets, symlinks, local logs, and writable OverlayBD upper layer data such as `overlaybd/upper.data` and `overlaybd/upper.index` |
| `serial_dir` | string | `"$AENV_HOME/logs/serial"` | Directory for persistent Firecracker serial output (per-sandbox subdirectories) |
| `log_level` | string | unset (disabled) | Optional Firecracker log level (`Error`, `Warning`, `Info`, `Debug`, `Trace`, case-insensitive). When set to a non-empty value, Firecracker's own logging is enabled and written to a `firecracker.log` file in each sandbox's log directory (alongside the serial output). Empty/unset disables it |

## `[kernel]`

Linux kernel image for microVMs.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `version` | string | manifest value | Optional kernel version override for auto-download |
| `url` | string | manifest value | Optional download URL template override with `{version}` placeholder |
| `image_path` | string | derived from manifest/config version | Explicit path to a local `vmlinux.bin`. Setup skips the kernel download and requires this to be a readable, non-empty regular file |

## `[tools]`

Tools drive image used to boot the AgentENV control plane inside each microVM.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `version` | string | manifest value | Immutable SemVer release of the complete tools drive; custom distributions should use a unique prerelease such as `0.1.0-custom.1` |
| `url` | string | manifest value | Optional OCI image URL template override with a `{version}` placeholder; requires an explicit `version` when set |
| `drive_path` | string | unset | Local tools ext4 source imported into the versioned dependency directory; requires an explicit `version` |
| `control_plane_port` | integer | `49983` | Port used by envd inside the guest |

Snapshots and paused sandboxes keep using the tools drive version they were
created with. Launch does not download missing releases: operators must install
the recorded version under `<deps_path>/tools/<version>/tools.ext4` before
restore. Setup retains previously installed versions until they are removed
manually.

## Template Rootfs Images

User-visible rootfs images are selected at the template API layer.
`POST /v2/templates/{templateID}/builds/{buildID}` accepts an optional
`fromImage` field:

- omitted: use `[image.resolver].default_image`
- full OCI reference: use the supplied image
- short name: normalize standard Docker Hub forms such as `ubuntu:24.04`
  and `node:20`

## `[image.resolver]`

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `default_image` | string | `ubuntu:24.04` | Image used when template builds omit `fromImage` |
| `search_registries` | array of strings | `["docker.io", "ghcr.io"]` | Registries tried when resolving short image references |
| `allowed_registries` | array of strings | unset (no restriction) | Whitelist of registry hosts (e.g. `docker.io`, `registry.example.com:5000`). **Omitting the key** imposes no restriction; an **explicit empty list `[]`** denies every registry. When set to a non-empty list, only references whose registry host is in the list resolve; any other host is rejected as a client (4xx, `ImageReferenceError`) error. See *How the three registry settings interact* below. |
| `try_referrers_overlaybd_prefixes` | array of strings | `[]` | Image reference prefixes for which AgentENV tries OCI Referrers API via `regctl` for an overlaybd-native artifact before converting a standard OCI image locally. Prefixes are matched with simple `starts_with`; include the trailing slash yourself, for example `registry.example.com/` or `registry.example.com/team/`. Requires `regctl` on `PATH`; lookup failures fall back to the source image. |

### How the three registry settings interact

Image resolution runs in two phases, and the three keys act at different points:

1. **`search_registries` — completion.** Only used for *short / unqualified*
   references (e.g. `ubuntu`). Each entry is prefixed to the name to build a
   list of fully-qualified candidates (`docker.io/library/ubuntu:latest`,
   `ghcr.io/ubuntu:latest`, …). Fully-qualified references skip this step.
2. **`allowed_registries` — gating.** Applied right after candidates are built,
   to *both* fully-qualified references and the candidates expanded from
   `search_registries`. Candidates whose registry host is not whitelisted are
   dropped; if none remain, the reference is rejected with a 4xx error. In
   effect the resolvable set of short-name hosts is the **intersection** of
   `search_registries` and `allowed_registries` — e.g. searching `docker.io`
   and `ghcr.io` while only allowing `ghcr.io` resolves short names to
   `ghcr.io` only.
3. **`try_referrers_overlaybd_prefixes` — per-candidate optimization.** Runs
   later, while resolving an *already-permitted* candidate: after its manifest
   is fetched, AgentENV may query the OCI Referrers API on the **same**
   registry/repository for an overlaybd-native artifact. Because referrer
   lookups never leave the source image's own host, they are implicitly
   covered by `allowed_registries` — no separate whitelist entry is needed for
   referrers.

   Two referrer `artifactType`s are recognized, in this order:

   | `artifactType` | Produced by |
   |-----|-----|
   | `application/vnd.containerd.overlaybd.native.v1+json` | accelerated-container-image (`obdconv`) |
   | `application/vnd.azure.artifact.streaming.v1` | Azure Container Registry artifact streaming (`az acr artifact-streaming create`) |

   Both point at an overlaybd-native manifest; only the discovery label
   differs. The referrer manifest is re-validated after it is fetched, so a
   referrer that is not actually overlaybd-native is rejected rather than
   used. Turbo-OCI referrers
   (`application/vnd.containerd.overlaybd.turbo.v1+json`) are never selected —
   AgentENV's overlaybd runtime does not implement the turbo read path.

   To stream from ACR, add your registry (with the trailing slash) to the
   list, e.g. `try_referrers_overlaybd_prefixes = ["myregistry.azurecr.io/"]`.

## `[image.cache]`

Node-local cache root for resolved and converted user images.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `root_dir` | string | `"$AENV_HOME/image-cache"` | Root directory for AgentENV image-cache artifacts |
| `capacity_gb` | integer | `100` | Budget for capacity-driven eviction of local commit bytes. Enforced only when `[image.cache.gc].enabled` is `true`: the background GC evicts least-recently-used source configs once usage crosses the high watermark, down to the low watermark. Unset = no capacity cap. |

## `[image.cache.gc]`

Background hard-commit garbage collection for the image cache. When enabled,
each pass reconciles metadata from the on-disk source configs and then deletes
hard-commit objects that are no longer rooted by source configs, held by
image-cache leases, or referenced by the in-process running set. Committed
snapshots are durable SnapshotRepository state and do not pin ImageCache
commits. With `capacity_gb` set, GC first evicts least-recently-used source
configs over the high watermark so hard-commit GC can reclaim what they unrooted.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enabled` | bool | `true` | Enable the background image-cache GC task |
| `interval_secs` | integer | `1800` | Seconds between GC passes (a value `<= 0` falls back to the default) |
| `min_age_secs` | integer | `600` | Minimum time since last use before a source config is eligible for capacity eviction (the LRU floor) |
| `high_watermark_ratio` | float | `0.95` | Begin capacity eviction once local commit bytes exceed `capacity_gb` × this ratio. Clamped to `(0, 1]` |
| `low_watermark_ratio` | float | `0.70` | Evict down to `capacity_gb` × this ratio once the high watermark trips. Clamped to `(0, high_watermark_ratio]` |

Capacity-driven eviction runs only when `[image.cache].capacity_gb` is set;
otherwise the GC still reclaims unreachable commits but performs no watermark
eviction.

## `[image.cache.remote_blocks]`

Overlaybd registryfs_v2 remote block cache settings. The directory is always
`<image.cache.root_dir>/remote-blocks`.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `max_size_gb` | integer | `100` | Maximum size of the overlaybd remote block cache in GiB. This value is written to generated overlaybd `cacheConfig.cacheSizeGB` |

Resolved image data is cached under:

```text
<image.cache.root_dir>/
  commits/
    <sha256-commit-digest>/
      overlaybd.commit
                  # full overlaybd commit store shared by OCI conversion and download
  indexes/        # OCI layer + conversion context -> overlaybd commit descriptor
  remote-blocks/  # overlaybd-native remote block cache
  configs/         # resolved image configs
    <slug>-<hash>-image.json
```

## `[sandbox_proxy]`

Optional host-based data-plane routing for sandbox services.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `domains` | array of strings | `[]` | DNS domains accepted by the server for host-based proxy URLs shaped like `{port}-{sandboxID}.{domain}`. The first configured domain is returned in sandbox create/detail responses as `domain`. |

When `domains` is empty, the server still supports `/proxy` and routing-header
proxy requests, but does not classify requests by `Host`. Domains are normalized
to lowercase, deduplicated, and must be valid DNS names. The configured order is
preserved because `domains[0]` is the advertised sandbox domain.

Environment variable override:

- `AENV_SANDBOX_PROXY_DOMAINS`

## `[network.egress]`

Node-level sandbox egress guardrails. These rules are installed before
per-sandbox `allowOut` / `denyOut` rules, so sandbox API requests cannot
override them.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `allow_internal_cidrs` | array of CIDR strings | `[]` | Subnets of the always-denied table below that per-sandbox `allowOut`/`denyOut` decides about. Every entry must sit inside one table entry; anything else is refused at startup, and each accepted entry is logged at `info` when the process starts. |

The always-denied table is a constant: `10.0.0.0/8`, `100.64.0.0/10`,
`127.0.0.0/8`, `169.254.0.0/16`, `172.16.0.0/12`, `192.168.0.0/16`, `::1/128`,
`fc00::/7`, `fe80::/10`. Everything in it is rejected before user egress policy
is evaluated, except for the subnets `allow_internal_cidrs` names. The IPv6
entries never reach an iptables rule: sandbox namespaces run with IPv6 disabled
and IPv6 entries in `allowOut`/`denyOut` are refused with 400.

## `[network.internal]`

AgentENV-internal sandbox address plan. Change these only when the defaults
overlap with host or deployment network ranges.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `host_interaction_cidr` | IPv4 CIDR | `10.11.0.0/16` | Per-slot host interaction address pool. Must contain at least 32768 addresses. |
| `veth_cidr` | IPv4 CIDR | `10.12.0.0/16` | Per-slot namespace veth pair pool. Must contain at least 65536 addresses. |

The two configured CIDRs must not overlap each other or AgentENV's fixed VM tap
link `169.254.0.20/30`. These networks are also treated as reserved sandbox
egress destinations regardless of `allow_internal_cidrs`.

## `[machine]`

Default VM resources for sandboxes.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `mem_size_mib` | integer | `1024` | Guest RAM in MiB |
| `vcpu_count` | integer | `2` | Number of virtual CPUs |

## `[envd]`

In-guest `envd` daemon settings.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `version` | string | `"0.6.13"` | Expected envd version baked into the tools drive image |
| `init_timeout_secs` | integer | `60` | Max seconds to wait for envd to become ready after VM start |
| `poll_ms` | integer | `3` | Poll interval (ms) for envd health check retries |

## `[sandbox]`

Sandbox control communication settings.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `access_token_hash_seed` | string | auto-generated | Optional override for the secret used to derive secure sandbox envd access tokens. When unset, normal server startup creates and reuses `$AENV_HOME/secrets/sandbox-access-token-hash-seed`. Configure an explicit shared value when the deployment needs to recover the same sandbox ID on another node. |

The managed seed is node-local persistent state and must be included in backups of `$AENV_HOME`. AgentENV refuses to generate a replacement when persisted secure sandboxes exist. An explicit environment or TOML value takes precedence over the managed file; changing that effective value invalidates access tokens for existing secure sandboxes.

Configure `AENV_SANDBOX_ACCESS_TOKEN_HASH_SEED` with the same value on every node when cross-node recovery of the same sandbox is required. Nodes use their own managed seed when it is unset.

`aenv-api` does not have that choice: it refuses to start when no seed is configured, rather than generating one. An API replica that invented its own would mint envd tokens its siblings cannot derive, and nothing about that is visible — the user is handed a token by whichever replica answered and it stops working when another one does. `aenv-node` still generates a managed seed.

Every role publishes `agentenv_access_token_seed_fingerprint{fingerprint="..."} 1` at startup, where the label is the first eight bytes of `SHA-256(seed)` in hex. It is what makes "every process in this cluster holds the same seed" answerable from a scrape: the required-seed check above catches a *missing* seed, but two replicas each configured with a different non-empty value pass every check and still disagree. Compare the label across processes; the seed itself never appears in a log or a metric.

## `[orchestrator]`

Sandbox lifecycle management.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `auto_evict_interval_ms` | integer | `1000` | Poll interval (ms) for background timeout eviction |
| `default_sandbox_timeout_secs` | integer | `15` | Default keep-alive timeout for sandboxes |
| `auto_resume_min_sandbox_timeout_secs` | integer | `300` | When a data-plane request targets a non-running sandbox, automatically resume it (if auto-resume is enabled) and refresh its timeout for no-less than this duration |
| `persisted_sandbox_store_path` | string | `"$AENV_HOME/persisted-sandboxes"` | Node-local scratch root for capture artifacts and node reclaim. Nothing under it survives a pause: the pause's bytes are staged on the snapshot repository and committed to the catalog, and startup reclaim sweeps the directory |

## `[pool]`

Shared process-wide warm-pool defaults used by network slots, block devices, and pre-spawned Firecracker processes. Pools prewarm to the low watermark, then grow the refill target geometrically toward the high watermark when real acquisitions drain the pool.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `low_watermark` | integer | `2` | Initial lower bound for all enabled warm-resource pools |
| `high_watermark` | integer | `64` | Maximum idle target for all enabled warm-resource pools |

Component sections:

| Section | Key | Type | Default | Description |
|---------|-----|------|---------|-------------|
| `[pool.network]` | `maintenance_enabled` | boolean | `true` | Enable the background network-slot maintenance worker |
| `[pool.block]` | `enabled` | boolean | `true` | Enable the ublk overlaybd warm-device pool |
| `[pool.block]` | `startup_prewarm` | boolean | `true` | Prewarm block devices after the first reusable image shape is known |
| `[pool.firecracker]` | `enabled` | boolean | `true` | Enable pre-spawned Firecracker processes for snapshot resume |
| `[pool.firecracker]` | `maintenance_enabled` | boolean | `true` | Enable the background Firecracker process maintenance worker |
| `[pool.firecracker]` | `startup_prewarm` | boolean | `true` | Spawn warm Firecracker entries up to the low watermark during server startup |
| `[pool.firecracker]` | `fill_concurrency` | integer | `4` | Maximum number of warm Firecracker processes created concurrently by one maintenance refill batch |

Validation rules:

- `low_watermark <= high_watermark`
- `[pool.firecracker].fill_concurrency > 0`

## `[node_identity]`

Stable identity fields for this node. These values appear in node API responses
and scheduler heartbeats.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `node_id` | string | hostname-derived | Stable node identifier returned by the admin/node APIs |
| `cluster_id` | string (UUID) | nil UUID | Logical cluster identifier included in node snapshots |
| `service_instance_id` | string | generated UUID | Unique process/service instance identifier for the current node runtime |

## `[observability]`

Node-level observability and host metrics collection.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enabled` | boolean | `true` | Enable the node/admin observability service. When disabled, `/nodes` returns an empty list and `/nodes/{nodeID}` returns `404` |

When observability is enabled, host CPU/memory/disk metrics are collected at request time. CPU percent is computed from two samples; the first node metrics request waits about 100ms to return a measured value.

## `[observability.scheduler_report]`

Optional scheduler heartbeat reporting for multi-node control plane integration.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enabled` | boolean | `false` | Enable periodic scheduler heartbeat reporting. Requires `[cluster].scheduler_endpoint` |
| `interval_secs` | integer | `5` | Heartbeat report interval in seconds |

Environment variable overrides:

- `AENV_OBSERVABILITY_SCHEDULER_REPORT_ENABLED`
- `AENV_OBSERVABILITY_SCHEDULER_ENDPOINT`
- `AENV_OBSERVABILITY_REPORT_INTERVAL_SECS`

`AENV_OBSERVABILITY_SCHEDULER_ENDPOINT` overrides `[cluster].scheduler_endpoint` for the reporter process only.

## `[cluster]`

Shared cluster-level service endpoints. 🔴 This table is not exhaustive — see
`docs/src/configuration/env-vars.md`'s
`AENV_BINDING_STORE_*` / `AENV_CLUSTER_*` entries and `config/default.toml`'s
`[cluster]` comments for the full set of keys (node service addresses,
warmup timeout, Kubernetes discovery sub-table, and more).

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `scheduler_endpoint` | string | unset | gRPC endpoint for the scheduler, for example `"http://agentenv-api:8002"`. Used by scheduler heartbeat reporting and P2P peer discovery. |
| `node_discovery_mode` | string | `"kubernetes"` | Which discovery strategy seeds `aenv-api`'s own in-process node registry — built unconditionally: `"kubernetes"` or `"static"`. See `AENV_CLUSTER_NODE_DISCOVERY_MODE` in `env-vars.md`. |
| `static_discovery_nodes` | array of `{id, endpoint}` | `[]` | Statically-configured node list, read only when `node_discovery_mode = "static"`. **TOML-file-only — no `env =` binding.** Set it in the file `AENV_CONFIG_PATH` names, or via an `AENV_CONFIG_OVERLAY_PATH` overlay (`deploy/docker/config/cluster-static-discovery-overlay.toml` is a tracked working example). |

## `[p2p]`

> **Experimental:** P2P has not been tested in production. Keep it disabled in
> production unless the deployment accepts that operational risk.

Project-wide artifact transport configuration. The transport is disabled by
default. When enabled, it is used by the overlaybd P2P HTTP facade and by
snapshot publication/runtime resolution as an optional acceleration path.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enabled` | boolean | `false` | Enable the P2P artifact transport. When false, AgentENV uses `DisabledP2pTransport`, so lookups miss and publishes are no-ops. |
| `transport` | string | `"iroh"` | Transport backend. Supported values are `"disabled"` and `"iroh"`. Ignored while `enabled = false`. |
| `store_dir` | string | `"$AENV_HOME/p2p/store"` | Local store used by the transport backend. Relative explicit paths are resolved against the config file directory. |
| `listen_addr` | string | `"0.0.0.0:0"` | Optional local listen address for the embedded transport endpoint. Port `0` lets the OS choose a free port. |
| `lookup_timeout_ms` | integer | `5000` | Timeout for one artifact catalog lookup against a peer. |
| `fetch_timeout_ms` | integer | `30000` | Timeout for fetching one artifact from a peer. |
| `peer_discovery_refresh_interval_secs` | integer | `5` | Interval for refreshing peer endpoints from scheduler. Values below one second are clamped to one second. |

## `[custom_extension]`

Custom extension service configuration. When `url` is unset, the integration is fully disabled. See [Custom Extension](../concepts/custom-extension.md).

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `url` | string | unset | HTTP base URL of the custom extension service. When set, AgentENV invokes sandbox lifecycle hooks under `POST {url}/sandbox-hook/*`. |
| `timeout_ms` | integer | `5000` | Timeout for each custom extension HTTP call, in milliseconds. |

## `[api]`

The node's own HTTP API and the credential this half presents to another node's gRPC gate. On `aenv-node` the credentials gate its REST and gRPC surfaces; on `aenv-api` no gate is attached and the value is only what the node client stamps outbound.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `control_plane_tokens` | array of strings | `[]` | Static credentials, accepted together so one can be rotated out. Empty together with `control_plane_token_file` leaves the gate open. |
| `control_plane_token_file` | path | `""` | The same credentials as a file, one per line, re-read when it changes. The effective set is the union with the static tokens. An unreadable file keeps the last set that was read: clearing the file is how a gate is turned off deliberately. |
| `node_client_token_file` | path | `""` | The credential this half presents at another node's gate, when it is not one of the credentials it accepts itself. Empty falls back to `control_plane_token_file`'s first line, and to a static token only when there is no file — a rotation moves the file, while the static list keeps the retired credential so nodes still accept what is in flight. An unreadable or empty file falls back and says so, rather than presenting nothing. |

## `[api.proxy]`

The data-plane reverse proxy, on the half that serves one (`aenv-node`).

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `max_incoming_per_sandbox` | integer | `0` | Requests one sandbox may have in flight through the proxy at once; the excess is refused with 429. `0` does not limit — the sandbox's own service decides what it can take. |

## `[egress_broker]`

How a node reaches the egress broker that serves sandboxes declaring `network.rules` or `network["x-aenv-endpoints"]`. A sandbox with rules is placed only on nodes whose heartbeat reports a usable broker; with `mode = "disabled"` this node reports none. See the proposal in `docs/proposals/2026-09-03-sandbox-egress-credential-brokering.md`.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `mode` | string | `"disabled"` | `disabled`, `embedded` (the broker core runs inside `aenv-node`; only valid with `[cluster].node_discovery_mode = "static"` and at most one static node) or `local` (a Unix socket to the `aenv-egress-node` DaemonSet on this same node). `remote` is refused at startup: the value no longer parses. |
| `socket_path` | path | unset | The broker's Unix socket on this node. Required in `local` mode; both DaemonSets mount its directory as the same `hostPath`. |
| `guest_ca_cert_path` | path | unset | PEM bundle guests with rules trust for intercepted names. It is the **root**, not this node's issuer: a sandbox that resumes on another node keeps the trust store it loaded before it moved. |
| `socket_group` | integer | `65532` | The gid the broker runs under, which `socket_path`'s directory must carry with mode `0770` before this node starts in `local` mode. The broker's init container is what chowns it, so this must match `runAsGroup` in `deploy/k8s/base/aenv-egress-daemonset.yaml`; `services/shared/config/egress_socket_group_manifest_test.go` holds them together. A node waits up to sixty seconds and then refuses to start, naming the path — it never chowns the directory itself. |
| `per_sandbox_conns` | integer | `256` | Concurrent brokered connections one sandbox may hold; excess connections are closed. |
| `node_conns` | integer | `20000` | Concurrent brokered connections across the node. |
| `open_timeout_ms` | integer | `3000` | How long the runtime waits for the broker to accept one connection. |

## `[secrets]`

Where `aenv-api` keeps secret values and grants for `/secrets`, `network.rules` and endpoint credentials. Only the api half reads this section. With `backend = "disabled"`, `/secrets` answers 503 and rules that reference secrets are refused.

With `backend = "postgres"` the values live in the same PostgreSQL that holds their names and versions, encrypted with AES-256-GCM under a master key this process reads from a file, and `aenv-api` also serves the broker's resolve endpoint. It is the only backend, and the only credential store a deployment needs beside AgentENV itself.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `backend` | string | `"disabled"` | `disabled` or `postgres`. |

## `[secrets.pg]`

Values in `aenv-api`'s own PostgreSQL, AES-256-GCM under one master key. The key is a path, never an inline value: an environment variable holding a master key is readable from `/proc`, a crash dump and `kubectl describe`.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `key_file` | path | unset | File holding the base64 32-byte master key. Required when `backend = "postgres"`. Mount it from a Secret; it is never written to the database holding the ciphertexts, and losing it loses every stored value. |

## `aenv-egress.toml`

The egress broker is a separate process with its own configuration file, not a section of the
file above: `aenv-egress --config <path>`, defaulting to `$AENV_EGRESS_CONFIG_PATH` and then
`/etc/aenv-egress/config.toml`. `deploy/k8s/base/config/aenv-egress.toml` is the shipped one, and
every path in it names a volume the broker DaemonSet mounts. See
[Egress Credentials](../concepts/egress-credentials.md).

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `listen.socket_path` | path | `/run/aenv-egress/broker.sock` | The Unix socket the node on this machine connects to. A stale socket from a previous run is unlinked; the bound one is chmodded `0660`. |
| `listen.peer_uid` | integer | `0` | The uid `aenv-node` runs as. A connection from any other non-root process on this machine is closed before its header is read. It is not a boundary against root, which can reach the socket whatever its mode says. |
| `metrics_listen` | address | unset | Prometheus scrape address; unset disables the exporter. |
| `admission_timeout_ms` | integer | `10000` | The deadline for the identity frame. Nothing on a connection is admitted until it arrives, so this is what bounds a peer that has not been admitted yet. |
| `max_connections` | integer | `4096` | Connections held at once. The excess is closed, not queued. |
| `per_sandbox_connections` | integer | `256` | Connections one sandbox holds at once, inside the limit above. |
| `shutdown_drain_secs` | integer | `25` | How long a shutdown lets live sessions finish before it stops waiting. Keep it under the Pod's `terminationGracePeriodSeconds`. |
| `ca.cert_path` | path | unset | The root that signs every intercepted-name leaf, mounted from the `egress-ca` Secret. It is the same certificate each node hands its guests as `[egress_broker].guest_ca_cert_path`: the chain on the wire is one leaf, and what verifies it is already in the trust store. |
| `ca.key_path` | path | unset | That root's private key. Required together with `ca.cert_path` — a broker holding half the pair refuses to start rather than serving certificates no guest trusts. |
| `ca.leaf_ttl_secs` | integer | `86400` | Lifetime of a minted leaf certificate. |
| `ca.cache_capacity` | integer | `4096` | How many minted leaves are kept before the oldest name is evicted. |
| `ca.mints_per_sandbox_per_minute` | integer | `60` | Per-sandbox signing budget. With match-before-sign this is what bounds the leaves an unconstrained CA can be made to issue. |
| `upstream.denied_cidrs` | array of CIDR strings | `[]` | Destinations nothing reaches through the broker. These are absolute: unlike the built-in private ranges, no per-handler `allowed_cidrs` reopens them, so a range that must never be reachable belongs here. The cluster's Service and Pod CIDRs are the ones that do. |
| `resolver.url` | string | unset | Base URL the broker resolves credentials against. With `[secrets].backend = "postgres"` that base is `aenv-api` itself — `http://agentenv-api:8000/internal` — and the NetworkPolicy has to admit it. Unset leaves the broker with no credential source: it warns once at startup and every marker answers 502. |
| `resolver.token_file` | path | unset | File holding the token presented on resolver calls: this Pod's projected ServiceAccount token for the `aenv-api` audience, the only credential that endpoint accepts. Required whenever `resolver.url` is set — the endpoint checks it on every call, so a broker without one would have every lookup refused, and startup fails instead. Read on every call, because kubelet rotates a projected token in place. A token the api half cannot resolve to a node is answered 401, which the broker reports as an outage (502 to the guest) and both halves log. |
| `resolver.timeout_ms` | integer | `5000` | Timeout for each resolver call. |
| `resolver.cache_ttl_secs` | integer | `10` | How long a resolved credential is reused. It bounds how long a revocation takes to bite, and it is the only bound there is: the api half has no reverse channel to a broker, so a revoked grant is noticed on the next lookup and not before. |
| `resolver.cache_capacity` | integer | `4096` | Credentials held at once. Expired entries leave on every insert and the soonest to expire is dropped at capacity, so credential bytes are not retained past the TTL. |
| `handlers.echo` | boolean | `false` | The `echo` identity handler, for smoke tests. Off in production. |
| `handlers.tcp.enabled` | boolean | `false` | The `tcp` byte relay, which endpoint declarations name. |
| `handlers.tcp.allowed_cidrs` | array of CIDR strings | `[]` | The only destinations `tcp` reaches. An enabled handler with an empty list reaches nothing: a declaration names the upstream, so the operator names where declarations may point. A range named here is reached even when it is private, because the upstream is the operator's choice and the guest never addressed it — neither the built-in private ranges nor the sandbox's own egress policy bounds it. It still cannot reach the broker's own host, link-local (cloud metadata), or anything in `upstream.denied_cidrs`. **TOML-file-only — no `env =` binding.** |
| `handlers.postgres.enabled` | boolean | `false` | The `postgres` handler, which terminates the guest's startup exchange and authenticates upstream with the brokered credential. |
| `handlers.postgres.allowed_cidrs` | array of CIDR strings | `[]` | The only destinations a `postgres` credential may point at, reading exactly as `handlers.tcp.allowed_cidrs` above. An enabled handler with an empty list reaches nothing; a tenant database on a private address belongs here. **TOML-file-only — no `env =` binding.** |
| `audit.level` | string | `"metadata"` | `metadata` writes one JSON line per brokered request to stdout under the `egress.audit` tracing target, plus `security_event` and `tls_handshake` lines; `none` writes none of it. Metadata means what it says: injected headers are recorded by name and never by value, and the query string is not recorded at all. |

Environment variable overrides for this file are listed under
[Egress Broker](env-vars.md#egress-broker-aenv-egress).

## `[snapshot]`

Snapshot storage/build configuration.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `local_cache_path` | string | `"$AENV_HOME/snapshot-local-cache"` | Manager-owned node-local snapshot artifact/cache root. Relative explicit paths are resolved against the config file directory. |
| `repository_backend` | string | `"posix_fs"` | Snapshot repository backend — where snapshot **bytes** live. Supported values: `"posix_fs"` and `"oss"`. Neither holds catalog rows: the catalog is PostgreSQL (`[pg]`). |
| `p2p_enabled` | boolean | `true` | When enabled, the snapshot manager publishes committed snapshots to the P2P transport and attempts to resolve from it before falling back to the repository backend. |

Environment variable overrides:

- `AENV_SNAPSHOT_LOCAL_CACHE_PATH`

## `[snapshot.image_publish]`

Source-registry image publication. Only takes effect when `snapshot.repository_backend = "oss"`.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enabled` | boolean | `false` | When enabled, publishing a snapshot also pushes its rootfs as an OverlayBD-native OCI image tag `agentenv-snapshot-{snapshot_id}` to the original source registry. Requires source images to be OverlayBD-native in that registry and push credentials in the Docker config (`~/.docker/config.json`). Existing remote layers are referenced by digest; only new delta layers are uploaded. The published reference is exposed as `imageRef` in snapshot APIs. Memory and VM-state artifacts always remain in the snapshot repository. |

## `[backend.posix_fs]`

POSIX filesystem-backed snapshot repository configuration. This section is used when `snapshot.repository_backend = "posix_fs"`.

🔴 A **byte** repository, not a catalog. It stores the memory image, rootfs and
attached-drive layers and `vm_state.bin`; the rows that name them are in
PostgreSQL (`[pg]`), which `aenv-api` requires. Object storage and this backend
both held a catalog until the Stage B cutover and neither does now, so there is
no PostgreSQL-free deployment.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `snapshot_store` | string | `"$AENV_HOME/snapshot-store"` | Root directory for durable committed snapshot repository state. Relative explicit paths are resolved against the config file directory. |

No environment variable reaches this section. `[backend.posix_fs]` is
`Option<PosixFsBackendConfig>` and confique never reads a non-`nested` `Option`
from the environment, so `AENV_SNAPSHOT_STORE` — which was declared here and
documented for years — never had any effect and has been removed. Use
[an overlay file](#layered-configuration-files) to set `snapshot_store` from
outside `AENV_CONFIG_PATH`.

## `[backend.oss]`

OSS-backed snapshot repository configuration. This section is required when `snapshot.repository_backend = "oss"`.

🔴 A **byte** repository, not a catalog — the same note as `[backend.posix_fs]`
above. The bucket held `catalog/records/*.json` and `catalog/aliases/*.json`
until the Stage B cutover; it holds byte artifacts alone now, and the rows are
in PostgreSQL (`[pg]`). Objects left under those prefixes on an existing bucket
are a frozen copy nothing reads.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `endpoint` | string | none | OSS endpoint URL, for example `"https://oss-cn-hangzhou.aliyuncs.com"` |
| `bucket` | string | none | OSS bucket name used for committed snapshot state |
| `prefix` | string | empty | Optional object key prefix under the bucket |
| `credential_process` | string | unset | External command used to fetch OSS credentials. Use a plain executable-plus-args form without shell expansion, pipes, or command substitution so it behaves consistently across AgentENV and overlaybd credential consumers |
| `access_key_id` | string | unset | Static OSS access key ID. Required when `credential_process` is not set |
| `access_key_secret` | string | unset | Static OSS access key secret. Required when `credential_process` is not set |
| `security_token` | string | unset | Optional session token paired with static access key credentials |
| `region` | string | none | Region passed to the S3-compatible object-store client; required for current OSS backend |
| `cache_max_size_gb` | integer | `10` | Maximum size of the node-local OSS artifact cache in GiB |

No environment variable reaches this section either, for the same reason as
`[backend.posix_fs]` above — including the credentials, which is why there is no
`secretKeyRef` form of them. Supply the section through
[an overlay file](#layered-configuration-files); because the merge is deep, the
endpoint, bucket, region and prefix can come from a tracked file while
`access_key_id` and `access_key_secret` come from a separate mounted Secret.

Notes:

- `credential_process` and static access key settings are mutually exclusive in practice; when `credential_process` is set, the backend ignores static credential fields.
- `credential_process` should be written as a portable argv-style command line. Avoid `$VAR`, backticks, `$(...)`, pipes, and shell builtins.
- Although the config section is still named `oss`, the runtime path is implemented via a shared S3-compatible client, so `region` must be configured.

## `[pg]`

Shared PostgreSQL connection settings for the control plane (`aenv-api`),
consumed by `crates/aenv-api/src/pg/mod.rs`: a per-replica connection pool and
a cluster-leadership primitive built on session-scoped advisory locks.

**Required for `aenv-api`.** The pool backs the committed-snapshot catalog,
which is PostgreSQL and nothing else — object storage held a catalog until the
Stage B cutover and holds byte artifacts alone now — so an api replica with no
`dsn` has no catalog and refuses to start rather than answering "no such
snapshot" to everything. The refusal is at construction: `build_pg_pool`
(`crates/aenv-api/src/bin/aenv-api.rs`) yields a pool or an error, never
"no pool", so startup stops before the catalog build reaper is wired rather
than after. The same catalog is where a paused sandbox lives: a sandbox-source
snapshot row whose committed payload carries the sandbox's configuration, read
back when the sandbox is resumed, listed, or deleted. It also backs the
`aenv-snapshot-image` operator tool. See
`deploy/k8s/base/agentenv-api-deployment.yaml` for the deployed shape.

🔴 `aenv-node` must never hold it: that binary links no PostgreSQL client and
refuses to start if `dsn` is configured.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `dsn` | string | unset | A libpq-style connection URL (`postgres://user:password@host:port/dbname`). Absent or blank means PostgreSQL is not configured for this process. |
| `max_connections` | integer | `8` | Per-replica pool cap. `aenv-api` runs more than one replica and each builds its own pool independently, so the cluster-wide connection count this deployment produces is `replica_count * max_connections`, not this number alone — keep it comfortably under PostgreSQL's own `max_connections`. |
| `connect_timeout_secs` | integer | `5` | Bounds the pool's initial connection attempt and every later acquire. |

No environment variable reaches this section, for the same reason as
`[backend.posix_fs]`/`[backend.oss]` above: `[pg]` is `Option<PgConfig>`, and
confique never reads a non-`nested` `Option` from the environment. Supply the
section through [an overlay file](#layered-configuration-files) — `dsn` is a
credential and must never be written into a tracked file such as
`config/default.toml`, the same rule `[backend.oss]`'s `access_key_id` and
`access_key_secret` follow.

### 🔴 `dsn` must never reach `aenv-node`

`aenv-node` runs user-submitted code; database credentials, the connection
budget and the schema belong to the deciding half only. Two things enforce
that, and they are not redundant: `aenv-node` does not link a PostgreSQL
client at all (`make check-crate-boundaries`), so it *cannot* use a DSN; and
it refuses to start if `[pg].dsn` resolves to a non-blank value
(`refuse_configured_pg_dsn`, checked in
`crates/aenv-node/src/bin/aenv-node.rs` before anything is assembled), so a
DSN cannot be left sitting unused on a machine that runs user code. `aenv-api`
may configure `[pg]` freely.

Other path override:

- `AENV_DEPS_PATH`

Setup sysctl tuning is host-level setup. It is skipped before reading `/proc/sys`
when the server detects that it is running inside a container. Set
`AENV_FORCE_SYSCTL_TUNING=1` only for a privileged container with writable
host sysctls; otherwise configure these kernel parameters on the host.

## `[protoc]`

Protobuf compiler metadata for code generation lives in
`config/deps_manifest.toml`, not `config.toml`.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `version` | string | `"33.4"` | protoc release version |
| `url` | string | GitHub release URL | Download URL template with `{version}` and `{platform}` placeholders |

## `[ublk]`

Userspace block device configuration. Rootfs is served through an OverlayBD-backed ublk device managed by `uvm-ublk-daemon`.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `daemon_binary_path` | string | `"$AENV_HOME/ublk/uvm-ublk-daemon"` | Path to the `uvm-ublk-daemon` binary |
| `daemon_socket_path` | string | `"$AENV_RUNTIME/ublk-daemon.sock"` | Unix socket path used by the daemon |
| `daemon_log_path` | string | `"$AENV_HOME/logs/ublk-daemon.log"` | File path for daemon logs; deployments are responsible for rotation and retention |
| `daemon_metrics_listen_addr` | string | `"0.0.0.0:9103"` | HTTP listen address for daemon Prometheus metrics; empty string disables it |
| `transport` | string | `"ublk"` | Kernel block transport devices are exposed through: `ublk` (ublk_drv, kernel 6.8+) or `nbd` (in-tree nbd module; the daemon needs `CAP_SYS_ADMIN`) |

Environment variable override:

- `AENV_UBLK_DAEMON_BINARY_PATH`
- `AENV_UBLK_DAEMON_METRICS_LISTEN_ADDR`
- `AENV_UBLK_TRANSPORT`

## `[ublk.nbd]`

Settings that apply only when `[ublk].transport = "nbd"`.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `connections` | integer | `4` | Sockets per device; each becomes a kernel hardware queue
| `io_timeout_secs` | integer | `90` | Kernel request timeout. After it the kernel takes that connection down and retries the request on the one the daemon puts in its place; a request the kernel gives up on fails with `EIO`. Keep it above the overlaybd registry request timeout (30 s per request) with room for its retries
| `dead_conn_timeout_secs` | integer | `30` | How long a request with no live connection waits for that replacement before failing with `EIO`. `0` fails it at once, which leaves one stalled request enough to turn a sandbox's disk into permanent `EIO`

## `[ublk.overlaybd]`

OverlayBD configuration for ublk. Legacy `enabled` and `device_type` keys are ignored.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `global_config_path` | string | `"$AENV_HOME/overlaybd/overlaybd-global.json"` | Path to overlaybd global config JSON (see note below). Relative explicit paths are resolved against the config file directory. |
| `read_only` | boolean | `false` | When set to `true`, materializes the rootfs without a writable upper |
| `runtime_upper_mode` | string | `"hybridLogStructured"` | Runtime upper format for newly materialized writable rootfs OverlayBD images. Supported values are `"logStructured"`, `"hybridLogStructured"`, and `"sparse"`. Existing source uppers keep their own mode |
| `allow_shrink` | boolean | `false` | Allows an explicit cold-start `diskSizeMB` smaller than the source rootfs. Explicit sizes use MiB and must be divisible by 1024. Growth is always allowed; snapshot resume never resizes. |
| `resize_timeout_secs` | integer | `120` | Timeout in seconds for the cold-start OverlayBD resize tool. Must be greater than zero. |
| `download_enable` | boolean | `false` | Enables overlaybd layer-level background download for remote layers |
| `p2p_lookup_timeout_ms` | integer | `300` | Timeout for one foreground Overlaybd descriptor lookup through the localhost P2P HTTP facade. Timeout is treated as a cacheable miss. |
| `p2p_fetch_range_timeout_ms` | integer | `2000` | Timeout for one foreground Overlaybd range fetch through the localhost P2P HTTP facade before falling back to the origin registry. |

### `global_config_path` and auto-generated config

The file at the configured default path
`$AENV_HOME/overlaybd/overlaybd-global.json` is **auto-generated** by the server
at startup. The generated JSON incorporates several TOML settings —
`[image.cache].root_dir`, `[image.cache.remote_blocks].max_size_gb`,
`download_enable`, `[backend.oss]` credentials, and Docker registry credentials
detected from `~/.docker/config.json` — into a single overlaybd runtime config
file.

The server regenerates the file at `global_config_path` on every startup, so
these TOML settings always take effect automatically — any manual edits to the
generated file are overwritten on the next startup. To keep customizations,
make them through the TOML settings, not by editing the generated JSON.

## `[memory_snapshot]`

Memory snapshot overlaybd configuration. The server auto-generates the file at
the default path on every startup.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `overlaybd_global_config_path` | string | `"$AENV_HOME/overlaybd/mem-overlaybd-global.json"` | Path to the overlaybd global config used for the memory-snapshot ublk backend. Regenerated at startup (manual edits are overwritten); change only to relocate the generated file. |
| `backend` | string | `"block"` | Path snapshot memory is restored through: `block` mmaps a read-only device (ublk or nbd per `[ublk].transport`) built from the stacked memory layers, `uffd` has the ublk daemon answer Firecracker's page faults over a Unix socket and creates no device. `uffd` requires `track_dirty_pages = true`, which is refused under PVM, so `uffd` is KVM-only. |
| `track_dirty_pages` | bool | `false` | Enable Firecracker KVM dirty-page tracking for memory snapshots. It defaults to false. The option is temporarily disabled in PVM mode because this combination has not been tested. Memory snapshot packaging always uses the direct OverlayBD path. Set `AGENTENV_MEMORY_SNAPSHOT_TRACK_DIRTY_PAGES=true` to enable it. |
| `compression_enabled` | bool | `false` | Enable compression for memory snapshot layers. When disabled, `compression_algorithm` is still parsed but has no effect. This setting affects only memory layers; the physical file name remains `overlaybd.commit`. |
| `compression_algorithm` | string | `"lz4"` | Compression algorithm for memory snapshot layers. Valid values are only `lz4` and `zstd`. |

Environment variable override:

- `AENV_MEMORY_SNAPSHOT_BACKEND`
- `AGENTENV_MEMORY_SNAPSHOT_TRACK_DIRTY_PAGES`

## `[memory_snapshot.uffd]`

Settings that apply only when `[memory_snapshot].backend = "uffd"`.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `max_inflight` | integer | `64` | Faults the daemon resolves concurrently for one VM; the queue behind it is bounded by the same number. Must be greater than zero |
| `read_retry_secs` | integer | `60` | How long one faulting read retries the memory image before the handler gives up and exits, which fails the sandbox and leaves the guest unbacked. Keep it above the overlaybd registry request timeout (30 s per request) with room for its retries. Must be greater than zero |
| `handshake_timeout_secs` | integer | `60` | How long the daemon waits for Firecracker to connect to the fault socket after the server starts; a snapshot load that takes longer fails the resume. Must be greater than zero |

Environment variable override:

- `AENV_MEMORY_SNAPSHOT_UFFD_MAX_INFLIGHT`
- `AENV_MEMORY_SNAPSHOT_UFFD_HANDSHAKE_TIMEOUT_SECS`
- `AENV_MEMORY_SNAPSHOT_UFFD_READ_RETRY_SECS`

## `[memory_snapshot.background_download]`

Background download settings dedicated to remote memory-snapshot OverlayBD layers.
They do not change the general rootfs or attached-drive defaults. All fields are
serialized into the generated memory OverlayBD global config. Each remote layer
is filled block by block into the node-local remote file cache: the cache-owned
background-download scheduler registers one task per remote layer (deduplicated
by blob, shared across sandboxes) and downloads only chunks still missing
from the entry bitmap — each source request fetches `block_size` bytes
(aligned to whole cache blocks) and publishes the chunk's cache blocks as
soon as they land. Submission is never rejected under load — tasks run as
scheduler capacity allows, with at most `maxConcurrentFiles` layer tasks
concurrently per file-cache backend (from the generated overlaybd download
config, default 8)
and at most `concurrency` chunk reads in parallel per layer, subject to the
scheduler's `max_inflight_blocks` cap. Downloads of a
sandbox-bound device start only after envd is ready (plus `delay`), with a 20s
fallback if the ready signal is lost; while foreground remote reads are in
flight, background block reads yield to a small guaranteed floor instead of
competing at full speed. The generated memory
config leaves throttling off (`maxMBps = 0`); image configs that carry a positive
`maxMBps` keep their historical shared rate limit across the block tasks.
A completed cache block becomes visible to foreground reads as soon as it is
committed to the cache bitmap; there is no staging file, no full-file digest
check, and no switch-to-local, so a failed or canceled block simply stays
uncached and is fetched on demand by foreground reads or a later retry. The
cache is a bounded working set: blocks may be evicted under capacity pressure
and are then re-fetched on demand.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enable` | boolean | `true` | Enables background download for remote memory-snapshot layers. |
| `delay` | integer | `0` | Delay in seconds after envd is ready before background download begins (downloads never start before envd readiness; a 20s fallback applies if the ready signal is lost). |
| `delay_extra` | integer | `1` | Exclusive upper bound for random extra delay. The default `1` ensures `delay = 0` adds no jitter. |
| `try_cnt` | integer | `5` | Retry count, with the same semantics as OverlayBD `DownloadConfig.tryCnt`. |
| `block_size` | integer | `16777216` | Background download chunk size in bytes (16 MiB): one source request fetches a chunk of this size, aligned down to whole cache blocks. The cache keeps its own smaller block size for foreground reads, so background downloads keep large-request throughput while foreground keeps fine-grained on-demand reads. Peak scratch per active layer download is `block_size × concurrency`. |
| `concurrency` | integer | `4` | Maximum number of in-flight block remote reads within a single remote layer. `1` keeps the historical serial behavior. Must be greater than zero. |
| `max_inflight_blocks` | integer | `16` | Cap on concurrently downloading chunks enforced by each file-cache backend's download scheduler, shared by every concurrent layer download on that backend; bounds total scratch memory to `max_inflight_blocks` × the download chunk size (`block_size`). The value is fixed when the backend is created from the global config; a per-image `download` override never resizes the scheduler-owned cap (the first mismatch per scheduler is logged as `max_inflight_blocks_override_ignored`). Must be greater than zero. |
