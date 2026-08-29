# CLAUDE.md

## What is AgentENV

AgentENV is a Rust workspace for running AI agents inside isolated, snapshot-capable Firecracker-based environments. It exposes an E2B-compatible HTTP API so agents can create, pause, resume, and reuse sandboxes. Requires a Linux host with `/dev/kvm` access and a host virtualization setup matching `virtualization_mode` (`kvm` by default; `pvm` requires x86_64 and `kvm_pvm`).

## Build, Lint, Test Commands

```bash
make                          # build the workspace
make fmt                      # rustfmt check (agentenv, envd, uvm-ublk, uvm-ublk-daemon)
make clippy                   # clippy with -D warnings
make test                     # full test suite (agent + envd + ublk)
make test-unit                # unit tests only (library *and* bin targets)
make test-agent-integration   # integration tests (tests/integration/*.rs)
make bench                    # snapshot benchmarks
make test-with-redis          # the orchestrator metadata store against a real redis-server
make test-with-postgres       # the src/pg/ shared control-plane suite against a real postgres server
make start-server             # build and run the node server, aenv-node (auto-provisions dependencies)
```

`make test-unit` selects `--lib --bins` over `aenv-core`, `aenv-api`,
`aenv-node`, `envd`, `linux-cap`, `aenv`, `adev`, `uvm-ublk`, and
`uvm-ublk-daemon`, and it is what CI's
`unit-tests` job runs. **Tests that live in a `main.rs` or under a crate's `src/bin/` are
part of daily validation, so put them where they belong rather than where the
runner can see them.** Several of them have to live there: two guards read
their own binary's source text
(`crates/aenv-node/src/bin/aenv-node.rs`'s
`async_main_actually_refuses_a_configured_pg_dsn` and
`the_shutdown_bounds_are_still_wired` — one keeps a `[pg].dsn` off a machine
that runs user code, the other keeps a stuck RocksDB `spawn_blocking` from
hanging the process past `terminationGracePeriodSeconds`), and `aenv` and
`adev` are bin-only crates with no library to move anything into. Naming
`--lib` alone once filtered all of that out and reported ok.

`make test-with-redis` runs `src/orchestrator/store/`'s suite (in `aenv-core`) with
`AENV_REDIS_TEST_REQUIRED=1`, which turns "no `redis-server` on this machine"
into a failure rather than a skip, and fails the target if any test printed a
`SKIPPED[redis]` line. Set `REDIS_SERVER_BIN` to use a particular binary. The
suite is part of `cargo test -p aenv-core --lib`, so `make test-unit` runs it too
— but only that target makes a missing dependency an error. **Anything that
changes `InMemoryMetadataStore` has to run it**: the two backends share one
contract suite (`src/orchestrator/store/contract.rs`) and a change made to one
and forgotten for the other is invisible anywhere else.

`make test-with-postgres` runs `crates/aenv-api/src/pg/`'s suite (per-process connection pool
plus the `pg_try_advisory_lock`-based cluster-leadership election `aenv-api`
runs) with `AENV_PG_TEST_REQUIRED=1`, which turns "no
usable `initdb`/`postgres` on this machine" into a failure rather than a skip,
and fails the target if any test printed a `SKIPPED[postgres]` line. It first
checks for `initdb`/`postgres` itself — Debian/Ubuntu's `postgresql` package
installs them under `/usr/lib/postgresql/<version>/bin/`, off `PATH` — using
the same lookup order as `crates/aenv-api/src/pg/harness.rs::find_bin`; set `INITDB_BIN`/
`POSTGRES_BIN` to point at specific binaries. The suite is part of
`cargo test -p aenv-core --lib`, so `make test-unit` runs it too — but, exactly
as with `test-with-redis`, only `test-with-postgres` makes a missing server an
error instead of a silently-green skip.

Dev/CI tooling via `cargo adev` (delegated from Makefile):
```bash
cargo adev codegen            # regenerate all OpenAPI clients/server
cargo adev mutants            # run mutation tests
cargo adev coverage           # run code coverage
make firecracker-client       # shorthand for cargo adev codegen firecracker
make envd-http-client         # shorthand for cargo adev codegen envd
make agentenv-server          # shorthand for cargo adev codegen server
make custom-extension-client  # shorthand for cargo adev codegen custom-extension
```

Dependency downloads, generated OverlayBD runtime configs, and OverlayBD packaging are provisioned automatically during server startup. Machine-wide `/dev/kvm` group access, ublk device permissions, OverlayBD system config, and network sysctls require a one-time root setup via `server --setup-host --runtime-user <user> --runtime-group <group>`; normal startup validates those prerequisites and the selected KVM/PVM mode, and fails with actionable errors when they are missing. AgentENV does not load or install `kvm_pvm`.

All registry access goes through `regctl`: userImage manifest fetch, config blob fetch, layer download, tools drive image download (`crates/aenv-node/src/setup/deps.rs::extract_ext4_from_ghcr`, unpacked with `umoci`), and OCI referrers lookup when `[image_resolver].try_referrers_overlaybd_prefixes` is non-empty (referrers lookup failures fall back to the source image). Server setup provisions both automatically: `regctl` is downloaded from the `[regclient]` entry in `config/deps_manifest.toml` to `/usr/local/bin/regctl`, and `umoci` is installed as a `[packages.runtime]` system package. `crates/aenv-node/src/image/oci_image.rs` fetches the manifest via `regctl manifest get` and classifies it — standard OCI tar images trigger a full `regctl image copy` + per-layer conversion into local `.commit` files, while overlaybd-native images skip blob download entirely and emit a remote-ref `image.json` that the overlaybd runtime's `registryfs_v2` backend reads directly from the registry. User-facing image references are normalized by `ImageResolver` from template API `userImage` fields and CLI image arguments. For private registries referenced by `userImage`, run `docker login <registry>` before starting the server; `write_generated_overlaybd_global_config` auto-detects `~/.docker/config.json` (or `$DOCKER_CONFIG/config.json`) and wires the overlaybd runtime's `credentialConfig.mode=file` so the runtime can authenticate too.

P2P artifact transport (`src/p2p/`) is a project-wide, optional node-to-node artifact layer. Consumers depend on `P2pTransport` rather than a concrete backend. `DisabledP2pTransport` is the default no-op implementation; `IrohBlobsP2pTransport` embeds an `iroh` endpoint and `iroh-blobs` `FsStore`, serving bytes, byte ranges, and a small AgentENV catalog protocol from the AgentENV server process. Configure it with `[p2p]`. When enabled for overlaybd, the server also starts a localhost HTTP facade: `/p2p-http/{*origin}` is patched into overlaybd registryfs for foreground range reads, while `/p2p-control/publish-layer` accepts by-reference publication of fully downloaded layers as full-layer artifacts. Overlaybd layer artifact identity is owned by `crates/aenv-node/src/overlaybd/p2p/artifact.rs` (`overlaybd-layer/v1/sha256:<digest>` plus `LayerMetadata`); snapshot publishing must reuse that helper instead of inventing snapshot-specific layer keys. Snapshot publishing also advertises fixed artifacts under `snapshot/v1/artifacts/{snapshot_id}/...` after repository commit; OSS runtime resolution tries P2P before object storage for those fixed artifacts, while POSIX resolution does not consume P2P. Scheduler-protocol integration covers endpoint discovery and a lightweight in-memory artifact-to-node index: heartbeats advertise the local `P2pEndpoint`, `ListP2pPeers` returns ready peers for a backend, `RecordP2pArtifact`/`ForgetP2pArtifact`/`LookupP2pArtifact` maintain a key-to-node hint index that accelerates artifact lookup before falling back to broad peer polling. Only key-to-node mappings are stored, never artifact locators, metadata, or proxied bytes; node unregister removes all artifact mappings for that node. `aenv-api`'s own in-process registry (`src/node_registry/`, built unconditionally) is the only production implementation of this protocol left — `services/scheduler`, the Go implementation the protocol was originally written against, has been deleted (see "Distributed Control Plane" below) — and `src/p2p/discovery/scheduler.rs`'s peer discovery dials whichever process `[cluster].scheduler_endpoint` currently names. See `docs/src/internals/p2p-design.md`.

Local RocksDB helper (`src/local_store.rs`) is the shared async-friendly wrapper for small node-local key/value metadata stores. Use `LocalKvStore` with an explicit `LocalStoreDurability` (`Memory`, `Wal`, or `Sync`) for new local record/catalog persistence instead of hand-maintaining per-record JSON files. It runs RocksDB operations through `spawn_blocking`; keep values compact (for example compact JSON or other binary encodings) and let callers choose durability in code rather than config unless a user-facing knob is explicitly required.

The `aenv-node`/`aenv-api` Docker images embed the commit they were built from rather than always reporting `unknown`: `.dockerignore` excludes `.git`, so `deploy/docker/Dockerfile.aenv-node`/`Dockerfile.aenv-api` each take an `AENV_GIT_COMMIT` build arg (`ARG AENV_GIT_COMMIT=unknown`) and set it as an `ENV` for `build.rs` to bake in via `cargo:rustc-env`; `src/identity.rs` reads it back through `option_env!("AENV_GIT_COMMIT")`. `make k8s-build` and `make deploy-up`/`make deploy-build` resolve it themselves (`AENV_GIT_COMMIT ?= $(shell git rev-parse --short HEAD ...)`) and pass it as `--build-arg`, so a normal `make` invocation needs nothing extra; only a manual `docker build` has to remember the flag.

Config note: `docker-compose.yml`'s `agentenv-api` service requires a reachable PostgreSQL — a `postgres:17-alpine` container is defined in `deploy/docker-compose.yml` alongside `redis` and depended on by `agentenv-api`'s `depends_on`, mirroring `[pg]`'s mandatory status everywhere else (see the snapshot-catalog section below).

🔴 **The Go scheduler is deleted, not merely disabled.** `services/scheduler`
— the Go implementation of the `Scheduler`/`PausedRegistry` RPCs — and the
`agentenv-scheduler` Deployment/Service/PDB that ran it are gone from the
tree. `services/` now ships one Go binary, `gateway`; the `Scheduler` gRPC
contract (`services/api/proto/scheduler.proto`) it still speaks as a client is
answered by `aenv-api`'s own in-process registry (`src/node_registry/`) on
every current deployment. See "Distributed Control Plane" below and
`services/README.md` for the full picture.

Go control-plane services (`services/` module — gateway only):
```bash
make -C services build               # build gateway
make -C services test                # test the whole module (gateway, shared, api)
make -C services test-with-postgres  # same, plus fails instead of silently skipping when redis-server is missing
make -C services run-gateway

# from services/ directly
go test ./...
```

`make -C services test` runs `gateway`/`shared`/`api`; no package left under
`services/` opens a real database connection any more —
`scheduler/internal/registry` and `scheduler/internal/catalog`, the only ones
that ever did, were deleted with `services/scheduler`. Without a
`redis-server` on `PATH`, the `RedisBindingStore`/routing-reader suites call
`t.Skip` and the package still reports `ok`, so `make -C services test` prints
a warning naming exactly that after the run. `make -C services test-with-postgres`
still starts a throwaway PostgreSQL in Docker for parity with the CI step it
mirrors, but the only tests it changes the outcome of today are Redis-gated:
`REDIS_SERVER_BIN` and `SCHEDULER_REDIS_TEST_REQUIRED=1` turn a missing
`redis-server` into a failure instead of a silent skip for those same
binding-store tests — the only ones that exercise the Redis implementation of
sandbox-to-node bindings, which is what every HA deployment runs.

Run a single test:
```bash
# Unit test by name (pick the crate that owns it)
cargo test -p aenv-core --lib test_name
cargo test -p aenv-node --lib test_name
cargo test -p aenv-api --lib test_name
# Integration test module (they live in crates/aenv-node/tests/)
sudo -E cargo test -p aenv-node --test orchestrator_integration orchestrator::
# Specific integration test
sudo -E cargo test -p aenv-node --test orchestrator_integration orchestrator::test_name
```

Integration tests require root (network namespaces), `/dev/kvm`, host modules matching `AENV_VIRTUALIZATION_MODE`, and `AENV_CONFIG_PATH` pointing to a valid config.

Tests must never write out an executable they then `execve` themselves. While any thread holds a write fd on that file, every concurrent `fork` in the process inherits the fd and the exec is rejected with `ETXTBSY` (writing to a temp name and renaming does not help — `rename` keeps the inode). Ship the script under `tests/fixtures/` instead, symlinking it into the per-test directory when it has to resolve `$0` to that directory, or let a child process write it.

## Architecture

See `docs/src/internals/architecture.md` for detailed design with data flow diagrams.

### Storage

The storage subsystem is the core of AgentENV. It serves two orthogonal data paths: **block devices** (rootfs/extra drives) and **memory snapshots** (ublk-backed memory restore from overlaybd layers).

**Block device pipeline**: overlaybd image layers -> ublk userspace block device -> `/dev/ublkbN` in VM.

**overlaybd** (`storage/overlaybd/`): LSMT-based layered image format. Stacks immutable compressed read-only layers with a single writable upper layer. Each layer has a `HeaderTrailer` (magic, UUID, offsets) and `DiskSegmentMapping` entries (16-byte bit-packed records mapping virtual block ranges to physical locations). Reads resolve through the layer stack top-down; writes append to the upper layer. Pluggable backends: `LocalFile` (io_uring pread/pwrite, optional O_DIRECT), `registryfs_v2` (OCI registry), `tar`. Compression via zstd with random-access jump tables. `image/image_file.rs` is the high-level entry point. LSMT implementation lives under `lsmt/file/`: `readonly.rs` and `readwrite.rs` define `LSMTReadOnlyFile` and `LSMTFile`, `stack.rs` handles open/merge/stack helpers, `helper.rs` owns shared format/cache helpers, and `types.rs` contains public LSMT types and `PremergedIndexCachePolicy`; the persistent premerged lower-index artifact cache remains under `cacheConfig.cacheDir/premerged-index/`. Image-level runtime helpers live in `image/helper.rs`, including runtime upper preparation and the path rewriting helpers formerly split across `runtime_upper.rs` and `path_rewrite.rs`. Image resolution also maintains `{image.cache.root_dir}/{commits,indexes,configs}` for content-addressed overlaybd commits, OCI conversion indexes, and resolved OCI image configs. The primary Firecracker pause path uses `close_seal + restack` to turn the live upper into the newest lower and reopen a fresh writable upper in place; the explicit upper-export helper (`export_upper_as_sealed`) it and packaging/export flows call remains in `image/image_file.rs` / `lsmt/file/readwrite.rs`.

OverlayBD write-path optimizations use in-memory append cursors (`rw_data_append_offset`, `rw_index_append_offset`) to avoid steady-state `size()` metadata calls. Small buffered writes (`<= 4 KiB`) use synchronous `pwrite` to reduce local write latency; this assumes fast local storage, while slow or network-backed filesystems can block the Tokio `LocalSet` thread.

**ublk** (`storage/ublk/`): Low-level async ublk block device primitives using Linux's ublk driver. Provides `UVMUblkCtrl` for ADD/DEL commands to `/dev/ublk-control` via io_uring, per-queue worker threads with thread-local `AsyncIoRing` and slab-allocated I/O slots, and the `OverlaybdTarget` implementation for full layered image access. Buffer handling supports `UBLK_F_AUTO_BUF_REG` on kernel 6.8+.

**ublk-daemon** (`storage/ublk-daemon/`): Long-running daemon process (`uvm-ublk-daemon`) that manages all ublk devices within a single process. Communicates with the main AgentENV server via a Unix domain socket using a length-prefixed JSON protocol. Supports `CreateOverlaybd`, `CreateOverlaybdRuntimeDevice`, `AcquireOverlaybd`, `ReleaseOverlaybd`, `UpdateSize`, `GetFeatures`, `Delete`, `RestackSnapshot`, and `Shutdown` RPCs. The daemon owns a shared `ImageService` and io_uring control ring, tracks active devices in a `DashMap`, and maintains a warm pool of reusable overlaybd ublk devices for pooled acquire/release flows. The client (`UblkDaemonClient`) spawns the daemon process, waits for readiness, and runs a background watchdog that detects unexpected daemon exits. `crates/aenv-node/src/sandbox/ublk/device.rs` provides the `UblkDeviceManager` global singleton that wraps the daemon client and keeps the node-local shared-memory device cache.

**storage-util** (`storage/util/`): Shared io_uring abstractions. `AsyncIoRing<S>` is a generic async wrapper with slab-based futures for CQE delivery. `IoRingWorker` spawns dedicated worker threads with thread-local io_uring instances. `ReloadableIDAllocator` provides reusable monotonic ID allocation/recycling with a free-list and lookup set.

**Sandbox integration** (`crates/aenv-node/src/sandbox/ublk/` + `crates/aenv-node/src/sandbox/extra_drive.rs`): `overlaybd.rs` materializes runtime configs (rewrites paths, creates symlinks to layer files). Rootfs and extra drives both create daemon-managed ublk devices through the process-wide `UblkDeviceManager`. `extra_drive.rs` prepares user-specified extra drives with rollback on failure, and attached-drive snapshots now export per-drive `drives/<id>/image.json` state so writable drives survive pause/resume and template publication.

**Memory snapshot pipeline**: On pause, Firecracker creates a state-only diff snapshot. AgentENV queries dirty/present memory ranges, reads the selected memory with `process_vm_readv`, and directly creates the OverlayBD memory layer. Parent layers from previous snapshots are stacked to form the full memory image. On resume, a read-only ublk device is created from the stacked OverlayBD layers and passed to Firecracker as a file-backed memory backend. Firecracker mmaps the block device and COWs pages into anonymous memory on write. Multiple sandboxes from the same snapshot template share a single memory ublk device via reference counting, enabling page cache reuse.

### Distributed Control Plane (`services/` + `src/node_registry/`)

🔴 **The Go scheduler is deleted.** `services/scheduler` — the standalone Go
implementation of the `Scheduler`/`PausedRegistry` RPCs — and the
`agentenv-scheduler` Deployment/Service/PDB that ran it are gone from the
repository outright, not commented out or kept as a rollback target.
`services/api/proto/scheduler.proto` (RPCs: `Schedule`, `ListNodes`,
`LookupNode`, `RecordAssignment`, `Heartbeat`, `ReportSandboxEvent`,
`ListObservedNodes`, `ListP2pPeers`, `RecordP2pArtifact`,
`ForgetP2pArtifact`, `LookupP2pArtifact`, `GetNode`, `UnregisterNode`,
`ListRegistrySandboxes`) and its generated Go/Rust bindings are unaffected by
the deletion — they are the contract, and `aenv-api`'s own in-process
registry (`src/node_registry/grpc_service.rs::NodeRegistryGrpcService`) is now
the only production implementation of it, on every current deployment
(`deploy/k8s/base`, `deploy/docker-compose.yml`), unconditionally. There used
to be a `[cluster].node_placement_source` switch (env
`AENV_NODE_PLACEMENT_SOURCE`) choosing between this in-process registry
(`"native"`) and dialling out over gRPC to a Go scheduler process
(`"scheduler"`, the code default) for the same placement/heartbeat/
paused-registry RPCs; that field, its `NodePlacementSource` enum, and the
gRPC-dialling implementation (`SchedulerNodePlacement`) are all deleted along
with the Go scheduler they dialled. `AENV_NODE_PLACEMENT_SOURCE` is inert
now: no field declares it, so confique ignores it and nothing refuses it —
the startup guard that carried un-migrated manifests through the cutover has
itself been removed. `Schedule` always
places with round-robin: `RoundRobinStrategy` (`src/node_registry/strategy.rs`)
is the only strategy in the tree — held concretely rather than behind a
one-implementation trait, with no config knob to pick another, and Go's
`random` strategy was never ported. Its `strategy` metric label is still
emitted, and still valued `round_robin`, because
`agentenv_api_schedule_duration_seconds` / `..._assignments_total` are
series identities that must not change shape. Node discovery
for that registry is `[cluster].node_discovery_mode` — `"kubernetes"` (the
default; watches EndpointSlices for `[cluster.kubernetes_discovery]`'s
namespace/Service) or `"static"` (seeds once at startup, no watch, from
`[cluster].static_discovery_nodes`, a `{id, endpoint}` list that is
**TOML-file-only with no `env =` binding** — set it via the config file
`AENV_CONFIG_PATH` names or an `AENV_CONFIG_OVERLAY_PATH` overlay, the way
`deploy/docker/config/cluster-static-discovery-overlay.toml` does for
`deploy/docker-compose.yml`, which has no Kubernetes API to discover
against).

🔴 **The sandbox-to-node binding store is Redis, and there is no other.**
`src/binding_store/` still holds two implementations, but only one is
deployable: `build_binding_store` (`crates/aenv-api/src/bin/aenv-api.rs`)
constructs `RedisBindingStore` unconditionally. There used to be a
`[binding_store].backend` switch (`AENV_BINDING_STORE_BACKEND`, `"in_memory"`
| `"redis"`, defaulting to `"in_memory"`) whose in-memory arm that same
function refused *unconditionally* — `aenv-api` is a multi-replica Deployment,
a routing table one replica cannot see misroutes silently, and nothing at that
layer can tell a lone replica from one of several — so the field, its
`BindingStoreBackendKind` enum, the refusal arm and the deployment values that
set it (`deploy/docker-compose.yml`, `deploy/k8s/base/agentenv-api-deployment.yaml`)
are all deleted; `AENV_BINDING_STORE_REDIS_URL` is now the whole
configuration. `InMemoryBindingStore` is **not** deleted: it is gated behind
`#[cfg(any(test, feature = "test-support"))]` (`src/binding_store/mod.rs`, on
both `pub mod in_memory` and the re-export), because the shared contract suite
(`src/binding_store/contract.rs`) runs the same assertions against both
backends and that is what keeps a fix made to one and forgotten for the other
from being invisible. `cfg(test)` alone would not do — it is per-crate, and
`aenv-node`'s and `aenv-api`'s own suites would lose the symbol; both already
take `aenv-core` with `features = ["test-support"]`, so no `Cargo.toml`
change was needed. There is **no** startup refusal for a leftover
`AENV_BINDING_STORE_BACKEND`, and there is none for any other removed
AgentENV variable either — the tombstone guards were a transition measure and
are gone. The only value a working deployment could have carried was `redis`,
which is exactly what happens with the variable absent, so an un-migrated
manifest cannot degrade.

`crates/aenv-api/src/orchestrator/paused_registry/postgres/`
similarly ports the paused-sandbox registry onto the same shared `[pg]` pool
the snapshot catalog uses (`[orchestrator.paused_registry].backend =
"postgres"`, requires a heartbeat roster — `aenv-api`'s own node registry,
which it always builds); `aenv-node` never holds this configuration
meaningfully — it warns and wires a no-op registry for any
`[orchestrator.paused_registry].backend` other than `"local"`, because
cluster-wide paused-sandbox state belongs to the API half alone.

Gateway (`services/gateway/`, the only Go binary `services/` ships now) is an
HTTP reverse proxy. Every user-facing REST call (`sandboxes`/`snapshots`/`templates`,
including `GET /sandboxes` and `GET /v2/sandboxes`) is forwarded unconditionally
to `gateway.rest_upstream_addr` — the api half — which `services/shared/config`'s
`Config.Validate` refuses to load empty; there is no more per-node fan-out
(`cluster_list.go`, which used to aggregate `GET /sandboxes` across every node
when no api half was configured, is deleted along with the position it existed
for). `GET /nodes` / `GET /nodes/{id}` and `GET /registry/sandboxes` are still
the gateway's own aggregations, answered from the `Scheduler` protocol's
`ListObservedNodes`/`GetNode`/`ListRegistrySandboxes` RPCs — today always
against `aenv-api`. Sandbox data-plane traffic (proxy headers or a sandbox
proxy domain) is routed by a `LookupNode` call through `gateway.scheduler_addr`
(now `agentenv-api:8002` on every shipped deployment). 🔴 There is no second
read endpoint any more: `gateway.query_only_scheduler_addr`, and the
`QueryOnlySchedulerClient` it selected, are deleted along with the Go
scheduler's `--query-only` replica mode. `GATEWAY_QUERY_ONLY_SCHEDULER_ADDR`,
`GATEWAY_SCHEDULER_FALLBACK_DISABLED` and `GATEWAY_SCHEDULER_FALLBACK_TIMEOUT`
are all inert now — `services/shared/config` reads no such keys and refuses no
such names, so a manifest that still sets one is simply ignored. The last of
the three was renamed rather than dropped: use `GATEWAY_COLD_LOOKUP_TIMEOUT`.

`services/` is a separate Go module. See `services/README.md` for build/run/deploy
instructions and the current architecture in full.

When changing code under `services/`, validate via `make -C services test` (or
`go test ./...` inside `services/`) in addition to Rust workspace checks.
Nothing left in the module needs a database to run its tests — see the build
command block above.

### Per-Node Subsystems

Each node is an AgentENV server binary (`crates/aenv-node/src/bin/aenv-node.rs`) running on a Linux host with `/dev/kvm` and one configured KVM/PVM mode. It wires together:

**API layer** (`src/api/`): Axum HTTP server with OpenAPI-generated endpoint traits (`src/api/generated/` from `src/api/openapi.yml`) plus a reverse proxy. Implementations live in `src/api/impls/` (sandbox CRUD, snapshot CRUD, template-facing CRUD, auth, generic cursor-based pagination). The proxy (`src/api/proxy.rs`) forwards HTTP/WebSocket to sandboxes using routing headers (`x-agentenv-sandbox-id`, `x-agentenv-target-port`). 🔴 That data plane is mounted by `aenv-node` alone: `src/api/server.rs`'s `new` (what `aenv-node` calls) carries `/proxy`, `/proxy/`, `/proxy/{*rest}`, the host-routed fallback behind them and the `sandbox_proxy_classifier` layer; `new_control_plane_only` (what `aenv-api` calls) carries none of them, because `aenv-api` runs no sandbox to forward to and real data-plane traffic is routed by the gateway's `LookupNode` straight at the node that holds the sandbox. An unrecognized path on `aenv-api` is therefore the generated router's own 404, not the proxy fallback's.

**Orchestrator** (`src/orchestrator/`): Manages sandbox lifecycle (create, fork, pause, resume, snapshot, delete) with state machine transitions (Creating -> Running -> Forking | Snapshotting | Pausing -> Paused, Resuming, Killing). `service.rs` is the core logic. `fork_sandbox` forks one running source sandbox into multiple running children on the same node, records child create successes/failures in `OrchestratorCounters`, and publishes one `SandboxLifecycleEventType::Fork` event per child with the child sandbox ID/resources. `capture_snapshot` drives the `Running -> Snapshotting -> Running` flow used by the user-facing snapshot API: it delegates the actual capture to the sandbox backend, rolls back to `Running` on recoverable failure, and tears the sandbox down on `SandboxCaptureError::Terminal` (when the runtime was mutated past the point of safe resume). Uses an in-memory metadata store and a proxy route table for fast sandbox discovery. Maintains incremental `OrchestratorCounters` for create success/failure totals, while running/starting sandbox counts and allocated CPU/memory are derived on demand from sandbox metadata via `aggregate_resource_metrics`. Publishes non-blocking broadcast lifecycle events for successful create/delete/pause/resume/fork; these are best-effort and dropped if there are no subscribers. Runs an auto-eviction task for expired sandboxes. On graceful shutdown, running sandboxes are paused and persisted rather than deleted; on next startup they are restored as `Paused` and can be resumed. The `SandboxPersister` trait abstracts durable storage of paused sandbox state across server restarts. On startup, `Orchestrator::new` restore persisted `Paused` sandboxes into the in-memory store.

**Observability** (`src/observability/`): Builds node-level snapshots for the admin/node APIs by combining static node identity (node ID, cluster ID, service instance ID, build version/commit), machine information detected from `/proc/cpuinfo`, request-time host metrics collection (CPU, memory, disks), orchestrator runtime metrics sampled on request via `metrics_snapshot()`, and the current sandbox ID roster from orchestrator. `reporter.rs` runs a background task that periodically sends gRPC `Heartbeat` RPCs to `[cluster].scheduler_endpoint` — `aenv-api`'s own `NodeRegistryGrpcService`, unconditionally, on every current deployment (the Go scheduler this endpoint used to name has been deleted; see "Distributed Control Plane" below), hot-reloadable without a Pod restart via `src/scheduler_endpoint.rs::SchedulerEndpointSource` — so the control plane can track live node state, reconcile sandbox bindings, and advertise the node's optional P2P endpoint; the same loop also drains orchestrator lifecycle events and sends `ReportSandboxEvent` RPCs for create/delete/pause/resume/fork (fork events use child sandbox IDs/resources). The event receiver handles lagged receivers and disables the event branch when the broadcast channel closes to avoid busy loops. The reporter is created via `ObservabilityReporter::new` and started explicitly with `start()` before shutdown via `shutdown()`. `observability.enabled` controls whether this subsystem is exposed by the node/admin APIs. At startup, `machine.rs` optionally runs `cpu-template-helper template dump` (resolved from `{deps_path}/firecracker/{version}/cpu-template-helper`) and includes the output in each heartbeat's `MachineInfo.cpu_config_json`; whichever side answers `Heartbeat` computes a cluster-wide bitwise AND intersection of all node configs and returns it in the heartbeat response — `src/node_registry/cpu_template.rs`'s port is golden-tested against fixed values recorded from the (now-deleted) Go scheduler's own algorithm — where it is stored in a shared `Arc<RwLock<Option<String>>>` and applied to new VMs via Firecracker's pre-boot `PUT /cpu-config` API.

**Sandbox** (`src/sandbox/`): Manages Firecracker VMs, network namespaces (veth pairs, iptables isolation), rootfs mounting/file injection via debugfs, envd init system communication, MMDS metadata service, and ublk block devices. `firecracker/sandbox.rs` is the main wrapper coordinating all subsystems, and now owns a stable `SandboxId` passed in by the orchestrator so that Firecracker serial logs live under `{serial_output_base_dir}/{SandboxId}/firecracker-*.log`. The `SandboxBackend` trait exposes `snapshot()` and `fork()` alongside `pause`/`resume`/`stop`; capture/fork paths return typed errors where recoverable failures roll back to `Running` and terminal failures mean the live runtime was mutated and must be torn down. Captured state travels as an opaque `CapturedSandboxSnapshot` handle (backed by `FirecrackerCapturedSnapshot` in the Firecracker impl) that keeps the temp artifact dir alive until the repository publishes it. `firecracker/mmds.rs` defines the metadata structure exposed to VMs via Firecracker's MMDS V2 interface, providing sandbox and snapshot identity information to envd at the standard 169.254.169.254 address.

**Snapshot + Template Builder** (`src/snapshot/`, `src/template/`): `src/snapshot/` owns the first-class committed snapshot model, repository backends, runtime resolution, and artifact layout conventions. `SnapshotManager::publish_captured` turns a `CapturedSandboxSnapshot` (produced by a running sandbox) into a committed snapshot, and `SnapshotMetadata::source_sandbox_id` records provenance. `src/template/` now contains the template-builder API that lets users declaratively build snapshots through RUN / ENV / WORKDIR steps while preserving the existing external template API semantics. `builder.rs` coordinates build / rebuild flows over committed snapshots. The committed snapshot manifest stores logical snapshot content (metadata, rootfs layers, attached-drive layers) rather than repeating fixed artifact file paths like `vm_state.bin` / `mem_image.json`; backend/resolver code derives those paths from layout conventions. When P2P is configured, `SnapshotManager` best-effort publishes fixed snapshot artifacts (`vm_state.bin`, `firecracker-manifest.json`) and local overlaybd layers after commit; failed P2P publish does not roll back the repository. OSS resolver consumes fixed artifacts P2P-first with backend fallback, but overlaybd layer acceleration belongs to overlaybd's P2P facade and POSIX resolver should not repair missing repository files from P2P.

🔴 **The snapshot catalog is PostgreSQL, and there is no other.** `src/snapshot/repository/` splits into a catalog
half (rows) and an artifact half (bytes) — `repository.catalog()` vs `repository.artifacts()` — and only the byte
half has a choice of backend. `[snapshot.repository_backend]`'s `posix_fs` and `oss` are byte repositories;
object storage held `catalog/records/*.json` and `catalog/aliases/*.json` until the Stage B cutover and holds
none now. The switches that steered that move (`[snapshot.catalog].write`/`.read`, `SnapshotCatalogWrite`,
`SnapshotCatalogRead`, `AENV_SNAPSHOT_CATALOG_WRITE`, `AENV_SNAPSHOT_CATALOG_READ`), the double write behind them
(`repository/mirror/`), the mirror backlog, the read-side confirmation gate and the `catalog_migration_state`
table are all deleted. Consequences worth knowing before touching this code:

- `aenv-api` **requires `[pg]`**, and refuses at construction rather than at first use: `build_pg_pool`
  (`crates/aenv-api/src/bin/aenv-api.rs`) returns a `PgPool`, not an `Option<PgPool>`, so an unconfigured
  `[pg].dsn` stops `assemble_api` *before* the catalog build reaper, the paused-registry factory and the
  paused-registry background tasks are wired — each of which used to be handed a `None` and silently become a
  no-op while the replica went on assembling. `build_snapshot_backend` keeps its own `Option` and its own
  refusal, because it is shared with `aenv-node`, which always passes `None`. Requiring the pool is **not** the
  same as selecting the PostgreSQL paused registry: `[orchestrator.paused_registry].backend` still decides that
  and `"local"` is still supported — `build_paused_registry`'s `Local` arm never touches the factory it is now
  always handed (`a_local_backend_ignores_a_postgres_factory_it_was_handed`). There is no PostgreSQL-free
  deployment any more — single-machine, compose or dev included. 🔴 There is **no `AENV_PG_DSN`**: `[pg]` is
  `Option<PgConfig>`, confique cannot descend into it, so the section is TOML-file-only (a config file or an
  `AENV_CONFIG_OVERLAY_PATH` overlay). Every operator-facing "no `[pg]`" message now says so, and each of the
  three is pinned by a test that fails if `AENV_PG_DSN` reappears in it: `build_pg_pool`'s
  (`the_api_half_refuses_to_assemble_without_a_postgres_catalog`), `build_snapshot_backend`'s
  (`the_refusal_names_no_environment_variable_that_does_not_exist`) and `aenv-snapshot-image`'s
  (`the_missing_pg_refusal_names_no_environment_variable_that_does_not_exist`).
- `aenv-node` holds **no catalog at all**. Its `SnapshotRepository` carries `NoSnapshotCatalog`
  (`src/snapshot/repository/no_catalog.rs`), which *refuses* every catalog call rather than reporting absence —
  absence is what callers act on by deleting artifacts and refusing resumes. A node stages bytes
  (`SnapshotRepository::stage`) and hands a `StagedSnapshot` back over gRPC; `aenv-api`'s `commit_staged` writes
  the row.
- `AENV_SNAPSHOT_CATALOG_WRITE`, `AENV_SNAPSHOT_CATALOG_READ` and `AENV_SNAPSHOT_CATALOG_MIRROR_PATH` are
  **inert**. Both binaries refused to start on them for one release; that guard is deleted along with the rest
  of the transition's scaffolding, so confique now ignores the undeclared names in silence. The risk it covered
  is real — an un-migrated manifest looks healthy while its operator believes the catalog is double-written —
  and what covers it now is manifest hygiene, not runtime: `services/shared/config/snapshot_catalog_manifest_test.go`
  walks every AgentENV workload under `deploy/k8s/base` and fails if one declares any of the three. **Keep it
  working.** Note that this is the opposite call from `--role`/`AENV_ROLE`, which is argument parsing rather than
  an ignored env var and still refuses.

For the OSS repository backend, `snapshot_image_storage = "source_registry"` publishes compatible overlaybd-native rootfs and attached-drive snapshot deltas back to their source OCI registry via `src/snapshot/repository/backends/common/acr/`; `object_storage` keeps the conservative OSS managed-layer behavior.

`crates/aenv-api/src/snapshot/image_export/` and `crates/aenv-api/src/bin/aenv-snapshot-image.rs` implement the standalone `aenv-snapshot-image` operator tool (built via `make build-snapshot-image`; excluded from the server binary, Docker image, and install packages — both Dockerfiles name `--bin` explicitly — although a plain workspace `cargo build` also compiles it as a Cargo bin target). 🔴 It lives in `aenv-api` and not `aenv-node` because it reads one snapshot catalog row, the catalog is PostgreSQL, and `[pg]` is the deciding half's alone; it therefore **requires `[pg]`** and refuses to run without it. It used to read the object-storage catalog directly from a node, which after the Stage B cutover answered "not found" for every snapshot published since. The `regctl` shell-out helpers it needs were hoisted out of `crates/aenv-node/src/image/oci_image.rs` into `aenv-core`'s `src/image/regctl.rs` for the same reason; `oci_image` re-exports them, so the node's own call sites are unchanged. It publishes a committed snapshot's rootfs — never attached drives or memory — as an OverlayBD-native OCI image through `regctl` shell-outs (`blob head/put/copy`, cross-registry `blob get | blob put` pipe, `manifest head/put`) against both POSIX and OSS repository backends (which supply the *bytes*; `repository_backend` decides nothing about the catalog). The target repository is explicit (`--target-repository`, default tag `latest`) or inferred from the canonical rootfs publication metadata and then the snapshot's unique external source repository (default tag `snapshot-<snapshot-id>`). Persisted publication metadata only supplies the original repository and never bypasses the registry manifest check. The tool records no new publication state: repeat runs are idempotent through the registry manifest digest, pre-existing conflicting references are refused by a non-atomic preflight check, and it warns when the normalized selected reference is already snapshot-managed and may therefore be deleted with the snapshot; other exports live independently of snapshot deletion.

**Custom extension** (`src/custom_extension_api/`, `src/sandbox/custom_extension/`): Optional external HTTP service configured via `[custom_extension].url` (unset = fully disabled). Its only current capability is sandbox lifecycle hooks under `POST {url}/sandbox-hook/*`: `start-fresh` (before a fresh boot, after network slot allocation; may return `extraBootArgs` appended to the kernel cmdline), `start-resume` (before snapshot resume), `patch-params` (applies an extension-defined patch to the custom extension params — the patch document is passed through verbatim and the hook returns the updated full params, which the runtime stores; failure rejects the patch), and `stop` (before network slot release, fired on any runtime teardown — including pause, which stops the VM and releases the netns after persisting; best-effort — delivery failures are only logged inside the client and it is also fired fire-and-forget on drop without blocking). Start hook requests carry `networkNamespacePath` (host path of the sandbox's netns file from `Slot::namespace_path()`) and `hostInteractionIp` (the current slot's per-runtime routed interaction IPv4 address); start and stop hooks also carry a fresh per-runtime-instance `sandboxInstanceId` (generated per start, reused by the matching stop) so the extension can ignore out-of-order stop notifications for superseded instances (sandbox ids are reused across pause/resume). All non-stop hook failures fail the corresponding sandbox operation. The process-wide hook client lives in `src/sandbox/custom_extension/client.rs` (backend-agnostic); the Firecracker backend only invokes start/stop hooks, while patch-params is driven by the orchestrator (`patch_sandbox_custom_extension_params` calls the hook, then pushes the approved value into the backend via the infallible `SandboxBackend::update_custom_extension_params` assignment). Users supply opaque `customExtensionParams` JSON at sandbox creation; an absent value and an empty object are equivalent (empty params), and non-empty params are rejected when no extension URL is configured. Internally params are `CustomExtensionParams` (`serde_json::Map<String, Value>`; `None` = empty). The params flow `CreateSandboxRequest -> SandboxLaunchConfig -> FirecrackerCommonConfig` into the start hooks (always invoked when the extension is configured, empty params included), persist through pause/resume (serde of the common config) and into committed snapshots (`SandboxMetadata -> SnapshotPublishMetadata -> CommittedSnapshot`; a launch-provided value overrides the snapshot-persisted one), and can be read via `GET /sandboxes/{sandboxID}/custom-extension-params` (returns `{}` when empty) or patched on Running sandboxes via `PATCH /sandboxes/{sandboxID}/custom-extension-params` (body semantics defined entirely by the extension; the extension returns the updated full params). The hook client crate (`custom_extension_client`) is generated from `src/custom_extension_api/openapi.yml` via `make custom-extension-client`; the generator does not delete removed model files, so prune orphan files under `src/custom_extension_api/generated/` manually after schema removals.

**Config** (`src/cfg.rs`): Reads `config/default.toml` (or `AENV_CONFIG_PATH`), optionally layered with `AENV_CONFIG_OVERLAY_PATH` — a `:`-separated list of extra TOML files, deep-merged table by table over the main file, later entries winning, with the environment still on top. Unset means the load is byte-for-byte what it was before the mechanism existed; a file that is named but missing is a startup error. This is the only way to set `[backend.oss]` and `[backend.posix_fs]`: confique descends into a struct only through `#[config(nested)]`, `nested` may not be `Option<_>`, so no `env =` attribute on those sections is ever read (`AENV_SNAPSHOT_STORE` was one such dead binding and has been removed). `deploy/k8s` uses it to assemble one `[backend.oss]` out of a tracked credential-free file and a mounted Secret. `home_path` (overridden by `AENV_HOME_PATH`) is the base for `$AENV_HOME/...` local state paths such as Firecracker work dirs, generated OverlayBD configs, image cache, snapshot local cache, p2p store, persisted sandboxes, and default deps. `deps_path` (overridden by `AENV_DEPS_PATH`) is only the root for downloaded runtime assets such as Firecracker, kernel, tools drive, and OverlayBD packages. The config also covers firecracker binary paths, kernel/rootfs image paths, machine specs, envd settings, orchestrator timeouts, observability identity/enablement settings, P2P transport settings, network pool tuning, ublk configuration, and custom extension settings (`[custom_extension]` url/timeout).

## Workspace Crates

The former single `agentenv` crate is now three, and the split is a fact about
the dependency graph rather than a convention: `aenv-api` links no overlaybd
and no ublk, `aenv-node` links no `sqlx`. `make check-crate-boundaries` is what
fails when that stops being true (it replaced a source-scanning test that could
only see one file).

🔴 **There is no `--role` flag and no `AENV_ROLE` environment variable.** They
existed while one binary could be `api`, `node` or `all`; `aenv-api` and
`aenv-node` are two binaries with two dependency graphs and two images, and
neither declares the argument — a manifest that still passes it is refused by
argument parsing before the process starts, deliberately, so an un-migrated
manifest fails loudly instead of being ignored. **Rolling back to one process
serving everything is an image-tag change on the DaemonSet, not a flag**; see
`services/README.md`. The one place the distinction survives at runtime is
`aenv-core`, which both binaries link: `ApiImpl::runs_sandbox_runtime` reads it
off `ResumeWiring`'s wake site (`aenv-node` builds `WakeSite::Local`, `aenv-api`
builds `WakeSite::Remote`), and a handful of shared functions take a parameter
naming the one decision they need (`CentralCatalogUse`,
`AccessTokenSeedPolicy`, `Assembly::drains_on_shutdown`). Everywhere else the
answer is a constant of the binary and the branch is simply gone.

🔴 **`runs_sandbox_runtime` no longer branches any user-facing REST handler.**
Four of them used to keep a "resolve it locally" arm for the running half —
cold create (`image_resolver.resolve` + `resolve_attached_drives`), warm create
(`load_runnable`), the paused cross-node restore (`resolve_runnable`) and the
template build (`run_the_build_locally`). Those arms were dead in production
twice over: `aenv-api` never took them, and `aenv-node` answers the whole
user-facing REST surface with 404 (`src/api/role_gate.rs`) before any handler
runs — its real create/build path is gRPC
(`crates/aenv-node/src/node_server/service.rs`), which does not go through
`ApiImpl` at all. They are collapsed to the single surviving arm, and the
abstractions that existed only to let the deciding half hold a stand-in went
with them: the `TemplateBuildDriver` trait and `RefusingTemplateBuildDriver`
(whose whole file, `src/template/driver.rs`, is gone), the
`RootfsImageResolver` trait and `RefusingImageResolver` (formerly in
`src/image/contract.rs`), `resolve_attached_drives`
(`unresolved_attached_drives` and its shared `validate_attached_drives` stay),
and `ApiImpl`'s `template_builder`/`image_resolver` fields with their two
constructor parameters. `aenv-node`'s concrete `ImageResolver` and
`TemplateBuilder` are untouched — the node gRPC service still holds and uses
both, now through inherent methods. `runs_sandbox_runtime` itself survives
through `owns_sandboxes`, which `src/api/server.rs` reads to attach the role
gate. The template build's door refusal survives too, narrowed from "no local
runtime *and* no node to send it to" to "no node to send it to".

- `aenv-core` (root, lib `aenv_core`): everything both halves need — the HTTP
  API surface, the orchestrator, the snapshot catalog model, cfg, the node
  registry and binding store, the node client, p2p
- `crates/aenv-node` (`aenv-node`): the running half — Firecracker, ublk,
  overlaybd, image resolution, host setup, the node gRPC service, the byte half
  of both snapshot backends. Ships `aenv-node`
- `crates/aenv-api` (`aenv-api`): the deciding half — everything that talks to
  PostgreSQL (`pg`, the paused-registry backend, the snapshot catalog backend).
  Ships `aenv-api` and `aenv-snapshot-image`
- `adev`: dev/CI tooling CLI (`cargo adev`) for codegen, mutation tests, coverage, and CI config
- `crates/aenv` (`aenv`): native Rust CLI wrapping the AgentENV HTTP API and envd Connect-RPC endpoints
- `crates/linux-cap`: shared Linux capability inspection and child-process delegation primitives
- `crates/object-store-operator`: shared S3-compatible object store client construction and refreshable credential handling
- `crates/test-support`: shared test fixtures and helpers used across workspace integration tests
- `crates/shell-util`: shared `shell_quote` helper used by `aenv-node` and `aenv` to single-quote shell arguments
- `crates/warm-pool`: generic watermark-based resource pool shared by the network slot manager and overlaybd ublk device pooling
- `services` (Go module): the gateway control-plane service (`gateway`) with independent `go.mod` and `services/Makefile` — the Go scheduler this module used to also ship has been deleted; see "Distributed Control Plane"
- `src/api/generated`: OpenAPI-generated Axum server; regenerate with `make agentenv-server`
- `src/custom_extension_api/generated` (`custom_extension_client`): generated custom extension hook client from `src/custom_extension_api/openapi.yml`; regenerate with `make custom-extension-client`
- `thirdparty/firecracker-client`: generated Firecracker API client; regenerate with `make firecracker-client`
- `thirdparty/envd`: container init system integration; regenerate HTTP client with `make envd-http-client`
- `storage/ublk` (`uvm-ublk`): async ublk block device primitives with an OverlayBD target implementation
- `storage/ublk-daemon` (`uvm-ublk-daemon`): single-process ublk device daemon with Unix socket IPC; manages all ublk devices and their lifecycle
- `storage/overlaybd`: layered filesystem image format with pluggable backends (local, registry, tar) and io_uring-based I/O
- `storage/util` (`storage-util`): shared io_uring abstraction and ID allocator used by ublk and overlaybd

Treat generated code in `thirdparty/`, `src/api/generated/`, and `src/custom_extension_api/generated/` as machine-managed. Prefer `make` targets over manual edits.
For Firecracker runtime upgrades, update `thirdparty/firecracker-client/firecracker.yaml`, run `make firecracker-client`, package the matching Firecracker binary as `firecracker-{version}-{arch}.tgz`, update `config/deps_manifest.toml`, and follow `docs/src/internals/sandbox-testing.md`'s Firecracker upgrade checklist.

## Coding Conventions

- Rust 2021 edition. Keep touched code `rustfmt`-clean and clippy-clean.
- Use `info` for lifecycle events, `debug` for internal transitions, `warn` for recoverable issues, `error` for unrecoverable failures. Initialize tracing only in binary entrypoints.
- Conventional Commit prefixes: `feat:`, `fix:`, `refactor:`, `ci:`, `chore:`.
