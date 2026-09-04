# CLAUDE.md

## What is AgentENV

AgentENV is a Rust workspace for running AI agents inside isolated, snapshot-capable Firecracker-based environments. It exposes an E2B-compatible HTTP API so agents can create, pause, resume, and reuse sandboxes. Requires a Linux host with `/dev/kvm` access and a host virtualization setup matching `virtualization_mode` (`kvm` by default; `pvm` requires x86_64 and `kvm_pvm`).

## Build, Lint, Test

```bash
make                          # build the workspace
make fmt                      # rustfmt check
make clippy                   # clippy with -D warnings
make test                     # full test suite (agent + envd + ublk)
make test-unit                # unit tests: --lib --bins over every workspace crate (what CI runs)
make test-agent-integration   # integration tests (crates/aenv-node/tests/*.rs)
make test-with-redis          # src/orchestrator/store/ suite against a real redis-server
make test-with-postgres       # crates/aenv-api/src/pg/ suite against a real postgres
make check-crate-boundaries   # aenv-api links no overlaybd/ublk; aenv-node links no sqlx
make bench                    # snapshot benchmarks
make start-server             # build and run aenv-node (auto-provisions dependencies)
```

- `make test-unit` runs `--lib --bins`. Tests in a `main.rs` or under `src/bin/` are part of daily validation, so put a test where it belongs, not where the runner can see it. `aenv-egress` is reached twice: once with default features (`core`) and once with `--features bin`, because its TLS signer, Vault source and `http` handler exist only under `bin`.
- `make test-agent-integration` is two cargo invocations: the egress tests need `[egress_broker].mode = "embedded"` (`tests/fixtures/egress-embedded-overlay.toml`, via `AENV_CONFIG_OVERLAY_PATH`) and refuse any other mode, so they run alone while the first invocation skips them. A new integration test that needs its own config joins that pattern rather than changing `config/default.toml`.
- `make test-with-redis` / `make test-with-postgres` set `AENV_REDIS_TEST_REQUIRED=1` / `AENV_PG_TEST_REQUIRED=1`, which turn a missing server (or any `SKIPPED[...]` line) into a failure. The same suites run under `make test-unit`, where a missing server is a silent skip. `REDIS_SERVER_BIN`, `INITDB_BIN` and `POSTGRES_BIN` pick specific binaries; Debian installs the PostgreSQL ones under `/usr/lib/postgresql/<version>/bin/`, off `PATH`.
- Two backends share one contract suite in each of `src/orchestrator/store/contract.rs` (in-memory / Redis metadata store) and `src/binding_store/contract.rs` (in-memory / Redis binding store). After changing either backend, run the Redis target so both sides are exercised.
- Tests must never write out an executable they then `execve`: a concurrent `fork` inherits the write fd and the exec fails with `ETXTBSY` (rename does not help). Ship such scripts under `tests/fixtures/`.

Dev/CI tooling via `cargo adev`:
```bash
cargo adev codegen            # regenerate all OpenAPI clients/server
cargo adev mutants            # mutation tests
cargo adev coverage           # code coverage
make firecracker-client       # = cargo adev codegen firecracker
make envd-http-client         # = cargo adev codegen envd
make agentenv-server          # = cargo adev codegen server
make custom-extension-client  # = cargo adev codegen custom-extension
```

Go control plane (`services/`, a separate module shipping only `gateway`):
```bash
make -C services build
make -C services test             # gateway, shared, api
make -C services test-with-redis  # same, but a missing redis-server fails instead of skipping
make -C services run-gateway
```
Without `redis-server` on `PATH`, `shared/routing`'s suite skips and still reports `ok`; `AENV_REDIS_TEST_REQUIRED=1` (the same variable the Rust harness reads) and `REDIS_SERVER_BIN` are the switch. Go caches test results: after editing files that Go guards scan (`config/default.toml`, `deploy/k8s/base/`), re-run with `-count=1`. Validate `services/` changes with `make -C services test` in addition to the Rust checks.

Run a single test:
```bash
cargo test -p aenv-core --lib test_name      # pick the crate that owns it: aenv-core, aenv-node, aenv-api
sudo -E cargo test -p aenv-node --test orchestrator_integration orchestrator::test_name
```
Integration tests require root (network namespaces), `/dev/kvm`, host modules matching `AENV_VIRTUALIZATION_MODE`, and `AENV_CONFIG_PATH` pointing to a valid config.

## Architecture

See `docs/src/internals/architecture.md` for the full design with data-flow diagrams, `docs/src/internals/services.md` and `services/README.md` for the control plane, and `docs/src/configuration/` for every config field and environment variable (including the removed ones and what setting them does today).

### Storage

Two orthogonal data paths: **block devices** (rootfs/extra drives) and **memory snapshots** (ublk-backed memory restore from overlaybd layers). Block pipeline: overlaybd image layers → ublk userspace block device → `/dev/ublkbN` in the VM.

- **overlaybd** (`storage/overlaybd/`): LSMT layered image format — immutable compressed read-only layers under one writable upper. Backends: `LocalFile` (io_uring, optional O_DIRECT), `registryfs_v2` (OCI registry), `tar`; zstd with random-access jump tables. Entry point `image/image_file.rs`; LSMT under `lsmt/file/` (`readonly.rs`, `readwrite.rs`, `stack.rs`, `helper.rs`, `types.rs`). Image resolution keeps `{image.cache.root_dir}/{commits,indexes,configs}`. The Firecracker pause path uses `close_seal + restack` and is the only path that seals an upper; `export_upper_as_sealed` has no production caller and is kept deliberately (its doc comment says so). Small buffered writes (≤ 4 KiB) use synchronous `pwrite`, which assumes fast local storage.
- **ublk** (`storage/ublk/`): async ublk primitives — `UVMUblkCtrl` for ADD/DEL on `/dev/ublk-control`, per-queue worker threads with thread-local `AsyncIoRing`, the `OverlaybdTarget`. `UBLK_F_AUTO_BUF_REG` on kernel 6.8+.
- **ublk-daemon** (`storage/ublk-daemon/`): `uvm-ublk-daemon` owns every ublk device in one process; length-prefixed JSON over a Unix socket (`CreateOverlaybd`, `AcquireOverlaybd`, `ReleaseOverlaybd`, `UpdateSize`, `RestackSnapshot`, `Delete`, …). Keeps a warm pool of reusable devices. `crates/aenv-node/src/sandbox/ublk/device.rs` wraps the client in the `UblkDeviceManager` singleton.
- **storage-util** (`storage/util/`): `AsyncIoRing<S>`, `IoRingWorker`, `ReloadableIDAllocator`.
- **Sandbox integration** (`crates/aenv-node/src/sandbox/ublk/`, `extra_drive.rs`): materializes runtime configs, creates daemon-managed devices for rootfs and extra drives; attached-drive snapshots export `drives/<id>/image.json` so writable drives survive pause/resume and template publication.
- **Memory snapshots**: on pause, Firecracker takes a state-only diff snapshot, AgentENV reads dirty/present ranges with `process_vm_readv` into an overlaybd memory layer stacked on the parents; on resume a read-only ublk device is passed to Firecracker as a file-backed memory backend. Sandboxes from one snapshot share the memory device by refcount.

Registry access goes through `regctl` (manifests, config blobs, layers, tools drive via `crates/aenv-node/src/setup/deps.rs::extract_ext4_from_ghcr` + `umoci`, OCI referrers when `[image_resolver].try_referrers_overlaybd_prefixes` is set). `crates/aenv-node/src/image/oci_image.rs` classifies manifests: standard OCI images are copied and converted per layer into local `.commit` files; overlaybd-native images emit a remote-ref `image.json` read directly from the registry. For private registries, `docker login` before starting the server; `write_generated_overlaybd_global_config` wires `~/.docker/config.json` into the overlaybd runtime.

P2P artifact transport (`src/p2p/`, `docs/src/internals/p2p-design.md`): consumers depend on `P2pTransport`; `DisabledP2pTransport` is the default, `IrohBlobsP2pTransport` embeds `iroh` + `iroh-blobs`. Overlaybd layer artifact identity is owned by `crates/aenv-node/src/overlaybd/p2p/artifact.rs`; snapshot publishing reuses it rather than inventing snapshot-specific keys. The node registry keeps only key-to-node hints (`RecordP2pArtifact`/`ForgetP2pArtifact`/`LookupP2pArtifact`), never locators or bytes.

Node-local metadata carries no embedded database (`make check-crate-boundaries` fails a workspace that links one). Rebuild it from the on-disk layout where the layout is the truth, or store it with `JsonRecordDir` (`src/record_dir.rs`) — one atomic JSON file per record, under an explicit `RecordDurability` chosen in code, not config.

### Distributed Control Plane

Three processes, two Rust binaries and one Go binary:

"scheduler" names the `scheduler.v1` protocol surface that `aenv-api` serves — config keys, proto, Role names and metric names all keep the word; prose describing the actor says "the api half".

- **`aenv-api`** (deciding half, multi-replica): the full REST surface, the in-process node registry (`src/node_registry/`) that answers the `Scheduler` gRPC contract (`services/api/proto/scheduler.proto`, the node-to-api face; `aenv-node` is its only client) — the only implementation of it — with round-robin placement (`RoundRobinStrategy`; the `strategy=round_robin` metric label is a series identity and must not change shape), the Redis sandbox-to-node binding store (`src/binding_store/`, `AENV_BINDING_STORE_REDIS_URL`) and the PostgreSQL snapshot catalog. A paused sandbox is a row in that catalog and nothing else: a sandbox-source snapshot whose committed payload carries its `PausedSandboxConfig` (`src/snapshot/types/paused.rs`), with `origin_node_id` naming the node that staged it and rewritten to wherever a resume lands (`SnapshotCatalog::set_origin_node_id`). Pause CASes `Running → Pausing`, commits the node's staged capture (`CommittingPausePublisher`), then deletes the record and the routing binding; resume is a create under the sandbox's own id from its newest ready row (`restore_sandbox`, `preferred_node_id = origin_node_id` as a placement hint only), and the next pause writes a new row. Single activation is the binding store's `Starting` reservation, not a claim on the row. It **requires `[pg]`** and refuses to assemble without it. Node discovery: `[cluster].node_discovery_mode` = `"kubernetes"` (default; watches EndpointSlices per `[cluster.kubernetes_discovery]`) or `"static"` (`[cluster].static_discovery_nodes`). Rolling this half back is an image digest change on its Deployment, no flags; see `services/README.md`'s "Rolling the API half back".
- **`aenv-node`** (running half, DaemonSet): holds no catalog — its `SnapshotRepository` carries `NoSnapshotCatalog`, which refuses catalog calls; a node stages bytes (`SnapshotRepository::stage`) and `aenv-api`'s `commit_staged` writes the row. Answers the user-facing REST surface with 404 (`src/api/role_gate.rs`); its real create/build path is gRPC (`crates/aenv-node/src/node_server/service.rs`). Heartbeats to `[cluster].scheduler_endpoint` (always `aenv-api`), hot-reloadable via `src/scheduler_endpoint.rs::SchedulerEndpointSource`. Holds no paused sandboxes: `Pause` captures the VM, stages the capture on the node's snapshot repository (`StagingPausePublisher`), stops the VM and forgets it; a node restart loses the sandboxes that were running on it.
- **`gateway`** (`services/gateway/`): sandbox data-plane reverse proxy, and nothing else — user-facing REST, `/nodes*` and `/registry/sandboxes` are `aenv-api`'s, at its own address. Traffic named by routing headers or a proxy domain is routed from the Redis routing projection; a miss goes to `apiproxy.ResumeSandbox` at `gateway.scheduler_addr`, which answers running sandboxes and wakes paused ones (and writes the projection back). The gateway calls no scheduler.v1 RPC; an api half it cannot ask is a 502, and a request naming no sandbox gets a 404. Responses the gateway synthesizes carry CORS headers (`internal/cors`); a sandbox's own responses never do.

Config that is TOML-file-only (an `Option<_>` section or a list, which confique cannot bind from the environment): `[pg]`, `[backend.oss]`, `[backend.posix_fs]`, `[cluster].static_discovery_nodes`. Set them in the file `AENV_CONFIG_PATH` names or an `AENV_CONFIG_OVERLAY_PATH` overlay (`:`-separated TOML files deep-merged over the main file, environment still on top; a named-but-missing file is a startup error). `deploy/k8s` and `deploy/docker-compose.yml` use overlays for exactly this.

Removed switches: `--role`/`AENV_ROLE` are refused by argument parsing; `AENV_NODE_PLACEMENT_SOURCE`, `AENV_BINDING_STORE_BACKEND`, `AENV_PAUSED_REGISTRY_BACKEND`, `AENV_SNAPSHOT_CATALOG_{WRITE,READ,MIRROR_PATH}`, the gateway's query-only/fallback variables, `GATEWAY_REST_UPSTREAM_ADDR`, `GATEWAY_COLD_LOOKUP_TIMEOUT` and `GATEWAY_ROUTING_PROJECTION_AUTHORITATIVE` are ignored. `docs/src/configuration/env-vars.md` lists each with what setting it does today. `services/shared/config/snapshot_catalog_manifest_test.go` walks `deploy/k8s/base` and fails if any workload still declares a catalog variable, and `gateway_removed_keys_manifest_test.go` does the same for the gateway variables over `deploy/k8s/base` and `deploy/docker-compose.yml` — keep both working.

### Per-Node Subsystems

Each node runs `crates/aenv-node/src/bin/aenv-node.rs` on a host with `/dev/kvm` and wires together:

- **API layer** (`src/api/`): Axum server with OpenAPI-generated endpoint traits (`src/api/generated/` from `src/api/openapi.yml`); implementations in `src/api/impls/`. The sandbox proxy (`src/api/proxy.rs`; headers `x-agentenv-sandbox-id`, `x-agentenv-target-port`) is mounted by `src/api/server.rs::new` (`aenv-node`) only; `new_control_plane_only` (`aenv-api`) carries no data-plane routes.
- **Orchestrator** (`src/orchestrator/`): sandbox lifecycle state machine (Creating → Running → Forking | Snapshotting | Pausing, Killing) in `service.rs`; a pause ends in a published snapshot row and no record, and publication failure resumes the VM in place (`OrchestratorError::PausePublicationFailed`) or tears it down if it cannot. `fork_sandbox` forks one running sandbox into N children on the same node; `capture_snapshot` drives Running → Snapshotting → Running, rolling back on recoverable failure and tearing down on `SandboxCaptureError::Terminal`. Keeps `OrchestratorCounters`, derives running counts and allocated resources from metadata (`aggregate_resource_metrics`), publishes best-effort broadcast lifecycle events, evicts expired sandboxes, and stops every sandbox on graceful shutdown.
- **Observability** (`src/observability/`): node snapshots for admin/node APIs; `reporter.rs` sends periodic `Heartbeat` RPCs and drains lifecycle events into `ReportSandboxEvent`. `machine.rs` optionally runs `cpu-template-helper template dump`; the registry ANDs every node's config (`src/node_registry/cpu_template.rs`, golden-tested) and returns the intersection, applied to new VMs via Firecracker's pre-boot `PUT /cpu-config`.
- **Sandbox** (`src/sandbox/`): Firecracker VMs, network namespaces (veth, iptables), rootfs file injection via debugfs, envd, MMDS (`firecracker/mmds.rs`, 169.254.169.254), ublk devices. `firecracker/sandbox.rs` owns a stable `SandboxId`; serial logs live under `{serial_output_base_dir}/{SandboxId}/`. `SandboxBackend` exposes `snapshot()`/`fork()`/`pause`/`resume`/`stop`; captured state is an opaque `CapturedSandboxSnapshot` that keeps its temp dir alive until published.
- **Snapshot + Template Builder** (`src/snapshot/`, `crates/aenv-node/src/template/`): `SnapshotManager::publish_captured` commits a captured snapshot; the manifest stores logical content (metadata, rootfs layers, drive layers) and backends derive fixed artifact paths (`vm_state.bin`, `mem_image.json`) from layout conventions. `crates/aenv-node/src/template/builder.rs` builds snapshots from RUN / ENV / WORKDIR steps. `repository.catalog()` (rows, PostgreSQL) vs `repository.artifacts()` (bytes, `[snapshot.repository_backend]` = `posix_fs` | `oss`). With `oss`, `snapshot_image_storage = "source_registry"` publishes overlaybd-native deltas back to the source OCI registry (`repository/backends/common/acr/`).
- **`aenv-snapshot-image`** (`crates/aenv-api/src/snapshot/image_export/`, `make build-snapshot-image`): operator tool that publishes a committed snapshot's rootfs as an overlaybd-native OCI image via `regctl` (`aenv-core`'s `src/image/regctl.rs`). Requires `[pg]`; idempotent through the registry manifest digest; refuses pre-existing conflicting references.
- **Custom extension** (`src/custom_extension_api/`, `src/sandbox/custom_extension/`): optional external HTTP service at `[custom_extension].url` with lifecycle hooks `POST {url}/sandbox-hook/{start-fresh,start-resume,patch-params,stop}`. Start hooks carry `networkNamespacePath`, `hostInteractionIp` and a per-instance `sandboxInstanceId`; `stop` is best-effort, every other hook failure fails the operation. `customExtensionParams` (opaque JSON; absent ≡ `{}`; rejected non-empty when no URL is configured) flow `CreateSandboxRequest → SandboxLaunchConfig → FirecrackerCommonConfig`, persist through pause/resume and snapshots, and are exposed via `GET`/`PATCH /sandboxes/{id}/custom-extension-params`. The generated client is regenerated with `make custom-extension-client`; prune orphan files under `src/custom_extension_api/generated/` by hand after schema removals.
- **Brokered egress** (`crates/aenv-node/src/sandbox/egress/`, `src/sandbox/network/policy.rs`): a policy with `rules` normalizes to internal `brokers`; the runtime opens a listener inside the sandbox namespace (`Slot::listen_in_namespace`), DNATs guest port 443 onto it (`Slot::install_intercept`, chains `AGENTENV-INTERCEPT` in nat and filter) and relays each accepted connection with an `IdentityHeader` through `EgressRuntime`'s `BrokerTransport` (`[egress_broker].mode`: `embedded` in-process, `remote` to `aenv-egress`). The api half grants secret names per `(sandbox, execution)` through `Orchestrator::set_grant_issuer` before start and revokes on delete and pause, and the node installs `GrantsIssuedUpstream` so its own orchestrator never refuses a policy the api half granted; nodes report `NodeSnapshot.egress_broker` and placement honours `NewSandboxHint.requires_egress_broker`. Where values live is `[secrets].backend`: `postgres` keeps them encrypted in `aenv-api`'s own database and answers the broker's lookups at `POST /internal/credentials/resolve`, a route mounted through `server::new_control_plane_only`'s extra-routes seam and deliberately absent from `src/api/openapi.yml`; `vault` and `external_resolver` keep them elsewhere and the api half then never reads one back.
- **Config** (`src/cfg.rs`): `config/default.toml` (or `AENV_CONFIG_PATH`) plus `AENV_CONFIG_OVERLAY_PATH`. `home_path`/`AENV_HOME_PATH` is the base for local state; `deps_path`/`AENV_DEPS_PATH` is only the root for downloaded runtime assets.

Host setup: `aenv-node --setup-host --runtime-user <user> --runtime-group <group>` does the one-time root work (`/dev/kvm` group, ublk permissions, overlaybd system config, sysctls); normal startup validates it and fails with actionable errors. The Docker images take an `AENV_GIT_COMMIT` build arg that `build.rs` bakes in; `make k8s-build`/`deploy-build` pass it automatically. For Firecracker upgrades follow the checklist in `docs/src/internals/sandbox-testing.md`.

## Workspace Crates

- `aenv-core` (root, lib `aenv_core`): everything both halves need — HTTP API surface, orchestrator, snapshot catalog model, cfg, node registry, binding store, node client, p2p
- `crates/aenv-node`: the running half — Firecracker, ublk, overlaybd, image resolution, host setup, node gRPC service, the byte half of both snapshot backends. Ships `aenv-node`
- `crates/aenv-api`: the deciding half — everything that talks to PostgreSQL (`pg`, snapshot catalog backend, `secret_refs`/`secret_values`), the three secrets backends and the broker's resolve endpoint. Ships `aenv-api` and `aenv-snapshot-image`
- `crates/aenv-egress`: the runtime-to-broker contract for `network.rules` (identity header, framing, `BrokerTransport`, `Handler`, `CredentialSource`, `UpstreamGuard`) and the broker dispatch core. `aenv-node` links its `core` feature only; `make check-crate-boundaries` fails if it links `tls` or if the crate gains a database, the byte half, `aenv-core` or a second TLS stack
- `adev`: dev/CI tooling CLI (`cargo adev`)
- `crates/aenv`: native Rust CLI over the HTTP API and envd Connect-RPC
- `crates/linux-cap`, `crates/object-store-operator`, `crates/test-support`, `crates/shell-util`, `crates/warm-pool`: shared helpers (capabilities, S3 client construction, test fixtures, `shell_quote`, watermark pool)
- `services` (Go): the gateway
- `storage/overlaybd`, `storage/ublk` (`uvm-ublk`), `storage/ublk-daemon` (`uvm-ublk-daemon`), `storage/util` (`storage-util`)
- Generated, machine-managed — regenerate with `make`, do not hand-edit: `src/api/generated`, `src/custom_extension_api/generated`, `thirdparty/firecracker-client`, `thirdparty/envd`

The runtime-role distinction survives in one place in `aenv-core`: `ApiImpl::runs_sandbox_runtime`/`owns_sandboxes`, read off `ResumeWiring`'s wake site (`aenv-node` builds `WakeSite::Local`, `aenv-api` `WakeSite::Remote`). Everywhere else the answer is a constant of the binary.

## Coding Conventions

- Rust 2021 edition. Keep touched code `rustfmt`-clean and clippy-clean.
- Logging: `info` for lifecycle events, `debug` for internal transitions, `warn` for recoverable issues, `error` for unrecoverable failures. Initialize tracing only in binary entrypoints.
- Conventional Commit prefixes: `feat:`, `fix:`, `refactor:`, `ci:`, `chore:`, `docs:`, `test:`.

### Comments

Comments state what the code cannot: an invariant, a non-obvious constraint, a pointer to the thing that depends on this. They do not narrate.

- `///` goes on `pub` items, one to three lines, describing the contract. Private functions, fields and test functions get no doc comment unless a single line prevents a real misreading.
- Test names state the behaviour; that is their documentation. Do not put a paragraph above a `#[test]`.
- History belongs in the commit message and `docs/proposals/`, not in the source: no "used to be", "before this existed", "X is deleted", no descriptions of code that no longer exists, no counterfactuals about what would break without this line.
- No emphasis markers (emoji, bold, headings) inside source comments.
- A guard that scans source or config text must anchor on syntax (a call expression, a line start), never on a bare substring — a comment describing the pattern matches too.

### This file

`CLAUDE.md` is instructions for working in this repository: how to build and test, where things live, what the conventions are. It is not a changelog. When something is removed, update the sentence that described it; do not add a paragraph explaining that it is gone and why — removed configuration goes in `docs/src/configuration/env-vars.md`, design rationale in `docs/proposals/`, operational history in the commit message. Keep it under roughly 200 lines.
