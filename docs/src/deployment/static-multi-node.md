# Static Multi-Node (Without Kubernetes)

🔴 **This guide is out of date and not currently actionable.** It carried a
systemd walkthrough for `services/scheduler` — the Go Scheduler binary — and
that package is **deleted from the repository**, not merely superseded:
`Scheduler`/`PausedRegistry` RPC handling now lives entirely in the Rust
`aenv-api` binary (`src/node_registry/`,
`crates/aenv-api/src/orchestrator/paused_registry/postgres/`), which requires
a reachable PostgreSQL (`[pg]`) unconditionally and today ships as a container
image (`deploy/docker/Dockerfile.aenv-api`), not as a bare systemd-friendly
binary with a config story equivalent to what this page describes. Kubernetes
([Kubernetes (Multi-Node)](./kubernetes.md)) is the only
currently-documented and current multi-node deployment path; Docker Compose
([Docker Compose](./docker-compose.md)) is the only currently-documented
single-host multi-node simulation.

**What has changed since this banner was first added:** static node discovery
was ported from the deleted Go scheduler into `aenv-api` itself
(`src/node_registry/static_discovery.rs`), so a no-Kubernetes, native-mode
topology is no longer blocked on a Kubernetes API to discover nodes against.
The pieces confirmed to exist and to be exercised by a real deployment today
(`deploy/docker-compose.yml`) are:

- `aenv-api` answers node placement/heartbeat/paused-registry itself,
  unconditionally, instead of dialing a Scheduler process — there is none to
  dial any more, and no switch left to choose otherwise (the
  `[cluster].node_placement_source` field this used to require setting to
  `"native"` is deleted along with the `"scheduler"` alternative it selected
  against).
- `[cluster].node_discovery_mode = "static"` (env
  `AENV_CLUSTER_NODE_DISCOVERY_MODE=static`) makes that native registry seed
  itself from a configured node list instead of Kubernetes EndpointSlice
  discovery. It defaults to `kubernetes`, so this must be set explicitly.
- `[cluster].static_discovery_nodes` supplies that list — an array of
  `{id, endpoint}` entries, deliberately **TOML-file-only with no `env =`
  binding** (see the field's own doc comment in `src/cfg.rs`), so it can only
  reach the process through the config file itself or an
  `AENV_CONFIG_OVERLAY_PATH` overlay. `deploy/docker/config/cluster-static-discovery-overlay.toml`
  is a tracked, working example of exactly that shape, and is what
  `deploy/docker-compose.yml` mounts via `AENV_CONFIG_OVERLAY_PATH` for its
  own `agentenv-api` service.
- The list is seeded into the registry **once**, at `aenv-api` start-up, with
  no ongoing watch (`start_native_node_registry`,
  `crates/aenv-api/src/bin/aenv-api.rs`) — the same one-shot behavior the
  deleted Go scheduler's static branch had, so adding, removing, or moving a
  node still means a restart, not a hot reload.
- `aenv-api` requires `[pg]` unconditionally now (there is no
  PostgreSQL-free deployment of it any more, single-machine included — see
  CLAUDE.md's snapshot-catalog section) — a prerequisite the deleted
  Go-scheduler-based guide never had.

🔴 **What is explicitly not verified and therefore not written here:** how to
distribute `aenv-api`/`aenv-node` as bare binaries outside a container image
(today's only shipped artifacts are the Docker images under `deploy/docker/`),
systemd unit shapes for either binary, the bare-metal host-setup sequence
(`/dev/kvm`, ublk, OverlayBD packages — normally provisioned by `server
--setup-host` inside the container image's own entrypoint) on a machine not
already running that container flow, and an actual end-to-end run of this
topology. Rewriting this guide into a complete, tested native-mode,
no-Kubernetes walkthrough is tracked as follow-up work, not done here — see
`services/README.md` for the current architecture status.
