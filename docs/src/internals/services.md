# Distributed Control Plane

The multi-node control plane's Go half lives in `services/` as a separate Go
module. It routes client traffic across multiple AgentENV backend nodes.

🔴 `services/scheduler` — the original standalone Go implementation of node
selection, sandbox-to-node binding, observed node snapshots, and P2P peer
endpoint discovery — has been deleted. That RPC surface is now answered
in-process by the Rust `aenv-api` binary (`src/node_registry/`); see
CLAUDE.md's "Distributed Control Plane" section for the full picture.

## Components

- **Gateway** (`services/gateway/`): HTTP reverse proxy that routes by sandbox ID — the only Go binary this module ships now
- **aenv-api native registry** (`src/node_registry/`): answers the same gRPC contract in-process; see `docs/src/internals/architecture.md`'s "Distributed Control Plane" section

## Build and Test

Prerequisites: Go 1.21+

```bash
# From services/
make build      # builds gateway
make test       # tests gateway, shared, api
make tidy       # go mod tidy + formatting
make proto      # regenerate protobuf
```

## Run Locally

```bash
# Start gateway
make -C services run-gateway
```

Point `gateway.scheduler_addr` at whichever process answers
`services/api/proto/scheduler.proto` — `aenv-api`'s gRPC listener on every
current deployment.

## Discovery Modes

`aenv-api`'s native registry supports two node discovery modes
(`[cluster].node_discovery_mode`):

- **kubernetes** (default): watches EndpointSlices for a headless Service, using ready Pod IPs as backends
- **static**: explicit node list from `[cluster].static_discovery_nodes` (config-file/overlay only — no environment-variable binding), seeded once at startup with no ongoing watch. `deploy/docker-compose.yml` sets this explicitly, since it has no Kubernetes API to discover against.

## Deployment

### Docker Compose

```bash
make deploy-up      # gateway + agentenv-api (native placement) + 2 backend nodes
make deploy-ps      # status
make deploy-logs    # logs
make deploy-down    # teardown
```

### Kubernetes

```bash
make k8s-render     # render manifests
make k8s-apply      # apply to cluster
```

Deployment model:
- `gateway`: Deployment + ClusterIP Service
- `agentenv-node`: privileged DaemonSet with `/dev/kvm`, one host-compatible
  KVM/PVM mode, and hostPath
- `agentenv-api`: Deployment answering the `Scheduler`/`PausedRegistry` RPCs
- `agentenv-nodes`: headless Service for Kubernetes-mode node discovery

## gRPC API

Proto contract: `services/api/proto/scheduler.proto`

RPCs: `Schedule`, `ListNodes`, `LookupNode`, `RecordAssignment`, `Heartbeat`, `ReportSandboxEvent`, `ListObservedNodes`, `ListP2pPeers`, `RecordP2pArtifact`, `ForgetP2pArtifact`, `LookupP2pArtifact`, `GetNode`, `UnregisterNode`, `ListRegistrySandboxes`

Runtime node heartbeats may include an opaque `P2pEndpoint` containing a backend name and backend-specific address. `aenv-api`'s native registry stores that endpoint with the observed-node record and returns ready peers through `ListP2pPeers(cluster_id, backend, exclude_node_id)`. It does not query artifact catalogs and never forwards artifact data.

For full configuration details (header compatibility, timeouts, logging), see the [services README](https://github.com/kvcache-ai/AgentENV/blob/main/services/README.md).
