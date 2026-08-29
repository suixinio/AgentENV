# Docker Compose (Multi-Node Simulation)

Run a full multi-node stack on a single host using Docker Compose. This simulates a production-like topology with a gateway, `aenv-api` (serving the `Scheduler`/`PausedRegistry` RPCs), and multiple AgentENV backend nodes.

For a real multi-machine deployment without Kubernetes, see
[Static Multi-Node](./static-multi-node.md).

## Prerequisites

- Linux kernel 6.8+
- `/dev/kvm` access (passed into the runtime containers)
- Docker and Docker Compose
- `build-essential` (`sudo apt install -y build-essential`)

The checked-in Compose setup uses standard KVM. If the host does not support
it, read [PVM Deployment](./pvm.md) before adapting the runtime image and host
configuration.

## Clone the Repository

```bash
git clone https://github.com/kvcache-ai/AgentENV.git
cd AgentENV
```

## Configure the Access-Token Seed (Optional)

See [Secure Sandboxes](../security/secure-sandboxes.md)
if the deployment needs future cross-node sandbox recovery.

## Start the Cluster

```bash
sudo bash scripts/docker-setup.sh
make deploy-up
```

The Gateway is available at `http://127.0.0.1:8000` and forwards requests to
the backend nodes.

To enable host-based sandbox data-plane URLs, set the shared sandbox proxy
domain variable when starting the stack:

```bash
SANDBOX_PROXY_DOMAINS=sandbox.example.com \
make deploy-up
```

Compose passes this value to both the gateway routing allowlist and runtime
nodes' sandbox response metadata. The domain must resolve to the gateway,
usually through wildcard DNS for `*.sandbox.example.com`.

## Verify

```bash
# Health check via gateway
curl http://127.0.0.1:8000/health

# Cluster node snapshots via gateway
curl http://127.0.0.1:8000/nodes
```

## Management Commands

```bash
make deploy-ps      # Show container status
make deploy-logs    # Stream logs from all services
make deploy-down    # Tear down the cluster
```

## Configuration

Container deployments use `deploy/docker/config/default.json`. Static node discovery and backend node endpoints are configured for the Docker network.

The runtime image includes the ublk **daemon** at `/usr/local/bin/uvm-ublk-daemon`, and `AENV_UBLK_DAEMON_BINARY_PATH` points at it. The `uvm-ublk` CLI is *not* in the image — `Dockerfile.aenv-node` builds and copies `uvm-ublk-daemon` only — so it is available for hand troubleshooting from a source checkout (`storage/ublk/README.md`) rather than from inside a container.

The compose manifest also wires node heartbeat reporting from runtime nodes to `agentenv-api`:

- `AENV_NODE_ID` is set explicitly per node container (`node-a`, `node-b`).
- `AENV_OBSERVABILITY_SCHEDULER_REPORT_ENABLED=true` enables heartbeat reporting.
- `AENV_OBSERVABILITY_SCHEDULER_ENDPOINT` is set to `http://agentenv-api:8002`.
- `SANDBOX_PROXY_DOMAINS`, when set, is passed through as both
  `GATEWAY_SANDBOX_PROXY_DOMAINS` and `AENV_SANDBOX_PROXY_DOMAINS`.
