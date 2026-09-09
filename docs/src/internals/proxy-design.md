# AgentENV Proxy Design and Usage

This document describes the per-node reverse proxy that forwards requests into individual sandboxes. For the distributed routing layer (gateway to scheduler to node), see [System Architecture](./architecture.md).

## Scope

Each AgentENV node runs a reverse proxy that accepts requests on its API server
and forwards them to sandbox services over the sandbox interaction network. In a
multi-node deployment, the gateway resolves sandbox ownership through the
scheduler and forwards data-plane requests to the owning node's proxy surface.

Current entrypoints:

- `ANY /proxy`
- `ANY /proxy/{*proxy_path}`
- Any otherwise unmatched path that carries sandbox routing headers
- Host-based sandbox proxy requests when `[sandbox_proxy].domains` is configured:
  `{port}-{sandboxID}.{domain}`

Implementation references:

- `crates/aenv-node/src/api/proxy.rs`
- `crates/aenv-node/src/orchestrator/service.rs`
- `crates/aenv-node/src/orchestrator/proxy.rs`

## Routing Contract

Each proxied request must identify:

- Sandbox ID
- Target port inside the sandbox service plane

Accepted headers:

- `x-agentenv-sandbox-id`
- `x-agentenv-target-port`
- E2B-compatible aliases:
  - `e2b-sandbox-id`
  - `e2b-sandbox-port`

Validation:

- Sandbox ID must be a valid UUID format.
- Target port must parse as `u16` and be greater than `0`.

Host-based routing derives both fields from `Host`. The configured domain must
match exactly after lowercase normalization and optional trailing-dot removal.
Sandbox IDs in host routes must be valid UUIDs and the target port must fit in
`u16`.

## Runtime Route Model

AgentENV keeps an in-memory runtime route table in orchestrator.

- Route key: `SandboxId`
- Route value: `ProxyTarget` (currently host interaction IP) plus route metadata (`version`, `updated_at`)

Design rule:

- Only `Running` sandboxes publish runtime routes.
- Non-running states rely on metadata fallback (not route-table state).

Lookup behavior (`proxy_lookup_for`):

1. If runtime route exists: `Ready(target)`
2. Else read metadata:
   - no metadata: `NotFound`
   - metadata state is `Running`: `RouteMissing`
   - other states: `Unavailable(state)`

A paused sandbox has no metadata on any node (it is a snapshot-catalog row), so it lands on `NotFound`.

This keeps hot-path reads lock-light and avoids reading sandbox instance internals in API request paths.

## Paused Sandboxes

🔴 **The proxy does not wake sandboxes.** It forwards bytes; deciding that a
sandbox should be alive belongs to the half that owns sandboxes, and the data
plane reaches that decision over the gateway's cold path
(`SandboxResumeService`, `crates/aenv-api/src/api/grpc/resume.rs`) before traffic ever arrives
at a proxy. A paused sandbox is a snapshot-catalog row, not anything a node
holds, so the node proxy has no paused state to report.

Request outcomes for paused sandboxes:

- `404 Not Found`, whatever `auto_resume` says: the node is not running the
  sandbox, and this request will not change that. The wake-up is a create
  from the row on `aenv-api` (refused with `auto_resume_disabled` when the
  row's `auto_resume` is false); the proxy only ever sees the sandbox once it
  is running.

The lifetime a woken sandbox gets and the bound the wake-up runs under are
declared in `crates/aenv-api/src/api/impls/resume_surface.rs` —
`auto_resume_min_sandbox_timeout()` (`EnsureMinimum`,
`orchestrator.auto_resume_min_sandbox_timeout_secs`, 5 minutes by default) and
`auto_resume_deadline()` (`60s` outside test builds) — beside the wake-up that
reads them, on the half that answers every wake over the gateway.

## Lifecycle Hooks and Race Hardening

Route publication/removal is tied to lifecycle transitions.

- Create/Resume success path:
  - Persist metadata to `Running`
  - Publish runtime route only if the launching sandbox handle is still the current handle
- Pause/Delete/Rollback paths:
  - Atomically detach sandbox handle and runtime route before stop/finalization

Race protections:

- Late route publication from stale handles is blocked via pointer identity check.
- Handle detachment and route removal happen in one critical section to reduce stale-route visibility windows.
- Launch rollback supports both transitional-state rollback and running-state rollback paths.

## HTTP Forwarding Semantics

### Path and Query

- `/proxy` forwards to upstream `/`
- `/proxy/{*proxy_path}` forwards raw URI path suffix after `/proxy`
- Header-routed fallback requests forward the original request path
- Host-based requests forward the original request path
- Query string is forwarded unchanged

Important details:

- Percent-encoded path segments are preserved.
- Repeated leading slashes are preserved.
  - Example: `/proxy//api` forwards as `//api`
- Host-based proxy routing runs before Axum route matching. When a configured
  sandbox proxy host is used, the request is data-plane traffic even if its path
  resembles a control-plane API.

### Header Handling

Control-plane routing headers are stripped before forwarding upstream:

- `x-agentenv-sandbox-id`
- `x-agentenv-target-port`
- `e2b-sandbox-id`
- `e2b-sandbox-port`

Hop-by-hop headers are stripped on both request and response paths, including:

- Standard hop-by-hop headers (`Connection`, `Upgrade`, `TE`, `Trailer`, `Transfer-Encoding`, `Proxy-Authenticate`, `Proxy-Authorization`, `Keep-Alive`)
- Any extra headers nominated by `Connection`

Forwarded headers are injected:

- `x-forwarded-host`
- `x-forwarded-proto`
- `x-forwarded-method`
- `x-forwarded-uri`

### Streaming

HTTP bodies are proxied as streams (request and response), including SSE and large uploads/downloads.

## WebSocket Semantics

WebSocket upgrade is supported through the same `/proxy` endpoints.

- Client upgrade request is validated and forwarded upstream.
- Bidirectional frame bridging is established after successful upstream handshake.
- Selected subprotocol from upstream is propagated to the client.

Handshake failure behavior:

- If upstream rejects with an HTTP response (for example `401`, `403`, `404`), status/body are forwarded as-is.
- Transport or connection failures return `502 Bad Gateway`.
- Handshake timeout returns `504 Gateway Timeout`.

## Error Mapping

- `400 Bad Request`
  - Missing/invalid sandbox routing header
  - Missing/invalid target port header
  - Invalid upstream URI construction
- `404 Not Found`
  - Sandbox not found, which includes every paused sandbox (regardless of `auto_resume`)
- `410 Gone`
  - Sandbox exists but is not proxyable in current state
- `502 Bad Gateway`
  - Upstream transport/connect failure
  - Sandbox is `Running` but runtime route is missing (`RouteMissing`)
- `504 Gateway Timeout`
  - Upstream response header timeout
  - Upstream websocket handshake timeout

## Usage Examples

### HTTP

```bash
curl -i \
  -H 'X-API-Key: test-key' \
  -H 'x-agentenv-sandbox-id: <sandbox-uuid>' \
  -H 'x-agentenv-target-port: 8080' \
  'http://127.0.0.1:8000/proxy/health?full=true'
```

### E2B-compatible headers

```bash
curl -i \
  -H 'X-API-Key: test-key' \
  -H 'e2b-sandbox-id: <sandbox-uuid>' \
  -H 'e2b-sandbox-port: 8080' \
  'http://127.0.0.1:8000/proxy/status'
```

### WebSocket

Example (generic):

- URL: `ws://127.0.0.1:8000/proxy/ws/echo`
- Headers:
  - `x-agentenv-sandbox-id: <sandbox-uuid>`
  - `x-agentenv-target-port: <port>`
  - API auth headers as required by your deployment

### Host-based

```bash
curl -i \
  -H 'X-API-Key: test-key' \
  'http://8080-<sandbox-uuid>.sandbox.example.com/status'
```

## Testing Notes

Relevant test coverage exists in:

- `crates/aenv-node/src/api/proxy.rs` unit tests (HTTP, SSE, large body, websocket, headers, path preservation, error mapping)
- `crates/aenv-node/src/orchestrator/service.rs` unit tests (route publication/removal behavior and stale-handle guard)
- Integration lifecycle tests in `crates/aenv-node/tests/integration/orchestrator.rs`
- E2E proxy suite in `scripts/tests/e2e/suites/06_proxy.sh` (header compatibility, and paused-sandbox wake-up end to end — that suite drives `AENV_PROXY_URL`, so the wake it observes is the gateway's cold path, not the proxy's)

For environment-backed integration validation, use repository-prescribed integration targets.
