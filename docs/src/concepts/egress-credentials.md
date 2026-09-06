# Egress Credentials

Code running in a sandbox often needs a credential for an outside service: an LLM API key, a
repository token, a private registry password. Putting it in `envVars` hands it to the agent's
code, every library it loads, its logs and its snapshots. Egress credentials keep the value out
of the microVM: the sandbox sends a request without it, and the credential is added where the
request leaves the platform.

## Using it

Store the value once:

```bash
curl -X POST "$AENV_URL/secrets" -H "X-API-Key: $KEY" -H 'content-type: application/json' \
  -d '{"name": "openai", "value": "sk-...", "allowedHosts": ["api.openai.com"]}'
```

`allowedHosts` is optional and pins the value to the hosts it may ever be sent to, in the same
grammar rules keys use. It is the secret's own bound, checked after the rule that named it, so a
rule that asks for this value for another host gets a `403` instead. Omit it and the rule is the
only bound. A new version of a secret replaces its pin, because the pin lives with the value.
It applies to a `value` only: a `fields` credential names its own upstream, so a pin given with
one is refused with `400`.

Then declare, per sandbox, which domains get which headers:

```json
{
  "templateID": "ubuntu",
  "network": {
    "rules": {
      "api.openai.com": [
        { "transform": { "headers": { "Authorization": "Bearer ${aenv.secrets.openai}" } } }
      ],
      "*.github.com": [
        { "transform": { "headers": { "Authorization": "token ${aenv.secrets.gh}" } } }
      ]
    }
  }
}
```

Code inside the sandbox calls `https://api.openai.com/...` as it always did, with or without an
`Authorization` header of its own. The request reaches the upstream carrying `Bearer sk-...`.
Nothing in the sandbox can read the value.

`PUT /sandboxes/{id}/network` replaces the rules of a running sandbox; connections in flight
are not cut. The rules follow the sandbox through pause and resume, and a forked child inherits
its parent's rules. `GET /secrets`, `GET /secrets/{id}`, `POST /secrets/{id}` (new version) and
`DELETE /secrets/{id}` manage the store; no response carries a value.

The shapes are E2B's `network.rules` and `/secrets`. `${e2b.secrets.NAME}` is accepted as an
alias for `${aenv.secrets.NAME}`.

## Rules

- A key is an exact DNS name or a single leading wildcard (`*.example.com`), lowercased on write.
  A wildcard matches any depth below it, never the apex; exact keys win over wildcards, the
  longest wildcard wins among wildcards, and matching lists are not merged.
- `headers` values are plain strings with markers. A header the sandbox sent under the same name
  is replaced, never appended to. Several markers may share one value (`Basic ${aenv.secrets.user}:${aenv.secrets.pass}`).
  A `${` that is not one of the two prefixes is literal text.
- Limits: 32 distinct secret names per domain, 8 KiB per header value, names match
  `^[a-zA-Z0-9_-]{1,128}$` on both sides.
- Rules do not grant network access. `allowOut`, `denyOut` and `allow_internet_access` apply to
  brokered traffic exactly as to any other: a sandbox with `allow_internet_access: false` gets
  `403` from the broker for a rule domain it could not otherwise reach.
- Creating or updating a sandbox whose rules name an unknown secret is a `400`. A deployment
  without a secrets store answers `503` for `/secrets` and for rules that name secrets.

## Explicit endpoints

`rules` cover HTTPS, where the credential is a header. A database connection is not that shape:
the credential is part of the connection itself. `network["x-aenv-endpoints"]` declares a port
inside the sandbox and the handler behind it:

```json
{
  "templateID": "ubuntu",
  "network": {
    "x-aenv-endpoints": [
      { "port": 5432, "handler": "postgres", "params": { "credential": "tenant_db" } }
    ]
  }
}
```

That credential is the other shape `/secrets` takes:

```bash
curl -X POST "$AENV_URL/secrets" -H "X-API-Key: $KEY" -H 'content-type: application/json' \
  -d '{"name": "tenant_db", "fields": {"host": "pg.internal", "port": "5432",
       "user": "app_rw", "password": "..."}}'
```

`value` and `fields` are mutually exclusive and one of them is required: a header substitution
takes the first, a handler that authenticates to the upstream itself takes the second. Field
names match `^[a-zA-Z0-9_-]{1,64}$` and there are at most 32 of them; the values are strings
(a port is `"5432"`) and a handler ignores the fields it does not know. A secret keeps the shape
it was created with: `POST /secrets/{id}` writes a new version in that same shape and refuses
the other with `400`, and a sandbox holding a grant for the name moves to the new version within
one credential-cache TTL without a new grant. Creating a sandbox whose policy reads a secret in
the other shape, a `fields` credential as a header value or a `value` as a `postgres` endpoint
credential, is refused with `400` before the sandbox is placed, rather than answered `502` on
its first brokered connection.

The sandbox connects to `169.254.0.22:5432` with a DSN whose user and password are placeholders:

```
postgresql://placeholder:placeholder@169.254.0.22:5432/tenant7?sslmode=disable
```

The broker throws that user and password away, connects to the upstream the credential names,
authenticates there with the credential's own account over SCRAM-SHA-256, and only then tells the
sandbox it is connected. Nothing in the sandbox ever holds the real credential, and editing the
DSN changes nothing the broker does.

- **Only the fields the credential returns are replaced.** A credential that returns `host`,
  `port`, `user` and `password` leaves the database name, `options` and `application_name` as the
  guest wrote them. That means a sandbox can name any database on the same instance; the
  operator's role is what decides whether it may read it, and the refusal comes from the database
  itself. It is a database-name probe, and an accepted one.
- **`sslmode` in the sandbox's DSN must be `disable`** (or absent). The hop from the guest to the
  listener is plaintext inside the sandbox's own network namespace and never leaves the host; the
  real TLS, with hostname verification, is the broker's connection to the upstream. The broker
  answers the guest's `SSLRequest` with `N`, so no CA reaches the guest for this path.
- **`interceptPort: true`** redirects every connection the guest makes to that port onto the
  listener, so any host name that resolves reaches the broker. The cost is that the sandbox can
  then no longer reach *any* other service on that port; leave it off unless the sandbox must use
  a host name you do not control.
- Port 443 is reserved for the `rules` intercept, and two endpoints may not claim the same port.
  `params` is validated against the handler, so an unknown key is a `400` rather than a setting
  that silently does nothing.
- `replication` in the startup packet is refused: it switches to a protocol this broker does not
  forward blind.
- The `tcp` handler relays bytes to the upstream its `params.upstream` names. It reads no
  credential; it is for reaching an operator-named service without giving the sandbox its address.

Whatever the handler, an upstream must fall inside the operator's per-handler allowlist
(`[handlers.<name>].allowed_cidrs` on the broker). An enabled handler with an empty allowlist
reaches nothing: the endpoint declaration names the upstream, so the operator names where
declarations may point.

Two lists have to agree, and they live in different files. `[handlers.<name>].allowed_cidrs`
is what the broker's own code enforces; `aenv-egress-networkpolicy.yaml` is what the cluster
enforces, and it ships permitting only public port 443. A handler enabled without a matching
egress rule there fails every connection with `upstream_unreachable`, and the policy is the one
that holds when the config is wrong.

That allowlist reaches a private range when it names one, and it has to — the databases this
exists for sit on private addresses inside a VPC. The upstream is the operator's choice and the
guest never addressed it, so neither the broker's built-in private ranges nor the sandbox's own
`allowOut` / `denyOut` / `allow_internet_access` bounds it; requiring the sandbox's policy to
name the database would mean publishing its address into the sandbox's own configuration, which
is the address the arrangement exists to keep out of it. Three things stay out of reach whatever
an allowlist says: the broker's own host, the link-local range that carries cloud metadata, and
everything the operator put in `[upstream].denied_cidrs` — which is where the cluster's own
Service and Pod CIDRs go.

`rules` is the other class: there the guest chose the destination itself, so the built-in ranges
and the sandbox's own policy apply to it exactly as before.

## What happens on the wire

A sandbox with rules gets a listener inside its own network namespace and a DNAT of its
outbound port 443 onto that listener; UDP 443 is rejected so HTTP/3 falls back to TCP. Every
connection accepted there is the sandbox's by construction, so the node attaches the sandbox's
identity and relays the bytes over a Unix socket to the broker on this same machine, which admits
the connection only because its peer uid is the node's. The broker reads the ClientHello:

- The server name matches a rule: the broker answers with a leaf certificate signed by the
  node's intermediate, terminates TLS, replaces the headers, and opens its own TLS connection to
  the real upstream, verified against the system trust store.
- The name matches nothing, or there is no name: the bytes are relayed to the original
  destination untouched, after the same policy check. This is the path `pip`, `npm` and
  `git clone` take.

The guest trusts the root because every envd `init` for a sandbox with rules carries it as
`caBundle`, along with `SSL_CERT_FILE`, `REQUESTS_CA_BUNDLE`, `CURL_CA_BUNDLE`,
`NODE_EXTRA_CA_CERTS` and `GIT_SSL_CAINFO` for runtimes that do not read the system store (a
user-provided value of the same name wins). A sandbox without rules gets an empty `caBundle`.
Clients that pin certificates cannot be brokered; Java keystores need a `RUN` step in the
template.

Failures the sandbox can observe, all with a fixed `x-aenv-egress-reason` header and no names or
values in the body:

| Symptom | Reason |
|---|---|
| `403` | the sandbox policy denies the destination, the secret is not granted to this sandbox, its value is gone, or the secret's `allowedHosts` does not cover this name (`secret_host_not_allowed`) |
| `405` | `CONNECT` or `TRACE` to an intercepted name; neither is brokered (`connect_unsupported`, `method-not-brokered`) |
| `502` | the upstream is unreachable or unresolvable, its TLS failed, or the secrets store is down |
| `505` | HTTP/2 to an intercepted name; use HTTP/1.1 |
| TLS fails on a rule domain, the broker logging `no-intermediate` | this node's broker has not been issued its intermediate. It cannot mint a leaf, and it closes rather than relaying the request to the real upstream without the credentials the rule exists to add |
| connection closed at once | the broker is unreachable, or the sandbox exceeded its per-sandbox connection budget |
| create answers `503` | no node reports `local_ok` — every broker is down, or none has been rolled onto `mode = "local"` yet |
| create fails with a CA probe error | the guest's envd does not support `caBundle` |

### What a guest sees that it did not before

The chain a rule domain presents is **two certificates**, not one: the leaf, and the node's
intermediate. The intermediate's issuer CN reads `AgentENV Egress Node <node>`, so a guest that
prints the chain sees which machine served it.

What does not change is the anchor. A guest trusts the **root**, and only the root. That is what
lets a sandbox pause on one node and resume on another: a process that loaded its trust store at
startup — Node.js, Go, the JVM all do — carries that store across the move, and a guest pinned to
the node it started on would keep working until it moved and then fail with nothing to say why.

A leaf also never outlives its issuer: it expires at its own TTL or an hour before the
intermediate does, whichever comes first.

## Authorization

The api half writes secret values to the store and never reads them back. Before a sandbox with
rules starts, it records a grant for `(sandbox, execution, names)`; the grant is revoked when the
sandbox is deleted or paused, and a resume or fork gets a new grant for its new execution. The
broker serves a value only under a matching grant, so what a sandbox can obtain through a healthy
broker is exactly its own grant and nothing else — that is the axis the grant bounds. The node
records nothing: it starts what the api half dispatched and holds no store to refuse or consult.

Two consequences follow from the credential being the operator's existing per-database account
rather than one minted per sandbox: every sandbox is the same role to the database, so its audit
log cannot tell them apart, and a revocation does not cut connections already open — it stops the
next one, no sooner than the broker's credential cache TTL. **The proxy removes the credential
from the sandbox; it does not by itself give each sandbox a database identity.**


### What a grant does not bound

**The isolation axis is the control-plane credential.** A secret name is unique across the
deployment, and any holder of an API key may reference any name that exists: nothing checks that
this sandbox is entitled to `tenant_db_ws99`. What `(sandbox, execution)` bounds is which names
*this run* may read, not who a secret belongs to.

Name secrets after where they came from — `tenant_db_ws42`, `app_7f3c9e21` — and keep the
identifier in the name. Nothing validates the prefix; its whole job is to make a sandbox that
references somebody else's credential visible in an audit, which a bare `db` never would.

A sandbox may reference up to 16 brokered endpoints, so one sandbox can front several such
credentials, each on its own port. Rotating one is a new version under the same name and needs no
new grant: the broker reads the current version, and every sandbox holding a grant for that name
moves to it within one credential-cache TTL.

The create and network-update paths refuse a policy naming a secret that does not exist. A
resume does not — the policy was accepted when the sandbox was created, and refusing to wake a
sandbox because a secret was deleted is worse than waking it degraded. The wake logs a warning
naming the sandbox and the missing names; the guest sees the same synthetic `403` any denied
credential produces.

### Where the check happens

The api half's own. Values live in `secret_values` in the same PostgreSQL that holds
`secret_refs`, encrypted with AES-256-GCM under a master key `aenv-api` reads from a file and the
database never sees; grants are rows in `secret_grants`. The broker holds a bearer for
`POST /internal/credentials/resolve` and asks about one `(sandbox, execution, name)` at a time —
it holds no credential that reads a value, so a compromised broker reads nothing that is not
granted to some live sandbox right now. Everything a grant does not cover is one `404` that says
nothing about what exists.

That endpoint is on the api half's own port, beside the REST surface, and no API key reaches it:
the bearer is the only thing in front of it, so it belongs behind the same boundary that port
already has. The shipped manifests give `agentenv-api` a ClusterIP Service and no ingress.

What it costs is on the other side: `aenv-api` can open every stored value. That is the price of
needing no credential store beside AgentENV, and it is the reason the master key is a mounted
file rather than a column, an environment variable or a `pgcrypto` argument that would reach the
query log. Whoever holds a dump of `secret_values` does not hold the key, and a database backup
does not recover it.

## Deployment

Three parts, configured in [`[egress_broker]`](../configuration/reference.md#egress_broker),
[`[egress_ca]`](../configuration/reference.md#egress_broker) and
[`[secrets]`](../configuration/reference.md#secrets):

- `aenv-egress` (`deploy/k8s/base/aenv-egress-daemonset.yaml`, image
  `deploy/docker/Dockerfile.aenv-egress`): the broker, **one per node**, reading
  `deploy/k8s/base/config/aenv-egress.toml`. It binds a Unix socket in `/run/aenv-egress`, a
  hostPath it shares with `agentenv-node` and nothing else, and admits only connections whose
  peer uid is the node's. It holds no CA key of its own: it asks the api half for a seven-day
  intermediate against its own projected ServiceAccount token.
  `aenv-egress-networkpolicy.yaml` lets nothing in but Prometheus — there is no network path to
  the broker at all — and lets the api half, DNS and port 443 of public addresses out.
- Nodes: `[egress_broker].mode = "local"`, `socket_path = "/run/aenv-egress/broker.sock"`, and
  `guest_ca_cert_path` pointing at the **root** guests trust. The node creates the socket
  directory at startup and hands it to the broker's group. A node reports its broker state in
  every heartbeat, and the api half places a sandbox with rules only on a node reporting
  `local_ok`; when none does the create answers `503`.
- The api half: `[secrets].backend = "postgres"` with `[secrets.pg].key_file` (base64 of 32
  bytes, the `agentenv-secrets-key` Secret) and `[secrets.pg].resolver_token_file`, plus
  `[egress_ca].root_cert_path` / `root_key_path` (the `egress-ca` Secret) — **the only workload
  that mounts the root's key**. PostgreSQL gains `secret_values` and `secret_grants` beside
  `secret_refs`. The broker points `[resolver].url` and `[ca].issuer_url` at
  `http://agentenv-api:8000/internal`, and its NetworkPolicy has to admit that port. Losing the
  master key loses every stored value; it is not recoverable from a database backup, which is the
  point.

Both internal endpoints check the broker's projected token through Kubernetes: `TokenReview` says
whose it is, the Pod says which machine it runs on, and a resolve is then answered only for
sandboxes bound to that machine. That needs the `agentenv-api-token-review` ClusterRole, and it is
why `[cluster].node_discovery_mode = "static"` closes both endpoints with a `503` — there is no
Kubernetes to ask. `[secrets].legacy_bearer_until` keeps the shared bearer working beside the
token during a migration; while it is open, a caller presenting it is scoped to no node.

`AENV_EGRESS_BROKER_MODE` and `AENV_SECRETS_BACKEND` are both read once at process startup, so
editing either ConfigMap changes nothing until the process that reads it restarts. 🔴 Rolling
`ds/agentenv-node` destroys every sandbox on every node — the Firecracker processes are its
children. Rolling `ds/aenv-egress` does not, which is the whole reason the broker is its own
workload.

`mode = "embedded"` runs the broker core inside `aenv-node` for a single static node: it has no
credential source and no TLS stack, so it carries the one handler that needs neither — `echo`,
which answers the sandbox's identity — and proves the intercept and identity path without
brokering HTTPS or Postgres.

### When a rule domain stops working

In this order, because each step rules out everything below it:

1. **`GET /nodes`** — does the sandbox's node report `egressBroker: "local_ok"`? `disabled` means
   its ConfigMap was never flipped; `local_unreachable` means the socket is not being read.
2. **`kubectl -n <ns> get pods -l app.kubernetes.io/name=aenv-egress -o wide`** — is there a
   broker Pod on that node, and is it `Ready`? Its readiness probe connects to its own socket, so
   `Ready` means it is serving and not merely up.
3. **The broker's log on that node** — `no-intermediate` means it never got a signing key: check
   that the api half has `[egress_ca]` set, that the `agentenv-api-token-review` ClusterRole
   exists, and that discovery is not `static`.
4. **`egress_intermediate_expires_seconds`** on that broker — zero means it holds none; a small
   number means renewal has been failing for days and the log says why.
5. **The audit trail** (`egress.audit` on the broker's stdout) — a `security_event` names what was
   refused and why; a request line with a `502` and no security event is an upstream problem, not
   a policy one.
6. **Only then the sandbox's own policy**: `allowOut`, `denyOut`, `allow_internet_access`, and
   whether the secret the rule names still exists and is granted to this run.

Metrics on the broker's `:9103`: `egress_conns_total{handler,outcome}`, `egress_active_conns`,
`egress_policy_denied_total{reason}`, `egress_intercept_no_sni_total`,
`egress_tls_leaf_minted_total`, `egress_peer_rejected_total` (a connection whose peer was not the
node's uid) and `egress_intermediate_expires_seconds` (how long this node's intermediate is still
good for; zero means the broker holds none and rules domains are closed).
`egress_sandbox_conns_total{sandbox}` is exported only while the broker runs at debug level — one
series per sandbox is unbounded cardinality on a node that runs thousands a day.
`egress_replay_rejected_total` is gone with the replay cache. On the api half's own `/metrics`,
`agentenv_node_egress_broker{node,state}` carries what each node last reported.

## The audit trail

The broker writes one JSON line per brokered request to stdout, under the `egress.audit` tracing
target, and two other kinds of line on the same target: `security_event` (a policy denial, a
credential refused, a name the rules do not cover, a broker holding no intermediate, and the
`credential_injected` line naming which headers a request had set) and `tls_handshake` (a
handshake that did not complete, on either side).

A request line carries `ts`, `node_id`, `sandbox_id`, `execution_id`, `dst_ip`, `dst_port`,
`scheme`, `host`, `method`, `path`, `status`, `bytes_in`, `bytes_out`, `latency_ms`,
`tls_version`, `cipher`, `upstream_addr`, `rule` and `injected_headers`.

What it never carries is a value. `injected_headers` is a list of **names**; the query string is
not recorded at all, because that is where an API that takes credentials in the URL puts them; and
`path` is truncated at 1 KiB. `tls_version` and `cipher` come back empty today: the TLS wrapper the
guest side terminates with exposes neither, and filling them is a change to how the broker accepts
rather than to how it audits.

`[audit].level = "none"` turns the whole trail off, security events included — a deployment that
sets it is saying it collects them somewhere else.

## Not covered

Per-grant scoped broker tokens and multi-tenant ownership are later stages. ECH hides the server name and leaves
only the passthrough path; the `egress_intercept_no_sni_total` counter shows how often that
happens. See `docs/proposals/2026-09-03-sandbox-egress-credential-brokering.md` for the design
and its review record.
