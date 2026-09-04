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
identity and relays the bytes to the broker over TLS. The broker reads the ClientHello:

- The server name matches a rule: the broker answers with a leaf certificate signed by the
  cluster CA, terminates TLS, replaces the headers, and opens its own TLS connection to the real
  upstream, verified against the system trust store.
- The name matches nothing, or there is no name: the bytes are relayed to the original
  destination untouched, after the same policy check. This is the path `pip`, `npm` and
  `git clone` take.

The guest trusts the cluster CA because every envd `init` for a sandbox with rules carries it as
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
| `502` | the upstream is unreachable or unresolvable, its TLS failed, or the secrets store is down |
| `505` | HTTP/2 to an intercepted name; use HTTP/1.1 |
| connection closed at once | the broker is unreachable, or the sandbox exceeded its connection budget |
| create fails with a CA probe error | the guest's envd does not support `caBundle` |

## Authorization

The api half writes secret values to the store and never reads them back. Before a sandbox with
rules starts, it records a grant for `(sandbox, execution, names)`; the grant is revoked when the
sandbox is deleted or paused, and a resume or fork gets a new grant for its new execution. The
broker serves a value only under a matching grant, so what a sandbox can obtain through a healthy
broker is exactly its own grant and nothing else — that is the axis the grant bounds. The node
records nothing: it starts what the api half dispatched and holds no store to refuse or consult.

With `[secrets].backend = "external_resolver"` the credential never enters AgentENV at all: the
api half posts the same `(sandbox, execution, names)` grant to a service the operator runs, and
the broker asks that service for the value. Two consequences follow from the credential being
the operator's existing per-database account rather than one minted per sandbox: every sandbox is
the same role to the database, so its audit log cannot tell them apart, and a revocation does not
cut connections already open — it stops the next one, no sooner than the broker's credential
cache TTL. **The proxy removes the credential from the sandbox; it does not by itself give each
sandbox a database identity.**

Which side enforces the grant depends on the backend, and that is the difference between the two.

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

With `vault`, the broker enforces it in its own code and the token it holds reads every value
under the mount: KV v2 policy has no way to say "read `secrets/X` only when `grants/E` names it".
What bounds a compromised broker is everything around the process — a token whose policy is
`read` and cannot write itself a grant, a NetworkPolicy that admits only nodes and allows only
Vault, DNS and port 443 out, and a non-root Pod with a read-only root filesystem. A per-grant
Vault token would need a policy minted per grant, which means giving the api half write access to
`sys/policies/acl/*` — the ability to write itself a policy for anything in Vault. That trades a
larger exposure for a smaller one and is why it is not the fix.

With `external_resolver` the check is the store's. The broker holds a token that lets it *ask*
about one `(sandbox, execution, name)` at a time, not one that reads values; a resolver that has
no grant for the triple answers `403`, and a compromised broker reads nothing that is not
currently granted to some sandbox. That is the confinement a scoped token was meant to buy,
reached from the other side.

## Deployment

Three parts, configured in [`[egress_broker]`](../configuration/reference.md#egress_broker) and
[`[secrets]`](../configuration/reference.md#secrets):

- `aenv-egress` (`deploy/k8s/base/aenv-egress-deployment.yaml`, image
  `deploy/docker/Dockerfile.aenv-egress`): the broker, reading
  `deploy/k8s/base/config/aenv-egress.toml`. It needs the `egress-ca`, `egress-server`,
  `egress-hmac` and `egress-vault` Secrets; `aenv-egress-secrets.example.yaml` says how to mint
  them. `aenv-egress-networkpolicy.yaml` lets only nodes in and only Vault, DNS and port 443 of
  public addresses out.
- Nodes: `[egress_broker].mode = "remote"`, `endpoint = "aenv-egress:8443"`, the CA certificate
  and the HMAC key. Two CAs where the operator has split them: `ca_cert_path` verifies the
  broker's server certificate and `guest_ca_cert_path` is what guests trust for intercepted
  names, which is what lets the second one carry name constraints without invalidating the
  broker's own `*.svc` certificate. A node reports its broker state in every heartbeat, and the api half places a
  sandbox with rules only on a node that reports `remote_ok`; when none does the create answers
  `503`.
- The api half: `[secrets].backend = "vault"` with the Vault address and a token that can write
  `<mount>/data/secrets/*` and `<mount>/data/grants/*` and delete the matching `<mount>/metadata/*`
  paths — the `secrets-vault-writer` Secret, a different credential from the broker's read-only
  `egress-vault`. PostgreSQL gets a `secret_refs` table holding names and versions only.

`AENV_EGRESS_BROKER_MODE` and `AENV_SECRETS_BACKEND` are both read once at process startup, so
editing either ConfigMap changes nothing until the process that reads it restarts:
`kubectl rollout restart ds/agentenv-node deploy/agentenv-api`, or `make k8s-redeploy` for all
four workloads.

`mode = "embedded"` runs the broker core inside `aenv-node` for a single static node: it has no
credential source and no TLS stack, so it carries the two handlers that need neither — `echo`,
which answers the sandbox's identity, and `tcp` — and proves the intercept and identity path
(`docker-compose.yml` uses it) without brokering HTTPS or Postgres.

Metrics on the broker's `:9103`: `egress_conns_total{handler,outcome}`, `egress_active_conns`,
`egress_policy_denied_total{reason}`, `egress_intercept_no_sni_total`,
`egress_tls_leaf_minted_total`, `egress_replay_rejected_total`.

## Not covered

Per-grant scoped broker tokens and multi-tenant ownership are later stages. ECH hides the server name and leaves
only the passthrough path; the `egress_intercept_no_sni_total` counter shows how often that
happens. See `docs/proposals/2026-09-03-sandbox-egress-credential-brokering.md` for the design
and its review record.
