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
  -d '{"name": "openai", "value": "sk-..."}'
```

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
| `403` | the sandbox policy denies the destination, the secret is not granted to this sandbox, or its value is gone |
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

The broker process is trusted rather than confined by that check. It enforces the grant in its own
code, and the Vault token it holds reads every value under the mount: KV v2 policy has no way to
say "read `secrets/X` only when `grants/E` names it". What bounds a compromised broker is
everything around the process — a token whose policy is `read` and cannot write itself a grant, a
NetworkPolicy that admits only nodes and allows only Vault, DNS and port 443 out, and a non-root
Pod with a read-only root filesystem. Confining the process itself needs a per-grant scoped or
response-wrapped token issued at grant time; that is a later stage.

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
  and the HMAC key. A node reports its broker state in every heartbeat, and the api half places a
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

`mode = "embedded"` runs the broker core inside `aenv-node` for a single static node: it has the
`tcp` identity-echo handler and no TLS, so it proves the intercept and identity path
(`docker-compose.yml` uses it) but does not broker HTTPS.

Metrics on the broker's `:9103`: `egress_conns_total{handler,outcome}`, `egress_active_conns`,
`egress_policy_denied_total{reason}`, `egress_intercept_no_sni_total`,
`egress_tls_leaf_minted_total`, `egress_replay_rejected_total`.

## Not covered

Explicit local endpoints, non-HTTP protocols, external credential resolvers, per-secret upstream
restrictions, per-grant scoped broker tokens and multi-tenant ownership are later stages. ECH hides the server name and leaves
only the passthrough path; the `egress_intercept_no_sni_total` counter shows how often that
happens. See `docs/proposals/2026-09-03-sandbox-egress-credential-brokering.md` for the design
and its review record.
