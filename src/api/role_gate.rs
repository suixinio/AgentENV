//! Which of the generated routes a process serves, given which half it is.
//!
//! `aenv-node` runs the half that owns VMs, not the half that owns sandboxes.
//! The user-facing REST surface — the `sandboxes`, `snapshots` and `templates`
//! groups — belongs to the API half, and a node that keeps answering it while
//! the API half believes it owns those sandboxes is two ledgers for one set of
//! machines. This layer is what stops that.
//!
//! # Why a layer and not a shorter route list
//!
//! Two reasons, and a third that is really the first one again.
//!
//! 1. The routes are one generated function
//!    (`agentenv_http_server::server::new`), and generated code is machine
//!    -managed. Serving a subset means editing the generator's template, which
//!    is a codegen debt taken on for a flag.
//! 2. 🔴 **Not attaching a route is not the same as refusing it.** The node's
//!    router is `generated.merge(proxy::router(..))`, and the data plane
//!    carries a fallback for host-routed sandbox traffic. A `/sandboxes` that
//!    was never attached does not 404 — it lands in the data-plane fallback and
//!    is answered as if it were sandbox traffic. "The node refuses user REST"
//!    would then fail in the single hardest way to diagnose.
//! 3. The two halves share one crate and one generated router, so "the node
//!    serves less" has to be expressible without taking anything away from what
//!    `aenv-api` serves. A layer satisfies that by construction: nothing is
//!    removed, and `aenv-api` does not attach the layer at all.
//!
//! # Where it is attached, and in which order
//!
//! On the **generated router**, before `proxy::router(..)` is merged in, for
//! the same structural reason [`super::control_plane_gate`] documents at
//! length: `Router::merge` keeps each router's own layers, so the data plane is
//! outside this gate as a fact about the shape of `super::server::assemble`,
//! not as a fact about this function remembering to check for `/proxy`.
//!
//! 🔴 **Outside the control-plane gate**, i.e. applied after it, so it runs
//! first. That ordering is the difference between a node answering
//! `POST /sandboxes` with 404 and answering it with 403. A 403 says "this
//! exists, you are not allowed" and invites a retry with better credentials; a
//! 404 says nothing at all. Since the whole point is that this route is not
//! part of a node's surface, the role question has to be settled before the
//! credential question is asked. Pinned by
//! `the_role_gate_answers_before_the_control_plane_gate_does`.
//!
//! # What it does not do
//!
//! It does not filter the data plane, the metrics endpoint, or anything a node
//! does on its own initiative. It is a gate on inbound HTTP and nothing else.
//!
//! 🔴 It also does not consume the `control_plane_config` ownership marker, and
//! must not be changed to. This layer answers "does this process serve this
//! route", which is a question about the process, not about any sandbox.

use axum::{
    body::Body,
    extract::Request,
    http::{Method, Response, StatusCode},
    middleware::{self, Next},
    response::IntoResponse,
    Router,
};
use tracing::debug;

/// The `role` label this gate's metrics carry.
///
/// 🔴 A constant, and the metric names around it are unchanged, on purpose.
/// The gate is only ever attached in `aenv-node` — [`attach`] returns
/// `generated` untouched otherwise — so the label has exactly one value now
/// that `--role` is gone. Renaming `agentenv_api_role_gate_*` or dropping the
/// label would break every dashboard and alert already reading them, which is a
/// worse outcome than the word "role" outliving the flag.
const GATE_ROLE_LABEL: &str = "node";

/// Attaches the gate to `generated` for the half that serves less than the full
/// generated surface, and returns it untouched for the half that serves all of
/// it.
///
/// 🔴 Untouched, not "attached with an allow-everything policy". `aenv-api`
/// serves every generated route, and an extra layer in its request path buys
/// nothing but a per-request branch that can only answer one way.
pub fn attach(generated: Router, serves_user_facing_rest: bool) -> Router {
    // Published before any request arrives, by both halves. "Is this process
    // refusing user REST" has to be answerable from a scrape of a node that has
    // had no traffic — otherwise a node whose gate never got attached and a
    // node nobody has called look identical.
    metrics::gauge!("agentenv_api_role_gate_enabled").set(if serves_user_facing_rest {
        0.0
    } else {
        1.0
    });

    if serves_user_facing_rest {
        return generated;
    }

    generated.layer(middleware::from_fn(refuse_outside_role))
}

/// What the gate did with one request. A closed set: the label goes on a metric
/// and a metric label with unbounded values is a memory leak.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RoleDecision {
    /// A route this half serves. Handed on untouched.
    Served,
    /// A route that belongs to the other half of the split.
    Refused,
}

impl RoleDecision {
    fn label(self) -> &'static str {
        match self {
            Self::Served => "served",
            Self::Refused => "refused",
        }
    }
}

/// The generated routes an `aenv-node` process still answers.
///
/// 🔴 Keep this list this short, and keep the reason for each entry attached to
/// it. Every addition is a piece of the user-facing surface coming back to a
/// node, and the only defence against that happening by accident is that the
/// list is small enough to read in one go.
///
/// * `GET /health` — kubelet polls it three ways (startup, readiness,
///   liveness) and does not go through the gateway. Refusing it means the pod
///   never becomes ready, which looks like the node being broken.
/// * `GET /nodes` — the node describing itself.
/// * `GET`/`POST` `/nodes/{nodeID}` — 🔴 the preStop hook posts to it to take
///   the node out of rotation, and reads it to decide when the node has
///   drained. Both halves of a node's own shutdown run through this one route.
///
/// Every other generated route — the `sandboxes`, `snapshots` and `templates`
/// groups — is answered with 404.
fn node_serves(method: &Method, path: &str) -> bool {
    match path {
        // Method-blind on purpose: kubelet's probes are GETs, and a
        // hypothetical other method on `/health` is the generated router's 405
        // to give, exactly as it is on the half that attaches no gate.
        "/health" => true,
        "/nodes" => method == Method::GET,
        _ => node_detail_id(path).is_some() && (method == Method::GET || method == Method::POST),
    }
}

/// The `{nodeID}` of a `/nodes/{nodeID}` path, or `None` when `path` is not
/// one.
///
/// 🔴 Exactly one segment after `/nodes/`, and it may not be empty. A prefix
/// test would let a future `/nodes/{nodeID}/anything` through without anyone
/// deciding that it should be served, and the direction that mistake runs in is
/// "a node serves more than it was meant to".
fn node_detail_id(path: &str) -> Option<&str> {
    let rest = path.strip_prefix("/nodes/")?;
    if rest.is_empty() || rest.contains('/') {
        return None;
    }
    Some(rest)
}

/// Refuses generated routes that belong to the other half of the split.
///
/// 🔴 No state. This function is only ever reached through the layer [`attach`]
/// installs, and `attach` installs it only on the half that serves less than
/// the full surface — so "does this half serve user REST" is already answered
/// `no` by the time a request gets here. It used to be re-asked from a
/// `State` on every request, which meant one arm of that branch was
/// unreachable in every process that had the layer at all.
async fn refuse_outside_role(request: Request, next: Next) -> Response<Body> {
    let decision = if node_serves(request.method(), request.uri().path()) {
        RoleDecision::Served
    } else {
        RoleDecision::Refused
    };

    metrics::counter!(
        "agentenv_api_role_gate_total",
        "role" => GATE_ROLE_LABEL,
        "decision" => decision.label(),
    )
    .increment(1);

    if decision == RoleDecision::Served {
        return next.run(request).await;
    }

    // `debug`, not `warn`. On a healthy cluster nothing calls a node's user
    // REST, but the thing that does call it during a migration is a client that
    // has not been repointed yet — a routine, expected, high-volume mistake,
    // and one the counter above already surfaces without a log line per
    // request.
    debug!(
        role = GATE_ROLE_LABEL,
        method = %request.method(),
        path = %request.uri().path(),
        "refusing a route this half does not serve"
    );

    not_found(request.method(), request.uri().path())
}

/// The answer a node gives for a route it does not serve.
///
/// 🔴 404, not 403 and not 405: a 403 advertises that the route exists and
/// invites a retry with credentials that would not help, and a 405 advertises
/// which methods it has.
///
/// 🔴 And **byte-for-byte what an unattached route already produces**. A path
/// that no generated route claims falls through to `proxy::proxy_via_fallback`,
/// which answers 404 with this exact envelope, for a reason that applies here
/// unchanged: it returns "the API error envelope so JSON clients surface 'route
/// not found' instead of failing to parse an empty 404 body". An empty body
/// here would break those clients *and* leave a node's refusal distinguishable
/// from a route that was never compiled in — which is the one property the
/// choice of 404 was for.
/// `a_refused_route_is_indistinguishable_from_one_that_never_existed` runs that
/// comparison against the real router.
fn not_found(method: &Method, path: &str) -> Response<Body> {
    let error =
        agentenv_http_server::models::Error::new(404, format!("route not found: {method} {path}"));

    (StatusCode::NOT_FOUND, axum::Json(error)).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The generated router's source, read at compile time so the test walks
    /// the routes that actually exist rather than a copy of them.
    const GENERATED_SERVER: &str = include_str!("generated/src/server/mod.rs");

    /// Every path passed to `.route(..)` in the generated router.
    fn generated_route_paths() -> Vec<String> {
        GENERATED_SERVER
            .split(".route(")
            .skip(1)
            .filter_map(|tail| {
                let tail = tail.trim_start();
                let quoted = tail.strip_prefix('"')?;
                let (path, _) = quoted.split_once('"')?;
                path.starts_with('/').then(|| path.to_string())
            })
            .collect()
    }

    /// Turns `/sandboxes/{sandbox_id}/pause` into a path a request could
    /// actually carry.
    fn concrete(path: &str) -> String {
        let mut out = String::with_capacity(path.len());
        let mut rest = path;
        while let Some((before, after)) = rest.split_once('{') {
            out.push_str(before);
            let (_, after) = after.split_once('}').expect("a route pattern is balanced");
            out.push_str("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
            rest = after;
        }
        out.push_str(rest);
        out
    }

    // ── The allowlist, both faces ───────────────────────────────────────────

    /// 🔴 T-RG-1. The control face, and the whole point of this test.
    ///
    /// A gate that refuses everything passes any test that only checks that
    /// user REST is refused — and it also stops kubelet's probes, stops the
    /// preStop hook draining the node, and makes every node in the fleet look
    /// unhealthy the moment the flag is turned on. So the two faces are
    /// asserted together, in one test, against the same gate.
    #[test]
    fn a_node_serves_its_own_routes_and_refuses_the_users() {
        // Must still be reachable.
        assert!(node_serves(&Method::GET, "/health"));
        assert!(node_serves(&Method::GET, "/nodes"));
        assert!(node_serves(&Method::GET, "/nodes/ip-10-0-1-7"));
        assert!(
            node_serves(&Method::POST, "/nodes/ip-10-0-1-7"),
            "the preStop hook posts to this to drain the node"
        );

        // Must not be.
        assert!(!node_serves(&Method::POST, "/sandboxes"));
        assert!(!node_serves(&Method::GET, "/sandboxes"));
        assert!(!node_serves(&Method::GET, "/v2/sandboxes"));
        assert!(!node_serves(&Method::POST, "/sandboxes-cold"));
        assert!(!node_serves(&Method::POST, "/sandboxes/sbx-1/pause"));
        assert!(!node_serves(&Method::DELETE, "/sandboxes/sbx-1"));
        assert!(!node_serves(&Method::GET, "/snapshots"));
        assert!(!node_serves(&Method::DELETE, "/snapshots/snap-1"));
        assert!(!node_serves(&Method::GET, "/templates"));
        assert!(!node_serves(&Method::POST, "/v3/templates"));
    }

    /// 🔴 T-RG-3. The allowlist and the generator cannot drift apart in
    /// silence.
    ///
    /// Reads the route table out of the generated source and partitions it. A
    /// route added by a future `make agentenv-server` lands on the refused side
    /// by default — the fail-closed direction — and this test is what makes
    /// somebody look at it and decide, rather than discovering it from a node
    /// that quietly stopped answering something.
    #[test]
    fn every_generated_route_is_either_named_by_the_allowlist_or_refused() {
        let paths = generated_route_paths();
        assert_eq!(
            paths.len(),
            25,
            "the generated route table changed. Partition the new route below and \
             update this count; the routes are {paths:?}"
        );

        let mut served = Vec::new();
        let mut refused = Vec::new();
        for path in &paths {
            let concrete = concrete(path);
            // A route is "served by a node" if the node answers it under any
            // method the generated table could carry.
            let any = [Method::GET, Method::POST, Method::DELETE, Method::PUT]
                .iter()
                .any(|method| node_serves(method, &concrete));
            if any {
                served.push(path.as_str());
            } else {
                refused.push(path.as_str());
            }
        }

        assert_eq!(
            served,
            vec!["/health", "/nodes", "/nodes/{node_id}"],
            "a aenv-node process serves exactly the three routes §7.2 names"
        );
        // ...and the rest is the user-facing surface, all of it.
        assert_eq!(refused.len(), 22);
        for group in ["/sandboxes", "/snapshots", "/templates", "/v2/", "/v3/"] {
            assert!(
                refused.iter().any(|path| path.starts_with(group)),
                "the {group} group must be on the refused side"
            );
        }
    }

    /// 🔴 T-RG-4. `/nodes/{id}` is one segment, and only one.
    ///
    /// The mistake this guards is writing the match as a prefix test, which
    /// costs nothing today (no such route exists) and hands a node every future
    /// `/nodes/{id}/…` route without anyone choosing to.
    #[test]
    fn only_a_single_segment_follows_the_nodes_prefix() {
        assert_eq!(node_detail_id("/nodes/ip-10-0-1-7"), Some("ip-10-0-1-7"));
        assert_eq!(node_detail_id("/nodes/"), None);
        assert_eq!(node_detail_id("/nodes/a/b"), None);
        assert_eq!(node_detail_id("/nodes/a/"), None);
        assert_eq!(node_detail_id("/nodes"), None);
        assert_eq!(node_detail_id("/sandboxes/a"), None);

        assert!(!node_serves(&Method::GET, "/nodes/a/sandboxes"));
    }

    /// T-RG-5. The methods `/nodes` and `/nodes/{id}` are served under are the
    /// ones §7.2 names, and not a wider set.
    #[test]
    fn the_node_routes_are_served_under_the_methods_they_were_granted() {
        assert!(node_serves(&Method::GET, "/nodes"));
        assert!(!node_serves(&Method::POST, "/nodes"));
        assert!(!node_serves(&Method::DELETE, "/nodes"));

        assert!(node_serves(&Method::GET, "/nodes/n1"));
        assert!(node_serves(&Method::POST, "/nodes/n1"));
        assert!(!node_serves(&Method::DELETE, "/nodes/n1"));
        assert!(!node_serves(&Method::PUT, "/nodes/n1"));
    }

    // ── The layer, as it is actually assembled ──────────────────────────────

    use axum::http::Request as HttpRequest;
    use axum::routing::{get, post};
    use tower::ServiceExt;

    /// A stand-in for the generated router, carrying the paths that matter on
    /// both sides of the allowlist.
    fn stand_in_generated() -> Router {
        Router::new()
            .route("/health", get(|| async { "ok" }))
            .route("/nodes", get(|| async { "described" }))
            .route(
                "/nodes/{node_id}",
                get(|| async { "described" }).post(|| async { "drained" }),
            )
            .route("/sandboxes", get(|| async { "listed" }))
            .route("/sandboxes", post(|| async { "created" }))
            .route("/v2/sandboxes", get(|| async { "listed" }))
    }

    fn stand_in_data_plane() -> Router {
        Router::new()
            .route("/proxy/{*rest}", get(|| async { "proxied" }))
            .fallback(get(|| async { "fallback" }))
    }

    async fn status(router: Router, method: Method, path: &str) -> StatusCode {
        router
            .oneshot(
                HttpRequest::builder()
                    .method(method)
                    .uri(path)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
            .status()
    }

    /// 🔴 T-RG-6. The layer refuses with a 404, and it has resolution.
    ///
    /// The first assertion shows an allowed route answering, so the refusals
    /// below mean "this route", not "this router is broken".
    #[tokio::test]
    async fn the_attached_layer_refuses_user_rest_and_serves_the_nodes_own_routes() {
        let node = || attach(stand_in_generated(), false);

        assert_eq!(status(node(), Method::GET, "/health").await, StatusCode::OK);
        assert_eq!(status(node(), Method::GET, "/nodes").await, StatusCode::OK);
        assert_eq!(
            status(node(), Method::POST, "/nodes/n1").await,
            StatusCode::OK
        );

        for (method, path) in [
            (Method::POST, "/sandboxes"),
            (Method::GET, "/sandboxes"),
            (Method::GET, "/v2/sandboxes"),
        ] {
            assert_eq!(
                status(node(), method.clone(), path).await,
                StatusCode::NOT_FOUND,
                "{method} {path} must be refused as if it did not exist"
            );
        }
    }

    /// 🔴 T-RG-7. A refusal says exactly what an absent route says.
    ///
    /// "A node should look like a process this route was never compiled into"
    /// is a claim about the whole response, not only its status. An unattached
    /// path on this server does not produce a bare 404 — it falls through to
    /// the data plane's fallback, which answers with the API error envelope —
    /// so a refusal with an empty body would be distinguishable from an absent
    /// route, and would break the JSON clients that envelope exists for.
    /// `a_refused_route_is_indistinguishable_from_one_that_never_existed` runs
    /// the comparison against the real router; this pins the shape.
    #[tokio::test]
    async fn a_refusal_says_only_what_an_absent_route_says() {
        let response = attach(stand_in_generated(), false)
            .oneshot(
                HttpRequest::builder()
                    .method(Method::POST)
                    .uri("/sandboxes")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok()),
            Some("application/json"),
            "the refusal must carry the envelope a JSON client can parse"
        );
        let body = axum::body::to_bytes(response.into_body(), 256)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["code"], 404);
        assert_eq!(body["message"], "route not found: POST /sandboxes");
    }

    /// 🔴 T-RG-8. The half that serves everything gets no layer at all.
    ///
    /// Not "a layer that allows everything": `aenv-api` serves the whole
    /// generated surface, and this is the cheapest place to keep that claim
    /// true. It also covers what a deleted T-RG-2 used to assert directly —
    /// that the user-facing surface is reachable on that half — now that
    /// `attach`'s early return is the only thing standing between the two.
    #[tokio::test]
    async fn the_half_that_serves_everything_gets_no_layer_at_all() {
        let router = || attach(stand_in_generated(), true);
        assert_eq!(
            status(router(), Method::POST, "/sandboxes").await,
            StatusCode::OK,
            "aenv-api must serve the user-facing surface"
        );
        assert_eq!(
            status(router(), Method::GET, "/health").await,
            StatusCode::OK
        );

        // 🔴 The contrast, against the same stand-in router. Without it an
        // `attach` that never attached anything would pass — and that is not a
        // hypothetical mistake, it is what this function does for one of the
        // two halves.
        assert_eq!(
            status(
                attach(stand_in_generated(), false),
                Method::POST,
                "/sandboxes"
            )
            .await,
            StatusCode::NOT_FOUND,
            "the same call on the half that does get a layer is refused"
        );
    }

    // ── The preStop hook, which is a caller of this gate ───────────────────

    /// The DaemonSet, read at compile time. The hook is a shell script in a
    /// YAML file: nothing at runtime type-checks it, and its failures are
    /// swallowed by its own `||`.
    const DAEMONSET: &str = include_str!("../../deploy/k8s/base/agentenv-daemonset.yaml");

    fn prestop_script() -> &'static str {
        DAEMONSET
            .split_once("preStop:")
            .expect("the daemonset has a preStop hook")
            .1
            .split_once("postStart:")
            .expect("preStop is followed by postStart")
            .0
    }

    /// Every call the preStop hook makes to the node's own API, as
    /// (method, path).
    fn prestop_calls() -> Vec<(Method, String)> {
        prestop_script()
            .split("curl ")
            .skip(1)
            .filter_map(|call| {
                let (flags, rest) = call.split_once("http://localhost:8000")?;
                let path: String = rest
                    .chars()
                    .take_while(|c| !c.is_whitespace() && *c != '"' && *c != '\'')
                    .collect();
                // The hook interpolates the node's own name into the path.
                let path = path.replace("${AENV_NODE_ID}", "ip-10-0-1-7");
                let method = if flags.contains("-X POST") {
                    Method::POST
                } else {
                    Method::GET
                };
                Some((method, path))
            })
            .collect()
    }

    /// 🔴 T-RG-10. **The hook only calls routes this gate still serves.**
    ///
    /// This is the test that would have caught the outage this batch exists to
    /// prevent. The hook used to poll `GET /sandboxes`; under `aenv-node`
    /// that answers 404, `curl -sf` fails, and the loop it fed had no exit for
    /// a failed read — so every node pod deletion, of any kind, hung in
    /// Terminating for the full 3600-second grace period, and the server never
    /// received SIGTERM and so never paused the sandboxes it was holding.
    ///
    /// Asserted against the allowlist itself rather than against a copy of it,
    /// so moving a route off the allowlist and forgetting the hook is a failing
    /// test rather than an hour per node.
    #[test]
    fn every_call_the_prestop_hook_makes_is_a_route_a_node_still_serves() {
        let calls = prestop_calls();
        assert_eq!(
            calls.len(),
            2,
            "the hook drains the node and then waits for it: {calls:?}"
        );

        for (method, path) in &calls {
            assert!(
                node_serves(method, path),
                "the preStop hook calls {method} {path}, which an aenv-node process answers \
                 with 404. The hook swallows its own failures, so this does not show up as an \
                 error — it shows up as a pod stuck in Terminating for the whole grace period."
            );
        }

        // The control face: the route it used to call is one this gate refuses,
        // so the assertion above is about the allowlist and not vacuous.
        assert!(!node_serves(&Method::GET, "/sandboxes"));
    }

    /// 🔴 T-RG-11. The drain wait is bounded, twice over.
    ///
    /// kubelet does not send SIGTERM until preStop returns. A hook that waits
    /// forever is therefore not a hook that is being careful — it is a hook
    /// that prevents the server's graceful shutdown, which is the thing that
    /// *pauses and persists* the sandboxes, from ever running. What follows is
    /// the grace period expiring and a SIGKILL. Both exits have to exist: one
    /// for "the count will not come" and one for "the count is not going down".
    #[test]
    fn the_drain_wait_cannot_run_forever() {
        let script = prestop_script();
        assert!(
            script.contains("DRAIN_READ_FAILURES") && script.contains("-ge 10"),
            "a read that keeps failing must give up rather than retry forever"
        );
        assert!(
            script.contains("DRAIN_DEADLINE") && script.contains("date +%s"),
            "a count that never reaches zero must hit a deadline"
        );
        // Both exits hand over rather than exiting the container themselves.
        assert_eq!(
            script
                .matches("handing over to the server's own shutdown")
                .count(),
            2,
            "each bounded exit must say what happens next"
        );
    }

    /// 🔴 T-RG-12. The drain waits on the sandboxes that are still
    /// transitioning, not on the ones already at rest.
    ///
    /// Two disjoint counters answer "is anything still transitioning here":
    /// `sandboxCount` alone misses `Creating`/`Resuming`, which is why
    /// `sandboxStartingCount` has to be added too. `sandboxPausedCount` must
    /// stay out of the sum: a Paused sandbox has no running VM, is already
    /// durably persisted to this node's local disk
    /// (`src/orchestrator/persistence/file_backed.rs`, restored by
    /// `Orchestrator::new` on the next process's startup), and is explicitly
    /// excluded from the set of sandboxes shutdown itself acts on
    /// (`run_shutdown_cleanup`'s `excluded_states: [SandboxState::Paused]`).
    /// It also never reaches zero on its own — pausing exists so a sandbox
    /// stays there until a later resume — so summing it in stalls every
    /// rolling update for the full drain timeout on any cluster that has a
    /// paused sandbox anywhere, which is the ordinary state of a cluster that
    /// uses pause at all.
    #[test]
    fn the_drain_counts_running_and_starting_sandboxes_but_not_paused() {
        let script = prestop_script();
        for field in ["sandboxCount", "sandboxStartingCount"] {
            assert!(
                script.contains(field),
                "the drain condition must include {field}"
            );
        }
        // The check is against the `jq` query line itself, not the whole
        // script: the surrounding comments name `sandboxPausedCount` on
        // purpose, to explain why it is excluded, and a substring check over
        // the full script would fail on that prose rather than on the query.
        let jq_line = script
            .lines()
            .find(|line| line.contains("sandboxCount|type"))
            .expect("the drain loop reads the count through a jq filter over sandboxCount");
        assert!(
            jq_line.contains(".sandboxCount + .sandboxStartingCount"),
            "the drain condition must sum exactly running + starting: {jq_line}"
        );
        assert!(
            !jq_line.contains("sandboxPausedCount"),
            "a paused sandbox is already durably persisted and outlives this process on \
             purpose; summing it into the drain query stalls every rollout for as long as the \
             cluster holds any paused sandbox at all: {jq_line}"
        );
        // 🔴 And a field that is not a number is not a zero. Without this, an
        // endpoint that answered `[]` — which it does when observability is
        // switched off — would read as "nothing left here".
        assert!(
            script.contains("jq -e") && script.contains(r#"|type)=="number""#),
            "a missing or non-numeric count must be a failed read, never a zero"
        );
    }

    /// 🔴 T-RG-9. The data plane is outside the gate, structurally.
    ///
    /// Same property [`super::super::server::assemble`] relies on for the
    /// control-plane gate, asserted again here because this layer is attached
    /// at the same seam and would take the node's entire sandbox traffic down
    /// with it if the seam ever changed. The first assertion gives the test
    /// resolution.
    #[tokio::test]
    async fn the_gate_does_not_reach_what_is_merged_after_it() {
        let router = || attach(stand_in_generated(), false).merge(stand_in_data_plane());

        assert_eq!(
            status(router(), Method::POST, "/sandboxes").await,
            StatusCode::NOT_FOUND,
            "the probe has resolution: a gated route is refused"
        );
        assert_eq!(
            status(router(), Method::GET, "/proxy/anything").await,
            StatusCode::OK,
            "the sandbox data plane must keep answering on a node"
        );
        assert_eq!(
            status(router(), Method::GET, "/not-a-route-at-all").await,
            StatusCode::OK,
            "the fallback carries host-routed sandbox traffic and is not this gate's business"
        );
    }
}
