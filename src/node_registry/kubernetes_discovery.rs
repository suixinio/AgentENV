//! Port of `services/scheduler/internal/kubernetes_discovery.go` (379
//! lines) — turning EndpointSlice/Pod watches into the `Node` lists
//! [`super::registry::AtomicNodeRegistry::set`] consumes.
//!
//! # What is fully ported and tested, and what is not
//!
//! Go's own test file (`kubernetes_discovery_test.go`) never drives
//! `NewKubernetesDiscovery`/`Run` against a real or fake apiserver either —
//! every test in it exercises either a pure function
//! (`nodesFromEndpointSlices`, `nodeFromEndpoint`,
//! `selectRoutableEndpointAddress`, `filterNodesByPodLabels`,
//! `validateOptionalPodSelector`) or `AtomicNodeRegistry` directly, fed by
//! those functions' output. This port draws the same boundary:
//!
//! - [`nodes_from_endpoint_slices`], [`filter_nodes_by_pod_labels`], and
//!   [`validate_optional_pod_selector`] are pure, fully unit-tested below,
//!   and cover every case `kubernetes_discovery_test.go` covers for them.
//! - [`KubernetesDiscovery`] is the live watch glue — three `kube::runtime`
//!   watchers (EndpointSlice, and two label-selected Pod watches) feeding a
//!   registry via the pure functions above. **It compiles but has not been
//!   exercised against a real Kubernetes API server in this change** — Stage
//!   A's own construction plan calls for that verification on a live k3s
//!   dev cluster (`docs/proposals/_sd-phase4-stageA-node-inventory.md` §9
//!   step 4), which this agent was not able to do (no cluster access in this
//!   session). Whoever picks this up next should treat `KubernetesDiscovery`
//!   as the first thing to point at a real cluster and watch — RBAC
//!   (`deploy/k8s/base/role.yaml`'s `endpointslices`/`pods` get/list/watch
//!   rules, and the new `agentenv-api` ServiceAccount that needs to bind to
//!   them), label-selector syntax the API server actually enforces, and the
//!   `Serving`/`Terminating` condition semantics on a real EndpointSlice
//!   controller's output.
//! - 🔴 P6-d correction: the line that used to stand here — "nothing in
//!   this file is wired into any assembly path" — stopped being true the
//!   moment `src/bin/server.rs`'s `start_native_node_registry` started
//!   calling [`KubernetesDiscovery::connect`] under
//!   `[cluster].node_placement_source = "native"`. It is wired in now; what
//!   is still true from the paragraph above is that none of that wiring has
//!   been exercised against a real apiserver.
//!
//! # 🔴 P6-a: no cache-sync gate across the three watchers (tracked, not fixed)
//!
//! Go's `Run` (`kubernetes_discovery.go`) calls `WaitForCacheSync` on all
//! three informers before `syncFromStore` ever runs, and `syncFromStore`'s
//! own first line is `if !d.cacheSynced() { return }` — so a discovery pass
//! never publishes a registry state built from only *some* of the three
//! watchers having reached their initial list. [`sync_from_state`] here has
//! no equivalent gate: [`watch_endpoint_slices`] calls
//! [`super::registry::AtomicNodeRegistry::set`] the moment its own
//! `Event::InitDone` arrives, regardless of whether either pod-selector
//! watch has reached its own `InitDone` yet. With a selector configured,
//! this is a real window — the filter set the not-yet-synced watch would
//! have contributed is empty until its `InitDone`, which
//! [`filter_nodes_by_pod_labels`] reads as "no filter" rather than "filter
//! not ready yet", so a node that should be excluded can be published as
//! active for the span of that window.
//!
//! Left unfixed here per the task's own D2 discipline (port faithfully in
//! this change; known gaps are tracked, not silently patched alongside
//! unrelated work) — and currently latent regardless: neither
//! `deploy/k8s/base/agentenv-api-deployment.yaml` nor
//! `deploy/k8s/base/config/scheduler.json` configures
//! `ignore_pod_selector`/`no_schedule_pod_selector` today, so every sync in
//! this deployment already has all the pod-selector data there is (none) by
//! construction. Whoever configures a selector for the first time should
//! close this gap before relying on it.

use std::collections::{HashMap, HashSet};
use std::net::IpAddr;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, bail, Context, Result};
use futures::StreamExt;
use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::api::discovery::v1::{Endpoint, EndpointPort, EndpointSlice};
use kube::runtime::watcher::{self, Event};
use kube::runtime::WatchStreamExt;
use kube::{Api, Client};
use tracing::warn;

use super::registry::AtomicNodeRegistry;
use super::types::Node;

/// `discoveryv1.LabelServiceName` (`k8s.io/api/discovery/v1/well_known_labels.go`).
const LABEL_SERVICE_NAME: &str = "kubernetes.io/service-name";

/// 🔴 P2: how many *consecutive* watch errors a single stream may absorb
/// before this task gives up and returns `Err`, ending `KubernetesDiscovery::run`
/// (its `tokio::join!` propagates the first task error) so
/// `run_kubernetes_discovery_with_retry` (`src/bin/server.rs`) tears the whole
/// `KubernetesDiscovery` down and rebuilds it — a fresh `kube::Client`, not
/// just a fresh watch — rather than the same task looping on the same
/// connection forever. `.default_backoff()` below throttles *how fast* those
/// errors can arrive (kube-runtime 4.2.0's `watcher()` has no backoff of its
/// own: an unrecoverable failure, e.g. RBAC that will never resolve without
/// an operator, would otherwise be an unthrottled LIST/WATCH loop against the
/// apiserver); this constant is what stops the throttled loop from running
/// forever in the first place. Reset to zero on any non-`Err` item, so a
/// flaky connection that recovers between failures never accumulates toward
/// this — only an unbroken run of failures does.
const MAX_CONSECUTIVE_WATCH_ERRORS: u32 = 5;

/// Stage A-local stand-in for `services/shared/config.SchedulerDiscoveryKubernetesConfig`.
/// Not yet registered on [`crate::cfg::AppConfig`] — same deferral as
/// [`super::filter::NodeResourceLimit`].
#[derive(Debug, Clone, Default)]
pub struct KubernetesDiscoveryConfig {
    pub namespace: String,
    pub service_name: String,
    pub port: i32,
    /// Defaults to `"http"` when empty.
    pub scheme: String,
    /// Empty disables the ignore-pod filter entirely.
    pub ignore_pod_selector: String,
    /// Empty disables the no-schedule-pod filter entirely.
    pub no_schedule_pod_selector: String,
}

// ─────────────────────────────────────────────────────────────────────────
// Pure functions — EndpointSlice/Pod objects to Node lists.
// ─────────────────────────────────────────────────────────────────────────

/// Converts a batch of `EndpointSlice` objects into active/lingering `Node`
/// lists. Uses the `Serving` condition (not `Ready`) to decide whether an
/// endpoint is usable, and `Terminating` to distinguish active from
/// lingering nodes.
pub fn nodes_from_endpoint_slices(
    slices: &[EndpointSlice],
    cfg: &KubernetesDiscoveryConfig,
) -> (Vec<Node>, Vec<Node>) {
    if slices.is_empty() {
        return (Vec::new(), Vec::new());
    }

    let scheme = {
        let trimmed = cfg.scheme.trim();
        if trimmed.is_empty() {
            "http"
        } else {
            trimmed
        }
    };

    let mut active_by_id: HashMap<String, Node> = HashMap::new();
    let mut lingering_by_id: HashMap<String, Node> = HashMap::new();
    for slice in slices {
        if !has_matching_endpoint_port(slice.ports.as_deref(), cfg.port) {
            continue;
        }
        for endpoint in &slice.endpoints {
            let Some((node, lingering)) = node_from_endpoint(endpoint, cfg.port, scheme) else {
                continue;
            };
            if lingering {
                lingering_by_id.insert(node.id.clone(), node);
            } else {
                active_by_id.insert(node.id.clone(), node);
            }
        }
    }

    (
        active_by_id.into_values().collect(),
        lingering_by_id.into_values().collect(),
    )
}

fn has_matching_endpoint_port(ports: Option<&[EndpointPort]>, port: i32) -> bool {
    ports
        .unwrap_or(&[])
        .iter()
        .any(|candidate| candidate.port == Some(port))
}

/// Converts one `EndpointSlice` endpoint into a `Node`. Returns `None` when
/// the endpoint is not `Serving`, names no pod, or offers no routable
/// address.
fn node_from_endpoint(endpoint: &Endpoint, port: i32, scheme: &str) -> Option<(Node, bool)> {
    let serving = endpoint
        .conditions
        .as_ref()
        .and_then(|c| c.serving)
        .unwrap_or(false);
    if !serving {
        return None;
    }

    let target_ref_name = endpoint
        .target_ref
        .as_ref()
        .and_then(|r| r.name.as_deref())
        .filter(|name| !name.is_empty())?;

    let id = node_id_for_endpoint(endpoint, target_ref_name);
    let address = select_routable_endpoint_address(&endpoint.addresses)?;

    let terminating = endpoint
        .conditions
        .as_ref()
        .and_then(|c| c.terminating)
        .unwrap_or(false);

    let host_port = format_host_port(&address, port);
    let pod_name = if id != target_ref_name {
        target_ref_name.to_string()
    } else {
        String::new()
    };

    Some((
        Node {
            id,
            endpoint: format!("{scheme}://{host_port}"),
            pod_name,
        },
        terminating,
    ))
}

/// Names the node an endpoint belongs to: the cluster's name for the
/// machine (`endpoint.node_name`) when present, falling back to the pod
/// name. See `node_registry.go`'s `nodeIDForEndpoint` doc comment for why
/// the machine's own name — not the pod's — is what a paused sandbox's
/// registry row has to keep matching across pod restarts.
fn node_id_for_endpoint(endpoint: &Endpoint, target_ref_name: &str) -> String {
    if let Some(name) = endpoint.node_name.as_deref() {
        let trimmed = name.trim();
        if !trimmed.is_empty() {
            return trimmed.to_string();
        }
    }
    target_ref_name.to_string()
}

fn select_routable_endpoint_address(addresses: &[String]) -> Option<String> {
    addresses
        .iter()
        .map(|a| a.trim())
        .find(|a| !a.is_empty() && IpAddr::from_str(a).is_ok())
        .map(|a| a.to_string())
}

/// `net.JoinHostPort` equivalent: wraps an IPv6 literal in brackets, leaves
/// an IPv4 one bare.
fn format_host_port(address: &str, port: i32) -> String {
    match IpAddr::from_str(address) {
        Ok(IpAddr::V6(_)) => format!("[{address}]:{port}"),
        _ => format!("{address}:{port}"),
    }
}

/// Removes nodes whose id (see [`node_id_for_endpoint`]) is present in
/// `ignore_pod_ids`, and moves nodes present in `no_schedule_pod_ids` from
/// `active` into `lingering`.
///
/// 🔴 Ported as-is from `filterNodesByPodLabels`, including a quirk it
/// inherits rather than introduces: the lookup key is the node's *id*, which
/// is the Kubernetes node name whenever `endpoint.node_name` was set (see
/// [`node_id_for_endpoint`]) — not necessarily the pod name the selector
/// actually filtered on. Go's own informer-store lookup has the identical
/// shape (`d.config.Namespace + "/" + node.ID"`, `kubernetes_discovery.go`
/// around line 300). Recorded here rather than fixed, per the task's
/// direction not to correct pre-existing behavior discovered while porting.
pub fn filter_nodes_by_pod_labels(
    active: Vec<Node>,
    lingering: Vec<Node>,
    ignore_pod_ids: Option<&HashSet<String>>,
    no_schedule_pod_ids: Option<&HashSet<String>>,
) -> (Vec<Node>, Vec<Node>) {
    if ignore_pod_ids.is_none() && no_schedule_pod_ids.is_none() {
        return (active, lingering);
    }

    let mut active_by_id: HashMap<String, Node> =
        active.into_iter().map(|n| (n.id.clone(), n)).collect();
    let mut lingering_by_id: HashMap<String, Node> =
        lingering.into_iter().map(|n| (n.id.clone(), n)).collect();

    let all_ids: HashSet<String> = active_by_id
        .keys()
        .chain(lingering_by_id.keys())
        .cloned()
        .collect();

    for node_id in all_ids {
        if node_id.is_empty() {
            continue;
        }
        if ignore_pod_ids.is_some_and(|set| set.contains(&node_id)) {
            active_by_id.remove(&node_id);
            lingering_by_id.remove(&node_id);
            continue;
        }
        if no_schedule_pod_ids.is_some_and(|set| set.contains(&node_id)) {
            if let Some(node) = active_by_id.remove(&node_id) {
                lingering_by_id.insert(node_id, node);
            }
        }
    }

    (
        active_by_id.into_values().collect(),
        lingering_by_id.into_values().collect(),
    )
}

// ─────────────────────────────────────────────────────────────────────────
// Pod selector syntax validation.
//
// 🔴 Not a port of `k8s.io/apimachinery/pkg/labels.Parse` — no equivalent
// client-side label-selector parser ships in the `kube`/`k8s-openapi` crate
// family (confirmed by inspecting `kube-core-4.2.0`'s `labels.rs`: `Selector`
// is a typed builder, not a string parser). `watcher::Config::labels` passes
// the raw string straight through as the `labelSelector` query parameter and
// leaves syntax enforcement to the API server. What follows is a permissive,
// self-contained syntax check covering the same requirement grammar
// (existence, `!`, `=`/`==`/`!=`, `in (...)`/`notin (...)`, comma-joined) —
// good enough to reject the kind of typo `validateOptionalPodSelector`
// exists to catch at config-load time, not a byte-for-byte reimplementation
// of Kubernetes' DNS-1123 key/value grammar.
// ─────────────────────────────────────────────────────────────────────────

pub fn validate_optional_pod_selector(raw: &str, field: &str) -> Result<()> {
    let selector = raw.trim();
    if selector.is_empty() {
        return Ok(());
    }
    parse_label_selector(selector)
        .with_context(|| format!("scheduler.discovery.kubernetes.{field} is invalid"))
}

fn parse_label_selector(selector: &str) -> Result<()> {
    for requirement in split_top_level_commas(selector) {
        let requirement = requirement.trim();
        if requirement.is_empty() {
            bail!("empty requirement in selector {selector:?}");
        }
        validate_requirement(requirement)?;
    }
    Ok(())
}

fn split_top_level_commas(s: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth = 0i32;
    let mut start = 0usize;
    for (i, c) in s.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => depth -= 1,
            ',' if depth == 0 => {
                parts.push(&s[start..i]);
                start = i + c.len_utf8();
            }
            _ => {}
        }
    }
    parts.push(&s[start..]);
    parts
}

fn validate_requirement(requirement: &str) -> Result<()> {
    if let Some(rest) = requirement.strip_prefix('!') {
        return validate_key(rest.trim());
    }
    for op in ["!=", "==", "="] {
        if let Some(idx) = requirement.find(op) {
            let key = requirement[..idx].trim();
            let value = requirement[idx + op.len()..].trim();
            validate_key(key)?;
            validate_value(value)?;
            return Ok(());
        }
    }
    if let Some(space_idx) = requirement.find(char::is_whitespace) {
        let key = requirement[..space_idx].trim();
        let rest = requirement[space_idx..].trim();
        let list = rest
            .strip_prefix("notin")
            .or_else(|| rest.strip_prefix("in"))
            .map(str::trim)
            .ok_or_else(|| anyhow!("unrecognized operator in requirement {requirement:?}"))?;
        validate_key(key)?;
        return validate_value_list(list);
    }
    // A bare key: existence requirement.
    validate_key(requirement)
}

fn validate_value_list(list: &str) -> Result<()> {
    let inner = list
        .strip_prefix('(')
        .and_then(|s| s.strip_suffix(')'))
        .ok_or_else(|| anyhow!("expected a parenthesized value list, got {list:?}"))?;
    if inner.trim().is_empty() {
        bail!("empty value list in {list:?}");
    }
    for value in inner.split(',') {
        validate_value(value.trim())?;
    }
    Ok(())
}

fn validate_key(key: &str) -> Result<()> {
    if key.is_empty() {
        bail!("empty label key");
    }
    if key.chars().any(char::is_whitespace) || key.contains(['(', ')']) {
        bail!("invalid label key {key:?}");
    }
    Ok(())
}

fn validate_value(value: &str) -> Result<()> {
    if value.is_empty() {
        bail!("empty label value");
    }
    if value.chars().any(char::is_whitespace) || value.contains(['(', ')']) {
        bail!("invalid label value {value:?}");
    }
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────
// Live watch glue — see the module doc comment for what has and has not
// been validated.
// ─────────────────────────────────────────────────────────────────────────

fn object_key(namespace: Option<&str>, name: Option<&str>) -> String {
    format!("{}/{}", namespace.unwrap_or(""), name.unwrap_or(""))
}

#[derive(Default)]
struct DiscoveryState {
    endpoint_slices: HashMap<String, EndpointSlice>,
    ignore_pod_names: HashSet<String>,
    no_schedule_pod_names: HashSet<String>,
}

/// Drives three `kube::runtime` watchers — one EndpointSlice watch scoped to
/// `cfg.service_name`, and up to two Pod watches scoped to
/// `cfg.ignore_pod_selector`/`cfg.no_schedule_pod_selector` — and calls
/// [`AtomicNodeRegistry::set`] every time any of them changes.
pub struct KubernetesDiscovery {
    client: Client,
    config: KubernetesDiscoveryConfig,
    registry: Arc<AtomicNodeRegistry>,
}

impl KubernetesDiscovery {
    /// Validates the configured pod selectors and builds a discovery
    /// instance. Connects no socket by itself — [`Self::run`] does that.
    pub fn new(
        client: Client,
        config: KubernetesDiscoveryConfig,
        registry: Arc<AtomicNodeRegistry>,
    ) -> Result<Self> {
        validate_optional_pod_selector(&config.ignore_pod_selector, "ignore_pod_selector")?;
        validate_optional_pod_selector(
            &config.no_schedule_pod_selector,
            "no_schedule_pod_selector",
        )?;
        Ok(Self {
            client,
            config,
            registry,
        })
    }

    /// [`Self::new`], building the client via in-cluster config (falling
    /// back to a local kubeconfig — `kube::Config::infer`'s standard
    /// resolution order, a superset of Go's `rest.InClusterConfig`-only
    /// approach, which is more useful for a developer running against a
    /// local cluster and behaves identically inside a Pod).
    pub async fn connect(
        config: KubernetesDiscoveryConfig,
        registry: Arc<AtomicNodeRegistry>,
    ) -> Result<Self> {
        let client = Client::try_default()
            .await
            .context("build kubernetes client")?;
        Self::new(client, config, registry)
    }

    /// Runs until one of the three watch streams ends (which, for
    /// `kube::runtime::watcher`, only happens on cancellation — it retries
    /// transient errors internally) or errors out. Never returns `Ok(())`
    /// under normal operation; callers drive it as a background task.
    pub async fn run(self) -> Result<()> {
        let state = Arc::new(Mutex::new(DiscoveryState::default()));

        let endpoint_task = {
            let state = state.clone();
            let registry = registry_handle(&self.registry);
            let config = self.config.clone();
            let api: Api<EndpointSlice> = Api::namespaced(self.client.clone(), &config.namespace);
            let label_selector = format!("{LABEL_SERVICE_NAME}={}", config.service_name);
            let stream = watcher::watcher(api, watcher::Config::default().labels(&label_selector))
                .default_backoff();
            tokio::spawn(watch_endpoint_slices(stream, state, registry, config))
        };

        let ignore_task = self.spawn_pod_watch(
            state.clone(),
            self.config.ignore_pod_selector.clone(),
            PodSelectorKind::Ignore,
        );
        let no_schedule_task = self.spawn_pod_watch(
            state.clone(),
            self.config.no_schedule_pod_selector.clone(),
            PodSelectorKind::NoSchedule,
        );

        // Any of the three ending (they should not, under normal retrying
        // operation) ends discovery entirely — mirrors Go's `Run`, where all
        // three informers share one `ctx` and the whole thing tears down
        // together.
        let (endpoint_result, ignore_result, no_schedule_result) =
            tokio::join!(endpoint_task, ignore_task, no_schedule_task);
        endpoint_result.context("endpointslice watch task panicked")??;
        ignore_result.context("ignore-pod watch task panicked")??;
        no_schedule_result.context("no-schedule-pod watch task panicked")??;
        Ok(())
    }

    fn spawn_pod_watch(
        &self,
        state: Arc<Mutex<DiscoveryState>>,
        selector: String,
        kind: PodSelectorKind,
    ) -> tokio::task::JoinHandle<Result<()>> {
        let registry = registry_handle(&self.registry);
        let config = self.config.clone();
        if selector.trim().is_empty() {
            // Disabled: resolve immediately, matching Go's `nil` informer
            // (never runs, never blocks `Run`'s cache-sync wait).
            return tokio::spawn(async { Ok(()) });
        }
        let api: Api<Pod> = Api::namespaced(self.client.clone(), &config.namespace);
        let stream =
            watcher::watcher(api, watcher::Config::default().labels(&selector)).default_backoff();
        tokio::spawn(watch_pod_selector(stream, state, registry, config, kind))
    }
}

#[derive(Clone, Copy)]
enum PodSelectorKind {
    Ignore,
    NoSchedule,
}

/// A cheap, cloneable handle so each watch task can call `set` without
/// holding a reference into `self`.
fn registry_handle(registry: &Arc<AtomicNodeRegistry>) -> Arc<AtomicNodeRegistry> {
    registry.clone()
}

async fn watch_endpoint_slices(
    stream: impl futures::Stream<Item = watcher::Result<Event<EndpointSlice>>> + Send,
    state: Arc<Mutex<DiscoveryState>>,
    registry: Arc<AtomicNodeRegistry>,
    config: KubernetesDiscoveryConfig,
) -> Result<()> {
    let mut stream = Box::pin(stream);
    let mut pending: Vec<EndpointSlice> = Vec::new();
    // 🔴 P2: reset on every non-`Err` item, incremented and checked only in
    // the `Err` arm below — see [`MAX_CONSECUTIVE_WATCH_ERRORS`]'s own doc.
    let mut consecutive_errors: u32 = 0;
    while let Some(event) = stream.next().await {
        if event.is_ok() {
            consecutive_errors = 0;
        }
        match event {
            Ok(Event::Init) => pending.clear(),
            Ok(Event::InitApply(obj)) => pending.push(obj),
            Ok(Event::InitDone) => {
                let mut s = state.lock().expect("discovery state lock poisoned");
                s.endpoint_slices = pending
                    .drain(..)
                    .map(|obj| {
                        (
                            object_key(
                                obj.metadata.namespace.as_deref(),
                                obj.metadata.name.as_deref(),
                            ),
                            obj,
                        )
                    })
                    .collect();
                drop(s);
                sync_from_state(&state, &registry, &config);
            }
            Ok(Event::Apply(obj)) => {
                let key = object_key(
                    obj.metadata.namespace.as_deref(),
                    obj.metadata.name.as_deref(),
                );
                state
                    .lock()
                    .expect("discovery state lock poisoned")
                    .endpoint_slices
                    .insert(key, obj);
                sync_from_state(&state, &registry, &config);
            }
            Ok(Event::Delete(obj)) => {
                let key = object_key(
                    obj.metadata.namespace.as_deref(),
                    obj.metadata.name.as_deref(),
                );
                state
                    .lock()
                    .expect("discovery state lock poisoned")
                    .endpoint_slices
                    .remove(&key);
                sync_from_state(&state, &registry, &config);
            }
            Err(err) => {
                consecutive_errors += 1;
                warn!(
                    error = %err,
                    consecutive_errors,
                    "kubernetes discovery: endpointslice watch error"
                );
                if consecutive_errors >= MAX_CONSECUTIVE_WATCH_ERRORS {
                    bail!(
                        "endpointslice watch failed {consecutive_errors} times in a row (last \
                         error: {err}); ending discovery so the caller rebuilds the watch from \
                         scratch"
                    );
                }
            }
        }
    }
    Ok(())
}

async fn watch_pod_selector(
    stream: impl futures::Stream<Item = watcher::Result<Event<Pod>>> + Send,
    state: Arc<Mutex<DiscoveryState>>,
    registry: Arc<AtomicNodeRegistry>,
    config: KubernetesDiscoveryConfig,
    kind: PodSelectorKind,
) -> Result<()> {
    let mut stream = Box::pin(stream);
    let mut pending: Vec<String> = Vec::new();
    let mut consecutive_errors: u32 = 0;
    while let Some(event) = stream.next().await {
        if event.is_ok() {
            consecutive_errors = 0;
        }
        match event {
            Ok(Event::Init) => pending.clear(),
            Ok(Event::InitApply(obj)) => {
                if let Some(name) = obj.metadata.name {
                    pending.push(name);
                }
            }
            Ok(Event::InitDone) => {
                let names: HashSet<String> = pending.drain(..).collect();
                let mut s = state.lock().expect("discovery state lock poisoned");
                set_pod_names(&mut s, kind, names);
                drop(s);
                sync_from_state(&state, &registry, &config);
            }
            Ok(Event::Apply(obj)) => {
                if let Some(name) = obj.metadata.name {
                    let mut s = state.lock().expect("discovery state lock poisoned");
                    pod_names_mut(&mut s, kind).insert(name);
                    drop(s);
                    sync_from_state(&state, &registry, &config);
                }
            }
            Ok(Event::Delete(obj)) => {
                if let Some(name) = obj.metadata.name {
                    let mut s = state.lock().expect("discovery state lock poisoned");
                    pod_names_mut(&mut s, kind).remove(&name);
                    drop(s);
                    sync_from_state(&state, &registry, &config);
                }
            }
            Err(err) => {
                consecutive_errors += 1;
                warn!(
                    error = %err,
                    consecutive_errors,
                    "kubernetes discovery: pod selector watch error"
                );
                if consecutive_errors >= MAX_CONSECUTIVE_WATCH_ERRORS {
                    bail!(
                        "pod selector watch failed {consecutive_errors} times in a row (last \
                         error: {err}); ending discovery so the caller rebuilds the watch from \
                         scratch"
                    );
                }
            }
        }
    }
    Ok(())
}

fn pod_names_mut(state: &mut DiscoveryState, kind: PodSelectorKind) -> &mut HashSet<String> {
    match kind {
        PodSelectorKind::Ignore => &mut state.ignore_pod_names,
        PodSelectorKind::NoSchedule => &mut state.no_schedule_pod_names,
    }
}

fn set_pod_names(state: &mut DiscoveryState, kind: PodSelectorKind, names: HashSet<String>) {
    match kind {
        PodSelectorKind::Ignore => state.ignore_pod_names = names,
        PodSelectorKind::NoSchedule => state.no_schedule_pod_names = names,
    }
}

fn sync_from_state(
    state: &Arc<Mutex<DiscoveryState>>,
    registry: &Arc<AtomicNodeRegistry>,
    config: &KubernetesDiscoveryConfig,
) {
    let (active, lingering, ignore, no_schedule) = {
        let s = state.lock().expect("discovery state lock poisoned");
        let slices: Vec<EndpointSlice> = s.endpoint_slices.values().cloned().collect();
        let (active, lingering) = nodes_from_endpoint_slices(&slices, config);
        (
            active,
            lingering,
            (!s.ignore_pod_names.is_empty()).then(|| s.ignore_pod_names.clone()),
            (!s.no_schedule_pod_names.is_empty()).then(|| s.no_schedule_pod_names.clone()),
        )
    };
    // Empty selector strings mean "no filter," represented the same way Go
    // represents a nil informer: `None` here, not an empty set — an empty
    // set would (correctly, but pointlessly) filter nothing, while `None`
    // documents the filter is off. Only meaningful when the selector is
    // configured *and* has not (yet) matched anything, which `sync_from_state`
    // cannot tell apart from "filter disabled" purely from an empty set, so
    // the caller's `config` is consulted instead.
    let ignore = if config.ignore_pod_selector.trim().is_empty() {
        None
    } else {
        ignore
    };
    let no_schedule = if config.no_schedule_pod_selector.trim().is_empty() {
        None
    } else {
        no_schedule
    };
    let (active, lingering) =
        filter_nodes_by_pod_labels(active, lingering, ignore.as_ref(), no_schedule.as_ref());
    registry.set(active, lingering);
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::core::v1::ObjectReference;
    use k8s_openapi::api::discovery::v1::EndpointConditions;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use std::time::Duration;

    fn default_cfg() -> KubernetesDiscoveryConfig {
        KubernetesDiscoveryConfig {
            namespace: "agentenv-system".to_string(),
            service_name: "agentenv-nodes".to_string(),
            port: 8000,
            scheme: "http".to_string(),
            ..Default::default()
        }
    }

    fn endpoint_slice(port: i32, endpoints: Vec<Endpoint>) -> EndpointSlice {
        EndpointSlice {
            metadata: ObjectMeta {
                name: Some("agentenv-nodes-slice".to_string()),
                namespace: Some("agentenv-system".to_string()),
                labels: Some(
                    [(LABEL_SERVICE_NAME.to_string(), "agentenv-nodes".to_string())]
                        .into_iter()
                        .collect(),
                ),
                ..Default::default()
            },
            address_type: "IPv4".to_string(),
            endpoints,
            ports: Some(vec![EndpointPort {
                port: Some(port),
                ..Default::default()
            }]),
        }
    }

    fn endpoint_with_conditions(
        name: &str,
        address: &str,
        serving: Option<bool>,
        terminating: Option<bool>,
    ) -> Endpoint {
        Endpoint {
            addresses: vec![address.to_string()],
            conditions: Some(EndpointConditions {
                serving,
                terminating,
                ..Default::default()
            }),
            target_ref: Some(ObjectReference {
                kind: Some("Pod".to_string()),
                name: Some(name.to_string()),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn serving_endpoint(name: &str, address: &str) -> Endpoint {
        endpoint_with_conditions(name, address, Some(true), None)
    }

    fn not_serving_endpoint(name: &str, address: &str) -> Endpoint {
        endpoint_with_conditions(name, address, Some(false), None)
    }

    fn terminating_endpoint(name: &str, address: &str) -> Endpoint {
        endpoint_with_conditions(name, address, Some(true), Some(true))
    }

    fn endpoint_with_addresses(name: &str, addresses: Vec<&str>) -> Endpoint {
        Endpoint {
            addresses: addresses.into_iter().map(str::to_string).collect(),
            conditions: Some(EndpointConditions {
                serving: Some(true),
                ..Default::default()
            }),
            target_ref: Some(ObjectReference {
                kind: Some("Pod".to_string()),
                name: Some(name.to_string()),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn ids(nodes: &[Node]) -> Vec<String> {
        let mut ids: Vec<String> = nodes.iter().map(|n| n.id.clone()).collect();
        ids.sort();
        ids
    }

    #[test]
    fn serving_endpoint_is_active() {
        let (active, lingering) = nodes_from_endpoint_slices(
            &[endpoint_slice(
                8000,
                vec![serving_endpoint("agentenv-node-a", "10.0.0.1")],
            )],
            &default_cfg(),
        );
        assert_eq!(active.len(), 1);
        assert!(lingering.is_empty());
        assert_eq!(active[0].id, "agentenv-node-a");
        assert_eq!(active[0].endpoint, "http://10.0.0.1:8000");
    }

    #[test]
    fn not_serving_is_excluded() {
        let (active, lingering) = nodes_from_endpoint_slices(
            &[endpoint_slice(
                8000,
                vec![not_serving_endpoint("agentenv-node-a", "10.0.0.1")],
            )],
            &default_cfg(),
        );
        assert!(active.is_empty());
        assert!(lingering.is_empty());
    }

    #[test]
    fn terminating_is_lingering() {
        let (active, lingering) = nodes_from_endpoint_slices(
            &[endpoint_slice(
                8000,
                vec![terminating_endpoint("agentenv-node-a", "10.0.0.1")],
            )],
            &default_cfg(),
        );
        assert!(active.is_empty());
        assert_eq!(lingering.len(), 1);
        assert_eq!(lingering[0].id, "agentenv-node-a");
    }

    #[test]
    fn not_serving_terminating_is_excluded() {
        let ep = endpoint_with_conditions("agentenv-node-a", "10.0.0.1", Some(false), Some(true));
        let (active, lingering) =
            nodes_from_endpoint_slices(&[endpoint_slice(8000, vec![ep])], &default_cfg());
        assert!(active.is_empty() && lingering.is_empty());
    }

    #[test]
    fn formats_ipv6_endpoint() {
        let (active, _) = nodes_from_endpoint_slices(
            &[endpoint_slice(
                8000,
                vec![serving_endpoint("agentenv-node-v6", "2001:db8::10")],
            )],
            &default_cfg(),
        );
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].endpoint, "http://[2001:db8::10]:8000");
    }

    #[test]
    fn skips_invalid_addresses() {
        let (active, _) = nodes_from_endpoint_slices(
            &[endpoint_slice(
                8000,
                vec![endpoint_with_addresses(
                    "agentenv-node-a",
                    vec!["not-an-ip"],
                )],
            )],
            &default_cfg(),
        );
        assert!(active.is_empty());
    }

    #[test]
    fn uses_first_valid_address() {
        let (active, _) = nodes_from_endpoint_slices(
            &[endpoint_slice(
                8000,
                vec![endpoint_with_addresses(
                    "agentenv-node-a",
                    vec!["not-an-ip", "10.0.0.3"],
                )],
            )],
            &default_cfg(),
        );
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].endpoint, "http://10.0.0.3:8000");
    }

    #[test]
    fn ignores_slices_without_matching_port() {
        let (active, _) = nodes_from_endpoint_slices(
            &[endpoint_slice(
                9000,
                vec![serving_endpoint("agentenv-node-a", "10.0.0.1")],
            )],
            &default_cfg(),
        );
        assert!(active.is_empty());
    }

    #[test]
    fn identifies_nodes_by_cluster_node_name() {
        let mut ep = serving_endpoint("agentenv-node-xk29f", "10.0.0.1");
        ep.node_name = Some("aenv-worker-01".to_string());
        let (active, _) =
            nodes_from_endpoint_slices(&[endpoint_slice(8000, vec![ep])], &default_cfg());
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].id, "aenv-worker-01");
    }

    #[test]
    fn keeps_identity_across_pod_recreation() {
        let mut before = serving_endpoint("agentenv-node-xk29f", "10.0.0.1");
        before.node_name = Some("aenv-worker-01".to_string());
        let mut after = serving_endpoint("agentenv-node-p4t7z", "10.0.0.9");
        after.node_name = Some("aenv-worker-01".to_string());

        let (first, _) =
            nodes_from_endpoint_slices(&[endpoint_slice(8000, vec![before])], &default_cfg());
        let (second, _) =
            nodes_from_endpoint_slices(&[endpoint_slice(8000, vec![after])], &default_cfg());

        assert_eq!(first[0].id, second[0].id);
        assert_eq!(second[0].endpoint, "http://10.0.0.9:8000");
    }

    #[test]
    fn falls_back_to_pod_name_without_node_name() {
        for ep in [serving_endpoint("agentenv-node-xk29f", "10.0.0.1"), {
            let mut e = serving_endpoint("agentenv-node-xk29f", "10.0.0.1");
            e.node_name = Some("   ".to_string());
            e
        }] {
            let (active, _) =
                nodes_from_endpoint_slices(&[endpoint_slice(8000, vec![ep])], &default_cfg());
            assert_eq!(active.len(), 1);
            assert_eq!(active[0].id, "agentenv-node-xk29f");
        }
    }

    // ---- filter_nodes_by_pod_labels ----

    fn node(id: &str) -> Node {
        Node {
            id: id.to_string(),
            endpoint: "http://10.0.0.1:8000".to_string(),
            pod_name: String::new(),
        }
    }

    #[test]
    fn no_schedule_pod_becomes_lingering() {
        let no_schedule: HashSet<String> = ["agentenv-node-a".to_string()].into_iter().collect();
        let (active, lingering) = filter_nodes_by_pod_labels(
            vec![node("agentenv-node-a")],
            vec![],
            None,
            Some(&no_schedule),
        );
        assert!(active.is_empty());
        assert_eq!(lingering.len(), 1);
        assert_eq!(lingering[0].id, "agentenv-node-a");
    }

    #[test]
    fn ignore_pod_is_excluded() {
        let ignore: HashSet<String> = ["agentenv-node-a".to_string()].into_iter().collect();
        let (active, lingering) = filter_nodes_by_pod_labels(
            vec![node("agentenv-node-a")],
            vec![node("agentenv-node-a")],
            Some(&ignore),
            None,
        );
        assert!(active.is_empty() && lingering.is_empty());
    }

    // 🔴 Regression guard: `ignore` must remove a node from *both* output
    // sets, not just the one it currently occupies. The two-line removal
    // (`active_by_id.remove` and `lingering_by_id.remove`) is easy to
    // simplify into one — e.g. by an editor mistaking the pair for
    // redundant, since a node is normally only ever in one set at a time —
    // but a node that is *already lingering* and starts matching the ignore
    // selector has to disappear entirely, not survive because only the
    // active-side removal was kept.
    #[test]
    fn ignore_removes_an_already_lingering_node_too() {
        let ignore: HashSet<String> = ["agentenv-node-a".to_string()].into_iter().collect();
        let (active, lingering) =
            filter_nodes_by_pod_labels(vec![], vec![node("agentenv-node-a")], Some(&ignore), None);
        assert!(
            active.is_empty() && lingering.is_empty(),
            "an already-lingering node matching the ignore selector must vanish entirely: \
             active={active:?} lingering={lingering:?}"
        );
    }

    #[test]
    fn no_filters_configured_is_a_no_op() {
        let (active, lingering) =
            filter_nodes_by_pod_labels(vec![node("a")], vec![node("b")], None, None);
        assert_eq!(ids(&active), vec!["a"]);
        assert_eq!(ids(&lingering), vec!["b"]);
    }

    // ---- validate_optional_pod_selector ----

    #[test]
    fn validates_pod_selector_syntax() {
        assert!(validate_optional_pod_selector("", "ignore_pod_selector").is_ok());
        assert!(validate_optional_pod_selector(
            "agentenv.io/scheduler-state in (draining,no-schedule)",
            "no_schedule_pod_selector"
        )
        .is_ok());
        assert!(validate_optional_pod_selector(
            "agentenv.io/scheduler-state in (",
            "no_schedule_pod_selector"
        )
        .is_err());
    }

    #[test]
    fn validates_equality_and_existence_and_negation() {
        assert!(validate_optional_pod_selector("agentenv.io/discovery=ignore", "f").is_ok());
        assert!(validate_optional_pod_selector("agentenv.io/discovery==ignore", "f").is_ok());
        assert!(validate_optional_pod_selector("agentenv.io/discovery!=ignore", "f").is_ok());
        assert!(validate_optional_pod_selector("agentenv.io/discovery", "f").is_ok());
        assert!(validate_optional_pod_selector("!agentenv.io/discovery", "f").is_ok());
        assert!(validate_optional_pod_selector(
            "agentenv.io/discovery=ignore,agentenv.io/other=x",
            "f"
        )
        .is_ok());
    }

    #[test]
    fn rejects_empty_value_list_and_unbalanced_parens() {
        assert!(validate_optional_pod_selector("a in ()", "f").is_err());
        assert!(validate_optional_pod_selector("a in (b", "f").is_err());
        assert!(validate_optional_pod_selector("a notin b)", "f").is_err());
    }

    // ---- registry integration (mirrors the non-k8s-specific tests that
    //      live alongside the discovery tests in kubernetes_discovery_test.go) ----

    fn unix(secs: u64) -> std::time::SystemTime {
        std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(secs)
    }

    #[test]
    fn node_registry_reflects_endpoint_removal_across_syncs() {
        use crate::node_registry::registry::NodeRegistry as _;
        use crate::proto::scheduler::{HeartbeatRequest, NodeSnapshot, NodeStatus};

        let registry = AtomicNodeRegistry::new(Vec::new(), Duration::from_secs(30));
        let now = unix(100);

        let (active1, _) = nodes_from_endpoint_slices(
            &[endpoint_slice(
                8000,
                vec![
                    serving_endpoint("agentenv-node-a", "10.0.0.1"),
                    serving_endpoint("agentenv-node-b", "10.0.0.2"),
                ],
            )],
            &default_cfg(),
        );
        registry.set(active1, Vec::new());
        for node_id in ["agentenv-node-a", "agentenv-node-b"] {
            registry
                .heartbeat(
                    &HeartbeatRequest {
                        node_id: node_id.to_string(),
                        cluster_id: "cluster-test".to_string(),
                        service_instance_id: format!("svc-{node_id}"),
                        snapshot: Some(NodeSnapshot {
                            status: NodeStatus::Ready as i32,
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                    now,
                )
                .unwrap();
        }
        assert_eq!(registry.snapshot(false).len(), 2);

        let (active2, _) = nodes_from_endpoint_slices(
            &[endpoint_slice(
                8000,
                vec![serving_endpoint("agentenv-node-b", "10.0.0.2")],
            )],
            &default_cfg(),
        );
        registry.set(active2, Vec::new());

        let snapshot = registry.snapshot(false);
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].id, "agentenv-node-b");
    }

    // ─────────────────────────────────────────────────────────────────────
    // P2: the consecutive-watch-error guard. `watch_endpoint_slices` and
    // `watch_pod_selector` both accept any `impl Stream<Item =
    // watcher::Result<Event<K>>>`, so these feed a synthetic, finite stream
    // built with `futures::stream::iter` rather than a real apiserver —
    // exactly the seam that makes this guard unit-testable at all without a
    // live cluster (see the module doc's own note on what is and is not
    // exercised against one).
    // ─────────────────────────────────────────────────────────────────────

    fn empty_state() -> Arc<Mutex<DiscoveryState>> {
        Arc::new(Mutex::new(DiscoveryState::default()))
    }

    fn empty_registry() -> Arc<AtomicNodeRegistry> {
        Arc::new(AtomicNodeRegistry::new(Vec::new(), Duration::from_secs(30)))
    }

    /// 🔴 The core of P2: an unbroken run of watch errors — RBAC that will
    /// never resolve without an operator, say — must end this task with an
    /// `Err` rather than loop on the same broken stream forever. That `Err`
    /// is what makes `KubernetesDiscovery::run`'s `tokio::join!` return
    /// `Err`, which is what makes `run_kubernetes_discovery_with_retry`
    /// (`src/bin/server.rs`) actually rebuild the client and the watch
    /// instead of a healthy-looking task quietly never doing either again.
    #[tokio::test]
    async fn endpoint_slice_watch_ends_after_consecutive_errors() {
        let events: Vec<watcher::Result<Event<EndpointSlice>>> = (0..MAX_CONSECUTIVE_WATCH_ERRORS)
            .map(|_| Err(watcher::Error::NoResourceVersion))
            .collect();
        let result = watch_endpoint_slices(
            futures::stream::iter(events),
            empty_state(),
            empty_registry(),
            default_cfg(),
        )
        .await;
        let err = result.expect_err("an unbroken run of watch errors must end the task");
        assert!(err.to_string().contains("times in a row"), "{err}");
    }

    /// The control for the test above: one error short of the threshold
    /// must not end the task — otherwise the test above would pass just as
    /// well against a guard that fires on the very first error, which is a
    /// stream that never recovers under real backoff either way and defeats
    /// the whole point of *consecutive*.
    #[tokio::test]
    async fn endpoint_slice_watch_survives_a_run_of_errors_below_the_threshold() {
        let events: Vec<watcher::Result<Event<EndpointSlice>>> = (0..MAX_CONSECUTIVE_WATCH_ERRORS
            - 1)
            .map(|_| Err(watcher::Error::NoResourceVersion))
            .collect();
        let result = watch_endpoint_slices(
            futures::stream::iter(events),
            empty_state(),
            empty_registry(),
            default_cfg(),
        )
        .await;
        assert!(
            result.is_ok(),
            "a run one short of the threshold must not end the task: {result:?}"
        );
    }

    /// The streak resets on any non-error item, so a flaky connection that
    /// recovers between failures never accumulates toward the threshold —
    /// only an *unbroken* run does. Twice the threshold's worth of errors,
    /// split by one successful event, must survive.
    #[tokio::test]
    async fn endpoint_slice_watch_error_streak_resets_on_a_successful_event() {
        let mut events: Vec<watcher::Result<Event<EndpointSlice>>> = Vec::new();
        for _ in 0..MAX_CONSECUTIVE_WATCH_ERRORS - 1 {
            events.push(Err(watcher::Error::NoResourceVersion));
        }
        events.push(Ok(Event::Init));
        for _ in 0..MAX_CONSECUTIVE_WATCH_ERRORS - 1 {
            events.push(Err(watcher::Error::NoResourceVersion));
        }
        let result = watch_endpoint_slices(
            futures::stream::iter(events),
            empty_state(),
            empty_registry(),
            default_cfg(),
        )
        .await;
        assert!(
            result.is_ok(),
            "a successful event must reset the consecutive-error streak: {result:?}"
        );
    }

    /// Same guard, the pod-selector watch's independent implementation of
    /// it — a change that fixed `watch_endpoint_slices` and forgot
    /// `watch_pod_selector` would pass every test above.
    #[tokio::test]
    async fn pod_selector_watch_ends_after_consecutive_errors() {
        let events: Vec<watcher::Result<Event<Pod>>> = (0..MAX_CONSECUTIVE_WATCH_ERRORS)
            .map(|_| Err(watcher::Error::NoResourceVersion))
            .collect();
        let result = watch_pod_selector(
            futures::stream::iter(events),
            empty_state(),
            empty_registry(),
            default_cfg(),
            PodSelectorKind::Ignore,
        )
        .await;
        let err = result.expect_err("an unbroken run of watch errors must end the task");
        assert!(err.to_string().contains("times in a row"), "{err}");
    }
}
