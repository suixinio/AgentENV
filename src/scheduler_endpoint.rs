//! One hot-reloadable gRPC channel to the cluster scheduler, shared by every
//! process-internal consumer that dials it.
//!
//! # Why this exists
//!
//! `4456481` gave `ObservabilityReporter` (heartbeats + lifecycle events) the
//! ability to move its scheduler target by rewriting a file, no pod restart
//! required — because a DaemonSet roll to change one config value costs an
//! hour-long, serial `terminationGracePeriodSeconds` wait
//! (`docs/proposals/2026-08-20-service-decomposition.md`'s phase four
//! section), and rolling back a phase-4 stage means switching `--role api`'s
//! implementation back to talking to the Go scheduler — an action that must
//! not itself require a fleet-wide roll.
//!
//! That capability lived entirely inside `src/observability/reporter.rs` as a
//! private `SchedulerChannelSource`, reachable only by the heartbeat loop.
//! Five other places in this process dial the scheduler the same way — the
//! paused registry's `central` backend, the snapshot catalog client,
//! scheduler-backed node placement, resume placement, and P2P peer discovery
//! — and none of them could hot-reload; changing their target still meant a
//! restart. This module is that type, promoted and generalized so all six
//! consumers share one implementation and one behavior.
//!
//! # Two lifecycles kept apart
//!
//! - **Construction** ([`SchedulerEndpointSource::spawn`]) can fail: an
//!   invalid *static* endpoint is refused immediately, exactly as every
//!   consumer's own direct `Endpoint::from_shared` call did before this type
//!   existed. What a construction failure *means* — hard-fail the process,
//!   degrade to node-local, degrade to a no-op discovery backend — is each
//!   consumer's own policy, argued for at its own call site (see
//!   `src/api/impls/resume_surface.rs`'s comment on why `from_config` and
//!   `cluster_from_config` disagree, for the sharpest example). This type
//!   does not flatten those differences into one answer.
//! - **Runtime reload** (the background watcher, once running) never fails
//!   outward. A missing, empty, or unparseable file is logged, metered under
//!   `component` (see [`SCHEDULER_ENDPOINT_RELOAD_METRIC`]), and the
//!   previous working channel keeps being handed out. A config-reload
//!   regression here is dangerous enough on its own — see
//!   `src/observability/reporter.rs`'s `against_a_scheduler` test module for
//!   what one looked like on this repository's dev cluster — that this is
//!   covered by dedicated tests below, not just argued in a comment.
//!
//! # No per-call file I/O
//!
//! The file, when configured, is re-read by a background task on a fixed
//! interval — never inside [`current`](SchedulerEndpointSource::current) or
//! [`channel`](SchedulerEndpointSource::channel), which are cheap
//! `tokio::sync::watch` reads safe to call on every request. This matters
//! for the two consumers on a request path (create placement, resume
//! placement): the heartbeat reporter could afford a `fs::metadata` call per
//! five-second tick, but that cost turns into a per-request stall for
//! anything driven by traffic rather than a timer.

use std::path::PathBuf;
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result};
use tokio::sync::watch;
use tonic::transport::{Channel, Endpoint};
use tracing::{error, info, warn};

use crate::cfg::{ClusterConfig, ObservabilitySchedulerReportConfig};

/// Metric name for a watcher's file re-read outcome.
///
/// Carries two labels: `component` (which of this process's scheduler
/// dialers this increment belongs to — `"heartbeat"`, `"p2p_discovery"`,
/// `"paused_registry"`, `"snapshot_catalog"`, `"create_placement"`,
/// `"resume_placement"`) and `result` (`switched` / `unchanged` /
/// `same_value` / `empty_kept_previous` / `invalid_kept_previous` /
/// `error_kept_previous`). Six consumers sharing one counter with no
/// `component` label would erase which of them reloaded; the extra
/// cardinality is six values, which is cheap.
pub const SCHEDULER_ENDPOINT_RELOAD_METRIC: &str = "agentenv_scheduler_endpoint_reload_total";

/// A live `(Channel, endpoint)` pair, optionally kept fresh by a background
/// file watcher, shared cheaply — `Clone` is an `Arc`-refcount bump via the
/// underlying `tokio::sync::watch::Receiver` — by every holder.
///
/// See the module docs for the split between construction and runtime
/// reload failure handling.
#[derive(Clone)]
pub struct SchedulerEndpointSource {
    rx: watch::Receiver<(Channel, String)>,
}

impl SchedulerEndpointSource {
    /// Builds the source from a static endpoint — fails immediately if it
    /// does not parse, unchanged from every consumer's previous direct
    /// `Endpoint::from_shared` call — and, if `file_watch` is `Some((path,
    /// interval))`, spawns a background task that re-reads `path` every
    /// `interval` and republishes a new channel through
    /// [`current`](Self::current) — see the struct's docs for why that
    /// republish can never itself fail outward.
    ///
    /// The task runs for the rest of the process's lifetime; nothing here
    /// stops it, the same as this process's other permanent background
    /// loops (the heartbeat reporter's own send loop, an unshut-down
    /// [`crate::pg::spawn_singleton_task`]). When `file_watch` is `None` —
    /// every deployment that has not opted in — no task is spawned at all,
    /// and this behaves exactly like a plain `Endpoint::connect_lazy` call:
    /// no stat, no re-read, ever.
    ///
    /// `component` labels every metric this source's watcher emits — see
    /// [`SCHEDULER_ENDPOINT_RELOAD_METRIC`].
    ///
    /// Requires a Tokio runtime context when `file_watch` is `Some` (for the
    /// spawned task) — the same requirement `Endpoint::connect_lazy` already
    /// has for every caller of this function today, so this adds no new
    /// constraint.
    pub fn spawn(
        static_endpoint: String,
        file_watch: Option<(PathBuf, Duration)>,
        component: &'static str,
    ) -> Result<Self> {
        // 🔴 `build_channel` is the only place this qualifies its input
        // (see [`qualified`] and `build_channel`'s own doc comment): three of
        // this type's six consumers (`central.rs`'s paused registry, the
        // snapshot catalog client, and the P2P discovery backend) pass their
        // configured endpoint straight through with no scheme handling of
        // their own, and `http::Uri` happily parses a bare `host:port` as an
        // *authority-form* URI (`Endpoint::from_shared` returns `Ok`,
        // `scheme() == None`) — so without that, those three would build a
        // channel that fails every RPC with "invalid URL, scheme is
        // missing" instead of refusing to start. The qualified string comes
        // back out of `build_channel` rather than being recomputed here, so
        // what gets published through `current()` is provably the same
        // string the channel was built from.
        let (channel, static_endpoint) = build_channel(&static_endpoint)?;
        let (tx, rx) = watch::channel((channel, static_endpoint));

        if let Some((path, interval)) = file_watch {
            tokio::spawn(watch_file(path, interval, component, tx));
        }

        Ok(Self { rx })
    }

    /// [`spawn`](Self::spawn), resolving `file_watch` from configuration per
    /// the precedence [`resolve_endpoint_file`] documents, and the watch
    /// interval from
    /// [`ObservabilitySchedulerReportConfig::interval_secs`] — the heartbeat
    /// cadence, reused rather than given every consumer its own timing knob
    /// to configure and reason about.
    pub fn spawn_from_config(
        static_endpoint: String,
        cluster: &ClusterConfig,
        scheduler_report: &ObservabilitySchedulerReportConfig,
        component: &'static str,
    ) -> Result<Self> {
        let file = resolve_endpoint_file(cluster, scheduler_report);
        let interval = Duration::from_secs(scheduler_report.interval_secs.max(1));
        Self::spawn(
            static_endpoint,
            file.map(|path| (path, interval)),
            component,
        )
    }

    /// The channel and endpoint to send the next RPC on. Cheap: an
    /// `Arc`-refcounted channel clone and a `String` clone out of a
    /// `tokio::sync::watch`, safe to call on every request — no file I/O, no
    /// lock held across blocking work.
    pub fn current(&self) -> (Channel, String) {
        self.rx.borrow().clone()
    }

    /// [`current`](Self::current) without the endpoint string, for the
    /// consumers that only ever build a typed client over the channel and
    /// never log or report the endpoint itself.
    pub fn channel(&self) -> Channel {
        self.rx.borrow().0.clone()
    }

    /// A source with no file-driven reload — every call to
    /// [`current`](Self::current) / [`channel`](Self::channel) returns
    /// `channel` forever. For tests that need a hand-built [`Channel`] (an
    /// in-process test server, a broken dial target) without going through
    /// [`spawn`](Self::spawn)'s endpoint-string parsing.
    #[cfg(test)]
    pub(crate) fn fixed(channel: Channel, endpoint: impl Into<String>) -> Self {
        let (_tx, rx) = watch::channel((channel, endpoint.into()));
        Self { rx }
    }
}

/// Prefixes `http://` onto `endpoint` unless it already names a scheme.
///
/// # 🔴 Why this exists
///
/// `http::Uri` — what `tonic::transport::Endpoint::from_shared` parses
/// through — accepts a schemeless `host:port` as a valid *authority-form*
/// URI. `Endpoint::from_shared("scheduler:9090")` therefore returns `Ok`
/// with `scheme() == None`: construction succeeds, `connect_lazy()`
/// succeeds, and the failure only shows up later, on the first RPC, as
/// `transport error: invalid URL, scheme is missing`. A bare `host:port` is
/// exactly what a static discovery list, this source's own hot-reload file,
/// and the scheduler's own `ListNodes`/`Heartbeat` answers commonly carry,
/// so every path that can end up inside [`build_channel`] has to be
/// qualified before it gets there — that function is this type's one choke
/// point, so qualifying inside it (both in [`SchedulerEndpointSource::spawn`]
/// for the static endpoint and in [`check_and_publish`] for a hot-reloaded
/// one) is what makes every consumer, and every reload, behave the same way.
pub fn qualified(endpoint: &str) -> String {
    if endpoint.contains("://") {
        endpoint.to_string()
    } else {
        format!("http://{endpoint}")
    }
}

/// Builds a channel from `endpoint`, qualifying it first if it names no
/// scheme (see [`qualified`]) — the one place, of every caller in this
/// module, that actually invokes [`Endpoint::from_shared`]. Both
/// [`SchedulerEndpointSource::spawn`] (the static endpoint) and
/// [`check_and_publish`] (a hot-reloaded one) route through here rather than
/// qualifying their own input, so a static endpoint and a file-driven reload
/// can never disagree about whether a bare `host:port` dials.
///
/// Returns the qualified endpoint alongside the channel — callers publish
/// and log that string rather than re-deriving it, so what `current()` hands
/// back is provably the same string the channel was actually built from.
fn build_channel(endpoint: &str) -> Result<(Channel, String)> {
    let raw_endpoint = qualified(endpoint);
    let built = Endpoint::from_shared(raw_endpoint.clone())
        .with_context(|| format!("invalid scheduler endpoint: {raw_endpoint}"))?;
    Ok((built.connect_lazy(), raw_endpoint))
}

/// Resolves the hot-reload file location per the `[cluster]` /
/// `[observability.scheduler_report]` precedence: [`ClusterConfig`]'s own
/// `scheduler_endpoint_file` wins when set, falling back to the deprecated
/// [`ObservabilitySchedulerReportConfig::scheduler_endpoint_file`] otherwise.
/// When both are set, the `[cluster]` value wins and this logs a `warn!` —
/// the deployment almost certainly meant to set (or is mid-migration away
/// from) only one of them.
///
/// Blank is the same as absent, for both fields, matching every other
/// optional-endpoint field in this configuration (`scheduler_endpoint`
/// itself, `control_plane_token_file`, ...).
pub fn resolve_endpoint_file(
    cluster: &ClusterConfig,
    scheduler_report: &ObservabilitySchedulerReportConfig,
) -> Option<PathBuf> {
    let primary = non_blank(&cluster.scheduler_endpoint_file);
    let deprecated = non_blank(&scheduler_report.scheduler_endpoint_file);

    match (primary, deprecated) {
        (Some(primary), Some(_deprecated)) => {
            warn!(
                "both [cluster].scheduler_endpoint_file and the deprecated \
                 [observability.scheduler_report].scheduler_endpoint_file are set; using \
                 [cluster].scheduler_endpoint_file and ignoring the deprecated one. Remove the \
                 deprecated field once every deployment has migrated"
            );
            Some(PathBuf::from(primary))
        }
        (Some(primary), None) => Some(PathBuf::from(primary)),
        (None, Some(deprecated)) => Some(PathBuf::from(deprecated)),
        (None, None) => None,
    }
}

fn non_blank(raw: &str) -> Option<&str> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed)
    }
}

fn record_reload_metric(component: &'static str, result: &'static str) {
    metrics::counter!(
        SCHEDULER_ENDPOINT_RELOAD_METRIC,
        "component" => component,
        "result" => result
    )
    .increment(1);
}

/// Local state the background watcher carries between ticks — the file
/// fingerprint that lets an unchanged file skip a re-read, and the endpoint
/// currently published, so a rewrite with identical content can be told
/// apart from an actual change.
struct WatcherState {
    /// Modification time and length of the file content backing `endpoint`.
    /// `None` until the file has been read successfully at least once.
    fingerprint: Option<(SystemTime, u64)>,
    endpoint: String,
}

async fn watch_file(
    path: PathBuf,
    interval: Duration,
    component: &'static str,
    tx: watch::Sender<(Channel, String)>,
) {
    let mut state = WatcherState {
        fingerprint: None,
        endpoint: tx.borrow().1.clone(),
    };

    // Checks immediately, then every `interval` — not interval-first. A
    // freshly started process should not run on a stale static endpoint for
    // a whole interval when the file already names the right target.
    loop {
        check_and_publish(&path, component, &tx, &mut state);
        tokio::time::sleep(interval).await;
    }
}

/// One watcher tick: re-reads `path` if its fingerprint has changed, and
/// publishes a new channel through `tx` on an actual endpoint change.
/// Mirrors `SchedulerChannelSource::current`'s original per-call logic
/// exactly, just restructured to publish instead of return.
fn check_and_publish(
    path: &std::path::Path,
    component: &'static str,
    tx: &watch::Sender<(Channel, String)>,
    state: &mut WatcherState,
) {
    // Skip the read when the file is byte-for-byte the one already held. A
    // watcher tick is rare enough that a stat per tick costs nothing, and
    // this keeps the common case — nobody has touched the file — to exactly
    // that.
    let fingerprint = std::fs::metadata(path)
        .and_then(|meta| Ok((meta.modified()?, meta.len())))
        .ok();
    if let (Some(fingerprint), Some(held)) = (fingerprint, state.fingerprint) {
        if fingerprint == held {
            record_reload_metric(component, "unchanged");
            return;
        }
    }

    match std::fs::read_to_string(path) {
        Ok(contents) => {
            let candidate = contents.trim();
            if candidate.is_empty() {
                warn!(
                    path = %path.display(),
                    component,
                    "scheduler endpoint file is empty; keeping the last endpoint that was read \
                     successfully. An empty file is not a valid target — write a real endpoint \
                     to change it, or stop mounting the file to fall back to the static endpoint \
                     at the next restart"
                );
                record_reload_metric(component, "empty_kept_previous");
                return;
            }

            // `candidate` is whatever the file said, not yet qualified —
            // that happens once, inside `build_channel`, which is why this
            // is built before the same-value comparison below rather than
            // compared to `state.endpoint` directly: `state.endpoint` always
            // holds a *qualified* value (see `build_channel`'s doc comment),
            // and comparing it against a raw `contents.trim()` would make
            // the fast path below never fire for a deployment whose file
            // carries a bare `host:port` — every tick would look like a
            // change and rebuild a channel, even when the file's content
            // never moved.
            match build_channel(candidate) {
                Ok((channel, qualified_candidate)) => {
                    if qualified_candidate == state.endpoint {
                        // Same target, different bytes on disk (e.g. a
                        // rewrite with identical content, or added trailing
                        // whitespace, or a scheme added/removed that
                        // `qualified` normalises away). Remember the new
                        // fingerprint so the next tick takes the fast path
                        // above, and drop the channel `build_channel` just
                        // built — nothing downstream needs a second one.
                        state.fingerprint = fingerprint;
                        record_reload_metric(component, "same_value");
                        return;
                    }

                    info!(
                        component,
                        previous_endpoint = %state.endpoint,
                        new_endpoint = %qualified_candidate,
                        "scheduler endpoint target changed"
                    );
                    state.fingerprint = fingerprint;
                    state.endpoint = qualified_candidate.clone();
                    // 🔴 Ignored on purpose: an `Err` here means every
                    // receiver — every clone this source ever handed out —
                    // has been dropped, which only happens once whatever
                    // owned this source is gone too. The watcher keeps
                    // running rather than trying to detect that and stop;
                    // see the module docs on why nothing here has a
                    // shutdown handle.
                    let _ = tx.send((channel, qualified_candidate));
                    record_reload_metric(component, "switched");
                }
                Err(err) => {
                    // 🔴 Never fail open, and never fail *closed* either — a
                    // malformed edit to the ConfigMap must not stop this
                    // consumer from working. Keep dialling the last endpoint
                    // that parsed, and do not update the fingerprint: an
                    // operator fixing the typo produces a new mtime/len,
                    // which is picked up on the very next tick without
                    // needing this branch to remember anything.
                    error!(
                        path = %path.display(),
                        component,
                        candidate_endpoint = candidate,
                        error = %err,
                        "scheduler endpoint file names an endpoint that cannot be dialled; \
                         keeping the previous target"
                    );
                    record_reload_metric(component, "invalid_kept_previous");
                }
            }
        }
        Err(err) => {
            // 🔴 Same "never fail open" rule as `ControlPlaneGate::file_tokens`
            // and the original `SchedulerChannelSource`: a read error is not
            // evidence the endpoint changed, it is evidence of nothing at
            // all, and treating it as "fall back to the static value" would
            // let a single disk hiccup silently redirect every future call.
            warn!(
                path = %path.display(),
                component,
                error = %err,
                "cannot read the scheduler endpoint file; keeping the last endpoint that was \
                 read successfully"
            );
            record_reload_metric(component, "error_kept_previous");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cluster_config(endpoint_file: &str) -> ClusterConfig {
        ClusterConfig {
            node_placement_source: crate::cfg::NodePlacementSource::Scheduler,
            scheduler_endpoint: None,
            scheduler_endpoint_file: endpoint_file.to_string(),
            node_service_addr: "0.0.0.0:8001".to_string(),
            api_grpc_addr: "0.0.0.0:8002".to_string(),
            node_service_port: 8001,
            kubernetes_discovery: Default::default(),
            native_warmup_timeout_secs: 15,
        }
    }

    fn scheduler_report_config(endpoint_file: &str) -> ObservabilitySchedulerReportConfig {
        ObservabilitySchedulerReportConfig {
            enabled: true,
            interval_secs: 5,
            scheduler_endpoint_file: endpoint_file.to_string(),
            dual_report_api_endpoint: String::new(),
        }
    }

    /// A bare `host:port` is what `[cluster].scheduler_endpoint` and this
    /// source's own hot-reload file usually hold, and `http::Uri` parses it
    /// without complaint as an authority-form URI with no scheme —
    /// `Endpoint::from_shared` never rejects it, so nothing downstream of
    /// `qualified` catches the omission either. See [`qualified`]'s doc
    /// comment for what that costs when it is skipped.
    #[test]
    fn a_scheme_is_added_only_when_one_is_missing() {
        assert_eq!(qualified("scheduler:9090"), "http://scheduler:9090");
        assert_eq!(qualified("http://scheduler:9090"), "http://scheduler:9090");
        assert_eq!(
            qualified("https://scheduler:9090"),
            "https://scheduler:9090",
            "a configured TLS endpoint must not be downgraded to plaintext"
        );
    }

    #[test]
    fn resolve_endpoint_file_is_none_when_neither_is_set() {
        assert_eq!(
            resolve_endpoint_file(&cluster_config(""), &scheduler_report_config("")),
            None
        );
    }

    #[test]
    fn resolve_endpoint_file_treats_blank_as_absent_on_both_sides() {
        assert_eq!(
            resolve_endpoint_file(&cluster_config("   "), &scheduler_report_config("  \t ")),
            None
        );
    }

    #[test]
    fn resolve_endpoint_file_uses_the_cluster_field_alone() {
        assert_eq!(
            resolve_endpoint_file(&cluster_config("  /etc/a  "), &scheduler_report_config("")),
            Some(PathBuf::from("/etc/a"))
        );
    }

    /// The deprecated field must keep working for every deployment that has
    /// not migrated — that is the entire point of keeping it as a fallback
    /// rather than deleting it outright.
    #[test]
    fn resolve_endpoint_file_falls_back_to_the_deprecated_field_alone() {
        assert_eq!(
            resolve_endpoint_file(&cluster_config(""), &scheduler_report_config("  /etc/b  ")),
            Some(PathBuf::from("/etc/b"))
        );
    }

    /// 🔴 The precedence this whole function exists to state: when both are
    /// configured, `[cluster]` wins, never a merge and never the deprecated
    /// one.
    #[test]
    fn resolve_endpoint_file_prefers_the_cluster_field_when_both_are_set() {
        assert_eq!(
            resolve_endpoint_file(
                &cluster_config("/etc/a"),
                &scheduler_report_config("/etc/b")
            ),
            Some(PathBuf::from("/etc/a"))
        );
    }

    /// The behaviour this whole slice exists to add: a source with no file
    /// configured is exactly what every consumer's direct `connect_lazy` call
    /// did before this type existed, forever — no stat, no re-read, no drift
    /// from the static endpoint.
    #[tokio::test]
    async fn no_file_watch_never_changes_the_endpoint() {
        let source =
            SchedulerEndpointSource::spawn("http://scheduler-a:9090".to_string(), None, "test")
                .expect("a valid endpoint builds a source");

        let (_channel, endpoint) = source.current();
        assert_eq!(endpoint, "http://scheduler-a:9090");
        let (_channel, endpoint) = source.current();
        assert_eq!(endpoint, "http://scheduler-a:9090");
    }

    #[tokio::test]
    async fn a_clone_observes_the_same_reloads_as_the_original() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("scheduler-endpoint");
        std::fs::write(&path, "http://scheduler-b:9090\n").expect("seed the file");

        let source = SchedulerEndpointSource::spawn(
            "http://scheduler-a:9090".to_string(),
            Some((path, Duration::from_millis(20))),
            "test",
        )
        .expect("a valid static endpoint builds a source");
        let clone = source.clone();

        wait_for(|| clone.current().1 == "http://scheduler-b:9090").await;
    }

    async fn wait_for(mut condition: impl FnMut() -> bool) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if condition() {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "condition was not met within the deadline"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn adopts_a_new_endpoint_from_the_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("scheduler-endpoint");
        std::fs::write(&path, "http://scheduler-b:9090\n").expect("seed the file");

        let source = SchedulerEndpointSource::spawn(
            "http://scheduler-a:9090".to_string(),
            Some((path, Duration::from_millis(20))),
            "test",
        )
        .expect("a valid static endpoint builds a source");

        wait_for(|| source.current().1 == "http://scheduler-b:9090").await;
    }

    /// 🔴 The regression this guards: `http::Uri` parses a schemeless
    /// `host:port` as a valid *authority-form* URI, so a hot-reload file
    /// naming one used to build fine (`Endpoint::from_shared` returns `Ok`)
    /// and switch the published channel — logging and metering exactly like
    /// a healthy reload — while every RPC on that channel then failed with
    /// "invalid URL, scheme is missing". A file carrying a bare `host:port`
    /// is not a hypothetical: it is what an operator copying the endpoint
    /// out of the *static* `[cluster].scheduler_endpoint` config (which
    /// worked, because its callers used to qualify it themselves before
    /// this type existed) would naturally paste in. This asserts on the
    /// published endpoint *string*, not just that a channel was returned —
    /// a scheme-less channel and a qualified one both come back as `Ok`, so
    /// only the string tells them apart from a test.
    #[tokio::test]
    async fn a_hot_reloaded_bare_host_port_is_qualified_before_it_is_published() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("scheduler-endpoint");
        std::fs::write(&path, "scheduler-b:9090").expect("seed the file with a bare host:port");

        let source = SchedulerEndpointSource::spawn(
            "http://scheduler-a:9090".to_string(),
            Some((path, Duration::from_millis(20))),
            "test",
        )
        .expect("a valid static endpoint builds a source");

        wait_for(|| source.current().1 == "http://scheduler-b:9090").await;
    }

    #[tokio::test]
    async fn falls_back_to_the_static_endpoint_when_the_file_is_missing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("does-not-exist");

        let source = SchedulerEndpointSource::spawn(
            "http://scheduler-a:9090".to_string(),
            Some((path, Duration::from_millis(20))),
            "test",
        )
        .expect("a valid static endpoint builds a source even if the file is absent");

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            source.current().1,
            "http://scheduler-a:9090",
            "a file that was never read successfully must not blank the target"
        );
    }

    #[tokio::test]
    async fn ignores_an_empty_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("scheduler-endpoint");
        std::fs::write(&path, "").expect("write an empty file");

        let source = SchedulerEndpointSource::spawn(
            "http://scheduler-a:9090".to_string(),
            Some((path, Duration::from_millis(20))),
            "test",
        )
        .expect("a valid static endpoint builds a source");

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            source.current().1,
            "http://scheduler-a:9090",
            "an empty file is not a valid target and must not clear the endpoint"
        );
    }

    #[tokio::test]
    async fn ignores_a_file_naming_an_endpoint_that_cannot_be_dialled() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("scheduler-endpoint");
        std::fs::write(&path, "not a valid endpoint\twith control chars").expect("write junk");

        let source = SchedulerEndpointSource::spawn(
            "http://scheduler-a:9090".to_string(),
            Some((path, Duration::from_millis(20))),
            "test",
        )
        .expect("a valid static endpoint builds a source");

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            source.current().1,
            "http://scheduler-a:9090",
            "a candidate that fails to parse must keep the previous, working target"
        );
    }

    #[tokio::test]
    async fn picks_up_a_second_edit_after_the_first() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("scheduler-endpoint");
        std::fs::write(&path, "http://scheduler-b:9090").expect("first write");

        let source = SchedulerEndpointSource::spawn(
            "http://scheduler-a:9090".to_string(),
            Some((path.clone(), Duration::from_millis(20))),
            "test",
        )
        .expect("a valid static endpoint builds a source");
        wait_for(|| source.current().1 == "http://scheduler-b:9090").await;

        // A different length as well as different bytes: the fingerprint
        // this relies on is (mtime, len), and two writes issued back to back
        // on a filesystem with coarse mtime resolution could otherwise
        // collide on both — this makes the length alone enough to tell them
        // apart even if that ever happens.
        std::fs::write(&path, "http://scheduler-charlie:9090").expect("second write");
        wait_for(|| source.current().1 == "http://scheduler-charlie:9090").await;
    }

    /// [`SchedulerEndpointSource::fixed`] exists for tests that need to hand
    /// in an already-built [`Channel`] — an in-process test server, a broken
    /// dial target — bypassing `spawn`'s endpoint-string parsing entirely.
    #[tokio::test]
    async fn fixed_never_changes() {
        let channel = Endpoint::from_shared("http://fixed:9090".to_string())
            .expect("valid endpoint")
            .connect_lazy();
        let source = SchedulerEndpointSource::fixed(channel, "http://fixed:9090");
        assert_eq!(source.current().1, "http://fixed:9090");
        // `channel()` is `current()` without the endpoint string; both must
        // be callable without panicking or ever needing a background task.
        let _channel = source.channel();
        assert_eq!(source.current().1, "http://fixed:9090");
    }
}
