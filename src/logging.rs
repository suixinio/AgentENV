use std::env;
use std::sync::Once;

use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{fmt, EnvFilter, Layer};

/// The filter used when `RUST_LOG` is unset.
///
/// 🔴 Every entry here is a **target prefix**, and a target that no entry is a
/// prefix of is dropped before it reaches any layer. `tracing`'s macros default
/// a target to `module_path!()`, which begins with the crate name with hyphens
/// turned into underscores — so this list has to name crates, and it goes stale
/// the moment one is renamed. It did: splitting `agentenv` into `aenv-core` /
/// `aenv-node` / `aenv-api` left `agentenv=info` matching nothing but the 51
/// call sites that pass `target: "agentenv"` explicitly, and silently dropped
/// the other ~1100. `default_filter_covers_this_crate` in each of the three
/// crates is the guard; it reads its own `module_path!()` so the next rename
/// turns it red instead of turning the logs off.
///
/// What each entry is for:
/// - `aenv_core`, `aenv_node`, `aenv_api`: the three workspace crates that log.
/// - `agentenv`: the explicit `target: "agentenv"` call sites, and — because
///   the match is a plain `starts_with` — the generated `agentenv_http_server`.
/// - `envd`, `uvm_ublk`: unchanged by the split; `uvm_ublk` also covers
///   `uvm_ublk_daemon` through the same prefix rule.
pub const DEFAULT_FILTER: &str =
    "aenv_core=info,aenv_node=info,aenv_api=info,agentenv=info,envd=info,uvm_ublk=info";

/// The filter this crate shipped before the `agentenv` -> `aenv-*` split.
///
/// Kept only so the guards can assert it no longer covers anything.
#[cfg(any(test, feature = "test-support"))]
pub const PRE_RENAME_FILTER: &str = "agentenv=info,envd=info,uvm_ublk=info";
const LOG_FORMAT_ENV: &str = "AENV_LOG_FORMAT";
const LOG_SPAN_EVENTS_ENV: &str = "AENV_LOG_SPAN_EVENTS";

static INIT_LOGGING: Once = Once::new();

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LogFormat {
    Compact,
    Pretty,
    Json,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SpanEvents {
    Off,
    New,
    Enter,
    Exit,
    Close,
    Active,
    Full,
}

impl LogFormat {
    fn from_env() -> Self {
        let raw = env::var(LOG_FORMAT_ENV).unwrap_or_default();
        Self::parse(&raw)
    }

    fn parse(raw: &str) -> Self {
        match raw.trim().to_ascii_lowercase().as_str() {
            "json" => Self::Json,
            "pretty" => Self::Pretty,
            "compact" | "" => Self::Compact,
            _ => Self::Compact,
        }
    }
}

impl SpanEvents {
    fn from_env() -> Self {
        let raw = env::var(LOG_SPAN_EVENTS_ENV).unwrap_or_default();
        Self::parse(&raw)
    }

    fn parse(raw: &str) -> Self {
        match raw.trim().to_ascii_lowercase().as_str() {
            "off" | "none" | "" => Self::Off,
            "new" => Self::New,
            "enter" => Self::Enter,
            "exit" => Self::Exit,
            "close" => Self::Close,
            "active" => Self::Active,
            "full" => Self::Full,
            _ => Self::Off,
        }
    }

    fn to_fmt_span(self) -> FmtSpan {
        match self {
            Self::Off => FmtSpan::NONE,
            Self::New => FmtSpan::NEW,
            Self::Enter => FmtSpan::ENTER,
            Self::Exit => FmtSpan::EXIT,
            Self::Close => FmtSpan::CLOSE,
            Self::Active => FmtSpan::ACTIVE,
            Self::Full => FmtSpan::FULL,
        }
    }
}

/// Initialize process-wide logging once.
///
/// - Log level filter comes from `RUST_LOG`, or defaults to [`DEFAULT_FILTER`].
/// - Output format comes from `AENV_LOG_FORMAT`: `compact` (default), `pretty`, or `json`.
/// - Span lifecycle events come from `AENV_LOG_SPAN_EVENTS`: `off` (default), `new`, `enter`,
///   `exit`, `close`, `active`, or `full`.
///
/// Repeated calls are no-ops. If another global subscriber has already been installed,
/// the initialization error is ignored.
pub fn init() {
    INIT_LOGGING.call_once(|| {
        let filter =
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(DEFAULT_FILTER));

        let format = LogFormat::from_env();
        let span_events = SpanEvents::from_env().to_fmt_span();
        let base = fmt::layer().with_span_events(span_events);

        let fmt_layer = match format {
            LogFormat::Compact => base.compact().boxed(),
            LogFormat::Pretty => base.pretty().boxed(),
            LogFormat::Json => base.json().boxed(),
        };

        let _ = tracing_subscriber::registry()
            .with(filter)
            .with(fmt_layer)
            .try_init();
    });
}

/// Initialize process-wide logging once for tests.
///
/// Same behavior as [`init`], but writes through the test writer so output is
/// captured by Rust test harness and only shown on failures (unless nocapture).
pub fn init_for_tests() {
    INIT_LOGGING.call_once(|| {
        let filter =
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(DEFAULT_FILTER));

        let format = LogFormat::from_env();
        let span_events = SpanEvents::from_env().to_fmt_span();
        let base = fmt::layer()
            .with_test_writer()
            .with_span_events(span_events);

        let fmt_layer = match format {
            LogFormat::Compact => base.compact().boxed(),
            LogFormat::Pretty => base.pretty().boxed(),
            LogFormat::Json => base.json().boxed(),
        };

        // 🔴 The filter moves onto the printing layer rather than sitting over
        // the whole stack, so that [`capture`] below sits underneath nothing.
        //
        // A callsite's interest is decided once, globally, the first time it is
        // reached, and a callsite that no subscriber wanted is cached as
        // uninteresting for the rest of the process. With the filter over the
        // registry, a `debug!` reached by a test that installed no recorder was
        // cached away, and the next test that did install one never saw its own
        // event. That is a test failing at random depending on the order the
        // harness happened to run in — which is what it did.
        let registry = tracing_subscriber::registry();
        #[cfg(any(test, feature = "test-support"))]
        let registry = registry.with(capture::layer());

        let _ = registry.with(fmt_layer.with_filter(filter)).try_init();
    });
}

/// Collects log events so a test can assert one was emitted.
///
/// Some of what this codebase logs is not decoration: an operator finding the
/// line is the whole mechanism. A claim that cost somebody their last
/// unpublished pause, and a registry that could not say whether a local copy is
/// still valid, are both cases where the code does the right thing silently and
/// the only way to know it happened is the log. Those lines can be deleted by a
/// refactor without any test noticing, which is what this is for.
#[cfg(any(test, feature = "test-support"))]
pub mod capture {
    use std::cell::RefCell;
    use std::sync::{Arc, Mutex};

    use tracing::field::{Field, Visit};
    use tracing::Level;
    use tracing_subscriber::layer::Context;
    use tracing_subscriber::Layer;

    thread_local! {
        /// The recorder collecting this thread's events, if any.
        ///
        /// Per thread rather than per process so tests running in parallel do
        /// not record each other's events. `#[tokio::test]` runs its future on
        /// a current-thread runtime driven by the test's own thread, so awaits
        /// stay inside the scope — a `flavor = "multi_thread"` test would not,
        /// and would record nothing.
        static ACTIVE: RefCell<Option<Recorder>> = const { RefCell::new(None) };
    }

    /// One recorded event: its level and its message.
    #[derive(Clone, Debug)]
    pub struct Recorded {
        pub level: Level,
        pub message: String,
    }

    #[derive(Clone, Default)]
    pub struct Recorder(Arc<Mutex<Vec<Recorded>>>);

    impl Recorder {
        pub fn events(&self) -> Vec<Recorded> {
            self.0.lock().unwrap().clone()
        }

        /// Whether any event at `level` contains `needle`.
        pub fn saw(&self, level: Level, needle: &str) -> bool {
            self.events()
                .iter()
                .any(|event| event.level == level && event.message.contains(needle))
        }

        /// Collects this thread's events until the returned guard is dropped.
        ///
        /// 🔴 The subscriber itself is process-wide and installed once, by
        /// [`super::init_for_tests`]; only the routing is per thread. Installing
        /// a scoped subscriber here instead is the obvious shape and does not
        /// work: whether a callsite is reachable at all is decided once for the
        /// whole process, the first time it is reached, so a test that got
        /// there first without a subscriber would silence the callsite for
        /// everyone afterwards.
        pub fn install(&self) -> Guard {
            super::init_for_tests();
            ACTIVE.with(|active| *active.borrow_mut() = Some(self.clone()));

            Guard
        }
    }

    /// Stops routing this thread's events to whatever installed it.
    pub struct Guard;

    impl Drop for Guard {
        fn drop(&mut self) {
            ACTIVE.with(|active| *active.borrow_mut() = None);
        }
    }

    pub fn layer<S: tracing::Subscriber>() -> impl Layer<S> {
        RecordingLayer
    }

    /// Runs `emit` under a throwaway subscriber gated by `filter`, and returns
    /// the target of every event that survived it.
    ///
    /// 🔴 This is the end-to-end shape on purpose. Asserting that a *string*
    /// contains a crate name proves nothing: the question is whether a real
    /// `info!` written in a real module comes out the other side of a real
    /// [`EnvFilter`], and the two halves that have to agree — the filter and
    /// `module_path!()` — are edited in different files by different people.
    /// So the caller passes a closure that logs from its own crate and never
    /// names a target, and this decides it the way the process does.
    ///
    /// The subscriber is scoped to this call rather than installed globally, so
    /// the same callsite can be run against two different filters. Call it
    /// twice with one shared `emit` function and the callsite is held fixed
    /// across both, which is what rules out "recorded nothing because the
    /// callsite was never interesting" as an explanation for an empty result.
    pub fn targets_passing_filter(filter: &str, emit: impl FnOnce()) -> Vec<String> {
        use tracing_subscriber::layer::SubscriberExt;

        let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::EnvFilter::new(filter))
            .with(TargetLayer(seen.clone()));

        tracing::subscriber::with_default(subscriber, emit);

        let targets = seen.lock().unwrap().clone();
        targets
    }

    struct TargetLayer(Arc<Mutex<Vec<String>>>);

    impl<S: tracing::Subscriber> Layer<S> for TargetLayer {
        fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
            self.0
                .lock()
                .unwrap()
                .push(event.metadata().target().to_string());
        }
    }

    struct RecordingLayer;

    impl<S: tracing::Subscriber> Layer<S> for RecordingLayer {
        fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
            ACTIVE.with(|active| {
                let Some(recorder) = active.borrow().clone() else {
                    return;
                };
                let mut message = MessageVisitor(String::new());
                event.record(&mut message);
                recorder.0.lock().unwrap().push(Recorded {
                    level: *event.metadata().level(),
                    message: message.0,
                });
            });
        }
    }

    /// Flattens every field into one string, so an assertion can look for the
    /// message or for any value that travelled with it.
    struct MessageVisitor(String);

    impl Visit for MessageVisitor {
        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            self.0.push_str(&format!("{}={:?} ", field.name(), value));
        }

        fn record_str(&mut self, field: &Field, value: &str) {
            self.0.push_str(&format!("{}={} ", field.name(), value));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_format_from_env() {
        assert_eq!(LogFormat::parse(""), LogFormat::Compact);
        assert_eq!(LogFormat::parse("compact"), LogFormat::Compact);
        assert_eq!(LogFormat::parse("pretty"), LogFormat::Pretty);
        assert_eq!(LogFormat::parse("json"), LogFormat::Json);
        assert_eq!(LogFormat::parse("unknown-value"), LogFormat::Compact);
    }

    #[test]
    fn parse_span_events_from_env() {
        assert_eq!(SpanEvents::parse(""), SpanEvents::Off);
        assert_eq!(SpanEvents::parse("off"), SpanEvents::Off);
        assert_eq!(SpanEvents::parse("none"), SpanEvents::Off);
        assert_eq!(SpanEvents::parse("new"), SpanEvents::New);
        assert_eq!(SpanEvents::parse("enter"), SpanEvents::Enter);
        assert_eq!(SpanEvents::parse("exit"), SpanEvents::Exit);
        assert_eq!(SpanEvents::parse("close"), SpanEvents::Close);
        assert_eq!(SpanEvents::parse("active"), SpanEvents::Active);
        assert_eq!(SpanEvents::parse("full"), SpanEvents::Full);
        assert_eq!(SpanEvents::parse("unknown-value"), SpanEvents::Off);
    }
}

/// Proves the default filter still lets this crate's own logs out.
///
/// See [`DEFAULT_FILTER`] for what went wrong; `aenv-node` and `aenv-api` each
/// carry the same guard, because the target of a log line is decided by the
/// crate it was written in and neither of them can be checked from here.
#[cfg(test)]
mod default_filter_guard {
    use super::capture::targets_passing_filter;
    use super::{DEFAULT_FILTER, PRE_RENAME_FILTER};

    /// One callsite, reused by both directions below.
    ///
    /// No `target:` — that is the whole point. `tracing` fills it in as
    /// `module_path!()`, so this line's target tracks the crate name and goes
    /// stale in exactly the way the real ~1100 call sites do.
    fn emit_untargeted() {
        tracing::info!("default-filter probe");
    }

    #[test]
    fn default_filter_covers_this_crate() {
        assert_eq!(
            targets_passing_filter(DEFAULT_FILTER, emit_untargeted),
            vec![module_path!().to_string()],
            "{DEFAULT_FILTER} drops this crate's own logs; a crate rename \
             (or a typo) has put it out of step with module_path!()"
        );
    }

    #[test]
    fn the_pre_rename_filter_no_longer_covers_this_crate() {
        // The same callsite as the passing case above, so an empty result here
        // cannot be blamed on the callsite: it is the filter rejecting it.
        assert_eq!(
            targets_passing_filter(PRE_RENAME_FILTER, emit_untargeted),
            Vec::<String>::new(),
            "the pre-split filter matched {}, so this guard cannot tell a \
             stale filter from a current one and is worth nothing",
            module_path!()
        );
    }

    #[test]
    fn default_filter_still_covers_the_explicit_agentenv_target() {
        // 51 call sites across the three crates pass this literal. They are the
        // only reason the node and api Pods logged anything at all while the
        // filter was stale, and dropping `agentenv=` from it would take them
        // out. Hardcoded here because it is hardcoded there too.
        fn emit_legacy() {
            tracing::info!(target: "agentenv", "legacy explicit target");
        }
        assert_eq!(
            targets_passing_filter(DEFAULT_FILTER, emit_legacy),
            vec!["agentenv".to_string()],
        );
    }

    #[test]
    fn default_filter_still_covers_the_generated_server() {
        // `agentenv_http_server` is codegen output (`make agentenv-server`), so
        // there is no `module_path!()` of it to borrow from here. What this
        // pins is the prefix rule that keeps it lit: EnvFilter matches a target
        // with a plain `starts_with`, so `agentenv=` covers `agentenv_*` too.
        // Its 179 `error!` sites were never dark, and must not go dark now.
        fn emit_generated() {
            tracing::error!(target: "agentenv_http_server::server", "probe");
        }
        assert_eq!(
            targets_passing_filter(DEFAULT_FILTER, emit_generated),
            vec!["agentenv_http_server::server".to_string()],
        );
    }

    #[test]
    fn the_prefix_rule_also_keeps_the_ublk_daemon_lit() {
        // Same rule, the other crate that depends on it: `uvm_ublk=` is what
        // covers `uvm_ublk_daemon`'s 63 call sites. Neither crate was renamed,
        // but the coverage is incidental, so say so out loud.
        fn emit_daemon() {
            tracing::info!(target: "uvm_ublk_daemon::device", "probe");
        }
        assert_eq!(
            targets_passing_filter(DEFAULT_FILTER, emit_daemon),
            vec!["uvm_ublk_daemon::device".to_string()],
        );
    }
}
