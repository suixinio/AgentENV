use std::env;
use std::sync::Once;

use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{fmt, EnvFilter, Layer};

const DEFAULT_FILTER: &str = "agentenv=info,envd=info,uvm_ublk=info";
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
/// - Log level filter comes from `RUST_LOG`, or defaults to `agentenv=info,envd=info,uvm_ublk=info`.
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
