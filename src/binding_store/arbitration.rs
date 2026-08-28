//! Task's own "D3": the two arbitration rules a write may run under. Ports
//! `services/scheduler/internal/store.go`'s `arbiter` type and its two
//! remaining implementations (`arbitrateFenced`/`arbitrateOff`), and their
//! Lua twins in `redis_store.go` (`redisArbitrationFenced`/`Off`, ported
//! verbatim in `super::redis::scripts`).
//!
//! 🔴 A third rule, `Observing`/`"observe"`, existed here and on both Go
//! twins through the rollout that proved enforcing was safe to turn on. That
//! rollout finished (`deploy/k8s/base/kustomization.yaml`'s
//! `execution-fencing-config` comment records the cluster reaching `enforce`)
//! and the mode was deleted from all three implementations together. A caller
//! that still passes the literal string `"observe"` is refused at config load
//! (`crate::cfg::AppConfig::validate`), not silently downgraded — see that
//! function's own doc comment.

/// Which arbitration rule a write runs under. Mirrors
/// `InMemoryArbitrationFor`/`RedisArbitrationFor`'s string-mode mapping:
/// `"off"` -> [`ArbitrationMode::Off`], anything else (including
/// unrecognized) -> [`ArbitrationMode::Fenced`], the safe default. The
/// literal string `"observe"` is refused earlier, at config validation, so it
/// never reaches this function in a process that loaded its config normally
/// — see [`crate::cfg::AppConfig::validate`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ArbitrationMode {
    #[default]
    Fenced,
    Off,
}

impl ArbitrationMode {
    pub fn from_str_relaxed(mode: &str) -> Self {
        match mode {
            "off" => ArbitrationMode::Off,
            _ => ArbitrationMode::Fenced,
        }
    }
}

/// Mirrors Go's `bindingDecision` enum (`store.go:99-125`) — the label
/// [`super::BindingStore::record`]/[`super::BindingStore::reconcile_node`]
/// hand back for the `agentenv_api_binding_execution_total{decision,source}`
/// metric. `NotArbitrated` is arbitration-off's answer ("" in Go): nothing
/// was compared, so nothing was decided.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BindingDecision {
    Installed,
    InstalledUnknown,
    Refreshed,
    Superseded,
    RejectedOlder,
    RejectedUnknown,
    NotArbitrated,
}

impl BindingDecision {
    pub fn as_str(self) -> &'static str {
        match self {
            BindingDecision::Installed => "installed",
            BindingDecision::InstalledUnknown => "installed_unknown",
            BindingDecision::Refreshed => "refreshed",
            BindingDecision::Superseded => "superseded",
            BindingDecision::RejectedOlder => "rejected_older",
            BindingDecision::RejectedUnknown => "rejected_unknown",
            BindingDecision::NotArbitrated => "",
        }
    }

    pub fn accepted(self) -> bool {
        !matches!(
            self,
            BindingDecision::RejectedOlder | BindingDecision::RejectedUnknown
        )
    }
}

/// Ports `arbitrateFenced` (`store.go:145-166`) verbatim. `incumbent` is the
/// held record's execution id (only meaningful when `held`); `challenger` is
/// the new write's execution id, already normalized (empty means unknown).
pub fn arbitrate_fenced(incumbent: &str, held: bool, challenger: &str) -> (bool, BindingDecision) {
    if !held || incumbent.is_empty() {
        return if challenger.is_empty() {
            (true, BindingDecision::InstalledUnknown)
        } else {
            (true, BindingDecision::Installed)
        };
    }
    if challenger.is_empty() {
        return (false, BindingDecision::RejectedUnknown);
    }
    if challenger == incumbent {
        (true, BindingDecision::Refreshed)
    } else if challenger > incumbent {
        (true, BindingDecision::Superseded)
    } else {
        (false, BindingDecision::RejectedOlder)
    }
}

/// Ports `arbitrateOff` (`store.go:176-180`): always accepts, reports no
/// decision.
pub fn arbitrate_off(_incumbent: &str, _held: bool, _challenger: &str) -> (bool, BindingDecision) {
    (true, BindingDecision::NotArbitrated)
}

/// Dispatches to the rule [`ArbitrationMode`] selects.
pub fn arbitrate(
    mode: ArbitrationMode,
    incumbent: &str,
    held: bool,
    challenger: &str,
) -> (bool, BindingDecision) {
    match mode {
        ArbitrationMode::Fenced => arbitrate_fenced(incumbent, held, challenger),
        ArbitrationMode::Off => arbitrate_off(incumbent, held, challenger),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fenced_installs_over_nothing_held() {
        assert_eq!(
            arbitrate_fenced("", false, "exec-1"),
            (true, BindingDecision::Installed)
        );
        assert_eq!(
            arbitrate_fenced("", false, ""),
            (true, BindingDecision::InstalledUnknown)
        );
    }

    #[test]
    fn fenced_refuses_unknown_over_a_known_incumbent() {
        assert_eq!(
            arbitrate_fenced("exec-1", true, ""),
            (false, BindingDecision::RejectedUnknown)
        );
    }

    #[test]
    fn fenced_refreshes_the_same_incarnation() {
        assert_eq!(
            arbitrate_fenced("exec-1", true, "exec-1"),
            (true, BindingDecision::Refreshed)
        );
    }

    #[test]
    fn fenced_accepts_a_lexicographically_newer_incarnation() {
        assert_eq!(
            arbitrate_fenced("exec-1", true, "exec-2"),
            (true, BindingDecision::Superseded)
        );
    }

    #[test]
    fn fenced_rejects_an_older_incarnation() {
        assert_eq!(
            arbitrate_fenced("exec-2", true, "exec-1"),
            (false, BindingDecision::RejectedOlder)
        );
    }

    #[test]
    fn off_always_accepts_and_reports_nothing() {
        assert_eq!(
            arbitrate_off("exec-2", true, "exec-1"),
            (true, BindingDecision::NotArbitrated)
        );
        assert_eq!(BindingDecision::NotArbitrated.as_str(), "");
    }

    #[test]
    fn mode_from_str_defaults_to_fenced() {
        assert_eq!(
            ArbitrationMode::from_str_relaxed("off"),
            ArbitrationMode::Off
        );
        assert_eq!(
            ArbitrationMode::from_str_relaxed("anything-else"),
            ArbitrationMode::Fenced
        );
        assert_eq!(
            ArbitrationMode::from_str_relaxed(""),
            ArbitrationMode::Fenced
        );
    }

    /// 🔴 The removed mode's own regression guard: `from_str_relaxed` no
    /// longer recognizes `"observe"` and folds it into the same safe-default
    /// bucket as any other unrecognized string. This function alone cannot
    /// enforce "explicit error" — it has no `Result` to return — so the real
    /// guard is `crate::cfg::AppConfig::validate`'s dedicated refusal; this
    /// test only pins that this lower-level function stopped granting
    /// `"observe"` special recognition, so nobody re-adds the arm here
    /// without also reading why it moved.
    #[test]
    fn from_str_relaxed_no_longer_recognizes_observe() {
        assert_eq!(
            ArbitrationMode::from_str_relaxed("observe"),
            ArbitrationMode::Fenced
        );
    }
}
