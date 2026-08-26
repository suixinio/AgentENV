//! Task's own "D3": the three arbitration rules a write may run under.
//! Ports `services/scheduler/internal/store.go`'s `arbiter` type and its
//! three implementations (`arbitrateFenced`/`arbitrateObserving`/
//! `arbitrateOff`), and their Lua twins in `redis_store.go`
//! (`redisArbitrationFenced`/`Observing`/`Off`, ported verbatim in
//! `super::redis::scripts`).

/// Which arbitration rule a write runs under. Mirrors
/// `InMemoryArbitrationFor`/`RedisArbitrationFor`'s string-mode mapping:
/// `"off"` -> [`ArbitrationMode::Off`], `"observe"` -> [`ArbitrationMode::Observing`],
/// anything else (including unrecognized) -> [`ArbitrationMode::Fenced`], the
/// safe default.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ArbitrationMode {
    #[default]
    Fenced,
    Observing,
    Off,
}

impl ArbitrationMode {
    pub fn from_str_relaxed(mode: &str) -> Self {
        match mode {
            "off" => ArbitrationMode::Off,
            "observe" => ArbitrationMode::Observing,
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

/// Ports `arbitrateObserving` (`store.go:168-174`): computes the same
/// decision as [`arbitrate_fenced`] but always accepts.
pub fn arbitrate_observing(
    incumbent: &str,
    held: bool,
    challenger: &str,
) -> (bool, BindingDecision) {
    let (_, decision) = arbitrate_fenced(incumbent, held, challenger);
    (true, decision)
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
        ArbitrationMode::Observing => arbitrate_observing(incumbent, held, challenger),
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
    fn observing_always_accepts_but_reports_the_same_decision_fenced_would() {
        assert_eq!(
            arbitrate_observing("exec-2", true, "exec-1"),
            (true, BindingDecision::RejectedOlder)
        );
        assert_eq!(
            arbitrate_observing("exec-1", true, ""),
            (true, BindingDecision::RejectedUnknown)
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
            ArbitrationMode::from_str_relaxed("observe"),
            ArbitrationMode::Observing
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
}
