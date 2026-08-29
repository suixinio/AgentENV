//! Task's own "D3": the arbitration rule every write runs under. Ports
//! `services/scheduler/internal/store.go`'s `arbitrateFenced`, and its Lua
//! twin in `redis_store.go` (`redisArbitrationFenced`, ported verbatim in
//! `super::redis::scripts`).
//!
//! 🔴 There is one rule, and there is no switch. Two others existed here and
//! on both Go twins: `Observing`/`"observe"` through the rollout that proved
//! enforcing was safe to turn on, and `Off`/`"off"` as that rollout's
//! rollback target. The rollout finished
//! (`deploy/k8s/base/kustomization.yaml`'s `execution-fencing-config`
//! comment records the cluster reaching `enforce`) and both modes are gone,
//! together with the `ArbitrationMode` enum that selected between them, the
//! `[binding_store].arbitration` config knob
//! (`AENV_BINDING_STORE_ARBITRATION`), and the Redis `ARBITRATION_OFF`
//! prelude. `aenv-api` builds one arbitration and there is no value a
//! deployment can set to get another; a manifest that still names one is
//! simply ignored, and the only value that ever worked in production is
//! what it now gets unconditionally.

/// Mirrors Go's `bindingDecision` enum (`store.go:99-125`) — the label
/// [`super::BindingStore::record`]/[`super::BindingStore::reconcile_node`]
/// hand back for the `agentenv_api_binding_execution_total{decision,source}`
/// metric.
///
/// 🔴 [`BindingDecision::NotArbitrated`] survived the deletion of the
/// arbitration switch, and it is **not** a leftover. It used to have two
/// producers: `arbitrate_off`, which is gone with the mode that selected it,
/// and the empty-sandbox-id no-op at the top of
/// [`super::BindingStore::record`] in *both* backends
/// (`super::in_memory::InMemoryBindingStore::record`,
/// `super::redis::RedisBindingStore::record`) — a write naming no sandbox is
/// dropped before any comparison happens, so there is nothing to report, and
/// that is exactly what this variant says. Its wire spelling stays the empty
/// string: it is the `decision` label on
/// `agentenv_api_binding_execution_total`, so renaming it would silently
/// re-partition an existing time series.
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

    /// 🔴 The empty spelling is a metric contract, not a formatting
    /// choice: `NotArbitrated` is what both backends report for a write that
    /// named no sandbox, and it reaches Prometheus as the `decision` label on
    /// `agentenv_api_binding_execution_total`. Giving it a word would
    /// re-partition an existing series.
    #[test]
    fn not_arbitrated_still_spells_itself_as_the_empty_string() {
        assert_eq!(BindingDecision::NotArbitrated.as_str(), "");
        assert!(BindingDecision::NotArbitrated.accepted());
    }
}
