//! Binding arbitration shared with the scheduler and Redis Lua implementation.

/// Arbitration outcome returned by both backends and used as a metric label.
/// `NotArbitrated` keeps its empty wire spelling.
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

/// Applies fenced arbitration to normalized incumbent and challenger execution ids.
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

    #[test]
    fn not_arbitrated_still_spells_itself_as_the_empty_string() {
        assert_eq!(BindingDecision::NotArbitrated.as_str(), "");
        assert!(BindingDecision::NotArbitrated.accepted());
    }
}
