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
    /// The incumbent is a reservation whose holder is still launching.
    RejectedInflight,
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
            BindingDecision::RejectedInflight => "rejected_inflight",
            BindingDecision::NotArbitrated => "",
        }
    }

    pub fn accepted(self) -> bool {
        !matches!(
            self,
            BindingDecision::RejectedOlder
                | BindingDecision::RejectedUnknown
                | BindingDecision::RejectedInflight
        )
    }
}

/// Applies fenced arbitration to normalized incumbent and challenger execution ids.
///
/// `incumbent_launching` is the incumbent being a reservation still inside
/// [`crate::binding_store::LAUNCH_RESERVATION_EXCLUSIVE_TTL`]: a newer
/// incarnation does not supersede one, because superseding it would start a
/// second runtime under an id somebody is already starting.
pub fn arbitrate_fenced(
    incumbent: &str,
    held: bool,
    challenger: &str,
    incumbent_launching: bool,
) -> (bool, BindingDecision) {
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
        if incumbent_launching {
            (false, BindingDecision::RejectedInflight)
        } else {
            (true, BindingDecision::Superseded)
        }
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
            arbitrate_fenced("", false, "exec-1", false),
            (true, BindingDecision::Installed)
        );
        assert_eq!(
            arbitrate_fenced("", false, "", false),
            (true, BindingDecision::InstalledUnknown)
        );
    }

    #[test]
    fn fenced_refuses_unknown_over_a_known_incumbent() {
        assert_eq!(
            arbitrate_fenced("exec-1", true, "", false),
            (false, BindingDecision::RejectedUnknown)
        );
    }

    #[test]
    fn fenced_refreshes_the_same_incarnation() {
        assert_eq!(
            arbitrate_fenced("exec-1", true, "exec-1", false),
            (true, BindingDecision::Refreshed)
        );
    }

    #[test]
    fn fenced_accepts_a_lexicographically_newer_incarnation() {
        assert_eq!(
            arbitrate_fenced("exec-1", true, "exec-2", false),
            (true, BindingDecision::Superseded)
        );
    }

    #[test]
    fn fenced_rejects_an_older_incarnation() {
        assert_eq!(
            arbitrate_fenced("exec-2", true, "exec-1", false),
            (false, BindingDecision::RejectedOlder)
        );
    }

    #[test]
    fn a_newer_incarnation_does_not_supersede_a_reservation_still_launching() {
        assert_eq!(
            arbitrate_fenced("exec-1", true, "exec-2", true),
            (false, BindingDecision::RejectedInflight)
        );
        assert!(!BindingDecision::RejectedInflight.accepted());
    }

    #[test]
    fn the_same_incarnation_still_refreshes_a_reservation_still_launching() {
        assert_eq!(
            arbitrate_fenced("exec-1", true, "exec-1", true),
            (true, BindingDecision::Refreshed),
            "the launch holding the reservation is the one confirming it"
        );
    }

    #[test]
    fn not_arbitrated_still_spells_itself_as_the_empty_string() {
        assert_eq!(BindingDecision::NotArbitrated.as_str(), "");
        assert!(BindingDecision::NotArbitrated.accepted());
    }
}
