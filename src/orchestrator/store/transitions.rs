//! Sandbox state-machine edges mirrored from orchestrator call sites.

use crate::orchestrator::SandboxState;

/// Record settlement after transition completion.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransitionEffect {
    /// Returns to the source state after the operation.
    Transient,
    /// Settles on the supplied state.
    Terminal(SandboxState),
    /// Completing it removes the record.
    Removal,
}

const fn index(state: SandboxState) -> usize {
    match state {
        SandboxState::Creating => 0,
        SandboxState::Resuming => 1,
        SandboxState::Running => 2,
        SandboxState::Snapshotting => 3,
        SandboxState::Forking => 4,
        SandboxState::Pausing => 5,
        SandboxState::Paused => 6,
        SandboxState::Killing => 7,
    }
}

// ALLOWED[from][to], ordered as in `index`.
#[rustfmt::skip]
const ALLOWED: [[bool; 8]; 8] = [
    //          Crea   Resu   Runn   Snap   Fork   Paus   Pausd  Kill
    /* Crea  */ [false, false, true,  false, false, false, false, false],
    /* Resu  */ [false, false, true,  false, false, false, true,  false],
    /* Runn  */ [false, true,  false, true,  true,  true,  false, true ],
    /* Snap  */ [false, false, true,  false, false, false, false, false],
    /* Fork  */ [false, false, true,  false, false, false, false, false],
    /* Paus  */ [false, false, true,  false, false, false, true,  false],
    /* Pausd */ [false, true,  false, false, false, false, false, true ],
    /* Kill  */ [false, false, true,  false, false, false, true,  false],
];

/// Stored serde token used by Lua predicates.
pub fn state_token(state: SandboxState) -> &'static str {
    match state {
        SandboxState::Creating => "Creating",
        SandboxState::Resuming => "Resuming",
        SandboxState::Running => "Running",
        SandboxState::Snapshotting => "Snapshotting",
        SandboxState::Forking => "Forking",
        SandboxState::Pausing => "Pausing",
        SandboxState::Paused => "Paused",
        SandboxState::Killing => "Killing",
    }
}

/// Decodes a stored state token.
pub fn state_from_token(token: &str) -> Option<SandboxState> {
    Some(match token {
        "Creating" => SandboxState::Creating,
        "Resuming" => SandboxState::Resuming,
        "Running" => SandboxState::Running,
        "Snapshotting" => SandboxState::Snapshotting,
        "Forking" => SandboxState::Forking,
        "Pausing" => SandboxState::Pausing,
        "Paused" => SandboxState::Paused,
        "Killing" => SandboxState::Killing,
        _ => return None,
    })
}

/// Whether the state machine has an edge from `from` to `to`.
pub fn is_allowed_transition(from: SandboxState, to: SandboxState) -> bool {
    ALLOWED[index(from)][index(to)]
}

#[cfg(test)]
mod tests {
    use super::*;

    const STATES: [SandboxState; 8] = [
        SandboxState::Creating,
        SandboxState::Resuming,
        SandboxState::Running,
        SandboxState::Snapshotting,
        SandboxState::Forking,
        SandboxState::Pausing,
        SandboxState::Paused,
        SandboxState::Killing,
    ];

    #[test]
    fn allowed_transition_table_matches_call_sites() {
        use SandboxState::*;
        let edges: &[(SandboxState, SandboxState, &str)] = &[
            (Creating, Running, "create finishes"),
            (Resuming, Running, "resume finishes"),
            (Resuming, Paused, "resume rolls back"),
            (Running, Resuming, "resume of an already-running sandbox"),
            (Running, Snapshotting, "snapshot starts"),
            (Running, Forking, "fork starts"),
            (Running, Pausing, "pause starts"),
            (Running, Killing, "delete starts"),
            (Snapshotting, Running, "snapshot finishes or rolls back"),
            (Forking, Running, "fork finishes or rolls back"),
            (Pausing, Running, "pause rolls back"),
            (Pausing, Paused, "pause finishes"),
            (Paused, Resuming, "resume starts"),
            (Paused, Killing, "delete starts"),
            (Killing, Running, "delete rolls back to a running sandbox"),
            (Killing, Paused, "delete rolls back to a paused sandbox"),
        ];

        for from in STATES {
            for to in STATES {
                let expected = edges.iter().any(|(f, t, _)| *f == from && *t == to);
                assert_eq!(
                    is_allowed_transition(from, to),
                    expected,
                    "edge {from} -> {to}: table says {}, call sites say {expected}",
                    is_allowed_transition(from, to)
                );
            }
        }
    }

    #[test]
    fn nothing_transitions_into_creating() {
        for from in STATES {
            assert!(!is_allowed_transition(from, SandboxState::Creating));
        }
    }

    #[test]
    fn state_token_matches_the_stored_form() {
        for state in STATES {
            assert_eq!(
                serde_json::to_value(state).unwrap(),
                serde_json::Value::String(state_token(state).to_string()),
                "the stored form of {state} is not what the scripts compare against"
            );
        }
    }

    #[test]
    fn every_state_survives_its_token() {
        for state in STATES {
            assert_eq!(state_from_token(state_token(state)), Some(state));
        }
        assert_eq!(state_from_token("running"), None);
        assert_eq!(state_from_token(""), None);
    }

    #[test]
    fn no_self_edges() {
        for state in STATES {
            assert!(!is_allowed_transition(state, state));
        }
    }

    #[test]
    fn creating_has_no_direct_edge_to_killing() {
        assert!(!is_allowed_transition(
            SandboxState::Creating,
            SandboxState::Killing
        ));
    }
}
