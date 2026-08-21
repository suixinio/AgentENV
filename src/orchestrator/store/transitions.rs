//! The sandbox state machine's edge set, read out of the orchestrator rather
//! than borrowed from e2b.
//!
//! 🔴 e2b's table is not ours and must not be copied. Two differences decide
//! it: its `Killing` is terminal while ours rolls back to whichever state the
//! sandbox came from, and it has no `Paused` at all — a paused sandbox there
//! leaves the active store entirely and becomes a catalog row.
//!
//! Every edge below was read off an actual call site in
//! `orchestrator/service.rs` (`update_state_if_state` and the state-setting
//! `update_if_state` callbacks), and `allowed_transition_table_matches_call_sites`
//! asserts the table cell by cell so that adding an edge in the state machine
//! without adding it here fails a test rather than a sandbox.

use crate::orchestrator::SandboxState;

/// What completing a transition settles the record on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransitionEffect {
    /// The state was borrowed for the duration of the operation; completing it
    /// hands the sandbox back to the state it came from.
    ///
    /// `Running -> Snapshotting` and `Running -> Forking` are the two.
    Transient,
    /// Completing it settles the record on this state.
    ///
    /// 🔴 `Running -> Pausing` settles on `Paused` and **does not** pull
    /// `expires_at` forward to now. e2b's terminal transitions do exactly that,
    /// because there "removing" a sandbox ends its life. Ours does not: a
    /// paused sandbox can be resumed, and `expires_at` still drives
    /// `timeout_action`. Copying that branch would drop every freshly paused
    /// sandbox into the evictor's reach, and delete the ones whose
    /// `timeout_action` is `Delete` — moments after the user paused them.
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

/// `ALLOWED[from][to]`.
///
/// Column order matches [`index`]: Creating, Resuming, Running, Snapshotting,
/// Forking, Pausing, Paused, Killing.
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

/// The token a state is written as inside a stored record.
///
/// 🔴 Not [`Display`][std::fmt::Display], and the difference is not cosmetic.
/// `Display` renders `SandboxState::Running` as `running`; serde renders it as
/// `Running`, and it is serde's form that ends up in the JSON a Lua predicate
/// compares against. Using `Display` here made every transition script report
/// `state_conflict` with `expected: [Running], actual: Running` — a message
/// that describes a contradiction, because the two `Running`s were different
/// strings. `state_token_matches_the_stored_form` keeps the two in step.
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

/// Whether the state machine has an edge from `from` to `to`.
pub fn is_allowed_transition(from: SandboxState, to: SandboxState) -> bool {
    ALLOWED[index(from)][index(to)]
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every state, in the table's column order.
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

    /// The table, cell by cell, against the call sites it was read from.
    ///
    /// Written as an explicit list of the true cells rather than as a second
    /// copy of the array, so that a mistake has to be made twice in the same
    /// direction to pass.
    #[test]
    fn allowed_transition_table_matches_call_sites() {
        use SandboxState::*;
        // (from, to, why)
        let edges: &[(SandboxState, SandboxState, &str)] = &[
            // `create_sandbox`'s finishing update.
            (Creating, Running, "create finishes"),
            // `resume_sandbox`'s finishing update, and its two rollbacks.
            (Resuming, Running, "resume finishes"),
            (Resuming, Paused, "resume rolls back"),
            // Everything a running sandbox can be asked to do.
            (Running, Resuming, "resume of an already-running sandbox"),
            (Running, Snapshotting, "snapshot starts"),
            (Running, Forking, "fork starts"),
            (Running, Pausing, "pause starts"),
            (Running, Killing, "delete starts"),
            // Transient states hand the sandbox back.
            (Snapshotting, Running, "snapshot finishes or rolls back"),
            (Forking, Running, "fork finishes or rolls back"),
            // Pause either settles or rolls back.
            (Pausing, Running, "pause rolls back"),
            (Pausing, Paused, "pause finishes"),
            // A paused sandbox can be resumed or deleted.
            (Paused, Resuming, "resume starts"),
            (Paused, Killing, "delete starts"),
            // 🔴 Two exits, not zero: a failed delete restores whichever state
            // the sandbox was in before it. e2b's `Killing` is terminal.
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

    /// 🔴 `Creating` is produced by `add` and by nothing else. A transition
    /// into it would mean some path can rewind a sandbox to before it existed.
    #[test]
    fn nothing_transitions_into_creating() {
        for from in STATES {
            assert!(!is_allowed_transition(from, SandboxState::Creating));
        }
    }

    /// No state transitions to itself: a transition is a change.
    /// 🔴 The guard on the token above. If serde's representation of
    /// `SandboxState` ever changes — a `rename_all`, a different repr — every
    /// Lua state predicate silently stops matching, and the symptom is a
    /// conflict message in which the expected and actual states read the same.
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
    fn no_self_edges() {
        for state in STATES {
            assert!(!is_allowed_transition(state, state));
        }
    }

    /// 🔴 `Creating -> Killing` is deliberately absent even though a delete
    /// arriving during creation eventually succeeds. It succeeds by waiting for
    /// `Creating` to end and then compare-and-setting from `Running`, not by
    /// stepping straight out of `Creating`. Adding the edge here would let a
    /// delete tear down a half-built VM.
    #[test]
    fn creating_has_no_direct_edge_to_killing() {
        assert!(!is_allowed_transition(
            SandboxState::Creating,
            SandboxState::Killing
        ));
    }
}
