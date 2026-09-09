//! Launch reservations: one sandbox id, one launch, in their own key space.
//!
//! A reservation is taken before a node is chosen, so a second launch of the
//! same id learns it is the later one before it spends a placement decision on
//! it. It names no node, which is why it cannot live in the routing record: a
//! routing record without a node is invisible to `parse_record` and to the
//! gateway reading the same keys.

use serde::{Deserialize, Serialize};

/// JSON value stored at `{prefix}:reservation:{sandbox_id}`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReservationRecord {
    pub execution_id: String,
    /// When the launch took the id, in Unix milliseconds. A record without one
    /// is read as older than any window, so a writer that leaves no stamp
    /// cannot hold a sandbox id forever.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reserved_at_ms: Option<i64>,
}

/// What asking to hold a sandbox id for a launch answered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LaunchReservationOutcome {
    /// Nothing held the id, or this launch already did; the caller holds it.
    Claimed,
    /// Another launch holds it and is still inside the exclusivity window. The
    /// caller waits out that launch instead of starting a second one.
    HeldElsewhere { execution_id: String },
    /// The holder's window had run out; its reservation is gone and the caller
    /// holds the id.
    ClaimedFromExpired { execution_id: String },
}

impl LaunchReservationOutcome {
    /// Whether the caller came out of it holding the id.
    pub fn claimed(&self) -> bool {
        !matches!(self, LaunchReservationOutcome::HeldElsewhere { .. })
    }

    /// The launch that held the id, when one did.
    pub fn holder(&self) -> Option<&str> {
        match self {
            LaunchReservationOutcome::Claimed => None,
            LaunchReservationOutcome::HeldElsewhere { execution_id }
            | LaunchReservationOutcome::ClaimedFromExpired { execution_id } => {
                Some(execution_id.as_str())
            }
        }
    }

    /// Metric label; a series identity, not prose.
    pub fn as_str(&self) -> &'static str {
        match self {
            LaunchReservationOutcome::Claimed => "claimed",
            LaunchReservationOutcome::HeldElsewhere { .. } => "held_elsewhere",
            LaunchReservationOutcome::ClaimedFromExpired { .. } => "claimed_from_expired",
        }
    }

    /// Reads back the label both backends return.
    pub fn from_label(label: &str, holder: &str) -> Self {
        match label {
            "held_elsewhere" => LaunchReservationOutcome::HeldElsewhere {
                execution_id: holder.to_string(),
            },
            "claimed_from_expired" => LaunchReservationOutcome::ClaimedFromExpired {
                execution_id: holder.to_string(),
            },
            _ => LaunchReservationOutcome::Claimed,
        }
    }
}

/// Serializes a reservation.
pub fn marshal_reservation(execution_id: &str, reserved_at_ms: i64) -> String {
    let record = ReservationRecord {
        execution_id: execution_id.to_string(),
        reserved_at_ms: Some(reserved_at_ms),
    };
    // This string-and-integer record shape is infallible to serialize.
    serde_json::to_string(&record).expect("ReservationRecord serialization is infallible")
}

/// Parses a reservation, rejecting malformed records and ones naming no launch.
pub fn parse_reservation(raw: &[u8]) -> Option<ReservationRecord> {
    let mut record: ReservationRecord = serde_json::from_slice(raw).ok()?;
    record.execution_id = record.execution_id.trim().to_lowercase();
    if record.execution_id.is_empty() {
        return None;
    }
    Some(record)
}

/// Builds a launch reservation key.
pub fn reservation_key(prefix: &str, sandbox_id: &str) -> String {
    format!("{prefix}:reservation:{sandbox_id}")
}

/// Whether a reservation stamped `reserved_at_ms` still excludes a newer launch
/// at `now_ms`. A reservation with no stamp excludes nobody.
pub fn still_exclusive(reserved_at_ms: Option<i64>, now_ms: i64, window_ms: i64) -> bool {
    reserved_at_ms.is_some_and(|reserved| now_ms.saturating_sub(reserved) < window_ms)
}

/// Decides one reservation attempt against whatever holds the id now.
///
/// First writer wins for the length of the window; there is no lexical order
/// between launches here, because the point of the reservation is that the
/// later one has not chosen a node yet and has nothing to compare.
pub fn arbitrate_reservation(
    incumbent: Option<&ReservationRecord>,
    challenger: &str,
    now_ms: i64,
    window_ms: i64,
) -> LaunchReservationOutcome {
    let Some(incumbent) = incumbent else {
        return LaunchReservationOutcome::Claimed;
    };
    if incumbent.execution_id == challenger {
        return LaunchReservationOutcome::Claimed;
    }
    if still_exclusive(incumbent.reserved_at_ms, now_ms, window_ms) {
        return LaunchReservationOutcome::HeldElsewhere {
            execution_id: incumbent.execution_id.clone(),
        };
    }
    LaunchReservationOutcome::ClaimedFromExpired {
        execution_id: incumbent.execution_id.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn held(execution_id: &str, reserved_at_ms: Option<i64>) -> ReservationRecord {
        ReservationRecord {
            execution_id: execution_id.to_string(),
            reserved_at_ms,
        }
    }

    #[test]
    fn the_key_is_beside_the_routing_key_not_inside_it() {
        assert_eq!(
            reservation_key("agentenv:scheduler:bindings", "sbx-1"),
            "agentenv:scheduler:bindings:reservation:sbx-1"
        );
        assert_ne!(
            reservation_key("agentenv:scheduler:bindings", "sbx-1"),
            crate::binding_store::record::binding_key("agentenv:scheduler:bindings", "sbx-1")
        );
    }

    #[test]
    fn marshal_round_trips_through_parse() {
        let json = marshal_reservation("exec-1", 1_700_000_000_000);
        assert_eq!(
            json,
            r#"{"execution_id":"exec-1","reserved_at_ms":1700000000000}"#
        );
        let record = parse_reservation(json.as_bytes()).expect("parses");
        assert_eq!(record.execution_id, "exec-1");
        assert_eq!(record.reserved_at_ms, Some(1_700_000_000_000));
    }

    #[test]
    fn parse_refuses_a_reservation_naming_no_launch() {
        assert!(parse_reservation(br#"{"execution_id":""}"#).is_none());
        assert!(parse_reservation(br#"{"execution_id":"   "}"#).is_none());
        assert!(parse_reservation(b"not json").is_none());
    }

    #[test]
    fn parse_normalizes_the_execution_id_the_way_the_routing_records_do() {
        let record = parse_reservation(br#"{"execution_id":"  EXEC-1  "}"#).expect("parses");
        assert_eq!(record.execution_id, "exec-1");
        assert_eq!(record.reserved_at_ms, None);
    }

    #[test]
    fn nothing_held_is_claimed() {
        assert_eq!(
            arbitrate_reservation(None, "exec-1", 0, 1_000),
            LaunchReservationOutcome::Claimed
        );
    }

    #[test]
    fn the_holder_reclaiming_its_own_id_is_claimed() {
        assert_eq!(
            arbitrate_reservation(Some(&held("exec-1", Some(0))), "exec-1", 10, 1_000),
            LaunchReservationOutcome::Claimed
        );
    }

    #[test]
    fn a_second_launch_inside_the_window_is_held_elsewhere_and_names_the_holder() {
        let outcome = arbitrate_reservation(Some(&held("exec-1", Some(0))), "exec-2", 999, 1_000);
        assert_eq!(
            outcome,
            LaunchReservationOutcome::HeldElsewhere {
                execution_id: "exec-1".to_string()
            }
        );
        assert!(!outcome.claimed());
        assert_eq!(outcome.holder(), Some("exec-1"));
    }

    #[test]
    fn a_second_launch_past_the_window_takes_the_id_over() {
        let outcome = arbitrate_reservation(Some(&held("exec-1", Some(0))), "exec-2", 1_000, 1_000);
        assert_eq!(
            outcome,
            LaunchReservationOutcome::ClaimedFromExpired {
                execution_id: "exec-1".to_string()
            }
        );
        assert!(outcome.claimed());
    }

    #[test]
    fn an_unstamped_reservation_excludes_nobody() {
        assert!(!still_exclusive(None, 0, 1_000));
        assert_eq!(
            arbitrate_reservation(Some(&held("exec-1", None)), "exec-2", 0, 1_000),
            LaunchReservationOutcome::ClaimedFromExpired {
                execution_id: "exec-1".to_string()
            }
        );
    }

    #[test]
    fn a_stamp_ahead_of_the_clock_still_excludes() {
        assert!(still_exclusive(Some(5_000), 0, 1_000));
    }

    #[test]
    fn labels_round_trip_through_the_backends_wire_form() {
        for outcome in [
            LaunchReservationOutcome::Claimed,
            LaunchReservationOutcome::HeldElsewhere {
                execution_id: "exec-1".to_string(),
            },
            LaunchReservationOutcome::ClaimedFromExpired {
                execution_id: "exec-1".to_string(),
            },
        ] {
            let holder = outcome.holder().unwrap_or_default().to_string();
            assert_eq!(
                LaunchReservationOutcome::from_label(outcome.as_str(), &holder),
                outcome
            );
        }
    }
}
