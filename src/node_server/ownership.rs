//! Which of the sandboxes a node is running belong to the control plane.
//!
//! # 🔴 Why this is its own file
//!
//! The answer feeds a reconciliation, and the far end of that reconciliation
//! tears sandboxes down. Two failures are available here and they are not
//! symmetric:
//!
//! - reporting a sandbox that is *not* the control plane's — the control plane
//!   finds a sandbox it has no record of, concludes it is an orphan, and
//!   deletes a live VM somebody else owns;
//! - omitting a sandbox that *is* the control plane's — the control plane finds
//!   a record with no sandbox behind it, which it can only resolve by looking
//!   again.
//!
//! So the filter fails closed: anything it is not sure about is left out.

use crate::orchestrator::{ControlPlaneConfig, LiveSandbox};

/// One sandbox that carries an ownership marker, with the marker pulled out of
/// the `Option` so that nothing downstream has to re-check it.
///
/// 🔴 The point of the type. A `Vec<LiveSandbox>` that has "already been
/// filtered" is indistinguishable from one that has not, and the check that
/// gets skipped is the one above.
pub struct OwnedSandbox<'a> {
    pub sandbox: &'a LiveSandbox,
    pub control_plane_config: &'a ControlPlaneConfig,
}

/// The sandboxes in `live` that the control plane owns.
///
/// 🔴 Ownership is read from the marker and from nothing else. Not from which
/// port the create arrived on, not from whether the node has a control plane
/// configured, not from the sandbox having a record at all: every one of those
/// is an inference that is true today and silently stops being true when
/// somebody adds a second caller.
pub fn owned_by_control_plane(live: &[LiveSandbox]) -> Vec<OwnedSandbox<'_>> {
    live.iter()
        .filter_map(|sandbox| {
            sandbox
                .control_plane_config
                .as_ref()
                .map(|control_plane_config| OwnedSandbox {
                    sandbox,
                    control_plane_config,
                })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ExecutionId, SandboxId};

    fn live(marker: Option<&[u8]>) -> LiveSandbox {
        LiveSandbox {
            sandbox_id: SandboxId::new(),
            execution_id: Some(ExecutionId::new()),
            facts_from_handle: true,
            host_interaction_ip: None,
            rootfs_virtual_size: None,
            created_at: None,
            expires_at: None,
            resources: None,
            control_plane_config: marker
                .and_then(|bytes| ControlPlaneConfig::from_bytes(bytes.to_vec())),
        }
    }

    #[test]
    fn only_the_marked_sandboxes_are_reported() {
        let sandboxes = vec![
            live(Some(b"owned-a")),
            live(None),
            live(Some(b"owned-b")),
            live(None),
        ];

        let owned = owned_by_control_plane(&sandboxes);

        assert_eq!(owned.len(), 2);
        assert_eq!(owned[0].sandbox.sandbox_id, sandboxes[0].sandbox_id);
        assert_eq!(owned[1].sandbox.sandbox_id, sandboxes[2].sandbox_id);
        assert_eq!(owned[0].control_plane_config.as_bytes(), b"owned-a");
        assert_eq!(owned[1].control_plane_config.as_bytes(), b"owned-b");
    }

    /// 🔴 The control probe for the test above. If the filter were inverted, or
    /// dropped, or replaced by "report everything with a record", the case
    /// above would still look plausible — a list came back, with things in it.
    /// This one cannot pass unless the predicate is the marker.
    #[test]
    fn a_node_running_nothing_marked_reports_nothing() {
        let sandboxes = vec![live(None), live(None), live(None)];
        assert!(owned_by_control_plane(&sandboxes).is_empty());
    }

    /// A marker that arrived as zero bytes is no marker.
    ///
    /// 🔴 The wire cannot tell an absent `bytes` field from an empty one, so a
    /// control plane that sent nothing and one that sent an empty blob produce
    /// the same record. Both mean "not mine", and this is the direction that
    /// leaves the sandbox alone rather than the one that offers it up for
    /// reconciliation.
    #[test]
    fn an_empty_marker_is_no_marker() {
        let sandboxes = vec![live(Some(b""))];
        assert!(
            sandboxes[0].control_plane_config.is_none(),
            "an empty blob must not become a marker in the first place"
        );
        assert!(owned_by_control_plane(&sandboxes).is_empty());
    }

    /// Order is preserved, and the marker stays attached to its own sandbox.
    ///
    /// 🔴 A filter that returned the right *set* of markers paired with the
    /// wrong sandboxes would satisfy every count-based assertion above, and
    /// would hand the control plane one sandbox's record under another's id.
    #[test]
    fn each_marker_stays_with_its_own_sandbox() {
        let sandboxes = (0..8)
            .map(|index| {
                if index % 3 == 0 {
                    live(None)
                } else {
                    live(Some(format!("marker-{index}").as_bytes()))
                }
            })
            .collect::<Vec<_>>();

        let owned = owned_by_control_plane(&sandboxes);

        assert_eq!(owned.len(), 5);
        for entry in &owned {
            let index = sandboxes
                .iter()
                .position(|candidate| candidate.sandbox_id == entry.sandbox.sandbox_id)
                .expect("a reported sandbox that was not in the input");
            assert_eq!(
                entry.control_plane_config.as_bytes(),
                format!("marker-{index}").as_bytes()
            );
        }
    }
}
