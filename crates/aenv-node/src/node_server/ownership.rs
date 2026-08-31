//! Fail-closed control-plane ownership filtering for node reconciliation.
//! Only sandboxes carrying a control-plane marker are reported; uncertain or
//! unmarked sandboxes remain untouched.

use crate::orchestrator::{ControlPlaneConfig, LiveSandbox};

/// A sandbox paired with its verified ownership marker.
pub struct OwnedSandbox<'a> {
    pub sandbox: &'a LiveSandbox,
    pub control_plane_config: &'a ControlPlaneConfig,
}

/// Returns only sandboxes carrying a control-plane ownership marker.
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

    #[test]
    fn a_node_running_nothing_marked_reports_nothing() {
        let sandboxes = vec![live(None), live(None), live(None)];
        assert!(owned_by_control_plane(&sandboxes).is_empty());
    }

    #[test]
    fn an_empty_marker_is_no_marker() {
        let sandboxes = vec![live(Some(b""))];
        assert!(
            sandboxes[0].control_plane_config.is_none(),
            "an empty blob must not become a marker in the first place"
        );
        assert!(owned_by_control_plane(&sandboxes).is_empty());
    }

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
