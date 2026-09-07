//! The directory `aenv-node` and the `aenv-egress` DaemonSet share.
//!
//! Both mount it as the same `hostPath`, and the broker is what prepares it:
//! its init container chowns the directory to the group it runs under before
//! the broker itself binds a socket in it. A node only waits for that to have
//! happened, so neither workload has to reach a machine before the other.

use std::fs;
use std::os::unix::fs::MetadataExt;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::{bail, Result};
use tracing::{debug, info};

/// Where the broker binds when this node has not been told otherwise.
pub const DEFAULT_SOCKET_DIR: &str = "/run/aenv-egress";

/// How long a `local` node waits for the broker to prepare the directory.
/// Long enough for the broker DaemonSet to land on a machine that is coming
/// up with it, short enough that a node configured for a broker that will
/// never arrive fails inside one Pod start.
const WAIT_FOR_DIRECTORY: Duration = Duration::from_secs(60);

const POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Waits until the broker's socket directory is one the broker can bind in.
///
/// Nothing here creates or chowns it. A `local` node that never sees it is a
/// node whose broker never started, and it refuses to come up rather than
/// reporting `local_unreachable` forever with nothing saying why. Every other
/// mode touches no directory at all.
pub async fn wait_until_ready(config: &crate::cfg::AppConfig) -> Result<()> {
    wait_within(config, WAIT_FOR_DIRECTORY, POLL_INTERVAL).await
}

async fn wait_within(
    config: &crate::cfg::AppConfig,
    limit: Duration,
    poll: Duration,
) -> Result<()> {
    if config.egress_broker.mode != crate::cfg::EgressBrokerMode::Local {
        return Ok(());
    }
    let directory = config
        .egress_broker
        .socket_path
        .as_deref()
        .and_then(Path::parent)
        .unwrap_or_else(|| Path::new(DEFAULT_SOCKET_DIR));
    let group = config.egress_broker.socket_group;

    let deadline = Instant::now() + limit;
    loop {
        match bindable_by(directory, group) {
            Ok(()) => {
                info!(
                    directory = %directory.display(),
                    group,
                    "the egress broker socket directory is ready"
                );
                return Ok(());
            }
            Err(why) => {
                if Instant::now() >= deadline {
                    bail!(
                        "[egress_broker].mode = \"local\" but {} is not a directory the broker \
                         can bind in after {}s: {why}. The `aenv-egress` DaemonSet's init \
                         container is what chowns it to gid {group} with mode 0770; check that \
                         it is scheduled on this machine and that both workloads mount the same \
                         hostPath",
                        directory.display(),
                        limit.as_secs()
                    );
                }
                debug!(
                    directory = %directory.display(),
                    reason = %why,
                    "waiting for the egress broker to prepare its socket directory"
                );
                tokio::time::sleep(poll).await;
            }
        }
    }
}

/// Whether a process in `group` could create the socket here. This node runs
/// as root and would be let in whatever the mode says, so the answer is read
/// off the metadata rather than attempted.
fn bindable_by(directory: &Path, group: u32) -> std::result::Result<(), String> {
    let metadata = match fs::metadata(directory) {
        Ok(metadata) => metadata,
        Err(err) => return Err(format!("{err}")),
    };
    if !metadata.is_dir() {
        return Err("not a directory".to_string());
    }
    if metadata.gid() != group {
        return Err(format!("group is {}, want {group}", metadata.gid()));
    }
    let mode = metadata.permissions().mode() & 0o777;
    if mode & 0o030 != 0o030 {
        return Err(format!(
            "mode is {mode:o}, want write and search for the group"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cfg::{AppConfig, EgressBrokerMode};

    fn prepared(mode: u32) -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().expect("a temp dir");
        let mounted = dir.path().join("run/aenv-egress");
        fs::create_dir_all(&mounted).expect("the mount point");
        fs::set_permissions(&mounted, fs::Permissions::from_mode(mode)).expect("the mode");
        (dir, mounted)
    }

    fn own_group(directory: &Path) -> u32 {
        fs::metadata(directory).unwrap().gid()
    }

    fn local_config(socket_path: std::path::PathBuf) -> AppConfig {
        let mut config = AppConfig::default();
        config.egress_broker.mode = EgressBrokerMode::Local;
        config.egress_broker.socket_path = Some(socket_path);
        config
    }

    #[test]
    fn the_default_group_is_the_one_the_broker_daemonset_runs_under() {
        assert_eq!(
            AppConfig::default().egress_broker.socket_group,
            65532,
            "the default has to match runAsGroup in aenv-egress-daemonset.yaml"
        );
    }

    #[test]
    fn a_directory_the_brokers_group_can_bind_in_is_ready() {
        let (_dir, mounted) = prepared(0o770);
        let group = own_group(&mounted);

        assert_eq!(bindable_by(&mounted, group), Ok(()));
    }

    #[test]
    fn kubelets_own_directory_is_not_ready() {
        // `DirectoryOrCreate` leaves root:root 0755. The broker's group has
        // search but no write, so it would fail to bind — which is what this
        // node refuses to start on rather than discovering at first use.
        let (_dir, mounted) = prepared(0o755);
        let group = own_group(&mounted);

        assert!(bindable_by(&mounted, group).is_err());
    }

    #[test]
    fn a_directory_of_another_group_is_not_ready() {
        let (_dir, mounted) = prepared(0o770);
        let group = own_group(&mounted);

        assert!(bindable_by(&mounted, group + 1).is_err());
    }

    #[tokio::test]
    async fn every_mode_but_local_waits_for_nothing() {
        for mode in [EgressBrokerMode::Disabled, EgressBrokerMode::Embedded] {
            let mut config = local_config(std::path::PathBuf::from(
                "/nonexistent/aenv-egress/broker.sock",
            ));
            config.egress_broker.mode = mode;

            wait_until_ready(&config)
                .await
                .expect("a node that is not brokering locally touches no directory");
        }
        assert!(!Path::new("/nonexistent/aenv-egress").exists());
    }

    #[tokio::test]
    async fn a_local_node_whose_broker_never_prepared_the_directory_refuses_to_start() {
        let (_dir, mounted) = prepared(0o755);
        let mut config = local_config(mounted.join("broker.sock"));
        config.egress_broker.socket_group = own_group(&mounted) + 1;

        let refused = wait_within(
            &config,
            Duration::from_millis(30),
            Duration::from_millis(10),
        )
        .await
        .expect_err("a node that cannot reach a broker must fail, not report it unreachable");

        let said = format!("{refused:#}");
        assert!(said.contains("aenv-egress"), "{said}");
        assert!(said.contains(&mounted.display().to_string()), "{said}");
    }

    #[tokio::test]
    async fn a_local_node_starts_once_the_directory_is_bindable() {
        let (_dir, mounted) = prepared(0o770);
        let mut config = local_config(mounted.join("broker.sock"));
        config.egress_broker.socket_group = own_group(&mounted);

        wait_within(
            &config,
            Duration::from_millis(30),
            Duration::from_millis(10),
        )
        .await
        .expect("a prepared directory is what this waits for");
    }
}
