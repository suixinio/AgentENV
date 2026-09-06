//! The directory `aenv-node` and the `aenv-egress` DaemonSet share.
//!
//! Both mount it as the same `hostPath`. The node runs as root and creates it;
//! the broker runs unprivileged and binds its socket inside it, so the mode
//! has to let the broker's group create a file there. Nothing else on the
//! machine gets a way in — root excepted, which this was never a boundary
//! against.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use anyhow::{Context, Result};
use tracing::{info, warn};

use crate::cfg::{AppConfig, EgressBrokerMode};

/// Owner-and-group access, and nothing for anyone else. The group is the
/// broker's, and it needs write: the socket is a file it creates.
const MODE: u32 = 0o770;

/// Creates the broker's socket directory when this node speaks to a broker
/// over one. A node in any other mode has nothing to prepare.
pub fn prepare(config: &AppConfig) -> Result<()> {
    if config.egress_broker.mode != EgressBrokerMode::Local {
        return Ok(());
    }
    let Some(socket_path) = config.egress_broker.socket_path.as_ref() else {
        return Ok(());
    };
    let Some(directory) = socket_path.parent() else {
        return Ok(());
    };

    fs::create_dir_all(directory)
        .with_context(|| format!("create the egress broker socket directory {directory:?}"))?;
    fs::set_permissions(directory, fs::Permissions::from_mode(MODE))
        .with_context(|| format!("set the mode of {directory:?}"))?;
    set_broker_group(directory);
    info!(
        directory = %directory.display(),
        mode = format_args!("{MODE:o}"),
        "the egress broker socket directory is ready"
    );

    Ok(())
}

/// Hands the directory to the broker's group. A node that cannot — one not
/// running as root — leaves the group alone and warns: the broker then fails
/// to bind, which is the failure an operator can act on.
fn set_broker_group(directory: &Path) {
    let group = broker_group();
    let path = std::ffi::CString::new(directory.as_os_str().as_encoded_bytes())
        .expect("a path holds no interior nul");
    // SAFETY: `path` is a valid nul-terminated C string for the duration of
    // the call, and -1 leaves the owner unchanged.
    let rc = unsafe { libc::chown(path.as_ptr(), u32::MAX, group) };
    if rc != 0 {
        warn!(
            directory = %directory.display(),
            group,
            error = %std::io::Error::last_os_error(),
            "could not give the egress broker's group the socket directory; the broker will \
             fail to bind"
        );
    }
}

/// The gid the broker DaemonSet runs under, matching `runAsGroup` in
/// `deploy/k8s/base/aenv-egress-daemonset.yaml`.
fn broker_group() -> u32 {
    const DEFAULT: u32 = 65532;
    std::env::var("AENV_EGRESS_BROKER_GID")
        .ok()
        .and_then(|raw| raw.trim().parse().ok())
        .unwrap_or(DEFAULT)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_mode_lets_the_brokers_group_create_the_socket_and_nobody_else_in() {
        assert_eq!(
            MODE & 0o070,
            0o070,
            "the broker's group must be able to bind"
        );
        assert_eq!(MODE & 0o007, 0, "no other account reaches the socket");
    }

    #[test]
    fn a_node_that_speaks_to_no_local_broker_prepares_nothing() {
        let mut config = AppConfig::default();
        config.egress_broker.mode = EgressBrokerMode::Disabled;
        config.egress_broker.socket_path = Some(std::path::PathBuf::from(
            "/nonexistent/aenv-egress/broker.sock",
        ));

        prepare(&config).expect("a disabled node touches no directory");
        assert!(!std::path::Path::new("/nonexistent/aenv-egress").exists());
    }

    #[test]
    fn a_local_node_creates_the_directory_its_socket_path_names() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let socket_path = dir.path().join("run/aenv-egress/broker.sock");
        let mut config = AppConfig::default();
        config.egress_broker.mode = EgressBrokerMode::Local;
        config.egress_broker.socket_path = Some(socket_path.clone());

        prepare(&config).expect("the directory is created");

        let created = socket_path.parent().expect("the socket has a directory");
        assert!(created.is_dir());
        assert_eq!(
            fs::metadata(created).unwrap().permissions().mode() & 0o777,
            MODE
        );
    }
}
