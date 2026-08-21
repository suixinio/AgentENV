//! Which half of the split control plane this process runs.
//!
//! One binary, three roles. `all` is what has always run: a single process that
//! both decides what should happen to a sandbox and runs the VM it happens to.
//! The split pulls those apart — `api` decides, `node` runs — and this enum is
//! the switch that says which half a given process is.
//!
//! 🔴 **`all` is the rollback target, so it is defined as *today's behaviour*,
//! not as the union of the other two.** Anything that reads "while we are here,
//! turn it on for `all` as well" takes the rollback away, because the thing
//! being rolled back to would no longer be the thing that was running before.

use anyhow::{bail, Result};
use clap::ValueEnum;

/// The name of the environment variable that selects the role when `--role` is
/// not passed.
///
/// 🔴 Deliberately not a config-file key. `deploy/k8s/run.sh` overwrites the
/// deployed `config/default.toml` from the repository on every apply, so a role
/// written into the toml is a role that silently reverts. Deployments can set
/// args and environment; those are the two places this may live.
pub const ROLE_ENV_VAR: &str = "AENV_ROLE";

/// Which half of the split this process runs.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, ValueEnum)]
pub enum ServerRole {
    /// The deciding half: user-facing REST, sandbox ownership, placement.
    ///
    /// Runs on an ordinary Deployment with no `/dev/kvm`, no ublk and no
    /// `CAP_*`, and must therefore never construct anything that touches them.
    Api,
    /// The running half: Firecracker, netns, ublk, and the sandbox handles.
    Node,
    /// Both halves in one process. The default, and today's behaviour verbatim.
    #[default]
    All,
}

impl ServerRole {
    /// The spelling this role is written as on a command line or in
    /// [`ROLE_ENV_VAR`].
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Api => "api",
            Self::Node => "node",
            Self::All => "all",
        }
    }

    /// Resolves the role from an explicit `--role` flag, falling back to
    /// [`ROLE_ENV_VAR`] and then to the default.
    ///
    /// An explicit flag wins over the environment; an environment value that is
    /// not a role is an error rather than a silent fall back to the default,
    /// because the default is the *other* deployment shape and starting the
    /// wrong half of the split quietly is worse than not starting.
    pub fn resolve(flag: Option<Self>) -> Result<Self> {
        if let Some(role) = flag {
            return Ok(role);
        }
        Self::from_env(std::env::var(ROLE_ENV_VAR).ok().as_deref())
    }

    /// The environment half of [`resolve`](Self::resolve), split out so it can
    /// be tested without touching the process environment.
    pub fn from_env(value: Option<&str>) -> Result<Self> {
        let Some(raw) = value else {
            return Ok(Self::default());
        };
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Ok(Self::default());
        }
        Self::from_str(trimmed, true).map_err(|_| {
            anyhow::anyhow!("{ROLE_ENV_VAR}={raw:?} is not a role; expected one of api, node, all")
        })
    }

    /// Whether this role brings up the machine-local sandbox runtime:
    /// capabilities, dependency provisioning, P2P, ublk, the Firecracker pool
    /// and the Firecracker backend factory.
    ///
    /// 🔴 The `api` Pod has none of the privileges any of that needs, so this
    /// gate is what keeps it from failing to start rather than failing to work.
    pub fn runs_sandbox_runtime(self) -> bool {
        matches!(self, Self::Node | Self::All)
    }

    /// Whether this role arbitrates who owns a paused sandbox: the cluster
    /// paused registry, the wiring around it, and the periodic upkeep that
    /// renews leases and reconciles local records against the cluster.
    ///
    /// That is a decision, and decisions belong to the deciding half. A `node`
    /// still pauses sandboxes and still writes their bytes; what it does not do
    /// is claim, renew or discard the cluster's record of them.
    pub fn arbitrates_paused_sandbox_ownership(self) -> bool {
        matches!(self, Self::Api | Self::All)
    }

    /// Whether this role answers the user-facing REST surface: the
    /// `sandboxes`, `snapshots` and `templates` route groups.
    ///
    /// 🔴 A `node` does not. It runs VMs; deciding that a sandbox should exist,
    /// be paused, or be thrown away is the other half's job, and a node that
    /// kept answering those routes while the API half believed it owned the
    /// same sandboxes would be a second ledger for one set of machines. The
    /// routes are still compiled in and still attached — see
    /// `crate::api::role_gate` for why refusing them is a layer rather than a
    /// shorter route table — they are simply answered with 404.
    pub fn serves_user_facing_rest(self) -> bool {
        matches!(self, Self::Api | Self::All)
    }

    /// Whether this role sweeps the host for what a previous process on this
    /// machine left behind: leftover Firecracker VMMs, their work directories,
    /// their serial logs.
    ///
    /// 🔴 `node` only, and this is a safety property rather than a preference.
    /// The sweep is sound because of *when* it runs: before the listener opens,
    /// on the premise that the previous process on this machine is gone, so
    /// nothing on the host is this process's yet. `all` is what runs on a
    /// developer's machine, where two servers sharing one host is ordinary and
    /// that premise is simply false — a sweep there would tear down the other
    /// one's VMs. `[orchestrator].startup_reclaim_enabled` can override this
    /// either way; this is what it defaults to.
    pub fn reclaims_host_leftovers_at_startup(self) -> bool {
        matches!(self, Self::Node)
    }

    /// Whether this role sends heartbeats to the scheduler.
    ///
    /// A heartbeat reports a *machine* — its CPU, its memory, the sandboxes on
    /// it. An `api` replica is not a machine in that sense and reporting itself
    /// as one would put a node in the scheduler's table that can never run
    /// anything.
    pub fn sends_heartbeats(self) -> bool {
        matches!(self, Self::Node | Self::All)
    }

    /// Whether this role takes itself out of scheduling rotation on shutdown
    /// and waits for the scheduler to notice.
    ///
    /// Only a role that can be scheduled onto has anything to withdraw.
    pub fn drains_on_shutdown(self) -> bool {
        matches!(self, Self::Node | Self::All)
    }

    /// Rejects role/flag combinations that cannot mean anything.
    ///
    /// `--setup-only` provisions the downloaded runtime assets (Firecracker,
    /// kernel, tools drive, OverlayBD) and `--setup-host` provisions machine
    /// -wide KVM, ublk and network prerequisites. The `api` half uses none of
    /// them, so asking it to do either is asking for a host to be prepared by a
    /// process that will never use the preparation — most likely a deployment
    /// that meant to run the setup on the node image and pointed it at the
    /// wrong workload.
    pub fn check_setup_flags(self, setup_only: bool, setup_host: bool) -> Result<()> {
        if self != Self::Api {
            return Ok(());
        }
        let flag = if setup_host {
            "--setup-host"
        } else if setup_only {
            "--setup-only"
        } else {
            return Ok(());
        };
        bail!(
            "--role api does not support {flag}: the API half has no /dev/kvm, no ublk and no \
             downloaded runtime assets, so there is nothing for it to provision. Run {flag} as \
             --role node (or --role all)"
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_default_role_is_the_one_that_runs_everything() {
        // 🔴 The rollback depends on this: an operator who passes no --role at
        // all gets the process that was running before the split existed.
        assert_eq!(ServerRole::default(), ServerRole::All);
        assert_eq!(ServerRole::from_env(None).unwrap(), ServerRole::All);
        assert_eq!(
            ServerRole::resolve(Some(ServerRole::Node)).unwrap(),
            ServerRole::Node
        );
    }

    #[test]
    fn the_flag_wins_over_the_environment() {
        // `resolve` never reads the environment when the flag is present, which
        // is what lets an operator override a Deployment's env from a shell.
        assert_eq!(
            ServerRole::resolve(Some(ServerRole::Api)).unwrap(),
            ServerRole::Api
        );
        assert_eq!(
            ServerRole::resolve(Some(ServerRole::All)).unwrap(),
            ServerRole::All
        );
    }

    #[test]
    fn the_environment_spells_roles_the_same_way_the_flag_does() {
        for (raw, expected) in [
            ("api", ServerRole::Api),
            ("node", ServerRole::Node),
            ("all", ServerRole::All),
            ("API", ServerRole::Api),
            ("  node  ", ServerRole::Node),
        ] {
            assert_eq!(
                ServerRole::from_env(Some(raw)).unwrap(),
                expected,
                "{raw:?} should parse as {expected:?}"
            );
        }
    }

    #[test]
    fn an_unset_or_blank_environment_is_the_default_and_a_wrong_one_is_an_error() {
        assert_eq!(ServerRole::from_env(Some("")).unwrap(), ServerRole::All);
        assert_eq!(ServerRole::from_env(Some("   ")).unwrap(), ServerRole::All);

        // 🔴 Not a fall back to `all`. A deployment that means to run the API
        // half and misspells it would otherwise come up as a node, on a Pod
        // with none of a node's privileges, and the first thing anyone would
        // see is a capability error rather than a typo.
        let err = ServerRole::from_env(Some("apo")).unwrap_err().to_string();
        assert!(err.contains("apo"), "{err}");
        assert!(err.contains("api, node, all"), "{err}");
    }

    #[test]
    fn every_role_spells_itself_back_to_what_parses_as_it() {
        for role in [ServerRole::Api, ServerRole::Node, ServerRole::All] {
            assert_eq!(ServerRole::from_env(Some(role.as_str())).unwrap(), role);
        }
    }

    #[test]
    fn all_keeps_every_capability_the_single_process_had() {
        // The rollback claim, as a test: `all` must answer yes to every gate,
        // because every gate exists to take something away from one of the
        // other two roles.
        let all = ServerRole::All;
        assert!(all.runs_sandbox_runtime());
        assert!(all.arbitrates_paused_sandbox_ownership());
        assert!(all.sends_heartbeats());
        assert!(all.drains_on_shutdown());
        assert!(all.serves_user_facing_rest());
        // 🔴 The one gate `all` answers *no* to, and it is not a capability
        // being taken away — it is a behaviour `all` never had. Sweeping the
        // host at startup is new, and turning it on for the rollback target
        // would mean the thing being rolled back to is not the thing that was
        // running before.
        assert!(!all.reclaims_host_leftovers_at_startup());
        assert!(all.check_setup_flags(true, false).is_ok());
        assert!(all.check_setup_flags(false, true).is_ok());
    }

    #[test]
    fn the_api_half_touches_nothing_that_needs_a_machine() {
        let api = ServerRole::Api;
        assert!(!api.runs_sandbox_runtime());
        assert!(!api.sends_heartbeats());
        assert!(!api.drains_on_shutdown());
        assert!(!api.reclaims_host_leftovers_at_startup());
        // It does decide who owns a paused sandbox — that is the half it is,
        // and it is the half that answers users.
        assert!(api.arbitrates_paused_sandbox_ownership());
        assert!(api.serves_user_facing_rest());
    }

    #[test]
    fn the_node_half_runs_sandboxes_and_decides_nothing_about_who_owns_them() {
        let node = ServerRole::Node;
        assert!(node.runs_sandbox_runtime());
        assert!(node.sends_heartbeats());
        assert!(node.drains_on_shutdown());
        assert!(!node.arbitrates_paused_sandbox_ownership());
        assert!(!node.serves_user_facing_rest());
        assert!(node.reclaims_host_leftovers_at_startup());
    }

    #[test]
    fn provisioning_flags_are_refused_for_the_api_half_and_only_for_it() {
        // 🔴 Pushed up rather than asserted: a refusal branch nothing ever
        // reaches is a branch nothing has checked.
        let err = ServerRole::Api
            .check_setup_flags(false, true)
            .unwrap_err()
            .to_string();
        assert!(err.contains("--setup-host"), "{err}");

        let err = ServerRole::Api
            .check_setup_flags(true, false)
            .unwrap_err()
            .to_string();
        assert!(err.contains("--setup-only"), "{err}");

        // Neither flag, no complaint — the refusal is about the combination.
        assert!(ServerRole::Api.check_setup_flags(false, false).is_ok());
        // And the other two roles provision as they always did.
        assert!(ServerRole::Node.check_setup_flags(true, true).is_ok());
        assert!(ServerRole::All.check_setup_flags(true, true).is_ok());
    }
}
