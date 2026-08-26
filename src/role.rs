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

    /// Whether this role decides, on its own initiative, that a paused sandbox
    /// should be brought back to life.
    ///
    /// 🔴 This is what the data plane's auto-resume hangs on. A `node` forwards
    /// bytes and refuses traffic addressed to a superseded incarnation; what it
    /// no longer does is notice that the sandbox is paused and start it. That
    /// decision belongs to the half that owns sandboxes, and the data plane
    /// reaches it through the gateway's cold path
    /// (`crate::api::grpc::resume`) rather than through whichever node the
    /// traffic happened to land on.
    ///
    /// 🔴 `all` answers yes, and that is the whole rollback: the four decision
    /// arms in `try_auto_resume` are still compiled and still reached, because
    /// deleting them would mean `--role all` is no longer the thing that was
    /// running before (`_sd-impl-phase3-role.md` §11.3).
    pub fn serves_wake_decisions(self) -> bool {
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

    /// Whether this role has to be *handed* its envd access-token seed rather
    /// than being allowed to invent a node-local one.
    ///
    /// envd access tokens are `HMAC(seed, sandbox_id)`, so the seed is not a
    /// private detail of the process that holds it: it is the only thing that
    /// makes two processes agree on what a sandbox's token is.
    ///
    /// 🔴 `api` only, and it is about replication rather than about being the
    /// deciding half. An `api` Deployment runs more than one replica, and every
    /// one of them mints tokens (`create`, `fork`), re-derives them (`resume`)
    /// and hands them back (`GET /sandboxes/{id}`). Two replicas with two
    /// invented seeds do not disagree loudly — the user is handed a token by
    /// whichever replica the load balancer picked, and it stops working the
    /// moment another one answers, with no error, no log and no metric
    /// (`_sd-impl-phase3-role.md` §9.2). Refusing to start is the only form of
    /// that fault anybody sees.
    ///
    /// `node` and `all` answer `false`, and that is today's behaviour verbatim:
    /// a single machine that generates its own seed and keeps it under
    /// `$AENV_HOME/secrets/` works, and a developer running `--role all` should
    /// not need a secret to boot. Cross-*node* agreement still matters for
    /// cross-node recovery, but that is a warning's job, not a refusal's — the
    /// deployment that needs it is not the deployment that is broken without
    /// it. What tells the two apart on a live cluster is the fingerprint gauge,
    /// not this gate; see [`crate::sandbox::SandboxAccessTokenGenerator`].
    pub fn needs_a_configured_access_token_seed(self) -> bool {
        matches!(self, Self::Api)
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

    /// Refuses a `--role node` process that has been handed a PostgreSQL
    /// DSN.
    ///
    /// `dsn` is [`crate::cfg::PgConfig::dsn`]'s output — already trimmed,
    /// already `None` for blank — so this only ever sees a value here when
    /// one is genuinely configured.
    ///
    /// A hard startup failure rather than a warning, for the same reason
    /// `src/snapshot/repository/backends/central/mod.rs` gives for the
    /// snapshot catalog and `PausedRegistryBackendKind::Postgres`
    /// (`src/cfg.rs`) already enforces for the paused registry: database
    /// credentials, the connection budget and the schema are the deciding
    /// half's business, never the machines that run user code. `--role node`
    /// runs user code; `--role api` and `--role all` decide, and both may
    /// configure `[pg]` freely.
    pub fn check_pg_dsn(self, dsn: Option<&str>) -> Result<()> {
        if self != Self::Node {
            return Ok(());
        }
        if dsn.is_none() {
            return Ok(());
        }
        bail!(
            "--role node must not be configured with [pg].dsn: database credentials, the \
             connection budget and the schema belong to the deciding half (--role api / --role \
             all), never to a machine that runs user code. Remove [pg] from this node's \
             configuration, or from whatever file AENV_CONFIG_OVERLAY_PATH names for it"
        )
    }

    /// Whether this role must never construct a central snapshot catalog of
    /// its own — no `PostgresSnapshotCatalog` (it cannot: [`check_pg_dsn`]
    /// above already refuses `--role node` any `[pg]` DSN to build one
    /// from) and no `CentralSnapshotCatalog` gRPC client either.
    ///
    /// 🔴 P2 (task's own "phase4-close"): `--role node` has exactly two
    /// request-time catalog reads (`create`'s snapshot-source arm,
    /// `build_template`'s base-snapshot arm), and both are already served
    /// by a record api pre-resolves and sends down with the request
    /// (`SnapshotSource.resolved_record` / `TemplateBuildRequest.base_snapshot_resolved`)
    /// — see `services/api/proto/node.proto`'s own doc on those fields.
    /// Before this gate existed, `build_snapshot_backend` had no
    /// role-awareness at all and built the *same* central-catalog matrix
    /// on `--role node` as on `--role api`/`--role all`, differing only in
    /// always passing a `None` `pg_pool` — which made
    /// `[snapshot.catalog].write = "postgres"` (the configuration this
    /// project's own decommissioning of `services/scheduler` needs) an
    /// unconditional `--role node` startup crash: that mode refuses to run
    /// without `[pg]`, with no gRPC fallback, and a node can never have
    /// `[pg]` at all. `[snapshot.catalog].write = "both"` did not crash —
    /// `CentralSnapshotCatalog` dials lazily — but still built a live gRPC
    /// client to a scheduler `--role node` never actually needs, for a
    /// fallback path (an unresolved pre-resolved field) that only exists
    /// to cover a mixed-version rolling upgrade window.
    pub fn never_constructs_a_central_snapshot_catalog(self) -> bool {
        matches!(self, Self::Node)
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
        assert!(all.serves_wake_decisions());
        // 🔴 The one gate `all` answers *no* to, and it is not a capability
        // being taken away — it is a behaviour `all` never had. Sweeping the
        // host at startup is new, and turning it on for the rollback target
        // would mean the thing being rolled back to is not the thing that was
        // running before.
        assert!(!all.reclaims_host_leftovers_at_startup());
        assert!(all.check_setup_flags(true, false).is_ok());
        assert!(all.check_setup_flags(false, true).is_ok());
        // The other new-behaviour gate (P2, task's own "phase4-close"):
        // `all` has always had unrestricted `[pg]` access and must keep
        // building a real central snapshot catalog when one is configured
        // — this is not a capability `node` had that `all` also has, it is
        // the thing `node` never had at all.
        assert!(!all.never_constructs_a_central_snapshot_catalog());
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
        assert!(api.serves_wake_decisions());
        assert!(!api.never_constructs_a_central_snapshot_catalog());
    }

    #[test]
    fn the_node_half_runs_sandboxes_and_decides_nothing_about_who_owns_them() {
        let node = ServerRole::Node;
        assert!(node.runs_sandbox_runtime());
        assert!(node.sends_heartbeats());
        assert!(node.drains_on_shutdown());
        assert!(!node.arbitrates_paused_sandbox_ownership());
        assert!(!node.serves_user_facing_rest());
        // 🔴 The predicate the local reverse proxy's auto-resume arm is gated
        // on. A node that answered `true` here would go on starting sandboxes
        // on its own initiative, and the topology change would be a no-op that
        // looked like it had landed.
        assert!(!node.serves_wake_decisions());
        assert!(node.reclaims_host_leftovers_at_startup());
        // P2 (task's own "phase4-close"): the gate that keeps `--role node`
        // from ever building a central snapshot catalog -- Postgres (it
        // structurally cannot: `check_pg_dsn` above refuses it any `[pg]`
        // DSN) or a gRPC client to a scheduler this role has no reason to
        // reach.
        assert!(node.never_constructs_a_central_snapshot_catalog());
    }

    /// 🔴 Both faces in one test, because either one alone is satisfied by a
    /// constant: "api needs a seed" passes on a predicate that is always true,
    /// and "node does not" passes on one that is always false. What the gate
    /// has to say is that the two differ.
    #[test]
    fn only_the_replicated_half_must_be_handed_its_access_token_seed() {
        assert!(
            ServerRole::Api.needs_a_configured_access_token_seed(),
            "an api replica that invents a seed mints tokens its siblings cannot derive"
        );
        assert!(
            !ServerRole::Node.needs_a_configured_access_token_seed(),
            "a node's managed seed is node-local state and has always been allowed to be"
        );
        assert!(
            !ServerRole::All.needs_a_configured_access_token_seed(),
            "the rollback target boots on a developer's machine with no secret at all"
        );
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

    /// 🔴 The security invariant this gate exists for: a `[pg].dsn` must
    /// never reach `--role node`, which runs user code, but is exactly what
    /// `--role api` and `--role all` are for.
    #[test]
    fn a_configured_pg_dsn_is_refused_for_node_and_only_for_node() {
        let dsn = Some("postgres://user:pw@db.internal:5432/agentenv");

        let err = ServerRole::Node.check_pg_dsn(dsn).unwrap_err().to_string();
        assert!(err.contains("[pg].dsn"), "{err}");
        assert!(err.contains("--role node"), "{err}");

        // The other two roles decide; both may hold the DSN.
        assert!(ServerRole::Api.check_pg_dsn(dsn).is_ok());
        assert!(ServerRole::All.check_pg_dsn(dsn).is_ok());

        // No DSN configured at all is fine for every role, node included —
        // this gate is about a DSN reaching a node, not about node's role
        // identity on its own.
        assert!(ServerRole::Node.check_pg_dsn(None).is_ok());
        assert!(ServerRole::Api.check_pg_dsn(None).is_ok());
        assert!(ServerRole::All.check_pg_dsn(None).is_ok());
    }
}
