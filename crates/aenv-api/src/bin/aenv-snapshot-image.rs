//! Standalone tool publishing a committed AgentENV snapshot's rootfs as a
//! plain OCI image. stdout carries exactly one line — the image reference —
//! so the tool composes in scripts; logs go to stderr and errors exit
//! non-zero with a credential-free context chain. It never starts
//! node-runtime machinery (KVM, ublk, network); registry authentication
//! stays with the Docker config that `regctl` reads itself.

use std::path::PathBuf;

use aenv_api::cfg::{regctl_path, validate_api_half, AppConfig, ConfigManager};
use aenv_api::pg::{self, PgPoolSettings};
use aenv_api::snapshot::image_export::SnapshotImageService;
use anyhow::Context as _;
use clap::Parser;

#[derive(Debug, Parser)]
#[command(
    name = "aenv-snapshot-image",
    about = "Publish a committed AgentENV snapshot rootfs as a standalone OCI image \
             and print the image reference on stdout"
)]
struct Cli {
    /// Snapshot ID or alias to export.
    snapshot: String,

    /// Target OCI repository including the registry host, for example
    /// "registry.example.com/team/app". When omitted, use the snapshot's
    /// recorded rootfs publication repository or unique external source.
    #[arg(long)]
    target_repository: Option<String>,

    /// OCI image tag. Defaults to latest for an explicit target, or
    /// snapshot-<snapshot-id> when the target repository is inferred.
    #[arg(long)]
    tag: Option<String>,

    /// Path to config file (same as AENV_CONFIG_PATH).
    #[arg(long)]
    config: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    let cli = Cli::parse();
    let config_manager = match cli.config.as_deref() {
        Some(path) => ConfigManager::init_global_from_path(path, validate_api_half)
            .with_context(|| format!("load AgentENV config from '{}'", path.display()))?,
        None => ConfigManager::init_global(validate_api_half)
            .context("load AgentENV config (AENV_CONFIG_PATH or the default config path)")?,
    };
    let settings = pg_settings(config_manager.config())?;
    let pool = pg::connect(&settings)
        .await
        .context("connect to the snapshot catalog database")?;

    let service = SnapshotImageService::from_global_config(
        &pool,
        regctl_path(&config_manager.config().deps_path),
    )
    .await
    .context("initialize the snapshot repository export backend")?;

    let lookup = cli.snapshot.clone();
    let result = service
        .export_rootfs_image(
            &cli.snapshot,
            cli.target_repository.as_deref(),
            cli.tag.as_deref(),
        )
        .await
        .with_context(|| format!("export snapshot '{lookup}' rootfs as an OCI image"))?;
    println!("{}", result.image_ref);
    Ok(())
}

fn pg_settings(config: &AppConfig) -> anyhow::Result<PgPoolSettings> {
    PgPoolSettings::from_config(config.pg.as_ref())?.context(
        "[pg] is not configured, and the snapshot catalog is PostgreSQL: this tool reads one \
         catalog row and then reaches the rootfs layer bytes, and it has nowhere to read that \
         row from. Set [pg].dsn to the same database the api replicas use — it is \
         TOML-file-only, with no environment binding (confique cannot descend into \
         AppConfig::pg's Option), so supply it through the file AENV_CONFIG_PATH names or an \
         AENV_CONFIG_OVERLAY_PATH overlay, the way deploy/k8s/base's pg-dsn.toml and \
         deploy/docker-compose.yml's /tmp/agentenv-pg/pg-dsn.toml both do",
    )
}

#[cfg(test)]
mod tests {
    use clap::{CommandFactory, Parser};

    use super::{pg_settings, AppConfig, Cli};

    #[test]
    fn the_missing_pg_refusal_names_no_environment_variable_that_does_not_exist() {
        let config = AppConfig::default();
        assert!(
            config.pg.is_none(),
            "the default has no [pg], which is what makes this refusal reachable"
        );

        let err = match pg_settings(&config) {
            Ok(_) => panic!("this tool has no catalog to read without [pg]"),
            Err(err) => format!("{err:#}"),
        };
        assert!(
            !err.contains("AENV_PG_DSN"),
            "there is no such environment variable: {err}"
        );
        assert!(err.contains("[pg].dsn"), "{err}");
        assert!(err.contains("TOML-file-only"), "{err}");
        assert!(err.contains("AENV_CONFIG_OVERLAY_PATH"), "{err}");
        assert!(err.contains("pg-dsn.toml"), "{err}");
    }

    #[test]
    fn cli_parses_options_and_requires_snapshot() {
        assert_eq!(Cli::command().get_name(), "aenv-snapshot-image");
        let cli = Cli::try_parse_from([
            "aenv-snapshot-image",
            "snap-1",
            "--target-repository",
            "registry.example.com/team/app",
            "--tag",
            "release-1",
            "--config",
            "/etc/aenv/config.toml",
        ])
        .unwrap();
        assert_eq!(cli.snapshot, "snap-1");
        assert_eq!(
            cli.target_repository.as_deref(),
            Some("registry.example.com/team/app")
        );
        assert_eq!(cli.tag.as_deref(), Some("release-1"));
        assert_eq!(
            cli.config.as_deref(),
            Some(std::path::Path::new("/etc/aenv/config.toml"))
        );

        let minimal = Cli::try_parse_from(["aenv-snapshot-image", "snap-1"]).unwrap();
        assert!(
            minimal.target_repository.is_none()
                && minimal.tag.is_none()
                && minimal.config.is_none()
        );
        assert!(Cli::try_parse_from(["aenv-snapshot-image"]).is_err());
    }
}
