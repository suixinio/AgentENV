use anyhow::Result;
use clap::Args;

use crate::ensure_tool;
use crate::util;

#[derive(Args)]
pub struct MutantsArgs {
    /// Package to test (default: agentenv)
    #[arg(long, default_value = "aenv-core")]
    pub package: String,

    /// Timeout per mutant in seconds
    #[arg(long, default_value = "300")]
    pub timeout: u32,

    /// Number of parallel jobs
    #[arg(long, default_value = "2")]
    pub jobs: u32,

    /// Memory limit per test in MB (0 = no limit)
    #[arg(long, default_value = "8192")]
    pub memory_limit_mb: u32,

    /// Only test mutants in this file
    #[arg(long)]
    pub file: Option<String>,
}

pub fn run(args: MutantsArgs) -> Result<()> {
    ensure_tool::ensure_cargo_tool("mutants", "cargo-mutants")?;

    // Check platform
    if args.package == "aenv-core" && std::env::consts::OS != "linux" {
        anyhow::bail!("aenv-core mutation tests require Linux");
    }

    util::info(&format!(
        "Running mutation tests for package={} (memory_limit={})",
        args.package,
        if args.memory_limit_mb > 0 {
            format!("{}MB", args.memory_limit_mb)
        } else {
            "unlimited".to_string()
        }
    ));

    let timeout_str = args.timeout.to_string();
    let jobs_str = args.jobs.to_string();

    let mut cmd_args = vec![
        "mutants",
        "--package",
        &args.package,
        "--timeout",
        &timeout_str,
        "--jobs",
        &jobs_str,
        "--no-shuffle",
        "--caught",
        "--unviable",
    ];

    if let Some(ref file) = args.file {
        cmd_args.push("--file");
        cmd_args.push(file);
    }

    cmd_args.extend_from_slice(&["--", "--lib"]);

    if args.memory_limit_mb > 0 {
        if std::process::Command::new("systemd-run")
            .arg("--version")
            .output()
            .is_err()
        {
            anyhow::bail!("systemd-run not found. Memory limiting requires systemd.");
        }

        let runner = format!(
            "systemd-run --user --scope -p MemoryMax={}M -p MemorySwapMax=0 --",
            args.memory_limit_mb
        );
        let target = format!("{}-unknown-linux-gnu", std::env::consts::ARCH);
        let runner_var = format!(
            "CARGO_TARGET_{}_RUNNER",
            target.to_uppercase().replace('-', "_")
        );
        std::env::set_var(&runner_var, &runner);
    }

    util::cmd("cargo", &cmd_args)?;
    Ok(())
}
