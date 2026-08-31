mod codegen;
mod config;
mod coverage;
mod ensure_tool;
mod mutants;
mod util;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "adev", about = "AENV dev/CI tooling")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run code generators (auto-installs protoc/openapi if missing)
    Codegen(codegen::CodegenArgs),
    /// Run mutation tests (auto-installs cargo-mutants)
    Mutants(mutants::MutantsArgs),
    /// Run code coverage (auto-installs cargo-llvm-cov)
    Coverage(coverage::CoverageArgs),
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Codegen(args) => codegen::run(args),
        Commands::Mutants(args) => mutants::run(args),
        Commands::Coverage(args) => coverage::run(args),
    }
}

#[cfg(test)]
mod makefile_bin_targets_carry_a_package {
    fn logical_lines(text: &str) -> Vec<(usize, String)> {
        let mut out: Vec<(usize, String)> = Vec::new();
        let mut pending: Option<(usize, String)> = None;
        for (idx, raw) in text.lines().enumerate() {
            let (start, mut buf) = pending.take().unwrap_or((idx + 1, String::new()));
            match raw.strip_suffix('\\') {
                Some(head) => {
                    buf.push_str(head);
                    buf.push(' ');
                    pending = Some((start, buf));
                }
                None => {
                    buf.push_str(raw);
                    out.push((start, buf));
                }
            }
        }
        if let Some(tail) = pending {
            out.push(tail);
        }
        out
    }

    fn names_a_package(line: &str) -> bool {
        line.split_whitespace().any(|tok| {
            tok == "-p"
                || tok == "--package"
                || tok.starts_with("--package=")
                || (tok.starts_with("-p") && tok.len() > 2 && !tok.starts_with("-p-"))
        })
    }

    #[test]
    fn every_makefile_bin_flag_is_paired_with_a_package_flag() {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../Makefile");
        let text = std::fs::read_to_string(path).unwrap_or_else(|err| {
            panic!("the guard could not read the workspace Makefile at {path}: {err}")
        });

        let mut scanned = 0usize;
        let mut offenders: Vec<String> = Vec::new();
        for (lineno, line) in logical_lines(&text) {
            // `--bins` (the plural sweep `test-unit` uses) is not `--bin` and
            // needs no package; the trailing space is what tells them apart.
            if line.trim_start().starts_with('#') || !line.contains("--bin ") {
                continue;
            }
            scanned += 1;
            if !names_a_package(&line) {
                offenders.push(format!("  Makefile:{lineno}: {}", line.trim()));
            }
        }

        assert!(
            scanned >= 6,
            "the guard only found {scanned} `--bin` recipe line(s) in {path}; it expects at \
             least 6. Either the Makefile shrank a lot (lower this floor deliberately) or the \
             scan is broken and would otherwise pass vacuously"
        );

        assert!(
            offenders.is_empty(),
            "these Makefile recipes pass `--bin` without a `-p`/`--package`. The workspace root \
             package owns no bin target, so cargo answers `error: no bin target named ... in \
             default-run packages` and the recipe fails before compiling anything. Add the \
             owning package, e.g. `cargo run -p aenv-node --bin aenv-node`:\n{}",
            offenders.join("\n")
        );
    }
}
