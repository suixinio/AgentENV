pub mod api;
pub mod binding_store;
pub mod cfg;
pub mod digest;
pub mod identity;
pub mod image;
pub mod leader_task;
pub mod logging;
pub mod node_client;
pub mod node_registry;
pub mod observability;
pub mod orchestrator;
pub mod p2p;
pub mod privileges;
pub mod proto;
pub mod record_dir;
#[cfg(test)]
mod redis_test_server;
pub mod runtime_snapshot;
pub mod sandbox;
pub mod scheduler_endpoint;
pub mod secrets;
pub mod server_main;
pub mod snapshot;
pub mod template;
pub mod types;
pub mod virtualization;

#[cfg(test)]
mod mock_module_gating {
    use std::path::PathBuf;

    // The anchor is a line start followed by the declaration itself, so a
    // comment or a doc line describing the pattern never matches.
    fn declares_a_mock_module(line: &str) -> bool {
        let Some(rest) = line.trim_start().strip_prefix("pub mod mock") else {
            return false;
        };
        matches!(rest.chars().next(), Some(';') | Some('{') | Some(' '))
    }

    fn gated(preceding: &[&str]) -> bool {
        for line in preceding.iter().rev() {
            let trimmed = line.trim_start();
            if trimmed.starts_with("#[cfg(") {
                return true;
            }
            if trimmed.starts_with("#[") || trimmed.starts_with("//") {
                continue;
            }
            return false;
        }
        false
    }

    /// One-based line numbers of the `pub mod mock` declarations this text
    /// leaves in an unconditional build.
    fn ungated_mock_declarations(source: &str) -> Vec<usize> {
        let lines: Vec<&str> = source.lines().collect();
        lines
            .iter()
            .enumerate()
            .filter(|(index, line)| declares_a_mock_module(line) && !gated(&lines[..*index]))
            .map(|(index, _)| index + 1)
            .collect()
    }

    #[test]
    fn no_mock_module_reaches_a_shipped_binary() {
        let src = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut files = 0usize;
        let mut declarations = 0usize;
        let mut offenders: Vec<String> = Vec::new();
        let mut stack = vec![src.clone()];
        while let Some(dir) = stack.pop() {
            let Ok(entries) = std::fs::read_dir(&dir) else {
                continue;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    stack.push(path);
                    continue;
                }
                if path.extension().and_then(|ext| ext.to_str()) != Some("rs") {
                    continue;
                }
                let Ok(contents) = std::fs::read_to_string(&path) else {
                    continue;
                };
                files += 1;
                declarations += contents
                    .lines()
                    .filter(|l| declares_a_mock_module(l))
                    .count();
                for line in ungated_mock_declarations(&contents) {
                    offenders.push(format!("{}:{line}", path.display()));
                }
            }
        }

        assert!(
            offenders.is_empty(),
            "{offenders:?} declare `pub mod mock` with no `#[cfg(...)]` above it. \
             Nothing here strips a non-generic `pub fn`, so an ungated test double is \
             linked into both shipped binaries and is a public path of both sibling \
             crates. Gate it with `#[cfg(any(test, feature = \"test-support\"))]`, the \
             way `src/p2p/mod.rs` and `src/image/mod.rs` do."
        );
        assert!(
            declarations > 0,
            "the walk over {} matched no `pub mod mock` line; a scan that finds nothing \
             passes everything",
            src.display()
        );
        assert!(
            files > 100,
            "only {files} files under {} were read",
            src.display()
        );

        assert_eq!(
            ungated_mock_declarations("pub mod mock;\n"),
            vec![1],
            "the scan does not notice an ungated declaration"
        );
        assert!(
            ungated_mock_declarations("#[cfg(any(test, feature = \"x\"))]\npub mod mock;\n")
                .is_empty(),
            "the scan reports a gated declaration"
        );
        assert!(
            ungated_mock_declarations("// pub mod mock;\n").is_empty(),
            "the scan matches a comment describing the pattern"
        );
    }
}
