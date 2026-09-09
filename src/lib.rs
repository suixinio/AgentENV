pub mod api;
pub mod cfg;
pub mod digest;
pub mod identity;
pub mod image;
pub mod leader_task;
pub mod logging;
pub mod observability;
pub mod orchestrator;
pub mod privileges;
pub mod proto;
#[cfg(any(test, feature = "test-support"))]
pub mod redis_test_server;
pub mod runtime_snapshot;
pub mod sandbox;
pub mod scheduler_endpoint;
pub mod secret_kind;
pub mod server_main;
pub mod snapshot;
pub mod template;
pub mod types;
pub mod virtualization;

#[cfg(test)]
mod mock_module_gating {
    use std::path::PathBuf;

    // Splits any attributes written on the declaration's own line from the
    // declaration, so `#[cfg(test)] pub mod mock;` is one line the scan reads as
    // both a gate and a declaration.
    fn split_inline_attributes(line: &str) -> (Vec<&str>, &str) {
        let mut rest = line.trim_start();
        let mut attributes = Vec::new();
        while rest.starts_with("#[") {
            let Some(end) = rest.find(']') else {
                break;
            };
            attributes.push(&rest[..=end]);
            rest = rest[end + 1..].trim_start();
        }
        (attributes, rest)
    }

    // Any visibility at all: a private `mod mock` is compiled into the same
    // binary, which is the harm; being a public path is only the loudest form.
    fn strip_visibility(declaration: &str) -> &str {
        let Some(rest) = declaration.strip_prefix("pub") else {
            return declaration;
        };
        let rest = match rest.strip_prefix('(') {
            Some(restricted) => match restricted.find(')') {
                Some(end) => &restricted[end + 1..],
                None => rest,
            },
            None => rest,
        };
        rest.trim_start()
    }

    // The anchor is a line start followed by the declaration itself, so a
    // comment or a doc line describing the pattern never matches.
    fn declares_a_mock_module(line: &str) -> bool {
        let (_, declaration) = split_inline_attributes(line);
        let Some(rest) = strip_visibility(declaration).strip_prefix("mod mock") else {
            return false;
        };
        matches!(rest.chars().next(), Some(';') | Some('{') | Some(' '))
    }

    // Every cfg that excludes a release build. An allowlist, not a shape test:
    // `#[cfg(not(test))]` and `#[cfg(target_os = "linux")]` are cfgs too, and
    // both leave the module in the shipped binaries.
    const TEST_ONLY_CFGS: [&str; 3] = [
        "#[cfg(test)]",
        "#[cfg(feature = \"test-support\")]",
        "#[cfg(any(test, feature = \"test-support\"))]",
    ];

    fn gated(inline: &[&str], preceding: &[&str]) -> bool {
        if let Some(cfg) = inline.iter().find(|attr| attr.starts_with("#[cfg(")) {
            return TEST_ONLY_CFGS.contains(cfg);
        }
        for line in preceding.iter().rev() {
            let trimmed = line.trim_start();
            if trimmed.starts_with("#[cfg(") {
                return TEST_ONLY_CFGS.contains(&trimmed);
            }
            if trimmed.starts_with("#[") || trimmed.starts_with("//") {
                continue;
            }
            return false;
        }
        false
    }

    // One-based line numbers of the `mod mock` declarations this text leaves in
    // an unconditional build, whatever their visibility.
    fn ungated_mock_declarations(source: &str) -> Vec<usize> {
        let lines: Vec<&str> = source.lines().collect();
        lines
            .iter()
            .enumerate()
            .filter(|(index, line)| {
                let (inline, _) = split_inline_attributes(line);
                declares_a_mock_module(line) && !gated(&inline, &lines[..*index])
            })
            .map(|(index, _)| index + 1)
            .collect()
    }

    // aenv-core's own `src/`, plus the `src/` of every sibling crate: a mock
    // declared in either is compiled into a shipped binary. Out of scope, and
    // deliberately so rather than because a double there would be harmless:
    // `storage/*` and `thirdparty/*`, which `aenv-node` does link, and `adev/`,
    // which it does not. The scan is by module name, so a double called `fakes`
    // or `stubs` is not something it can see either.
    fn scanned_roots() -> Vec<PathBuf> {
        let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let mut roots = vec![workspace.join("src")];
        let mut crates: Vec<PathBuf> = std::fs::read_dir(workspace.join("crates"))
            .expect("the workspace has a crates directory")
            .flatten()
            .map(|entry| entry.path().join("src"))
            .filter(|src| src.is_dir())
            .collect();
        crates.sort();
        roots.extend(crates);
        roots
    }

    #[test]
    fn no_mock_module_reaches_a_shipped_binary() {
        let roots = scanned_roots();
        let mut files = 0usize;
        let mut declarations = 0usize;
        let mut offenders: Vec<String> = Vec::new();
        let mut stack = roots.clone();
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
            "{offenders:?} declare a `mod mock` under no cfg that excludes a release build. \
             Nothing here strips a non-generic `pub fn`, so such a test double is linked \
             into a shipped binary whatever its visibility, and a `pub` one is a public \
             path of its crate as well. Gate it with one of {TEST_ONLY_CFGS:?}, the way \
             `src/image/mod.rs` and `crates/aenv-node/src/p2p/mod.rs` do."
        );
        assert!(
            declarations > 0,
            "the walk over {roots:?} matched no `pub mod mock` line; a scan that finds \
             nothing passes everything"
        );
        assert!(files > 100, "only {files} files under {roots:?} were read");
        assert!(
            roots.len() > 3,
            "{roots:?} is not every crate's source root"
        );

        assert_eq!(
            ungated_mock_declarations("pub mod mock;\n"),
            vec![1],
            "the scan does not notice an ungated declaration"
        );
        assert!(
            ungated_mock_declarations(
                "#[cfg(any(test, feature = \"test-support\"))]\npub mod mock;\n"
            )
            .is_empty(),
            "the scan reports a gated declaration"
        );
        assert!(
            ungated_mock_declarations("// pub mod mock;\n").is_empty(),
            "the scan matches a comment describing the pattern"
        );
        assert_eq!(
            ungated_mock_declarations("#[cfg(not(test))]\npub mod mock;\n"),
            vec![2],
            "a cfg that keeps the module out of test builds and in release ones passes \
             the gate"
        );
        assert_eq!(
            ungated_mock_declarations("#[cfg(target_os = \"linux\")]\npub mod mock;\n"),
            vec![2],
            "any cfg at all satisfies the gate, so it proves nothing about release builds"
        );
        assert_eq!(
            ungated_mock_declarations("mod mock;\n"),
            vec![1],
            "a private module is compiled into the same binary, and `pub use mock::*;` \
             publishes it anyway"
        );
        assert_eq!(
            ungated_mock_declarations("pub(crate) mod mock;\n"),
            vec![1],
            "a restricted visibility is still a module in the shipped binary"
        );
        assert!(
            ungated_mock_declarations("#[cfg(test)] pub mod mock;\n").is_empty(),
            "a cfg written on the declaration's own line is that declaration's gate"
        );
        assert_eq!(
            ungated_mock_declarations("#[cfg(feature = \"other\")] pub mod mock;\n"),
            vec![1],
            "and a same-line cfg that is not one of the test-only ones must not hide the \
             declaration from the scan either"
        );
    }
}
