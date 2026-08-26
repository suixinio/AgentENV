use std::path::{Component, Path, PathBuf};

/// Lexically normalizes a path: drops `.` components and resolves `..` against
/// the components already accumulated, without touching the filesystem.
///
/// 🔴 Lives here, not in `overlaybd`, because both the storage layer and the
/// configuration layer normalize the paths they hand each other, and the
/// configuration layer must not depend on the storage crate. `overlaybd::config`
/// re-exports this, so its callers are unaffected.
pub fn lexically_normalize_path(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            other => normalized.push(other.as_os_str()),
        }
    }
    normalized
}

/// Wraps a shell argument in single quotes if it contains characters that need quoting.
/// Internal single quotes are escaped as `'\''`.
pub fn shell_quote(s: &str) -> String {
    if s.is_empty() {
        return "''".to_string();
    }
    let safe = s
        .chars()
        .all(|c| c.is_alphanumeric() || matches!(c, '-' | '_' | '.' | '/' | ':' | '@' | '+' | '='));
    if safe {
        s.to_string()
    } else {
        format!("'{}'", s.replace('\'', "'\\''"))
    }
}

#[cfg(test)]
mod tests {
    use super::shell_quote;

    #[test]
    fn safe_chars_pass_through() {
        assert_eq!(shell_quote("nginx"), "nginx");
        assert_eq!(shell_quote("/bin/sh"), "/bin/sh");
        assert_eq!(shell_quote("node:20"), "node:20");
    }

    #[test]
    fn wraps_special_chars() {
        assert_eq!(shell_quote("daemon off;"), "'daemon off;'");
        assert_eq!(shell_quote("$HOME"), "'$HOME'");
        assert_eq!(shell_quote(""), "''");
    }

    #[test]
    fn escapes_embedded_single_quotes() {
        assert_eq!(shell_quote("it's"), "'it'\\''s'");
        // Three consecutive single quotes: each ' becomes '\'' giving ''\'''\'''\'''
        assert_eq!(shell_quote("'''"), "''\\'''\\'''\\'''");
    }

    #[test]
    fn prevents_variable_expansion() {
        // $ inside single quotes is not expanded by bash.
        assert_eq!(shell_quote("$HOME"), "'$HOME'");
        assert_eq!(shell_quote("${VAR:-default}"), "'${VAR:-default}'");
    }

    #[test]
    fn backslash_is_literal_inside_single_quotes() {
        // Backslash has no special meaning inside single quotes; only quoting needed.
        assert_eq!(shell_quote("a\\b"), "'a\\b'");
    }
}
