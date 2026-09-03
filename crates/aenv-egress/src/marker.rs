//! `${aenv.secrets.NAME}` markers inside a header value. The grammar is the
//! one the api half validates against; the broker only substitutes.

use crate::credential::is_valid_secret_name;

pub const MARKER_PREFIXES: [&str; 2] = ["${aenv.secrets.", "${e2b.secrets."];

/// One marker occurrence: the name it refers to and the bytes it occupies.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecretMarker<'a> {
    pub name: &'a str,
    pub range: std::ops::Range<usize>,
}

/// Every well-formed marker in `value`, in order. A `${` that is not one of
/// the known prefixes, or a name the grammar refuses, stays literal text.
pub fn secret_markers(value: &str) -> Vec<SecretMarker<'_>> {
    let mut markers = Vec::new();
    let mut cursor = 0;
    while let Some(offset) = value[cursor..].find("${") {
        let start = cursor + offset;
        let rest = &value[start..];
        let Some(prefix) = MARKER_PREFIXES.iter().find(|p| rest.starts_with(*p)) else {
            cursor = start + 2;
            continue;
        };
        let name_start = start + prefix.len();
        let Some(close) = value[name_start..].find('}') else {
            break;
        };
        let name = &value[name_start..name_start + close];
        let end = name_start + close + 1;
        if is_valid_secret_name(name) {
            markers.push(SecretMarker {
                name,
                range: start..end,
            });
            cursor = end;
        } else {
            cursor = name_start;
        }
    }
    markers
}

/// Replaces every marker in `template` with what `resolve` returns for its
/// name. Resolution errors stop the substitution and are returned as-is.
pub fn substitute<E>(
    template: &str,
    mut resolve: impl FnMut(&str) -> Result<String, E>,
) -> Result<String, E> {
    let markers = secret_markers(template);
    if markers.is_empty() {
        return Ok(template.to_string());
    }
    let mut out = String::with_capacity(template.len());
    let mut cursor = 0;
    for marker in markers {
        out.push_str(&template[cursor..marker.range.start]);
        out.push_str(&resolve(marker.name)?);
        cursor = marker.range.end;
    }
    out.push_str(&template[cursor..]);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn markers_of_both_prefixes_are_found_in_order() {
        let found = secret_markers(
            "Basic ${aenv.secrets.user}:${e2b.secrets.pass} ${HOME} ${aenv.secrets.}",
        );
        assert_eq!(
            found.iter().map(|m| m.name).collect::<Vec<_>>(),
            ["user", "pass"]
        );
    }

    #[test]
    fn substitution_replaces_markers_and_keeps_literal_text() {
        let out = substitute::<()>("Bearer ${aenv.secrets.k} for ${HOME}", |name| {
            Ok(format!("<{name}>"))
        })
        .unwrap();
        assert_eq!(out, "Bearer <k> for ${HOME}");
        assert_eq!(
            substitute::<()>("plain", |_| unreachable!()).unwrap(),
            "plain"
        );
    }

    #[test]
    fn a_resolution_error_stops_the_substitution() {
        let result = substitute("${aenv.secrets.a}${aenv.secrets.b}", |name| {
            if name == "b" {
                Err("denied")
            } else {
                Ok("A".to_string())
            }
        });
        assert_eq!(result, Err("denied"));
    }
}
