use anyhow::Result;
use clap::{Args, Subcommand};

use crate::config;
use crate::util;

#[derive(Args)]
pub struct CodegenArgs {
    #[command(subcommand)]
    pub target: Option<CodegenTarget>,

    /// Only ensure dependencies are installed, don't run codegen
    #[arg(long)]
    pub ensure_deps_only: bool,
}

#[derive(Subcommand)]
pub enum CodegenTarget {
    /// Regenerate Firecracker API client
    Firecracker,
    /// Regenerate envd HTTP client
    Envd,
    /// Regenerate AENV HTTP server stubs
    Server,
    /// Regenerate custom extension HTTP client
    CustomExtension,
}

/// npm wrapper version for @openapitools/openapi-generator-cli.
/// The actual generator jar version is controlled by openapitools.json in the project root.
const OPENAPI_GENERATOR_CLI_VERSION: &str = "2.32.0";

pub fn run(args: CodegenArgs) -> Result<()> {
    let project_root = config::project_root()?;

    if args.ensure_deps_only {
        let cfg = config::load_config_from_root(&project_root)?;
        // Also ensure protoc
        crate::ensure_tool::ensure_protoc(&cfg.protoc.version, &cfg.protoc.url)?;
        util::info("All codegen dependencies are ready.");
        return Ok(());
    }

    match args.target {
        Some(CodegenTarget::Firecracker) => run_firecracker(&project_root),
        Some(CodegenTarget::Envd) => run_envd(&project_root),
        Some(CodegenTarget::Server) => run_server(&project_root),
        Some(CodegenTarget::CustomExtension) => run_custom_extension(&project_root),
        None => {
            run_firecracker(&project_root)?;
            run_envd(&project_root)?;
            run_server(&project_root)?;
            run_custom_extension(&project_root)?;
            Ok(())
        }
    }
}

/// Regenerate the custom extension HTTP client into
/// `src/custom_extension_api/generated`.
fn run_custom_extension(project_root: &std::path::Path) -> Result<()> {
    let ext_dir = project_root.join("src/custom_extension_api/generated");
    let spec = project_root.join("src/custom_extension_api/openapi.yml");

    util::info("Regenerating custom extension HTTP client...");
    run_openapi_generator(
        project_root,
        &[
            "generate",
            "-i",
            &spec.to_string_lossy(),
            "-g",
            "rust",
            "-o",
            &ext_dir.to_string_lossy(),
            "--additional-properties=packageName=custom_extension_client,hideGenerationTimestamp=true",
            "--skip-validate-spec",
        ],
    )?;

    prepend_allow_attrs(&ext_dir.join("src/lib.rs"))?;
    util::cmd("cargo", &["fmt", "-p", "custom_extension_client"])?;
    util::info("custom extension client generated.");
    Ok(())
}

/// Run openapi-generator-cli via npx.
/// The generator jar version is read from openapitools.json in the project root.
fn run_openapi_generator(project_root: &std::path::Path, args: &[&str]) -> Result<()> {
    let package = format!(
        "@openapitools/openapi-generator-cli@{}",
        OPENAPI_GENERATOR_CLI_VERSION
    );
    let mut cmd_args: Vec<&str> = vec!["--yes", &package, "--"];
    cmd_args.extend_from_slice(args);
    util::cmd_in_dir("npx", &cmd_args, project_root)
}

fn run_firecracker(project_root: &std::path::Path) -> Result<()> {
    let fc_dir = project_root.join("thirdparty/firecracker-client");
    let spec = fc_dir.join("firecracker.yaml");

    util::info("Regenerating Firecracker API client...");
    run_openapi_generator(
        project_root,
        &[
            "generate",
            "-i",
            &spec.to_string_lossy(),
            "-g",
            "rust",
            "-o",
            &fc_dir.to_string_lossy(),
            "--global-property",
            "models,supportingFiles",
            "--additional-properties=packageName=firecracker_client,hideGenerationTimestamp=true",
        ],
    )?;

    prepend_allow_attrs(&fc_dir.join("src/lib.rs"))?;
    util::cmd("cargo", &["fmt", "-p", "firecracker_client"])?;
    util::info("Firecracker client generated.");
    Ok(())
}

fn run_envd(project_root: &std::path::Path) -> Result<()> {
    let envd_dir = project_root.join("thirdparty/envd/http-client");
    let spec = envd_dir.join("envd.yaml");

    util::info("Regenerating envd HTTP client...");
    run_openapi_generator(
        project_root,
        &[
            "generate",
            "-i",
            &spec.to_string_lossy(),
            "-g",
            "rust",
            "-o",
            &envd_dir.to_string_lossy(),
            "--additional-properties=packageName=http_client,hideGenerationTimestamp=true",
            "--skip-validate-spec",
        ],
    )?;

    prepend_allow_attrs(&envd_dir.join("src/lib.rs"))?;
    util::cmd("cargo", &["fmt", "-p", "http_client"])?;
    util::info("envd HTTP client generated.");
    Ok(())
}

fn run_server(project_root: &std::path::Path) -> Result<()> {
    let server_dir = project_root.join("src/api/generated");
    let spec = project_root.join("src/api/openapi.yml");

    util::info("Regenerating AENV HTTP server...");
    run_openapi_generator(
        project_root,
        &[
            "generate",
            "-g",
            "rust-axum",
            "-i",
            &spec.to_string_lossy(),
            "-o",
            &server_dir.to_string_lossy(),
            "--additional-properties=packageName=agentenv_http_server,hideGenerationTimestamp=true",
        ],
    )?;

    // Port of fix_rust_axum_duplicate_auth_trait.py
    let mod_rs = server_dir.join("src/apis/mod.rs");
    fix_duplicate_auth_trait(&mod_rs)?;

    // Anchored on formatted text, so format first and again afterwards.
    util::cmd("cargo", &["fmt", "-p", "agentenv_http_server"])?;
    add_zeroize_dependency(&server_dir.join("Cargo.toml"))?;
    redact_secret_value_models(&server_dir.join("src/models.rs"))?;
    util::cmd("cargo", &["fmt", "-p", "agentenv_http_server"])?;
    util::info("AENV server generated.");
    Ok(())
}

/// The generator writes the manifest too, so the one dependency the redacted
/// value field needs is added back after every run.
fn add_zeroize_dependency(cargo_toml: &std::path::Path) -> Result<()> {
    let content = std::fs::read_to_string(cargo_toml)?;
    if content.contains("\nzeroize = ") {
        return Ok(());
    }
    let anchor = "\n[dev-dependencies]";
    let at = content.find(anchor).ok_or_else(|| {
        anyhow::anyhow!(
            "the generated Cargo.toml has no [dev-dependencies] to insert before; a secret \
             value would ship without the crate that wipes it"
        )
    })?;
    let added =
        "\n# The secret value field `redact_secret_value_models` rewrites is one of these.\n\
                 zeroize = { version = \"1\", features = [\"serde\"] }\n";
    std::fs::write(
        cargo_toml,
        format!("{}{added}{}", &content[..at], &content[at..]),
    )?;
    Ok(())
}

/// Request models carrying a secret value the generator treats as ordinary text.
const SECRET_VALUE_MODELS: [&str; 2] = ["NewSecret", "SecretUpdate"];

const GENERATED_DERIVE: &str = "#[derive(Debug, Clone, PartialEq, serde::Serialize, \
                                serde::Deserialize, validator::Validate)]";
const REDACTED_DERIVE: &str =
    "#[derive(Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]";

/// A secret is opaque bytes: markup inside one is not an attack, it must never
/// reach a log line, and its buffer is wiped when the model drops rather than
/// freed with the value still in it. Both shapes a write can carry — the
/// opaque `value` and every entry of the structured `fields` — get that.
fn redact_secret_value_models(models_rs: &std::path::Path) -> Result<()> {
    let mut content = std::fs::read_to_string(models_rs)?;
    for model in SECRET_VALUE_MODELS {
        content = redact_secret_value_model(&content, model)?;
    }
    std::fs::write(models_rs, content)?;
    Ok(())
}

fn redact_secret_value_model(content: &str, model: &str) -> Result<String> {
    let header = format!(
        "{GENERATED_DERIVE}\n\
         #[cfg_attr(feature = \"conversion\", derive(frunk::LabelledGeneric))]\n\
         pub struct {model} {{"
    );
    let start = content.find(&header).ok_or_else(|| {
        anyhow::anyhow!(
            "{model} no longer starts with the derive this step rewrites; the generator's \
             output changed and a secret value would ship with a printing Debug"
        )
    })?;
    let body_end = content[start..]
        .find("\n}\n")
        .map(|offset| start + offset + "\n}\n".len())
        .ok_or_else(|| anyhow::anyhow!("{model} has no struct body to rewrite"))?;

    // `Zeroizing` rather than a `Drop` on the model: the `conversion` feature
    // derives `frunk::LabelledGeneric`, which moves out of the struct and
    // cannot do that for a type that implements `Drop`. Dropping the `fields`
    // validation drops nothing that guarded a value — the generated check
    // only rejects markup in the *keys* — but the map's value type has to
    // stop being `models::SecretString`, which prints.
    let unvalidate = [
        (
            "    #[serde(rename = \"value\")]\n    \
             #[validate(custom(function = \"check_xss_string\"))]\n    \
             #[serde(skip_serializing_if = \"Option::is_none\")]\n    \
             pub value: Option<String>,",
            "    #[serde(rename = \"value\")]\n    \
             #[serde(skip_serializing_if = \"Option::is_none\")]\n    \
             pub value: Option<zeroize::Zeroizing<String>>,",
            "value",
        ),
        (
            "    #[serde(rename = \"fields\")]\n    \
             #[validate(custom(function = \"check_xss_map_nested\"))]\n    \
             #[serde(skip_serializing_if = \"Option::is_none\")]\n    \
             pub fields: Option<std::collections::HashMap<String, models::SecretString>>,",
            "    #[serde(rename = \"fields\")]\n    \
             #[serde(skip_serializing_if = \"Option::is_none\")]\n    \
             pub fields: \
             Option<std::collections::HashMap<String, zeroize::Zeroizing<String>>>,",
            "fields",
        ),
    ];
    let body = &content[start..body_end];
    let mut rewritten = body.to_string();
    for (validated, plain, field) in unvalidate {
        if !rewritten.contains(validated) {
            anyhow::bail!(
                "{model} has no XSS-validated {field} field to unvalidate; the generator's \
                 output changed and a secret containing markup would be rejected with a 400"
            );
        }
        rewritten = rewritten.replace(validated, plain);
    }
    let rewritten = rewritten.replacen(GENERATED_DERIVE, REDACTED_DERIVE, 1);

    let redacted_debug = format!(
        "\nimpl std::fmt::Debug for {model} {{\n    \
         fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {{\n        \
         f.write_str(\"{model}([redacted])\")\n    }}\n}}\n"
    );

    let rest = redact_secret_value_tail(&content[body_end..], model)?;
    Ok(format!(
        "{}{rewritten}{redacted_debug}{rest}",
        &content[..start],
    ))
}

/// The two fields' type changes and the printing `Display` both live after the
/// struct body: `FromStr` parses into an intermediate representation and
/// builds them, and `Display` writes the value into a query string.
///
/// Every edit is one line inside one impl block, so each is scoped to that
/// block: the same line appears in every other model.
fn redact_secret_value_tail(tail: &str, model: &str) -> Result<String> {
    let from_str = format!("impl std::str::FromStr for {model} {{");
    let tail = rewrite_in_block(
        tail,
        &from_str,
        "            pub fields: Vec<std::collections::HashMap<String, models::SecretString>>,\n",
        "            pub fields: \
         Vec<std::collections::HashMap<String, zeroize::Zeroizing<String>>>,\n",
        &format!("{model}'s FromStr no longer parses fields into what the struct holds"),
    )?;
    let tail = rewrite_in_block(
        &tail,
        &from_str,
        "            value: intermediate_rep.value.into_iter().next(),\n",
        "            value: intermediate_rep.value.into_iter().next().map(Into::into),\n",
        &format!("{model}'s FromStr no longer builds the value field"),
    )?;
    rewrite_in_block(
        &tail,
        &format!("impl std::fmt::Display for {model} {{"),
        ".map(|value| [\"value\".to_string(), value.to_string()].join(\",\")),",
        ".map(|_| [\"value\".to_string(), \"[redacted]\".to_string()].join(\",\")),",
        &format!("{model}'s Display no longer writes the value, or writes it differently"),
    )
}

/// Replaces `needle` once inside the block that starts at `header` and ends at
/// the first line holding only `}`.
fn rewrite_in_block(
    content: &str,
    header: &str,
    needle: &str,
    replacement: &str,
    what: &str,
) -> Result<String> {
    let start = content
        .find(header)
        .ok_or_else(|| anyhow::anyhow!("{what}: no {header:?} block"))?;
    let end = content[start..]
        .find("\n}\n")
        .map(|offset| start + offset + "\n}\n".len())
        .ok_or_else(|| anyhow::anyhow!("{what}: {header:?} has no end"))?;
    let block = &content[start..end];
    if !block.contains(needle) {
        anyhow::bail!(
            "{what}; the generator's output changed and a secret value would ship in a plain \
             String or in a query string"
        );
    }
    Ok(format!(
        "{}{}{}",
        &content[..start],
        block.replacen(needle, replacement, 1),
        &content[end..]
    ))
}

/// Prepend #![allow(clippy::all)] and #![allow(warnings)] to a file if not already present.
fn prepend_allow_attrs(path: &std::path::Path) -> Result<()> {
    let content = std::fs::read_to_string(path)?;
    if content.starts_with("#![allow(clippy::all)]") {
        return Ok(());
    }
    let new_content = format!("#![allow(clippy::all)]\n#![allow(warnings)]\n{content}");
    std::fs::write(path, new_content)?;
    Ok(())
}

/// Port of scripts/fix_rust_axum_duplicate_auth_trait.py
/// Removes duplicate ApiKeyAuthHeader trait blocks from generated code.
fn fix_duplicate_auth_trait(path: &std::path::Path) -> Result<()> {
    use std::sync::LazyLock;

    if !path.exists() {
        anyhow::bail!("file not found: {}", path.display());
    }

    let content = std::fs::read_to_string(path)?;

    static PATTERN: LazyLock<regex::Regex> = LazyLock::new(|| {
        regex::Regex::new(concat!(
            r"(?s)/// API Key Authentication - Header\.\r?\n",
            r"\s*#\[async_trait::async_trait\]\r?\n",
            r"\s*pub trait ApiKeyAuthHeader \{\r?\n",
            r"\s+type Claims;\r?\n\r?\n",
            r"\s*/// Extracting Claims from Header\. Return None if the Claims are invalid\.\r?\n",
            r"\s+async fn extract_claims_from_header\(&self, headers: &axum::http::header::HeaderMap, key: &str\) -> Option<Self::Claims>;\r?\n",
            r"\s*\}\r?\n\r?\n"
        ))
        .unwrap()
    });

    let matches: Vec<_> = PATTERN.find_iter(&content).collect();

    if matches.is_empty() {
        anyhow::bail!(
            "ApiKeyAuthHeader trait block not found in {}",
            path.display()
        );
    }

    if matches.len() == 1 {
        util::info(&format!("No duplicate trait blocks in {}", path.display()));
        return Ok(());
    }

    // Keep the first match, remove subsequent duplicates
    let first_end = matches[0].end();
    let before = &content[..first_end];
    let after = PATTERN.replace_all(&content[first_end..], "");
    let deduped = format!("{before}{after}");

    std::fs::write(path, deduped)?;
    util::info(&format!(
        "Removed {} duplicate ApiKeyAuthHeader trait block(s) in {}",
        matches.len() - 1,
        path.display()
    ));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The four places the generator writes one of the two secret fields,
    /// in the shape it writes them: both are optional, and `fields` is a map
    /// of a printing newtype.
    fn generated_model(model: &str) -> String {
        format!(
            "prelude\n\n{GENERATED_DERIVE}\n\
             #[cfg_attr(feature = \"conversion\", derive(frunk::LabelledGeneric))]\n\
             pub struct {model} {{\n    \
             #[serde(rename = \"value\")]\n    \
             #[validate(custom(function = \"check_xss_string\"))]\n    \
             #[serde(skip_serializing_if = \"Option::is_none\")]\n    \
             pub value: Option<String>,\n\n    \
             #[serde(rename = \"fields\")]\n    \
             #[validate(custom(function = \"check_xss_map_nested\"))]\n    \
             #[serde(skip_serializing_if = \"Option::is_none\")]\n    \
             pub fields: Option<std::collections::HashMap<String, models::SecretString>>,\n}}\n\n\
             impl {model} {{\n    \
             pub fn new(name: String) -> {model} {{\n        \
             {model} {{\n            \
             name,\n            \
             value: None,\n            \
             fields: None,\n        }}\n    }}\n}}\n\n\
             impl std::str::FromStr for {model} {{\n        \
             struct IntermediateRep {{\n            \
             pub value: Vec<String>,\n            \
             pub fields: Vec<std::collections::HashMap<String, models::SecretString>>,\n        \
             }}\n        \
             {model} {{\n            \
             value: intermediate_rep.value.into_iter().next(),\n            \
             fields: intermediate_rep.fields.into_iter().next(),\n        }}\n}}\n\n\
             impl std::fmt::Display for {model} {{\n            \
             self.value\n                .as_ref()\n                \
             .map(|value| [\"value\".to_string(), value.to_string()].join(\",\")),\n}}\n\ntail\n"
        )
    }

    #[test]
    fn both_secret_fields_lose_their_validator_and_their_printing_debug() {
        let rewritten = redact_secret_value_model(&generated_model("NewSecret"), "NewSecret")
            .expect("the generated shape is the one this step rewrites");

        assert!(!rewritten.contains("check_xss_string"));
        assert!(!rewritten.contains("check_xss_map_nested"));
        assert!(!rewritten.contains(GENERATED_DERIVE));
        assert!(rewritten.contains(REDACTED_DERIVE));
        assert!(rewritten.contains("f.write_str(\"NewSecret([redacted])\")"));
        assert!(rewritten.contains("pub value: Option<zeroize::Zeroizing<String>>,"));
        assert!(rewritten.contains(
            "pub fields: Option<std::collections::HashMap<String, \
                      zeroize::Zeroizing<String>>>,"
        ));
        assert!(
            !rewritten.contains("models::SecretString"),
            "that newtype's derived Debug prints, so no secret field may keep it"
        );
        assert!(
            !rewritten.contains("impl Drop for NewSecret"),
            "the conversion feature's LabelledGeneric moves out of the model, so the wipe \
             belongs to the field's type and not to the model"
        );
        assert!(rewritten.contains(
            ".map(|_| [\"value\".to_string(), \"[redacted]\".to_string()].join(\",\")),"
        ));
        assert!(!rewritten.contains("value.to_string()"));
        assert!(
            rewritten.contains("value: intermediate_rep.value.into_iter().next().map(Into::into),")
        );
        assert!(rewritten.starts_with("prelude\n"));
        assert!(rewritten.ends_with("tail\n"));
    }

    #[test]
    fn the_dependency_the_injected_drop_calls_is_added_once_and_only_once() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let cargo_toml = dir.path().join("Cargo.toml");
        std::fs::write(
            &cargo_toml,
            "[package]\nname = \"x\"\n\n[dependencies]\nserde = \"1\"\n\n[dev-dependencies]\ntracing-subscriber = \"0.3\"\n",
        )
        .unwrap();

        add_zeroize_dependency(&cargo_toml).unwrap();
        add_zeroize_dependency(&cargo_toml).unwrap();

        let written = std::fs::read_to_string(&cargo_toml).unwrap();
        assert_eq!(written.matches("\nzeroize = ").count(), 1);
        assert!(
            written.find("\nzeroize = ").unwrap() < written.find("[dev-dependencies]").unwrap(),
            "it belongs in [dependencies], not the dev ones"
        );
    }

    #[test]
    fn a_manifest_without_the_anchor_is_an_error_rather_than_a_silent_skip() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let cargo_toml = dir.path().join("Cargo.toml");
        std::fs::write(&cargo_toml, "[package]\nname = \"x\"\n").unwrap();
        assert!(add_zeroize_dependency(&cargo_toml).is_err());
    }

    #[test]
    fn a_generator_that_stopped_writing_either_anchor_is_an_error() {
        let no_struct = generated_model("NewSecret").replace("NewSecret", "SomethingElse");
        assert!(redact_secret_value_model(&no_struct, "NewSecret").is_err());

        for gone in [
            "    #[validate(custom(function = \"check_xss_string\"))]\n",
            "    #[validate(custom(function = \"check_xss_map_nested\"))]\n",
            "            pub fields: Vec<std::collections::HashMap<String, \
             models::SecretString>>,\n",
            "            value: intermediate_rep.value.into_iter().next(),\n",
            ".map(|value| [\"value\".to_string(), value.to_string()].join(\",\")),",
        ] {
            let without = generated_model("NewSecret").replace(gone, "");
            assert!(
                redact_secret_value_model(&without, "NewSecret").is_err(),
                "a missing {gone:?} must be an error, not a silent skip"
            );
        }
    }
}
