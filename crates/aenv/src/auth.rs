use anyhow::{bail, Context, Result};
use directories::ProjectDirs;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io;
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Credentials {
    /// REST entry point: sandbox, snapshot, template and node calls.
    pub url: String,
    /// Data-plane entry point for sandbox traffic. Absent means `url` carries
    /// both, which is what a credentials file written before the split says.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proxy_url: Option<String>,
    pub api_key: String,
}

impl Credentials {
    /// The address sandbox data-plane traffic is sent to.
    pub fn data_plane_url(&self) -> &str {
        self.proxy_url
            .as_deref()
            .map(str::trim)
            .filter(|url| !url.is_empty())
            .unwrap_or(&self.url)
    }
}

fn credentials_path() -> Result<PathBuf> {
    let dirs = ProjectDirs::from("", "", "aenv").context("could not determine config directory")?;
    Ok(dirs.config_dir().join("credentials"))
}

pub fn load() -> Result<Credentials> {
    let path = credentials_path()?;
    let text = match fs::read_to_string(&path) {
        Ok(t) => t,
        Err(e) if e.kind() == io::ErrorKind::NotFound => bail!(
            "not authenticated — run `aenv auth` first (expected credentials at {})",
            path.display()
        ),
        Err(e) => return Err(e).with_context(|| format!("reading {}", path.display())),
    };
    let creds: Credentials =
        toml::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;
    Ok(creds)
}

pub fn save(creds: &Credentials) -> Result<()> {
    let path = credentials_path()?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;
    }
    let text = toml::to_string(creds)?;
    fs::write(&path, text).with_context(|| format!("writing {}", path.display()))?;
    restrict_permissions(&path)?;
    Ok(())
}

#[cfg(unix)]
fn restrict_permissions(path: &std::path::Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mut perms = fs::metadata(path)?.permissions();
    perms.set_mode(0o600);
    fs::set_permissions(path, perms)?;
    Ok(())
}

#[cfg(not(unix))]
fn restrict_permissions(_path: &std::path::Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::Credentials;

    #[test]
    fn a_credentials_file_written_before_the_split_still_loads() {
        let creds: Credentials = toml::from_str("url = \"http://gateway:8000\"\napi_key = \"k\"\n")
            .expect("an older credentials file parses");

        assert_eq!(
            creds.data_plane_url(),
            "http://gateway:8000",
            "🔴 one address must keep carrying both surfaces, or every existing \
             install loses its data plane"
        );
    }

    #[test]
    fn a_configured_proxy_url_carries_the_data_plane() {
        let creds: Credentials = toml::from_str(
            "url = \"http://api:8010\"\nproxy_url = \"http://gateway:8000\"\napi_key = \"k\"\n",
        )
        .expect("a two-address credentials file parses");

        assert_eq!(creds.url, "http://api:8010");
        assert_eq!(creds.data_plane_url(), "http://gateway:8000");
    }

    #[test]
    fn a_blank_proxy_url_falls_back_rather_than_addressing_nothing() {
        let creds: Credentials =
            toml::from_str("url = \"http://api:8010\"\nproxy_url = \"  \"\napi_key = \"k\"\n")
                .expect("it parses");

        assert_eq!(creds.data_plane_url(), "http://api:8010");
    }

    #[test]
    fn an_unset_proxy_url_is_not_written_back() {
        let text = toml::to_string(&Credentials {
            url: "http://gateway:8000".to_string(),
            proxy_url: None,
            api_key: "k".to_string(),
        })
        .expect("it serializes");

        assert!(!text.contains("proxy_url"), "got {text}");
    }
}
