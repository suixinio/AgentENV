use crate::auth::{save, Credentials};
use anyhow::{bail, Result};
use std::io::{self, BufRead, Write};

const DEFAULT_URL: &str = "http://localhost:8000";

pub fn run() -> Result<()> {
    let stdin = io::stdin();
    let mut stdout = io::stdout();

    write!(stdout, "AENV server URL [{}]: ", DEFAULT_URL)?;
    stdout.flush()?;
    let mut url = String::new();
    stdin.lock().read_line(&mut url)?;
    let url = url.trim();
    let url = if url.is_empty() { DEFAULT_URL } else { url };

    write!(stdout, "Sandbox proxy URL: ")?;
    stdout.flush()?;
    let mut proxy_url = String::new();
    stdin.lock().read_line(&mut proxy_url)?;

    let api_key = rpassword::prompt_password("API key: ")?;

    save(&credentials_from_answers(url, &proxy_url, &api_key)?)?;
    println!("Credentials saved.");
    Ok(())
}

/// Turns the prompt answers into a credentials record.
///
/// The proxy URL has no default: nothing serves both surfaces, so there is no
/// address to fall back to. It is stored as given, even when it equals `url`,
/// because one Ingress name may legitimately front both.
fn credentials_from_answers(url: &str, proxy_url: &str, api_key: &str) -> Result<Credentials> {
    let proxy_url = proxy_url.trim();
    if proxy_url.is_empty() {
        bail!(
            "sandbox proxy URL cannot be empty: it is the data-plane (gateway) address, \
             which is separate from the REST address"
        );
    }
    let api_key = api_key.trim();
    if api_key.is_empty() {
        bail!("API key cannot be empty");
    }
    Ok(Credentials {
        url: url.to_string(),
        proxy_url: Some(proxy_url.to_string()),
        api_key: api_key.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::credentials_from_answers;

    #[test]
    fn a_proxy_url_equal_to_the_server_url_is_kept() {
        let creds = credentials_from_answers("http://one:8000", "http://one:8000\n", "k")
            .expect("one Ingress name may front both surfaces");

        assert_eq!(creds.proxy_url.as_deref(), Some("http://one:8000"));
        assert_eq!(creds.data_plane_url().unwrap(), "http://one:8000");
    }

    #[test]
    fn a_blank_proxy_url_is_refused_and_named_as_the_data_plane_address() {
        let err = credentials_from_answers("http://api:8010", "  \n", "k")
            .expect_err("there is no address to default to");

        let message = err.to_string();
        assert!(message.contains("data-plane"), "got: {message}");
        assert!(
            message.contains("separate from the REST address"),
            "got: {message}"
        );
    }

    #[test]
    fn a_blank_api_key_is_refused() {
        let err = credentials_from_answers("http://api:8010", "http://gateway:8000", " ")
            .expect_err("an empty key authenticates nothing");

        assert!(err.to_string().contains("API key"), "got: {err}");
    }
}
