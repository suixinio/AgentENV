use crate::auth::{save, Credentials};
use anyhow::Result;
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

    write!(stdout, "Sandbox proxy URL [same as server URL]: ")?;
    stdout.flush()?;
    let mut proxy_url = String::new();
    stdin.lock().read_line(&mut proxy_url)?;
    let proxy_url = proxy_url.trim();
    let proxy_url = (!proxy_url.is_empty() && proxy_url != url).then(|| proxy_url.to_string());

    let api_key = rpassword::prompt_password("API key: ")?;
    let api_key = api_key.trim().to_string();
    if api_key.is_empty() {
        anyhow::bail!("API key cannot be empty");
    }

    save(&Credentials {
        url: url.to_string(),
        proxy_url,
        api_key,
    })?;
    println!("Credentials saved.");
    Ok(())
}
