use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::Path;

use anyhow::{bail, Context, Result};
use hmac::{Hmac, Mac};
use rand::{rngs::SysRng, TryRng};
use sha2::{Digest, Sha256};
use tracing::{info, warn};

use crate::cfg::AppConfig;
use crate::types::SandboxId;

type HmacSha256 = Hmac<Sha256>;

const MANAGED_SEED_RELATIVE_PATH: &str = "secrets/sandbox-access-token-hash-seed";
const MANAGED_SEED_BYTES: usize = 32;
const SEED_HEX_LEN: usize = MANAGED_SEED_BYTES * 2;
const MANAGED_SEED_FILE_MAX_LEN: usize = SEED_HEX_LEN + 1;

const SEED_ENV_VAR: &str = "AENV_SANDBOX_ACCESS_TOKEN_HASH_SEED";

/// Bytes of `SHA-256(seed)` exposed in the comparison-only metric label.
const SEED_FINGERPRINT_BYTES: usize = 8;

/// Gauge label used to compare configured seeds across replicas.
const SEED_FINGERPRINT_METRIC: &str = "agentenv_access_token_seed_fingerprint";

/// Whether this process may generate an envd access-token seed.
///
/// Replicas must share the seed because tokens are `HMAC(seed, sandbox_id)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AccessTokenSeedPolicy {
    /// A single machine may generate and persist its own seed.
    MayGenerate,
    /// A replicated process must be given the shared seed.
    MustBeConfigured,
}

impl AccessTokenSeedPolicy {
    fn refuses_an_invented_seed(self) -> bool {
        matches!(self, Self::MustBeConfigured)
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct EnvdAccessToken(String);

impl EnvdAccessToken {
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for EnvdAccessToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("EnvdAccessToken(<redacted>)")
    }
}

#[derive(Clone)]
pub struct SandboxAccessTokenGenerator {
    seed: Vec<u8>,
}

impl SandboxAccessTokenGenerator {
    pub fn new(seed: &str) -> Result<Self> {
        let seed = validate_explicit_seed(seed)?;
        Ok(Self {
            seed: seed.as_bytes().to_vec(),
        })
    }

    /// Resolves the signing seed and publishes its fingerprint.
    pub fn load_or_create(
        config: &AppConfig,
        seed_policy: AccessTokenSeedPolicy,
        managed_seed_must_exist: bool,
    ) -> Result<Self> {
        let generator = Self::resolve(config, seed_policy, managed_seed_must_exist)?;
        generator.publish_seed_fingerprint(seed_policy);
        Ok(generator)
    }

    fn resolve(
        config: &AppConfig,
        seed_policy: AccessTokenSeedPolicy,
        managed_seed_must_exist: bool,
    ) -> Result<Self> {
        if let Some(seed) = config.sandbox.access_token_hash_seed.as_deref() {
            return Self::new(seed);
        }

        // Reject before the node-local managed-seed fallback.
        if seed_policy.refuses_an_invented_seed() {
            bail!(
                "aenv-api has no envd access-token seed configured. Set {SEED_ENV_VAR} (or \
                 [sandbox].access_token_hash_seed) to the same value on every replica, and do \
                 not let this process generate one: envd access tokens are \
                 HMAC(seed, sandbox_id), so a seed invented here would make this replica hand out \
                 tokens its siblings reject and reject the ones they handed out — silently, on \
                 whichever request the load balancer sent where. In Kubernetes the value is the \
                 `sandbox-access-token-hash-seed` key of the `agentenv-runtime-secrets` Secret, \
                 which deploy/k8s/base/agentenv-api-deployment.yaml already reads with \
                 `optional: false`; create the Secret before rolling out the Deployment."
            );
        }

        let managed_seed_path = config.home_path.join(MANAGED_SEED_RELATIVE_PATH);
        let seed = resolve_seed(&managed_seed_path, managed_seed_must_exist)?;

        if config.cluster.scheduler_endpoint.is_some() {
            warn!(
                path = %managed_seed_path.display(),
                "using a node-local managed envd access-token seed; configure AENV_SANDBOX_ACCESS_TOKEN_HASH_SEED with the same value on every node before enabling cross-node sandbox recovery"
            );
        }

        Self::new(&seed)
    }

    /// Returns the comparison fingerprint: the leading hash bytes in lowercase hex.
    pub fn seed_fingerprint(&self) -> String {
        let digest = Sha256::digest(&self.seed);
        hex::encode(&digest[..SEED_FINGERPRINT_BYTES])
    }

    fn publish_seed_fingerprint(&self, seed_policy: AccessTokenSeedPolicy) {
        let fingerprint = self.seed_fingerprint();
        // Only the fingerprint reaches logs and metrics.
        info!(
            seed_policy = ?seed_policy,
            fingerprint = %fingerprint,
            "resolved the envd access-token seed"
        );
        metrics::gauge!(SEED_FINGERPRINT_METRIC, "fingerprint" => fingerprint).set(1.0);
    }

    pub fn generate(&self, subject: SandboxId) -> EnvdAccessToken {
        let mut mac =
            HmacSha256::new_from_slice(&self.seed).expect("HMAC accepts keys of any length");
        mac.update(subject.to_string().as_bytes());
        EnvdAccessToken(hex::encode(mac.finalize().into_bytes()))
    }

    pub fn matches(&self, subject: SandboxId, candidate: &str) -> bool {
        let mut candidate_bytes = [0_u8; 32];
        let decoded = hex::decode_to_slice(candidate, &mut candidate_bytes).is_ok();
        let mut mac =
            HmacSha256::new_from_slice(&self.seed).expect("HMAC accepts keys of any length");
        mac.update(subject.to_string().as_bytes());
        mac.verify_slice(&candidate_bytes).is_ok() & decoded
    }
}

fn validate_explicit_seed(seed: &str) -> Result<&str> {
    let seed = seed.trim();
    if seed.is_empty() {
        bail!("[sandbox].access_token_hash_seed must be non-empty when configured");
    }
    Ok(seed)
}

fn resolve_seed(managed_path: &Path, managed_seed_must_exist: bool) -> Result<String> {
    let parent = managed_path
        .parent()
        .context("managed envd access-token seed path has no parent")?;
    match validate_managed_seed_directory(parent) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error).with_context(|| {
                format!("validate managed secret directory {}", parent.display())
            });
        }
    }

    match open_managed_seed(managed_path) {
        Ok(file) => return read_managed_seed(managed_path, file),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error).with_context(|| {
                format!(
                    "open managed envd access-token seed {}",
                    managed_path.display()
                )
            });
        }
    }

    if managed_seed_must_exist {
        bail!(
            "managed envd access-token seed {} is missing while persisted secure sandboxes exist; restore the file or configure AENV_SANDBOX_ACCESS_TOKEN_HASH_SEED",
            managed_path.display()
        );
    }

    create_managed_seed(managed_path)
}

fn open_managed_seed(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);

    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }

    options.open(path)
}

fn validate_managed_seed_file(path: &Path, file: &File) -> Result<fs::Metadata> {
    let metadata = file
        .metadata()
        .with_context(|| format!("inspect managed envd access-token seed {}", path.display()))?;
    if !metadata.is_file() {
        bail!(
            "managed envd access-token seed {} must be a regular file",
            path.display()
        );
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        let mode = metadata.permissions().mode() & 0o777;
        if mode != 0o600 {
            bail!(
                "managed envd access-token seed {} must have permissions 0600, found {mode:04o}",
                path.display()
            );
        }
        let expected_uid = nix::unistd::Uid::effective().as_raw();
        if metadata.uid() != expected_uid {
            bail!(
                "managed envd access-token seed {} must be owned by uid {expected_uid}, found uid {}",
                path.display(),
                metadata.uid()
            );
        }
    }

    Ok(metadata)
}

fn read_managed_seed(path: &Path, mut file: File) -> Result<String> {
    let metadata = validate_managed_seed_file(path, &file)?;

    if metadata.len() > MANAGED_SEED_FILE_MAX_LEN as u64 {
        bail!(
            "managed envd access-token seed {} must be at most {MANAGED_SEED_FILE_MAX_LEN} bytes",
            path.display()
        );
    }

    let mut contents = String::with_capacity(MANAGED_SEED_FILE_MAX_LEN);
    Read::by_ref(&mut file)
        .take((MANAGED_SEED_FILE_MAX_LEN + 1) as u64)
        .read_to_string(&mut contents)
        .with_context(|| format!("read managed envd access-token seed {}", path.display()))?;
    if contents.len() > MANAGED_SEED_FILE_MAX_LEN {
        bail!(
            "managed envd access-token seed {} must be at most {MANAGED_SEED_FILE_MAX_LEN} bytes",
            path.display()
        );
    }
    let seed = contents.strip_suffix('\n').unwrap_or(&contents);
    if !is_valid_managed_seed(seed) {
        bail!(
            "managed envd access-token seed {} must contain exactly {SEED_HEX_LEN} lowercase hexadecimal characters, optionally followed by a newline",
            path.display()
        );
    }

    Ok(seed.to_owned())
}

fn create_managed_seed(path: &Path) -> Result<String> {
    let parent = path
        .parent()
        .context("managed envd access-token seed path has no parent")?;
    create_private_directory(parent)
        .with_context(|| format!("create managed secret directory {}", parent.display()))?;
    validate_managed_seed_directory(parent)
        .with_context(|| format!("validate managed secret directory {}", parent.display()))?;

    let mut random = [0_u8; MANAGED_SEED_BYTES];
    SysRng
        .try_fill_bytes(&mut random)
        .context("generate managed envd access-token seed")?;
    let seed = hex::encode(random);

    let mut temporary = tempfile::NamedTempFile::new_in(parent)
        .with_context(|| format!("create temporary seed file in {}", parent.display()))?;
    set_permissions(temporary.path(), 0o600)?;
    writeln!(temporary, "{seed}")
        .with_context(|| format!("write temporary seed file in {}", parent.display()))?;
    temporary
        .as_file()
        .sync_all()
        .with_context(|| format!("sync temporary seed file in {}", parent.display()))?;

    match temporary.persist_noclobber(path) {
        Ok(_) => {
            fs::File::open(parent)
                .and_then(|directory| directory.sync_all())
                .with_context(|| format!("sync managed secret directory {}", parent.display()))?;
            info!(path = %path.display(), "generated managed envd access-token seed");
            Ok(seed)
        }
        Err(error) if error.error.kind() == io::ErrorKind::AlreadyExists => {
            let file = open_managed_seed(path).with_context(|| {
                format!("open managed envd access-token seed {}", path.display())
            })?;
            read_managed_seed(path, file)
        }
        Err(error) => Err(error.error)
            .with_context(|| format!("persist managed envd access-token seed {}", path.display())),
    }
}

fn is_valid_managed_seed(seed: &str) -> bool {
    seed.len() == SEED_HEX_LEN
        && seed
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

/// Atomically creates only the leaf directory at mode 0700.
///
/// Existing paths are validated by the caller; restrictive umasks are not corrected afterward.
fn create_private_directory(path: &Path) -> io::Result<()> {
    if let Some(ancestors) = path.parent() {
        fs::create_dir_all(ancestors)?;
    }

    let mut builder = fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;

        builder.mode(0o700);
    }

    match builder.create(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => Ok(()),
        Err(error) => Err(error),
    }
}

fn validate_managed_seed_directory(path: &Path) -> io::Result<()> {
    let metadata = validate_managed_seed_directory_identity(path)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mode = metadata.permissions().mode() & 0o777;
        if mode != 0o700 {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!("must have permissions 0700, found {mode:04o}"),
            ));
        }
    }

    Ok(())
}

fn validate_managed_seed_directory_identity(path: &Path) -> io::Result<fs::Metadata> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "must be a directory and not a symbolic link",
        ));
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;

        let expected_uid = nix::unistd::Uid::effective().as_raw();
        if metadata.uid() != expected_uid {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!(
                    "must be owned by uid {expected_uid}, found uid {}",
                    metadata.uid()
                ),
            ));
        }
    }

    Ok(metadata)
}

#[cfg(unix)]
fn set_permissions(path: &Path, mode: u32) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(path, fs::Permissions::from_mode(mode))
        .with_context(|| format!("set permissions on {}", path.display()))
}

#[cfg(not(unix))]
fn set_permissions(_path: &Path, _mode: u32) -> Result<()> {
    Ok(())
}

impl fmt::Debug for SandboxAccessTokenGenerator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SandboxAccessTokenGenerator(<redacted>)")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};

    use tempfile::TempDir;

    fn create_private_managed_seed_directory(path: &Path) -> Result<()> {
        Ok(create_private_directory(path)?)
    }

    #[test]
    #[cfg(unix)]
    fn the_secret_directory_is_private_from_the_moment_it_exists() -> Result<()> {
        use std::os::unix::fs::PermissionsExt;

        let temp = TempDir::new()?;
        let secrets = temp.path().join("nested").join("secrets");

        create_private_directory(&secrets)?;

        let mode = fs::symlink_metadata(&secrets)?.permissions().mode() & 0o777;
        assert_eq!(mode, 0o700, "found {mode:04o}");

        let ancestor = fs::symlink_metadata(temp.path().join("nested"))?
            .permissions()
            .mode()
            & 0o777;
        assert_ne!(ancestor, 0o700, "only the leaf should be locked down");

        create_private_directory(&secrets)?;

        Ok(())
    }

    #[test]
    fn generates_lowercase_hex_hmac_sha256() {
        let generator = SandboxAccessTokenGenerator::new("test-seed").unwrap();
        let subject = SandboxId::try_from("01936f8e-72f5-7000-8000-000000000001").unwrap();

        let token = generator.generate(subject);

        assert_eq!(token.expose().len(), 64);
        assert_eq!(
            token.expose(),
            "4f00f2a93a87c37161ae01c59b6d4f84506668113441277e9f6272dd4bfae1a7"
        );
        assert!(token.expose().bytes().all(|byte| byte.is_ascii_hexdigit()));
        assert_eq!(token.expose(), token.expose().to_ascii_lowercase());
        assert!(generator.matches(subject, token.expose()));
        assert!(!generator.matches(subject, "not-a-token"));
        assert!(!generator.matches(subject, &"0".repeat(64)));
    }

    #[test]
    fn rejects_empty_seed_and_redacts_secrets() {
        assert!(SandboxAccessTokenGenerator::new("  ").is_err());
        let generator = SandboxAccessTokenGenerator::new("super-secret").unwrap();
        let subject = SandboxId::default();
        let token = generator.generate(subject);

        assert!(!format!("{generator:?}").contains("super-secret"));
        assert!(!format!("{token:?}").contains(token.expose()));
    }

    fn config_with_seed(home: &Path, seed: Option<&str>) -> AppConfig {
        AppConfig {
            home_path: home.to_owned(),
            sandbox: crate::cfg::SandboxConfig {
                access_token_hash_seed: seed.map(str::to_owned),
            },
            ..Default::default()
        }
    }

    #[test]
    fn explicit_seed_takes_precedence_without_creating_managed_state() -> Result<()> {
        let temp = TempDir::new()?;
        let managed_path = temp.path().join(MANAGED_SEED_RELATIVE_PATH);
        let config = config_with_seed(temp.path(), Some("configured-seed"));

        let generator = SandboxAccessTokenGenerator::load_or_create(
            &config,
            AccessTokenSeedPolicy::MayGenerate,
            false,
        )?;

        assert_eq!(generator.seed, "configured-seed".as_bytes());
        assert!(!managed_path.exists());
        Ok(())
    }

    #[test]
    fn the_api_half_refuses_to_invent_a_seed_and_the_node_half_still_may() -> Result<()> {
        {
            let temp = TempDir::new()?;
            let managed_path = temp.path().join(MANAGED_SEED_RELATIVE_PATH);
            let config = config_with_seed(temp.path(), None);

            let generator = SandboxAccessTokenGenerator::load_or_create(
                &config,
                AccessTokenSeedPolicy::MayGenerate,
                false,
            )
            .unwrap_or_else(|error| panic!("aenv-node should still generate: {error:#}"));

            assert_eq!(generator.seed.len(), SEED_HEX_LEN);
            assert!(
                managed_path.exists(),
                "aenv-node did not write the managed seed it generated"
            );
        }

        let temp = TempDir::new()?;
        let managed_path = temp.path().join(MANAGED_SEED_RELATIVE_PATH);
        let config = config_with_seed(temp.path(), None);

        let error = SandboxAccessTokenGenerator::load_or_create(
            &config,
            AccessTokenSeedPolicy::MustBeConfigured,
            false,
        )
        .expect_err("aenv-api must not invent a seed its siblings cannot derive");

        let message = format!("{error:#}");
        assert!(message.contains(SEED_ENV_VAR), "{message}");
        assert!(message.contains("same value on every replica"), "{message}");
        assert!(message.contains("agentenv-runtime-secrets"), "{message}");
        assert!(
            !managed_path.exists(),
            "aenv-api generated a seed on its way to refusing"
        );
        Ok(())
    }

    #[test]
    fn a_configured_seed_is_what_the_api_half_wants_and_a_blank_one_is_not() -> Result<()> {
        let temp = TempDir::new()?;

        let configured = config_with_seed(temp.path(), Some("cluster-wide-seed"));
        let generator = SandboxAccessTokenGenerator::load_or_create(
            &configured,
            AccessTokenSeedPolicy::MustBeConfigured,
            false,
        )?;
        assert_eq!(generator.seed, "cluster-wide-seed".as_bytes());

        let blank = config_with_seed(temp.path(), Some("   "));
        let error = SandboxAccessTokenGenerator::load_or_create(
            &blank,
            AccessTokenSeedPolicy::MustBeConfigured,
            false,
        )
        .expect_err("a whitespace-only seed is not a seed");
        assert!(
            format!("{error:#}").contains("must be non-empty"),
            "{error:#}"
        );

        assert!(!temp.path().join(MANAGED_SEED_RELATIVE_PATH).exists());
        Ok(())
    }

    #[test]
    fn the_fingerprint_tells_two_seeds_apart_and_agrees_with_itself() -> Result<()> {
        let alpha = SandboxAccessTokenGenerator::new("seed-alpha")?;
        let alpha_again = SandboxAccessTokenGenerator::new("seed-alpha")?;
        let beta = SandboxAccessTokenGenerator::new("seed-beta")?;

        assert_eq!(alpha.seed_fingerprint(), alpha_again.seed_fingerprint());
        assert_ne!(
            alpha.seed_fingerprint(),
            beta.seed_fingerprint(),
            "a fingerprint two different seeds share cannot answer whether two replicas agree"
        );

        assert_eq!(alpha.seed_fingerprint(), "ba316cd7abc9b7dc");
        assert_eq!(beta.seed_fingerprint(), "d9c30acfd5686611");
        assert_eq!(alpha.seed_fingerprint().len(), SEED_FINGERPRINT_BYTES * 2);

        assert!(!alpha.seed_fingerprint().contains("seed-alpha"));
        assert!(alpha
            .seed_fingerprint()
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)));
        Ok(())
    }

    #[test]
    fn the_seed_fingerprint_is_published_where_two_replicas_can_be_compared() -> Result<()> {
        let temp = TempDir::new()?;

        let first = published_fingerprints(&config_with_seed(temp.path(), Some("replica-seed")))?;
        let second = published_fingerprints(&config_with_seed(temp.path(), Some("replica-seed")))?;
        let divergent = published_fingerprints(&config_with_seed(temp.path(), Some("other-seed")))?;

        assert_eq!(
            first,
            vec![SandboxAccessTokenGenerator::new("replica-seed")?.seed_fingerprint()],
            "exactly one fingerprint should be published, and it should be this seed's"
        );
        assert_eq!(first, second, "two replicas holding one seed must agree");
        assert_ne!(
            first, divergent,
            "a third replica with its own seed must be distinguishable, or the gauge is a constant"
        );
        assert!(!first[0].contains("replica-seed"));
        Ok(())
    }

    fn published_fingerprints(config: &AppConfig) -> Result<Vec<String>> {
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let guard = metrics::set_default_local_recorder(&recorder);
        let loaded = SandboxAccessTokenGenerator::load_or_create(
            config,
            AccessTokenSeedPolicy::MustBeConfigured,
            false,
        );
        drop(guard);
        loaded?;

        Ok(snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .filter(|(composite, _unit, _description, value)| {
                composite.key().name() == SEED_FINGERPRINT_METRIC
                    && matches!(value, DebugValue::Gauge(reading) if reading.into_inner() == 1.0)
            })
            .map(|(composite, _unit, _description, _value)| {
                composite
                    .key()
                    .labels()
                    .find(|label| label.key() == "fingerprint")
                    .map_or_else(
                        || "<no fingerprint label>".to_owned(),
                        |label| label.value().to_owned(),
                    )
            })
            .collect())
    }

    #[test]
    fn managed_seed_is_private_and_stable() -> Result<()> {
        let temp = TempDir::new()?;
        let managed_path = temp.path().join(MANAGED_SEED_RELATIVE_PATH);

        let first = resolve_seed(&managed_path, false)?;
        let second = resolve_seed(&managed_path, false)?;

        assert_eq!(first, second);
        assert_eq!(first.len(), 64);
        assert!(first
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)));

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let directory_mode = fs::metadata(managed_path.parent().unwrap())?
                .permissions()
                .mode()
                & 0o777;
            let file_mode = fs::metadata(&managed_path)?.permissions().mode() & 0o777;
            assert_eq!(directory_mode, 0o700);
            assert_eq!(file_mode, 0o600);
        }

        let subject = SandboxId::new();
        let first_generator = SandboxAccessTokenGenerator::new(&first)?;
        let second_generator = SandboxAccessTokenGenerator::new(&second)?;
        assert_eq!(
            first_generator.generate(subject),
            second_generator.generate(subject)
        );
        Ok(())
    }

    #[test]
    fn concurrent_managed_seed_creation_converges() -> Result<()> {
        const THREADS: usize = 8;

        let temp = TempDir::new()?;
        let managed_path = Arc::new(temp.path().join(MANAGED_SEED_RELATIVE_PATH));
        let barrier = Arc::new(Barrier::new(THREADS));
        let handles = (0..THREADS)
            .map(|_| {
                let managed_path = Arc::clone(&managed_path);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    resolve_seed(&managed_path, false)
                })
            })
            .collect::<Vec<_>>();

        let seeds = handles
            .into_iter()
            .map(|handle| handle.join().expect("seed creation thread panicked"))
            .collect::<Result<Vec<_>>>()?;
        assert!(seeds.iter().all(|seed| seed == &seeds[0]));
        Ok(())
    }

    #[test]
    fn empty_or_invalid_managed_seed_is_not_replaced() -> Result<()> {
        let temp = TempDir::new()?;
        let managed_path = temp.path().join(MANAGED_SEED_RELATIVE_PATH);
        create_private_managed_seed_directory(managed_path.parent().unwrap())?;

        for contents in ["", "invalid\n"] {
            fs::write(&managed_path, contents)?;
            set_permissions(&managed_path, 0o600)?;

            let error = resolve_seed(&managed_path, false).unwrap_err();

            assert!(error.to_string().contains("64 lowercase hexadecimal"));
            assert_eq!(fs::read_to_string(&managed_path)?, contents);
        }
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn permissive_managed_seed_is_rejected() -> Result<()> {
        let temp = TempDir::new()?;
        let managed_path = temp.path().join(MANAGED_SEED_RELATIVE_PATH);
        create_private_managed_seed_directory(managed_path.parent().unwrap())?;
        fs::write(&managed_path, format!("{}\n", "a".repeat(64)))?;
        set_permissions(&managed_path, 0o640)?;

        let error = resolve_seed(&managed_path, false).unwrap_err();

        assert!(error.to_string().contains("permissions 0600"));
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn managed_seed_symlink_is_rejected() -> Result<()> {
        use std::os::unix::fs::symlink;

        let temp = TempDir::new()?;
        let managed_path = temp.path().join(MANAGED_SEED_RELATIVE_PATH);
        let target_path = temp.path().join("seed-target");
        create_private_managed_seed_directory(managed_path.parent().unwrap())?;
        fs::write(&target_path, format!("{}\n", "a".repeat(64)))?;
        set_permissions(&target_path, 0o600)?;
        symlink(&target_path, &managed_path)?;

        let error = resolve_seed(&managed_path, false).unwrap_err();

        assert!(error
            .to_string()
            .contains("open managed envd access-token seed"));
        Ok(())
    }

    #[test]
    fn oversized_managed_seed_is_rejected_before_reading_contents() -> Result<()> {
        let temp = TempDir::new()?;
        let managed_path = temp.path().join(MANAGED_SEED_RELATIVE_PATH);
        create_private_managed_seed_directory(managed_path.parent().unwrap())?;
        let file = File::create(&managed_path)?;
        file.set_len(1024 * 1024)?;
        set_permissions(&managed_path, 0o600)?;

        let error = resolve_seed(&managed_path, false).unwrap_err();

        assert!(error.to_string().contains("must be at most 65 bytes"));
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn permissive_managed_seed_directory_is_rejected() -> Result<()> {
        let temp = TempDir::new()?;
        let managed_path = temp.path().join(MANAGED_SEED_RELATIVE_PATH);
        fs::create_dir_all(managed_path.parent().unwrap())?;
        set_permissions(managed_path.parent().unwrap(), 0o770)?;

        let error = resolve_seed(&managed_path, false).unwrap_err();

        assert!(format!("{error:#}").contains("permissions 0700"));
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn managed_seed_directory_symlink_is_rejected() -> Result<()> {
        use std::os::unix::fs::symlink;

        let temp = TempDir::new()?;
        let managed_path = temp.path().join(MANAGED_SEED_RELATIVE_PATH);
        let target_directory = temp.path().join("target-secrets");
        create_private_managed_seed_directory(&target_directory)?;
        symlink(&target_directory, managed_path.parent().unwrap())?;

        let error = resolve_seed(&managed_path, false).unwrap_err();

        assert!(format!("{error:#}").contains("not a symbolic link"));
        Ok(())
    }

    #[test]
    fn missing_managed_seed_is_not_recreated_for_secure_state() -> Result<()> {
        let temp = TempDir::new()?;
        let managed_path = temp.path().join(MANAGED_SEED_RELATIVE_PATH);

        let error = resolve_seed(&managed_path, true).unwrap_err();

        assert!(error
            .to_string()
            .contains("persisted secure sandboxes exist"));
        assert!(!managed_path.exists());
        Ok(())
    }
}
