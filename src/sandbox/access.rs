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

/// The name of the environment variable an operator sets, quoted in the refusal
/// so the message can be acted on without opening the configuration reference.
const SEED_ENV_VAR: &str = "AENV_SANDBOX_ACCESS_TOKEN_HASH_SEED";

/// How many leading bytes of `SHA-256(seed)` stand in for the seed in
/// [`SEED_FINGERPRINT_METRIC`].
///
/// 🔴 A prefix of a hash, never the seed. What the label has to support is one
/// question — "do these two processes hold the same seed?" — and eight bytes
/// answer it. It is not a secret in the sense the seed is, but it is also not
/// nothing: a seed guessable from a short list stays guessable through its
/// hash, so this is a comparison aid and not a reason to relax how the seed
/// itself is handled.
const SEED_FINGERPRINT_BYTES: usize = 8;

/// The gauge that makes "every replica holds the same seed" answerable from a
/// scrape.
///
/// 🔴 Always `1`, and the value is not the point: the *label* is. A missing
/// seed is caught at startup by [`AccessTokenSeedPolicy::MustBeConfigured`],
/// but two replicas each configured with a different non-empty seed pass every
/// check there is and still hand users tokens the other one rejects. Comparing
/// this label across replicas is the only place that divergence is visible
/// (`_sd-impl-phase3-role.md` §9.3 item 4).
const SEED_FINGERPRINT_METRIC: &str = "agentenv_access_token_seed_fingerprint";

/// Whether this process may invent its own envd access-token seed when none is
/// configured.
///
/// envd access tokens are `HMAC(seed, sandbox_id)`, so the seed is not a private
/// detail of the process that holds it: it is the only thing that makes two
/// processes agree on what a sandbox's token is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AccessTokenSeedPolicy {
    /// A single machine, which generates a seed and keeps it under
    /// `$AENV_HOME/secrets/`. What `aenv-node` passes, and today's behaviour
    /// verbatim: a developer running one should not need a secret to boot.
    ///
    /// Cross-*node* agreement still matters for cross-node recovery, but that
    /// is a warning's job, not a refusal's — the deployment that needs it is
    /// not the deployment that is broken without it. What tells the two apart
    /// on a live cluster is the fingerprint gauge, not this;
    /// see [`SandboxAccessTokenGenerator`].
    MayGenerate,
    /// A replicated process, which must be *handed* the seed. What `aenv-api`
    /// passes.
    ///
    /// 🔴 About replication rather than about being the deciding half. An
    /// `aenv-api` Deployment runs more than one replica, and every one of them
    /// mints tokens (`create`, `fork`), re-derives them (`resume`) and hands
    /// them back (`GET /sandboxes/{id}`). Two replicas with two invented seeds
    /// do not disagree loudly — the user is handed a token by whichever replica
    /// the load balancer picked, and it stops working the moment another one
    /// answers, with no error, no log and no metric
    /// (`_sd-impl-phase3-role.md` §9.2). Refusing to start is the only form of
    /// that fault anybody sees.
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

    /// Resolves the seed this process signs envd access tokens with, and
    /// publishes its fingerprint.
    ///
    /// The fingerprint is published by *both* halves. Scraping it from two
    /// nodes answers "do these two agree?", which is the same question two
    /// `aenv-api` replicas need answered.
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

        // 🔴 Before the managed file is even looked for. Falling back to a
        // node-local seed is the failure, not a step on the way to it: the
        // process would come up, serve requests, and mint tokens no sibling
        // replica can verify.
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

    /// The first [`SEED_FINGERPRINT_BYTES`] bytes of `SHA-256(seed)`, in
    /// lowercase hex.
    ///
    /// Two processes holding the same seed produce the same string; two holding
    /// different seeds do not. That is the whole contract.
    pub fn seed_fingerprint(&self) -> String {
        let digest = Sha256::digest(&self.seed);
        hex::encode(&digest[..SEED_FINGERPRINT_BYTES])
    }

    fn publish_seed_fingerprint(&self, seed_policy: AccessTokenSeedPolicy) {
        let fingerprint = self.seed_fingerprint();
        // The seed itself never reaches either sink; the fingerprint is what
        // both carry.
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

/// Creates the secret directory already private, in one step.
///
/// 🔴 Creating it and then tightening it are two steps, and between them the
/// directory exists at whatever the umask allowed — 0755 under the usual 022.
/// That is not a harmless intermediate state: [`resolve_seed`] starts by
/// validating this directory and treats anything other than 0700 as a hard
/// error, with no retry. So a second caller arriving inside that window fails
/// outright, on a directory this process was in the middle of securing. Worse
/// than the failure is what it would mean if the window ever widened: a secret
/// written into a world-readable directory.
///
/// `mkdir(2)` takes the mode with the directory, so there is no window to land
/// in. Only the leaf is created this way; ancestors keep ordinary permissions,
/// since making `$AENV_HOME` itself 0700 would lock out everything else that
/// legitimately reads from it.
///
/// `AlreadyExists` is success: either a previous run created it, or a
/// concurrent caller won the race. Both leave the caller's own validation to
/// decide whether what is there is acceptable.
///
/// An unusual umask (one masking bits inside 0700) yields a *stricter*
/// directory, which validation then rejects with the mode it found. Left to
/// fail deliberately: forcing the mode afterwards would restore the very window
/// this removes, and an operator running such a umask needs to know.
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

    /// The directory has to be private the instant it exists, not a moment
    /// after. Anything that creates it first and tightens it second leaves a
    /// window in which it is 0755 — and `resolve_seed` rejects that outright,
    /// so a concurrent caller landing in the window fails on a directory this
    /// process is in the middle of securing.
    ///
    /// Asserted against the mode on disk rather than through `resolve_seed`,
    /// because the window is invisible to any test that only looks at the end
    /// state.
    #[test]
    #[cfg(unix)]
    fn the_secret_directory_is_private_from_the_moment_it_exists() -> Result<()> {
        use std::os::unix::fs::PermissionsExt;

        let temp = TempDir::new()?;
        let secrets = temp.path().join("nested").join("secrets");

        create_private_directory(&secrets)?;

        let mode = fs::symlink_metadata(&secrets)?.permissions().mode() & 0o777;
        assert_eq!(mode, 0o700, "found {mode:04o}");

        // The ancestor it had to create on the way keeps ordinary permissions:
        // making $AENV_HOME itself 0700 would lock out everything else that
        // legitimately reads from it.
        let ancestor = fs::symlink_metadata(temp.path().join("nested"))?
            .permissions()
            .mode()
            & 0o777;
        assert_ne!(ancestor, 0o700, "only the leaf should be locked down");

        // Idempotent: a second run finds it already there and says so quietly.
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

    /// A config whose only variable is the seed, so the tests below differ from
    /// each other by exactly one thing.
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

    /// 🔴 The refusal and the thing it refuses, one config apart.
    ///
    /// Asserting only that [`AccessTokenSeedPolicy::MustBeConfigured`] fails
    /// would pass on a `load_or_create` that had simply stopped working;
    /// asserting only that [`AccessTokenSeedPolicy::MayGenerate`] succeeds
    /// would pass on the code as it was before this gate existed. The evidence
    /// is that the same directory, the same absent seed and the same call give
    /// opposite answers for the two policies — and that the refusing arm leaves
    /// no managed file behind, which is what says it refused *instead of*
    /// falling back rather than after having done so.
    ///
    /// 🔴 This seam is the only place the refusal can be covered, and the
    /// reason is worth knowing before somebody goes looking for the same test
    /// one layer up: `ConfigManager::set_global` injects
    /// `TEST_ACCESS_TOKEN_HASH_SEED` into every `#[cfg(test)]` build
    /// (`src/cfg.rs`), so an `Orchestrator::new(MustBeConfigured, ..)` in any
    /// unit test always finds a configured seed and always succeeds. A test
    /// there would look like it covered this and would not.
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
        // Actionable, in the words an operator would go looking for.
        assert!(message.contains(SEED_ENV_VAR), "{message}");
        assert!(message.contains("same value on every replica"), "{message}");
        assert!(message.contains("agentenv-runtime-secrets"), "{message}");
        // Refused before the fallback, not after it.
        assert!(
            !managed_path.exists(),
            "aenv-api generated a seed on its way to refusing"
        );
        Ok(())
    }

    /// What satisfies the refusal, and what does not. A seed that is only
    /// whitespace is the shape a Secret key present-but-empty takes, and it has
    /// to be as loud as a missing one rather than quietly becoming a valid
    /// zero-length key.
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

    /// 🔴 The half of the fingerprint that gives it any value: two different
    /// seeds must produce two different labels. "The same seed hashes to the
    /// same thing" is also true of a function that returns a constant, and a
    /// constant is exactly the failure mode this metric exists to rule out —
    /// two replicas reporting equal fingerprints while holding different seeds.
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

        // Pinned to the digest rather than to itself: a fingerprint that is
        // *some* stable function of the seed still fails the cluster if the two
        // replicas run builds that compute it differently.
        assert_eq!(alpha.seed_fingerprint(), "ba316cd7abc9b7dc");
        assert_eq!(beta.seed_fingerprint(), "d9c30acfd5686611");
        assert_eq!(alpha.seed_fingerprint().len(), SEED_FINGERPRINT_BYTES * 2);

        // And it is a hash, not the seed wearing a hat.
        assert!(!alpha.seed_fingerprint().contains("seed-alpha"));
        assert!(alpha
            .seed_fingerprint()
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)));
        Ok(())
    }

    /// The gauge, read the way a scrape reads it.
    ///
    /// 🔴 Three loads in one test, and the third is the control: without a run
    /// that publishes a *different* label, "the label matched" is satisfied by
    /// a recorder that only ever saw one value. What is asserted is the shape
    /// the §12 P4 probe compares across replicas — same seed, same label;
    /// different seed, different label — plus the fact that neither label is
    /// the seed.
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

    /// Every `fingerprint` label `load_or_create` published, at value 1.
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
